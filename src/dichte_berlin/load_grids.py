# -*- coding: utf-8 -*-

import os
import numpy as np
from osgeo import gdal, osr

from simcommon.raster.grid import Grid
from simcommon.matrixio import Aufwand, XArray, XRecArray
from simcommon.raster.raster_gis import Grids

from argparse import ArgumentParser
from extractiontools.ausschnitt import Extract
from extractiontools.connection import Login, Connection


class SmoothGrid(Extract):
    def __init__(self, options):

        """"""
        self.options = options
        self.check_platform()
        self.login1 = Login(self.options.host,
                            self.options.port,
                            self.options.user,
                            db=self.options.destination_db)

    def pg_raster_to_array(self, schema, tablename, grid_folder='%TEMP%'):
        """
        read array from postgis raster table
        """
        virtual_path = '/vsimem/from_postgis'
        with Connection(self.login1) as conn:
            cur = conn.cursor()
            sql = '''
SELECT ST_AsGDALRaster(st_union(rast), 'GTiff')
FROM {schema}.{table};
            '''.format(schema=schema, table=tablename)
            cur.execute(sql)
            # read results
            gdal.FileFromMemBuffer(virtual_path, bytes(cur.fetchone()[0]))
            ds = gdal.Open(virtual_path)
            self.grids = Grids.from_gdal_virtual_tiff(ds,
                                                 grid_folder,
                                                 grid_name=tablename,
                                                 max_rings=10)
            ds = None
            gdal.Unlink(virtual_path)
        return getattr(self.grids, tablename).array

    def geotiff_to_array(self, in_file, grid_folder):
        """
        read an array from a geotiff
        """
        ds = gdal.Open(in_file)
        self.grids = Grids.from_gdal_tiff(ds)
        ds = None
        fn = os.path.splitext(os.path.split(in_file)[-1])[0]
        return getattr(self.grids, fn).array


    def build_weights(self,
                      size=3,
                      beta=-1):
        """
        create the weight kernel

        Parameters
        ----------
        size : int
            the number of pixels in each direction the kernel comprises
        beta : float
            the impedance for the impedance function

        Returns
        -------
        weights : 2D-Array
            array of shape (2 * size + 1, 2 * size + 1)
        """
        # erzeuge Kernel
        x = np.arange(-size, size+1)
        y = np.arange(-size, size+1)
        xx, yy = np.meshgrid(x, y)
        # Distanz zum Kernel-Mittelpunkt
        dist = np.sqrt(xx ** 2 + yy ** 2)
        # Distance Decay-Parameter
        weights = np.exp(beta * dist)
        return weights

    def build_frequencies(self, input_array):
        """
        create the frequency-array
        """
        # Gewichtung der Bezugsflächen 1
        freq = np.ones(input_array.shape)
        # only include inhabited raster cells into Bezugsflächen
        freq[input_array == 0] = 0
        return freq


    def smooth(self, val, weights, freq):
        """
        Glätte Werte val mit Kernel weights
        und gewichte umliegende Zellen zudem mit freq
        """
        g = self.grids
        g.init_array('result', val.shape, default=0)
        g.set_array('data_d', val, val.shape)
        g.set_array('frequencies', freq, freq.shape)
        g.set_weights(weights)
        g.calc_moving_window()

        self.set_result_to_zero(val)

    def set_result_to_zero(self, val):
        # setze Ergebnis auf 0 auf unbesiedelten Rasterzellen
        # (Dichte der Eingangsdaten = 0)
        self.grids.result[val == 0] = 0

    def write_result(self, grid_name):
        """
        write result for grid with grid_name
        """
        self.grids.addGrid(grid_name, 'd', pixelsize=self.grids.pixelsize_x)
        self.grids.set_value_to_grid(grid_name, self.grids.result)
        self.grids.write_grid_to_file(grid_name)


    def calc_sectors(self):
        """
        Calc the sectors
        """
        brandenburger_tor = (4550200, 3272800)
        dx = (brandenburger_tor[0] - grids.xOrigin) / 100
        dy = (brandenburger_tor[1] - grids.yOrigin) / -100

        def winkel_function(y, x):
            return np.degrees(np.arctan2(x-dx, y-dy))
        def dist_function(y, x):
            return np.sqrt(np.power(x-dx, 2) + np.power(y-dy, 2))

        grids.addGrid('ew_ha_raster', 'uint16', recreate=False)
        grids.addGrid('winkel', 'float', recreate=False)
        grids.addGrid('sector_ring', 'i2', recreate=False)
        grids.addGrid('sector', 'i2', recreate=False)
        w = grids.winkel
        w.initArray()
        winkel = np.fromfunction(winkel_function, (grids.ysize, grids.xsize))
        dist = np.fromfunction(dist_function, (grids.ysize, grids.xsize))
        w.array[:] = winkel
        w.writeValues()
        s = grids.sector
        sr = grids.sector_ring
        s.initArray()
        sr.initArray()
        bins = np.arange(-180, 225, 45)
        s.array[:] = np.mod((winkel + 22.5).view(XArray).classify(bins=bins), 8)
        bins_dist = np.arange(0, 2000, 10)
        sr.array = s.array + (dist.view(XArray).classify(bins=bins_dist).astype('i2') * 10)
        g = grids.ew_ha_raster.array
        n_elems = np.ceil((sr.array.max() + 1)/ 10.) * 10
        bc = np.bincount(sr.array.flatten(),
                         weights=g.flatten(),
                         minlength=n_elems)
        bc_besiedelt = np.bincount(sr.array.flatten(),
                         weights=(g>0).flatten(),
                         minlength=n_elems)
        dichte_besiedelte_rasterzellen = bc / bc_besiedelt
        res = bc.reshape(-1, 10)[:, :8][1:].astype(int)
        res_cumsum = res.cumsum(0)
        res_besiedelt = bc_besiedelt.reshape(-1, 10)[:, :8][1:].astype(float)
        n_rows = res.shape[0]

        res_a0 = np.zeros_like(res.reshape(-1, 4))
        res_a0[:n_rows] = res[::-1, :4]
        res_a0[n_rows:] = res[:, 4:]

        res_achsen = np.zeros_like(res.reshape(-1, 4))
        res_achsen[:n_rows] = res_cumsum[::-1, :4]
        res_achsen[n_rows:] = res_cumsum[:, 4:]

        res_achsen_besiedelt = np.zeros_like(res.reshape(-1, 4))
        res_achsen_besiedelt[:n_rows] = res_besiedelt[::-1, :4]
        res_achsen_besiedelt[n_rows:] = res_besiedelt[:, 4:]

        fn0 = 'result_rings.csv'
        file0 = os.path.join(grids._sim._grid_folder, fn0)
        np.savetxt(file0, res_a0, delimiter=';', fmt="%s")

        fn = 'result_rings2.csv'
        file = os.path.join(grids._sim._grid_folder, fn)
        np.savetxt(file, res_achsen, delimiter=';', fmt="%s")

        fn_besiedelt = 'result_rings_besiedelt.csv'
        file_besiedelt = os.path.join(grids._sim._grid_folder, fn_besiedelt)
        np.savetxt(file_besiedelt, res_achsen_besiedelt, delimiter=';', fmt="%s")

        lines = np.zeros(dtype=bool, shape=winkel.shape)
        lines[dy] = True
        lines[:, dx] = True
        # Südwest nach Nordost
        start_y = dx + dy
        start_x = 0
        stop_x = dx + dy
        stop_y = 0
        if start_y > grids.ysize:
            start_x = start_y - grids.ysize
            start_y = grids.ysize
        if stop_x > grids.xsize:
            stop_y = stop_x - grids.xsize
            stop_x = 0
        lines[np.arange(start_y, stop_y, -1).astype(int),
              np.arange(start_x, stop_x).astype(int)] = True
        # Südost nach Nordwest
        offset = int(dx - dy)
        if offset > 0:
            np.fill_diagonal(lines[:, offset:], True)
        else:
            np.fill_diagonal(lines[-offset:], True)
        sr.array[~lines] = 0
        arr = np.take(dichte_besiedelte_rasterzellen, sr.array)
        sr.array[:] = arr



        sr.writeValues()
        s.writeValues()



if __name__ == '__main__':
    parser = ArgumentParser(description="Create Raster with Berlin Density Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='dichte_berlin')

    arg_db = parser.add_argument_group('DB_Config', 'Database connection arguments')
    arg_db.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    arg_db.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)
    arg_db.add_argument("-U", '--user', action="store",
                        help="user", type=str,
                        dest="user", default='osm')
    parser.add_argument('--subfolder', action="store",
                        help="subfolder to store the tiffs", type=str,
                        dest="subfolder", default='tiffs')

    table_grp = parser.add_argument_group('PostgisRaster', 'Postgis Raster to smooth')
    table_grp.add_argument('--schema', action="store",
                           help="schema of raster to smooth", type=str,
                           dest="schema", default='xx_dichte_ha')
    table_grp.add_argument('--tablename', action="store",
                           help="tablename of raster to smooth", type=str,
                           dest="tablename", default='einwohner_2014_raster')

    infile_grp = parser.add_argument_group('Tiff', 'Tiff to Smooth')
    infile_grp.add_argument('--infile', action="store",
                            help="path to geotiff-raster", type=str,
                            dest="in_file", default=None)

    parser.add_argument('--outfolder', action="store",
                        help="folder to store the smoothed raster", type=str,
                        dest="out_folder", default=None)


    kernel_grp = parser.add_argument_group('KernelParams', 'Kernel Parameters')
    kernel_grp.add_argument('--kernelsize', action="store",
                        help="size of the kernel in pixel from the center", type=int,
                        dest="kernelsize", default=4)
    kernel_grp.add_argument('--kernel_beta', action="store",
                        help="distance decay parameter of the kernel ", type=float,
                        dest="beta", default=-1)
    options = parser.parse_args()

    sm = SmoothGrid(options)
    if options.in_file:
        out_folder = options.out_folder or os.path.split(options.in_file)[0]
        filename = os.path.splitext(os.path.split(options.in_file)[1])[0]
        val = sm.geotiff_to_array(options.in_file, out_folder)
    else:
        filename = options.tablename
        if not options.out_folder:
            raise ValueError('out_folder has to be defined')
        val = sm.pg_raster_to_array(
            schema=options.schema,
            tablename=filename,
            grid_folder=options.out_folder)
    result_table = '{}_smoothed'.format(filename)
    weights = sm.build_weights(size=options.kernelsize, beta=options.beta)
    freq = sm.build_frequencies(val)
    sm.smooth(val, weights, freq)
    sm.write_result(grid_name=result_table)