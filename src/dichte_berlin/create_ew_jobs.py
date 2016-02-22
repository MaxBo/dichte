# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.zensus2raster import (Zensus2Raster)
from extractiontools.raster_from_points import Points2km2Raster


class Data2Raster(Zensus2Raster):
    schema = 'laea'


    def do_stuff(self):
        """
        define here, what to execute
        """
        self.ew2hectar()
        #self.create_jobs_hamburg_raster()

    def create_jobs_hamburg_raster(self):
        """Intersect the Jobs with the raster data"""
        self.create_raster_for_point(tablename='jobs',
                                     source_table='mrhh.arbeitspl',
                                     value_column='alle',
                                     pixeltype='32BF',
                                     noData=0)


class Data2km2Raster(Data2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_km2'




if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with Density Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='dichte_wien')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)
    parser.add_argument("-U", '--user', action="store",
                        help="user", type=str,
                        dest="user", default='osm')
    parser.add_argument('--subfolder', action="store",
                        help="subfolder to store the tiffs", type=str,
                        dest="subfolder", default='tiffs')
    parser.add_argument('--gridsize', action="store",
                        help="gridsize to use", type=str,
                        choices=['km2', 'ha'],
                        dest="gridsize", default='ha')
    options = parser.parse_args()

    Models = {'km2': Data2km2Raster,
              'ha': Data2Raster,}
    Model = Models[options.gridsize]

    model = Model(options,
                  db=options.destination_db)
    model.set_login(host=options.host,
                  port=options.port,
                  user=options.user)
    model.run()
