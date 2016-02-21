# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)


class Data2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'tourismus'

    def do_stuff(self):
        """
        define here, what to execute
        """
        self.create_tourism_raster()

    def create_raster(self,
                      tablename,
                      pixeltype='32BF',
                      noData=0,
                      value_col='value'):
        """
        intersect feature with raster and create raster tiff
        """

        self.point2raster(
            point_feature='{sc}.{tn}'.format(sc=self.schema,
                                                 tn=tablename),
            geom_col='pnt_laea',
            value_col=value_col,
            target_raster='{sc}.{tn}_raster'.format(sc=self.schema,
                                                    tn=tablename),
            pixeltype=pixeltype,
            srid=self.srid,
            reference_raster=self.reference_raster,
            raster_pkey='rid',
            raster_col='rast',
            band=1,
            noData=noData,
            overwrite=True)


    def create_tourism_raster(self):
        """Intersect the Verkehrszellen with the raster data"""
        self.create_raster('laea_ha_flickr',
                           pixeltype='32BF', noData=0,
                           value_col='pictures')



class Data2km2Raster(Data2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'dichte_km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with Berlin Density Data")

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
                        dest="gridsize", default='km2')
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
