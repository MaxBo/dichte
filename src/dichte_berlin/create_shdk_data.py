# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)


class Jobs2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        self.intersect_jobs_corine_umland()

    def intersect_jobs_corine_umland(self):
        """Intersect the Verkehrszellen with the corine raster data"""
        weights = 'landuse.corine_weights'
        self.intersect_polygon_with_weighted_raster(
            tablename='jobs_2014_corine',
            source_table='verwaltungsgrenzen.gem_2014_ew_svb_laea',
            id_column='ags',
            value_column='svb_ao',
            weights=weights)


class Jobskm2Raster(Jobs2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with Berlin Density Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='shdk')

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
    parser.add_argument('--ags', action="store",
                        help="ags der Kernstadt", type=str,
                        dest="kernstadt", default='11000000')


    options = parser.parse_args()

    Models = {'km2': Jobskm2Raster,
              'ha': Jobs2Raster,}
    Model = Models[options.gridsize]

    model = Model(options,
                  db=options.destination_db)
    model.set_login(host=options.host,
                  port=options.port,
                  user=options.user)
    model.kernstadt_ags = options.kernstadt
    model.run()
