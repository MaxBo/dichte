# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)


class ALKIS2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        #self.gmes_to_raster()
        self.alkis_gfl_to_raster()
        #self.export2tiff('geschossflaeche_raster')
        #self.export2tiff('grundflaeche_raster')

    def alkis_gfl_to_raster(self):
        """convert ALKIS Geschlossfläche and Grundfläche to Raster"""
        self.create_raster_for_polygon(
            tablename='wohnflaeche',
            source_table='gebaeude_geschossflaeche.lsoa_residential_m2',
            value_column='wfl_m2', noData=0)
        self.create_raster_for_polygon(
            tablename='gewerbl_geschossflaeche',
            source_table='gebaeude_geschossflaeche.lsoa_commercial_all',
            value_column='commercial_m2', noData=0)
        self.create_raster_for_polygon(
            tablename='grundflaeche',
            source_table='gebaeude_geschossflaeche.opmplc_buildings',
            value_column='area', noData=0)



class ALKIS2km2Raster(ALKIS2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with London Density Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='dichte_london')

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

    Models = {'km2': ALKIS2km2Raster,
              'ha': ALKIS2Raster,}
    Model = Models[options.gridsize]

    model = Model(options,
                  db=options.destination_db)
    model.set_login(host=options.host,
                  port=options.port,
                  user=options.user)
    model.run()
