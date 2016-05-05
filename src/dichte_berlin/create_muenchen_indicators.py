# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)


class Jobs2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'arbeitsplaetze'
    schema_gfl = 'xx_dichte_ha'
    gridsize = 'ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        #self.intersect_jobs()

    def intersect_jobs(self):
        """Intersect the Verkehrszellen with the raster data"""
        weights = '{}.geschossflaeche_raster'.format(self.schema_gfl)
        self.intersect_polygon_with_weighted_raster(
            tablename='jobs_2005_{gr}'.format(gr=self.gridsize),
            source_table='arbeitsplaetze.arbeitsplaetze_viertel',
            id_column='zv03 ',
            value_column='apl2005',
            weights=weights)

class Einwohner2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'einwohner'
    schema_gfl = 'xx_dichte_ha'
    gridsize = 'ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        self.intersect_einwohner_jahre()

    def intersect_einwohner(self):
        """Intersect the Verkehrszellen with the raster data"""
        weights = '{}.geschossflaeche_raster'.format(self.schema_gfl)
        self.intersect_polygon_with_weighted_raster(
            tablename='einwohner_{gr}'.format(gr=self.gridsize),
            source_table='einwohner.einwohner_block',
            id_column='block ',
            value_column='hauptwohnsitz',
            weights=weights)

    def intersect_einwohner_viertel_jahr(self, jahr):
        """Intersect the Verkehrszellen with the raster data"""
        weights = 'einwohner.einwohner_{}_raster'.format(self.gridsize)
        self.intersect_polygon_with_weighted_raster(
            tablename='einwohner_{y}_{gr}'.format(y=jahr, gr=self.gridsize),
            source_table='einwohner.matview_viertel_zeitreihe',
            id_column='nr',
            value_column='ew_{y}'.format(y=jahr),
            weights=weights)

    def intersect_einwohner_jahre(self):
        """
        """
        for jahr in [2000, 2006, 2012, 2015]:
            self.intersect_einwohner_viertel_jahr(jahr)

class Jobs2km2Raster(Jobs2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema_gfl = 'xx_dichte_km2'
    gridsize = 'km2'


class Einwohner2km2Raster(Einwohner2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema_gfl = 'xx_dichte_km2'
    gridsize = 'km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with München Density Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='dichte_muenchen')

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

    AllModels = {'km2': (Jobs2km2Raster, Einwohner2km2Raster),
              'ha': (Jobs2Raster, Einwohner2Raster),}
    Models = AllModels[options.gridsize]

    for Model in Models:
        model = Model(options,
                      db=options.destination_db)
        model.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
        model.run()
