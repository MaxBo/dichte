# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)


class Jobs2Raster(Points2Raster):
    """Convert data to raster data"""
    #schema = 'arbeitsplaetze'
    schema = 'xx_dichte_ha'
    schema_gfl = 'xx_dichte_ha'
    gridsize = 'ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        #self.intersect_jobs()
        #self.gmes_weighted()
        #self.intersect_jobs_umland()
        #self.intersect_einwohner_umland()
        self.intersect_jobs_corine_umland()

    def intersect_jobs(self):
        """Intersect the Verkehrszellen with the raster data"""
        weights = '{}.geschossflaeche_raster'.format(self.schema_gfl)
        self.intersect_polygon_with_weighted_raster(
            tablename='jobs_2005_{gr}'.format(gr=self.gridsize),
            source_table='arbeitsplaetze.ap',
            id_column='zgb_id ',
            value_column='ap_gesamt',
            weights=weights)

    def intersect_jobs_umland(self):
        """Intersect the Verkehrszellen with the gmes raster data"""
        weights = '{}.gmes12_weight_gewerbe_raster'.format(self.schema_gfl)
        self.intersect_polygon_with_weighted_raster(
            tablename='jobs_2013_{gr}'.format(gr=self.gridsize),
            source_table='gemeinden.gem_ew_apl',
            id_column='gem_nr',
            value_column='jobs',
            weights=weights)

    def intersect_jobs_corine_umland(self):
        """Intersect the Verkehrszellen with the corine raster data"""
        weights = '{}.corine_gewerbe_weight_raster'.format(self.schema_gfl)
        self.intersect_polygon_with_weighted_raster(
            tablename='jobs_2013_corine',
            source_table='gemeinden.gem_ew_apl',
            id_column='gem_nr',
            value_column='jobs',
            weights=weights)

    def intersect_einwohner_umland(self):
        """Intersect the Verkehrszellen with the raster data"""
        weights = '{}.gmes12_weight_gewerbe_raster'.format(self.schema_gfl)
        self.intersect_polygon_with_weighted_raster(
            tablename='einwohner_2013_{gr}'.format(gr=self.gridsize),
            source_table='gemeinden.gem_ew_apl',
            id_column='gem_nr',
            value_column='einwohner',
            weights=weights)

    def gmes_weighted(self):
        """
weighted gmes data
        """
        self.create_raster_for_polygon(
            tablename='gmes12_weight_wohnen',
            source_table='gmes.density_weight',
            value_column='wohnen', noData=0)

        self.create_raster_for_polygon(
            tablename='gmes12_weight_gewerbe',
            source_table='gmes.density_weight',
            value_column='gewerbe', noData=0)


class Einwohner2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'einwohner'
    schema_gfl = 'xx_dichte_ha'
    gridsize = 'ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        #self.intersect_einwohner()

    def intersect_einwohner(self):
        """Intersect the Verkehrszellen with the raster data"""
        weights = '{}.geschossflaeche_raster'.format(self.schema_gfl)
        self.intersect_polygon_with_weighted_raster(
            tablename='einwohner_{gr}'.format(gr=self.gridsize),
            source_table='einwohner.ew_hws',
            id_column='blk_id',
            value_column='ew_hws',
            weights=weights)


class Jobs2km2Raster(Jobs2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_km2'
    schema_gfl = 'xx_dichte_km2'
    gridsize = 'km2'


class Einwohner2km2Raster(Einwohner2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema_gfl = 'xx_dichte_km2'
    gridsize = 'km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with Wien Density Data")

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
