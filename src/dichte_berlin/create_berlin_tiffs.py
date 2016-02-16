# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)


class ALKIS2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'dichte_ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        self.alkis_gfl_to_raster()
        self.export2tiff('geschossflaeche_raster')
        self.export2tiff('grundflaeche_raster')
        self.intersect_jobs()

    def alkis_gfl_to_raster(self):
        """convert ALKIS Geschlossfläche and Grundfläche to Raster"""
        self.create_alkis_geb_gfl()
        self.create_raster_for_polygon(
            tablename='geschossflaeche',
            source_table='geobasisdaten.alkis_gebaeude_gfl',
            value_column='gfl', noData=0)
        self.create_raster_for_polygon(
            tablename='grundflaeche',
            source_table='geobasisdaten.alkis_gebaeude_gfl',
            value_column='grfl', noData=0)

    def intersect_jobs(self):
        """Intersect the Verkehrszellen with the raster data"""
        weights = '{}.geschossflaeche_raster'.format(self.schema)
        self.intersect_polygon_with_weighted_raster(tablename='jobs_ew_1',
                                       source_table='geobasisdaten.vz_apl',
                                       id_column='vbz_no',
                                       value_column='summe_ew_1',
                                       weights=weights)

    def create_alkis_geb_gfl(self):
        """Create view with alkis Geschossflaeche"""
        sql = """
CREATE OR REPLACE VIEW geobasisdaten.alkis_gebaeude_gfl AS
SELECT
  g.objectid,
  g.geom,
  st_area(g.geom) AS grfl,
  st_area(g.geom) * coalesce(g.aog, a.aog_default) AS gfl
FROM geobasisdaten.alkis_gebaeude g
LEFT JOIN geobasisdaten.geschossanrechnung a
ON g.bat = a.bat
        """
        self.run_query(sql)


class ALKIS2km2Raster(ALKIS2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'dichte_km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with Berlin Density Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='dichte_berlin')

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
    options = parser.parse_args()

    model = ALKIS2km2Raster(options,
                            db=options.destination_db)
    model.set_login(host=options.host,
                  port=options.port,
                  user=options.user)
    model.run()
