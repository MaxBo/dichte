# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)


class Data2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_ha'

    def do_stuff(self):
        """
        define here, what to execute
        """
        #self.intersect_einwohner()
        #self.intersect_jobs()

    def intersect_jobs(self):
        """Intersect the Verkehrszellen with the raster data"""
        weights = '{}.geschossflaeche_raster'.format(self.schema)
        self.intersect_polygon_with_weighted_raster(tablename='jobs_ew_1',
                                       source_table='geobasisdaten.vz_apl',
                                       id_column='vbz_no',
                                       value_column='summe_ew_1',
                                       weights=weights)

    def intersect_einwohner(self):
        """Intersect the Einwohner with the raster data"""
        self.create_ew_view()
        weights = '{}.geschossflaeche_raster'.format(self.schema)
        self.intersect_polygon_with_weighted_raster(
            tablename='einwohner_2014',
            source_table='geobasisdaten.ew_teilbaublock',
            id_column='schluessel',
            value_column='ew2014',
            weights=weights)
        sql = """
CREATE OR REPLACE VIEW geobasisdaten.ew_hauptblock_{jahr} AS
SELECT
  b.schluessel8,
  b.geom,
  b.einwohner
FROM geobasisdaten.ew_hauptblock_jahr b
WHERE jahr = {jahr};
        """
        for jahr in [1991, 1995, 2000, 2005, 2010, 2014]:
            self.run_query(sql.format(jahr=jahr))
            self.intersect_polygon_with_weighted_raster(
                tablename='ew_{}'.format(jahr),
                source_table='geobasisdaten.ew_hauptblock_{}'.format(jahr),
                id_column='schluessel8',
                value_column='einwohner',
                weights=weights)

    def create_ew_view(self):
        """Create View with Einwohnern"""
        sql = """
CREATE OR REPLACE VIEW geobasisdaten.ew_teilbaublock AS
SELECT
  b.schluessel,
  b.geom,
  e."EW2010" AS ew2010,
  e."EW2011" AS ew2011,
  e."EW2012" AS ew2012,
  e."EW2013" AS ew2013,
  e."EW2014" AS ew2014,
  b.schluessel8

FROM
  geobasisdaten.teilbaublock b,
  geobasisdaten.einwohner_2010_2014 e
WHERE b.schluessel = e.schluessel15;
        """
        self.run_query(sql)

        sql = """
CREATE OR REPLACE VIEW geobasisdaten.ew_hauptblock_jahr AS

SELECT
  b.schluessel8,
  b.geom,
  e.jahr,
  e.einwohner

FROM
  geobasisdaten.baublock b LEFT JOIN
  geobasisdaten.einwohner_hauptblock_1991_2005 e
ON b.schluessel8 = e.schluessel8

UNION ALL

SELECT
  b.schluessel8,
  b.geom,
  2010::integer AS jahr,
  sum(e."EW2010") AS einwohner
FROM
  geobasisdaten.baublock b LEFT JOIN
  geobasisdaten.einwohner_2010_2014 e
ON b.schluessel8 = e.schluessel8
GROUP BY b.schluessel8

UNION ALL

SELECT
  b.schluessel8,
  b.geom,
  2014::integer AS jahr,
  sum(e."EW2014") AS einwohner
FROM
  geobasisdaten.baublock b LEFT JOIN
  geobasisdaten.einwohner_2010_2014 e
ON b.schluessel8 = e.schluessel8
GROUP BY b.schluessel8;
            """
        self.run_query(sql)

class Data2km2Raster(Data2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_km2'


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
