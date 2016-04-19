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
        #self.alkis_gfl_to_raster()
        #self.export2tiff('geschossflaeche_raster')
        #self.export2tiff('grundflaeche_raster')
        self.create_kernstadt_raster()
        #self.gmes_weighted()
        #self.alkis_wfl_gfl_to_raster()

    def create_kernstadt_raster(self):
        """Convert kernstadt to raster"""
        sql = """
CREATE OR REPLACE VIEW verwaltungsgrenzen.kernstadt AS
SELECT
g.ags,
g.gen,
CASE WHEN ags = '{ags}' THEN 1
ELSE 0
END AS kernstadt,
g.geom
FROM verwaltungsgrenzen.gem_2014_ew_svb g;

CREATE OR REPLACE VIEW {sc}.kernstadt_pnt AS
SELECT
l.cellcode,
g.kernstadt AS value,
l.pnt_laea,
row_number() OVER(ORDER BY l.cellcode)::integer AS rn

FROM verwaltungsgrenzen.kernstadt g,
laea.laea_vector_100 l
WHERE st_intersects(l.pnt, g.geom);
        """
        self.run_query(sql.format(ags=self.kernstadt_ags, sc=self.schema))
        self.create_raster_for_table(tablename='kernstadt', pixeltype='1BB', noData=-1)

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

    def alkis_wfl_gfl_to_raster(self):
        """convert ALKIS Geschlossfläche and Grundfläche to Raster"""
        #self.create_alkis_geb_gfl()
        self.create_raster_for_polygon(
            tablename='wohnflaeche',
            source_table='geobasisdaten.geobasisdaten_wfl_gewerbfl',
            value_column='wohnfl', noData=0)
        self.create_raster_for_polygon(
            tablename='gewerbl_nutzfl',
            source_table='geobasisdaten.geobasisdaten_wfl_gewerbfl',
            value_column='gewerbl_nutzfl', noData=0)

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


    def gmes_to_raster(self):
        # convert GMES Flaechen to Raster"""
        #self.create_gmes06_vfl()
        self.create_gmes12_vfl()
        #self.create_gmes06_sfl_wohnen()
        self.create_gmes12_sfl_wohnen()
        #self.create_gmes06_sfl_gewerbe()
        self.create_gmes12_sfl_gewerbe()
        #self.create_gmes06_sfl_gruen_sport()
        self.create_gmes12_sfl_gruen_sport()
        self.conn.commit()

        #create Polygons for converted Rasters
        #self.create_raster_for_polygon(
            #tablename='gmes06_vfl',
            #source_table='gmes.gmes06_vfl',
            #value_column='area', noData=0)

        #self.create_raster_for_polygon(
            #tablename='gmes06_sfl_wohnen',
            #source_table='gmes.gmes06_sfl_wohnen',
            #value_column='area', noData=0)

        #self.create_raster_for_polygon(
            #tablename='gmes06_sfl_gewerbe',
            #source_table='gmes.gmes06_sfl_gewerbe',
            #value_column='area', noData=0)

        #self.create_raster_for_polygon(
            #tablename='gmes06_sfl_gruen_sport',
            #source_table='gmes.gmes06_sfl_gruen_sport',
            #value_column='area', noData=0)

        self.create_raster_for_polygon(
            tablename='gmes12_vfl',
            source_table='gmes.gmes12_vfl',
            value_column='area', noData=0)

        self.create_raster_for_polygon(
            tablename='gmes12_sfl_wohnen',
            source_table='gmes.gmes12_sfl_wohnen',
            value_column='area', noData=0)

        self.create_raster_for_polygon(
            tablename='gmes12_sfl_gewerbe',
            source_table='gmes.gmes12_sfl_gewerbe',
            value_column='area', noData=0)

        self.create_raster_for_polygon(
            tablename='gmes12_sfl_gruen_sport',
            source_table='gmes.gmes12_sfl_gruen_sport',
            value_column='area', noData=0)


    def create_gmes06_vfl(self):
        """Create view with gmes Verkehrsflaeche 06"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes06_vfl AS
        SELECT g."CODE2006" AS code06,
        g."ITEM2006" AS item06,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2006 g
        WHERE g."CODE2006"::text = '12210'::text OR
        g."CODE2006"::text = '12220'::text OR
        g."CODE2006"::text = '12230'::text OR
        g."CODE2006"::text = '12300'::text OR
        g."CODE2006"::text = '12400'::text;
        """
        self.run_query(sql)

    def create_gmes12_vfl(self):
        """Create view with gmes Verkehrsflaeche 12"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes12_vfl AS
        SELECT g."CODE2012" AS code12,
        g."ITEM2012" AS item12,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2012 g
        WHERE g."CODE2012"::text = '12210'::text OR
        g."CODE2012"::text = '12220'::text
        OR g."CODE2012"::text = '12230'::text
        OR g."CODE2012"::text = '12300'::text
        OR g."CODE2012"::text = '12400'::text;
        """
        self.run_query(sql)

    def create_gmes06_sfl_wohnen(self):
        """Create view with gmes SFL Wohnen 06"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes06_sfl_wohnen AS
        SELECT g."CODE2006" AS code06,
        g."ITEM2006" AS item06,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2006 g
        WHERE g."CODE2006"::text = '11100'::text
        OR g."CODE2006"::text = '11210'::text
        OR g."CODE2006"::text = '11220'::text
        OR g."CODE2006"::text = '11230'::text
        OR g."CODE2006"::text = '11240'::text
        OR g."CODE2006"::text = '11300'::text;
        """
        self.run_query(sql)

    def create_gmes06_sfl_gewerbe(self):
        """Create view with gmes Verkehrsflaeche 06"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes06_sfl_gewerbe AS
        SELECT g."CODE2006" AS code06,
        g."ITEM2006" AS item06,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2006 g
        WHERE g."CODE2006"::text = '12100'::text
        """
        self.run_query(sql)

    def create_gmes12_sfl_wohnen(self):
        """Create view with gmes Verkehrsflaeche 12"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes12_sfl_wohnen AS
        SELECT g."CODE2012" AS code12,
        g."ITEM2012" AS item12,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2012 g
        WHERE g."CODE2012"::text = '11100'::text
        OR g."CODE2012"::text = '11210'::text
        OR g."CODE2012"::text = '11220'::text
        OR g."CODE2012"::text = '11230'::text
        OR g."CODE2012"::text = '11240'::text
        OR g."CODE2012"::text = '11300'::text;
        """
        self.run_query(sql)

    def create_gmes12_sfl_gewerbe(self):
        """Create view with gmes Verkehrsflaeche 12"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes12_sfl_gewerbe AS
        SELECT g."CODE2012" AS code12,
        g."ITEM2012" AS item12,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2012 g
        WHERE g."CODE2012"::text = '12100'::text
        """
        self.run_query(sql)


    def create_gmes06_sfl_gruen_sport(self):
        """Create view with gmes Sport und Gruenflaeche 06"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes06_sfl_gruen_sport AS
        SELECT g."CODE2006" AS code06,
        g."ITEM2006" AS item06,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2006 g
        WHERE g."CODE2006"::text = '14100'::text
        OR g."CODE2006"::text = '14200'::text
        """
        self.run_query(sql)

    def create_gmes12_sfl_gruen_sport(self):
        """Create view with gmes Sport und Gruenflaeche 12"""
        sql = """
        CREATE OR REPLACE VIEW gmes.gmes12_sfl_gruen_sport AS
        SELECT g."CODE2012" AS code12,
        g."ITEM2012" AS item12,
        g.geom,
        st_area(g.geom) AS area
        FROM gmes.ua2012 g
        WHERE g."CODE2012"::text = '14100'::text
        OR g."CODE2012"::text = '14200'::text
        """
        self.run_query(sql)

class ALKIS2km2Raster(ALKIS2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'xx_dichte_km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with Berlin Density Data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='dichte_hamburg')

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

    Models = {'km2': ALKIS2km2Raster,
              'ha': ALKIS2Raster,}
    Model = Models[options.gridsize]

    model = Model(options,
                  db=options.destination_db)
    model.set_login(host=options.host,
                  port=options.port,
                  user=options.user)
    model.kernstadt_ags = options.kernstadt
    model.run()
