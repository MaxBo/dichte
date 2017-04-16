# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from extractiontools.raster_from_points import (Points2Raster,
                                                Points2km2Raster)
import logging
logger = logging.getLogger()
logger.level = logging.DEBUG

import subprocess, os

class Einrichtungen2Raster(Points2Raster):
    """Convert data to raster data"""
    schema = 'dichte_ha'
    gridsize = 'ha'

    def process_pointlayer(self,
                           schema,
                           tablename,
                           nodata=-1,
                           value_column=None):
        """
        intersect point layer with raster and weight with "value_column"
        """
        self.intersect_point(tablename, schema, value_column)
        self.export_geotiff(schema, tablename, nodata)

    def intersect_point(self, tablename, schema, value_column=None):
        self.create_raster_for_point(
            tablename='{tn}'.format(tn=tablename),
            source_table='{sc}.{tn}'.format(sc=schema,
                                            tn=tablename),
            value_column=value_column)

    def export_geotiff(self, schema, tablename, nodata=-1):
        sql="""
        COPY (
        SELECT encode(st_astiff(st_setbandnodatavalue(st_union(rast), {nd}),
        ARRAY['COMPRESS=LZW']), 'hex')
        FROM {sc}.{tn}_raster)
        TO STDOUT;
        """.format(sc=self.schema, tn=tablename, nd=nodata)
        cmd = 'psql -c "{sql}" -d {db} -h {host} -U {user}'.format(
            sql=sql, db=self.login.db,
            host=self.login.host,
            user=self.login.user,
        )
        dump = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                shell=self.SHELL,
                                )
        folder = os.path.join(os.environ['HOME'], 'gis', 'projekte',
                              self.login.db, 'tiffs', 'einrichtungen')
        target = os.path.join(folder, '{tn}.tif'.format(tn=tablename))
        logger.info('write {}'.format(target))
        cmd_xxd = """xxd -p -r > '{target}'""".format(target=target)
        try:

            xxd = subprocess.check_output(cmd_xxd,
                                              stdin=dump.stdout,
                                              shell=self.SHELL)
        except subprocess.CalledProcessError as err:
            logger.info(err)

    def do_stuff(self):
        """
        define here, what to execute
        """
        self.intersect_bildung()
        self.intersect_gesundheit()
        self.intersect_kultur()
        self.intersect_oeff_dl()
        self.intersect_priv_dl()
        self.intersect_sport()
        self.intersect_ov()

    def intersect_ov(self):
        """Intersect the Bildung shapes with the raster data"""
        self.intersect_point(schema='timetables',
                             tablename='anzahl_abfahrten',
                             value_column='cnt')

    def intersect_bildung(self):
        """Intersect the Bildung shapes with the raster data"""
        self.process_pointlayer('bildung', 'allg')
        self.process_pointlayer('bildung', 'beruf')
        self.process_pointlayer('bildung', 'familienzentren')
        self.process_pointlayer('bildung', 'jugendhilfe_allgemeininfor')
        self.process_pointlayer('bildung', 'kitas')
        self.process_pointlayer('bildung', 'volkshochschulen')
        self.process_pointlayer('bildung', 'weiterbildung')
        self.process_pointlayer('bildung', 'grundschulen')
        self.process_pointlayer('bildung', 'sek1')
        self.process_pointlayer('bildung', 'sek2')
        self.process_pointlayer('bildung', 'foerderschulen')

    def intersect_gesundheit(self):
        """Intersect the Gesundheit shapes with the raster data"""
        self.process_pointlayer('gesundheit', 'nur_hausaerzte')
        self.process_pointlayer('gesundheit', 'nur_fachaerzte')
        self.process_pointlayer('gesundheit', 'geburtshilfe')
        self.process_pointlayer('gesundheit', 'krhs')
        self.process_pointlayer('gesundheit', 'pflege_dienste_neu')
        self.process_pointlayer('gesundheit', 'pflege_heime')
        #self.process_pointlayer('gesundheit', 'pflege_stuetzpunkte')
        self.process_pointlayer('gesundheit', 'zahnaerzte')

    def intersect_kultur(self):
        """Intersect the Kultureinrichtungen shapes with the raster data"""
        #self.process_pointlayer('kultureinrichtungen', 'dorfgemeinschaftshaus_mv')
        self.process_pointlayer('kultureinrichtungen', 'kinos_al')
        #self.process_pointlayer('kultureinrichtungen', 'kinos_osm')
        self.process_pointlayer('kultureinrichtungen', 'theater_konzert_veranstaltung_al')
        #self.process_pointlayer('kultureinrichtungen', 'theater_oper_osm')
        self.process_pointlayer('kultureinrichtungen', 'versammlungsstaetten_al')

    def intersect_oeff_dl(self):
        """Intersect the öffentlichen Dienstleistungen shapes with the raster data"""
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'aemter')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'berufsfeuerwehr')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'buergerbueros')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'buechereien')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'finanzaemter')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'gerichte')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'jobcenter')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'jugendfreizeiteinrichtung')
        self.process_pointlayer('oeffentl_dienstleisteinrichtung', 'polizei')

    def intersect_priv_dl(self):
        """Intersect the private Dienstleistungen shapes with the raster data"""
        self.process_pointlayer('private_versorgungseinrichtungen', 'apotheken')
        self.process_pointlayer('private_versorgungseinrichtungen', 'bankwesen')
        self.process_pointlayer('private_versorgungseinrichtungen', 'einkaufszentren')
        self.process_pointlayer('private_versorgungseinrichtungen', 'geschaefte')
        self.process_pointlayer('private_versorgungseinrichtungen', 'markttreff')
        self.process_pointlayer('private_versorgungseinrichtungen', 'poststellen')
        self.process_pointlayer('private_versorgungseinrichtungen', 'lebensmittel', value_column='gewicht')


    def intersect_sport(self):
        """Intersect the Sportanlagen shapes with the raster data"""
        self.process_pointlayer('sportanlagen', 'hallen_plaetze_baeder')
        #self.process_pointlayer('sportanlagen', 'hallen_baeder_al')
        #self.process_pointlayer('sportanlagen', 'sportflaechen_al')


class Einrichtungen2km2Raster(Einrichtungen2Raster, Points2km2Raster):
    """Convert data to raster data"""
    schema = 'dichte_km2'
    gridsize = 'km2'


if __name__ == '__main__':

    parser = ArgumentParser(description="Create Raster with sh data")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='sh')

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

    AllModels = {'km2': (Einrichtungen2km2Raster, ),
                 'ha': (Einrichtungen2Raster, ),}
    Models = AllModels[options.gridsize]

    for Model in Models:
        model = Model(options,
                      db=options.destination_db)
        model.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
        model.run()
