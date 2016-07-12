# -*- coding: utf-8 -*-

from grass.script.core import run_command
import os
import subprocess
import unicodecsv

SAGA = '/usr/bin/saga_cmd'
PROJECT = '/home/ggr/gis/projekte/sh'
PROJECT_FOLDER = os.path.join(PROJECT, 'tiffs')
RESULT_FOLDER = '/home/ggr/gis/datenaustausch/SH'
config_file = os.path.join(RESULT_FOLDER, 'gewichtung.txt')
einrichtungen_folder = os.path.join(PROJECT_FOLDER, 'einrichtungen')
smoothed_tiff_folder = os.path.join(PROJECT_FOLDER, 'er_buffer')
input_saga_folder = os.path.join(PROJECT_FOLDER, 'input_saga')
resampled_saga_folder = os.path.join(PROJECT_FOLDER, 'input_resampled')
out_saga_folder = os.path.join(PROJECT_FOLDER, 'er_clipped_saga')
result_tif_folder = os.path.join(RESULT_FOLDER, 'er_clipped_tifs')

combine_saga_folder = os.path.join(PROJECT_FOLDER, 'combine_saga')
combine_tiff_folder = os.path.join(RESULT_FOLDER, 'combine_tifs')
reference_file = os.path.join(PROJECT_FOLDER, 'reference', 'reference.sgrd')

RADIUS = 10
FACTORS = {20: 157.0268156729,
           10: 136.15303162,}


def main():
    """"""
    #convert()

    combine(zentrums_typ='lzo')
    #combine(zentrums_typ='gz')
    #combine(zentrums_typ='mz')

def combine(zentrums_typ):
    factor_name = 'weight_{}'.format(zentrums_typ)
    grids = []
    xgrids = []
    formula_elems = []
    i = -1

    with open(config_file) as f:
        r = unicodecsv.DictReader(f, dialect='excel-tab')
        for l in r:
            if l[factor_name]:
                fn = l['name']
                smoothed_file = os.path.join(smoothed_tiff_folder, fn)
                smoothed_file = os.extsep.join((smoothed_file, 'sgrd'))

                o_folder = '_'.join((out_saga_folder, zentrums_typ))
                if not os.path.exists(o_folder):
                    os.mkdir(o_folder)

                max_value = float(l['max_value_{}'.format(zentrums_typ)])
                out_saga = clip_values(o_folder, fn, smoothed_file,
                                       max_value)


                sgrd = os.path.join(o_folder,
                                    os.extsep.join((fn, 'sgrd')))


                #print sgrd
                i += 1
                if i == 0:
                    elem = 'g1'
                    grids.append(sgrd)
                else:
                    elem = 'h{}'.format(i)
                    xgrids.append(sgrd)
                factor = l[factor_name]
                formula_elems.append('{f}*{e}'.format(f=factor,
                                                        e=elem))
    formula = '+'.join(formula_elems)
    out_saga = calculate_combination(out_saga_folder=combine_saga_folder,
                                     fn=zentrums_typ,
                                     grids=grids,
                                     xgrids=xgrids,
                                     formula=formula)
    tif = os.extsep.join((zentrums_typ, 'tif'))
    export_result_to_tif(result_tif_folder=combine_tiff_folder,
                         tif=tif,
                         out_saga=out_saga)


def resample(in_saga_file,
             resampled_saga_folder,
             reference_file):
    f = os.path.split(in_saga_file)[1]
    resampled_saga_file = os.path.join(resampled_saga_folder, f)
    cmd = 'grid_tools'

    cmd_no = 0  # Resample
    NEAREST_NEIGHBOUR = 0
    GRIDSYSTEM = 1
    full_cmd = '{s} {c} {n} -INPUT "{g}" -OUTPUT "{o}" -KEEP_TYPE 1 -SCALE_UP {nn} -SCALE_DOWN {nn} -TARGET_DEFINITION {gs} -TARGET_TEMPLATE {t}'.format(
        s=SAGA, c=cmd, n=cmd_no, g=in_saga_file, o=resampled_saga_file,
        nn=NEAREST_NEIGHBOUR, gs=GRIDSYSTEM, t=reference_file)
    p = subprocess.Popen(full_cmd, shell=True)
    r = p.communicate()[0]

    cmd_no = 15  # Reclassify Grid Values
    full_cmd = '{s} {c} {n} -INPUT "{g}" -RESULT "{g}" -METHOD 0 -OLD 0 -NEW 0 -NODATAOPT 1 -NODATA 0 -RESULT_NODATA_CHOICE 1 -RESULT_NODATA_VALUE -1'.format(
        s=SAGA, c=cmd, n=cmd_no, g=resampled_saga_file)
    p = subprocess.Popen(full_cmd, shell=True)
    r = p.communicate()[0]


    return resampled_saga_file




def convert():
    with open(config_file) as f:
        r = unicodecsv.DictReader(f, dialect='excel-tab')
        for l in r:
            if l['use']:
                fn = l['name']
                tif = os.extsep.join((fn, 'tif'))
                print fn, tif

                in_saga_file = convert_input_to_saga(einrichtungen_folder,
                                                     tif,
                                                     input_saga_folder,
                                                     fn)

                resampled_saga_file = resample(in_saga_file,
                                               resampled_saga_folder,
                                               reference_file)

                smoothed_file = smooth(smoothed_tiff_folder, fn, resampled_saga_file)

                max_value = float(l['max_value'])
                out_saga = clip_values(out_saga_folder, fn, smoothed_file,
                                       max_value)

                export_result_to_tif(result_tif_folder, tif, out_saga)


def export_result_to_tif(result_tif_folder, tif, out_saga):
    # export as geotiff
    cmd = 'io_gdal'
    cmd_no = 2  # Export Geotiff
    out_tif = os.path.join(result_tif_folder, tif)
    full_cmd = '{s} {c} {n} -GRIDS "{g}" -FILE "{o}" -OPTIONS "COMPRESS=LZW"'.format(
        s=SAGA, c=cmd, n=cmd_no, g=out_saga, o=out_tif)
    p = subprocess.Popen(full_cmd, shell=True)
    r = p.communicate()[0]

def clip_values(out_saga_folder, fn, smoothed_file, max_value):
    out_saga = os.path.join(out_saga_folder, fn)
    out_saga = os.extsep.join((out_saga, 'sgrd'))
    cmd = 'grid_calculus'
    cmd_no = 1  # GridCalculator

    factor = FACTORS[RADIUS]
    full_cmd = '{s} {c} {n} -GRIDS "{g}" -RESAMPLING "0" -RESULT "{o}" -FORMULA "ifelse(((g1*{f})<{m}), g1*{f}, {m})" -TYPE 7'.format(
        s=SAGA, c=cmd, n=cmd_no, g=smoothed_file, o=out_saga, f=factor, m=max_value)
    p = subprocess.Popen(full_cmd, shell=True)
    r = p.communicate()[0]
    return out_saga

def smooth(smoothed_tiff_folder, fn, in_saga_file):
    smoothed_file = os.path.join(smoothed_tiff_folder, fn)
    smoothed_file = os.extsep.join((smoothed_file, 'sgrd'))
    cmd = 'grid_filter'
    cmd_no = 1  # Gaussian_filter
    sigma = 5
    radius = RADIUS
    CIRCLE = 1
    full_cmd = '{s} {c} {n} -INPUT "{g}" -RESULT "{o}" -SIGMA {si} -MODE {m} -RADIUS {r}'.format(
        s=SAGA, c=cmd, n=cmd_no, g=in_saga_file, o=smoothed_file, si=sigma,
        m=CIRCLE, r=radius)
    p = subprocess.Popen(full_cmd, shell=True)
    r = p.communicate()[0]
    return smoothed_file

def convert_input_to_saga(einrichtungen_folder, tif, input_saga_folder, fn):
    infile = os.path.join(einrichtungen_folder, tif)
    in_saga_file = os.path.join(input_saga_folder, fn)
    in_saga_file = os.extsep.join((in_saga_file, 'sgrd'))
    cmd = 'io_gdal'
    cmd_no = 0  # Input Tif
    TRANSFORM = 1
    NO_TRANSFORM = 0
    NEAREST_NEIGHBOUR = 0
    full_cmd = '{s} {c} {n} -FILES "{g}" -GRIDS "{o}" -TRANSFORM {t} -RESAMPLING {r}'.format(
        s=SAGA, c=cmd, n=cmd_no, g=infile, o=in_saga_file, t=TRANSFORM,
        r=NEAREST_NEIGHBOUR)
    p = subprocess.Popen(full_cmd, shell=True)
    r = p.communicate()[0]
    return in_saga_file


def calculate_combination(out_saga_folder, fn, grids, xgrids, formula):
    out_saga = os.path.join(out_saga_folder, fn)
    out_saga = os.extsep.join((out_saga, 'sgrd'))
    cmd = 'grid_calculus'
    cmd_no = 1  # GridCalculator

    grid_str = '\;'.join(('"{g}"'.format(g=grid) for grid in grids))
    xgrid_str = '\;'.join(('"{g}"'.format(g=grid) for grid in xgrids))

    full_cmd = '{s} {c} {n} -GRIDS {g} -XGRIDS {x} -RESAMPLING 0 -RESULT "{o}" -FORMULA "{f}" -TYPE 7'.format(
        s=SAGA, c=cmd, n=cmd_no, g=grid_str, x=xgrid_str, o=out_saga, f=formula)
    print(full_cmd)
    p = subprocess.Popen(full_cmd, shell=True)
    r = p.communicate()[0]
    return out_saga



if __name__ == '__main__':
    main()