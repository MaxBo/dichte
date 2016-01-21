# -*- coding: utf-8 -*-

import os
import numpy as np
from elan.agents.raster_gis import Grid
from simcommon.matrixio import Aufwand, XArray, XRecArray
from dichte_berlin.grid import BerlinGrids

class Sim(object):
    def __init__(self, grid_folder):
        self._grid_folder = grid_folder



if __name__ == '__main__':
    grid_folder = r'E:\GGR\Berlin Dichte\30 Gis\31 gisserver_backup\tiffs'
    sim = Sim(grid_folder)
    grids = BerlinGrids(sim=sim)
    brandenburger_tor = (4550200, 3272800)
    dx = (brandenburger_tor[0] - grids.xOrigin) / 100
    dy = (brandenburger_tor[1] - grids.yOrigin) / -100

    def winkel_function(y, x):
        return np.degrees(np.arctan2(x-dx, y-dy))
    def dist_function(y, x):
        return np.sqrt(np.power(x-dx, 2) + np.power(y-dy, 2))

    grids.addGrid('ew_ha_raster', 'uint16', recreate=False)
    grids.addGrid('winkel', 'float', recreate=False)
    grids.addGrid('sector_ring', 'i2', recreate=False)
    grids.addGrid('sector', 'i2', recreate=False)
    w = grids.winkel
    w.initArray()
    winkel = np.fromfunction(winkel_function, (grids.ysize, grids.xsize))
    dist = np.fromfunction(dist_function, (grids.ysize, grids.xsize))
    w.array[:] = winkel
    w.writeValues()
    s = grids.sector
    sr = grids.sector_ring
    s.initArray()
    sr.initArray()
    bins = np.arange(-180, 225, 45)
    s.array[:] = np.mod((winkel + 22.5).view(XArray).classify(bins=bins), 8)
    bins_dist = np.arange(0, 2000, 10)
    sr.array = s.array + (dist.view(XArray).classify(bins=bins_dist).astype('i2') * 10)
    g = grids.ew_ha_raster.array
    n_elems = np.ceil((sr.array.max() + 1)/ 10.) * 10
    bc = np.bincount(sr.array.flatten(),
                     weights=g.flatten(),
                     minlength=n_elems)
    bc_besiedelt = np.bincount(sr.array.flatten(),
                     weights=(g>0).flatten(),
                     minlength=n_elems)
    dichte_besiedelte_rasterzellen = bc / bc_besiedelt
    res = bc.reshape(-1, 10)[:, :8][1:].astype(int)
    res_cumsum = res.cumsum(0)
    res_besiedelt = bc_besiedelt.reshape(-1, 10)[:, :8][1:].astype(float)
    n_rows = res.shape[0]

    res_a0 = np.zeros_like(res.reshape(-1, 4))
    res_a0[:n_rows] = res[::-1, :4]
    res_a0[n_rows:] = res[:, 4:]

    res_achsen = np.zeros_like(res.reshape(-1, 4))
    res_achsen[:n_rows] = res_cumsum[::-1, :4]
    res_achsen[n_rows:] = res_cumsum[:, 4:]

    res_achsen_besiedelt = np.zeros_like(res.reshape(-1, 4))
    res_achsen_besiedelt[:n_rows] = res_besiedelt[::-1, :4]
    res_achsen_besiedelt[n_rows:] = res_besiedelt[:, 4:]

    fn0 = 'result_rings.csv'
    file0 = os.path.join(grids._sim._grid_folder, fn0)
    np.savetxt(file0, res_a0, delimiter=';', fmt="%s")

    fn = 'result_rings2.csv'
    file = os.path.join(grids._sim._grid_folder, fn)
    np.savetxt(file, res_achsen, delimiter=';', fmt="%s")

    fn_besiedelt = 'result_rings_besiedelt.csv'
    file_besiedelt = os.path.join(grids._sim._grid_folder, fn_besiedelt)
    np.savetxt(file_besiedelt, res_achsen_besiedelt, delimiter=';', fmt="%s")

    lines = np.zeros(dtype=bool, shape=winkel.shape)
    lines[dy] = True
    lines[:, dx] = True
    # Südwest nach Nordost
    start_y = dx + dy
    start_x = 0
    stop_x = dx + dy
    stop_y = 0
    if start_y > grids.ysize:
        start_x = start_y - grids.ysize
        start_y = grids.ysize
    if stop_x > grids.xsize:
        stop_y = stop_x - grids.xsize
        stop_x = 0
    lines[np.arange(start_y, stop_y, -1).astype(int),
          np.arange(start_x, stop_x).astype(int)] = True
    # Südost nach Nordwest
    offset = int(dx - dy)
    if offset > 0:
        np.fill_diagonal(lines[:, offset:], True)
    else:
        np.fill_diagonal(lines[-offset:], True)
    sr.array[~lines] = 0
    arr = np.take(dichte_besiedelte_rasterzellen, sr.array)
    sr.array[:] = arr



    sr.writeValues()
    s.writeValues()




    pass
