# -*- coding: utf-8 -*-

from elan.agents.raster_gis import Grids, Grid
from osgeo.osr import SpatialReference
from cythonhelpers.configure_logger import get_logger

class BerlinGrids(Grids):
    """
    Collection of raster-grids
    """
    def __init__(self, sim=None):
        self.grids = {}
        self._sim = sim
        self.xsize = 1886
        self.ysize = 2030
        self.pixelsize_x = 100
        self.pixelsize_y = 100
        self.xOrigin = 4.4644e+06
        self.yOrigin = 3.3749e+06  #+ self.pixelsize_y * self.ysize
        self.srs = SpatialReference()
        err = self.srs.ImportFromEPSG(3035)
        # create Class logger
        self.logger = get_logger(self)

        if err:
            self.logger.warning('Spatial Ref System not recognized')

         # maximum distance where to search within a radius
        self.max_rings = 10


