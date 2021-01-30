
# 
# Copyright (c) 2019, 2020, 2021, John Grundback
# All rights reserved.
# 

from gfs.common.log import GFSLogger
from gfs.common.base import GFSBase



class GremlinFSEvent(GFSBase):

    logger = GFSLogger.getLogger("GremlinFSEvent")

    def __init__(self, **kwargs):
        self.setall(kwargs)

    def toJSON(self):
        pass
