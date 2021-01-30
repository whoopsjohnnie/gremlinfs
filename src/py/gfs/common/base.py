# 
# Copyright (c) 2019, 2020, John Grundback
# All rights reserved.
# 

from gfs.common.log import GFSLogger
from gfs.common.obj import GFSObj
from gfs.common.obj import gfslist
from gfs.common.obj import gfsmap

from gfs.error.error import GFSError
from gfs.error.error import GFSExistsError
from gfs.error.error import GFSNotExistsError
from gfs.error.error import GFSIsFileError
from gfs.error.error import GFSIsFolderError



class GFSBase(GFSObj):

    logger = GFSLogger.getLogger("GFSBase")

    def __init__(self, **kwargs):
        self.setall(kwargs)

    def property(self, name, default = None, prefix = None):
        return self.get(name, default, prefix = prefix)
