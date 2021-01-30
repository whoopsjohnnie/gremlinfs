# 
# Copyright (c) 2019, 2020, John Grundback
# All rights reserved.
# 

# 
from __future__ import print_function
from future.utils import iteritems

# 
import os
import sys
import logging
import errno
import stat
import uuid
import re
import string

import contextlib

# 
from time import time

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# 3.3.0
# http://tinkerpop.apache.org/docs/3.3.0-SNAPSHOT/reference/#gremlin-python
# from gremlin_python import statics
# from gremlin_python.structure.graph import Graph, Vertex, Edge
# from gremlin_python.process.graph_traversal import __
# from gremlin_python.process.strategies import *
# from gremlin_python.process.traversal import T, P, Operator
# from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# 
# import pika

# 
from gfs.common.log import GFSLogger
from gfs.common.obj import GFSObj
from gfs.common.base import GFSBase

from gfs.error.error import GFSError
from gfs.error.error import GFSExistsError
from gfs.error.error import GFSNotExistsError
from gfs.error.error import GFSIsFileError
from gfs.error.error import GFSIsFolderError

from gfs.model.vertex import GFSVertex
from gfs.model.edge import GFSEdge

# from gfs.api.common.api import GFSAPI
from gfs.api.common.api import GFSCachingAPI

# 
# 
# import config



class GremlinFSConfig(GFSBase):

    logger = GFSLogger.getLogger("GremlinFSConfig")

    @classmethod
    def defaults(clazz):
        return {
            # "mount_point": None,

            "gfs_host": None,
            "gfs_port": None,
            "gfs_username": None,
            # "gfs_password": None,
            "gfs_url": None,

            "log_level": GFSLogger.getLogLevel(),

            "client_id": "0010",
            "fs_ns": "gfs1",
            "fs_root": None,
            "fs_root_init": False,

            "folder_label": 'group',
            "ref_label": 'ref',
            "in_label": 'in',
            "self_label": 'self',
            "template_label": 'template',
            "view_label": 'view',

            "in_name": 'in0',
            "self_name": 'self0',

            "vertex_folder": '.V',
            "edge_folder": '.E',
            "in_edge_folder": 'IN', # 'EI',
            "out_edge_folder": 'OUT', # 'EO',

            "uuid_property": 'uuid',
            "name_property": 'name',
            "data_property": 'data',
            "template_property": 'template',

            "default_uid": 1001,
            "default_gid": 1001,
            "default_mode": 0o777,

            "labels": []
        }

    def __init__(self, **kwargs):

        # Defaults
        self.setall(GremlinFSConfig.defaults())

        # Overrides
        self.setall(kwargs)

        # Build label regexes
        if self.has("labels"):
            for label_config in self.get("labels"):
                if "pattern" in label_config:
                    try:
                        label_config["compiled"] = re.compile(label_config["pattern"])
                    except Exception as e:
                        self.logger.exception(' GremlinFS: failed to compile pattern ' + label_config["pattern"], e)
                    pass
