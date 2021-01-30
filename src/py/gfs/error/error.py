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
from .common.log import GFSLogger
from .common.obj import GFSObj

# from .api.common.api import GFSAPI
from .api.common.api import GFSCachingAPI

# 
# 
# import config



class GremlinFSError(Exception):

    def __init__(self, path = None):
        self.path = path



class GremlinFSExistsError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path



class GremlinFSNotExistsError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path



class GremlinFSIsFileError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path



class GremlinFSIsFolderError(GremlinFSError):

    def __init__(self, path = None):
        self.path = path
