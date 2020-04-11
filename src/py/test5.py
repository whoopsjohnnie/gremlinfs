# 
# Copyright (c) 2019, John Grundback
# All rights reserved.
# 

# 
import os
import sys
import logging
import errno
import stat
import uuid
import re
import traceback
import string

import contextlib

# 
from time import time

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# 
# from fuse import FUSE
# from fuse import Operations
# from fuse import FuseOSError

# 3.3.0
# http://tinkerpop.apache.org/docs/3.3.0-SNAPSHOT/reference/#gremlin-python
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

# 
# import pika

# 
# 
# from gremlinfs.gremlinfslog import GremlinFSLogger

from gremlinfs.gremlinfslib import GremlinFSPath
from gremlinfs.gremlinfslib import GremlinFSUtils
from gremlinfs.gremlinfslib import GremlinFS

# from gremlinfs.gremlinfs import GremlinFSOperations

#
# 
import config



class GremlinFSOperations(GremlinFS):
 
    # logger = GremlinFSLogger.getLogger("GremlinFSOperations") # __name__)
 
#     def __init__(
#         self,
#  
#         mount_point,
#  
#         gremlin_host,
#         gremlin_port,
#         gremlin_username,
#         gremlin_password,
#  
#         rabbitmq_host = None,
#         rabbitmq_port = None,
#         rabbitmq_username = None,
#         rabbitmq_password = None,
#  
#         **kwargs):
#  
#         # JS jump:
#         # ReferenceError: Must call super constructor in derived class before accessing 'this' or returning from derived constructor
#         super().__init__(
#             mount_point = mount_point,
#  
#             gremlin_host = gremlin_host,
#             gremlin_port = gremlin_port,
#             gremlin_username = gremlin_username,
#             gremlin_password = gremlin_password,
#  
#             rabbitmq_host = rabbitmq_host,
#             rabbitmq_port = rabbitmq_port,
#             rabbitmq_username = rabbitmq_username,
#             rabbitmq_password = rabbitmq_password
#         )
#  
#         # self._utils = GremlinFSOperationsUtils()
#         self._utils = GremlinFSUtils()
 
#     def connection(self, ro = False):
# 
#         graph = Graph()
# 
#         if ro:
#             strategy = ReadOnlyStrategy() # .build().create()
#             ro = graph.traversal().withStrategies(strategy).withRemote(DriverRemoteConnection(
#                 self.gremlin_url,
#                 'g',
#                 username = self.gremlin_username,
#                 password = self.gremlin_password
#             ))
#             return ro
# 
#         g = graph.traversal().withRemote(DriverRemoteConnection(
#             self.gremlin_url,
#             'g',
#             username = self.gremlin_username,
#             password = self.gremlin_password
#         ))
# 
#         return g
#  
#     def mqconnection(self):
#  
#         # url = 'amqp://rabbitmq:rabbitmq@rabbitmq:5672/%2f'
#         #        amqp://rabbitmq:rabbitmq@rabbitmq:5672/%2f
#         url = "amqp://%s:%s@%s:%s/%s" % (
#             self.rabbitmq_username,
#             self.rabbitmq_password,
#             self.rabbitmq_host,
#             str(self.rabbitmq_port),
#             '%2f'
#         )
#  
#         params = pika.URLParameters(url)
#         params.socket_timeout = 5
#  
#         connection = pika.BlockingConnection(params) # Connect to CloudAMQP
#  
#         return connection
#  
#     def mqchannel(self):
#  
#         mqconnection = self.mqconnection()
#         mqchannel = mqconnection.channel()
#         mqchannel.queue_declare(
#             queue = 'hello'
#         )
#  
#         return mqchannel
#  
#     def g(self):
# 
#         if self._g:
#             return self._g
# 
#         g = self.connection()
#         self._g = g
# 
#         return self._g
# 
#     def ro(self):
# 
#         if self._ro:
#             return self._ro
# 
#         ro = self.connection(True)
#         self._ro = ro
# 
#         return self._ro
#  
#     def a(self):
#         return __
#  
#     def mq(self):
#  
#         if self._mq:
#             return self._mq
#  
#         mqchannel = self.mqchannel()
#         mqchannel.queue_declare(
#             queue = 'hello'
#         )
#  
#         self._mq = mqchannel
#  
#         return self._mq
#  
#     def mqevent(self, event):
#         pass
#  
#     def mqonevent(self, node, event, chain = [], data = {}, propagate = True):
#         pass
#  
#     def mqonmessage(self, ch, method, properties, body):
#         pass
#  
#     def query(self, query, node = None, _default_ = None):
#         return self.utils().query(query, node, _default_)
#  
#     def eval(self, command, node = None, _default_ = None):
#         return self.utils().eval(command, node, _default_)
#  
#     def config(self, key=None, _default_=None):
#         return self._config.get(key, _default_)
#  
#     def utils(self):
#         return self._utils
#  
#     # def initfs(self):
#     # 
#     #     newfs = GremlinFSVertex.make(
#     #         name = self.config("fs_root"),
#     #         label = ...,
#     #         uuid = None
#     #     ).createFolder(
#     #         parent = None,
#     #         mode = None
#     #     )
#  
#     #     return newfs.get("id")
#  
#     def getfs(self, fsroot, fsinit = False):
#  
#         fsid = fsroot
#  
#         # if not ... and fsinit:
#         #     fs = None
#         #     fsid = self.initfs()
#  
#         return fsid
#  
#     # 
#  
#     # JS jump:
#     # UnsupportedSyntaxError: Having both param accumulator and keyword args is unsupported
#     def enter(self, functioname, *args): # , **kwargs):
#         pass
#  
#     # 
#  
#     def __call__(self, op, *args):
#         if not hasattr(self, op):
#             raise FuseOSError(errno.EFAULT)
#         return getattr(self, op)(*args)
#  
#     def access(self, path, amode):
#         return 0
#  
#     # 
    pass



def main(

    mount_point,

    gremlin_host,
    gremlin_port,
    gremlin_username,
    gremlin_password,

    rabbitmq_host = None,
    rabbitmq_port = None,
    rabbitmq_username = None,
    rabbitmq_password = None,

    **kwargs):

    try:

        operations = GremlinFSOperations()
        

        # operations = GremlinFSOperations(
        operations.configure(
            mount_point = mount_point,

            gremlin_host = gremlin_host,
            gremlin_port = gremlin_port,
            gremlin_username = gremlin_username,
            gremlin_password = gremlin_password,

            rabbitmq_host = rabbitmq_host,
            rabbitmq_port = rabbitmq_port,
            rabbitmq_username = rabbitmq_username,
            rabbitmq_password = rabbitmq_password
        )
        GremlinFS.instance(operations)

        print(" >> G ");
        print(GremlinFS.instance().g())

#         match = GremlinFSPath.match("/")
#         print(" >> MATCH ");
#         print(match);
#         print(match.isFolder())
#         print(match.isFile())
#         print(match.isFound())
#         print(match.parent())
#         print(match.node())
#         if match:
#             if match.isFolder() and match.isFound():
#                 print(" >> FOLDER AND FOUND ");
#             elif match.isFile() and match.isFound():
#                 print(" >> FILE AND FOUND ");
    
        match = GremlinFSPath.match("/test")
        print(" >> MATCH ");
        print(match);
        print(match.isFolder())
        print(match.isFile())
        print(match.isFound())
        print(match.parent())
        print(match.node())
        print(match.node().get("id"))
        # print(match.node().getall())
        print(match.node().all())
        if match:
            if match.isFolder() and match.isFound():
                print(" >> FOLDER AND FOUND ");
            elif match.isFile() and match.isFound():
                print(" >> FILE AND FOUND ");

    except:
        # self.logger.error(' GremlinFS: main/init exception ')
        traceback.print_exc()


def sysarg(
    args,
    index,
    default = None):
    if args and len(args) > 0 and index >= 0 and index < len(args):
        return args[index]
    return default


if __name__ == '__main__':

    mount_point = "/home/project";

    gremlin_host = "localhost";
    gremlin_port = "8182";
    gremlin_username = "root";
    gremlin_password = "root";

    rabbitmq_host = "localhost";
    rabbitmq_port = "5672";
    rabbitmq_username = "rabbitmq";
    rabbitmq_password = "rabbitmq";

    main(

        mount_point = mount_point,

        gremlin_host = gremlin_host,
        gremlin_port = gremlin_port,
        gremlin_username = gremlin_username,
        gremlin_password = gremlin_password,

        rabbitmq_host = rabbitmq_host,
        rabbitmq_port = rabbitmq_port,
        rabbitmq_username = rabbitmq_username,
        rabbitmq_password = rabbitmq_password

    )
