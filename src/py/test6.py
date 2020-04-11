# 
# Copyright (c) 2019, John Grundback
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

# 3.3.4?
# https://pypi.org/project/aiogremlin/
# import asyncio
# from aiogremlin import DriverRemoteConnection, Graph
# from gremlin_python.process.graph_traversal import __

# 
import pika

# 
# 
from gremlinfs.gremlinfslog import GremlinFSLogger

from gremlinfs.gremlinfslib import GremlinFSError
from gremlinfs.gremlinfslib import GremlinFSExistsError
from gremlinfs.gremlinfslib import GremlinFSNotExistsError
from gremlinfs.gremlinfslib import GremlinFSIsFileError
from gremlinfs.gremlinfslib import GremlinFSIsFolderError

from gremlinfs.gremlinfslib import GremlinFSPath
from gremlinfs.gremlinfslib import GremlinFSUtils
from gremlinfs.gremlinfslib import GremlinFS

#
# 
# import config




class GremlinFSOperationsUtils(GremlinFSUtils):

    logger = GremlinFSLogger.getLogger("GremlinFSOperationsUtils") # __name__)

    def tobytes(self, data):
        # return data

        if data:

            # ensure that we have a string
            try:

                data = str(data)

            except:
                pass

            try:

                # convert to byte
                data = data.encode(
                    encoding='utf-8', 
                    errors='strict'
                )

                return data

            except:
                return data

        return data

    def tostring(self, data):
        # return data

        if data:

            try:

                # convert to string
                data = data.decode(
                    encoding='utf-8', 
                    errors='strict'
                    )

                return data

            except:
                return data

        return data

    def decode(self, data, encoding = "base64"):
        import base64
        # return data
        data = base64.b64decode(data)
        return data

    def encode(self, data, encoding = "base64"):
        import base64
        # return data
        data = base64.b64encode(data)
        return data

#     def splitpath(self, path):
#         import os
#         splitpath = (os.path.normpath(os.path.splitdrive(path)[1]))
#         # return splitpath
#         # groupIDs = splitpath(path)
#         groupIDs = splitpath # splitpath(splitpath)
#         if groupIDs == "/":
#             return None
#         else:
#             if not "/" in groupIDs:
#                 return [groupIDs]
#             else:
#                 groupIDs = groupIDs.split('/')
#  
#                 if groupIDs[0] == "" and len(groupIDs) > 1:
#                     return groupIDs[1:]
#                 else:
#                     self.logger.error(' GremlinFS: Error parsing path [{}] '.format(path))
#                     raise ValueError(' GremlinFS: Error parsing path [{}] '.format(path))

    def rematch(self, pattern, data):
        import re
        return re.match(pattern, data)

    def recompile(self, pattern):
        import re
        return re.compile(pattern)

    def genuuid(self, UUID = None):
        import uuid
        if UUID:
            return uuid.UUID(UUID)
        else:
            return uuid.uuid1()

    def gentime(self):
        from time import time
        return time()


class GremlinFSOperations(GremlinFS): # , Operations):

    logger = GremlinFSLogger.getLogger("GremlinFSOperations") # __name__)

    def __init__(
        self,
        **kwargs):

        super().__init__()

        self._utils = GremlinFSOperationsUtils()

    # def __init__(
    def configure(
        self,

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

        super().configure(

            mount_point,

            gremlin_host,
            gremlin_port,
            gremlin_username,
            gremlin_password,

            rabbitmq_host,
            rabbitmq_port,
            rabbitmq_username,
            rabbitmq_password,

        )

        self._utils = GremlinFSOperationsUtils()

        return self

    def connection(self, ro = False):

        graph = Graph()

        if ro:
            strategy = ReadOnlyStrategy() # .build().create()
            ro = graph.traversal().withStrategies(strategy).withRemote(DriverRemoteConnection(
                self.gremlin_url,
                'g',
                username = self.gremlin_username,
                password = self.gremlin_password
            ))
            return ro

        g = graph.traversal().withRemote(DriverRemoteConnection(
            self.gremlin_url,
            'g',
            username = self.gremlin_username,
            password = self.gremlin_password
        ))

        return g

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

    def mqconnection(self):

        # url = 'amqp://rabbitmq:rabbitmq@rabbitmq:5672/%2f'
        #        amqp://rabbitmq:rabbitmq@rabbitmq:5672/%2f
        url = "amqp://%s:%s@%s:%s/%s" % (
            self.rabbitmq_username,
            self.rabbitmq_password,
            self.rabbitmq_host,
            str(self.rabbitmq_port),
            '%2f'
        )

        params = pika.URLParameters(url)
        params.socket_timeout = 5

        connection = pika.BlockingConnection(params) # Connect to CloudAMQP

        return connection

#     def mqchannel(self):
# 
#         mqconnection = self.mqconnection()
#         mqchannel = mqconnection.channel()
#         mqchannel.queue_declare(
#             queue = 'hello'
#         )
# 
#         return mqchannel

    def mqchannel(self):

        mqconnection = self.mqconnection()
        mqchannel = mqconnection.channel()
        mqchannel.queue_declare(
            queue = self.config("mq_queue"),
            durable = True
        )

        return mqchannel

    def g(self):

        if self._g:
            return self._g

        g = self.connection()
        self._g = g

        return self._g

    def ro(self):

        if self._ro:
            return self._ro

        ro = self.connection(True)
        self._ro = ro

        return self._ro

    def a(self):
        return __

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

    def mq(self):

        if self._mq:
            return self._mq

        mqchannel = self.mqchannel()
        mqchannel.queue_declare(
            queue = self.config("mq_queue"),
            durable = True
        )

        self._mq = mqchannel

        return self._mq

#     def mqevent(self, event):
#         pass

    def mqevent(self, event, **kwargs):

#         try:

        # import json
        import simplejson as json

#         data = {
#             "path": kwargs.get("path", {}).get("full", None),
#             "node": {},
#             "parent": {},
#             "event": event,
#             "property": kwargs.get("property", None),
#             "value": kwargs.get("value", None),
#             "mode": kwargs.get("mode", None),
#             "offset": kwargs.get("offset", None),
#             "encoding": kwargs.get("encoding", None),
#             "ine": kwargs.get("ine", None)
#         }
# 
#         node = kwargs.get("node", None)
#         if node:
#             data["node"]["id"] = node.get("id", None)
#             data["node"]["uuid"] = node.get("uuid", None)
#             data["node"]["name"] = node.get("name", None)
#             data["node"]["label"] = node.get("label", None)
# 
#         parent = kwargs.get("parent", None)
#         if parent:
#             data["parent"]["id"] = parent.get("id", None)
#             data["parent"]["uuid"] = parent.get("uuid", None)
#             data["parent"]["name"] = parent.get("name", None)
#             data["parent"]["label"] = parent.get("label", None)

#         data = {
#             "event": event.get("event"),
#         }
# 
#         if event.has("node"):
#             data["node"] = event.get("node").all()

        data = event.toJSON()

        logging.info(' GremlinFS: OUTBOUND AMQP/RABBIT EVENT ')
        logging.info(data)

        try:

            self.mq().basic_publish(
                exchange = self.config("mq_exchange"),
                routing_key = self.config("fs_ns"),
                body = json.dumps(
                    data, 
                    indent=4, 
                    sort_keys=False
                )
            )

        except pika.exceptions.ConnectionClosedByBroker:

            logging.info(' GremlinFS: Outbound AMQP/RABBIT event, connection was closed, retry ')

            self._mq = None

            self.mq().basic_publish(
                exchange = self.config("mq_exchange"),
                routing_key = self.config("fs_ns"),
                body = json.dumps(
                    data, 
                    indent=4, 
                    sort_keys=False
                )
            )

        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            logging.error(' GremlinFS: Outbound AMQP/RABBIT event error: {} '.format(err))
            return

        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:

            logging.info(' GremlinFS: Outbound AMQP/RABBIT event, connection was closed, retry ')

            self._mq = None

            self.mq().basic_publish(
                exchange = self.config("mq_exchange"),
                routing_key = self.config("fs_ns"),
                body = json.dumps(
                    data, 
                    indent=4, 
                    sort_keys=False
                )
            )

#         except:
#             logging.error(' GremlinFS: MQ/AMQP send exception ')
#             traceback.print_exc()

    def mqonevent(self, node, event, chain = [], data = {}, propagate = True):
        pass

    def mqonmessage(self, ch, method, properties, body):
        pass

    def query(self, query, node = None, _default_ = None):
        return self.utils().query(query, node, _default_)

    def eval(self, command, node = None, _default_ = None):
        return self.utils().eval(command, node, _default_)

    def config(self, key=None, _default_=None):
        return self._config.get(key, _default_)

    def utils(self):
        return self._utils

    # def initfs(self):
    # 
    #     newfs = GremlinFSVertex.make(
    #         name = self.config("fs_root"),
    #         label = ...,
    #         uuid = None
    #     ).createFolder(
    #         parent = None,
    #         mode = None
    #     )

    #     return newfs.get("id")

    def getfs(self, fsroot, fsinit = False):

        fsid = fsroot

        # if not ... and fsinit:
        #     fs = None
        #     fsid = self.initfs()

        return fsid

    # 

    # JS jump:
    # UnsupportedSyntaxError: Having both param accumulator and keyword args is unsupported
    def enter(self, functioname, *args): # , **kwargs):
        pass

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
# 
#     def chmod(self, path, mode):
#         self.notReadOnly()
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("chmod", path, mode)
#             if match:
#                 if match.isFound():
#                     match.setProperty(
#                         "mode", 
#                         mode
#                     )
# 
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: chmod exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         return 0
# 
#     def chown(self, path, uid, gid):
#         self.notReadOnly()
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("chown", path, uid, gid)
#             if match:
#                 if match.isFound():
#                     match.setProperty(
#                         "owner", 
#                         uid
#                     )
#                     match.setProperty(
#                         "group", 
#                         gid
#                     )
# 
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: chown exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         return 0
# 
#     def create(self, path, mode, fi=None):
#         self.notReadOnly()
# 
#         created = False
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("create", path, mode)
#             if match:
#                 if not match.isFound():
#                     created = match.createFile(mode)
# 
#                 else:
#                     # TODO: Wrong exception
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: create exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if created:
#             return 0
# 
#         return 0
# 
#     # def destroy(self, path):
#     #     pass
# 
#     def flush(self, path, fh):
#         return 0
# 
#     def fsync(self, path, datasync, fh):
#         return 0
# 
#     # def fsyncdir(self, path, datasync, fh):
#     #     return 0
# 
#     def getattr(self, path, fh=None):
# 
#         now = time()
#         owner = self.config("default_uid", 0) # 1001 # 0
#         group = self.config("default_gid", 0) # 1001 # 0
#         mode = self.config("default_mode", 0o777) # 0o777
#         # data = None
# 
#         attrs = {
#             "st_ino": 0,
#             "st_mode": 0,
#             "st_nlink": 0,
#             "st_uid": owner,
#             "st_gid": group,
#             "st_size": 0,
#             "st_atime": now,
#             "st_mtime": now,
#             "st_ctime": now,
#         }
# 
#         try:
# 
#             # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             # loop.close()
#             match = GremlinFSPath.match(path)
#             match.enter("getattr", path)
#             if match:
#                 if match.isFolder() and match.isFound():
#                     attrs.update({
#                         "st_mode": (stat.S_IFDIR | int( match.getProperty("mode", 0o777) ) ),
#                         "st_nlink": int( match.getProperty("links", 1) ),
#                         "st_uid": int( match.getProperty("owner", owner) ),
#                         "st_gid": int( match.getProperty("group", group) ),
#                         "st_size": 1024,
#                         # "st_atime": match.getProperty("", now),
#                         # "st_mtime": match.getProperty("modified", now),
#                         # "st_ctime": match.getProperty("created", now)
#                     })
# 
#                 elif match.isFile() and match.isFound():
#                     attrs.update({
#                         "st_mode": (stat.S_IFREG | int( match.getProperty("mode", 0o777) ) ),
#                         "st_nlink": int( match.getProperty("links", 1) ),
#                         "st_uid": int( match.getProperty("owner", owner) ),
#                         "st_gid": int( match.getProperty("group", group) ),
#                         "st_size": match.readFileLength(),
#                         # "st_atime": match.getProperty("", now),
#                         # "st_mtime": match.getProperty("modified", now),
#                         # "st_ctime": match.getProperty("created", now)
#                     })
# 
#                 elif match.isLink() and match.isFound():
#                     attrs.update({
#                         "st_mode": (stat.S_IFLNK | int( match.getProperty("mode", 0o777) ) ),
#                         "st_nlink": int( match.getProperty("links", 1) ),
#                         "st_uid": int( match.getProperty("owner", owner) ),
#                         "st_gid": int( match.getProperty("group", group) ),
#                         "st_size": 0,
#                         # "st_atime": match.getProperty("", now),
#                         # "st_mtime": match.getProperty("modified", now),
#                         # "st_ctime": match.getProperty("created", now)
#                     })
# 
#                 else:
#                     # Note; Unless I throw this here, I am unable to
#                     # touch files as attributes. I think the default
#                     # here should be to throw FuseOSError(errno.ENOENT)
#                     # unless file/node is actually found
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             logging.error(' GremlinFS: getattr exception ')
#             logging.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         return attrs
# 
#     # def getxattr(self, path, name, position=0):
#     #     raise FuseOSError(ENOTSUP)
# 
#     # def init(self, path):
#     #     pass
# 
#     # def ioctl(self, path, cmd, arg, fip, flags, data):
#     #     raise FuseOSError(errno.ENOTTY)
# 
#     def link(self, target, source):
#         self.notReadOnly()
# 
#         created = False
# 
#         try:
# 
#             # # targetmatch = await( GremlinFSPath.match(target) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # targetmatch = loop.run_until_complete(GremlinFSPath.match(target))
#             targetmatch = GremlinFSPath.match(target)
# 
#             # # sourcematch = await( GremlinFSPath.match(source) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # sourcematch = loop.run_until_complete(GremlinFSPath.match(source))
#             sourcematch = GremlinFSPath.match(source)
# 
#             targetmatch.enter("link", target, source)
#             if targetmatch and sourcematch:
#                 if not targetmatch.isFound():
#                     created = targetmatch.createLink(sourcematch)
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: link exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if created:
#             return 0
# 
#         return 0
# 
#     # def listxattr(self, path):
#     #     return []
# 
#     # 
# 
#     def mkdir(self, path, mode):
#         self.notReadOnly()
# 
#         created = False
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             # match.enter("mkdir", path, mode)
#             match = GremlinFSPath.match(path)
#             if match:
#                 if not match.isFound():
#                     created = match.createFolder(mode)
# 
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: mkdir exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if created:
#             return 0
# 
#         return 0
# 
#     def mknod(self, path, mode, dev):
#         self.notReadOnly()
#         raise FuseOSError(errno.ENOENT)
# 
#     def open(self, path, flags):
# 
#         found = False
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("open", path, flags)
#             if match:
#                 if match.isFile() and match.isFound():
#                     found = True
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: open exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if found:
#             return 0
# 
#         return 0
# 
#     # def opendir(self, path):
#     #     return 0
# 
#     def read(self, path, size, offset, fh):
# 
#         data = None
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("read", path, size, offset)
#             if match:
#                 if match.isFile() and match.isFound():
#                     data = match.readFile(size, offset)
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: read exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if data:
#             return data
# 
#         return None
# 
#     def readdir(self, path, fh):
# 
#         entries = [
#             '.',
#             '..'
#         ]
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("readdir", path)
#             if match:
#                 if match.isFolder() and match.isFound():
#                     # entries.extend(
#                     #     match.readFolder()
#                     # )
#                     # loop = asyncio.new_event_loop()
#                     # asyncio.set_event_loop(loop)
#                     # entries.extend(
#                     #     loop.run_until_complete(match.readFolder())
#                     # )
#                     entries.extend(
#                         match.readFolder()
#                     )
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             # logging.error(' GremlinFS: readdir exception ')
#             # logging.error(sys.exc_info()[0])
#             # traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         return entries
# 
#     def readlink(self, path):
# 
#         newpath = None
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("readlink", path)
#             if match:
#                 if match.isLink() and match.isFound():
#                     newpath = match.readLink()
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: readlink exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if newpath:
#             return newpath
# 
#         return None
# 
#     def release(self, path, fh):
#         return 0
# 
#     # def releasedir(self, path, fh):
#     #     return 0
# 
#     # def removexattr(self, path, name):
#     #     raise FuseOSError(ENOTSUP)
# 
#     def rename(self, old, new):
#         self.notReadOnly()
# 
#         renamed = False
# 
#         try:
# 
#             oldmatch = GremlinFSPath.match(old)
#             newmatch = GremlinFSPath.match(new)
#             oldmatch.enter("rename", old, new)
#             if oldmatch and newmatch:
# 
#                 # if oldmatch.isFile() and \
#                 #    oldmatch.isFound() and \
#                 #    not newmatch.isFound():
#                 #    renamed = oldmatch.renameFile(newmatch)
#                 # elif oldmatch.isFolder() and \
#                 #    oldmatch.isFound() and \
#                 #    not newmatch.isFound():
#                 #    renamed = oldmatch.renameFolder(newmatch)
# 
#                 if oldmatch.isFile() and \
#                    oldmatch.isFound():
#                     if newmatch.isFound():
#                         newmatch.deleteFile()
#                     renamed = oldmatch.renameFile(newmatch)
#                 elif oldmatch.isFolder() and \
#                    oldmatch.isFound():
#                     if newmatch.isFound():
#                         newmatch.deleteFolder()
#                     renamed = oldmatch.renameFolder(newmatch)
# 
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         # except GremlinFSNotExistsError:
#         #     # Don't log here
#         #     raise FuseOSError(errno.ENOENT)
# 
#         # except FuseOSError:
#         #     # Don't log here
#         #     raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: rename exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if renamed:
#             return 0
# 
#         return 0
# 
#     def rmdir(self, path):
#         self.notReadOnly()
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("rmdir", path)
#             if match:
#                 if match.isFolder() and match.isFound():
#                     match.deleteFolder()
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: rmdir exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         return 0
# 
#     # def setxattr(self, path, name, value, options, position=0):
#     #     raise FuseOSError(ENOTSUP)
# 
#     def statfs(self, path):
#         return {}
# 
#     def symlink(self, target, source):
#         self.notReadOnly()
# 
#         created = False
# 
#         try:
# 
#             # # targetmatch = await( GremlinFSPath.match(target) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # targetmatch = loop.run_until_complete(GremlinFSPath.match(target))
#             targetmatch = GremlinFSPath.match(target)
# 
#             # # sourcematch = await( GremlinFSPath.match(source) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # sourcematch = loop.run_until_complete(GremlinFSPath.match(source))
#             sourcematch = GremlinFSPath.match(source)
# 
#             targetmatch.enter("symlink", target, source)
#             if targetmatch and sourcematch:
#                 if not targetmatch.isFound():
#                     created = targetmatch.createLink(sourcematch)
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: symlink exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if created:
#             return 0
# 
#         return 0
# 
#     def truncate(self, path, length, fh=None):
#         self.notReadOnly()
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("truncate", path)
#             if match:
#                 if match.isFile() and match.isFound():
#                     match.clearFile()
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: truncate exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         # raise FuseOSError(errno.ENOENT)
#         return 0
# 
#     def unlink(self, path):
#         self.notReadOnly()
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("unlink", path)
#             if match:
#                 if match.isFile() and match.isFound():
#                     match.deleteFile()
#                 elif match.isLink() and match.isFound():
#                     match.deleteLink()
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: unlink exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         return 0
# 
#     def utimens(self, path, times=None):
#         self.enter("utimens", path)
#         return 0
# 
#     def write(self, path, data, offset, fh):
#         self.notReadOnly()
# 
#         if not data:
#             data = ""
# 
#         try:
# 
#             # # match = await( GremlinFSPath.match(path) )
#             # loop = asyncio.new_event_loop()
#             # asyncio.set_event_loop(loop)
#             # match = loop.run_until_complete(GremlinFSPath.match(path))
#             match = GremlinFSPath.match(path)
#             match.enter("write", path, data, offset)
#             if match:
#                 if match.isFile() and match.isFound():
#                     data = match.writeFile(data, offset)
#                 else:
#                     raise FuseOSError(errno.ENOENT)
# 
#         except GremlinFSNotExistsError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except FuseOSError:
#             # Don't log here
#             raise FuseOSError(errno.ENOENT)
# 
#         except:
#             self.logger.error(' GremlinFS: write exception ')
#             self.logger.error(sys.exc_info()[0])
#             traceback.print_exc()
#             raise FuseOSError(errno.ENOENT)
# 
#         if data:
#             return len(data)
# 
#         # raise FuseOSError(errno.ENOENT)
#         return 0
# 
#     # 
# 
#     def isReadOnly(self):
#         return False
# 
#     def notReadOnly(self):
#         if self.isReadOnly():
#             raise FuseOSError(errno.EROFS)
#         return True



# class GremlinFS(object):
# 
#     __operations = None
# 
#     @staticmethod
#     def operations():
# 
#         if GremlinFS.__operations == None:
#             GremlinFS.__operations = GremlinFSOperations()
# 
#         return GremlinFS.__operations 

# 
# https://softwareengineering.stackexchange.com/questions/191623/best-practices-for-execution-of-untrusted-code
# https://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
# http://lucumr.pocoo.org/2011/2/1/exec-in-python/
# 


class PyExec():

    @classmethod
    def instance(clazz, environment={}, whitelist={}, blacklist=[], allowed=[], notallowed=[], defaults=None):
        instance = clazz(environment=environment, whitelist=whitelist, blacklist=blacklist, allowed=allowed, notallowed=notallowed, defaults=defaults)
        return instance

    def __init__(self, environment={}, whitelist={}, blacklist=[], allowed=[], notallowed=[], defaults=None):
        self.logger = logging.getLogger("PyExec")
        if not defaults:
            defaults = self.defaults()
        allalloweds = self.allowed()
        if allowed:
            allalloweds.extend(allowed)
        self.alloweds = []
        for allowed in allalloweds:
            self.alloweds.append(re.compile(allowed))
        allnotalloweds = self.notallowed()
        if notallowed:
            allnotalloweds.extend(notallowed)
        self.notalloweds = []
        for notallowed in allnotalloweds:
            self.notalloweds.append(re.compile(notallowed))
        definitions = self.definitions(whitelist, blacklist, defaults)
        self.globalenv = self.globals(environment, definitions)
        self.localenv = self.locals(environment, definitions)

    def defaults(self):
        return {
            "True": True,
            "False": False,
            "eval": eval,
            "len": len
        }

    def allowed(self):
        return []

    def notallowed(self):
        # Prevent using os, system and introspective __ objects
        return [
            '[\"\']+os[\"\']+',
            '(os)?\.system',
            '__[a-zA-Z]+__'
        ]

    def environment(self):
        return self.localenv

    def definitions(self, whitelist={}, blacklist=[], defaults=None):
        definitions = {}
        if defaults:
            definitions = dict(definitions, **defaults)
        if whitelist:
            definitions = dict(definitions, **whitelist)
        if blacklist:
            for key in blacklist:
                if key in definitions:
                    del definitions[key]
        return definitions

    def globals(self, environment={}, definitions={}):
        # Disable builtin functions, 
        # place needed and safe builtins into defaults or whitelist
        return {
            "__builtins__": {}
        }

    def locals(self, environment={}, definitions={}):
        locals = {}
        if environment:
            locals = dict(locals, **environment)
        if definitions:
            locals = dict(locals, **definitions)
        return locals

    # # https://stackoverflow.com/questions/3906232/python-get-the-print-output-in-an-exec-statement
    # @contextlib.contextmanager
    # def stdoutIO(stdout=None):
    #     old = sys.stdout
    #     if stdout is None:
    #         stdout = StringIO.StringIO()
    #     sys.stdout = stdout
    #     yield stdout
    #     sys.stdout = old

    def pyeval(self, command):
        ret = None
        # with stdoutIO() as s:
        # from cStringIO import StringIO
        try:
            # from StringIO import StringIO ## for Python 2
            from cStringIO import StringIO
        except ImportError:
            from io import StringIO ## for Python 3
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        redirected_output = sys.stdout = StringIO()
        redirected_error = sys.stderr = StringIO()
        if not command:
            # print "Empty line"
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return ret, redirected_output.getvalue(), redirected_error.getvalue()
        if self.notalloweds:
            for notallowed in self.notalloweds:
                if notallowed.search(command):
                    # print "Illegal line"
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
                    return ret, redirected_output.getvalue(), redirected_error.getvalue()
        if self.alloweds:
            ok = False
            for allowed in self.alloweds:
                if allowed.search(command):
                    ok = True
            if not ok:
                # print "Illegal line"
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                return ret, redirected_output.getvalue(), redirected_error.getvalue()
        try:
            ret = eval(
                command,
                self.globalenv,
                self.localenv
            )
        except:
            # print "Exception"
            traceback.print_exc()
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return ret, redirected_output.getvalue(), redirected_error.getvalue()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        return ret, redirected_output.getvalue(), redirected_error.getvalue()

    def pyexec(self, command):
        # with stdoutIO() as s:
        # from cStringIO import StringIO
        try:
            # from StringIO import StringIO ## for Python 2
            from cStringIO import StringIO
        except ImportError:
            from io import StringIO ## for Python 3
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        redirected_output = sys.stdout = StringIO()
        redirected_error = sys.stderr = StringIO()
        if not command:
            # print "Empty line"
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return None, redirected_output.getvalue(), redirected_error.getvalue()
        if self.notalloweds:
            for notallowed in self.notalloweds:
                if notallowed.search(command):
                    # print "Illegal line"
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
                    return None, redirected_output.getvalue(), redirected_error.getvalue()
        if self.alloweds:
            ok = False
            for allowed in self.alloweds:
                if allowed.search(command):
                    ok = True
            if not ok:
                # print "Illegal line"
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                return None, redirected_output.getvalue(), redirected_error.getvalue()
        try:
            exec(
                command,
                self.globalenv,
                self.localenv
            )
        except:
            # print "Exception"
            traceback.print_exc()
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            return None, redirected_output.getvalue(), redirected_error.getvalue()
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        return None, redirected_output.getvalue(), redirected_error.getvalue()

    def pyrun(self, command, execfn="eval"):
        if execfn == "eval":
            return self.pyeval(command)
        elif execfn == "exec":
            self.pyexec(command)


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

        print(" >> G ")
        print(GremlinFS.instance().g())

#         match = GremlinFSPath.match("/")
#         print(" >> MATCH ")
#         print(match)
#         print(match.isFolder())
#         print(match.isFile())
#         print(match.isFound())
#         print(match.parent())
#         print(match.node())
#         if match:
#             if match.isFolder() and match.isFound():
#                 print(" >> FOLDER AND FOUND ")
#             elif match.isFile() and match.isFound():
#                 print(" >> FILE AND FOUND ")
    
#         match = GremlinFSPath.match("/test9")
#         print(" >> MATCH ")
#         print(match)
#         print(match.isFolder())
#         print(match.isFile())
#         print(match.isFound())
#         print(match.parent())
#         print(match.node())
#         # print(match.node().get("id"))
#         # print(match.node().getall())
#         # print(match.node().all())
#         if match:
#             if match.isFolder() and match.isFound():
#                 print(" >> FOLDER AND FOUND ")
#             elif match.isFile() and match.isFound():
#                 print(" >> FILE AND FOUND ")
# 
#             else:
#                 match.createFile()

        # match1 = GremlinFSPath.match("/test11")
        # match1.createFile()

        match2 = GremlinFSPath.match("/test11")
        match2.deleteFile()
        

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

    mount_point = "/home/project"

    gremlin_host = "localhost"
    gremlin_port = "8182"
    gremlin_username = "root"
    gremlin_password = "root"

    rabbitmq_host = "localhost"
    rabbitmq_port = "5672"
    rabbitmq_username = "rabbitmq"
    rabbitmq_password = "rabbitmq"

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
