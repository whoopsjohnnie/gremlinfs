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
from gfs.common.base import GremlinFSBase

from gfs.error.error import GremlinFSError
from gfs.error.error import GremlinFSExistsError
from gfs.error.error import GremlinFSNotExistsError
from gfs.error.error import GremlinFSIsFileError
from gfs.error.error import GremlinFSIsFolderError

from gfs.model.vertex import GremlinFSVertex
from gfs.model.edge import GremlinFSEdge

# from gfs.api.common.api import GFSAPI
from gfs.api.common.api import GFSCachingAPI

# 
# 
# import config



class GremlinFSUtils(GremlinFSBase):

    logger = GFSLogger.getLogger("GremlinFSUtils")

    @classmethod
    def value(clazz, value, default = None):

        # if value and "@value" in value:
        if value and type(value) == dict and "@value" in value:
            return value["@value"]

        elif type(value) in (tuple, list):
            return value[0]

        # Check truthy, int value 0 is False, but defined
        # hence can't check simple if
        # elif value:
        elif value is not None:
            return value

        else:
            return default

    # TODO, remove?
    # clean up config and let be dict that we pass in
    @classmethod
    def conf(clazz, key, default = None):
        # 
        return default

    @classmethod
    def missing(clazz, value):
        if value:
            raise GremlinFSExistsError()

    @classmethod
    def found(clazz, value):
        if not value:
            raise GremlinFSNotExistsError()
        return value

    @classmethod
    def irepl(clazz, old, data, index = 0):

        offset = index

        if not old:
            if data and index == 0:
                return data
            return None

        if not data:
            return old

        if index < 0:
            return old

        if offset > len(old):
            return old

        new = ""

        prefix = ""
        lprefix = 0

        infix = data
        linfix = len(data)

        suffix = None
        lsuffix = 0

        # if not old and index

        if offset > 0 and offset <= len(old):

            prefix = old[:offset]
            lprefix = len(prefix)

        if len(old) > lprefix + linfix:

            suffix = old[lprefix + linfix:]
            lsuffix = len(old)

        if lprefix > 0 and linfix > 0 and lsuffix > 0:
            new = prefix + infix + suffix
        elif lprefix > 0 and linfix > 0:
            new = prefix + infix
        elif linfix > 0 and lsuffix > 0:
            new = infix + suffix
        else:
            new = infix

        return new

    @classmethod
    def link(clazz, path):
        pass

    @classmethod
    def utils(clazz):
        return GremlinFSUtils()

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    # 

    def api(self):
        return GremlinFS.operations().api()

    def query(self, query, node = None, default = None):
        pass

    def eval(self, command, node = None, default = None):
        pass

    def config(self, key = None, default = None):
        return GremlinFS.operations().config(key, default)

    # def utils(self):
    #     return GremlinFS.operations().utils()

    # 

    def nodelink(self, node, path = None):

#         newpath = None
# 
#         if node and path:
#             newpath = self.linkpath("%s/.V/%s" % (
#                 path,
#                 node.toid()
#             ))
#         elif node:
#             newpath = self.linkpath("/.V/%s" % (
#                 node.toid()
#             ))

        nodepath = ""

        if node:
            path = node.path()
            if path:
                for node in path:
                    nodename = node.get("name", None)
                    nodepath += "/" + nodename

        return self.linkpath("%s" % (nodepath))

    def linkpath(self, path):

        # if not path:
        #     return None

        # return "%s%s" % (self.config("mount_point"), path)
        pass

    # 

    def tobytes(self, data):

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

    # Rely on encoding prefix
    # def decode(self, data, encoding = "base64"):
    def decode(self, data):
        try:
            if data and data.startswith("base64:"):
                import base64
                data = self.utils().tostring(
                    base64.b64decode(
                        self.utils().tobytes(data[7:])
                    )
                )
        except:
            return data
        # else:
        #     data = self.utils().tostring(
        #         base64.b64decode(
        #             self.utils().tobytes(data)
        #         )
        #     )
        return data

    def encode(self, data, encoding = "base64"):
        import base64
        data = "%s:%s" % ( encoding, self.utils().tostring(
            base64.b64encode(
                self.utils().tobytes(data)
            )
        ))
        return data

    # def render(self, template, templatectx):
    #     pass

    def splitpath(self, path):
        if path == "/":
            return None
        elif not "/" in path:
            return [path]
        elems = path.split("/")
        if elems[0] == "" and len(elems) > 1:
            return elems[1:]
        return elems

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



class GremlinFSEvent(GremlinFSBase):

    logger = GFSLogger.getLogger("GremlinFSEvent")

    def __init__(self, **kwargs):
        self.setall(kwargs)

    def toJSON(self):
        pass



class GremlinFSConfig(GremlinFSBase):

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



class GremlinFS():

    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a GremlinFSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    '''

    logger = GFSLogger.getLogger("GremlinFS")

    __instance = None

    @classmethod
    def instance(clazz, instance = None):
        if instance:
            GremlinFS.__instance = instance
        return GremlinFS.__instance

    @classmethod
    def operations(clazz):
        return GremlinFS.__instance

    def __init__(
        self,
        **kwargs):



        self._config = None

    # def __init__(
    def configure(
        self,

        # mount_point,

        gfs_host,
        gfs_port,
        gfs_username,
        gfs_password,

        **kwargs):

        # self.mount_point = mount_point
        # 
        # self.logger.debug(' GremlinFS mount point: ' + self.mount_point)

        self.gfs_host = gfs_host
        self.gfs_port = gfs_port
        self.gfs_username = gfs_username
        self.gfs_password = gfs_password

        self.gfs_url = "http://" + self.gfs_host + ":" + self.gfs_port

        self.logger.debug(' GremlinFS gfs host: ' + self.gfs_host)
        self.logger.debug(' GremlinFS gfs port: ' + self.gfs_port)
        # self.logger.debug(' GremlinFS gfs username: ' + self.gfs_username)
        # self.logger.debug(' GremlinFS gfs password: ' + self.gfs_password)
        self.logger.debug(' GremlinFS gfs URL: ' + self.gfs_url)

        self._config = GremlinFSConfig(

            # mount_point = mount_point,

            gfs_host = gfs_host,
            gfs_port = gfs_port,
            gfs_username = gfs_username,
            gfs_password = gfs_password,

        )

        # self._api = GFSAPI(
        self._api = GFSCachingAPI(
            gfs_host = gfs_host,
            gfs_port = gfs_port,
            gfs_username = gfs_username,
            gfs_password = gfs_password,
        )

        self._utils = GremlinFSUtils()

        # register
        self.register()

        return self

    # 

    def api(self):
        return self._api

    def query(self, query, node = None, default = None):
        return self.utils().query(query, node, default)

    def eval(self, command, node = None, default = None):
        return self.utils().eval(command, node, default)

    def config(self, key = None, default = None):
        return self._config.get(key, default)

    def utils(self):
        return GremlinFSUtils.utils()

    def getfs(self, fsroot, fsinit = False):

        fsid = fsroot

        # if not ... and fsinit:
        #     fs = None
        #     fsid = self.initfs()

        return fsid

    def register(self):

        import socket
        import platform

        client_id = self.config("client_id")
        namespace = self.config("fs_ns")
        type_name = "register"

        hostname = socket.gethostname()
        ipaddr = socket.gethostbyname(hostname)

        # hwaddr = hex(uuid.getnode())
        hwaddr = ':'.join(['{:02x}'.format((uuid.getnode() >> ele) & 0xff) for ele in range(0,8*6,8)][::-1]).upper()

        exists = False
        node = None

        try:

            match = GremlinFSVertex.fromVs(
                self.api().vertices(
                    type_name, {
                        'namespace': namespace,
                        'name': client_id + "@" + hostname,
                        'hw_address': hwaddr
                    }
                )
            )

            if match:
                exists = True
                node = match[0]

            else:
                exists = False
                node = None

        except Exception as e:
            self.logger.exception(' GremlinFS: Failed to register ', e)
            exists = False

        try:

            if not exists:

                pathuuid = uuid.uuid1()
                pathtime = time()

                self.api().createVertex(
                    type_name, {
                        'name': client_id + "@" + hostname,
                        'uuid': str(pathuuid),
                        'namespace': namespace,
                        'created': int(pathtime),
                        'modified': int(pathtime),
                        'client_id': client_id,
                        'hostname': hostname,
                        'ip_address': ipaddr,
                        'hw_address': hwaddr,
                        'machine_architecture': platform.processor(),
                        'machine_hardware': platform.machine(),
                        'system_name': platform.system(),
                        'system_release': platform.release(),
                        'system_version': platform.version()
                    }
                )

        except Exception as e:
            self.logger.exception(' GremlinFS: Failed to register ', e)

        return node

    def defaultLabel(self):
        return "vertex"

    def defaultFolderLabel(self):
        return self.config("folder_label")

    def isFileLabel(self, label):
        if self.isFolderLabel(label):
            return False
        return True

    def isFolderLabel(self, label):
        if label == self.defaultFolderLabel():
            return True
        return False
