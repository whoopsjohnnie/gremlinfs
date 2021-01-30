
# 
# Copyright (c) 2019, 2020, 2021, John Grundback
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

# 
from gfs.common.log import GFSLogger

from gfs.error.error import GFSError
from gfs.error.error import GFSExistsError
from gfs.error.error import GFSNotExistsError
from gfs.error.error import GFSIsFileError
from gfs.error.error import GFSIsFolderError

# from gfs.api.client.api import GFSAPI
from gfs.api.client.api import GFSCachingAPI



class GremlinFS():

    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a GFSError exception on
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

        # Cannot include at top
        from gfs.lib.util import GremlinFSUtils
        from gfs.lib.config import GremlinFSConfig

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
        # Cannot include at top
        from gfs.lib.util import GremlinFSUtils
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

        # Cannot include at top
        from gfs.model.vertex import GFSVertex

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

            match = GFSVertex.fromVs(
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
