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
import addict
from addict import Dict

# 
from time import time

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# 
from fuse import FUSE
from fuse import Operations
from fuse import FuseOSError

# 
from gfs.common.log import GFSLogger

from gfs.model.vertex import GFSVertex
from gfs.model.edge import GFSEdge

from gfs.lib.path import GremlinFSPath
from gfs.lib.util import GremlinFSUtils
from gfs.lib.event import GremlinFSEvent
from gfs.lib.config import GremlinFSConfig

# from gfs.api.client.api import GFSAPI
from gfs.api.client.api import GFSCachingAPI



class GremlinFSOperations(Operations):

    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a FuseOSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    '''

    logger = GFSLogger.getLogger("GremlinFSOperations")

    def __init__(
        self,
        **kwargs):

        self._gfs = None

        # self._config = None

    # def __init__(
    def configure(
        self,

        mount_point,

        # gfs_host,
        # gfs_port,
        # gfs_username,
        # gfs_password,
        gfs,

        **kwargs):

        self.mount_point = mount_point

        self.logger.debug(' GremlinFS mount point: ' + self.mount_point)

        # self.gfs_host = gfs_host
        # self.gfs_port = gfs_port
        # self.gfs_username = gfs_username
        # self.gfs_password = gfs_password

        # self.gfs_url = "http://" + self.gfs_host + ":" + self.gfs_port

        # self.logger.debug(' GremlinFS gfs host: ' + self.gfs_host)
        # self.logger.debug(' GremlinFS gfs port: ' + self.gfs_port)
        # self.logger.debug(' GremlinFS gfs username: ' + self.gfs_username)
        # # self.logger.debug(' GremlinFS gfs password: ' + self.gfs_password)
        # self.logger.debug(' GremlinFS gfs URL: ' + self.gfs_url)

        self._gfs = gfs

        # self._config = GremlinFSConfig(

        #     mount_point = mount_point,

        #     gfs_host = gfs_host,
        #     gfs_port = gfs_port,
        #     gfs_username = gfs_username,
        #     gfs_password = gfs_password,

        # )

        # self._api = GFSAPI(
        #     gfs_host = gfs_host,
        #     gfs_port = gfs_port,
        #     gfs_username = gfs_username,
        #     gfs_password = gfs_password,
        # )

        # self._utils = GremlinFSUtils()

        # # register
        # self.register()

        return self

    # 

    def api(self):
        return self._gfs.api()

    def query(self, query, node = None, _default_ = None):
        return self._gfs.utils().query(query, node, _default_)

    def eval(self, command, node = None, _default_ = None):
        return self._gfs.utils().eval(command, node, _default_)

    def config(self, key=None, _default_=None):
        return self._gfs.config().get(key, _default_)

    def utils(self):
        return GremlinFSUtils.utils()

    # 

    def enter(self, functioname, *args, **kwargs):
        self.logger.debug(' GremlinFSPath: ENTER: %s ' % (functioname))
        self.logger.debug(args)
        self.logger.debug(kwargs)

    # 

    def __call__(self, op, *args):
        if not hasattr(self, op):
            raise FuseOSError(errno.EFAULT)
        return getattr(self, op)(*args)

    def access(self, path, amode):
        return 0

    # 

    def chmod(self, path, mode):
        self.notReadOnly()
        try:

            match = GremlinFSPath.match(path)
            match.enter("chmod", path, mode)
            if match:
                if match.isFound():
                    match.setProperty(
                        "mode", 
                        mode
                    )

                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: chmod exception ', e)
            raise FuseOSError(errno.ENOENT)

        return 0

    def chown(self, path, uid, gid):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("chown", path, uid, gid)
            if match:
                if match.isFound():
                    match.setProperty(
                        "owner", 
                        uid
                    )
                    match.setProperty(
                        "group", 
                        gid
                    )

                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: chown exception ', e)
            raise FuseOSError(errno.ENOENT)

        return 0

    def create(self, path, mode, fi=None):
        self.notReadOnly()

        created = False

        try:

            match = GremlinFSPath.match(path)
            match.enter("create", path, mode)
            if match:
                if not match.isFound():
                    created = match.createFile() # mode)

                else:
                    # TODO: Wrong exception
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: create exception ', e)
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

    # def destroy(self, path):
    #     pass

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    # def fsyncdir(self, path, datasync, fh):
    #     return 0

    def getattr(self, path, fh=None):

        now = time()
        owner = GremlinFSUtils.conf("default_uid", 0) # 1001 # 0
        group = GremlinFSUtils.conf("default_gid", 0) # 1001 # 0
        mode = GremlinFSUtils.conf("default_mode", 0o777) # 0o777
        # data = None

        attrs = {
            "st_ino": 0,
            "st_mode": 0,
            "st_nlink": 0,
            "st_uid": owner,
            "st_gid": group,
            "st_size": 0,
            "st_atime": now,
            "st_mtime": now,
            "st_ctime": now,
        }

        try:

            match = GremlinFSPath.match(path)
            match.enter("getattr", path)
            if match:
                if match.isFolder() and match.isFound():
                    attrs.update({
                        "st_mode": (stat.S_IFDIR | int( match.getProperty("mode", 0o777) ) ),
                        "st_nlink": int( match.getProperty("links", 1) ),
                        "st_uid": int( match.getProperty("owner", owner) ),
                        "st_gid": int( match.getProperty("group", group) ),
                        "st_size": 1024,
                        # "st_atime": match.getProperty("", now),
                        # "st_mtime": match.getProperty("modified", now),
                        # "st_ctime": match.getProperty("created", now)
                    })

                elif match.isFile() and match.isFound():
                    match_file_length = 0
                    try:
                        # This one can throw errors via API as render returns 
                        # 404 for empty file
                        match_file_length = match.readFileLength()
                    except Exception as e:
                        # Don't log here and don't throw exception, just set 
                        # file length to 0
                        pass
                    attrs.update({
                        "st_mode": (stat.S_IFREG | int( match.getProperty("mode", 0o777) ) ),
                        "st_nlink": int( match.getProperty("links", 1) ),
                        "st_uid": int( match.getProperty("owner", owner) ),
                        "st_gid": int( match.getProperty("group", group) ),
                        "st_size": match_file_length, # match.readFileLength(),
                        # "st_atime": match.getProperty("", now),
                        # "st_mtime": match.getProperty("modified", now),
                        # "st_ctime": match.getProperty("created", now)
                    })

                elif match.isLink() and match.isFound():
                    attrs.update({
                        "st_mode": (stat.S_IFLNK | int( match.getProperty("mode", 0o777) ) ),
                        "st_nlink": int( match.getProperty("links", 1) ),
                        "st_uid": int( match.getProperty("owner", owner) ),
                        "st_gid": int( match.getProperty("group", group) ),
                        "st_size": 0,
                        # "st_atime": match.getProperty("", now),
                        # "st_mtime": match.getProperty("modified", now),
                        # "st_ctime": match.getProperty("created", now)
                    })

                else:
                    # Note; Unless I throw this here, I am unable to
                    # touch files as attributes. I think the default
                    # here should be to throw FuseOSError(errno.ENOENT)
                    # unless file/node is actually found
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: getattr exception ', e)
            raise FuseOSError(errno.ENOENT)

        return attrs

    # def getxattr(self, path, name, position=0):
    #     raise FuseOSError(ENOTSUP)

    # def init(self, path):
    #     pass

    # def ioctl(self, path, cmd, arg, fip, flags, data):
    #     raise FuseOSError(errno.ENOTTY)

    def link(self, target, source):
        self.notReadOnly()

        created = False

        try:

            targetmatch = GremlinFSPath.match(target)
            sourcematch = GremlinFSPath.match(source)

            targetmatch.enter("link", target, source)
            if targetmatch and sourcematch:
                if not targetmatch.isFound():
                    created = targetmatch.createLink(sourcematch)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: link exception ', e)
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

    # def listxattr(self, path):
    #     return []

    # 

    def mkdir(self, path, mode):
        self.notReadOnly()

        created = False

        try:

            match = GremlinFSPath.match(path)
            match.enter("mkdir", path, mode)

            if match:
                if not match.isFound():
                    created = match.createFolder() # mode)

                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: mkdir exception ', e)
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

    def mknod(self, path, mode, dev):
        self.notReadOnly()
        raise FuseOSError(errno.ENOENT)

    def open(self, path, flags):

        found = False

        try:

            match = GremlinFSPath.match(path)
            match.enter("open", path, flags)
            if match:
                if match.isFile() and match.isFound():
                    found = True
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: open exception ', e)
            raise FuseOSError(errno.ENOENT)

        if found:
            return 0

        return 0

    # def opendir(self, path):
    #     return 0

    def read(self, path, size, offset, fh):

        data = None

        try:

            match = GremlinFSPath.match(path)
            match.enter("read", path, size, offset)
            if match:
                if match.isFile() and match.isFound():
                    data = match.readFile(size, offset)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: read exception ', )
            raise FuseOSError(errno.ENOENT)

        if data:
            return data

        return None

    def readdir(self, path, fh):

        entries = [
            '.',
            '..'
        ]

        try:

            match = GremlinFSPath.match(path)
            match.enter("readdir", path)
            if match:
                if match.isFolder() and match.isFound():
                    entries.extend(
                        match.readFolder()
                    )
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: readdir exception ', e)
            raise FuseOSError(errno.ENOENT)

        return entries

    def readlink(self, path):

        newpath = None

        try:

            match = GremlinFSPath.match(path)
            match.enter("readlink", path)
            if match:
                if match.isLink() and match.isFound():
                    newpath = match.readLink()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: readlink exception ', e)
            raise FuseOSError(errno.ENOENT)

        if newpath:
            return newpath

        return None

    def release(self, path, fh):
        return 0

    # def releasedir(self, path, fh):
    #     return 0

    # def removexattr(self, path, name):
    #     raise FuseOSError(ENOTSUP)

    def rename(self, old, new):
        self.notReadOnly()

        renamed = False

        try:

            oldmatch = GremlinFSPath.match(old)
            newmatch = GremlinFSPath.match(new)
            oldmatch.enter("rename", old, new)
            if oldmatch and newmatch:

                # if oldmatch.isFile() and \
                #    oldmatch.isFound() and \
                #    not newmatch.isFound():
                #    renamed = oldmatch.renameFile(newmatch)
                # elif oldmatch.isFolder() and \
                #    oldmatch.isFound() and \
                #    not newmatch.isFound():
                #    renamed = oldmatch.renameFolder(newmatch)

                if oldmatch.isFile() and \
                   oldmatch.isFound():
                    if newmatch.isFound():
                        newmatch.deleteFile()
                    renamed = oldmatch.renameFile(newmatch)
                elif oldmatch.isFolder() and \
                   oldmatch.isFound():
                    if newmatch.isFound():
                        newmatch.deleteFolder()
                    renamed = oldmatch.renameFolder(newmatch)

                else:
                    raise FuseOSError(errno.ENOENT)

        # except FuseOSError:
        #     # Don't log here
        #     raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: rename exception ', )
            raise FuseOSError(errno.ENOENT)

        if renamed:
            return 0

        return 0

    def rmdir(self, path):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("rmdir", path)
            if match:
                if match.isFolder() and match.isFound():
                    match.deleteFolder()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: rmdir exception ', e)
            raise FuseOSError(errno.ENOENT)

        return 0

    # def setxattr(self, path, name, value, options, position=0):
    #     raise FuseOSError(ENOTSUP)

    def statfs(self, path):
        return {}

    def symlink(self, target, source):
        self.notReadOnly()

        created = False

        try:

            targetmatch = GremlinFSPath.match(target)
            sourcematch = GremlinFSPath.match(source)

            targetmatch.enter("symlink", target, source)
            if targetmatch and sourcematch:
                if not targetmatch.isFound():
                    created = targetmatch.createLink(sourcematch)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: symlink exception ', e)
            raise FuseOSError(errno.ENOENT)

        if created:
            return 0

        return 0

    def truncate(self, path, length, fh=None):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("truncate", path)
            if match:
                if match.isFile() and match.isFound():
                    match.clearFile()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: truncate exception ', e)
            raise FuseOSError(errno.ENOENT)

        # raise FuseOSError(errno.ENOENT)
        return 0

    def unlink(self, path):
        self.notReadOnly()

        try:

            match = GremlinFSPath.match(path)
            match.enter("unlink", path)
            if match:
                if match.isFile() and match.isFound():
                    match.deleteFile()
                elif match.isLink() and match.isFound():
                    match.deleteLink()
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: unlink exception ', e)
            raise FuseOSError(errno.ENOENT)

        return 0

    def utimens(self, path, times=None):
        self.enter("utimens", path)
        return 0

    def write(self, path, data, offset, fh):
        self.notReadOnly()

        if not data:
            data = ""

        try:

            match = GremlinFSPath.match(path)
            match.enter("write", path, data, offset)
            if match:
                if match.isFile() and match.isFound():
                    data = match.writeFile(data, offset)
                else:
                    raise FuseOSError(errno.ENOENT)

        except FuseOSError:
            # Don't log here
            raise FuseOSError(errno.ENOENT)

        except Exception as e:
            self.logger.exception(' GremlinFS: write exception ', e)
            raise FuseOSError(errno.ENOENT)

        if data:
            return len(data)

        # raise FuseOSError(errno.ENOENT)
        return 0

    # 

    def isReadOnly(self):
        return False

    def notReadOnly(self):
        if self.isReadOnly():
            raise FuseOSError(errno.EROFS)
        return True



class GremlinFSCachingOperations(GremlinFSOperations):

    logger = GFSLogger.getLogger("GremlinFSCachingOperations")

    def __init__(
        self,
        **kwargs):
        super().__init__(**kwargs)

        self.caching = True # False
        self.cache = Dict() # {}

    # 

    def lookupCache(self, path, oper):

        from datetime import datetime
        self.logger.debug("CACHE: lookup: path: %s, oper: %s", path, oper)

        pick = False
        cachehit = self.cache[path][oper]
        if cachehit:
            if cachehit['expire']:
                if cachehit['expire'] > datetime.now():
                    self.logger.debug("CACHE: lookup: cachehit: found entry with expire, not expired")
                    pick = cachehit
                else:
                    self.logger.debug("CACHE: lookup: cachehit: found entry with expire, is expired")
            else:
                pick = cachehit

        if pick:
            cachehit = pick
            exp = ""
            if cachehit['expire']:
                exp = str(cachehit['expire'])
            self.logger.debug("CACHE: lookup: cachehit: PATH: %s, OPER: %s, CREATED: %s, EXPIRE: %s", 
                cachehit.path, cachehit.oper, str(cachehit.created), exp
            )
            return cachehit.data

        else:
            return False

    def prepareCache(self, path, oper, expire_seconds = None):

        from datetime import datetime, timedelta
        self.logger.debug("CACHE: prepare: path: %s, oper: %s", path, oper)

        expire = None
        if expire_seconds:
            time = datetime.now()
            expire = time + timedelta(0, expire_seconds) # days, seconds, then other fields.

        self.cache[path][oper]['path'] = path
        self.cache[path][oper]['oper'] = oper
        self.cache[path][oper]['flags'] = 1 # Indicate not yet active cache entry
        if expire:
            self.cache[path][oper]['expire'] = expire

        return self.cache[path][oper]

    def finalizeCache(self, path, oper, data):

        self.logger.debug("CACHE: finalize: path: %s, oper: %s", path, oper)
        self.cache[path][oper]['data'] = data
        self.cache[path][oper]['flags'] = 0 # Indicate active cache entry

        return self.cache[path][oper]

    def readCache(self, path, oper):

        cachepath = path
        cacheoper = oper
        expire = 60

        if cachepath and self.caching:
            try:
                cachedata = self.lookupCache(cachepath, cacheoper)
                if cachedata:
                    return cachedata
            except Exception as e:
                self.logger.warning("Client call not fatal error: lookup cache error: exception: %s" % ( str(e) ))
                self.logger.warning(e)

        if cachepath and self.caching:
            cache = None
            try:
                cache = self.prepareCache(cachepath, cacheoper, expire)
            except Exception as e:
                self.logger.warning("Client call not fatal error: prepare cache error: exception: %s" % ( str(e) ))
                self.logger.warning(e)

    def updateCache(self, path, oper, data):

        cachepath = path
        cacheoper = oper

        if data:
            if cachepath and self.caching:
                try:
                    self.finalizeCache(cachepath, cacheoper, data)
                except Exception as e:
                    self.logger.warning("Client call not fatal error: finalize cache error: exception: %s" % ( str(e) ))
                    self.logger.warning(e)

        return data

    def clearCache(self, path):

        cachepath = path

        self.logger.debug("CACHE: clear: path: %s", path)
        del self.cache[path]

    # 

    def chmod(self, path, mode):
        ret = super().chmod(path, mode)
        self.clearCache(path)
        return ret

    def chown(self, path, uid, gid):
        ret = super().chown(path, uid, gid)
        self.clearCache(path)
        return ret

    def create(self, path, mode, fi=None):
        ret = super().create(path, mode, fi)
        self.clearCache(path)
        return ret

    # def destroy(self, path):
    #     pass

    def flush(self, path, fh):
        ret = super().flush(path, fh)
        self.clearCache(path)
        return ret

    def fsync(self, path, datasync, fh):
        ret = super().fsync(path, datasync, fh)
        self.clearCache(path)
        return ret

    # def fsyncdir(self, path, datasync, fh):
    #     pass

    def getattr(self, path, fh=None):

        cachepath = path
        cacheoper = 'getattr'

        cachedata = self.readCache(cachepath, cacheoper)
        if cachedata:
            return cachedata

        ret = super().getattr(path, fh)
        self.updateCache(cachepath, cacheoper, ret)

        return ret

    # def getxattr(self, path, name, position=0):
    #     pass

    # def init(self, path):
    #     pass

    # def ioctl(self, path, cmd, arg, fip, flags, data):
    #     pass

    def link(self, target, source):
        ret = super().link(target, source)
        self.clearCache(target)
        self.clearCache(source)
        return ret

    # def listxattr(self, path):
    #     pass

    # 

    def mkdir(self, path, mode):
        ret = super().mkdir(path, mode)
        self.clearCache(path)
        return ret

    def mknod(self, path, mode, dev):
        ret = super().mknod(path, mode, dev)
        self.clearCache(path)
        return ret

    def open(self, path, flags):
        ret = super().open(path, flags)
        return ret

    # def opendir(self, path):
    #     pass

    def read(self, path, size, offset, fh):
        ret = super().read(path, size, offset, fh)
        return ret

    def readdir(self, path, fh):

        cachepath = path
        cacheoper = 'readdir'

        cachedata = self.readCache(cachepath, cacheoper)
        if cachedata:
            return cachedata

        ret = super().readdir(path, fh)
        self.updateCache(cachepath, cacheoper, ret)

        return ret

    def readlink(self, path):
        ret = super().readlink(path)
        return ret

    def release(self, path, fh):
        ret = super().release(path, fh)
        return ret

    # def releasedir(self, path, fh):
    #     pass

    # def removexattr(self, path, name):
    #     pass

    def rename(self, old, new):
        ret = super().rename(old, new)
        self.clearCache(old)
        self.clearCache(new)
        return ret

    def rmdir(self, path):
        ret = super().rmdir(path)
        self.clearCache(path)
        return ret

    # def setxattr(self, path, name, value, options, position=0):
    #     pass

    def statfs(self, path):
        ret = super().statfs(path)
        return ret

    def symlink(self, target, source):
        ret = super().symlink(target, source)
        self.clearCache(target)
        self.clearCache(source)
        return ret

    def truncate(self, path, length, fh=None):
        ret = super().truncate(path, length, fh)
        self.clearCache(path)
        return ret

    def unlink(self, path):
        ret = super().unlink(path)
        self.clearCache(path)
        return ret

    def utimens(self, path, times=None):
        ret = super().utimens(path, times)
        return ret

    def write(self, path, data, offset, fh):
        ret = super().write(path, data, offset, fh)
        self.clearCache(path)
        return ret
