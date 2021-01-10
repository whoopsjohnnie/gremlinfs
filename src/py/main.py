
#!python

import os
import sys
import logging

# 
from fuse import FUSE
from fuse import Operations
from fuse import FuseOSError

# 
# from gremlinfs.gremlinfs import GremlinFSLogger

from gremlinfs.gremlinfslog import GremlinFSLogger
from gremlinfs.gremlinfslib import GremlinFS

from gremlinfs.gremlinfs import GremlinFSOperations
# from gremlinfs.gremlinfs import GremlinFSCachingOperations

logging.basicConfig(level=logging.DEBUG)



def main(

    mount_point,

    gfs_host,
    gfs_port,
    gfs_username,
    gfs_password,

    **kwargs):

    try:

        logger = GremlinFSLogger.getLogger("main")

        gfs = GremlinFS()
        gfs.configure(
            gfs_host = gfs_host,
            gfs_port = gfs_port,
            gfs_username = gfs_username,
            gfs_password = gfs_password,
        )
        GremlinFS.instance(gfs)

        operations = GremlinFSOperations()
        # operations = GremlinFSCachingOperations()
        operations.configure(
            mount_point = mount_point,
            gfs = gfs
        )

        FUSE(
            operations,
            mount_point,
            nothreads = True,
            foreground = True,
            allow_other = True
        )

    except Exception as e:
        logger.exception(' GremlinFS: main/init exception ', e)



def sysarg(
    args,
    index,
    default = None):
    if args and len(args) > 0 and index >= 0 and index < len(args):
        return args[index]
    return default


if __name__ == '__main__':

    mount_point = sysarg(sys.argv, 1)

    gfs_host = sysarg(sys.argv, 2)
    gfs_port = sysarg(sys.argv, 3)
    # gfs_username = sysarg(sys.argv, 4)
    # gfs_password = sysarg(sys.argv, 5)

    main(

        mount_point = mount_point,

        gfs_host = gfs_host,
        gfs_port = gfs_port,
        gfs_username = None,
        gfs_password = None,

    )
