#!/bin/bash

GFS_MOUNTPOINT="/gfs"
mkdir -p $GFS_MOUNTPOINT

GFSAPI_HOST=${GFSAPI_HOST:-192.168.56.60}
GFSAPI_PORT=${GFSAPI_PORT:-5000}

python \
    /gremlinfs/src/py/gremlinfs/gremlinfs.py \
    $GFS_MOUNTPOINT \
    $GFSAPI_HOST \
    $GFSAPI_PORT
