#!/bin/bash

set -x
set -e

GFS_MOUNTPOINT="/data"
GREMLINFS_DIR="/gremlinfs"

# If using a non root user, then
# make sure mount dir is writable as user
mkdir -p $GFS_MOUNTPOINT
chmod 777 $GFS_MOUNTPOINT

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
THIS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "This Dir: $THIS_DIR"

systemctl daemon-reload
systemctl reset-failed
systemctl stop gfs.mount.service
