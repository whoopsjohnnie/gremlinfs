#!/bin/bash

set -x
set -e

GFS_MOUNTPOINT="/data"
GREMLINFS_DIR="/gremlinfs"

mkdir -p $GFS_MOUNTPOINT

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
systemctl start gfs.mount.service
