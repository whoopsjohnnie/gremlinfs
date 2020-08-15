#!/bin/bash

set -x
set -e

apt-get -y install python3
apt-get -y install python3-pip

GFS_MOUNTPOINT="/data"
GREMLINFS_DIR="/gremlinfs"

mkdir -p $GFS_MOUNTPOINT
mkdir -p $GREMLINFS_DIR

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
THIS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "This Dir: $THIS_DIR"

# echo "Cleaning up..."
# rm -rf $GREMLINFS_DIR/* || true
# rmdir -rf $GREMLINFS_DIR || true
# rm -rf $GREMLINFS_DIR || true

# echo "Cloning gremlinfs from https://github.com/whoopsjohnnie/gremlinfs.git"
# git clone https://github.com/whoopsjohnnie/gremlinfs.git $GREMLINFS_DIR
# cd $GREMLINFS_DIR
# git fetch
# git checkout deploy1
# git pull origin deploy1

# source /opt/rh/rh-python36/enable
# -H /opt/rh/rh-python36/root/usr/bin/pip3 install -r $GREMLINFS_DIR/requirements.txt
pip3 install -r $GREMLINFS_DIR/requirements.txt

# pip3 list
# pip3 freeze

echo "Umounting current FUSE FS with umount -l $GFS_MOUNTPOINT"
systemctl stop gfs.mount.service || true
systemctl disable gfs.mount.service || true
umount -l $GFS_MOUNTPOINT || true
rm -f /etc/systemd/system/gfs.mount.service
systemctl daemon-reload
systemctl reset-failed

echo "Installing gremlinfs.mount.service systemd unit"
cp -f $THIS_DIR/gfs.mount.service /etc/systemd/system/gfs.mount.service
# cp -f $THIS_DIR/systemd_start_gremlinfs.sh $GREMLINFS_DIR/
systemctl daemon-reload
systemctl start gfs.mount.service
systemctl enable gfs.mount.service
systemctl status gfs.mount.service
