#!/bin/bash
set -ex

GFS_SERVICE_IP="10.88.88.183"
GFS_MOUNTPOINT="/mnt/gfs"
GREMLINFS_DIR="/gremlinfs"
GREMLINFS_USER="bots"

if [[ -z "${PRIMARY_IP}" ]]; then
    PRIMARY_IP=$(ifconfig eth0 2>/dev/null |grep inet |grep netmask | cut -d "n" -f 2 | cut -d " " -f 2)
fi
if [[ -z "${PRIMARY_IP}" ]]; then
  "Didnt find eth0 as primary interface - please override PRIMARY_ID env var with correct device (maybe enp0sXXX ...etc.)"
  exit 1
fi


SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
THIS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "Script Dir: $THIS_DIR"
cd $THIS_DIR

echo "Cleaning up $GREMLINFS_DIR ..."
rm -fR $GREMLINFS_DIR || true
mkdir $GREMLINFS_DIR || true
chown ${GREMLINFS_USER}.${GREMLINFS_USER} $GREMLINFS_DIR

echo "Cloning gremlinfs from https://github.com/whoopsjohnnie/gremlinfs.git"
git clone -b deploy1 https://github.com/whoopsjohnnie/gremlinfs.git $GREMLINFS_DIR

source /opt/rh/rh-python36/enable \
  -H /opt/rh/rh-python36/root/usr/bin/pip3 \
  install -r $GREMLINFS_DIR/requirements.txt

# echo "exiting"
# exit 0

echo "Umounting current FUSE FS with umount -l $GFS_MOUNTPOINT"
systemctl stop gfs.mount.service || true
systemctl disable gfs.mount.service || true
umount -l $GFS_MOUNTPOINT || true
mkdir -p $GFS_MOUNTPOINT
chown ${GREMLINFS_USER}.${GREMLINFS_USER} $GFS_MOUNTPOINT

rm -f /etc/systemd/system/gfs.mount.service
systemctl daemon-reload
systemctl reset-failed

echo "Installing gremlinfs.mount.service systemd unit"
# alias cp=cp
# cp -f $THIS_DIR/gfs.mount.service /etc/systemd/system/gfs.mount.service

echo "Using a HereDoc will allow us to templatize setting some vars." 
cat << EOF > /etc/systemd/system/gfs.mount.service
[Unit]
Description=gfsmount

[Service]
User=${GREMLINFS_USER}
Restart=always
StartLimitInterval=0
RestartSec=5
StartLimitBurst=200
StandardOutput=journal
StandardError=journal
Type=forking
TimeoutStartSec=0
Environment=GFS_SERVICE_IP=${GFS_SERVICE_IP} GFS_MOUNTPOINT=${GFS_MOUNTPOINT}

ExecStart=${THIS_DIR}/systemd_start_gremlinfs.sh 

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl start gfs.mount.service
systemctl enable gfs.mount.service
systemctl status gfs.mount.service
