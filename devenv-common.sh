export COMMON_NETWORK="botcanics"
export GFS_VOLUME="gfs"

CMD="mkdir -p /gfs"
echo "you can run '$CMD' as root if sudo is not setup for this user."
sudo bash -c "$CMD"