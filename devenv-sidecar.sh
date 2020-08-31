#!/bin/bash
set -e

CONTAINER_NAME="gremlinfs-dev-sidecar"
GIT_SHA=$(git rev-parse HEAD | cut -c 1-8)
IMAGE="bash"

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
THIS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "This Dir: $THIS_DIR"
source $THIS_DIR/devenv-common.sh

docker stop $CONTAINER_NAME || true
docker rm $CONTAINER_NAME || true
docker run \
  -it \
  --privileged \
  -v /$GFS_VOLUME:/$GFS_VOLUME \
  --network host \
  --name $CONTAINER_NAME \
  $IMAGE 