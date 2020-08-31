#!/bin/bash
set -e

CONTAINER_NAME="gremlinfs-dev"
GIT_SHA=$(git rev-parse HEAD | cut -c 1-8)
IMAGE="jeremykuhnash/$CONTAINER_NAME:$GIT_SHA"
IMAGE_LATEST="jeremykuhnash/$CONTAINER_NAME:latest"
COMMON_NETWORK="botcanics"

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
THIS_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "This Dir: $THIS_DIR"

docker build -t $IMAGE -t $IMAGE_LATEST -f docker/Dockerfile.debian .
docker stop $CONTAINER_NAME || true
docker rm $CONTAINER_NAME || true
docker run \
  --privileged \
  -d \
  -v $THIS_DIR/etc/targetd:/etc/target \
  -v /sys/kernel/config:/sys/kernel/config \
  -v /run/lvm:/run/lvm \
  -v /lib/modules:/lib/modules \
  -v /dev:/dev \
  -v $PWD:/gremlinfs \
  --network host \
  --name $CONTAINER_NAME \
  $IMAGE
docker exec -it $CONTAINER_NAME sh