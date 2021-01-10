#!/bin/bash
set -e

image() {
    BASE="$1"
    GIT_SHA=$(git rev-parse HEAD | cut -c 1-8)
    IMAGE="jeremykuhnash/gremlinfs:$BASE-$GIT_SHA"
    IMAGE_LATEST="jeremykuhnash/gremlinfs:$BASE-latest"
    docker build -t $IMAGE -t $IMAGE_LATEST -f docker/Dockerfile.$BASE .
    docker push $IMAGE
    docker push $IMAGE_LATEST
}

image "alpine"
image "debian"
