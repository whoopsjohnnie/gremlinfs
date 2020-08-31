#!/bin/bash

docker build -f docker/Dockerfile.orientdb .
docker build -f docker/Dockerfile.theia . 
