#!/bin/bash

docker build -f Dockerfile.orientdb .
docker build -f Dockerfile.theia . 
