#!/bin/bash
set -x

GFS_SERVICE_IP="10.88.88.183"

RABBITMQ_USER=rabbitmq
RABBITMQ_PASSWORD=rabbitmq
RABBITMQ_PORT=5672
ORIENTDB_USER=root
ORIENTDB_PASSWORD=root
ORIENTDB_PORT=8182


if [ -z ${GFS_SERVICE_IP} ]; then
    echo "GFS_SERVICE_IP not set - assuming this hosts the GFS_SERVICE, constructing from default interface..."
    DEFAULT_INTERFACE=$(netstat -r | grep default | awk '{print $NF}')
    if [ -z ${DEFAULT_INTERFACE} ]; then 
        echo "Didn't find a default interface; exiting.";
        exit 1 
    else 
        echo "DEFAULT_INTERFACE is set to '$DEFAULT_INTERFACE'"; 
    fi
    GFS_SERVICE_IP=$(ifconfig $DEFAULT_INTERFACE | grep inet | grep -v inet6 | awk '{ print $2 }')
    echo "GFS_SERVICE_IP=$GFS_SERVICE_IP"
fi

# To test: GFS_SERVICE_IP=10.0.2.15 && /opt/rh/rh-python36/root/usr/bin/python3.6 /gremlinfs/src/py/gremlinfs/gremlinfs.py /home/vagrant/data $GFS_SERVICE_IP 8182 root root $GFS_SERVICE_IP 5672 rabbitmq rabbitmq 
/opt/rh/rh-python36/root/usr/bin/python3.6 \
    /gremlinfs/src/py/gremlinfs/gremlinfs.py \
    $GFS_MOUNTPOINT \
    $GFS_SERVICE_IP \
    $ORIENTDB_PORT \
    $ORIENTDB_USER \
    $ORIENTDB_PASSWORD \
    $GFS_SERVICE_IP \
    $RABBITMQ_PORT \
    $RABBITMQ_USER \
    $RABBITMQ_PASSWORD & 
