#!/bin/bash

set -x

# If using a non root user, then
# make sure mount dir is writable as user
# whoami

# GFSAPI_USER=
# GFSAPI_PASSWORD=
GFSAPI_PORT=5000

# if [ -z ${DEFAULT_IP} ]; then
#     echo "DEFAULT_IP not set, constructing from default interface..."
#     DEFAULT_INTERFACE=$(netstat -r | grep default | awk '{print $NF}')
#     if [ -z ${DEFAULT_INTERFACE} ]; then 
#         echo "Didn't find a default interface; exiting.";
#         exit 1 
#     else 
#         echo "DEFAULT_INTERFACE is set to '$DEFAULT_INTERFACE'"; 
#     fi
#     DEFAULT_IP=$(ifconfig $DEFAULT_INTERFACE | grep inet | grep -v inet6 | awk '{ print $2 }')
#     echo "DEFAULT_IP=$DEFAULT_IP"
# fi

# export DEFAULT_IP="192.168.0.27"
# export DEFAULT_IP="botwork"
# export DEFAULT_IP="192.168.59.3"
export DEFAULT_IP="127.0.0.1"
echo "CONNECTING TO IP: $DEFAULT_IP"

# To test: DEFAULT_IP=10.0.2.15 && /opt/rh/rh-python36/root/usr/bin/python3.6 /gremlinfs/gremlinfs.py /home/vagrant/data $DEFAULT_IP 5000
# /opt/rh/rh-python36/root/usr/bin/python3.6 \
python3 \
    /gremlinfs/src/py/gremlinfs/gremlinfs.py \
    /data \
    $DEFAULT_IP \
    $GFSAPI_PORT &

#     $GFSAPI_USER \
#     $GFSAPI_PASSWORD & 
