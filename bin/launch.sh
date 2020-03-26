#!/bin/sh

set -x


echo ${GFS_FS_NAME}
echo ${GFS_FS_MOUNT}


echo ${GREMLIN_HOST}
echo ${GREMLIN_PORT}
# echo ${GREMLIN_USERNAME}
# echo ${GREMLIN_PASSWORD}


echo ${RABBITMQ_HOST}
echo ${RABBITMQ_PORT}
# echo ${RABBITMQ_USERNAME}
# echo ${RABBITMQ_PASSWORD}



cd /home/theia

mkdir -p ${GFS_FS_MOUNT}

# python /app/src/py/gremlinfs/gremlinfs.py ${GFS_FS_MOUNT} ${GREMLIN_HOST} ${GREMLIN_PORT} ${GREMLIN_USERNAME} ${GREMLIN_PASSWORD} ${RABBITMQ_HOST} ${RABBITMQ_PORT} ${RABBITMQ_USERNAME} ${RABBITMQ_PASSWORD} &
python3 /app/src/py/gremlinfs/gremlinfs.py ${GFS_FS_MOUNT} ${GREMLIN_HOST} ${GREMLIN_PORT} ${GREMLIN_USERNAME} ${GREMLIN_PASSWORD} ${RABBITMQ_HOST} ${RABBITMQ_PORT} ${RABBITMQ_USERNAME} ${RABBITMQ_PASSWORD} &

node /home/theia/src-gen/backend/main.js /home/project --hostname=0.0.0.0
