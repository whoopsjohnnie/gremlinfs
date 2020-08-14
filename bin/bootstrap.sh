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



# 
# MQ Health Check
# 
# https://stackoverflow.com/questions/31746182/docker-compose-wait-for-container-x-before-starting-y/41854997#41854997
mqtries=10
for i in `seq 1 $mqtries`
do
	# echo "$i"

	mqstatus=$(curl -s -o /dev/null -w "%{http_code}" http://${RABBITMQ_HOST}:${RABBITMQ_PORT})
	if [ "$mqstatus" -eq 0 ]; then
		echo " *** RABBITMQ CONNECTION FAILED "
		sleep 2
	else
		echo " *** RABBITMQ CONNECTION SUCCEEDED "
		break
	fi

done



adduser theia sudo
adduser theia fuse

sudo chmod a+rwx /dev/fuse

# su theia 
sudo -E -u theia /app/bin/launch.sh
