
FROM python:2.7


# 
# docker build -t gremlinfs:latest .
# docker run --rm -it --privileged --cap-add SYS_ADMIN --cap-add MKNOD --device /dev/fuse gremlinfs:latest
# 


ENV NAME GREMLIN_HOST
ENV NAME GREMLIN_PORT
ENV NAME GREMLIN_USERNAME
ENV NAME GREMLIN_PASSWORD


ENV NAME RABBITMQ_HOST
ENV NAME RABBITMQ_PORT
ENV NAME RABBITMQ_USERNAME
ENV NAME RABBITMQ_PASSWORD


# Update and upgrade the software
RUN apt-get update -y --allow-unauthenticated
RUN apt-get upgrade -y --allow-unauthenticated

# Install FUSE
RUN apt-get install sshfs -y --allow-unauthenticated
RUN apt-get install libfuse-dev -y --allow-unauthenticated

ADD ./ /app

WORKDIR /app

RUN pip install -r ./requirements.txt
RUN pip install -U fusepy

# CMD mkdir -p /tmp/mntpoint && python /app/gremlinfs.py /tmp/mntpoint
CMD ["/app/bin/bootstrap.sh"]
