> docker run --rm -it -v "$(pwd)/data/demodb.json":/demodb.json orientdb:3.0.21-tp3 /orientdb/bin/console.sh

orientdb> CONNECT remote:192.168.0.41/demodb root root

Connecting to database [remote:192.168.0.41/demodb] with user 'root'...OK
orientdb {db=demodb}> IMPORT DATABASE /demodb.json -preserveClusterIDs=true
