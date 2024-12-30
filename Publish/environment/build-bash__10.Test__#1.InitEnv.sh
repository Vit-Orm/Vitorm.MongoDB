set -e


#---------------------------------------------------------------------
# args

args_="

export basePath=/root/temp

# "


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #1 start MongoDB container'


docker rm vitorm-mongodb2 -f || true
docker rm vitorm-mongodb -f || true

docker run -d --net host --name vitorm-mongodb  mongo:4.4.29 mongod --replSet my-mongo-set
docker run -d --net host --name vitorm-mongodb2 mongo:4.4.29 mongod --port 27018 --replSet my-mongo-set


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #2 wait for MongoDB to init'
docker run -t --rm --net host mongo:4.4.29 timeout 120 sh -c "until (echo 'use db_orm;' | mongo); do echo waiting for MongoDB; sleep 2; done;"

docker run -t --rm --net host mongo:4.4.29 timeout 120 sh -c "until (echo 'use db_orm;' | mongo --host localhost --port 27018); do echo waiting for MongoDB; sleep 2; done;"


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #3 start replica set'
docker run -t --rm --net host mongo:4.4.29 sh -c "echo 'rs.initiate({\"_id\":\"my-mongo-set\",\"members\":[{\"_id\":0,\"host\":\"localhost:27017\"},{\"_id\":1,\"host\":\"localhost:27018\"}]})' | mongo"

docker run -t --rm --net host mongo:4.4.29 timeout 120 sh -c "until (echo 'use db_orm;\nuse db_orm2;' | mongo); do echo waiting for MongoDB; sleep 2; done;"


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #9 init test environment success!'