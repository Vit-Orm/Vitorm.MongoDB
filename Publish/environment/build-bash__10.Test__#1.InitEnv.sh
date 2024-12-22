set -e


#---------------------------------------------------------------------
# args

args_="

export basePath=/root/temp

# "


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #1 start MongoDB container'


docker network create net-vitorm-mongodb || true
docker rm vitorm-mongodb2 -f || true
docker rm vitorm-mongodb -f || true

docker run -d -p 27017:27017 --name vitorm-mongodb  --net net-vitorm-mongodb mongo:4.4.29 mongod --replSet my-mongo-set
docker run -d -p 30001:27017 --name vitorm-mongodb2 --net net-vitorm-mongodb mongo:4.4.29 mongod --replSet my-mongo-set


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #2 wait for MongoDB to init'
docker run -t --rm --net net-vitorm-mongodb mongo:4.4.29 timeout 120 sh -c "until (echo 'use db_orm;' | mongo --host vitorm-mongodb); do echo waiting for MongoDB; sleep 2; done;"

docker run -t --rm --net net-vitorm-mongodb mongo:4.4.29 timeout 120 sh -c "until (echo 'use db_orm;' | mongo --host vitorm-mongodb2); do echo waiting for MongoDB; sleep 2; done;"


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #3 start replica set'
docker run -t --rm --net net-vitorm-mongodb mongo:4.4.29 sh -c "echo 'rs.initiate({\"_id\":\"my-mongo-set\",\"members\":[{\"_id\":0,\"host\":\"vitorm-mongodb:27017\"},{\"_id\":1,\"host\":\"vitorm-mongodb2:27017\"}]})' | mongo --host vitorm-mongodb"

docker run -t --rm --net net-vitorm-mongodb mongo:4.4.29 timeout 120 sh -c "until (echo 'use db_orm;\nuse db_orm2;' | mongo --host vitorm-mongodb); do echo waiting for MongoDB; sleep 2; done;"


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #9 init test environment success!'