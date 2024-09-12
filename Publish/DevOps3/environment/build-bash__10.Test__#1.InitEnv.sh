set -e


#---------------------------------------------------------------------
# args

args_="

export basePath=/root/temp

# "


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #1 start MongoDB container'
docker rm vitorm-mongodb -f || true
docker run -d \
--name vitorm-mongodb \
-p 27017:27017 \
-e MONGO_INITDB_ROOT_USERNAME=mongoadmin \
-e MONGO_INITDB_ROOT_PASSWORD=mongoadminsecret \
mongo:4.4.29


#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #8 wait for MongoDB to init'
docker run -t --rm --link vitorm-mongodb mongo:4.4.29 timeout 120 sh -c "until (echo 'use db_dev' | mongo --host vitorm-mongodb -u mongoadmin -p mongoadminsecret); do echo waiting for MongoDB; sleep 2; done;"



#---------------------------------------------------------------------
echo '#build-bash__10.Test__#1.InitEnv.sh -> #9 init test environment success!'