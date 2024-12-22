set -e


#---------------------------------------------------------------------
# args

args_="

export basePath=/root/temp

# "


#---------------------------------------------------------------------
echo '#build-bash__10.Test_#3.CleanEnv.sh'


echo '#build-bash__10.Test_#3.CleanEnv.sh -> #1 remove MongoDB'
docker rm vitorm-mongodb2 -f || true
docker rm vitorm-mongodb -f || true
docker network rm net-vitorm-mongodb || true


