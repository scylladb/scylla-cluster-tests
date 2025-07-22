
### build from scylla fork master
```
export YCSB_DOCKER_IMAGE=scylladb/hydra-loaders:ycsb-jdk21-$(date +'%Y%m%d')
docker build . -t ${YCSB_DOCKER_IMAGE}
docker push ${YCSB_DOCKER_IMAGE}
```


### build from private fork/branch
```
export YCSB_DOCKER_IMAGE=scylladb/hydra-loaders:ycsb-jdk21-$(date +'%Y%m%d')
docker build . -t ${YCSB_DOCKER_IMAGE} --build-arg REPO=https://github.com/fruch/YCSB.git --build-arg BRANCH=dynamodb_aws_sdk_v2
docker push ${YCSB_DOCKER_IMAGE}
```
