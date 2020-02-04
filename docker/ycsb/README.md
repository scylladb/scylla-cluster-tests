```
export YCSB_DOCKER_IMAGE=scylladb/hydra-loaders:ycsb-jdk8-$(date +'%Y%m%d')
docker build . -t ${YCSB_DOCKER_IMAGE}
docker push ${YCSB_DOCKER_IMAGE}
echo "${YCSB_DOCKER_IMAGE}" > image
```
