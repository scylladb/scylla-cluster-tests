```
export NDBENCH_DOCKER_IMAGE=scylladb/hydra-loaders:ndbench-jdk8-$(date +'%Y%m%d')
docker build . -t ${NDBENCH_DOCKER_IMAGE}
docker push ${NDBENCH_DOCKER_IMAGE}
echo "${NDBENCH_DOCKER_IMAGE}" > image
```
