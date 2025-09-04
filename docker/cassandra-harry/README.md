```
export HARRY_DOCKER_IMAGE=scylladb/hydra-loaders:cassandra-harry-jdk21-$(date +'%Y%m%d')
docker build . -t ${HARRY_DOCKER_IMAGE}
docker push ${HARRY_DOCKER_IMAGE}
echo "${HARRY_DOCKER_IMAGE}" > image
```
