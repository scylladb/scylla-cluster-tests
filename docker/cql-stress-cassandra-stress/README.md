### build from scylla repo
```
export CQL_STRESS_CS_DOCKER_IMAGE=scylladb/hydra-loaders:cql-stress-cassandra-stress-$(date +'%Y%m%d')
docker build . -t ${CQL_STRESS_CS_DOCKER_IMAGE}
docker push ${CQL_STRESS_CS_DOCKER_IMAGE}
echo "${CQL_STRESS_CS_DOCKER_IMAGE}" > image
```
