### build from scylla repo
```
export CQL_STRESS_CS_DOCKER_IMAGE=scylladb/hydra-loaders:cql-stress-cassandra-stress-$(date +'%Y%m%d')
docker build . -t ${CQL_STRESS_CS_DOCKER_IMAGE}
docker push ${CQL_STRESS_CS_DOCKER_IMAGE}
echo "${CQL_STRESS_CS_DOCKER_IMAGE}" > image
```

### build from a fork/branch
```
BRANCH=fix_mixed_stmt_preparation
GIT_FORK=https://github.com/muzarski/cql-stress.git

export CQL_STRESS_CS_DOCKER_IMAGE=scylladb/hydra-loaders:cql-stress-cassandra-stress-$(date +'%Y%m%d')-${BRANCH}
docker build . -t ${CQL_STRESS_CS_DOCKER_IMAGE} --build-arg BRANCH=${BRANCH} --build-arg REPO=${GIT_FORK}
docker push ${CQL_STRESS_CS_DOCKER_IMAGE}

```
