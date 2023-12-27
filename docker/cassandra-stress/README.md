
### Building from DEB files (quicker)

```
export DEB_LIST_URL=https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/branch-5.1/deb/unified/2022-08-18T12%3A17%3A46Z/scylladb-5.1/scylla.list
export CS_DOCKER_IMAGE=scylladb/hydra-loaders:cassandra-stress-$(date +'%Y%m%d')
docker build . -t ${CS_DOCKER_IMAGE} -f Dockerfile-deb --build-arg DEB_LIST_URL=$DEB_LIST_URL
docker push ${CS_DOCKER_IMAGE}
```

### Building from fork source (slower)

this is just example of such usage, you should point it to the fork/branch you need

```
export BRANCH=use_rack_aware
export GIT_FORK=https://github.com/sylwiaszunejko/scylla-tools-java
export CS_DOCKER_IMAGE=scylladb/hydra-loaders:cassandra-stress-${BRANCH}-$(date +'%Y%m%d')
docker build . -t ${CS_DOCKER_IMAGE} -f Dockerfile-src --build-arg BRANCH=${BRANCH} --build-arg GIT_FORK=${GIT_FORK}
docker push ${CS_DOCKER_IMAGE}
```
