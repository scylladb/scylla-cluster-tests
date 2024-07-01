
### Building from DEB files (quicker)

```
export DEB_LIST_URL=https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/branch-6.0/deb/unified/latest/scylladb-6.0/scylla.list
export CS_DOCKER_IMAGE=scylladb/hydra-loaders:cassandra-stress-6.0-$(date +'%Y%m%d')
docker build . -t ${CS_DOCKER_IMAGE} -f Dockerfile-deb --build-arg DEB_LIST_URL=$DEB_LIST_URL --build-arg DEB_KEY=87722433EBB454AE
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
