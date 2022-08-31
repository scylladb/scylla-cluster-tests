
### Building from DEB files (quicker)

```
export DEB_LIST_URL=https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/branch-5.1/deb/unified/2022-08-18T12%3A17%3A46Z/scylladb-5.1/scylla.list
export CS_DOCKER_IMAGE=scylladb/hydra-loaders:cassandra-stress-$(date +'%Y%m%d')
docker build . -t ${CS_DOCKER_IMAGE} -f Dockerfile-deb --build-arg DEB_LIST_URL=$DEB_LIST_URL
docker push ${CS_DOCKER_IMAGE}
echo "${CS_DOCKER_IMAGE}" > image
```

### Building from source without shard aware driver (slower)

```
export BRANCH=branch-5.0
export CS_DOCKER_IMAGE=scylladb/hydra-loaders:cassandra-stress-${BRANCH}-$(date +'%Y%m%d')
docker build . -t ${CS_DOCKER_IMAGE} -f Dockerfile
docker build . -t ${CS_DOCKER_IMAGE} -f Dockerfile-src --build-arg BRANCH=${BRANCH}
docker push ${CS_DOCKER_IMAGE}
echo "${CS_DOCKER_IMAGE}" > image
```
