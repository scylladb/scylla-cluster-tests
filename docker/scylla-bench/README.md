
### build release
```
export SCYLLA_BENCH_VERSION=tags/v0.1.8
export SCYLLA_BENCH_DOCKER_IMAGE=scylladb/hydra-loaders:scylla-bench-$SCYLLA_BENCH_VERSION
docker build . -t ${SCYLLA_BENCH_DOCKER_IMAGE} --build-arg version=$SCYLLA_BENCH_VERSION
docker push ${SCYLLA_BENCH_DOCKER_IMAGE}
```

### build from fork
```
export SCYLLA_BENCH_BRANCH=heads/some_fixes
export SCYLLA_BENCH_FORK=fruch/scylla-bench
export SCYLLA_BENCH_DOCKER_IMAGE=scylladb/hydra-loaders:scylla-bench-${SCYLLA_BENCH_BRANCH}
docker build . -t ${SCYLLA_BENCH_DOCKER_IMAGE} --build-arg version=${SCYLLA_BENCH_BRANCH} --build-arg fork=${SCYLLA_BENCH_FORK}
docker push ${SCYLLA_BENCH_DOCKER_IMAGE}
```
