
### build from fork
```
export SCYLLA_BENCH_BRANCH=heads/some_fixes
export SCYLLA_BENCH_FORK=fruch/scylla-bench
export NAME=`echo $SCYLLA_BENCH_BRANCH | cut -d "/" -f 2`
export SCYLLA_BENCH_DOCKER_IMAGE=scylladb/scylla-bench:scylla-bench-${NAME}
docker build . -t ${SCYLLA_BENCH_DOCKER_IMAGE} --build-arg version=${SCYLLA_BENCH_BRANCH} --build-arg fork=${SCYLLA_BENCH_FORK}
docker push ${SCYLLA_BENCH_DOCKER_IMAGE}
```
