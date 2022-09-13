# running a YCSB based test with dynamodb

A test that setup only loaders nodes, and run with dynamodb
You need to specific the dynamodb table in the ycsb commands
(at least for now, SCT won't create the tables)

credentials can be pass using as defined in:
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default

we have a profile we'll add by default to loader using dynamodb that would have
access to dynamodb apis

### with docker backend

```bash
# need to specify scylla version, since the docker loader node is base on scylla image
export SCT_SCYLLA_VERSION=4.6.3
export SCT_DOCKER_IMAGE=scylladb/scylla
export SCT_REGION_NAME=eu-west-1

hydra run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/dynamodb/dynamodb.yaml
```

### with AWS backend

```bash
# need those until we can fix the configurtion to be aware of it
export SCT_INSTANCE_TYPE_DB=fake_type
export SCT_SCYLLA_VERSION=4.6.3
export SCT_REGION_NAME=eu-west-1
export SCT_IP_SSH_CONNECTIONS=public
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/dynamodb/dynamodb.yaml
```


### run performance test with docker backend

```bash
export SCT_N_DB_NODES=0
export SCT_N_MONITOR_NODES=0
export SCT_REGION_NAME=eu-west-1
export SCT_DB_TYPE=dynamodb
export SCT_AWS_INSTANCE_PROFILE_NAME=qa-dynamodb-access-profile

export SCT_CONFIG_FILES=test-cases/performance/perf-regression-alternator.100threads.30M-keys.yaml
hydra run-test performance_regression_alternator_test.PerformanceRegressionAlternatorTest.test_write --backend docker
```
