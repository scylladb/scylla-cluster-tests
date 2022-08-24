# running a YCSB based test with dynamodb

A test that setup only loaders nodes, and run with dynamodb
You need to specific the dynamodb table in the ycsb commands
(at least for now, SCT won't create the tables)

credentials can be pass using as defined in:
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default

we have a profile we'll add by default to loader using dynamodb that would have
access to dynamodb apis

### with docker backend
```
# need to specify scylla version, since the docker loader node is base on scylla image
export SCT_SCYLLA_VERSION=4.6.3
export SCT_DOCKER_IMAGE=scylladb/scylla

hydra run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/dynamodb/dynamodb.yaml
```

# with AWS backend
```
# need those until we can fix the configurtion to be aware of it
export SCT_INSTANCE_TYPE_DB=fake_type
export SCT_SCYLLA_VERSION=4.6.3

export SCT_IP_SSH_CONNECTIONS=public
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/dynamodb/dynamodb.yaml
```
