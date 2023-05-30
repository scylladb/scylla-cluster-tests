# SCT - Scylla Cluster Tests

SCT tests are designed to test Scylla database on physical/virtual servers under high read/write load.
Currently, the tests are run using built in unittest
These tests automatically create:

* Scylla clusters - Run Scylla database
* Loader machines - used to run load generators like cassandra-stress
* Monitoring server - uses official Scylla Monitoring repo_ to monitor Scylla clusters and Loaders

## Quickstart

```bash
# Ask your AWS account admin to create a user and access key for AWS) and then configure AWS

> aws configure
AWS Access Key ID [****************7S5A]:
AWS Secret Access Key [****************5NcH]:
Default region name [us-east-1]:
Default output format [None]:

# Install hydra (docker holding all requirements for running SCT)
sudo ./install-hydra.sh
```

### Run a test

Example running test using Hydra using `test-cases/PR-provision-test.yaml` configuration file

on AWS:
```bash
export SCT_SCYLLA_VERSION=5.2.1
# configuration needed for running from a local development machine (default communication is via private addresses)
export SCT_IP_SSH_CONNECTIONS=public
export SCT_INTRA_NODE_COMM_PUBLIC=true
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml
```

on AWS using SCT Runner:
```bash
hydra create-runner-instance --cloud-provider <cloud_name> -r <region_name> -z <az> -t <test-id> -d <run_duration>

export SCT_SCYLLA_VERSION=5.2.1
hydra --execute-on-runner <runner-ip|`cat sct_runner_ip> "run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml"
```

on GCE:
```bash
export SCT_SCYLLA_VERSION=5.2.1
hydra run-test longevity_test.LongevityTest.test_custom_time --backend gce --config test-cases/PR-provision-test.yaml
```

on Azure:
```bash
export SCT_SCYLLA_VERSION=5.2.1
hydra run-test longevity_test.LongevityTest.test_custom_time --backend azure --config test-cases/PR-provision-test.yaml
```

You can also enter the containerized SCT environment using::
```bash
hydra bash
```

Depending on which backend hardware/cloud provider/virtualization you will use, relevant configuration of those backend
services should be done.

List resources being used by user:
```bash
hydra list-resources --user `whoami`
```

Clear resources being used by the last test run:
```bash
SCT_CLUSTER_BACKEND= hydra list-resources --test-id `cat ~/sct-results/latest/test_id`
```

### [Install local development environment](docs/install-local-env.md)
### [Frequently Asked Questions (FAQ)](docs/faq.md)
### [Contribution instructions](docs/contrib.md)

## Configuring test run configuration YAML

Take a look at the `test-cases/PR-provision-test.yaml` file. It contains a number of
configurable test parameters, such as DB cluster instance types and AMI IDs.
In this example, we're assuming that you have copied `test-cases/PR-provision-test.yaml`
to `test-cases/your_config.yaml`.

All the test run configurations are stored in `test-cases` directory.

Important: Some tests use custom hardcoded operations due to their nature,
so those tests won't honor what is set in `test-cases/your_config.yaml`.

the available configuration options are listed in [configuration_options](./docs/configuration_options.md)

## Different backends

### Docker
**NOTE:** for docker run work with monitoring stack, user should be part of sudo group,
and setup with passwordless access, see https://unix.stackexchange.com/a/468417 for example on how to setup

We can also enable running with scylla formal docker images:
```bash
# example of running specific docker version
export SCT_SCYLLA_VERSION=5.2.1
hydra run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test-docker.yaml
```

## Types of Tests
### [Artifact tests](./docs/artifacts_test.md)
### [Longevity Tests](./docs/longevity.md) (TODO: write explanation for them)
### Upgrade Tests (TODO: write explanation for them)
### Performance Tests (TODO: write explanation for them)
### Features Tests (TODO: write explanation for them)
### Manager Tests (TODO: write explanation for them)
### [K8S Functional Tests](./docs/k8s-functional-test.md)
### [Microbenchmarking Tests](./docs/microbenchmarking.md)
