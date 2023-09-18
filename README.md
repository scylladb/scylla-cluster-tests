# SCT - Scylla Cluster Tests

SCT tests are designed to test Scylla database on physical/virtual servers under high read/write load.
Currently, the tests are run using built in unittest
These tests automatically create:

* Scylla clusters - Run Scylla database
* Loader machines - used to run load generators like cassandra-stress
* Monitoring server - uses official Scylla Monitoring [repo](https://github.com/scylladb/scylla-monitoring) to monitor Scylla clusters and Loaders

## Quickstart

```bash
# install aws cli
sudo apt install awscli # Debian/Ubuntu
sudo dnf install awscli # Redhat/Fedora
# or follow amazon instructions to get it: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

# Ask your AWS account admin to create a user and access key for AWS) and then configure AWS

> aws configure
AWS Access Key ID [****************7S5A]:
AWS Secret Access Key [****************5NcH]:
Default region name [us-east-1]:
Default output format [None]:

# if using OKTA, use any of the tools to create the AWS profile, and export it as such,
# anywhere you are gonna use hydra command (replace DeveloperAccessRole with the name of your profile):
export AWS_PROFILE=DeveloperAccessRole

# Install hydra (docker holding all requirements for running SCT)
sudo ./install-hydra.sh
```

### Run a test

Example running test using Hydra using `test-cases/PR-provision-test.yaml` configuration file

#### Run test locally with AWS backend:
```bash
export SCT_SCYLLA_VERSION=5.2.1
# configuration needed for running from a local development machine (default communication is via private addresses)
export SCT_IP_SSH_CONNECTIONS=public
export SCT_INTRA_NODE_COMM_PUBLIC=true
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml
```

#### Run test using [SCT Runner](./docs/sct-runners.md) with AWS backend:
```bash
hydra create-runner-instance --cloud-provider <cloud_name> -r <region_name> -z <az> -t <test-id> -d <run_duration>

export SCT_SCYLLA_VERSION=5.2.1
hydra --execute-on-runner <runner-ip|`cat sct_runner_ip> "run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml"
```

#### Run test locally with GCE backend:
```bash
export SCT_SCYLLA_VERSION=5.2.1
hydra run-test longevity_test.LongevityTest.test_custom_time --backend gce --config test-cases/PR-provision-test.yaml
```

#### Run test locally with Azure backend:
```bash
export SCT_SCYLLA_VERSION=5.2.1
hydra run-test longevity_test.LongevityTest.test_custom_time --backend azure --config test-cases/PR-provision-test.yaml
```

#### Run test locally with docker backend:
```bash
# **NOTE:** user should be part of sudo group, and setup with passwordless access,
# see https://unix.stackexchange.com/a/468417 for example on how to setup

# example of running specific docker version
export SCT_SCYLLA_VERSION=5.2.1
hydra run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test-docker.yaml
```

#### You can also enter the containerized SCT environment using:
```bash
hydra bash
```

#### List resources being used by user:
```bash
hydra list-resources --user `whoami`
```

#### Clear resources being used by the last test run:
```bash
SCT_CLUSTER_BACKEND= hydra list-resources --test-id `cat ~/sct-results/latest/test_id`
```

## [Install local development environment](docs/install-local-env.md)
## [Frequently Asked Questions (FAQ)](docs/faq.md)
## [Contribution instructions](docs/contrib.md)

## Supported backends

* `aws` - the mostly used backed, most longevity run on top of this backend
* `gce` - most of the artifacts and rolling upgrades run on top of this backend
* `azure` -
* `docker` - should be used for local development
* `baremetal` - can be used to run with already setup cluster

* `k8s-eks` -
* `k8s-gke` -
* `k8s-local-kind` - used for run k8s functional test locally
* `k8s-local-kind-gce` - used for run k8s functional test locally on GCE
* `k8s-local-kind-aws` - used for run k8s functional test locally on AWS

## Configuring test run configuration YAML

Take a look at the `test-cases/PR-provision-test.yaml` file. It contains a number of
configurable test parameters, such as DB cluster instance types and AMI IDs.
In this example, we're assuming that you have copied `test-cases/PR-provision-test.yaml`
to `test-cases/your_config.yaml`.

All the test run configurations are stored in `test-cases` directory.

Important: Some tests use custom hardcoded operations due to their nature,
so those tests won't honor what is set in `test-cases/your_config.yaml`.

the available configuration options are listed in [configuration_options](./docs/configuration_options.md)


## Types of Tests
### [Artifact tests](./docs/artifacts_test.md)
### [Longevity Tests](./docs/longevity.md) (TODO: write explanation for them)
### Upgrade Tests (TODO: write explanation for them)
### Performance Tests (TODO: write explanation for them)
### Features Tests (TODO: write explanation for them)
### Manager Tests (TODO: write explanation for them)
### [K8S Functional Tests](./docs/k8s-functional-test.md)
### [Microbenchmarking Tests](./docs/microbenchmarking.md)
