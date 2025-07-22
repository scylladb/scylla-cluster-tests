# SCT - Scylla Cluster Tests

SCT tests are designed to test Scylla database on physical/virtual servers under high read/write load.
Currently, the tests are run using built in unittest
These tests automatically create:

* Scylla clusters - Run Scylla database
* Loader machines - used to run load generators like cassandra-stress
* Monitoring server - uses official Scylla Monitoring [repo](https://github.com/scylladb/scylla-monitoring) to monitor Scylla clusters and Loaders

## Quickstart

#### Option 1 - Config AWS using OKTA (preferred option)

https://www.notion.so/AWS-864b26157112426f8e74bab61001425d

#### Option 2 - Config AWS using AWS credentials
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

# if using podman, we need to disable enforcing of short name usage, without it monitoring stack won't run from withing hydra
echo 'unqualified-search-registries = ["registry.fedoraproject.org", "registry.access.redhat.com", "docker.io", "quay.io"]
short-name-mode="permissive"
' > ~/.config/containers/registries.conf
```

### Run a test

Example running test using Hydra using `test-cases/PR-provision-test.yaml` configuration file

#### Run test locally with AWS backend:
```bash
export SCT_SCYLLA_VERSION=5.2.1
# Test fails to report to Argus. So we need to disable it
export SCT_ENABLE_ARGUS=false
# configuration is needed for running from a local development machine (default communication is via private addresses)
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml --config configurations/network_config/test_communication_public.yaml

# Run with IPv6 configuration
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml --config configurations/network_config/all_addresses_ipv6_public.yaml

```

#### Run test using [SCT Runner](./docs/sct-runners.md) with AWS backend:
```bash
hydra create-runner-instance --cloud-provider <cloud_name> -r <region_name> -z <az> -t <test-id> -d <run_duration>

export SCT_SCYLLA_VERSION=5.2.1
# For choose correct network configuration, check test jenkins pipeline.
# All predefined configurations are located under `configurations/network_config`
hydra --execute-on-runner <runner-ip|`cat sct_runner_ip> "run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml"
```

#### Run test locally with GCE backend:
```bash
export SCT_SCYLLA_VERSION=5.2.1
export SCT_IP_SSH_CONNECTIONS="public"
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
# NOTE: Only use `whoami` if your local use is the same as your okta/email username
hydra list-resources --user `whoami`
```

#### Reuse already running cluster:
```bash
export SCT_REUSE_CLUSTER=$(cat ~/sct-results/latest/test_id)
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/PR-provision-test.yaml --config configurations/network_config/test_communication_public.yaml
```
More details on reusing a cluster can be found in [reuse_cluster](./docs/reuse_cluster.md)

#### Clear resources:
```bash
hydra clean-resources --user `whoami`
# by default, it only cleans aws resources
# to clean other backends, specify manually
hydra clean-resources --user `whoami` -b gce
```

#### Clear resources being used by the last test run:
```bash
SCT_CLUSTER_BACKEND= hydra clean-resources --test-id `cat ~/sct-results/latest/test_id`
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

# Local developer setup

* Requires [uv](https://github.com/astral-sh/uv#installation)

```bash
uv python install
uv venv
export UV_PROJECT_ENVIRONMENT=.venv
uv sync
```
