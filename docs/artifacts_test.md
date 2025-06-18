# What are artifacts tests?

Artifacts tests are kind of smoke tests which verify that packaging artifacts like .rpm/.deb files, cloud or container images produced by ScyllaDB builds can be deployed successfully.

Private repo test is also kind of artifacts test.

# Jenkins pipelines

Artifacts tests triggered by another projects which build .rpm's and .deb's. E.g., tests for CentOS triggered by `centos-rpm` project.

There are pipeline files for Jenkins in the repository:

```
vars
`-- artifactsPipeline.groovy
jenkins-pipelines
|-- artifacts-amazon2.jenkinsfile
|-- artifacts-ami.jenkinsfile
|-- artifacts-centos7.jenkinsfile
|-- artifacts-debian10.jenkinsfile
|-- artifacts-debian11.jenkinsfile
|-- artifacts-docker.jenkinsfile
|-- artifacts-oel76.jenkinsfile
|-- artifacts-ubuntu2004.jenkinsfile
`-- private-repo.jenkinsfile
```

`vars/artifactsPipeline.groovy` is a pipeline call definition which used to run same pipeline with different parameters.

You should use different parameters for .rpm/.deb tests and for AMI test:
- The only required parameter for .rpm/.deb jobs is `scylla_repo`, a path to a ScyllaDB repo (e.g., `https://s3.amazonaws.com/downloads.scylladb.com/rpm/unstable/centos/master/latest/scylla.repo`)
- For AMI job you need two parameters: `scylla_ami_id` and `region_name`
- For Docker job you need two parameters: `scylla_docker_image` (e.g., `scylladb/scylla` or `scylladb/scylla-nightly`) and `scylla_version` (it'll be used as Docker image tag)

To verify Scylla Manager package you need to provide `scylla_repo` and `scylla_mgmt_address` parameters. In this case Scylla Manager will be installed during artifacts test as well as ScyllaDB.

Alternatively, and even recommended, one can use the `manager_version` parameter to just choose the desired version. Branch options can be view in defaults/manager_versions.yaml

Optionally, you can override default cloud instance type by providing `instance_type` parameter. This parameter can be a space-separated list of instance types.

`artifacts-*.jenkinsfile` files can be used to create Jenkins projects.

`private-repo.jenkinsfile` can be used to create a Jenkins project which verifies private repo correctness. You need to provide `scylla_repo` parameter which points to a private repo.

# How to run artifacts tests using `hydra`

If you want to run it locally you should use `ip_ssh_connections: 'public'`:

```sh
export SCT_IP_SSH_CONNECTIONS=public
```

and run one of the following commands:

## CentOS 7
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos7.yaml
```

## CentOS 8
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos8.yaml
```

## Debian 10 (buster)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/debian10.yaml
```

## Debian 11 (bullseye)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/debian11.yaml
```

## RHEL 7
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/rhel7.yaml
```

## RHEL 8
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/rhel8.yaml
```

## Oracle Enterprise Linux 7.6
```sh
hydra run-test artifacts_test --backend aws --config test-cases/artifacts/oel76.yaml
```

## Amazon Linux 2
```sh
hydra run-test artifacts_test --backend aws --config test-cases/artifacts/amazon2.yaml
```

## Ubuntu 22.04 LTS (jammy)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2204.yaml
```

## Ubuntu 24.04 LTS (noble)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2404.yaml
```

## AMI

You can use `hydra` to find Scylla AMI in the desired region (`us-east-1` in example below):
```sh
hydra list-ami-versions -r us-east-1
```

and run the test:

```sh
SCT_AMI_ID_DB_SCYLLA="<ami you've found>" SCT_REGION_NAME=us-east-1 hydra run-test artifacts_test --backend aws --config test-cases/artifacts/ami.yaml
```

## Docker

If you want to run against some official ScyllaDB Docker image, you should go to [tag list](https://hub.docker.com/r/scylladb/scylla/tags) and choose some tag name (i.e., `3.3.rc1`)

```sh
SCT_SCYLLA_VERSION="<tag you've chose>" hydra run-test artifacts_test --backend docker --config test-cases/artifacts/docker.yaml
```
