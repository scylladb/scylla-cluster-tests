# What are artifacts tests?

Artifacts tests are kind of smoke tests which verify that packaging artifacts like .rpm/.deb files, cloud or container images produced by ScyllaDB builds can be deployed successfully.

# Jenkins pipelines

Artifacts tests triggered by another projects which build .rpm's and .deb's. E.g., tests for CentOS triggered by `centos-rpm` project.

There are pipeline files for Jenkins in the repository:

```
vars
`-- artifactsPipeline.groovy
jenkins-pipelines
|-- artifacts-ami.jenkinsfile
|-- artifacts-centos7.jenkinsfile
|-- artifacts-centos8.jenkinsfile
|-- artifacts-debian9.jenkinsfile
|-- artifacts-debian10.jenkinsfile
|-- artifacts-oel76.jenkinsfile
|-- artifacts-ubuntu1604.jenkinsfile
`-- artifacts-ubuntu1804.jenkinsfile
```

`vars/artifactsPipeline.groovy` is a pipeline call definition which used to run same pipeline with different parameters.

You should use different parameters for .rpm/.deb tests and for AMI test:
- The only required parameter for .rpm/.deb jobs is `scylla_repo`, a path to a ScyllaDB repo (e.g., `https://s3.amazonaws.com/downloads.scylladb.com/rpm/unstable/centos/master/latest/scylla.repo`)
- For AMI job you need two parameters: `scylla_ami_id` and `region_name`

To verify Scylla Manager package you need to provide `scylla_repo` and `scylla_mgmt_repo` parameters. In this case Scylla Manager will be installed during artifacts test as well as ScyllaDB.

Optionally, you can override default cloud instance type by providing `instance_type` parameter.

`artifacts-*.jenkinsfile` files can be used to create Jenkins projects.

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

## Debian 9 (stretch)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/debian9.yaml
```

## Debian 10 (buster)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/debian10.yaml
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

## Ubuntu 16.04 LTS (xenial)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu1604.yaml
```

## Ubuntu 18.04 LTS (bionic)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu1804.yaml
```

## AMI

You can use `hydra` to find Scylla AMI in the desired region (`us-east-1` in example below):
```sh
hydra list-ami-versions -r us-east-1
```

and run the test:

```sh
SCT_AMI_ID_DB_SCYLLA=<ami you've found> SCT_REGION_NAME=us-east-1 hydra run-test artifacts_test --backend aws --config test-cases/artifacts/ami.yaml
```
