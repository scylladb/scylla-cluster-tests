# What are artifacts tests?

Artifacts tests are kind of smoke tests which verify that packaging artifacts like .rpm/.deb files, cloud or container images produced by ScyllaDB builds can be deployed successfully.

# Jenkins pipelines

Artifacts tests triggered by another projects which build .rpm's and .deb's. E.g., tests for CentOS triggered by `centos-rpm` project.

There are pipeline files for Jenkins in the repository:

```
vars
`-- artifactsPipeline.groovy
jenkins-pipelines
|-- artifacts-centos7.jenkinsfile
|-- artifacts-centos8.jenkinsfile
|-- artifacts-debian9.jenkinsfile
|-- artifacts-debian10.jenkinsfile
|-- artifacts-oel76.jenkinsfile
|-- artifacts-ubuntu1604.jenkinsfile
`-- artifacts-ubuntu1804.jenkinsfile
```

`vars/artifactsPipeline.groovy` is a pipeline call definition which used to run same pipeline with different parameters.
The only required parameter for each job is `scylla_repo`, a path to a ScyllaDB repo (e.g., `https://s3.amazonaws.com/downloads.scylladb.com/rpm/unstable/centos/master/latest/scylla.repo`)

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
