## Contribution

### setting pre commit hooks

Since we are trying to keep the code neat, please install this git precommit hooks, that would fix the code style, and run more checks::

```bash
pip install pre-commit==2.15.0
pre-commit install
pre-commit install --hook-type commit-msg
```
If you want to remove the hook::

```bash
pre-commit uninstall
```

Doing a commit without the hook checks::
```
    git commit ... -n
```

### update the configuration docs

Any time new SCT configuration option is added, the docs should be updated
with the following command:
```bash
hydra update-conf-docs
```

### Building Hydra Docker image

Once you have changes in the requirements.in or in Hydra Dockerfile

- change the version in docker/env/version
- run ``./docker/env/build_n_push.sh`` to build and push to Docker Hub


### Creating pipeline jobs for new branch

Once a new branch is created, we could build all the need job for this branch with the following script ::
```bash
JENKINS_USERNAME=[jenkins username] JENKINS_PASSWORD=[token from jenkins] hydra create-test-release-jobs scylla-2025.1 --sct_branch branch-2025.1
```

### Creating pipeline jobs for new scylla-operator branch/release/tag

Create new set of scylla-operator jobs using following command ::
```bash
hydra create-operator-test-release-jobs \
  operator-1.2 \
  jenkins-username \
  jenkins-user-password-or-api-token \
  --sct_branch dev \
  --sct_repo git@github.com:some-github-username-123321/scylla-cluster-tests.git
```

### [Setup new regions AWS](./aws_configuration.md)

### [Configure new project GCE](./gcp_create_new_project.md)

### [Docker Based Loaders](./docker-loaders.md)

### Directory Structure of the project

1. A library, called `sdcm` (stands for scylla distributed cluster
   manager). The word 'distributed' was used here to differentiate
   between that and CCM, since with CCM the cluster nodes are usually
   local processes running on the local machine instead of using actual
   machines running scylla services as cluster nodes. It contains:

   * `sdcm.cluster`: Base classes for Clusters
   * `sdcm.remote`: SSH library
   * `sdcm.nemesis`: Nemesis classes (a nemesis is a class that does disruption in the node)
   * `sdcm.tester`: Contains the base test class, see below.

2. A data directory, aptly named `data_dir`. It contains:

   * scylla repo file (to prepare a loader node)
   * Files that need to be copied to cluster nodes as part of setup/test procedures
   * yaml file containing test config data:

     * AWS machine image ids
     * Security groups
     * Number of loader nodes
     * Number of cluster nodes
   * SCT dashboards definition files for Grafana

3. Python files with tests. The convention is that test files have the `_test.py` suffix.
4. Utilities directory named `utils`. Contains help utilities for log analyzing
