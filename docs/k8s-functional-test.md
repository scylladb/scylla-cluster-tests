## k8s functional tests

Functional tests are stored in ``functional_tests/`` directory
You can use any configuration file for them, but in general you need ``test-cases/scylla-operator/functional.yaml``
You can use any backend, but for scylla_operator tests it needs to be any kubernetes backend,
such as ``k8s-eks, k8s-gke, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce``

Hardware requirements:
    You need to have at least **16Gb** of RAM and **120Gb** of disk to get it running

After the run tests logs are stored in the directory you passed to --logdir, or to ``~/sct-results`` if you did not

### Running in hydra

on EKS:
```bash
hydra "run-pytest functional_tests/scylla_operator --backend k8s-eks --config test-cases/scylla-operator/functional.yaml --logdir='`pwd`'"
```
on Local kind cluster
```bash
hydra "run-pytest functional_tests/scylla_operator --backend k8s-local-kind --config test-cases/scylla-operator/functional.yaml  --logdir='`pwd`'"
```

### Running via sct.py

The benefit of running in this mode, is that you can reuse your local kind binary

on EKS
```bash
sct.py run-pytest functional_tests/scylla_operator --backend k8s-eks --config test-cases/scylla-operator/functional.yaml --logdir="`pwd`"
```

on Local kind cluster
```bash
sct.py run-pytest functional_tests/scylla_operator --backend k8s-local-kind --config test-cases/scylla-operator/functional.yaml --logdir="`pwd`"
```

### Running via python

The benefit of running in this mode, is that not only you can reuse your local kind binary
But you also can use breakpoints to debug tests

on EKS
```bash
SCT_CLUSTER_BACKEND=k8s-eks SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator
```

on Local kind cluster
```bash
    SCT_CLUSTER_BACKEND=k8s-local-kind SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator
```

### Reuse cluster

You can reuse cluster in any mode you are running by populating "SCT_REUSE_CLUSTER" env variable.
There is only difference for local mini kubernetes cluster, in such case it won't respect SCT_REUSE_CLUSTER value
 and will reuse any cluster it find

on EKS
```bash
SCT_REUSE_CLUSTER=<test_id> SCT_CLUSTER_BACKEND=k8s-eks SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator
```

on Local kind cluster

```bash
SCT_REUSE_CLUSTER=1 SCT_CLUSTER_BACKEND=k8s-local-kind SCT_CONFIG_FILES=test-cases/scylla-operator/functional.yaml python -m pytest functional_tests/scylla_operator
```

### Using additional pytest options

It is possible to provide any pytest option to the test runner using `PYTEST_ADDOPTS` env variable.
For example, to make test runner stop after first failure do following
```bash
PYTEST_ADDOPTS='--maxfail=1' ./sct.py run-pytest functional_tests/scylla_operator ...
```

Or if it is needed to run tests in random order following can be used::
```bash
# Always random
PYTEST_ADDOPTS='--random-order' ./sct.py run-pytest functional_tests/scylla_operator ...

# Keeping seed for specific chain reproducing/debugging
PYTEST_ADDOPTS='--random-order-seed=12321' ./sct.py run-pytest functional_tests/scylla_operator ...

# Changing test mixing groups by using --random-order-bucket=module (can also be class, package and global)
PYTEST_ADDOPTS='--random-order-bucket=module' ./sct.py run-pytest functional_tests/scylla_operator ...
```

It is possible to provide multiple additional options doing following::
```bash
PYTEST_ADDOPTS='--maxfail=1 --random-order' ./sct.py run-pytest functional_tests/scylla_operator ...
```
