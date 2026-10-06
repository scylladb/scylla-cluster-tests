## k8s functional tests

Functional tests are stored in ``functional_tests/`` directory
You can use any configuration file for them, but in general you need ``test-cases/scylla-operator/functional.yaml``
You can use any backend, but for scylla_operator tests it needs to be any kubernetes backend,
such as ``k8s-eks, k8s-gke, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce``

Hardware requirements for local kind:
    a run peaks at ~25GiB of RAM (Scylla pods are sized ~4GiB each, plus kind, loaders and SCT itself),
    so use a 32GB host with ~26GiB available, and ~100GiB free disk for docker

After the run tests logs are stored in the directory you passed to --logdir, or to ``~/sct-results`` if you did not

### Local kind on your own machine

`k8s-local-kind` treats the machine running SCT as the kind host, so outside hydra it changes that machine:
it needs passwordless `sudo`, installs `kubectl` into `/usr/local/bin` and kind into `/var/tmp/kind` when missing,
writes `/etc/sysctl.d/99-sct-local-k8s.conf`, creates `/dev/loop*` device nodes and adds routes to the
pod (`10.16.0.0/16`) and service (`10.19.0.0/16`) subnets. It deletes any kind cluster named `kind` on start.

Be logged in to Docker Hub (`docker login`): `~/.docker/config.json` is mounted into the kind nodes,
otherwise image pulls hit the anonymous rate limit.

Bringing the cluster up takes ~20 minutes. To run a single test, pass `-k` through `PYTEST_ADDOPTS`
(`run-pytest` has no `-k` option):
```bash
PYTEST_ADDOPTS="-k test_scylla_operator_pods --maxfail=1" \
  ./sct.py run-pytest functional_tests/scylla_operator/test_functional.py \
  --backend k8s-local-kind --config test-cases/scylla-operator/functional.yaml --logdir="`pwd`/kind-logs"
```

While it runs, the kubeconfig is at `<logdir>/latest/functional-*/.kube/config`. If setup hangs, look at
pods in the `xfs-disk-setup` and `local-csi-driver` namespaces first, then `kubectl -n scylla logs <pod> -c scylla`
(add `--previous` for a crash-looping pod).

The kind cluster stays up after a run. Add `SCT_REUSE_CLUSTER=1` to run the next test against it without
the bring-up, and delete it with `/var/tmp/kind delete cluster` when done.

### EKS / GKE from your own machine

The hydra image ships the tools these backends shell out to. Outside hydra, install them yourself:

| Tool | Needed for | Notes |
|---|---|---|
| `docker` | both | gcloud runs inside a Cloud SDK container |
| `kubectl`, `helm` | both | |
| `aws` CLI | `k8s-eks` | kubeconfig tokens come from `aws eks get-token` |
| `eksctl` | `k8s-eks` | use the `EKSCTL_VERSION` pinned in `Dockerfile` |
| `gke-gcloud-auth-plugin` | `k8s-gke` | kubectl auth plugin; the `gcloud` snap can't add components, so install the `google-cloud-cli-gke-gcloud-auth-plugin` package or the standalone binary |

AWS credentials that can read the SCT KeyStore are needed for both (GCE credentials come from there too).

**AWS permissions for `k8s-eks`.** Besides EKS itself, SCT creates per-cluster IAM objects through `eksctl`:
an IAM OIDC provider and IAM roles for service accounts (EBS CSI driver, Scylla Manager S3 backups),
which teardown removes again. The AWS user needs:

| Permissions | Used for |
|---|---|
| `eks:*` on the test clusters, `iam:PassRole` for `eks_role_arn` / `eks_nodegroup_role_arn` | cluster, node groups, addons, access entries |
| `iam:CreateOpenIDConnectProvider`, `iam:TagOpenIDConnectProvider`, `iam:DeleteOpenIDConnectProvider` | OIDC provider (`eksctl utils associate-iam-oidc-provider`) |
| `iam:CreateRole`, `iam:TagRole`, `iam:AttachRolePolicy`, `iam:DetachRolePolicy`, `iam:DeleteRole`, `cloudformation:*` | service account roles (`eksctl create iamserviceaccount` runs CloudFormation stacks) |

Without them setup stops with `AccessDenied ... iam:CreateOpenIDConnectProvider`, or teardown leaves roles behind.
In the SCT AWS account CI runs as the `qa` user; to get the same rights, attach the account's
`CreateOpenIDConnectProvider`, `iamTagOpenIDConnectProvider`, `iam,delete,open,id,connect.provider`, `iamDeleteRole`
and `iamDetachRolePolicy` policies to your user (the `Kubernetes` and `QA` groups cover the rest).
`k8s-gke` needs no extra cloud permissions: its GCE service account comes from the KeyStore.

**Network: SCT must reach the Scylla pod IPs.** Cluster bring-up only needs the public K8S API endpoint, so it works
from anywhere, but SCT then opens CQL connections to the Scylla pods' IPs, which live inside the cloud VPC
(i.e. `10.4.x.x` on EKS, `10.88.x.x` on GKE). From a laptop outside the VPC, setup ends with
`Tried connecting to [('10.88.6.7', 9042)]. Last error: timed out`. `ip_ssh_connections: public` doesn't help here:
pods have no public IPs. So for EKS/GKE either run SCT on a machine inside the VPC, or launch from your machine
with hydra executing on an SCT runner in the cloud, as CI does (`--execute-on-new-runner` creates one and stores
its IP; later runs use `--execute-on-runner <ip>` to reuse it; remove the runner when done):
```bash
hydra --execute-on-new-runner run-pytest functional_tests/scylla_operator --backend k8s-gke \
  --config test-cases/scylla-operator/functional.yaml
```
Local kind has no such limit, which makes it the place to iterate; move to EKS/GKE once kind is green.

Run one test while keeping the cluster, then reuse it by its test id and clean up when done:
```bash
SCT_POST_BEHAVIOR_K8S_CLUSTER=keep PYTEST_ADDOPTS="-k test_scylla_operator_pods" \
  ./sct.py run-pytest functional_tests/scylla_operator/test_functional.py \
  --backend k8s-gke --config test-cases/scylla-operator/functional.yaml --logdir="`pwd`/gke-logs"
SCT_REUSE_CLUSTER=$(cat gke-logs/latest/test_id) SCT_POST_BEHAVIOR_K8S_CLUSTER=keep PYTEST_ADDOPTS="-k <next test>" \
  ./sct.py run-pytest ...  # same arguments
./sct.py clean-resources --backend k8s-gke --test-id $(cat gke-logs/latest/test_id)
```
A kept cluster keeps costing money until cleaned. GKE zones can run out of capacity (`GCE_STOCKOUT`);
pick another zone with `SCT_AVAILABILITY_ZONE`.

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
