# Lab builders as baremetal test nodes, reserved through Jenkins

**Jira:** SCT-900

## Problem

The lab machines, the `spider*` and `minipc*` hosts, are Jenkins agents
already. We want to run SCT performance tests on them as DB, loader, and
monitor nodes instead of on rented cloud VMs. Today nothing connects the
Jenkins agents and the SCT baremetal backend:

- Nothing lets a test claim N lab hosts. A dtest or SCT job can start on a
  host while our test uses it.
- The baremetal backend reads a hosts file with the address of each host.
  Somebody writes that file by hand and keeps it in sync with the free hosts.
- The baremetal example pipeline still creates an AWS SCT runner. That runner
  cannot reach hosts on the lab network.
- A missing SSH key or missing passwordless sudo on a host shows only about
  20 minutes into the run, during node init.
- The perf demo needs 9 hosts for one sub-test (3 DB, 4 loaders, 1 monitor,
  and the runner). The lab pool is small, and each host that we hold is a
  host that dtest cannot use.

## Who it affects

- The performance tests of epic SCT-898 (Running Performance tests on Bare
  Metal), first `perf-regression-predefined-throughput-steps`.
- `perfRegressionParallelPipeline` and the jobs that use it.
- The dtest and minicloud jobs that share the `spider*` and `minipc*` agents.
- The owners of those hosts, because a test changes the host state.

## Evidence

- SCT-901 ran the baremetal backend end to end on simulated hosts (Fedora 44
  on EC2) and left this question open: "which Spider hosts are allocated to
  this effort, and are they labelled in Jenkins?"
- SCT-901 found that a host that a previous run left dirty does not start
  Scylla. Host cleanup must run after each run. After its first run, a host
  keeps `SELINUX=disabled`, because `scylla_setup` writes it.
- The lab agents on the live Jenkins:

  | Hosts | Executors | Mode | Labels (today) |
  |---|---|---|---|
  | `spider*.cloudius-systems.com` | 1 | `EXCLUSIVE` | `builders_x86 dtest_local` |
  | `minipc*.cloudius-systems.com` | 1 | `NORMAL` | `dtest_local dtest_ci_local minicloud-kvm-builders-*` |

- The proof of concept, PR #16346, held two AWS builder agents and reached
  them over SSH from a third
  ([results](https://github.com/scylladb/scylla-cluster-tests/pull/16346#issuecomment-6016860369)):

  | Build | What it tested | Result |
  |---|---|---|
  | [#2](https://jenkins.scylladb.com/job/scylla-staging/job/fruch/job/performance_staging/job/lab-nodes-hold-poc-test/2/) | Full run, release with the **Release** button | `SUCCESS`. Both executors were freed when Release was pressed. |
  | [#3](https://jenkins.scylladb.com/job/scylla-staging/job/fruch/job/performance_staging/job/lab-nodes-hold-poc-test/3/) | Release by **abort** | `ABORTED` within 1s; both held executors empty afterwards. |
  | [#4](https://jenkins.scylladb.com/job/scylla-staging/job/fruch/job/performance_staging/job/lab-nodes-hold-poc-test/4/) | Release by **hold timeout** (`hold_minutes=2`) | `ABORTED` by the timeout; both held executors empty afterwards. |
  | [#5](https://jenkins.scylladb.com/job/scylla-staging/job/fruch/job/performance_staging/job/lab-nodes-hold-poc-test/5/) | Full run again, after the `quiet: true` change | `SUCCESS`. Released 11s after Release was pressed; no polling noise in the log. |

  From the same results: "hydra, run from the checkout, synced
  `scylla_test_id_ed25519` and read the hosts file back through
  `KeyStore.get_baremetal_config`". The same run on lab hosts is not done yet.

## Definition of done

1. A perf pipeline run in lab mode reserves exactly as many lab hosts as the
   test configuration asks for, per role.
2. While a host is reserved, Jenkins starts no other build on it. The host
   goes back to the pool on success, failure, abort, and timeout.
3. The run makes the baremetal hosts file from the reserved hosts, with no
   hand edits. The file is a build artifact, and post-run cleanup can read it.
4. A host with a missing key, sudo, or docker fails the run before the test
   starts. One report lists every problem on every host.
5. The `predefined-throughput-steps` demo reaches its stress phase on lab
   hosts and finishes cleanly.
6. Over the staging runs, no other Jenkins build ran on a host while it was
   reserved.
7. Cloud perf jobs behave as before.

## Out of scope

- Nemesis tests on lab hosts. The baremetal backend cannot reboot a node.
- A reboot of each host to a clean boot disk. The hosts do not have a
  separate boot disk yet.
- Lab and cloud nodes in one cluster.
- Hosts that are not Jenkins agents.
