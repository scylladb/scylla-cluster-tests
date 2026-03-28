## Reusing an already running cluster

### Overview

SCT allows running tests using a previously provisioned cluster. This helps in efficiently utilizing resources and reduces setup time for subsequent tests.

Reusing a cluster is primarily intended for local development and debugging. Scenarios that can benefit from it include:
- re-run a test without having to wait for a new cluster to be provisioned
- re-run a test with updated (or different) configuration
- update Scylla packages on an existing cluster and re-run the test

### Prerequisites

To provision a cluster for later reuse, the initial test run must be executed with the `post_behavior` configuration parameters set to `keep` value.

This can be done in the test configuration YAML file as:
```yaml
post_behavior_db_nodes: "keep"
post_behavior_loader_nodes: "keep"
post_behavior_monitor_nodes: "keep"
```
For K8s cluster the configuration parameter to use is:
```yaml
post_behavior_k8s_cluster: "keep"
```
Alternatively, `SCT_POST_BEHAVIOR` environment variables can be set before running the test, for each node type to be kept:
```bash
export SCT_POST_BEHAVIOR_DB_NODES=keep
export SCT_POST_BEHAVIOR_LOADER_NODES=keep
export SCT_POST_BEHAVIOR_MONITOR_NODES=keep
```
For K8S cluster the environment variable to use is:
```bash
export SCT_POST_BEHAVIOR_K8S_CLUSTER=keep
```

### Configuration

Specify a cluster for reuse by adding its identifier to the SCT test configuration.
The identifier can be found in a few places:
- Jenkins logs of a build that provisioned the cluster - look for `test_id` attribute
- test run details in Argus - look for `Id` attribute on `Details` tab of the run
- console of a backend (AWS/GCE/Azure) where the cluster is provisioned - look for `TestId` tag of a cluster instance
- if the initial test run was executed locally, look for `test_id` attribute in test results inventory, e.g. `cat ~/sct-results/latest/test_id`

To specify which cluster to reuse set `reuse_cluster` parameter to `test_id` value in test configuration YAML file:
```yaml
reuse_cluster: 7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84
```
Alternatively, set `SCT_REUSE_CLUSTER` environment variable before running the test:
```bash
export SCT_REUSE_CLUSTER=7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84
```

### Example scenario of reusing a cluster

Example demonstrates reusing a cluster for updating Scylla packages on DB nodes and then re-running the initial test:
```bash
# initial test run
export SCT_POST_BEHAVIOR_DB_NODES=keep
export SCT_POST_BEHAVIOR_LOADER_NODES=keep
export SCT_POST_BEHAVIOR_MONITOR_NODES=keep
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/longevity/longevity-10gb-3h.yaml --config configurations/network_config/test_communication_public.yaml

# reuse the cluster and update scylla packages (the new packages are placed on a local machine)
export SCT_REUSE_CLUSTER=$(cat ~/sct-results/latest/test_id)
hydra update-scylla-packages --test-id $SCT_REUSE_CLUSTER -p ~/new_scylla_packages --backend aws

# reuse the cluster and re-run the initial test
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/longevity/longevity-10gb-3h.yaml --config configurations/network_config/test_communication_public.yaml
```

### Jenkins pipeline reuse

Cluster reuse is also supported in Jenkins pipelines. This allows re-running a test against an existing cluster directly from the Jenkins UI, without provisioning new infrastructure.

#### How it works

When a Jenkins build runs with `post_behavior_*` parameters set to `keep`, the test cluster and the SCT runner VM are both preserved after the build completes. A subsequent build can then reuse the preserved cluster and runner by specifying the original run's `test_id` in the `reuse_cluster` parameter.

The reuse flow:
1. **SCT runner**: instead of creating a new runner VM, the pipeline looks up the runner from the original test by its `test_id` and reuses it
2. **Provisioning**: the `Provision Resources` stage is skipped entirely
3. **Test execution**: the test runs against the existing cluster using `SCT_REUSE_CLUSTER`
4. **Cleanup**: post-test cleanup respects the current run's `post_behavior` settings — set them to `keep` again to preserve the cluster for another reuse, or `destroy` to tear everything down

#### Step-by-step

1. **Run the initial build with `post_behavior_*=keep`**

   In the Jenkins build parameters, set:
   - `post_behavior_db_nodes` = `keep`
   - `post_behavior_loader_nodes` = `keep`
   - `post_behavior_monitor_nodes` = `keep`

   This preserves both the test cluster and the SCT runner after the build finishes.

2. **Find the test ID**

   The `test_id` of the completed build can be found in:
   - The Argus link in the Jenkins build description (the UUID in the URL)
   - Jenkins build logs — search for `test_id`
   - The `SCT_TEST_ID` value shown in the build's environment variables

3. **Start a reuse build**

   Trigger a new build of the same pipeline (or a compatible one). In the build parameters:
   - Set `reuse_cluster` to the `test_id` from step 2
   - Set `post_behavior_*` to `keep` if you plan to reuse again, or `destroy` to clean up

4. **Verify the reuse**

   In the build log, look for:
   - `"Reuse mode: looking up existing SCT runner"` — confirms runner reuse
   - `"Cluster reuse mode: skipping resource provisioning"` — confirms provisioning was skipped

#### Supported pipelines

The `reuse_cluster` parameter is available in the following pipeline types:
- Longevity (`longevityPipeline`)
- Manager (`managerPipeline`)
- Rolling Upgrade (`rollingUpgradePipeline`)
- Jepsen (`jepsenPipeline`)
- Performance Regression (`perfRegressionParallelPipeline`)

#### Safety: runner expiry

Preserved runners are tagged with a numeric `keep` value (hours from VM launch time) that acts as a safety ceiling. The existing cleanup logic automatically terminates runners once the elapsed time since launch exceeds this value (default: 120 hours / 5 days).

If a runner expires between builds, the reuse build will fail with an error indicating no runner was found. In that case, run a fresh build without `reuse_cluster`.

#### Limitations

- **Same backend required**: the reuse build must use the same cloud backend as the original build
- **Same pipeline type recommended**: while cross-pipeline reuse is technically possible, it is only reliable when both pipelines use compatible test configurations
- **No partial reuse**: you cannot reuse only the DB nodes and create new loaders — the entire test environment is reused
- **Runner state**: a reused runner may have artifacts from the previous run; if the reuse build fails due to runner issues, run a fresh build

### Scylla Cloud (xcloud) backend

When reusing clusters deployed in Scylla Cloud backend, the test environment consists of:
- **Scylla Cloud cluster itself**: managed DB and Vector Search nodes (operated via Scylla Cloud API)
- **Loader and monitor nodes**: regular AWS/GCE instances

This adds additional step when identifying cluster resources to be reused.

#### Cluster Identification

The Scylla Cloud cluster to be reused is identified by searching for cluster where the test_id's first 8 characters appear in the cluster name.
This matches the naming convention used during cluster creation: `{user_prefix}-{test_id[:8]}`.

Example:
- Test ID: `7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84`
- Cluster name: `longevity-test-7c86f6de`
- Search pattern: `7c86f6de`

Loader and monitor nodes are identified by the `TestId` tag on the underlying AWS/GCE instances, following the same pattern as when reusing native AWS/GCE clusters.

NOTE: the identification logic may be updated in the future to use tags/metadata on Scylla Cloud clusters once that functionality is supported by the Siren/Scylla Cloud API.

#### Configuration

The same `reuse_cluster` test configuration parameter (as for other backends) is used for selecting existing cluster:

```yaml
reuse_cluster: 7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84
```

Or via environment variable:

```bash
export SCT_REUSE_CLUSTER=7c86f6de-f87d-45a8-9e7f-61fe7b7dbe84
```

#### Important Notes

**Partial reuse is not supported**: Reusing only the Scylla Cloud DB cluster without loader/monitor nodes, or vice versa, is not supported. The `reuse_cluster` parameter applies to the entire test environment.

When resuing a cluster SCT validates that the existing cluster configuration matches parameters defined in test configuration:
- DB nodes count must match `n_db_nodes` parameter
- Instance type must match `instance_type_db` parameter
- VS node count must match `n_vector_store_nodes` parameter
- Cluster status must be ACTIVE (not PROVISIONING, etc.)
```
