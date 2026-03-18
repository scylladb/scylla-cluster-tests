---
status: draft
domain: ci-cd
created: 2026-03-18
last_updated: 2026-03-18
owner: null
---

# Jenkins Pipeline Cluster Reuse

## Problem Statement

SCT's cluster reuse feature (`SCT_REUSE_CLUSTER`) is currently designed for local development workflows only — developers run a test with `post_behavior_*=keep`, then manually set `SCT_REUSE_CLUSTER=<test_id>` to re-run tests on the same cluster without re-provisioning. This saves significant time during development iterations.

Jenkins pipelines do not support this workflow. Every Jenkins run creates a new SCT runner, provisions fresh cloud resources, and tears everything down at the end — even when the user wants to keep resources for a subsequent run. When `post_behavior_*` is set to `keep`, the test cluster nodes survive, but the **SCT runner itself is always terminated** by the `cleanSctRunners` stage. The next Jenkins build must then create a new runner, re-provision from scratch, and rediscover the kept cluster — a process that is neither automated nor supported in the pipeline logic.

The goal is to extend Jenkins pipelines so that:
1. When `post_behavior` is set to `keep`, the SCT runner is preserved alongside the test cluster
2. A subsequent Jenkins build can reuse both the existing SCT runner and the kept cluster
3. The provisioning stage is skipped when reusing a cluster

This will enable CI-driven workflows where clusters are intentionally kept alive across multiple Jenkins runs, useful for:
- Re-running tests on an existing cluster after SCT code changes
- Multi-phase test campaigns (e.g., run longevity, then run upgrade on the same cluster)
- Debugging failed tests without re-provisioning infrastructure

## Current State

### Cluster Reuse in Local Development

The cluster reuse mechanism is documented in `docs/reuse_cluster.md` and works as follows:

1. **Initial run**: User sets `SCT_POST_BEHAVIOR_DB_NODES=keep`, `SCT_POST_BEHAVIOR_LOADER_NODES=keep`, `SCT_POST_BEHAVIOR_MONITOR_NODES=keep` and runs a test
2. **Reuse run**: User sets `SCT_REUSE_CLUSTER=<test_id>` from the initial run
3. **SCT behavior**: When `reuse_cluster` is set in `sdcm/sct_config.py` (line 753), the framework looks up existing cloud instances by `TestId` tag, skips provisioning, and reconnects to the existing cluster

The `TestConfig` class in `sdcm/test_config.py` (line 50) maintains a class-level `REUSE_CLUSTER = False` flag that controls whether provisioning is skipped.

### SCT Runner Lifecycle in Jenkins

The SCT runner is a dedicated VM that executes the test. Its lifecycle in Jenkins is:

1. **Creation** (`vars/createSctRunner.groovy`): Calls `hydra create-runner-instance` which invokes `sct.py create-runner-instance` (line 2085). The runner is tagged with `TestId`, `keep` (hours to live), and `keep_action=terminate`. The runner IP is written to `sct_runner_ip` file.

2. **Usage**: All subsequent stages (`provisionResources`, `runSctTest`, `runCollectLogs`, `runCleanupResource`) read `sct_runner_ip` and execute commands on the runner via `--execute-on-runner`.

3. **Cleanup** (`vars/cleanSctRunners.groovy`): Calls `hydra clean-runner-instances` which invokes `sdcm/sct_runner.py:clean_sct_runners()` (line 1966). This function manages the runner's `keep` tag and eventually terminates the runner based on elapsed time.

### Runner Tag-Based Lifecycle Management

The `_manage_runner_keep_tag_value()` function in `sdcm/sct_runner.py` (line 1933) controls runner termination:

- **Logs collected, no timeout**: Sets `keep=0, keep_action=terminate` → runner terminates immediately
- **Timeout + logs collected**: Reduces `keep` value but adds 6-hour buffer
- **Logs not collected**: Extends `keep` by 48 hours for debugging

The `clean_sct_runners()` function (line 1966) checks:
- Skip if `keep` tag contains `"alive"`
- Skip if `keep_action != "terminate"`
- Skip if elapsed time < `keep` hours
- Otherwise: terminate

### Pipeline Stage Flow (longevityPipeline.groovy)

```
Create SCT Runner → Provision Resources → Run SCT Test → Collect Logs → Clean Resources → Clean SCT Runners
```

The `Clean SCT Runners` stage (`vars/cleanSctRunners.groovy`, line 5) **always runs** in the pipeline post section, regardless of `post_behavior` settings. This means the runner is terminated even when cluster resources are intentionally kept alive.

### Existing Reuse in Jenkinsfile (PR Provision Tests Only)

The main `Jenkinsfile` (line 394) has a limited reuse flow for PR provision tests only:

```groovy
if (pullRequestContainsLabels("test-provision-${backend}-reuse")) {
    withEnv(["SCT_REUSE_CLUSTER=${env.SCT_TEST_ID}"]) {
        runSctTest(curr_params, builder.region, ...)
    }
}
```

This reuses the cluster **within the same Jenkins build** — it does not preserve the runner or cluster for a subsequent build. The runner is still cleaned up at the end.

### Resource Cleanup and Post Behavior

The `runCleanupResource.groovy` (line 3) calls `hydra clean-resources --post-behavior --test-id $SCT_TEST_ID`, which invokes `clean_resources_according_post_behavior()` in `sdcm/utils/resources_cleanup.py` (line 576). This function respects `post_behavior` settings for cluster nodes but has no interaction with the SCT runner lifecycle.

## Goals

1. **Preserve SCT runner when post_behavior is `keep`**: When all `post_behavior_*` settings are `keep`, the SCT runner should not be terminated, allowing it to be reused in a subsequent Jenkins build
2. **Enable cross-build cluster reuse**: A subsequent Jenkins build should be able to specify `SCT_REUSE_CLUSTER=<test_id>` and find + connect to both the existing SCT runner and the kept cluster
3. **Skip provisioning on reuse**: When reusing a cluster, the `Provision Resources` stage should be skipped entirely
4. **Support reuse in longevity pipeline**: Extend `longevityPipeline.groovy` (and other major pipelines) with a `reuse_cluster` parameter and conditional stage logic
5. **Extend runner tag management**: Add a `keep=alive` tag option so that runners explicitly marked for reuse are never terminated by the periodic cleanup
6. **Provide reuse_cluster parameter in Jenkins UI**: Allow users to input a `test_id` for cluster reuse when triggering a Jenkins build

## Implementation Phases

### Phase 1: Preserve SCT Runner When Post Behavior Is Keep

**Objective**: Prevent the SCT runner from being terminated when all `post_behavior_*` settings indicate resources should be kept.

**Implementation**:

1. **Modify `vars/cleanSctRunners.groovy`**: Before calling `hydra clean-runner-instances`, check if all `post_behavior_*` params are set to `keep`. If so, update the runner's `keep` tag to `alive` instead of terminating it.

   ```groovy
   def shouldKeepRunner = (
       params.post_behavior_db_nodes == 'keep' &&
       params.post_behavior_loader_nodes == 'keep' &&
       params.post_behavior_monitor_nodes == 'keep'
   )

   if (shouldKeepRunner) {
       // Update runner tag to keep=alive instead of terminating
       sh """
           ./docker/env/hydra.sh update-runner-tags \
               --runner-ip \${RUNNER_IP} \
               --backend "$cloud_provider" \
               --tags '{"keep": "alive", "keep_action": "none"}'
       """
   } else {
       // Normal cleanup
       sh """
           ./docker/env/hydra.sh clean-runner-instances \
               --test-status "$test_status" \
               --runner-ip \${RUNNER_IP} \
               --backend "$cloud_provider"
       """
   }
   ```

2. **Add `update-runner-tags` CLI command to `sct.py`**: Create a new CLI command that wraps `update_sct_runner_tags()` from `sdcm/sct_runner.py` (line 1905) to allow updating runner tags from the pipeline. Alternatively, the existing function could be extended with a `--keep-alive` flag on the `clean-runner-instances` command.

3. **Verify `keep=alive` behavior**: The existing `clean_sct_runners()` function already skips runners with `keep=alive` (line 2014 in `sdcm/sct_runner.py`). Verify this path works correctly when invoked from periodic cleanup jobs.

**Definition of Done**:
- [ ] `cleanSctRunners.groovy` conditionally preserves runner when `post_behavior_*=keep`
- [ ] Runner tagged with `keep=alive` survives periodic cleanup
- [ ] `sct.py` has a mechanism to update runner tags from pipeline (new command or flag)
- [ ] Unit tests for the tag-update logic
- [ ] Manual test: Run longevity pipeline with `post_behavior_*=keep`, verify runner is not terminated

**Dependencies**: None

---

### Phase 2: Add `reuse_cluster` Parameter to Pipeline Configurations

**Objective**: Allow users to specify a `test_id` for cluster reuse when triggering a Jenkins build.

**Implementation**:

1. **Add `reuse_cluster` parameter to pipeline groovy files**: Add the parameter to the `parameters` block of `longevityPipeline.groovy`, `managerPipeline.groovy`, `rollingUpgradePipeline.groovy`, `jepsenPipeline.groovy`, `perfRegressionParallelPipeline.groovy`, and other pipeline definitions.

   ```groovy
   // In parameters block:
   string(defaultValue: '',
          description: 'Test ID of an existing cluster to reuse. When set, provisioning is skipped and the existing cluster is used.',
          name: 'reuse_cluster')
   ```

2. **Export `SCT_REUSE_CLUSTER` in `vars/runSctTest.groovy`**: When `reuse_cluster` param is set, export it as an environment variable before running the test.

   ```groovy
   if [[ -n "${params.reuse_cluster ? params.reuse_cluster : ''}" ]] ; then
       export SCT_REUSE_CLUSTER="${params.reuse_cluster}"
   fi
   ```

3. **Export `SCT_REUSE_CLUSTER` in `vars/provisionResources.groovy`**: Pass the reuse_cluster parameter through so the provisioning command can detect it and behave appropriately.

**Definition of Done**:
- [ ] `reuse_cluster` parameter added to all major pipeline definitions
- [ ] `runSctTest.groovy` exports `SCT_REUSE_CLUSTER` when the parameter is set
- [ ] `provisionResources.groovy` exports `SCT_REUSE_CLUSTER` when the parameter is set
- [ ] Jenkins UI shows `reuse_cluster` field when triggering a build

**Dependencies**: None (can be done in parallel with Phase 1)

---

### Phase 3: Conditional Stage Execution for Reuse

**Objective**: Skip provisioning and conditionally skip runner creation when reusing a cluster.

**Implementation**:

1. **Skip `Provision Resources` stage**: In pipeline groovy files, wrap the provisioning stage with a condition:

   ```groovy
   stage('Provision Resources') {
       when {
           expression { return !params.reuse_cluster }
       }
       steps {
           // existing provisioning logic
       }
   }
   ```

2. **Reuse existing SCT runner**: When `reuse_cluster` is set, look up the SCT runner associated with that `test_id` instead of creating a new one. This requires:

   a. **Add `find-runner-instance` CLI command to `sct.py`**: A new command that finds an existing runner by `test_id`, verifies it's running, and writes its IP to `sct_runner_ip`.

   ```python
   @cli.command("find-runner-instance", help="Find an existing SCT runner by test ID")
   @click.option("-t", "--test-id", required=True, type=str)
   @click.option("-b", "--backend", type=str)
   def find_runner_instance(test_id, backend):
       runners = list_sct_runners(backend=backend, test_id=test_id)
       if not runners:
           LOGGER.error("No SCT runner found for test_id: %s", test_id)
           sys.exit(1)
       runner = runners[0]
       runner_ip = runner.public_ips[0]
       sct_runner_ip_path = Path("sct_runner_ip")
       sct_runner_ip_path.write_text(runner_ip)
       LOGGER.info("Found SCT Runner at %s for test %s", runner_ip, test_id)
   ```

   b. **Modify `vars/createSctRunner.groovy`** (or create `vars/findOrCreateSctRunner.groovy`): Add conditional logic:

   ```groovy
   if (params.reuse_cluster) {
       // Find existing runner
       sh """
           ./docker/env/hydra.sh find-runner-instance \
               --test-id ${params.reuse_cluster} \
               --backend ${cloud_provider}
       """
   } else {
       // Create new runner (existing logic)
       sh """
           ./docker/env/hydra.sh create-runner-instance ...
       """
   }
   ```

3. **Update runner's `TestId` tag on reuse**: When reusing a runner, update its `TestId` tag to the new test's ID so that cleanup and log collection work correctly.

4. **Extend runner `keep` time on reuse**: Reset the `keep` tag to extend the runner's lifetime for the new test duration.

**Definition of Done**:
- [ ] `find-runner-instance` CLI command implemented in `sct.py`
- [ ] `createSctRunner.groovy` finds existing runner when `reuse_cluster` is set
- [ ] `Provision Resources` stage skipped when `reuse_cluster` is set
- [ ] Runner's `TestId` and `keep` tags updated for the new run
- [ ] Unit tests for `find-runner-instance` command
- [ ] Integration test: Reuse flow works end-to-end in provision test pipeline

**Dependencies**: Phase 1 (runner must be preserved first), Phase 2 (parameter must exist)

---

### Phase 4: Cleanup Behavior Adaptation for Reused Clusters

**Objective**: Ensure cleanup stages behave correctly when a cluster has been reused.

**Implementation**:

1. **Conditional resource cleanup**: When `reuse_cluster` is set, the `runCleanupResource.groovy` should still call `clean-resources --post-behavior` — the existing `post_behavior` settings will control whether resources are destroyed or kept. No change is needed in the cleanup logic itself, but the interaction needs to be validated.

2. **Runner cleanup on reuse**: The `cleanSctRunners.groovy` should check whether the current run's `post_behavior` settings indicate keep. If so, preserve the runner (Phase 1 behavior). If `post_behavior=destroy`, the runner should be terminated as usual even if it was originally a reused runner.

3. **Log collection compatibility**: Verify that `runCollectLogs.groovy` works correctly when the runner was reused (different `test_id` from the one the runner was originally created with). The `SCT_TEST_ID` environment variable should reference the current test, not the original one.

4. **Argus integration**: Verify that `finishArgusTestRun` correctly records the reused test run with the correct test ID and links to the original run.

**Definition of Done**:
- [ ] Resource cleanup respects `post_behavior` when cluster was reused
- [ ] Runner preserved/destroyed based on current run's `post_behavior`, not original
- [ ] Log collection works with reused runners
- [ ] Argus records reuse relationship correctly
- [ ] Manual test: Full reuse cycle (create → keep → reuse → destroy)

**Dependencies**: Phase 1, Phase 2, Phase 3

---

### Phase 5: Documentation and Safety Guardrails

**Objective**: Document the Jenkins reuse workflow and add safety mechanisms.

**Implementation**:

1. **Update `docs/reuse_cluster.md`**: Add a "Jenkins Pipeline Reuse" section documenting:
   - How to trigger a reuse run from Jenkins UI
   - Which parameters to set
   - How to find the `test_id` of a previous run
   - Limitations and caveats

2. **Runner expiry safety**: Add a maximum `keep=alive` duration (e.g., 7 days). The periodic cleanup should terminate `alive` runners that exceed this threshold. Implement this in `clean_sct_runners()`.

   ```python
   MAX_ALIVE_HOURS = 168  # 7 days
   if "alive" in str(sct_runner_info.keep):
       if sct_runner_info.launch_time:
           elapsed = (utc_now - sct_runner_info.launch_time).total_seconds() / 3600
           if elapsed > MAX_ALIVE_HOURS:
               LOGGER.warning("Runner %s exceeded max alive time (%dh). Terminating.", ...)
               sct_runner_info.terminate()
       continue
   ```

3. **Pipeline validation**: When `reuse_cluster` is set but no matching runner/cluster is found, fail early with a clear error message instead of proceeding with a broken state.

4. **Jenkins UI hints**: Add clear descriptions to the `reuse_cluster` parameter explaining the prerequisites (original run must have used `post_behavior_*=keep`).

**Definition of Done**:
- [ ] `docs/reuse_cluster.md` updated with Jenkins reuse section
- [ ] Runner expiry safety implemented in `clean_sct_runners()`
- [ ] Early validation and clear error messages for missing runners/clusters
- [ ] Unit tests for runner expiry logic
- [ ] All reuse-related parameters have helpful descriptions

**Dependencies**: Phase 3, Phase 4

## Testing Requirements

### Unit Tests
- `_manage_runner_keep_tag_value()` with `keep=alive` tag behavior
- `clean_sct_runners()` skips runners with `keep=alive` (already tested, verify)
- `clean_sct_runners()` terminates `alive` runners past maximum age
- `find-runner-instance` CLI command with mock runners
- `update-runner-tags` CLI command with mock runners

### Integration Tests
- **Needs Investigation**: Determine if Jenkins pipeline tests can be run in a sandbox environment. Currently, pipeline logic is only tested via PR provision tests triggered by labels.

### Manual Tests
- Run longevity pipeline with `post_behavior_*=keep`, verify runner survives
- Trigger second build with `reuse_cluster=<test_id_from_first_run>`, verify it finds the runner and skips provisioning
- Verify cleanup works correctly after reuse (both keep and destroy scenarios)
- Verify reuse across different pipeline types (longevity, manager, upgrade)
- Verify runner expiry safety terminates stale runners

## Success Criteria

1. A Jenkins user can run a longevity test with `post_behavior_*=keep`, then start a second Jenkins build with `reuse_cluster=<test_id>` that reuses both the SCT runner and the test cluster
2. The second build skips the `Provision Resources` stage, reducing build time by ~30 minutes
3. Stale runners with `keep=alive` are automatically cleaned up after 7 days
4. The workflow is documented and discoverable via Jenkins UI parameter descriptions
5. No regression in the existing cleanup and post-behavior logic

## Risk Mitigation

### Resource Leaks
**Risk**: Runners tagged `keep=alive` could accumulate if cleanup fails or users forget to destroy them.
**Mitigation**: Phase 5 introduces a maximum alive duration (7 days). The periodic cleanup job will terminate runners exceeding this threshold. Additionally, cost monitoring dashboards should flag long-running runners.

### Runner State Corruption
**Risk**: A reused runner may have stale state (old test artifacts, filled disk, conflicting configurations) that causes the second test to fail.
**Mitigation**: The reuse flow should run a lightweight health check on the runner (SSH connectivity, disk space) before proceeding. If the check fails, fall back to creating a new runner.

### Test ID Conflicts
**Risk**: When a runner's `TestId` tag is updated for the new run, log collection and cleanup for the original test could be disrupted.
**Mitigation**: Either preserve the original `TestId` as a secondary tag (e.g., `OriginalTestId`), or ensure that log collection has already completed before the tag is updated.

### Backend-Specific Behavior
**Risk**: Runner reuse may work on AWS but fail on GCE, Azure, or OCI due to different instance lifecycle semantics.
**Mitigation**: Initially scope the feature to AWS only (most common backend), then extend to other backends in subsequent iterations.

### Jenkins Workspace Conflicts
**Risk**: When reusing a runner, the Jenkins workspace on the runner may contain files from the previous run that interfere with the new run.
**Mitigation**: The `runSctTest.groovy` already runs `rm -fv ./latest` before tests. Verify this is sufficient; if not, add a workspace cleanup step at the beginning of the reuse flow.
