## Skip test stages in Scylla Cluster Tests

### Overview

SCT allows to skip certain stages of a test during its execution. By configuring specific stages to be skipped the flow of a test can be optimized - saving execution time and focusing on the most relevant parts of a test scenario.

Skipping test stages is useful in scenarios like:
- reusing an existing cluster. For example, the prepare stage can be skipped because the cluster is already populated with data
- focusing on specific stages of a test configuration. For example, Nemeses execution can be skipped if disruptions are not needed for a particular scenario
- resource optimization. Skipping unnecessary steps can reduce resource consumption and speed up test execution, which is often useful in a local development environment

For the moment, the following test stages can be skipped:
- prepare_write
- wait_total_space_used_per_node
- main_load
- post_test_load
- nemesis
- data_validation
- perf_preload_data: false
- perf_steady_state_calc: false

### Configuration

The test configuration parameter `skip_test_stages` is used to specify which test stages are to be skipped. It is a dictionary where keys are the names of test stages, and boolean values indicate whether the stage should be skipped.
The parameter can be set in a test configuration file or passed as an environment variable.

#### 1. Using configuration file

If `skip_test_stages` is to be defined in a test configuration, it can be included as a separate dedicated test configuration file, or as a part of existing configuration.

Example of defining test stages to skip in a configuration file:
```yaml
skip_test_stages:
  prepare_write: true
  nemesis: true
```

#### 2. Using SCT_SKIP_TEST_STAGES environment variable

Alternatively, `skip_test_stages` parameter can be set using the SCT_SKIP_TEST_STAGES environment variable:
```bash
export SCT_SKIP_TEST_STAGES='{"prepare_write": true, "nemesis": true}'
```

### Example scenario of skipping test stages

The following example demonstrates executing a longevity test with skipped nemesis stage execution:
```bash
# prepare a dedicated configuration file with only the 'skip_test_stages' parameter
cat <<EOF > configurations/skip_test_stages.yaml
skip_test_stages:
  nemesis: true
EOF

export SCT_SCYLLA_VERSION=6.1
export SCT_ENABLE_ARGUS=false

# run longevity test, passing the newly created configuration file
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config configurations/nemesis/longevity-5gb-1h-nemesis.yaml --config configurations/network_config/test_communication_public.yaml --config configurations/skip_test_stages.yaml

# or use environment variable to pass the 'skip_test_stages' parameter value to the test
export SCT_SKIP_TEST_STAGES='{"nemesis": true}'
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config configurations/nemesis/longevity-5gb-1h-nemesis.yaml --config configurations/network_config/test_communication_public.yaml

# later it can be checked in SCT log what was skipped
❯ grep -Ei 'test stage' ~/sct-results/latest/sct.log
< t:2024-10-09 15:35:22,108 f:decorators.py   l:362  c:sdcm.utils.decorators p:WARNING > 'add_nemesis' is skipped as 'nemesis' test stage(s) is disabled.
< t:2024-10-09 15:35:22,108 f:decorators.py   l:362  c:sdcm.utils.decorators p:WARNING > 'start_nemesis' is skipped as 'nemesis' test stage(s) is disabled.
< t:2024-10-09 15:37:02,931 f:decorators.py   l:362  c:sdcm.utils.decorators p:WARNING > 'stop_nemesis' is skipped as 'nemesis' test stage(s) is disabled.
```
**NOTE:** if there are multiple SCT directories in the local dev environment (e.g. as a result of using the `git worktree` tool) the `hydra` command is symlinked to the SCT directory where the `install-hydra.sh` script was executed.
In such a case, if a new test configuration is created, the `./docker/env/hydra.sh` script should be used directly, so that it picks up configurations files from the proper SCT directory. E.g.:
```bash
./docker/env/hydra.sh run-test longevity_test.LongevityTest.test_custom_time --backend aws --config configurations/nemesis/longevity-5gb-1h-nemesis.yaml --config configurations/network_config/test_communication_public.yaml --config configurations/skip_test_stages.yaml
```

### Ensuring Consistency

When skipping a test stage, it's crucial to ensure that its absence doesn't adversely affect subsequent test stages of the test scenario.
