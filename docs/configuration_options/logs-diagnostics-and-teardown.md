# Logs, diagnostics and teardown

[← All configuration options](configuration_options.md)

How logs and diagnostics are collected, and what happens to the resources when the test ends.

**13 options.** Jump to: [collect_logs](#collect_logs) · [collect_nvme_diagnostics](#collect_nvme_diagnostics) · [execute_post_behavior](#execute_post_behavior) · [logs_transport](#logs_transport) · [nvme_self_test_type](#nvme_self_test_type) · [post_behavior_db_nodes](#post_behavior_db_nodes) · [post_behavior_dedicated_host](#post_behavior_dedicated_host) · [post_behavior_emr_cluster](#post_behavior_emr_cluster) · [post_behavior_k8s_cluster](#post_behavior_k8s_cluster) · [post_behavior_loader_nodes](#post_behavior_loader_nodes) · [post_behavior_monitor_nodes](#post_behavior_monitor_nodes) · [post_behavior_vector_store_nodes](#post_behavior_vector_store_nodes) · [teardown_validators](#teardown_validators)


## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** False

**type:** bool


## **collect_nvme_diagnostics** / SCT_COLLECT_NVME_DIAGNOSTICS

Collect NVMe SMART logs, error logs, and self-test results from DB nodes during test teardown. Requires nvme-cli to be installed on the nodes. Skipped gracefully on backends without NVMe devices.

**default:** False

**type:** bool


## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** False

**type:** bool


## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** vector

**type:** Literal['ssh', 'docker', 'syslog-ng', 'vector']

**backend overrides:**
- `docker`: docker


## **nvme_self_test_type** / SCT_NVME_SELF_TEST_TYPE

NVMe device self-test type to run: 1 (short, ~2 min) or 2 (extended, may take hours). Only used when collect_nvme_diagnostics is enabled. Honored only on controllers that advertise Device Self-test support (Identify Controller OACS bit 4); unsupported controllers are skipped without issuing the command. This has no effect on AWS: neither instance-store (Nitro SSD) nor EBS implements Device Self-test, so on AWS the diagnostics rely on SMART counters and the error log instead.

**default:** 1

**type:** int


## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_dedicated_host** / SCT_POST_BEHAVIOR_DEDICATED_HOST

Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.<br><br>'destroy' - Destroy hosts (default)<br>'keep' - Keep hosts allocated

**default:** N/A

**type:** Literal['keep', 'destroy']

**backend overrides:**
- `destroy`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **post_behavior_emr_cluster** / SCT_POST_BEHAVIOR_EMR_CLUSTER

Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.<br><br>'destroy' - Destroy EMR cluster (default)<br>'keep' - Keep EMR cluster running<br>'keep-on-failure' - Keep EMR cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **post_behavior_vector_store_nodes** / SCT_POST_BEHAVIOR_VECTOR_STORE_NODES

Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


## **teardown_validators** / SCT_TEARDOWN_VALIDATORS

Validators to use during teardown phase

**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}, 'nvme': {'enabled': False}}

**type:** dict | YAML/JSON string → dict
