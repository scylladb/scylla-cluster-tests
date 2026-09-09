# Logs, diagnostics and teardown

[← All configuration options](../configuration_options.md)

How logs and diagnostics are collected, and what happens to the resources when the test ends.

**11 options.**


<a id="collect_logs"></a>

## **collect_logs** / SCT_COLLECT_LOGS

Collect logs from instances and sct runner

**default:** N/A

**type:** bool


<a id="execute_post_behavior"></a>

## **execute_post_behavior** / SCT_EXECUTE_POST_BEHAVIOR

Run post behavior actions in sct teardown step

**default:** N/A

**type:** bool


<a id="logs_transport"></a>

## **logs_transport** / SCT_LOGS_TRANSPORT

How to transport logs: syslog-ng, ssh or docker

**default:** vector

**type:** Literal['ssh', 'docker', 'syslog-ng', 'vector']


<a id="post_behavior_db_nodes"></a>

## **post_behavior_db_nodes** / SCT_POST_BEHAVIOR_DB_NODES

Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


<a id="post_behavior_dedicated_host"></a>

## **post_behavior_dedicated_host** / SCT_POST_BEHAVIOR_DEDICATED_HOST

Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.<br><br>'destroy' - Destroy hosts (default)<br>'keep' - Keep hosts allocated

**default:** N/A

**type:** Literal['keep', 'destroy']


<a id="post_behavior_emr_cluster"></a>

## **post_behavior_emr_cluster** / SCT_POST_BEHAVIOR_EMR_CLUSTER

Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.<br><br>'destroy' - Destroy EMR cluster (default)<br>'keep' - Keep EMR cluster running<br>'keep-on-failure' - Keep EMR cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


<a id="post_behavior_k8s_cluster"></a>

## **post_behavior_k8s_cluster** / SCT_POST_BEHAVIOR_K8S_CLUSTER

Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.<br><br>'destroy' - Destroy k8s cluster and credentials (default)<br>'keep' - Keep k8s cluster running and leave credentials alone<br>'keep-on-failure' - Keep k8s cluster if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


<a id="post_behavior_loader_nodes"></a>

## **post_behavior_loader_nodes** / SCT_POST_BEHAVIOR_LOADER_NODES

Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


<a id="post_behavior_monitor_nodes"></a>

## **post_behavior_monitor_nodes** / SCT_POST_BEHAVIOR_MONITOR_NODES

Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


<a id="post_behavior_vector_store_nodes"></a>

## **post_behavior_vector_store_nodes** / SCT_POST_BEHAVIOR_VECTOR_STORE_NODES

Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.<br><br>'destroy' - Destroy instances and credentials (default)<br>'keep' - Keep instances running and leave credentials alone<br>'keep-on-failure' - Keep instances if testrun failed

**default:** destroy

**type:** Literal['destroy', 'keep', 'keep-on-failure']


<a id="teardown_validators"></a>

## **teardown_validators** / SCT_TEARDOWN_VALIDATORS

Validators to use during teardown phase

**default:** {'scrub': {'enabled': False, 'timeout': 1200, 'keyspace': '', 'table': ''}, 'test_error_events': {'enabled': False, 'failing_events': [{'event_class': 'DatabaseLogEvent', 'event_type': 'RUNTIME_ERROR', 'regex': '.*runtime_error.*'}, {'event_class': 'CoreDumpEvent'}]}, 'rackaware': {'enabled': False}}

**type:** dict | str
