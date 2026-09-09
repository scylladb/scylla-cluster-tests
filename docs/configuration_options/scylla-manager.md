# Scylla Manager

[← All configuration options](../configuration_options.md)

Scylla Manager server and agent: versions, repos and backup/restore settings.

**26 options.**


<a id="backup_bucket_backend"></a>

## **backup_bucket_backend** / SCT_BACKUP_BUCKET_BACKEND

the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `s3`: aws, oci, aws-siren, k8s-local-kind-aws, k8s-gke, k8s-eks
- `gcs`: gce, gce-siren
- `azure`: azure


<a id="backup_bucket_location"></a>

## **backup_bucket_location** / SCT_BACKUP_BUCKET_LOCATION

the bucket name to be used for backup (e.g., 'manager-backup-tests')

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `manager-backup-tests-{region}`: aws, aws-siren, k8s-eks
- `manager-backup-tests-sct-project-1-us-east1`: gce, gce-siren
- `manager-backup-tests-us-east-1`: azure
- `minio-bucket`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke


<a id="backup_bucket_region"></a>

## **backup_bucket_region** / SCT_BACKUP_BUCKET_REGION

the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')

**default:** N/A

**type:** str (appendable)


<a id="manager_backup_restore_method"></a>

## **manager_backup_restore_method** / SCT_MANAGER_BACKUP_RESTORE_METHOD

The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.

**default:** N/A

**type:** str (appendable)


<a id="manager_prometheus_port"></a>

## **manager_prometheus_port** / SCT_MANAGER_PROMETHEUS_PORT

Port to be used by the manager to contact Prometheus

**default:** 5090

**type:** int


<a id="manager_scylla_backend_version"></a>

## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Version of ScyllaDB to install as Manager backend

**default:** 2025.4

**type:** str


<a id="manager_version"></a>

## **manager_version** / SCT_MANAGER_VERSION

Version of Scylla Manager server and agent to install

**default:** 3.12

**type:** str


<a id="mgmt_agent_backup_config"></a>

## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


<a id="mgmt_docker_image"></a>

## **mgmt_docker_image** / SCT_MGMT_DOCKER_IMAGE

Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'

**default:** scylladb/scylla-manager:3.12.0

**type:** str (appendable)


<a id="mgmt_nodetool_refresh_flags"></a>

## **mgmt_nodetool_refresh_flags** / SCT_MGMT_NODETOOL_REFRESH_FLAGS

Nodetool refresh extra options like --load-and-stream or --primary-replica-only

**default:** N/A

**type:** str (appendable)


<a id="mgmt_prepare_snapshot_size"></a>

## **mgmt_prepare_snapshot_size** / SCT_MGMT_PREPARE_SNAPSHOT_SIZE

Size of backup snapshot in Gb to be prepared for backup

**default:** N/A

**type:** int


<a id="mgmt_restore_extra_params"></a>

## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


<a id="mgmt_reuse_backup_snapshot_name"></a>

## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


<a id="mgmt_skip_post_restore_stress_read"></a>

## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


<a id="mgmt_snapshots_preparer_params"></a>

## **mgmt_snapshots_preparer_params** / SCT_MGMT_SNAPSHOTS_PREPARER_PARAMS

Custom parameters of c-s write operation used in snapshots preparer

**default:** {'cs_cmd_template': "cassandra-stress {operation} cl={cl} n={num_of_rows} -schema 'keyspace={ks_name} replication(strategy={replication},replication_factor={rf}) compaction(strategy={compaction})' -mode cql3 native -rate threads={threads_num} -col 'size=FIXED({col_size}) n=FIXED({col_n})' -pop seq={sequence_start}..{sequence_end}", 'operation': 'write', 'cl': 'QUORUM', 'replication': 'NetworkTopologyStrategy', 'rf': 3, 'compaction': 'IncrementalCompactionStrategy', 'threads_num': 500, 'col_size': 1024, 'col_n': 1, 'ks_name': '', 'num_of_rows': '', 'sequence_start': '', 'sequence_end': ''}

**type:** dict | YAML/JSON string → dict


<a id="scylla_mgmt_address"></a>

## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** str (appendable)


<a id="scylla_mgmt_agent_address"></a>

## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** str (appendable)


<a id="scylla_mgmt_agent_version"></a>

## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION

Version of Scylla Manager agent to install for management tests

**default:** 3.12.0

**type:** str


<a id="scylla_mgmt_pkg"></a>

## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


<a id="scylla_mgmt_upgrade_to_repo"></a>

## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


<a id="scylla_repo_m"></a>

## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** str (appendable)


<a id="target_manager_version"></a>

## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Version of Scylla Manager server and agent to upgrade to

**default:** N/A

**type:** str


<a id="target_scylla_mgmt_agent_address"></a>

## **target_scylla_mgmt_agent_address** / SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager agents

**default:** N/A

**type:** str (appendable)


<a id="target_scylla_mgmt_server_address"></a>

## **target_scylla_mgmt_server_address** / SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager server

**default:** N/A

**type:** str (appendable)


<a id="use_cloud_manager"></a>

## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** False

**type:** bool


<a id="use_mgmt"></a>

## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** bool
