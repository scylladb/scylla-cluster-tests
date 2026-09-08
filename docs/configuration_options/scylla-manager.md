# Scylla Manager

[← All configuration options](configuration_options.md)

Scylla Manager server and agent: versions, repos and backup/restore settings.

**26 options.** Jump to: [backup_bucket_backend](#backup_bucket_backend) · [backup_bucket_location](#backup_bucket_location) · [backup_bucket_region](#backup_bucket_region) · [manager_backup_restore_method](#manager_backup_restore_method) · [manager_prometheus_port](#manager_prometheus_port) · [manager_scylla_backend_version](#manager_scylla_backend_version) · [manager_version](#manager_version) · [mgmt_agent_backup_config](#mgmt_agent_backup_config) · [mgmt_docker_image](#mgmt_docker_image) · [mgmt_nodetool_refresh_flags](#mgmt_nodetool_refresh_flags) · [mgmt_prepare_snapshot_size](#mgmt_prepare_snapshot_size) · [mgmt_restore_extra_params](#mgmt_restore_extra_params) · [mgmt_reuse_backup_snapshot_name](#mgmt_reuse_backup_snapshot_name) · [mgmt_skip_post_restore_stress_read](#mgmt_skip_post_restore_stress_read) · [mgmt_snapshots_preparer_params](#mgmt_snapshots_preparer_params) · [scylla_mgmt_address](#scylla_mgmt_address) · [scylla_mgmt_agent_address](#scylla_mgmt_agent_address) · [scylla_mgmt_agent_version](#scylla_mgmt_agent_version) · [scylla_mgmt_pkg](#scylla_mgmt_pkg) · [scylla_mgmt_upgrade_to_repo](#scylla_mgmt_upgrade_to_repo) · [scylla_repo_m](#scylla_repo_m) · [target_manager_version](#target_manager_version) · [target_scylla_mgmt_agent_address](#target_scylla_mgmt_agent_address) · [target_scylla_mgmt_server_address](#target_scylla_mgmt_server_address) · [use_cloud_manager](#use_cloud_manager) · [use_mgmt](#use_mgmt)


## **backup_bucket_backend** / SCT_BACKUP_BUCKET_BACKEND

the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `s3`: aws, oci, aws-siren, k8s-local-kind-aws, k8s-gke, k8s-eks
- `gcs`: gce, gce-siren
- `azure`: azure


## **backup_bucket_location** / SCT_BACKUP_BUCKET_LOCATION

the bucket name to be used for backup (e.g., 'manager-backup-tests')

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `manager-backup-tests-{region}`: aws, aws-siren, k8s-eks
- `manager-backup-tests-sct-project-1-us-east1`: gce, gce-siren
- `manager-backup-tests-us-east-1`: azure
- `minio-bucket`: k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke


## **backup_bucket_region** / SCT_BACKUP_BUCKET_REGION

the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')

**default:** N/A

**type:** str (appendable)


## **manager_backup_restore_method** / SCT_MANAGER_BACKUP_RESTORE_METHOD

The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.

**default:** N/A

**type:** str (appendable)


## **manager_prometheus_port** / SCT_MANAGER_PROMETHEUS_PORT

Port to be used by the manager to contact Prometheus

**default:** 5090

**type:** int


## **manager_scylla_backend_version** / SCT_MANAGER_SCYLLA_BACKEND_VERSION

Version of ScyllaDB to install as Manager backend

**default:** 2025.4

**type:** str


## **manager_version** / SCT_MANAGER_VERSION

Version of Scylla Manager server and agent to install

**default:** 3.12

**type:** str


## **mgmt_agent_backup_config** / SCT_MGMT_AGENT_BACKUP_CONFIG

Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}

**default:** N/A

**type:** sdcm.mgmt.common.AgentBackupParameters


## **mgmt_docker_image** / SCT_MGMT_DOCKER_IMAGE

Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'

**default:** scylladb/scylla-manager:3.12.0

**type:** str (appendable)


## **mgmt_nodetool_refresh_flags** / SCT_MGMT_NODETOOL_REFRESH_FLAGS

Nodetool refresh extra options like --load-and-stream or --primary-replica-only

**default:** N/A

**type:** str (appendable)


## **mgmt_prepare_snapshot_size** / SCT_MGMT_PREPARE_SNAPSHOT_SIZE

Size of backup snapshot in Gb to be prepared for backup

**default:** N/A

**type:** int


## **mgmt_restore_extra_params** / SCT_MGMT_RESTORE_EXTRA_PARAMS

Manager restore operation extra parameters: batch-size, parallel, etc. For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd

**default:** N/A

**type:** str (appendable)


## **mgmt_reuse_backup_snapshot_name** / SCT_MGMT_REUSE_BACKUP_SNAPSHOT_NAME

Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)

**default:** N/A

**type:** str (appendable)


## **mgmt_skip_post_restore_stress_read** / SCT_MGMT_SKIP_POST_RESTORE_STRESS_READ

Skip post-restore c-s verification read in the Manager restore benchmark tests

**default:** N/A

**type:** bool


## **mgmt_snapshots_preparer_params** / SCT_MGMT_SNAPSHOTS_PREPARER_PARAMS

Custom parameters of c-s write operation used in snapshots preparer

**default:** {'cs_cmd_template': "cassandra-stress {operation} cl={cl} n={num_of_rows} -schema 'keyspace={ks_name} replication(strategy={replication},replication_factor={rf}) compaction(strategy={compaction})' -mode cql3 native -rate threads={threads_num} -col 'size=FIXED({col_size}) n=FIXED({col_n})' -pop seq={sequence_start}..{sequence_end}", 'operation': 'write', 'cl': 'QUORUM', 'replication': 'NetworkTopologyStrategy', 'rf': 3, 'compaction': 'IncrementalCompactionStrategy', 'threads_num': 500, 'col_size': 1024, 'col_n': 1, 'ks_name': '', 'num_of_rows': '', 'sequence_start': '', 'sequence_end': ''}

**type:** dict | YAML/JSON string → dict


## **scylla_mgmt_address** / SCT_SCYLLA_MGMT_ADDRESS

Url to the repo of scylla manager version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_address** / SCT_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager agent version to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_agent_version** / SCT_SCYLLA_MGMT_AGENT_VERSION

Version of Scylla Manager agent to install for management tests

**default:** 3.12.0

**type:** str


## **scylla_mgmt_pkg** / SCT_SCYLLA_MGMT_PKG

Url to the scylla manager packages to install for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_mgmt_upgrade_to_repo** / SCT_SCYLLA_MGMT_UPGRADE_TO_REPO

Url to the repo of scylla manager version to upgrade to for management tests

**default:** N/A

**type:** str (appendable)


## **scylla_repo_m** / SCT_SCYLLA_REPO_M

Url to the repo of scylla version to install scylla from for management tests

**default:** N/A

**type:** str (appendable)


## **target_manager_version** / SCT_TARGET_MANAGER_VERSION

Version of Scylla Manager server and agent to upgrade to

**default:** N/A

**type:** str


## **target_scylla_mgmt_agent_address** / SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager agents

**default:** N/A

**type:** str (appendable)


## **target_scylla_mgmt_server_address** / SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS

Url to the repo of scylla manager version used to upgrade the manager server

**default:** N/A

**type:** str (appendable)


## **use_cloud_manager** / SCT_USE_CLOUD_MANAGER

When define true, will install scylla cloud manager

**default:** False

**type:** bool


## **use_mgmt** / SCT_USE_MGMT

When define true, will install scylla management

**default:** True

**type:** bool
