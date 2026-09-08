# AWS backend

[← All configuration options](configuration_options.md)

AWS-specific provisioning: AMIs, EC2 instance and disk settings, placement groups, capacity
reservations and dedicated hosts.

**22 options.** Jump to: [ami_db_cassandra_user](#ami_db_cassandra_user) · [ami_db_scylla_user](#ami_db_scylla_user) · [ami_id_db_cassandra](#ami_id_db_cassandra) · [ami_id_db_oracle](#ami_id_db_oracle) · [ami_id_db_scylla](#ami_id_db_scylla) · [ami_id_db_scylla_desc](#ami_id_db_scylla_desc) · [ami_id_loader](#ami_id_loader) · [ami_id_monitor](#ami_id_monitor) · [ami_id_vector_store](#ami_id_vector_store) · [ami_loader_user](#ami_loader_user) · [ami_monitor_user](#ami_monitor_user) · [ami_vector_store_user](#ami_vector_store_user) · [aws_dedicated_host_ids](#aws_dedicated_host_ids) · [aws_fallback_to_next_availability_zone](#aws_fallback_to_next_availability_zone) · [aws_instance_profile_name_db](#aws_instance_profile_name_db) · [aws_instance_profile_name_loader](#aws_instance_profile_name_loader) · [extra_network_interface](#extra_network_interface) · [sct_aws_account_id](#sct_aws_account_id) · [spot_max_price](#spot_max_price) · [use_capacity_reservation](#use_capacity_reservation) · [use_dedicated_host](#use_dedicated_host) · [use_placement_group](#use_placement_group)


## **ami_db_cassandra_user** / SCT_AMI_DB_CASSANDRA_USER

SSH login user baked into the Cassandra AMI, for the auxiliary cluster.

**default:** N/A

**type:** str (appendable)


## **ami_db_scylla_user** / SCT_AMI_DB_SCYLLA_USER

SSH login user baked into the DB node AMI (e.g. 'centos', 'ubuntu').

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_db_cassandra** / SCT_AMI_ID_DB_CASSANDRA

AMS AMI id to use for cassandra node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_oracle** / SCT_AMI_ID_DB_ORACLE

AMS AMI id to use for oracle node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** str (appendable)


## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** str (appendable)


## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/{arch}/hvm/ebs-gp3/ami-id`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_id_vector_store** / SCT_AMI_ID_VECTOR_STORE

AMS AMI id to use for vector store node

**default:** N/A

**type:** str (appendable)


## **ami_loader_user** / SCT_AMI_LOADER_USER

SSH login user baked into the loader AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_monitor_user** / SCT_AMI_MONITOR_USER

SSH login user baked into the monitoring node AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


## **ami_vector_store_user** / SCT_AMI_VECTOR_STORE_USER

SSH login user baked into the Vector Store AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_dedicated_host_ids** / SCT_AWS_DEDICATED_HOST_IDS

List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `[]`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Deprecated alias of [`fallback_to_next_availability_zone`](general-and-provisioning.md#fallback_to_next_availability_zone). Kept for backward compatibility.

**default:** False

**type:** bool


## **aws_instance_profile_name_db** / SCT_AWS_INSTANCE_PROFILE_NAME_DB

This is the name of the instance profile to set on all db instances

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `qa-scylla-manager-backup-instance-profile`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


## **aws_instance_profile_name_loader** / SCT_AWS_INSTANCE_PROFILE_NAME_LOADER

This is the name of the instance profile to set on all loader instances

**default:** N/A

**type:** str (appendable)


## **extra_network_interface** / SCT_EXTRA_NETWORK_INTERFACE

if true, create extra network interface on each node

**default:** N/A

**type:** bool


## **sct_aws_account_id** / SCT_SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


## **spot_max_price** / SCT_SPOT_MAX_PRICE

The max percentage of the on demand price we set for spot/fleet instances

**default:** N/A

**type:** float


## **use_capacity_reservation** / SCT_USE_CAPACITY_RESERVATION

Flag to use capacity reservation for instances

**default:** False

**type:** bool


## **use_dedicated_host** / SCT_USE_DEDICATED_HOST

Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)

**default:** False

**type:** bool


## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** False

**type:** bool
