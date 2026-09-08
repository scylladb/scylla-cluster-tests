# AWS backend

[← All configuration options](../configuration_options.md)

AWS-specific provisioning: AMIs, EC2 instance and disk settings, placement groups, capacity
reservations and dedicated hosts.

**26 options.**


<a id="ami_db_cassandra_user"></a>

## **ami_db_cassandra_user** / SCT_AMI_DB_CASSANDRA_USER

SSH login user baked into the Cassandra AMI, for the auxiliary cluster.

**default:** N/A

**type:** str (appendable)


<a id="ami_db_scylla_user"></a>

## **ami_db_scylla_user** / SCT_AMI_DB_SCYLLA_USER

SSH login user baked into the DB node AMI (e.g. 'centos', 'ubuntu').

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="ami_id_db_cassandra"></a>

## **ami_id_db_cassandra** / SCT_AMI_ID_DB_CASSANDRA

AMS AMI id to use for cassandra node

**default:** N/A

**type:** str (appendable)


<a id="ami_id_db_oracle"></a>

## **ami_id_db_oracle** / SCT_AMI_ID_DB_ORACLE

AMS AMI id to use for oracle node

**default:** N/A

**type:** str (appendable)


<a id="ami_id_db_scylla"></a>

## **ami_id_db_scylla** / SCT_AMI_ID_DB_SCYLLA

AMS AMI id to use for scylla db node

**default:** N/A

**type:** str (appendable)


<a id="ami_id_db_scylla_desc"></a>

## **ami_id_db_scylla_desc** / SCT_AMI_ID_DB_SCYLLA_DESC

version name to report stats to Elasticsearch and tagged on cloud instances

**default:** N/A

**type:** str (appendable)


<a id="ami_id_loader"></a>

## **ami_id_loader** / SCT_AMI_ID_LOADER

AMS AMI id to use for loader node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `resolve:ssm:/aws/service/canonical/ubuntu/server/26.04/stable/current/{arch}/hvm/ebs-gp3/ami-id`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="ami_id_monitor"></a>

## **ami_id_monitor** / SCT_AMI_ID_MONITOR

AMS AMI id to use for monitor node

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="ami_id_vector_store"></a>

## **ami_id_vector_store** / SCT_AMI_ID_VECTOR_STORE

AMS AMI id to use for vector store node

**default:** N/A

**type:** str (appendable)


<a id="ami_loader_user"></a>

## **ami_loader_user** / SCT_AMI_LOADER_USER

SSH login user baked into the loader AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="ami_monitor_user"></a>

## **ami_monitor_user** / SCT_AMI_MONITOR_USER

SSH login user baked into the monitoring node AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, azure, oci, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="ami_vector_store_user"></a>

## **ami_vector_store_user** / SCT_AMI_VECTOR_STORE_USER

SSH login user baked into the Vector Store AMI.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `ubuntu`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="aws_dedicated_host_ids"></a>

## **aws_dedicated_host_ids** / SCT_AWS_DEDICATED_HOST_IDS

List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)

**backend overrides:**
- `[]`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="aws_fallback_to_next_availability_zone"></a>

## **aws_fallback_to_next_availability_zone** / SCT_AWS_FALLBACK_TO_NEXT_AVAILABILITY_ZONE

Deprecated alias of [`fallback_to_next_availability_zone`](general-and-provisioning.md#fallback_to_next_availability_zone). Kept for backward compatibility.

**default:** False

**type:** bool


<a id="aws_instance_profile_name_db"></a>

## **aws_instance_profile_name_db** / SCT_AWS_INSTANCE_PROFILE_NAME_DB

This is the name of the instance profile to set on all db instances

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `qa-scylla-manager-backup-instance-profile`: aws, aws-siren, k8s-local-kind-aws, k8s-eks


<a id="aws_instance_profile_name_loader"></a>

## **aws_instance_profile_name_loader** / SCT_AWS_INSTANCE_PROFILE_NAME_LOADER

This is the name of the instance profile to set on all loader instances

**default:** N/A

**type:** str (appendable)


<a id="extra_network_interface"></a>

## **extra_network_interface** / SCT_EXTRA_NETWORK_INTERFACE

if true, create extra network interface on each node

**default:** N/A

**type:** bool


<a id="sct_aws_account_id"></a>

## **sct_aws_account_id** / SCT_SCT_AWS_ACCOUNT_ID

AWS account id on behalf of which the test is run

**default:** N/A

**type:** str (appendable)


<a id="spot_max_price"></a>

## **spot_max_price** / SCT_SPOT_MAX_PRICE

The max percentage of the on demand price we set for spot/fleet instances

**default:** N/A

**type:** float


<a id="spot_placement_score_min"></a>

## **spot_placement_score_min** / SCT_SPOT_PLACEMENT_SCORE_MIN

Drop availability zones scoring below this value (1-10) from spot placement candidates. Default 0 keeps every AZ, which matches the AWS contract that a score is a recommendation and not a guarantee. Note AWS returns structurally low scores when fewer than 3 instance types are requested, so a non-zero value here is only safe alongside instance-type diversification. If fewer AZs reach the threshold than the configured [`availability_zone`](general-and-provisioning.md#availability_zone) asks for, provisioning fails rather than quietly spanning fewer AZs than the test was written for.

**default:** 0

**type:** int


<a id="spot_score_overrides_configured_az"></a>

## **spot_score_overrides_configured_az** / SCT_SPOT_SCORE_OVERRIDES_CONFIGURED_AZ

Let the spot placement score override an explicitly configured [`availability_zone`](general-and-provisioning.md#availability_zone) rather than only ordering the AZs backfilled around it. Off by default so existing AZ pins keep their meaning.

**default:** False

**type:** bool


<a id="spot_score_region_relocation_margin"></a>

## **spot_score_region_relocation_margin** / SCT_SPOT_SCORE_REGION_RELOCATION_MARGIN

Relocate the cluster to a better-scoring region BEFORE the first provisioning attempt, when that region's spot placement score exceeds the configured region's by at least this many points (1-10). Useful for `region: random` jobs that land on a poor region by chance. 0 (default) disables it, leaving region relocation purely reactive. Only relocates to VPC-peered regions with an equivalent AMI; note the SCT runner stays in the original region, so the cluster is reached over the peering.

**default:** 0

**type:** int


<a id="use_capacity_reservation"></a>

## **use_capacity_reservation** / SCT_USE_CAPACITY_RESERVATION

Flag to use capacity reservation for instances

**default:** False

**type:** bool


<a id="use_dedicated_host"></a>

## **use_dedicated_host** / SCT_USE_DEDICATED_HOST

Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)

**default:** False

**type:** bool


<a id="use_placement_group"></a>

## **use_placement_group** / SCT_USE_PLACEMENT_GROUP

if true, create 'cluster' placement group for test case for low-latency network performance achievement

**default:** False

**type:** bool


<a id="use_spot_placement_scores"></a>

## **use_spot_placement_scores** / SCT_USE_SPOT_PLACEMENT_SCORES

Order availability zones and region-fallback candidates by `ec2:GetSpotPlacementScores` instead of alphabetically, so spot requests go to the AZ/region most likely to have capacity. Scores only reorder candidates that already passed the instance-type-offering filter; they never veto one. Ignored for `instance_provision: on_demand`, and silently ignored when the IAM permission is missing. AWS-only.

**default:** False

**type:** bool

**backend overrides:**
- `True`: aws, aws-siren, k8s-local-kind-aws, k8s-eks
