# Scylla Cloud (xcloud) backend

[← All configuration options](configuration_options.md)

Clusters provisioned through the Scylla Cloud API, including the legacy siren `cloud_*` options.

**12 options.**


## **cloud_cluster_id** / SCT_CLOUD_CLUSTER_ID

ID of an existing Scylla Cloud cluster to run against, instead of provisioning a new one.

**default:** N/A

**type:** int


## **cloud_credentials_path** / SCT_CLOUD_CREDENTIALS_PATH

Path to the SSH private key for nodes in a Scylla Cloud (siren) cluster, which SCT does not provision itself.

**default:** N/A

**type:** str (appendable)


## **cloud_prom_bearer_token** / SCT_CLOUD_PROM_BEARER_TOKEN

scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_host** / SCT_CLOUD_PROM_HOST

scylla cloud promproxy hostname to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **cloud_prom_path** / SCT_CLOUD_PROM_PATH

scylla cloud promproxy path to federate monitoring data into our monitoring instance

**default:** N/A

**type:** str (appendable)


## **xcloud_availability_zones** / SCT_XCLOUD_AVAILABILITY_ZONES

Comma-separated availability zones for Scylla Cloud DB placement.<br>AWS values are AZ IDs (e.g., 'use1-az1,use1-az2,use1-az3'); GCE values are zone names<br>(e.g., 'us-east1-b,us-east1-c'). When set, SCT sends 'availabilityZoneIdsOverride' and forces placement.<br>Provide one zone per DB node, or provide a shorter list to cycle round-robin (node count must divide evenly).<br>Repeat the same zone to keep all nodes in one AZ. Leave empty (default) to let Scylla Cloud choose placement<br>(multi-AZ spread). Cannot be used with [`xcloud_scaling_config`](#xcloud_scaling_config).

**default:** N/A

**type:** str (appendable)


## **xcloud_credentials_path** / SCT_XCLOUD_CREDENTIALS_PATH

Path to Scylla Cloud credentials file, if stored locally

**default:** N/A

**type:** str (appendable)


## **xcloud_env** / SCT_XCLOUD_ENV

Scylla Cloud environment (e.g., lab).

**default:** N/A

**type:** str (appendable)


## **xcloud_provider** / SCT_XCLOUD_PROVIDER

Cloud provider for Scylla Cloud deployment (aws, gce)

**default:** N/A

**type:** str (appendable)


## **xcloud_replication_factor** / SCT_XCLOUD_REPLICATION_FACTOR

Replication factor for Scylla Cloud cluster

**default:** N/A

**type:** int


## **xcloud_scaling_config** / SCT_XCLOUD_SCALING_CONFIG

Scaling policy configuration. The payload should follow the following structure:<br><br>{<br>"InstanceFamilies": ["i8g"],<br>"Mode": "xcloud",<br>"Policies": {<br>"Storage": {"Min": 0, "TargetUtilization": 0.8},<br>"VCPU": {"Min": 0}<br>}<br>}<br><br>- InstanceFamilies(list): instance families to use for scaling (e.g., ["i4i", "i8g"])<br>- Mode(str): scaling mode, always "xcloud"<br>- Policies(dict): scaling policies with the following keys:<br>- Storage(dict):<br>- Min(int): minimum storage in TB to maintain<br>- TargetUtilization(float): target storage utilization from 0.7 to 0.9 with 0.05 step<br>- VCPU(dict):<br>- Min(int): minimum number of virtual CPUs to maintain<br><br>For more details, see `scaling` parameter description in Cloud REST API documentation:<br>https://cloud.docs.scylladb.com/stable/api.html#tag/Cluster/operation/createCluster

**default:** N/A

**type:** dict

**backend overrides:**
- `{}`: xcloud


## **xcloud_vpc_peering** / SCT_XCLOUD_VPC_PEERING

Dictionary of VPC peering parameters for private connectivity between<br>SCT infrastructure and Scylla Cloud. The following parameters are used:<br>enabled: bool - indicates whether VPC peering is to be used<br>cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)<br>cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)

**default:** N/A

**type:** dict

**backend overrides:**
- `{'enabled': True, 'cidr_pool_base': '172.31.0.0/16', 'cidr_subnet_size': 24}`: xcloud
