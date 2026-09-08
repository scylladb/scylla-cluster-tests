# Auxiliary DB cluster (oracle / Cassandra)

[← All configuration options](configuration_options.md)

A second database cluster used for comparison or migration testing -- the 'oracle' cluster in
Gemini runs, or a Cassandra cluster in migration tests. Named for the role, not for Gemini,
since other test types use it too.

**10 options.** Jump to: [append_scylla_args_oracle](#append_scylla_args_oracle) · [cassandra_broadcast_rpc_public](#cassandra_broadcast_rpc_public) · [cassandra_num_tokens](#cassandra_num_tokens) · [cassandra_oracle_version](#cassandra_oracle_version) · [cassandra_version](#cassandra_version) · [docker_image_cassandra](#docker_image_cassandra) · [install_cassandra_exporter](#install_cassandra_exporter) · [n_test_oracle_db_nodes](#n_test_oracle_db_nodes) · [oracle_scylla_version](#oracle_scylla_version) · [oracle_user_data_format_version](#oracle_user_data_format_version)


## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** str (appendable)


## **cassandra_broadcast_rpc_public** / SCT_CASSANDRA_BROADCAST_RPC_PUBLIC

When True, set broadcast_rpc_address to the public IP of the node in cassandra.yaml, so clients outside the VPC (e.g. sct-runner driver connection that reads system.peers) can reach the nodes. Defaults to False (private IP, matches intra-VPC behavior).

**default:** N/A

**type:** bool


## **cassandra_num_tokens** / SCT_CASSANDRA_NUM_TOKENS

num_tokens value to configure in cassandra.yaml.

**default:** 16

**type:** int


## **cassandra_oracle_version** / SCT_CASSANDRA_ORACLE_VERSION

Cassandra version for the oracle cluster, i.e. '4.1' or '5.0'

**default:** N/A

**type:** str (appendable)


## **cassandra_version** / SCT_CASSANDRA_VERSION

Cassandra version / docker image tag, i.e. '4.1' or '5.0'

**default:** 4.1

**type:** str (appendable)


## **docker_image_cassandra** / SCT_DOCKER_IMAGE_CASSANDRA

Cassandra docker image repo, i.e. 'cassandra'. Used when db_type is 'cassandra'.

**default:** cassandra

**type:** str (appendable)


## **install_cassandra_exporter** / SCT_INSTALL_CASSANDRA_EXPORTER

Install Criteo cassandra_exporter on Cassandra nodes for Prometheus metrics collection. The exporter connects to JMX (port 7199) and exposes metrics on port 8080.

**default:** True

**type:** bool


## **n_test_oracle_db_nodes** / SCT_N_TEST_ORACLE_DB_NODES

Number list of oracle test nodes in multiple data centers.

**default:** 1

**type:** int | list[int] | space-separated ints → list[int]


## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically looks up cloud images for formal versions.<br>WARNING: can't be used together with the backend's oracle image param<br>([`ami_id_db_oracle`](aws-backend.md#ami_id_db_oracle), [`gce_image_db_oracle`](gce-backend.md#gce_image_db_oracle), [`azure_image_db_oracle`](azure-backend.md#azure_image_db_oracle) or [`oci_image_db_oracle`](oci-backend.md#oci_image_db_oracle))

**default:** 2026.1

**type:** str


## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Same as [`user_data_format_version`](scylla-installation-and-configuration.md#user_data_format_version), but for the auxiliary oracle cluster's images.

**default:** N/A

**type:** str
