# Auxiliary DB cluster (oracle / Cassandra)

[← All configuration options](../configuration_options.md)

A second database cluster used for comparison or migration testing -- the 'oracle' cluster in
Gemini runs, or a Cassandra cluster in migration tests. Named for the role, not for Gemini,
since other test types use it too.

**4 options.**


<a id="append_scylla_args_oracle"></a>

## **append_scylla_args_oracle** / SCT_APPEND_SCYLLA_ARGS_ORACLE

More arguments to append to oracle command line

**default:** --enable-cache false

**type:** str
* appendable


<a id="n_test_oracle_db_nodes"></a>

## **n_test_oracle_db_nodes** / SCT_N_TEST_ORACLE_DB_NODES

Number list of oracle test nodes in multiple data centers.

**default:** 1

**type:** int | list[int]


<a id="oracle_scylla_version"></a>

## **oracle_scylla_version** / SCT_ORACLE_SCYLLA_VERSION

Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'<br>Automatically looks up cloud images for formal versions.<br>WARNING: can't be used together with [`ami_id_db_oracle`](aws-backend.md#ami_id_db_oracle) and [`oci_image_db_oracle`](oci-backend.md#oci_image_db_oracle)

**default:** 2026.1

**type:** str


<a id="oracle_user_data_format_version"></a>

## **oracle_user_data_format_version** / SCT_ORACLE_USER_DATA_FORMAT_VERSION

Same as [`user_data_format_version`](scylla-installation-and-configuration.md#user_data_format_version), but for the auxiliary oracle cluster's images.

**default:** N/A

**type:** str
