# Amazon EMR (spark-migrator)

[← All configuration options](configuration_options.md)

The EMR cluster that runs the Spark migrator job.

**13 options.** Jump to: [emr_applications](#emr_applications) · [emr_install_spark4_via_bootstrap](#emr_install_spark4_via_bootstrap) · [emr_instance_count_core](#emr_instance_count_core) · [emr_instance_count_task](#emr_instance_count_task) · [emr_instance_type_core](#emr_instance_type_core) · [emr_instance_type_master](#emr_instance_type_master) · [emr_instance_type_task](#emr_instance_type_task) · [emr_keep_alive](#emr_keep_alive) · [emr_log_uri](#emr_log_uri) · [emr_release_label](#emr_release_label) · [emr_spark_migrator_jar_path](#emr_spark_migrator_jar_path) · [emr_spark_migrator_release](#emr_spark_migrator_release) · [emr_spot_bid_percentage](#emr_spot_bid_percentage)


## **emr_applications** / SCT_EMR_APPLICATIONS

List of EMR applications to install (default: ['Spark'])

**default:** N/A

**type:** list

**backend overrides:**
- `['Spark']`: aws


## **emr_install_spark4_via_bootstrap** / SCT_EMR_INSTALL_SPARK4_VIA_BOOTSTRAP

Legacy fallback: install Spark 4.x via an EMR bootstrap action and submit the migrator through script-runner.jar (for emr-7.x releases). Default value is false - i.e. deployment of native Spark on an `emr-spark-8.x` release label.

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: aws


## **emr_instance_count_core** / SCT_EMR_INSTANCE_COUNT_CORE

How many EMR core nodes to launch.

**default:** N/A

**type:** int

**backend overrides:**
- `2`: aws


## **emr_instance_count_task** / SCT_EMR_INSTANCE_COUNT_TASK

How many EMR task nodes to launch (compute only, no HDFS).

**default:** N/A

**type:** int

**backend overrides:**
- `0`: aws


## **emr_instance_type_core** / SCT_EMR_INSTANCE_TYPE_CORE

EC2 instance type for the EMR core nodes (they run both compute and HDFS).

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_master** / SCT_EMR_INSTANCE_TYPE_MASTER

Instance type for EMR master node (e.g., 'm5.xlarge')

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `m5.xlarge`: aws


## **emr_instance_type_task** / SCT_EMR_INSTANCE_TYPE_TASK

Instance type for EMR task nodes (optional, uses Spot instances)

**default:** N/A

**type:** str (appendable)


## **emr_keep_alive** / SCT_EMR_KEEP_ALIVE

Whether EMR cluster stays alive after job completion (default: true for reuse during testing)

**default:** N/A

**type:** bool

**backend overrides:**
- `True`: aws


## **emr_log_uri** / SCT_EMR_LOG_URI

S3 URI for EMR cluster logs (e.g., 's3://sct-emr-spark-migrator-{region}/logs/')

**default:** N/A

**type:** str (appendable)


## **emr_release_label** / SCT_EMR_RELEASE_LABEL

EMR release version (e.g., 'emr-7.8.0'). When set, an EMR cluster is provisioned alongside the Scylla cluster.

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_jar_path** / SCT_EMR_SPARK_MIGRATOR_JAR_PATH

S3 path or local path to the spark-migrator JAR file

**default:** N/A

**type:** str (appendable)


## **emr_spark_migrator_release** / SCT_EMR_SPARK_MIGRATOR_RELEASE

scylla-migrator release tag (e.g., 'v1.1.2'). When set, JAR is auto-downloaded from GitHub releases and uploaded to S3. Takes precedence over emr_spark_migrator_jar_path.

**default:** N/A

**type:** str (appendable)


## **emr_spot_bid_percentage** / SCT_EMR_SPOT_BID_PERCENTAGE

Max Spot price as percentage of On-Demand for EMR task nodes (default: 100)

**default:** N/A

**type:** int

**backend overrides:**
- `100`: aws
