# OCI backend

[← All configuration options](configuration_options.md)

Oracle Cloud Infrastructure provisioning.

**10 options.** Jump to: [oci_image_db](#oci_image_db) · [oci_image_db_oracle](#oci_image_db_oracle) · [oci_image_loader](#oci_image_loader) · [oci_image_monitor](#oci_image_monitor) · [oci_image_username](#oci_image_username) · [oci_instance_type_db](#oci_instance_type_db) · [oci_instance_type_db_oracle](#oci_instance_type_db_oracle) · [oci_instance_type_loader](#oci_instance_type_loader) · [oci_instance_type_monitor](#oci_instance_type_monitor) · [oci_region_name](#oci_region_name)


## **oci_image_db** / SCT_OCI_IMAGE_DB

Oracle Cloud image to use for DB node(s)

**default:** N/A

**type:** str (appendable)


## **oci_image_db_oracle** / SCT_OCI_IMAGE_DB_ORACLE

Oracle Cloud image to use for oracle (2nd ref cluster) DB node(s). If not set and [`oracle_scylla_version`](auxiliary-db-cluster-oracle-cassandra.md#oracle_scylla_version) is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **oci_image_loader** / SCT_OCI_IMAGE_LOADER

Oracle Cloud image to use for the loader node(s). Empty value results into latest ubuntu image

**default:** N/A

**type:** str (appendable)


## **oci_image_monitor** / SCT_OCI_IMAGE_MONITOR

Oracle Cloud image to use for the monitor node. Empty value results into latest ubuntu image

**default:** N/A

**type:** str (appendable)


## **oci_image_username** / SCT_OCI_IMAGE_USERNAME

Username used in the Oracle Cloud images utilized by the DB node(s)

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: oci


## **oci_instance_type_db** / SCT_OCI_INSTANCE_TYPE_DB

Oracle Cloud instance shape to use for DB node(s). Usage of flex shapes allows setting of the ocpus, memory and nvme disks. Format is following: <shape-name>:<ocpus>:<ram>:<nvmes> . For DenseIO shapes it makes sense to specify only 'ocpus' part, because ram and amount of NVMe disks will be fixed based on the OCPUs count.

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_db_oracle** / SCT_OCI_INSTANCE_TYPE_DB_ORACLE

Oracle Cloud instance shape to use for 'oracle' (2nd ref cluster) ScylladbDB cluster

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_loader** / SCT_OCI_INSTANCE_TYPE_LOADER

Oracle Cloud instance shape to use for loader node(s). Usage of flex shapes allows setting of the ocpus, memory. Format is following: <shape-name>:<ocpus>:<ram>

**default:** N/A

**type:** str (appendable)


## **oci_instance_type_monitor** / SCT_OCI_INSTANCE_TYPE_MONITOR

Oracle Cloud instance shape to use for monitor node. Usage of flex shapes allows setting of the ocpus, memory. Format is following: <shape-name>:<ocpus>:<ram>

**default:** N/A

**type:** str (appendable)


## **oci_region_name** / SCT_OCI_REGION_NAME

OCI region where the resources will be deployed

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['us-phoenix-1']`: oci
