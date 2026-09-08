# GCE backend

[← All configuration options](configuration_options.md)

Google Compute Engine provisioning.

**23 options.** Jump to: [gce_datacenter](#gce_datacenter) · [gce_image_db](#gce_image_db) · [gce_image_db_oracle](#gce_image_db_oracle) · [gce_image_loader](#gce_image_loader) · [gce_image_monitor](#gce_image_monitor) · [gce_image_username](#gce_image_username) · [gce_instance_type_db](#gce_instance_type_db) · [gce_instance_type_db_oracle](#gce_instance_type_db_oracle) · [gce_instance_type_loader](#gce_instance_type_loader) · [gce_instance_type_monitor](#gce_instance_type_monitor) · [gce_n_local_ssd_disk_db](#gce_n_local_ssd_disk_db) · [gce_n_local_ssd_disk_loader](#gce_n_local_ssd_disk_loader) · [gce_n_local_ssd_disk_monitor](#gce_n_local_ssd_disk_monitor) · [gce_network](#gce_network) · [gce_pd_ssd_disk_size_db](#gce_pd_ssd_disk_size_db) · [gce_pd_ssd_disk_size_loader](#gce_pd_ssd_disk_size_loader) · [gce_pd_ssd_disk_size_monitor](#gce_pd_ssd_disk_size_monitor) · [gce_pd_standard_disk_size_db](#gce_pd_standard_disk_size_db) · [gce_project](#gce_project) · [gce_root_disk_type_db](#gce_root_disk_type_db) · [gce_root_disk_type_loader](#gce_root_disk_type_loader) · [gce_root_disk_type_monitor](#gce_root_disk_type_monitor) · [gce_setup_hybrid_raid](#gce_setup_hybrid_raid)


## **gce_datacenter** / SCT_GCE_DATACENTER

Supported regions: us-east1, us-east4, us-west1, us-central1. Specifying just the region (e.g., us-east1) means the zone will be selected automatically, or you can mention the zone explicitly (e.g., us-east1-b)

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `us-east1`: gce, gce-siren, k8s-gke


## **gce_image_db** / SCT_GCE_IMAGE_DB

gce image to use for db nodes

**default:** N/A

**type:** str (appendable)


## **gce_image_db_oracle** / SCT_GCE_IMAGE_DB_ORACLE

GCE image to use for oracle (2nd ref cluster) DB node(s). If not set and [`oracle_scylla_version`](auxiliary-db-cluster-oracle-cassandra.md#oracle_scylla_version) is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **gce_image_loader** / SCT_GCE_IMAGE_LOADER

Google Compute Engine image to use for loader nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2604-lts-{arch}`: gce, gce-siren, k8s-gke


## **gce_image_monitor** / SCT_GCE_IMAGE_MONITOR

gce image to use for monitor nodes

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylladb-monitor-4-16-0-amd64-2026-08-30t08-46-39z`: gce, gce-siren, k8s-gke


## **gce_image_username** / SCT_GCE_IMAGE_USERNAME

Username for the Google Compute Engine image

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scylla-test`: gce, gce-siren, k8s-gke


## **gce_instance_type_db** / SCT_GCE_INSTANCE_TYPE_DB

Instance type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `n2-standard-8`: k8s-gke


## **gce_instance_type_db_oracle** / SCT_GCE_INSTANCE_TYPE_DB_ORACLE

Instance type for the oracle (2nd ref cluster) DB nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)


## **gce_instance_type_loader** / SCT_GCE_INSTANCE_TYPE_LOADER

Instance type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `e2-standard-4`: k8s-gke


## **gce_instance_type_monitor** / SCT_GCE_INSTANCE_TYPE_MONITOR

Instance type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `e2-medium`: k8s-gke


## **gce_n_local_ssd_disk_db** / SCT_GCE_N_LOCAL_SSD_DISK_DB

Number of local SSD disks for database nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `4`: gce, gce-siren, k8s-gke


## **gce_n_local_ssd_disk_loader** / SCT_GCE_N_LOCAL_SSD_DISK_LOADER

Number of local SSD disks for loader nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_n_local_ssd_disk_monitor** / SCT_GCE_N_LOCAL_SSD_DISK_MONITOR

Number of local SSD disks for monitor nodes in Google Compute Engine

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_network** / SCT_GCE_NETWORK

GCP VPC network the instances are attached to.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `qa-vpc`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_db** / SCT_GCE_PD_SSD_DISK_SIZE_DB

Size in GB of the persistent SSD disk attached to each DB node.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_loader** / SCT_GCE_PD_SSD_DISK_SIZE_LOADER

Size in GB of the persistent SSD disk attached to each loader.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_ssd_disk_size_monitor** / SCT_GCE_PD_SSD_DISK_SIZE_MONITOR

Size in GB of the persistent SSD disk attached to the monitoring node.

**default:** N/A

**type:** int

**backend overrides:**
- `0`: gce, gce-siren, k8s-gke


## **gce_pd_standard_disk_size_db** / SCT_GCE_PD_STANDARD_DISK_SIZE_DB

The size of the standard persistent disk in GB used for GCE database nodes

**default:** 0

**type:** int


## **gce_project** / SCT_GCE_PROJECT

GCP project that owns the provisioned resources.

**default:** N/A

**type:** str (appendable)


## **gce_root_disk_type_db** / SCT_GCE_ROOT_DISK_TYPE_DB

Root disk type for database nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-ssd`: gce, gce-siren, k8s-gke


## **gce_root_disk_type_loader** / SCT_GCE_ROOT_DISK_TYPE_LOADER

Root disk type for loader nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-standard`: gce, gce-siren, k8s-gke


## **gce_root_disk_type_monitor** / SCT_GCE_ROOT_DISK_TYPE_MONITOR

Root disk type for monitor nodes in Google Compute Engine

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `pd-standard`: gce, gce-siren, k8s-gke


## **gce_setup_hybrid_raid** / SCT_GCE_SETUP_HYBRID_RAID

If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data

**default:** N/A

**type:** bool

**backend overrides:**
- `False`: gce, gce-siren, k8s-gke
