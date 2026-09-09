# Azure backend

[← All configuration options](configuration_options.md)

Microsoft Azure provisioning.

**13 options.**


## **azure_image_db** / SCT_AZURE_IMAGE_DB

The Azure image to be used for database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_image_db_oracle** / SCT_AZURE_IMAGE_DB_ORACLE

The Azure image to be used for oracle (2nd ref cluster) DB nodes. If not set and [`oracle_scylla_version`](auxiliary-db-cluster-oracle-cassandra.md#oracle_scylla_version) is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-26_04-lts:{arch_sku}:latest`: azure


## **azure_image_monitor** / SCT_AZURE_IMAGE_MONITOR

The Azure image to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-24_04-lts:server:latest`: azure


## **azure_image_username** / SCT_AZURE_IMAGE_USERNAME

The username for the Azure image.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: azure


## **azure_instance_type_db** / SCT_AZURE_INSTANCE_TYPE_DB

The Azure virtual machine size to be used for database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_db_oracle** / SCT_AZURE_INSTANCE_TYPE_DB_ORACLE

The Azure virtual machine size to be used for Oracle database nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_loader** / SCT_AZURE_INSTANCE_TYPE_LOADER

The Azure virtual machine size to be used for loader nodes.

**default:** N/A

**type:** str (appendable)


## **azure_instance_type_monitor** / SCT_AZURE_INSTANCE_TYPE_MONITOR

The Azure virtual machine size to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)


## **azure_provision_stuck_vm_recreate_attempts** / SCT_AZURE_PROVISION_STUCK_VM_RECREATE_ATTEMPTS

How many times to recreate a stuck Azure VM (full node: VM, NIC and public IP) onto<br>fresh capacity before giving up with a non-retryable error.

**default:** N/A

**type:** int

**backend overrides:**
- `3`: azure


## **azure_provision_stuck_vm_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TIMEOUT

Seconds to wait for an Azure VM to reach the 'Succeeded' provisioning state before<br>treating it as stuck (accepted by Azure but never started by the host - SCT-434) and<br>recreating it. Detection is gated on the polled instanceView provisioning state.

**default:** N/A

**type:** int

**backend overrides:**
- `900`: azure


## **azure_provision_stuck_vm_total_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TOTAL_TIMEOUT

Total timeout (seconds) for the whole stuck-VM recovery attempts.<br>Recovery stops with a non-retryable error when either this timeout or<br>[`azure_provision_stuck_vm_recreate_attempts`](#azure_provision_stuck_vm_recreate_attempts) is exhausted. This way a degraded Azure<br>region cannot keep provisioning running until the CI stage times out SCT.<br>This value must be at least [`azure_provision_stuck_vm_timeout`](#azure_provision_stuck_vm_timeout), otherwise SCT may<br>give up during the initial wait without making even one recreate attempt.

**default:** N/A

**type:** int

**backend overrides:**
- `4500`: azure


## **azure_region_name** / SCT_AZURE_REGION_NAME

Azure region(s) where the resources will be deployed. Supports single or multiple regions.

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eastus']`: azure
