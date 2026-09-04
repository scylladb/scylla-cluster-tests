# Azure backend

[← All configuration options](../configuration_options.md)

Microsoft Azure provisioning.

**14 options.**


<a id="azure_image_db"></a>

## **azure_image_db** / SCT_AZURE_IMAGE_DB

The Azure image to be used for database nodes.

**default:** N/A

**type:** str (appendable)


<a id="azure_image_db_oracle"></a>

## **azure_image_db_oracle** / SCT_AZURE_IMAGE_DB_ORACLE

The Azure image to be used for oracle (2nd ref cluster) DB nodes. If not set and [`oracle_scylla_version`](auxiliary-db-cluster-oracle-cassandra.md#oracle_scylla_version) is provided, it will be resolved automatically.

**default:** N/A

**type:** str (appendable)


<a id="azure_image_loader"></a>

## **azure_image_loader** / SCT_AZURE_IMAGE_LOADER

The Azure image to be used for loader nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-26_04-lts:{arch_sku}:latest`: azure


<a id="azure_image_monitor"></a>

## **azure_image_monitor** / SCT_AZURE_IMAGE_MONITOR

The Azure image to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `Canonical:ubuntu-24_04-lts:server:latest`: azure


<a id="azure_image_username"></a>

## **azure_image_username** / SCT_AZURE_IMAGE_USERNAME

The username for the Azure image.

**default:** N/A

**type:** str (appendable)

**backend overrides:**
- `scyllaadm`: azure


<a id="azure_instance_type_db"></a>

## **azure_instance_type_db** / SCT_AZURE_INSTANCE_TYPE_DB

The Azure virtual machine size to be used for database nodes.

**default:** N/A

**type:** str (appendable)


<a id="azure_instance_type_db_oracle"></a>

## **azure_instance_type_db_oracle** / SCT_AZURE_INSTANCE_TYPE_DB_ORACLE

The Azure virtual machine size to be used for Oracle database nodes.

**default:** N/A

**type:** str (appendable)


<a id="azure_instance_type_loader"></a>

## **azure_instance_type_loader** / SCT_AZURE_INSTANCE_TYPE_LOADER

The Azure virtual machine size to be used for loader nodes.

**default:** N/A

**type:** str (appendable)


<a id="azure_instance_type_monitor"></a>

## **azure_instance_type_monitor** / SCT_AZURE_INSTANCE_TYPE_MONITOR

The Azure virtual machine size to be used for monitor nodes.

**default:** N/A

**type:** str (appendable)


<a id="azure_network_interfaces"></a>

## **azure_network_interfaces** / SCT_AZURE_NETWORK_INTERFACES

Describes how each Azure network interface of a DB node is provisioned.<br>One list item per NIC, ordered by device index. Where [`scylla_network_config`](scylla-installation-and-configuration.md#scylla_network_config) says<br>which NIC/IP Scylla uses, this option says how that NIC is built. Keys per item:<br>- subnet: name of the subnet inside the test VNet. Index 0 must stay on 'default';<br>the other indexes default to 'nic<index>'<br>- public_ip: attach an IPv4 Public IP resource to this NIC (only valid on index 0)<br>- ipv6: add an IPv6 ipConfiguration from the VNet's IPv6 (ULA) prefix<br>- public_ipv6: attach an IPv6 Public IP resource to the IPv6 ipConfiguration<br>An Azure IPv6 address is a billed Public IP resource, so IPv6 is strictly opt-in:<br>leaving 'ipv6' false everywhere creates no IPv6 resource at all.<br>The number of NICs to create is the length of this list.

**default:** N/A

**type:** list

**backend overrides:**
- `[{'subnet': 'default', 'public_ip': True, 'ipv6': False, 'public_ipv6': False}]`: azure


<a id="azure_provision_stuck_vm_recreate_attempts"></a>

## **azure_provision_stuck_vm_recreate_attempts** / SCT_AZURE_PROVISION_STUCK_VM_RECREATE_ATTEMPTS

How many times to recreate a stuck Azure VM (full node: VM, NIC and public IP) onto<br>fresh capacity before giving up with a non-retryable error.

**default:** N/A

**type:** int

**backend overrides:**
- `3`: azure


<a id="azure_provision_stuck_vm_timeout"></a>

## **azure_provision_stuck_vm_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TIMEOUT

Seconds to wait for an Azure VM to reach the 'Succeeded' provisioning state before<br>treating it as stuck (accepted by Azure but never started by the host - SCT-434) and<br>recreating it. Detection is gated on the polled instanceView provisioning state.

**default:** N/A

**type:** int

**backend overrides:**
- `900`: azure


<a id="azure_provision_stuck_vm_total_timeout"></a>

## **azure_provision_stuck_vm_total_timeout** / SCT_AZURE_PROVISION_STUCK_VM_TOTAL_TIMEOUT

Total timeout (seconds) for the whole stuck-VM recovery attempts.<br>Recovery stops with a non-retryable error when either this timeout or<br>[`azure_provision_stuck_vm_recreate_attempts`](#azure_provision_stuck_vm_recreate_attempts) is exhausted. This way a degraded Azure<br>region cannot keep provisioning running until the CI stage times out SCT.<br>This value must be at least [`azure_provision_stuck_vm_timeout`](#azure_provision_stuck_vm_timeout), otherwise SCT may<br>give up during the initial wait without making even one recreate attempt.

**default:** N/A

**type:** int

**backend overrides:**
- `4500`: azure


<a id="azure_region_name"></a>

## **azure_region_name** / SCT_AZURE_REGION_NAME

Azure region(s) where the resources will be deployed. Supports single or multiple regions.

**default:** N/A

**type:** str | list[str] → list[str]

**backend overrides:**
- `['eastus']`: azure
