---
status: draft
domain: cluster
created: 2026-09-04
last_updated: 2026-09-04
owner: null
---

# Azure Multi-VNIC Support Plan

## Problem Statement

SCT can provision Scylla DB nodes with several network interfaces on AWS, GCE and OCI, and drive
which address Scylla binds and broadcasts through the `scylla_network_config` option. Azure is the
only VM backend left out: every Azure VM gets exactly one NIC, one private IPv4 and (optionally) one
public IPv4.

Concrete gaps this causes today:

- **No split-network coverage on Azure.** Tests that separate `listen_address` from `rpc_address`
  onto different NICs (`configurations/network_config/scylla_addresses_on_different_interfaces.yaml`,
  `configurations/network_config/two_interfaces.yaml`) cannot run on the Azure backend at all.
- **Three network nemesis are permanently skipped on Azure.** `sdcm/nemesis/monkey/network.py`
  gates them behind `extra_network_interface_precheck()`, and `AzureCluster` never sets
  `extra_network_interface`, so those disruptions have zero Azure coverage.
- **No IPv6 coverage on Azure.** `sdcm/cluster_azure.py:AzureNode._get_ipv6_ip_address()` is a stub
  that returns `""` with a `# todo: fix it` comment, so `ip_ssh_connections: ipv6` and every
  `ip_type: ipv6` entry in `scylla_network_config` are unusable on Azure. AWS and OCI both support it.
- **`AzureNode.network_interfaces` returns `None`.** Any framework code that walks interfaces
  (`sdcm/provision/network_configuration.py:ScyllaNetworkConfiguration`) breaks on Azure.

Azure is a supported production target for Scylla, and the multi-NIC/IPv6 topologies that customers
run there are currently untested by SCT.

## Current State

### Config layer

- `sdcm/sct_config.py:1389` — `scylla_network_config` (list). Its docstring still says
  *"Supported for AWS and GCE meanwhile"*. Each entry carries `address`, `ip_type`, `public`, `nic`,
  and optionally `listen_all` / `use_dns`.
- `sdcm/sct_config.py:1342` — `extra_network_interface` (bool), the legacy flag the network nemesis
  still gate on.
- `sdcm/sct_config.py:3526-3574` — validation block 17. Enforces the mandatory address list, rejects
  `ipv4 + public + nic != 0`, rejects `use_dns` on non-primary GCE NICs, and rejects multi-NIC in
  multi-region runs. There is no per-backend NIC-count validation here.
- `sdcm/sct_config.py:3520-3523` — validation block 16 restricts `use_dns_names` to `aws`, `gce`, `oci`.
- `sdcm/sct_config.py:2763-2800` — `backend_required_params`. The `aws` entry lists
  `scylla_network_config`; `azure` does not.
- `defaults/azure_config.yaml` — no `scylla_network_config` block at all (compare
  `defaults/gce_config.yaml:50-77` and `defaults/aws_config.yaml`).
- `sdcm/provision/network_configuration.py` — backend-agnostic. `NetworkInterface` dataclass,
  `ScyllaNetworkConfiguration`, `network_interfaces_count(params)` (counts distinct `nic` values in
  `scylla_network_config`), `ssh_connection_ip_type(params)`.

### Reference backends

- **AWS** — `sdcm/cluster_aws.py:254-273` builds one `NetworkInterfaces` entry per NIC from
  `self._ec2_subnet_id[dc_idx][az_idx][i]`, attaches the public IP only when there is a single
  interface, and post-attaches an EIP to device index 0 otherwise (`sdcm/cluster_aws.py:865-880`).
  `sdcm/cluster_aws.py:670-712` builds the `NetworkInterface` list, sorted by `device_index`.
- **GCE** — `sdcm/provision/gce/instance_provider.py:75-101` `build_network_interfaces()` puts the
  primary NIC on the auto-mode subnet with the external NAT access config and every secondary NIC in
  its own regional subnet. `sdcm/provision/gce/instance_provider.py:426-455`
  `_validate_network_interfaces_count()` + `max_network_interfaces()` (line 70) reject impossible
  counts *before* the insert call, using `GCE_MIN/MAX/SUPPORTED_NETWORK_INTERFACES` from
  `sdcm/provision/gce/constants.py:50-53`. `sdcm/cluster_gce.py:133-170` installs policy routing for
  secondary NICs and re-applies it after `start_network_interface()`.
- **OCI (the IPv6 reference)** — `sdcm/provision/oci/virtual_machine_provider.py:282-296`
  `_build_primary_vnic_details()` sets `assign_ipv6_ip=True` on every instance;
  `attach_secondary_vnics()` (line 597) creates one private subnet per `nic_index` and attaches a
  VNIC with `assign_ipv6_ip=True`; `get_vnic_ipv6_addresses()` (line 678) reads them back through
  `list_ipv6s`. `sdcm/cluster_oci.py:104-130` attaches the secondary VNICs, then
  `_configure_secondary_vnics_os()` (line 290) installs
  `sdcm/utils/oci_utils.py:SECONDARY_VNICS_SCRIPT` (line 1080) — an IMDS-driven script that installs
  addresses, source-based policy-routing rules and per-NIC routing tables for **both** IPv4 and IPv6 —
  plus the `sct-secondary-vnics` systemd oneshot that re-applies it on boot and after
  `start_network_interface()`. `sdcm/cluster_oci.py:170-192` `_discover_ipv6_from_os()` is the
  fallback used when the API has not published the IPv6 address yet.
  `sdcm/sct_runner.py:1678-1685` gives the OCI SCT runner `assign_ipv6_ip=True` unconditionally.

### Azure provisioning today

- `sdcm/provision/azure/provisioner.py:245-268` `_provision_resources()` — creates the resource group,
  one NSG, one VNet, **one** subnet, one IPv4 public IP per definition, **one** NIC per definition,
  then passes a flat `nics_ids` list to the VM provider.
- `sdcm/provision/azure/virtual_network_provider.py:44-62` — VNet `default`, address space
  `["10.0.0.0/16"]`, IPv4 only.
- `sdcm/provision/azure/subnet_provider.py:44-63` — subnet `default`, hardcoded prefix
  `"10.0.0.0/24"`, IPv4 only, NSG attached.
- `sdcm/provision/azure/network_interface_provider.py:48-88` — one `ip_configurations` entry per NIC,
  `enable_accelerated_networking=True`, optional IPv4 public IP. `get_nic_name()` is `f"{name}-nic"`.
- `sdcm/provision/azure/ip_provider.py:47-100` — already parameterised by `version` (`"IPV4"` /
  `"IPV6"`) with Standard SKU and static allocation, but only ever called with `"IPV4"`
  (`provisioner.py:250`). `_get_ip_name()` is `f"{name}-{version.lower()}"`.
- `sdcm/provision/azure/virtual_machine_provider.py:97-113` — `networkProfile.networkInterfaces` is
  built from a single `nic_id`, with no `primary` flag.
- `sdcm/provision/azure/provisioner.py:220-243, 275-283` — `_delete_stuck_node()` and
  `terminate_instance()` assume exactly one NIC and one public IP per VM.
- `sdcm/provision/azure/provisioner.py:330-333` — `_vm_to_instance()` reads
  `nic.ip_configurations[0].private_ip_address` of the single NIC.
- `sdcm/provision/azure/network_security_group_provider.py:27-47` — `rules_to_payload()` uses
  `source_address_prefix: "*"`, which already matches IPv6.

### Azure cluster/node layer

- `sdcm/cluster_azure.py:112-116` — `network_interfaces` returns `None`, `refresh_network_interfaces_info()`
  is a no-op.
- `sdcm/cluster_azure.py:184-187` — `_get_ipv6_ip_address()` returns `""` (`# todo: fix it`).
- `sdcm/cluster_azure.py:212-248` — `AzureCluster.__init__` never passes `extra_network_interface`
  to `cluster.BaseCluster` (compare `sdcm/cluster_gce.py:452`, `sdcm/cluster_oci.py:561`,
  `sdcm/cluster_aws.py:1136`).
- `sdcm/cluster_azure.py:206-208` — `private_dns_name` resolves through `resolve_ip_to_dns()` of the
  private IP; there is no per-NIC DNS name.

### Shared region resources and the SCT runner

- `sdcm/utils/azure_region.py:216-261` — `AzureRegion.create_sct_virtual_network()` /
  `create_sct_subnet()` build the long-lived `SCT-<region>` VNet (`10.0.0.0/16`) and subnet
  (`10.0.0.0/24`), IPv4 only. Both early-return when the resource already exists, so they never
  upgrade an existing VNet.
- `sdcm/utils/azure_region.py:464-475` — `configure()`, invoked by
  `sct.py:2856 prepare-regions --cloud-provider azure`.
- `sdcm/sct_runner.py:1237-1349` — `AzureSctRunner._create_instance()` builds an `InstanceDefinition`
  with `use_public_ip=True` and no NIC/IPv6 options. Compare `sdcm/sct_runner.py:1678-1685` (OCI).
- The runner lives in the shared `SCT-<region>` resource group/VNet while test nodes live in the
  per-test `SCT-<test_id>-<region>[-<az>]` group, so runner→node traffic always goes over public
  addresses.

### Wiring that already exists and can be reused

- `sdcm/provision/provisioner.py:59` — `InstanceDefinition.network_interfaces_count: int = 1`.
- `sdcm/sct_provision/region_definition_builder.py:157` — every backend, **Azure included**, already
  populates it from `network_interfaces_count(self.params)`. The Azure provisioner simply ignores it.
- `sdcm/cluster.py:6334` — `run_scylla_sysconfig_setup()` is already called for any backend when
  `network_interfaces_count(params) > 1`.
- `sdcm/cluster.py:1406-1426` — `BaseNode.network_configuration` returns the MAC → device-name map
  used by AWS/OCI to fill `NetworkInterface.device_name`.
- `sdcm/utils/azure_utils.py:170-211` — `_get_ip_configuration_dict()` and
  `list_known_virtual_machine_resources()`. The latter already iterates all NICs of a VM, but the
  former only ever returns `ipConfigurations[0]`, so a second (IPv6) ipConfiguration's public IP is
  invisible to cleanup.

### What's Missing

- A backend option describing the Azure NICs to create (subnet, public IPv4, IPv6, public IPv6).
- Multi-subnet, multi-NIC and dual-stack support across the five Azure providers.
- A `primary` flag on the VM's `networkProfile` and multi-NIC-aware teardown/cleanup.
- Guest-OS address and policy-routing configuration for Azure secondary NICs.
- `AzureNode.network_interfaces` / `_get_ipv6_ip_address()` implementations and
  `extra_network_interface` wiring on `AzureCluster`.
- NIC-count validation against the VM size (Azure `resource_skus` → `MaxNetworkInterfaces`).
- On-demand dual-stack shared region resources plus a conditional IPv6 address on the Azure SCT
  runner.
- Azure `scylla_network_config` defaults, test configurations, and docs.

## Goals

1. **Provision up to `MaxNetworkInterfaces` NICs per Azure DB VM**, driven by a new
   `azure_network_interfaces` option, with each secondary NIC in its own dedicated subnet of the
   test's VNet.
2. **Make `scylla_network_config` fully functional on the Azure backend** — a shared
   `configurations/network_config/*.yaml` profile runs unmodified on Azure, exactly as it does on
   AWS, once the Azure NIC layout it needs sits beside it.
3. **Support IPv6 on Azure DB nodes as an opt-in**, both private (VNet ULA prefix) and public
   (IPv6 Public IP resource), so `ip_type: ipv6` entries and `ip_ssh_connections: ipv6` work end to
   end — and **no IPv6 resource is created for a run that does not ask for one**. Unlike OCI, where
   `assign_ipv6_ip=True` is free, every internet-routable Azure IPv6 is a billed Public IP resource,
   so IPv6 is driven entirely by `azure_network_interfaces[*].ipv6` / `public_ipv6`.
4. **Give the Azure SCT runner an IPv6 address exactly when the test config enables IPv6**, so the
   runner can reach DB nodes over IPv6 whenever a test selects it and costs nothing when it does
   not.
5. **Keep monitoring connectivity intact** — Prometheus scrapes every DB/loader node over the address
   `scylla_network_config` selects, IPv4 or IPv6, in every supported profile.
6. **Fail fast on impossible NIC counts** — a request exceeding the VM size's `MaxNetworkInterfaces`
   is rejected before the create call, with a message naming the size, the limit and the request.
7. **Zero regression for single-NIC Azure runs** — existing Azure tests provision byte-for-byte
   equivalent resources (same NIC name, same public IP name) and existing cleanup tooling keeps working.

## Implementation Phases

### Phase 1: `azure_network_interfaces` config option and validation

**Importance**: Critical
**Description**: Introduce the option, its Azure default, and the cross-validation against
`scylla_network_config`. No provisioning behaviour changes yet, so the phase is independently
mergeable and testable.

The option describes *how each NIC is provisioned*; `scylla_network_config` keeps describing *which
NIC/IP Scylla uses*. Shape:

```yaml
azure_network_interfaces:
  - subnet: default      # subnet name within the test VNet; index 0 must stay on 'default'
    public_ip: true      # attach an IPv4 Public IP resource to this NIC
    ipv6: false          # add an IPv6 ipConfiguration from the VNet's IPv6 prefix
    public_ipv6: false   # attach an IPv6 Public IP resource to the IPv6 ipConfiguration
```

The NIC count is `len(azure_network_interfaces)`. `subnet` defaults to `default` for index 0 and
`nic<index>` for the rest.

**Deliverables**:
- `azure_network_interfaces: list` `SctField` in `sdcm/sct_config.py`, next to the other `azure_*`
  options (around line 1634), with a docstring covering all four keys.
- Single-NIC default block in `defaults/azure_config.yaml`, plus the Azure `scylla_network_config`
  default block mirroring `defaults/gce_config.yaml:50-77`.
- `scylla_network_config` added to the `azure` entry of `backend_required_params`
  (`sdcm/sct_config.py:2790`), and the option's docstring updated from
  *"Supported for AWS and GCE meanwhile"* to name OCI and Azure too.
- New validation, run only when `cluster_backend == "azure"`, in validation block 17:
  - `len(azure_network_interfaces) >= max(nic for nic in scylla_network_config) + 1`.
  - Every `ip_type: ipv6` entry targets a NIC with `ipv6: true`.
  - Every `ip_type: ipv6, public: true` entry targets a NIC with `public_ipv6: true`.
  - Every `ip_type: ipv4, public: true` entry targets a NIC with `public_ip: true` (complements the
    existing "public IPv4 only on nic 0" rule).
  - `ip_ssh_connections: ipv6` requires `public_ipv6: true` on NIC 0 — the SCT runner is outside the
    test VNet, so it can only reach a node over a routable IPv6.
  - `azure_network_interfaces[0].subnet` is `default`.
- `azure_ipv6_enabled(params) -> bool` helper in `sdcm/provision/network_configuration.py`, returning
  `any(nic.get("ipv6") for nic in params.get("azure_network_interfaces") or [])`. This is the single
  predicate every later phase gates IPv6 provisioning on — the providers (Phase 6), the shared region
  resources and the runner (Phase 7), and the monitor node (Phase 8). Because the validation above
  already forces every `ip_type: ipv6` entry onto a NIC with `ipv6: true`, this one flag covers
  `scylla_network_config` and `ip_ssh_connections` too.
- `unit_tests/unit/test_network_config.py` extended with the new cases.

**Definition of Done**:
- [ ] `azure_network_interfaces` appears in `uv run sct.py conf-docs` output
- [ ] Each of the six validation rules has a passing negative unit test asserting the error message
- [ ] `azure_ipv6_enabled()` is `False` for the Azure default config and for every Azure NIC
      layout except `all_addresses_ipv6_public.yaml`
- [ ] Every Azure NIC layout validates against the shared `configurations/network_config/` profile
      of the same name, and no layout ships without that check
- [ ] Omitting `azure_network_interfaces` on Azure yields the single-NIC default and changes nothing
- [ ] `docs/configuration_options.md` regenerated from `uv run sct.py conf-docs`
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 2: Multi-subnet and multi-NIC provisioning (IPv4)

**Importance**: Critical
**Description**: Teach the Azure providers to create N subnets and N NICs per VM and to attach them
all at VM creation. Azure only allows attaching a NIC to a *deallocated* VM, so unlike OCI (which
attaches VNICs to a running instance) all NICs must be in the create call — the AWS/GCE model.

**Dependencies**: Phase 1

**Deliverables**:
- `sdcm/provision/azure/subnet_provider.py`: `get_or_create(vnet_name, nsg_id, subnet_name, index)`
  derives the prefix as `10.0.<index>.0/24`, keeping `default` at `10.0.0.0/24` and attaching the
  same NSG to every subnet.
- `sdcm/provision/azure/network_interface_provider.py`: `get_or_create()` accepts a per-definition
  list of NIC specs and returns the NICs per definition. Naming keeps backward compatibility —
  `f"{name}-nic"` for index 0, `f"{name}-nic{index}"` for the rest — and adds
  `get_all(name) -> list[NetworkInterface]` ordered by index.
- `sdcm/provision/azure/ip_provider.py`: `_get_ip_name()` keeps `f"{name}-{version}"` for index 0 and
  uses `f"{name}-nic{index}-{version}"` for secondaries; public IPs are created only for NIC specs
  with `public_ip: true` **and** `definition.use_public_ip`.
- `sdcm/provision/azure/virtual_machine_provider.py`: `networkProfile.networkInterfaces` built from a
  list, with `"properties": {"primary": True, "deleteOption": "Detach"}` on index 0.
- `sdcm/provision/azure/provisioner.py`: `_provision_resources()` creates one subnet per NIC index and
  passes per-definition NIC lists; `_vm_to_instance()` reads the private IP from the **primary** NIC;
  `terminate_instance()` and `_delete_stuck_node()` delete every NIC and every public IP of the VM;
  `_reset_resource_providers()` unchanged.
- New `azure_network_interfaces` → NIC-spec plumbing: `InstanceDefinition` gains
  `network_interfaces: list[dict] | None` (kept alongside the existing
  `network_interfaces_count`), populated by
  `sdcm/sct_provision/azure/azure_region_definition_builder.py`.

**Needs Investigation**: whether the Azure VM sizes SCT uses for DB nodes (`Standard_L*s_v3`) allow
accelerated networking on *all* NICs or only a subset. `enable_accelerated_networking=True` is
currently unconditional in the NIC provider; if a size caps accelerated NICs, secondary NICs must
fall back to `False` rather than fail the create call.

**Definition of Done**:
- [ ] A 2-NIC Azure DB VM provisions with both NICs attached and the primary flagged
- [ ] A single-NIC run produces the same NIC name and public IP name as before this phase
- [ ] `terminate_instance()` leaves no orphaned NIC, subnet or public IP in the resource group
- [ ] Unit tests over `unit_tests/unit/provisioner/fake_azure_service.py` cover 1-NIC and 3-NIC
      provisioning and teardown
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 3: NIC-count validation against the VM size

**Importance**: Critical
**Description**: Reject an impossible NIC count before the create call, mirroring
`sdcm/provision/gce/instance_provider.py:_validate_network_interfaces_count()`. Azure publishes the
per-size limit as the `MaxNetworkInterfaces` capability of `compute.resource_skus.list()`.

**Dependencies**: Phase 2

**Deliverables**:
- `max_network_interfaces(vm_size, location)` in `sdcm/utils/azure_utils.py`, reading
  `MaxNetworkInterfaces` from `resource_skus` filtered by location, with an `lru_cache`.
- `AZURE_SUPPORTED_NETWORK_INTERFACES` constant capping what SCT itself supports (subnet CIDRs are
  carved out of a `/16` as `10.0.<index>.0/24`, so the practical ceiling is well above any VM size
  limit; the constant documents the intent and guards the CIDR maths).
- `_validate_network_interfaces_count()` on `AzureProvisioner`, called from `_provision_resources()`
  before any resource is created, raising `ProvisionError` naming the VM size, its limit and the
  requested count.
- `unit_tests/unit/test_azure_network_interfaces.py` (new), following
  `unit_tests/unit/test_gce_network_interfaces.py`.

**Definition of Done**:
- [ ] Requesting more NICs than the VM size allows raises `ProvisionError` before any Azure API call
      that creates a resource
- [ ] The error message names the VM size, the limit and the requested count
- [ ] The SKU lookup is cached — one `resource_skus.list()` call per (size, location) per run
- [ ] Unit tests cover under-limit, at-limit and over-limit
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 4: Guest-OS configuration for Azure secondary NICs

**Importance**: Critical
**Description**: Azure hands a secondary NIC an address over DHCP but installs no policy routing, so
replies sourced from a secondary NIC address leave through the primary default route and are dropped.
Install the same source-based policy routing OCI uses, driven by Azure IMDS.

**Dependencies**: Phase 2

**Deliverables**:
- `SECONDARY_NICS_SCRIPT`, `SECONDARY_NICS_SCRIPT_PATH` (`/usr/local/sbin/sct-configure-secondary-nics.sh`),
  `SECONDARY_NICS_SERVICE` (`sct-secondary-nics`) and `SECONDARY_NICS_SERVICE_UNIT_TMPL` in
  `sdcm/utils/azure_utils.py`, modelled on `sdcm/utils/oci_utils.py:1067-1200`. The script reads
  `http://169.254.169.254/metadata/instance/network/interface?api-version=2021-02-01` with the
  `Metadata: true` header, and for every non-primary interface installs the address, a
  `ip rule from <addr> lookup <table>` rule and a per-NIC routing table with the subnet route and a
  default route via the subnet's first usable address.
- `AzureNode._configure_secondary_nics_os()` and the `start_network_interface()` override that
  restarts the oneshot service, in `sdcm/cluster_azure.py`, mirroring
  `sdcm/cluster_oci.py:290-329`.
- Called from `AzureNode.init()` when `network_interfaces_count(params) > 1`.

**Needs Investigation**: how Azure IMDS reports the IPv6 gateway. OCI publishes
`ipv6VirtualRouterIp` directly; the Azure IMDS network schema exposes `ipv6.ipAddress[]` and
`ipv6.subnet[]` but no gateway field, so the IPv6 default route for a secondary NIC likely has to be
derived as the first address of the subnet prefix. Confirm on a live dual-stack VM before writing
the IPv6 half of the script (Phase 6 depends on it).

**Definition of Done**:
- [ ] On a 2-NIC Azure DB node, `ip rule show` lists a `from <secondary-ip> lookup <table>` rule and
      `ip route show table <table>` has the subnet and default routes
- [ ] `ping` from another node to the secondary NIC address succeeds
- [ ] The configuration survives a reboot and an interface down/up cycle
- [ ] A partially-configured NIC fails the script loudly instead of being skipped (OCI semantics)
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 5: `AzureNode` network-interface introspection

**Importance**: Critical
**Description**: Implement the node-side half so `ScyllaNetworkConfiguration` has real data on Azure.

**Dependencies**: Phase 4

**Deliverables**:
- `AzureNode.network_interfaces` builds a `NetworkInterface` list from the VM's NICs, ordered with
  the primary first, filling `device_name` from `BaseNode.network_configuration` (MAC → device map)
  the way `sdcm/cluster_oci.py:194-243` does. Cached, with `_invalidate_network_interfaces_cache()`
  and a `refresh_network_interfaces_info()` override.
- `AzureProvisioner`/`VmInstance` accessors for the NICs of a VM, so `cluster_azure` does not reach
  into `_nic_provider` directly.
- `AzureCluster.__init__` passes `extra_network_interface=network_interfaces_count(params) > 1` to
  `cluster.BaseCluster`, matching `sdcm/cluster_gce.py:452`.
- `AzureNode._refresh_instance_state()` returns the per-NIC public/private IPv4 lists when
  `scylla_network_configuration` is set, mirroring `sdcm/cluster_oci.py:333-348`.

**Definition of Done**:
- [ ] `AzureNode.network_interfaces` returns one `NetworkInterface` per NIC with `device_index`,
      `device_name` and `mac_address` populated
- [ ] A two-NIC layout produces a `scylla.yaml` with `listen_address` and `rpc_address` on
      different NIC addresses
- [ ] The three `extra_network_interface`-gated nemesis in `sdcm/nemesis/monkey/network.py` no longer
      skip on Azure
- [ ] Unit tests assert interface ordering and the MAC → device mapping
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 6: IPv6 for Azure DB nodes

**Importance**: Critical
**Description**: Make the per-test VNet dual-stack and give NICs an IPv6 ipConfiguration, with an
optional IPv6 Public IP — **only when `azure_ipv6_enabled(params)` is true**. Unlike OCI, where VCN
IPv6 addresses are globally routable GUAs handed out for free, Azure VNet IPv6 space is a private ULA
prefix and an internet-routable IPv6 is a separate, billed Public IP resource. So IPv6 is strictly
opt-in here: a run whose config does not ask for it must create no IPv6 address space, no IPv6
ipConfiguration and no IPv6 Public IP. The ULA/Public IP split maps cleanly onto
`NetworkInterface.ipv6_private_address` vs `NetworkInterface.ipv6_public_addresses`.

**Dependencies**: Phase 5

**Deliverables**:
- `sdcm/provision/azure/virtual_network_provider.py`: address space becomes
  `["10.0.0.0/16", "fd00:db8:5c7::/48"]` only when a NIC spec sets `ipv6: true`; otherwise it stays
  `["10.0.0.0/16"]` exactly as today.
- `sdcm/provision/azure/subnet_provider.py`: a subnet gets
  `address_prefixes: ["10.0.<i>.0/24", "fd00:db8:5c7:<i>::/64"]` (Azure requires exactly `/64`) only
  when the NIC spec for that index sets `ipv6: true`; the others stay single-prefix IPv4.
- `sdcm/provision/azure/network_interface_provider.py`: a second ipConfiguration with
  `private_ip_address_version: "IPv6"` and, when `public_ipv6: true`, an IPv6 Public IP; the IPv4
  ipConfiguration stays `primary: true` (Azure requires an IPv4 primary).
- `sdcm/provision/azure/ip_provider.py`: called with `version="IPV6"` for those NICs — the code path
  already exists and needs only the caller.
- `sdcm/provision/azure/provisioner.py`: IPv6 addresses surfaced on `VmInstance`.
- `AzureNode._get_ipv6_ip_address()` returns
  `self.scylla_network_configuration.interface_ipv6_address` when a network config is set, replacing
  the `# todo: fix it` stub; falls back to the primary NIC's IPv6 otherwise.
- IPv6 half of the Phase 4 guest script (addresses, `ip -6 rule`, per-NIC IPv6 routes), gated on the
  gateway question resolved in Phase 4.
- `sdcm/utils/azure_utils.py:_get_ip_configuration_dict()` returns **all** ipConfigurations so
  `list_known_virtual_machine_resources()` also collects IPv6 public IPs for deletion; a matching fix
  in `get_virtual_machine_ips()`.
- An OS-level IPv6 discovery fallback on `AzureNode`, equivalent to
  `sdcm/cluster_oci.py:170-192 _discover_ipv6_from_os()`, for the window before the API publishes the
  address.

**Definition of Done**:
- [ ] A DB node provisioned with `ipv6: true` has an IPv6 address in `ip -6 addr show scope global`
- [ ] `all_addresses_ipv6_public.yaml` runs on Azure: Scylla binds and broadcasts IPv6, and the
      cluster forms
- [ ] `ip_ssh_connections: ipv6` reaches the node from the SCT runner
- [ ] Terminating a dual-stack VM leaves no orphaned IPv6 public IP
- [ ] A run with `azure_ipv6_enabled() == False` creates no IPv6 address space, no IPv6
      ipConfiguration and no IPv6 Public IP — asserted against the fake Azure service, not just
      observed manually
- [ ] `public_ipv6: false` with `ipv6: true` yields a ULA-only NIC and bills no Public IP
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 7: On-demand IPv6 for the shared region resources and the SCT runner

**Importance**: Critical
**Description**: The runner sits in the shared, long-lived `SCT-<region>` VNet, which is IPv4-only,
so it cannot reach a DB node over IPv6 even after Phase 6. Give it IPv6 **only when the test config
enables it**. A runner is created fresh for a single test and receives that test's full
`SCTConfiguration` (`sct.py:2958` builds it and passes it through `get_sct_runner()` into
`AzureSctRunner.__init__`), so `azure_ipv6_enabled(params)` is always evaluated against the config
the runner will actually serve. That makes the conditional safe with no runtime check to fall back
on, and no unconditional dual-stack shared infrastructure is needed.

Rather than upgrading the existing `default` subnet, the dual-stack resources are added **alongside**
it: the IPv6 address space is added to the shared VNet and a second subnet (`sct-subnet-ipv6`) is
created on demand, the way the OCI runner already lazily creates its public subnet
(`sdcm/sct_runner.py:1657-1671`). Runners that do not need IPv6 keep landing on the untouched
`default` subnet, so an IPv4-only region is never modified and no Public IP is billed for them.

**Dependencies**: Phase 6

**Deliverables**:
- `sdcm/utils/azure_region.py`: `sct_ipv6_subnet_name`, `create_sct_ipv6_subnet()` and an
  `ensure_ipv6_address_space()` that adds the IPv6 prefix to the shared VNet idempotently. The
  existing `create_sct_virtual_network()` / `create_sct_subnet()` are left IPv4-only and unchanged.
- `sdcm/utils/azure_region.py:configure()` gains an `ipv6: bool = False` parameter, surfaced as
  `hydra prepare-regions --cloud-provider azure --ipv6`, so a region can be pre-provisioned ahead of
  a scheduled IPv6 job instead of paying the creation latency on the first run.
- `sdcm/sct_runner.py:AzureSctRunner._create_instance()`: when `azure_ipv6_enabled(self.params)`,
  ensure the dual-stack subnet exists, place the runner NIC on it and request an IPv6
  ipConfiguration with an IPv6 Public IP. Otherwise the code path is byte-for-byte what it is today.
- `docs/sct-runners.md`: the IPv6 opt-in and the `--ipv6` flag.

**Needs Investigation**: whether Azure permits adding an IPv6 address space to an existing VNet that
already has IPv4 subnets with attached NICs. Adding a *subnet* is uncontroversial; adding the
address space to a live VNet is the open question. If it is rejected, the fallback is a separate
dual-stack VNet in the same shared resource group, which the runner joins instead — still additive,
still leaving the IPv4 VNet untouched.

**Definition of Done**:
- [ ] With IPv6 disabled, creating an Azure runner touches no IPv6 resource and leaves the shared
      region resources bit-identical to before this phase
- [ ] With IPv6 enabled, a newly created Azure SCT runner has a global IPv6 address
- [ ] The runner reaches a DB node's public IPv6 on port 22 and 9042
- [ ] `hydra prepare-regions --cloud-provider azure --ipv6 -r <region>` is idempotent on both a fresh
      and a pre-existing region, and without `--ipv6` changes nothing about IPv6
- [ ] Existing IPv4-only runners keep working unchanged
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 8: Monitoring and loader connectivity

**Importance**: Critical
**Description**: Confirm and, where needed, fix the paths monitoring uses to reach DB and loader nodes
in every supported profile. Monitor and DB nodes share the per-test VNet, so intra-VNet routing
covers the secondary subnets automatically; the risks are NSG scope, the reply path from a secondary
NIC (Phase 4) and IPv6 target formatting.

**Dependencies**: Phase 6

**Deliverables**:
- Verification that `sdcm/cluster.py:7744-7751` Prometheus targets, built from
  `DB_NODES_IP_ADDRESS = "ip_address"` (i.e. `ScyllaNetworkConfiguration.broadcast_address`), resolve
  to a reachable address on every NIC and both address families; `normalize_ipv6_url()` is already
  applied there and at line 7560.
- NSG coverage check for the secondary subnets — the current `source_address_prefix: "*"` in
  `sdcm/provision/azure/network_security_group_provider.py:27-47` matches IPv6, so this is expected
  to be a test-only deliverable; any gap found becomes an explicit rule.
- Monitor node given an IPv6 address **only** when `azure_ipv6_enabled(params)` — it shares the test
  VNet with the DB nodes, so a ULA ipConfiguration is enough and no IPv6 Public IP is needed for it.
- Grafana/Prometheus reachability assertion added to the Azure multi-NIC integration test.

**Definition of Done**:
- [ ] Prometheus shows all DB and loader targets `UP` in a 2-NIC Azure run
- [ ] Prometheus shows all targets `UP` in an IPv6 Azure run
- [ ] The monitor node gets no IPv6 ipConfiguration in an IPv4-only run
- [ ] Grafana is reachable from the SCT runner in both
- [ ] Node exporter (9100) and Scylla Prometheus API (9180) are scraped over the address
      `scylla_network_config` selects, not just the primary NIC
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 9: Test configurations, CI and documentation

**Importance**: Important
**Description**: Make the feature reachable from pipelines and documented.

**Dependencies**: Phase 8

**Deliverables**:
- `configurations/azure/network_config/*.yaml` — an Azure `azure_network_interfaces` block for the
  shared `configurations/network_config/` profile a pipeline actually loads. Only the profile the
  job below uses ships here; the rest get a layout when a job needs one, so nothing untested and
  unused accumulates.
- An Azure pipeline that exercises multi-NIC/IPv6 end to end: a counterpart of
  `longevity-multidc-schema-topology-changes-12h-oci`, which pairs multi-DC with routable IPv6.
- `docs/configuration_options.md` regenerated; a new `docs/azure_configuration.md` section on
  multi-NIC and IPv6, in the spirit of `docs/aws_configuration.md`.
- `provision-azure` label added to the new test cases per the repo's provision-label convention.
- `AGENTS.md` / skill guidance updated if the new option changes how configs are written.

**Definition of Done**:
- [ ] `uv run sct.py lint-pipelines` passes for the new Azure job configuration
- [ ] The Azure multi-NIC job is defined in `jenkins-pipelines/` and runs green once
- [ ] `docs/configuration_options.md` documents `azure_network_interfaces` with all four keys
- [ ] `uv run sct.py pre-commit` passes

## Testing Requirements

### Unit Tests

- `unit_tests/unit/test_network_config.py` — the five Phase 1 validation rules, positive and negative,
  plus `network_interfaces_count()` against Azure configs.
- `unit_tests/unit/test_azure_network_interfaces.py` (new) — NIC-spec → Azure parameter translation,
  subnet CIDR derivation for indices 0..N, NIC/public-IP naming backward compatibility,
  `max_network_interfaces()` and the over-limit `ProvisionError`.
- `unit_tests/unit/provisioner/` — extend `fake_azure_service.py` so `FakeNetworkInterface`,
  `FakeSubnet` and `FakeIpAddress` handle multiple NICs, multiple subnets and IPv6, then cover
  1-NIC/2-NIC/3-NIC provisioning, dual-stack provisioning and full teardown. One test asserts the
  negative case explicitly: with `azure_ipv6_enabled() == False`, the recorded Azure calls contain no
  IPv6 address prefix, no `IPv6` ipConfiguration and no `IPV6` public IP.
- `unit_tests/unit/test_cluster.py` — `AzureNode.network_interfaces` ordering and MAC → device mapping
  with a mocked provisioner, mirroring `unit_tests/unit/test_cluster_oci.py:436-500`.

Run with `uv run sct.py unit-tests -t <file>`.

### Integration Tests

- A real Azure provision/teardown test under the `writing-integration-tests` skill's service-labelled
  conventions, skipped without Azure credentials:
  - 1 NIC (regression baseline), 2 NICs, dual-stack 2 NICs.
  - Assert NIC count, subnet membership, address families, and that teardown leaves no orphaned NIC,
    subnet or public IP.
  - Assert the IPv4-only cases create zero IPv6 resources in the resource group.
- `uv run sct.py integration-tests`.

### Manual Testing

- Full longevity run on Azure with a two-NIC layout: cluster forms, nemesis run, Prometheus
  targets all `UP`, Grafana reachable, and `listen_address` and `rpc_address` land on different
  NICs in the rendered `scylla.yaml`.
- Full run with `all_addresses_ipv6_public.yaml` on Azure with `ip_ssh_connections: ipv6`.
- The three `extra_network_interface` network nemesis execute rather than skip.
- Reboot a 2-NIC node and confirm policy routing is re-applied by `sct-secondary-nics.service`.
- `hydra prepare-regions --cloud-provider azure` against a region prepared before this work, then
  create a runner in it — once without `--ipv6` (nothing about IPv6 changes) and once with it.
- Create an Azure runner for an IPv4-only config and confirm it has no IPv6 address and that the
  shared region resources are unchanged.

### Performance Testing

Not applicable — no hot-path changes. One check only: multi-NIC provisioning must not add more than
~2 minutes to Azure cluster setup, since NIC and subnet creation are additional serialised Azure API
round trips.

## Success Criteria

All Definition of Done items across the nine phases are met. Additionally:

1. A `configurations/network_config/*.yaml` profile paired with an Azure NIC layout runs on Azure
   with results equivalent to the AWS run of the same profile.
2. Existing single-NIC Azure pipelines show no change in provisioning time, resource naming, or
   cleanup behaviour.
3. No Azure run that does not enable IPv6 creates a single IPv6 resource — no VNet/subnet IPv6
   prefix, no IPv6 ipConfiguration, no IPv6 Public IP — on DB nodes, monitors, loaders or the SCT
   runner.

## Risk Mitigation

### Risk: Azure NICs cannot be attached to a running VM

**Likelihood**: High (this is documented Azure behaviour)
**Impact**: The OCI approach — attach secondary VNICs after the instance is running — is unavailable,
so a NIC-count change forces a full VM recreate and any post-hoc NIC repair is impossible.
**Mitigation**: Follow the AWS/GCE model and put every NIC in the create call (Phase 2). Validate the
NIC count before creating any resource (Phase 3) so a bad count fails in seconds rather than after a
VM exists.

### Risk: IPv6 address space cannot be added to an existing shared VNet

**Likelihood**: Medium
**Impact**: The shared `SCT-<region>` VNet cannot be given IPv6 space, blocking Phase 7 and therefore
IPv6 runner connectivity.
**Mitigation**: Flagged as "Needs Investigation" in Phase 7. Fallback is a separate dual-stack VNet in
the same shared resource group that IPv6 runners join. Either way the change is additive and the
IPv4-only path is untouched, so IPv4 runs and already-running runners are unaffected.

### Risk: unnecessary IPv6 Public IP spend

**Likelihood**: Low
**Impact**: Every internet-routable Azure IPv6 is a billed Standard SKU Public IP; a default that
leaks IPv6 into IPv4-only runs would add a per-NIC charge across the whole Azure fleet.
**Mitigation**: `azure_ipv6_enabled()` gates every IPv6 resource, its default is `False`, and Phases 6
and 7 each carry a DoD item asserting that an IPv6-disabled run creates no IPv6 resource — checked
against the fake Azure service in unit tests, not only by manual observation.

### Risk: Azure IMDS does not publish an IPv6 gateway for secondary NICs

**Likelihood**: Medium
**Impact**: The Phase 4 guest script cannot install an IPv6 default route per NIC the way the OCI
script does with `ipv6VirtualRouterIp`, so IPv6 traffic sourced from a secondary NIC breaks.
**Mitigation**: Flagged as "Needs Investigation" in Phase 4, to be answered on a live dual-stack VM
before the IPv6 half of the script is written. Fallback is deriving the gateway as the first address
of the subnet's IPv6 prefix, which SCT already knows because it created the subnet.

### Risk: Accelerated networking is not available on every NIC of a VM size

**Likelihood**: Medium
**Impact**: `enable_accelerated_networking=True` is unconditional today; if a size caps the number of
accelerated NICs, VM creation fails with an opaque Azure error.
**Mitigation**: Flagged as "Needs Investigation" in Phase 2. Fallback is enabling it only on the
primary NIC, which costs some secondary-NIC throughput but keeps provisioning working.

### Risk: Multi-NIC teardown leaks Azure resources

**Likelihood**: Medium
**Impact**: Orphaned NICs, subnets and public IPs accumulate in the subscription and eventually hit
quota, breaking unrelated runs.
**Mitigation**: Phase 2 makes `terminate_instance()` and `_delete_stuck_node()` iterate all NICs;
Phase 6 fixes `_get_ip_configuration_dict()` so IPv6 public IPs are visible to
`list_known_virtual_machine_resources()`. Both have explicit "no orphaned resource" DoD items, and
the per-test resource group deletion in `cleanup()` remains the backstop.

### Risk: `use_dns_names` on Azure

**Likelihood**: Low
**Impact**: Azure publishes an internal DNS name only for the primary NIC (as GCE does), so a DNS
name on a secondary NIC resolves back to the primary — silently wrong addresses in `scylla.yaml`.
**Mitigation**: Out of scope. `sdcm/sct_config.py:3520-3523` already restricts `use_dns_names` to
`aws`, `gce` and `oci`, so Azure is rejected today; Phase 1 leaves that list untouched and documents
the reason.

### Risk: Multi-region Azure runs

**Likelihood**: Low
**Impact**: Multi-NIC and multi-region are mutually exclusive today
(`sdcm/sct_config.py:3570-3573` rejects the combination for all backends).
**Mitigation**: Inherit the existing restriction unchanged; Azure multi-NIC is single-region only,
same as AWS and GCE.

## Related Plans

- [multi-cloud-provisioning-resilience.md](multi-cloud-provisioning-resilience.md) — touches the same
  Azure provisioner; coordinate on `_provision_resources()` changes.

## PR History

| Phase | PR | Status |
|-------|-----|--------|
| Phase 1: config option and validation | — | Not started |
| Phase 2: multi-subnet/multi-NIC provisioning | — | Not started |
| Phase 3: NIC-count validation | — | Not started |
| Phase 4: guest-OS secondary NIC config | — | Not started |
| Phase 5: `AzureNode` interface introspection | — | Not started |
| Phase 6: IPv6 for DB nodes | — | Not started |
| Phase 7: on-demand IPv6 for region resources and runner | — | Not started |
| Phase 8: monitoring connectivity | — | Not started |
| Phase 9: test configs, CI, docs | — | Not started |
