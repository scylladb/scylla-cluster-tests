## Multiple network interfaces on Azure

SCT can give an Azure DB node more than one network interface, so tests can separate Scylla's
internal traffic from its client traffic the way they already do on AWS, GCE and OCI.

Two options work together:

- `scylla_network_config` says **which** interface and address family each Scylla address uses. It is
  backend-independent, and the profiles under `configurations/network_config/` are shared.
- `azure_network_interfaces` says **how** each interface is provisioned. It is Azure-specific, one
  list item per device index:

```yaml
azure_network_interfaces:
  - subnet: default      # index 0 must stay on 'default'; the others default to 'nic<index>'
    public_ip: true      # attach an IPv4 Public IP resource (only valid on index 0)
    ipv6: false          # add an IPv6 ipConfiguration from the VNet's IPv6 (ULA) prefix
    public_ipv6: false   # attach an IPv6 Public IP resource to the IPv6 ipConfiguration
  - subnet: nic1
    public_ip: false
```

The number of interfaces is the length of the list. Every key is optional; the defaults above are
what an omitted key gets.

Pair a shared profile with its Azure layout, for example:

```
--config configurations/network_config/all_addresses_ipv6_public.yaml
--config configurations/azure/network_config/all_addresses_ipv6_public.yaml
```

`configurations/azure/network_config/` holds a layout only for the profiles a pipeline loads, so a
profile without one there does not run on Azure yet - add the layout beside it, with the same file
name, and the unit tests pick it up.

SCT cross-validates the two at config time, so an address asking for something its interface was not
built with (an IPv6 address on an IPv4-only NIC, a public address on a NIC with no Public IP) fails
immediately instead of while the node boots.

### What gets multiple interfaces

Only DB nodes. Loaders and monitors keep a single interface: they reach the secondary subnets over
intra-VNet routing, and their smaller VM sizes often accept fewer NICs than a DB node's.

Every interface is created with the VM. Azure only accepts a NIC attachment on a *deallocated* VM,
so unlike OCI, SCT cannot add one to a running node. The number of interfaces is validated against
the VM size's `MaxNetworkInterfaces` before any resource is created.

Azure gives a secondary NIC an address over DHCP but installs no routing policy for it, so SCT
installs source-based policy routing on the node (`sct-secondary-nics.service`), re-applied on every
boot and after an interface restart.

## IPv6 on Azure

IPv6 is opt-in, driven entirely by `azure_network_interfaces`. Unlike OCI, where a VCN IPv6 address
comes free with the VNIC and is globally routable, an Azure VNet's IPv6 space is a private ULA range
and an internet-routable IPv6 is a **separate, billed Public IP resource**. So:

- `ipv6: true` makes the interface's subnet dual-stack and adds a VNet-local IPv6 address. Use this
  when only nodes inside the VNet need to talk over IPv6.
- `public_ipv6: true` additionally attaches an IPv6 Public IP, which is what makes the node reachable
  from outside the VNet.

A run that sets neither creates no IPv6 resource at all: no VNet or subnet IPv6 prefix, no IPv6
ipConfiguration and no IPv6 Public IP.

`ip_ssh_connections: ipv6` requires `public_ipv6: true` on the first interface — SCT connects from
outside the node's subnet, so a VNet-local address cannot reach it. The SCT runner picks its own
IPv6 up automatically whenever the configuration enables it; see [sct-runners.md](sct-runners.md).

Azure requires the primary ipConfiguration of a NIC to be IPv4, so an IPv6-enabled interface is
always dual-stack, never IPv6-only.
