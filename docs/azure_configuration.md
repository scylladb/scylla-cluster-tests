## Multiple network interfaces on Azure

SCT can give an Azure DB node more than one network interface, so tests can separate Scylla's
internal traffic from its client traffic the way they already do on AWS, GCE and OCI.

There is one option, the backend-independent `scylla_network_config`, and the profiles under
`configurations/network_config/` are shared with the other backends. SCT derives the Azure NIC
layout from it:

| Derived from `scylla_network_config` | Rule |
|---|---|
| number of interfaces | the number of distinct `nic` indexes |
| subnet | `default` for nic 0, `nic<index>` for the others |
| IPv4 Public IP | the primary NIC only |
| IPv6 ipConfiguration | every NIC carrying an `ip_type: ipv6` address |
| IPv6 Public IP | every NIC carrying an `ip_type: ipv6` address that is also `public: true` |

So a shared profile is enough on its own:

```
--config configurations/network_config/two_interfaces.yaml
```

`configurations/azure/network_config/` holds an override only where Azure cannot honour a profile as
written - see the routable-IPv6 case below. It is loaded after the profile of the same name.

What is left to validate at config time is what Azure cannot place where the profile asks for it: a
public IPv4 outside the primary NIC, or a gap in the `nic` indexes. Both fail immediately instead of
while the node boots.

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

IPv6 is opt-in. Unlike OCI, where a VCN IPv6 address comes free with the VNIC and is globally
routable, an Azure VNet's IPv6 space is a private ULA range and an internet-routable IPv6 is a
**separate, billed Public IP resource**. So:

- an `ip_type: ipv6` address makes its interface's subnet dual-stack and adds a VNet-local IPv6
  address. That is enough when only nodes inside the VNet need to talk over IPv6.
- adding `public: true` to it attaches an IPv6 Public IP as well, which is what makes the node
  reachable from outside the VNet.

A run with no `ip_type: ipv6` address creates no IPv6 resource at all: no VNet or subnet IPv6
prefix, no IPv6 ipConfiguration and no IPv6 Public IP.

An IPv6 `test_communication` therefore has to be `public: true` — SCT connects from outside the
node's subnet, so a VNet-local address cannot reach it. The SCT runner picks its own IPv6 up
automatically whenever the configuration enables it; see [sct-runners.md](sct-runners.md).

Azure requires the primary ipConfiguration of a NIC to be IPv4, so an IPv6-enabled interface is
always dual-stack, never IPv6-only.
