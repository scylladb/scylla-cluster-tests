# Mini-Plan: SCT-310 — Align GCE Network Configuration with the OCI/AWS Pattern

**Date:** 2026-08-31 (synced with the merged OCI work 2026-09-01)
**Estimated LOC:** ~200 for this PR; see Follow-ups A and B for the tracked remainder
**Related PR:** TBD
**Jira:** [SCT-310](https://scylladb.atlassian.net/browse/SCT-310)

## Problem

SCT-310 asks to align GCP network provisioning with the per-address `scylla_network_config`
pattern introduced for AWS in [PR #6575](https://github.com/scylladb/scylla-cluster-tests/pull/6575).

An audit of `master` shows the bulk of that scope already landed after the ticket was filed:

| Commit | What it delivered |
|--------|-------------------|
| `c3e00544c` | `_build_scylla_network_configuration()` / `refresh_network_interfaces_info()` moved to `BaseNode` (`sdcm/cluster.py:1439-1456`); AWS lost 68 lines of duplicated logic |
| `3981444c1` | GCE multiple network interfaces (SCT-308): secondary subnets, per-machine-type NIC-count validation, `GCENode.network_interfaces` |
| `437d0efcf` | Secondary-NIC policy routing on GCE |

`defaults/gce_config.yaml:50-77` already ships a full `scylla_network_config` block, and GCE
nemesis jobs already run `configurations/network_config/two_interfaces.yaml`
(`jenkins-pipelines/oss/nemesis/longevity-5gb-1h-*NetworkMonkey-gce.jenkinsfile`).

Three real gaps remain, and they are what this plan covers:

1. **IPv6 is unimplemented on GCE and fails silently.** `GCENode.network_interfaces` hardcodes
   `ipv6_public_addresses=[]` and `ipv6_private_address=""` (`sdcm/cluster_gce.py:271-273`), and
   `_get_ipv6_ip_address()` returns `""` behind a stale warning (`sdcm/cluster_gce.py:364-370`).
   Running `configurations/network_config/all_addresses_ipv6_public.yaml` on GCE makes
   `ScyllaNetworkConfiguration.get_ip_by_address_config()` return `None`, so the run fails far from
   the cause instead of at config validation.

   Re-checked on 2026-09-01 against the recent IPv6 work, which does **not** close this: `ff781b5ab`
   gave `BaseNode._get_ipv6_ip_address()` a generic implementation returning
   `scylla_network_configuration.interface_ipv6_address`, but `GCENode` still overrides it with the
   `""` stub; and `63a5510dd` built its IPv6 support inside `sdcm/cluster_oci.py` /
   `sdcm/utils/oci_region.py`, touching shared code only to edit two comments in
   `sdcm/provision/network_configuration.py` and add `oci` to the `use_dns_names` allowlist.
2. **Backend rules are hardcoded in one shared validation block.** `sdcm/sct_config.py:3534-3541`
   rejects `ipv4 + public + nic != 0` for *every* backend, but that is an EC2 limitation
   (`AssociatePublicIpAddress` is only valid on device index 0). `sdcm/sct_config.py:3543-3552` adds a
   GCE-only DNS rule next to it. The block is accumulating `if backend ==` branches with no structure.
3. **Validation test coverage was lost.** `unit_tests/test_configs/network_config_interface_not_defined.yaml`,
   `network_config_interface_param_not_defined.yaml`, and `network_config_interface_param_public_not_primary.yaml`
   exist but no test module references them — the tests PR #6575 added were dropped somewhere along the way.

## Reference implementation: OCI (SCT-317 / SCT-582)

This ticket is one of three siblings — SCT-310 (GCE), SCT-315 (Azure), SCT-317 (OCI). **OCI is
merged and is the reference; Azure is not started.** `AzureNode.network_interfaces` still returns
`pass` (`sdcm/cluster_azure.py:112-116`) and `_get_ipv6_ip_address()` is a `# todo: fix it` stub
(`:184-186`). GCE going second means it should reuse OCI's shape rather than invent one, so Azure
has a settled pattern to follow third.

Two commits make up the OCI work: `63a5510dd` *feature(oci): add IPv6 and multi-VNIC support*
and `5cbe8071a` *ci(oci): add CI jobs reusing multi-vnic configuration*. Between them they establish
four layers, and GCE already has three of them for IPv4:

| Layer | OCI | GCE today |
|-------|-----|-----------|
| Region prep | `OciRegion._enable_vcn_ipv6()` adds an IPv6 CIDR to the VCN and ensures IPv6 route-table and security-list rules (`sdcm/utils/oci_region.py:142-262`) | `GceRegion.create_secondary_subnet()` (`sdcm/utils/gce_region.py:304-345`) — IPv4 only, **no IPv6 equivalent** |
| Provisioning | subnets created `is_ipv6_enabled=True` (`oci_region.py:283`); per-NIC subnets named by `nic_index`; IPv6 assigned to every VNIC including the SCT runner | `build_network_interfaces()` (`sdcm/provision/gce/instance_provider.py:75-101`) — IPv4 only |
| Node | `_build_network_interfaces()` fills real IPv6 fields from the API, with `_discover_ipv6_from_os()` (`ip -6 -j addr show scope global`) as fallback (`sdcm/cluster_oci.py:170-243`); `_configure_secondary_vnics_os()` installs policy routing | `GCENode.network_interfaces` (`:259-278`) — IPv6 fields hardcoded empty; `_configure_secondary_nic_routing()` is the direct analogue of `_configure_secondary_vnics_os()` and already exists |
| CI | new jenkinsfiles + a test-case override reusing the shared `configurations/network_config/` files (`5cbe8071a`) | `*NetworkMonkey-gce` jobs already reuse `two_interfaces.yaml`; **no IPv6 job** |

Three things to carry over deliberately:

- **OCI implemented IPv6; it did not reject it.** An earlier draft of this plan proposed rejecting
  `ip_type: ipv6` on GCE outright. That would diverge from the pattern the team just set. The reject
  becomes an explicit *interim guard* here, removed when [Follow-up B](#follow-up-b-implement-ipv6-on-gce--sct-922) lands.
- **The OS-discovery fallback is a good idea and cheap on GCE.** `GCENode.network_configuration`
  already shells out to `ip -j link` and parses JSON, so `_discover_ipv6_from_os()` is a small
  extension of an existing habit rather than new machinery.
- **Backend-validation tests live in `unit_tests/test_gce_use_dns_names.py`.** That is where
  `63a5510dd` added its OCI case (`:333-345`), despite the file name. Extend it rather than opening a
  third location — and rename it in a separate commit, since it stopped being GCE-specific.

### Explicitly out of scope

- **`use_public_ip` is ignored on GCE** — see [Follow-up A](#follow-up-a-honour-use_public_ip-on-gce-cloud-nat--sct-921).
- **Full IPv6 support on GCE** — see [Follow-up B](#follow-up-b-implement-ipv6-on-gce--sct-922).

Both, and the resolved item below, belong in a scope comment on SCT-310 before this PR opens.

### Already resolved on master

- **Oracle nodes on GCE** — the ticket's "db, loader, monitor, oracle" bullet. An earlier draft of
  this plan listed it as out of scope because GCE had no oracle cluster at all. `d961e11a7`
  *feature(oracle): support the oracle cluster on GCE and Azure* landed on 2026-08-25 and closes it:
  `sdcm/sct_provision/gce/gce_region_definition_builder.py:52-60` now carries an `oracle-db` mapper
  derived from `db_map`, and `sdcm/tester.py` grew a shared `_create_oracle_cluster()` with a GCE
  branch. Oracle nodes on GCE go through the same `build_instance_definition()` path as every other
  role, so they pick up `network_interfaces_count()` and `scylla_network_config` with no extra work —
  which is exactly what the ticket asked for. Worth an explicit check in verification, nothing more.

## Approach

1. **Restore validation unit tests first.** Wire the three orphaned fixtures in
   `unit_tests/test_configs/` back into `unit_tests/test_gce_use_dns_names.py`, following the
   `SCTConfiguration()` + `monkeypatch.setenv` fixture pattern already there (`:255-326`). These
   pin *current* behaviour so step 2 is provably behaviour-preserving.
2. **Extract per-backend `scylla_network_config` rules.** Replace the inline `if` chain in
   `SCTConfiguration.verify()` step 17 with a rule table keyed by backend, so each constraint states
   which backend it belongs to. Behaviour-preserving for AWS and GCE; it is the seam Follow-up B and
   the Azure ticket both need.
3. **Add the interim IPv6 guard for GCE** as one entry in that table: reject `ip_type: ipv6` with an
   error naming the offending address and pointing at Follow-up B. Update the stale warning in
   `GCENode._get_ipv6_ip_address()` to say "not implemented yet, see SCT-310 follow-up" rather than
   the incorrect "GCE VPC networks only support IPv4" — `compute_v1.Subnetwork` exposes
   `stack_type` / `ipv6_access_type` / `ipv6_cidr_range` and `NetworkInterface` exposes
   `ipv6_access_configs`, so the platform limitation the warning claims does not exist.
4. **Comment on SCT-310** recording what already landed, and file Follow-ups A and B.
   Done 2026-09-03: scope comment posted, [SCT-921](https://scylladb.atlassian.net/browse/SCT-921)
   and [SCT-922](https://scylladb.atlassian.net/browse/SCT-922) filed and linked to SCT-310.

## Files to Modify

- `sdcm/sct_config.py` — replace the step-17 inline backend branches (`:3516-3565`) with a
  per-backend rule table; add the GCE interim ipv6 rule.
- `sdcm/cluster_gce.py` — correct the stale IPv4-only warning in `_get_ipv6_ip_address()` (`:364-370`).
- `unit_tests/test_gce_use_dns_names.py` — restore the three orphaned fixtures as test cases; add the
  GCE ipv6-rejected and ipv4-accepted cases, and a case pinning that the AWS-only
  public-on-secondary-nic rule is not applied to other backends.
- `docs/configuration_options.md` — regenerate if the `scylla_network_config` description changes.

Unchanged but referenced for verification:
- `defaults/gce_config.yaml:50-77` — GCE default network config block.
- `configurations/network_config/all_addresses_ipv6_public.yaml` — the config that must now fail fast on GCE.

## Verification

- [ ] Tests pass: `uv run python -m pytest unit_tests/test_gce_use_dns_names.py unit_tests/unit/test_gce_network_interfaces.py unit_tests/unit/test_network_config.py -v`
- [ ] `SCT_CLUSTER_BACKEND=gce` with `configurations/network_config/all_addresses_ipv6_public.yaml`
      raises a `ValueError` naming the address and pointing at the follow-up, instead of failing later with `None`
- [ ] The same config on `SCT_CLUSTER_BACKEND=oci` still passes — the interim guard must not leak to
      the backend that already implements IPv6
- [ ] AWS keeps rejecting `ipv4 + public: true + nic: 1`; the same config on GCE is evaluated by the
      GCE rule set, not the AWS one
- [ ] The three previously orphaned `unit_tests/test_configs/network_config_interface_*.yaml`
      fixtures are each referenced by at least one test
- [ ] Oracle nodes on GCE (`db_type: mixed_scylla`) resolve `scylla_network_config` like any other
      role — covered by the `oracle-db` mapper added in `d961e11a7`, so this is a check, not a change
- [ ] GCE two-NIC path unaffected: `longevity-5gb-1h-BlockNetworkMonkey-gce` completes
- [ ] GCE single-NIC path unaffected: a plain GCE sanity longevity completes
- [ ] `uv run sct.py pre-commit` passes

## Follow-up A: honour `use_public_ip` on GCE (Cloud NAT) — [SCT-921](https://scylladb.atlassian.net/browse/SCT-921)

Filed as [SCT-921](https://scylladb.atlassian.net/browse/SCT-921) on 2026-09-03, from this section.

### What is wrong

`build_network_interfaces()` (`sdcm/provision/gce/instance_provider.py:75-101`) unconditionally
attaches an `AccessConfig` to nic 0. `definition.use_public_ip` reaches GCE provisioning but only
selects firewall tags (`sdcm/provision/gce/instance_provider.py:385` →
`NetworkProvider.get_network_tags()`). AWS honours it via `AssociatePublicIpAddress`
(`sdcm/cluster_aws.py:271-273`), Azure via `ip_provider.py:56`, OCI via
`virtual_machine_provider.py:239`. GCE is the only backend that does not.

Consequence: every GCE node gets a billed, internet-reachable external IP even though
`defaults/gce_config.yaml:73-77` sets `test_communication` to `public: false` and
`defaults/test_default.yaml:12` sets `ip_ssh_connections: 'private'` — i.e. SCT already talks to GCE
nodes over private addresses (`sdcm/cluster_gce.py:627-630`) and does not need the public one.

### Why it is not a one-line fix

`sdcm/utils/gce_region.py` provisions no Cloud NAT. On GCE, an instance with no external IP and no
NAT gateway has **no internet egress at all** — no package repos, no Scylla repos, no S3. Deleting
the `AccessConfig` without adding NAT first breaks every GCE run. The `build_network_interfaces()`
docstring already records the current assumption ("the primary interface ... carries the public IP"),
so the change has to move that assumption, not just the code.

### Shape of the fix

1. Add `GceRegion.create_cloud_nat()` modelled on the existing `create_secondary_subnet()`
   (`sdcm/utils/gce_region.py:304-345`): idempotent, `NotFound` → create, already-exists → log and
   return. It needs a Cloud Router plus a `RouterNat` with `nat_ip_allocate_option=AUTO_ONLY` and
   `source_subnetwork_ip_ranges_to_nat=ALL_SUBNETWORKS_ALL_IP_RANGES` (so the secondary subnet is
   covered too). `compute_v1.RoutersClient`, `compute_v1.Router`, and `compute_v1.RouterNat` are all
   present in the pinned SDK — verified.
2. Call it from `GceRegion.configure()` (`:347-359`), next to `create_secondary_subnet()`. Skip it
   under `_is_minicloud` — the emulator does not serve the Routers API, same reasoning as the
   backup-storage skip already there.
3. Only then make `build_network_interfaces()` attach the `AccessConfig` conditionally on
   `definition.use_public_ip`, and drop the stale "carries the public IP" line from its docstring.
4. Keep monitor nodes public: `sdcm/sct_provision/region_definition_builder.py:134` already forces
   `use_public_ip=True` for `node_type == "monitor"`, so Grafana stays reachable with no extra work.

### Risks specific to this follow-up

- **Rollout ordering.** `hydra prepare-regions -c gce` must run against every GCE region in the QA
  project *before* step 3 merges, or runs in an unprepared region lose egress. Steps 1-2 are safe to
  merge alone and should go first, in their own PR.
- **Cost is not obviously a win.** Cloud NAT bills per gateway-hour plus per GB processed; external
  IPs bill per address-hour. Worth measuring on one region before claiming a saving — the honest
  justification for this change is consistency and reduced public exposure, not cost.
- **Network nemeses.** `sdcm/nemesis/monkey/network.py` and the SCT runner's reachability both
  assume the current addressing; the `*NetworkMonkey-gce` jobs are the ones to run.
- **`sct-allow-public` firewall tag** becomes meaningless for nodes with no external IP. Check
  whether `get_network_tags()` should stop emitting it in that case.

### Needs Investigation

- Does anything besides egress depend on the GCE external IP? `GCENode.public_dns_name`
  (`sdcm/cluster_gce.py:385-391`) resolves it when `use_dns_names` is on — confirm that path is
  either private-safe or gated.
- Do any GCE jobs set `ip_ssh_connections: public` or `test_communication.public: true` today, and
  would they keep working (they should — they would just set `use_public_ip=True`)?

## Follow-up B: implement IPv6 on GCE — [SCT-922](https://scylladb.atlassian.net/browse/SCT-922)

Filed as [SCT-922](https://scylladb.atlassian.net/browse/SCT-922) on 2026-09-03, from this section. The GCE
counterpart of SCT-582, following `63a5510dd` layer for layer. Removes the interim guard added in
this PR's step 3.

### Shape of the fix

1. **Region prep.** Add `GceRegion.enable_ipv6()` alongside `create_secondary_subnet()`: set the
   subnet's `stack_type=IPV4_IPV6` and an `ipv6_access_type`, and add the matching IPv6 firewall
   rules to `configure_firewall()` (`sdcm/utils/gce_region.py:114-241`), which today has IPv4-only
   `source_ranges`. Idempotent and skipped under `_is_minicloud`, like every other step in
   `configure()`. This mirrors `OciRegion._enable_vcn_ipv6()`.
2. **Provisioning.** Set `stack_type` and `ipv6_access_configs` on the interfaces built by
   `build_network_interfaces()`. OCI chose to make subnets dual-stack unconditionally ("Always create
   subnets with both IPv4 and IPv6 CIDRs") rather than gate on config — worth matching, so a run
   never fails on a subnet that predates the flag.
3. **Node.** Populate the real `ipv6_public_addresses` / `ipv6_private_address` fields in
   `GCENode.network_interfaces` from `iface.ipv6_access_configs` / `iface.ipv6_address`, add
   `_discover_ipv6_from_os()` as the fallback (copy `sdcm/cluster_oci.py:170-191`), and delete the
   `GCENode._get_ipv6_ip_address()` override so `BaseNode`'s generic implementation takes over.
4. **SCT runner.** OCI gave the runner IPv6 too (`sdcm/sct_runner.py`), needed for SSH to
   IPv6-only nodes. The GCE runner needs the same or `ssh_connection_ip_type == "ipv6"` will not work.
5. **CI.** Add a GCE IPv6 job reusing `configurations/network_config/all_addresses_ipv6_public.yaml`,
   mirroring `5cbe8071a`. `jenkins-pipelines/oss/longevity/longevity-10gb-3h-ipv6.jenkinsfile` is the
   AWS precedent to copy.

### Needs Investigation

- **Internal vs external IPv6.** GCE distinguishes ULA internal IPv6 from external IPv6, chosen per
  subnet via `ipv6_access_type`. `all_addresses_ipv6_public.yaml` wants public addresses, so external
  is the target — confirm the QA project's org policy permits external IPv6 subnets before building on it.
- **Does `sct-allow-public` cover IPv6?** The firewall rules in `configure_firewall()` use
  `source_ranges` (IPv4). IPv6 needs `source_ranges` entries of `::/0` or the rules silently do not apply.
- **Multi-NIC + IPv6 together.** The secondary subnet created by `create_secondary_subnet()` would
  also need dual-stack, and `SECONDARY_SUBNET_CIDR_TMPL` allocates IPv4 CIDRs only.
