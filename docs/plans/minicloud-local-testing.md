---
status: in_progress
domain: cluster
created: 2026-03-11
last_updated: 2026-07-23
owner: fruch
---
# Minicloud Local Testing Integration

## 1. Problem Statement

Running AMI artifact tests (`artifacts_test.py::ArtifactsTest::test_scylla_service`) requires an AWS account, incurs cloud costs ($0.30–$1.00+ per run for `i4i.large` spot instances), and depends on network connectivity to AWS. This creates friction for local development:

- **Cost**: Every test iteration launches a real EC2 instance (even for verifying basic Scylla service behavior)
- **Latency**: Instance provisioning takes 2–5 minutes before the test logic even starts
- **Access barriers**: Developers need AWS credentials, correct IAM permissions, and VPN/network access
- **CI dependency**: Artifact tests can only run in Jenkins pipelines with cloud access, not on developer machines

[minicloud](https://github.com/scylladb/minicloud) is a local cloud emulator backed by QEMU/KVM that implements a subset of the EC2 Query API and GCE Compute API. It launches real Linux VMs from AMI/GCE images locally, with NVMe storage, userspace-switch networking, and IMDS metadata — making it a viable target for running AMI artifact tests without cloud costs.

### Architecture decision: server-side routing

An earlier revision of this plan proposed *client-side* selective endpoint routing in SCT: a centralized boto3 client factory that would send instance-lifecycle calls to minicloud while keeping AMI resolution on real AWS. That approach was dropped — minicloud handles the routing itself, server-side:

- **Emulated locally**: instance lifecycle (`RunInstances`, `DescribeInstances`, `TerminateInstances`), VPC/subnet/SG/IGW/route-table creation, key pairs, IMDS
- **Passthrough to real AWS** (static allowlist, SigV4 re-signing): `DescribeImages`, SSM `GetParameter` (`resolve:ssm:` AMI lookups), STS, SecretsManager, opt-in S3 buckets, and further EC2 actions (full list in section 2's API inventory)
- **Rejected**: any action not on either list returns `UnsupportedOperation` — passthrough is not a silent fallback

Consequently SCT needs **no routing layer and no changes to the AWS backend code**: it only points the AWS SDK at minicloud (`AWS_ENDPOINT_URL`) once the emulator is confirmed healthy. `sdcm/cluster_aws.py` is untouched by the implementation ([#14875](https://github.com/scylladb/scylla-cluster-tests/pull/14875)).

## 2. Current State

### Reference minicloud version

Everything in this section is stated against **minicloud v0.1.0** (released 2026-07-16, first tagged release). Minicloud publishes:

- Docker images: `ghcr.io/scylladb/minicloud` and `docker.io/scylladb/minicloud` (tags: `X.Y.Z`, `latest`, `dev` = every master merge, `master-<shortsha>`)
- Binary tarball: `https://qa-minicloud-releases.s3.amazonaws.com/minicloud-{version}-linux-amd64.tar.gz`

The SCT integration consumes minicloud strictly as a container (`MinicloudManager` supports only docker mode); the tarball is a general minicloud distribution artifact, not used by the integration.

The integration must pin a release tag. `scylladb/minicloud:dev` is a moving target — several capabilities discussed below exist only in unmerged minicloud PRs and must not be assumed present in `dev` or `latest`.

### Integration state (implementation PR #14875)

The SCT side is implemented in [#14875](https://github.com/scylladb/scylla-cluster-tests/pull/14875) (branch `feature/minicloud-integration`, draft):

| Component | Files | Status |
|-----------|-------|--------|
| MinicloudManager (container lifecycle, health checks, env-var config) | `sdcm/utils/minicloud.py` | done |
| Test integration (setUp/tearDown hooks) | `sdcm/tester.py` | done |
| Nested virt on sct-runner (AWS `CpuOptions` for c8i/m8i/r8i, GCE `enableNestedVirtualization`) | `sdcm/sct_runner.py` | done |
| Hydra (SCT's dockerized CLI wrapper) env-var forwarding (`MINICLOUD_*`) | `docker/env/hydra.sh` | done |
| Start/packaging/runner-setup scripts | `scripts/start-minicloud.sh`, `scripts/minicloud-package.sh`, `scripts/minicloud-runner-setup.sh`, `scripts/test-minicloud-*.sh` | done |
| Test config (3× `i4i.large`, KMS disabled, 2.5 GiB/VM lightweight mode) | `test-cases/minicloud-provision-test.yaml` | done |
| Jenkins pipeline | `vars/minicloudPipeline.groovy`, `jenkins-pipelines/oss/minicloud/*.jenkinsfile`, `jenkins-pipelines/oss/artifacts/artifacts-minicloud.jenkinsfile` | done, not green yet |
| Log collection (skip SSH-dependent collectors under minicloud) | `sdcm/logcollector.py` | done |
| Shutdown hang prevention (event system) | `sdcm/sct_events/` | done |
| GCE DNS names support | `sdcm/cluster_gce.py` (`use_dns_names`) | done |
| Unit tests | `unit_tests/unit/test_minicloud.py` (52 tests) | done |

**Open item — configuration mechanism (Needs Decision)**: #14875 both forwards `MINICLOUD_*` environment variables (no `SCT_` prefix, hence no `sct_config.py` involvement) *and* adds a `minicloud_endpoint_url` parameter to `sdcm/sct_config.py` + `defaults/test_default.yaml`. The endpoint helpers currently ignore the config parameter (only `AWS_ENDPOINT_URL` containing "localhost" is honored). One mechanism should be chosen and the other removed — tracked in [QAINFRA-86](https://scylladb.atlassian.net/browse/QAINFRA-86).

### minicloud capabilities (as of v0.1.0)

- **EC2 API — emulated locally**: `RunInstances` (param allowlist incl. `NetworkInterface(s).`, `BlockDeviceMapping.`, `TagSpecification.` prefixes; `ClientToken` idempotency), `DescribeInstances`, `TerminateInstances` (graceful QEMU shutdown, idempotent), `CreateVpc`, `ModifyVpcAttribute`, `CreateSubnet`, `ModifySubnetAttribute`, `CreateInternetGateway`, `AttachInternetGateway`, `CreateRouteTable`, `CreateRoute`, `AssociateRouteTable`, `CreateSecurityGroup`, `AuthorizeSecurityGroupIngress` (rules stored, **not enforced**), `CreateKeyPair`, `DescribeKeyPairs`, `CreateVpcEndpoint` (stub)
- **EC2 API — passthrough to real AWS**: `DescribeImages`, `DescribeInstanceStatus`, `StopInstances`, `StartInstances`, `DescribeSecurityGroups`, `DescribeInternetGateways`, `DescribeInstanceTypeOfferings`, `DescribeAvailabilityZones`, `DescribeRegions`, `DescribeVpcAttribute`, `RequestSpotInstances`, Delete*/Detach*/Disassociate* teardown actions. **Caveat**: passthrough of actions that reference minicloud-local resource IDs (instance/SG/IGW IDs) hits real AWS and returns NotFound — these are passthrough for API compatibility, not emulation
- **Instance lifecycle**: `RunInstances` returns `pending` (as real EC2 does) and the instance transitions to `running` once QEMU starts ("running" means the QEMU process is up, not that the guest booted). Background AMI download on first use, then launch
- **AMI download**: `DescribeImages` resolves against the real AWS catalog; image bytes are fetched via the EBS Direct API (`ebs:ListSnapshotBlocks`/`GetSnapshotBlock`), converted to qcow2 and cached in `~/.cache/minicloud/amis/`. Public/cross-account snapshots are rejected by EBS Direct — CopyImage fallback tracked in [QATOOLS-283](https://scylladb.atlassian.net/browse/QATOOLS-283)
- **VMs**: QEMU/KVM, UEFI/OVMF boot, NVMe controllers with EC2-like model strings, SMBIOS spoofing so cloud-init detects EC2, IMDSv2, serial log. NVMe model strings require a patched QEMU (`avikivity/qemu` commit `8c70589`); with stock QEMU the root disk falls back to virtio-blk
- **Instance types**: only `i4i.large` (AWS) and `n2-highmem-2` (GCP); anything else is rejected. A full instance-type catalog (159 AWS + 89 GCE types, backs `DescribeInstanceTypes`) is in review upstream
- **Networking**: userspace Ethernet switch, per-VM socketpair netdevs, DHCP/ARP/IPv6-RA, IMDS DNAT. Host connectivity via a persistent TUN device (`minicloud0`) created once by `sudo ./minicloud-setup.sh`. Public IP is reported equal to the private IP (host-routable, no NAT)
- **GCE API**: networks, subnetworks, firewalls, instances, operations, metadata server, image downloads (same QEMU backend)
- **Not implemented / gaps** (tracked in Jira):
  - `CreateTags` after resource creation ([QATOOLS-284](https://scylladb.atlassian.net/browse/QATOOLS-284)); tag *filtering* in `DescribeInstances` is silently ignored (all instances returned), and subnet/SG/IGW Describe* drop tags ([QATOOLS-296](https://scylladb.atlassian.net/browse/QATOOLS-296)) — fixes WIP upstream
  - `DescribeVpcs` ([QATOOLS-340](https://scylladb.atlassian.net/browse/QATOOLS-340)), `DescribeSubnets` ([QATOOLS-319](https://scylladb.atlassian.net/browse/QATOOLS-319)), `DescribeRouteTables` ([QATOOLS-284](https://scylladb.atlassian.net/browse/QATOOLS-284)) — fixes WIP upstream
  - `RequestSpotInstances` is passthrough and forwards minicloud-local subnet IDs to real AWS (`InvalidSubnetID.NotFound`) — local mock in [QATOOLS-291](https://scylladb.atlassian.net/browse/QATOOLS-291), blocking the passthrough until then in [QATOOLS-321](https://scylladb.atlassian.net/browse/QATOOLS-321)
  - KMS (no emulation; SCT config must disable KMS) — [QATOOLS-282](https://scylladb.atlassian.net/browse/QATOOLS-282)
  - EIP management (`AllocateAddress`/`AssociateAddress`) — not needed while public IP == private IP
  - GCE: label filtering in `instances.list` ([QATOOLS-297](https://scylladb.atlassian.net/browse/QATOOLS-297), fix WIP upstream); local-ssd NVMe missing from the distributed binary ([QATOOLS-312](https://scylladb.atlassian.net/browse/QATOOLS-312))

### EC2 API inventory for artifact test flow

Every EC2 API action SCT calls during the AMI artifact test (`test_scylla_service`, 1 db node, 0 loaders, 0 monitors), with its minicloud v0.1.0 status:

| EC2 API Action | SCT usage | Flow phase | minicloud v0.1.0 status |
|----------------|-----------|------------|-------------------------|
| **Config / AMI resolution** | | | |
| DescribeImages (filter by name/tag) | `sdcm/utils/common.py` (`convert_name_to_ami_if_needed`, `get_ami_images`) | config | passthrough to real AWS |
| DescribeImages (SSM resolve) | `sdcm/utils/common.py` | config | SSM `GetParameter` passthrough |
| DescribeInstanceTypes | `sdcm/utils/aws_utils.py` | config | **UnsupportedOperation** — instance catalog WIP upstream |
| **VPC / network setup** | | | |
| DescribeVpcs | `sdcm/utils/aws_region.py` | network | **not implemented** — [QATOOLS-340](https://scylladb.atlassian.net/browse/QATOOLS-340) (WIP) |
| CreateVpc | `sdcm/utils/aws_region.py` | network | emulated |
| ModifyVpcAttribute (DNS hostnames) | `sdcm/utils/aws_region.py` | network | emulated |
| CreateTags (VPC, subnet, SG, IGW, RT) | `sdcm/utils/aws_region.py` | network | **not implemented post-creation** (creation-time `TagSpecification` works) — [QATOOLS-284](https://scylladb.atlassian.net/browse/QATOOLS-284) |
| DescribeAvailabilityZones | `sdcm/utils/aws_region.py` | network | passthrough to real AWS |
| DescribeInstanceTypeOfferings | `sdcm/utils/aws_region.py` | network | passthrough to real AWS |
| DescribeSubnets | `sdcm/utils/aws_region.py` | network | **not implemented** — [QATOOLS-319](https://scylladb.atlassian.net/browse/QATOOLS-319) (WIP) |
| CreateSubnet | `sdcm/utils/aws_region.py` | network | emulated |
| ModifySubnetAttribute (public IP, IPv6) | `sdcm/utils/aws_region.py` | network | emulated |
| DescribeInternetGateways | `sdcm/utils/aws_region.py` | network | passthrough — tag filters missing ([QATOOLS-296](https://scylladb.atlassian.net/browse/QATOOLS-296)) |
| CreateInternetGateway / AttachInternetGateway | `sdcm/utils/aws_region.py` | network | emulated |
| DescribeRouteTables | `sdcm/utils/aws_region.py` | network | **not implemented** — [QATOOLS-284](https://scylladb.atlassian.net/browse/QATOOLS-284) (WIP) |
| CreateRouteTable / CreateRoute / AssociateRouteTable | `sdcm/utils/aws_region.py` | network | emulated |
| DescribeSecurityGroups | `sdcm/utils/aws_region.py` | network | passthrough — tag filters missing ([QATOOLS-296](https://scylladb.atlassian.net/browse/QATOOLS-296)) |
| CreateSecurityGroup / AuthorizeSecurityGroupIngress | `sdcm/utils/aws_region.py` | network | emulated (rules stored, not enforced) |
| DescribeKeyPairs / CreateKeyPair | `sdcm/utils/aws_region.py` | network | emulated |
| ImportKeyPair | `sdcm/utils/aws_region.py` | network | **not implemented** — [QATOOLS-338](https://scylladb.atlassian.net/browse/QATOOLS-338) (fix WIP upstream) |
| **Instance lifecycle** | | | |
| RunInstances | `sdcm/cluster_aws.py` | instance | emulated (incl. `NetworkInterfaces` format, `TagSpecification`) |
| DescribeInstances | `sdcm/ec2_client.py` | instance | emulated; **tag/state filters silently ignored** — [QATOOLS-296](https://scylladb.atlassian.net/browse/QATOOLS-296) (WIP) |
| CreateTags (instance) | `sdcm/ec2_client.py` | instance | **not implemented post-creation** — [QATOOLS-284](https://scylladb.atlassian.net/browse/QATOOLS-284) |
| AllocateAddress / AssociateAddress (EIP) | `sdcm/cluster_aws.py` | instance | not needed — public IP == private IP, use `ip_ssh_connections: 'private'` |
| **Teardown** | | | |
| TerminateInstances | `sdcm/cluster_aws.py` | teardown | emulated |
| ReleaseAddress (EIP) | `sdcm/cluster_aws.py` | teardown | not needed (no EIP) |
| **Spot (disabled for minicloud runs)** | | | |
| RequestSpotInstances / RequestSpotFleet | `sdcm/ec2_client.py` | instance | **do not enable** — passthrough leaks local subnet IDs to real AWS; [QATOOLS-291](https://scylladb.atlassian.net/browse/QATOOLS-291)/[QATOOLS-321](https://scylladb.atlassian.net/browse/QATOOLS-321) |

**Key finding**: the test itself makes zero EC2 API calls — all Scylla checks go via SSH/CQL. The EC2 surface is entirely in setup and teardown.

## 3. Goals

1. **Run AMI artifact test locally** against minicloud-managed VMs with zero AWS instance costs — core subtests pass; hardware-specific subtests get skip/relaxed paths
2. **Transparent integration**: minicloud routes API calls server-side; SCT only sets the endpoint URL after a health check — **zero changes to AWS backend code** (`sdcm/cluster_aws.py` untouched)
3. **Zero impact on existing backends** — the integration is opt-in via environment configuration; default behavior is unchanged
4. **CI validation**: the `minicloud-provision-test` Jenkins pipeline (builder → KVM-capable sct-runner → hydra) passes end-to-end: AMI download → 3 KVM VMs → Scylla cluster up → provision test green
5. **GCE parity (stretch)**: the same integration works for the GCE backend against minicloud's Compute API emulation (in progress on both sides; blocked upstream by [QATOOLS-312](https://scylladb.atlassian.net/browse/QATOOLS-312))
6. **Developer experience**: a developer can run the artifact test locally with a pinned minicloud release and a documented setup ([SCT-686](https://scylladb.atlassian.net/browse/SCT-686))

## 4. Implementation Phases

| Phase | Objective | Status |
|-------|-----------|--------|
| 1 | Minicloud configuration and activation | in progress — config decision and release pin open |
| 2 | VPC/subnet/security-group setup via minicloud | in progress — upstream API gaps |
| 3 | Instance provisioning and lifecycle | done |
| 4 | Test config and end-to-end validation | in progress — CI not green |
| 5 | Documentation and developer guide | pending |
| 6 | GCE backend support | in progress |

---

### Phase 1: Minicloud configuration and activation — In progress

**Importance**: High

- `sdcm/utils/minicloud.py` — `MinicloudManager`: starts/stops the minicloud container (`docker` with `/dev/kvm` + `/dev/net/tun` passthrough), health-checks the endpoint, and only then exports `AWS_ENDPOINT_URL` for the test process
- Configuration via `MINICLOUD_*` environment variables (forwarded into hydra by `docker/env/hydra.sh`), not `SCT_`-prefixed options
- Version sourcing: minicloud is consumed as a container image (see section 2)

**Definition of Done**:
- [x] Container lifecycle management with health check before endpoint activation
- [x] `MINICLOUD_*` env vars forwarded through hydra
- [x] Unit tests for manager behavior (`unit_tests/unit/test_minicloud.py`)
- [ ] **Needs Decision**: reconcile `minicloud_endpoint_url` in `sct_config.py` vs `MINICLOUD_*` env vars — one mechanism must win (currently the config parameter is added but ignored by the endpoint helpers; [QAINFRA-86](https://scylladb.atlassian.net/browse/QAINFRA-86))
- [ ] Pin a minicloud release tag (v0.1.0+) instead of `dev`

---

### Phase 2: VPC/subnet/security-group setup via minicloud — In progress

**Importance**: High

`AwsRegion` (`sdcm/utils/aws_region.py`) works against minicloud for all Create*/Modify* operations. Remaining blockers are upstream minicloud API gaps, all tracked and with WIP fixes:

- `DescribeVpcs`/`DescribeSubnets`/`DescribeRouteTables` return `UnsupportedOperation` — breaks `AwsRegion` re-discovery of SCT-prepared resources ([QATOOLS-340](https://scylladb.atlassian.net/browse/QATOOLS-340), [QATOOLS-319](https://scylladb.atlassian.net/browse/QATOOLS-319), [QATOOLS-284](https://scylladb.atlassian.net/browse/QATOOLS-284))
- `CreateTags` post-creation and tag filtering ([QATOOLS-284](https://scylladb.atlassian.net/browse/QATOOLS-284), [QATOOLS-296](https://scylladb.atlassian.net/browse/QATOOLS-296))

**Definition of Done**:
- [x] VPC, subnet, security group, internet gateway, route table, and key pair creation succeeds against minicloud
- [ ] `AwsRegion.configure()` full flow (create + re-discover by tags) passes once the upstream fixes merge and a release containing them is pinned

**Dependencies**: upstream fixes for QATOOLS-340/QATOOLS-319/QATOOLS-284/QATOOLS-296, landing in a tagged minicloud release.

---

### Phase 3: Instance provisioning and lifecycle — Done

**Importance**: High

`AWSCluster`/`AWSNode` run unmodified against minicloud:

- Instance state machine: instances transition `pending` → `running` once QEMU starts, so `wait_until_running` behaves as on real AWS
- Teardown: `TerminateInstances` shuts VMs down gracefully and idempotently
- SSH: standard `AWSNode` init flow; minicloud reports the host-routable private IP as public IP, tests use `ip_ssh_connections: 'private'`
- Spot: **must stay disabled** (`instance_provision: 'on_demand'`) until [QATOOLS-291](https://scylladb.atlassian.net/browse/QATOOLS-291)/[QATOOLS-321](https://scylladb.atlassian.net/browse/QATOOLS-321) are resolved — the passthrough currently leaks local subnet IDs to real AWS

**Definition of Done**:
- [x] `RunInstances` via minicloud provisions the cluster with the standard on-demand path
- [x] `AWSNode` resolves IPs and establishes SSH without bypasses
- [x] No changes to `sdcm/cluster_aws.py`
- [x] Teardown terminates instances cleanly

**Dependencies**: Phase 1.

---

### Phase 4: Test config and end-to-end validation — In progress

**Importance**: High

- Test config: `test-cases/minicloud-provision-test.yaml` (3× `i4i.large` DB nodes, KMS disabled; minicloud lightweight mode — VM resources capped below the real instance-type specs — at 2.5 GiB/VM, since 1.5 GiB causes Scylla per-shard OOM; Argus reporting disabled until CI is stable)
- Jenkins: `vars/minicloudPipeline.groovy` + `jenkins-pipelines/oss/minicloud/minicloud-provision-test.jenkinsfile`, `minicloud-artifact-test.jenkinsfile`, `jenkins-pipelines/oss/artifacts/artifacts-minicloud.jenkinsfile`. Builder node → KVM-capable sct-runner (`m8i.large`, nested virt) → hydra `--execute-on-runner`
- CI simulation scripts: `scripts/test-minicloud-*.sh`

```text
Jenkins builder ──ssh──► sct-runner (m8i.large, nested KVM)
                           └─► hydra (SCT CLI container)
                                 └─► MinicloudManager ──docker──► minicloud container (:5000 API, :5100 IMDS)
                                       ├─► QEMU/KVM VMs (3× emulated i4i.large, Scylla)
                                       └─► real AWS (passthrough: DescribeImages, SSM, STS)
```

**Current status**: the staging job (`scylla-staging/fruch/oss/minicloud/minicloud-provision-test-test`) is **not green** — last run failed (build #23, 2026-06-10) and needs re-validation against a pinned minicloud release once the Phase 2 upstream gaps land.

Hardware-specific subtests under minicloud:

- **ENA check**: guests use virtio-net — the check must be skipped or given an alternative path under minicloud
- **NVMe write cache / IO params**: NVMe device model strings require a patched QEMU (`avikivity/qemu` `8c70589`); with stock QEMU the root disk is virtio-blk, which scylla-machine-image's NVMe-only disk scan ignores. Needs either the patched QEMU in the minicloud image or a relaxed check path
- **perftune / XFS discard**: expected to differ on QEMU; relaxed checks acceptable
- **node_exporter liveness**: skipped (`n_monitor_nodes: 0`)

**Definition of Done**:
- [x] Test config and Jenkins pipeline exist and execute (not yet green)
- [ ] Full pipeline green: AMI download → 3 KVM VMs → Scylla cluster up → provision test pass
- [ ] Hardware-specific subtests have documented minicloud-aware paths (skip/relaxed) with clear messages
- [ ] Run is reproducible against a pinned minicloud release tag

**Dependencies**: Phase 2 (upstream API gaps), minicloud release containing the fixes.

---

### Phase 5: Documentation and developer guide — Pending

**Importance**: Medium

Tracked as [SCT-686](https://scylladb.atlassian.net/browse/SCT-686). Deliverables:

- `docs/minicloud-testing.md`: prerequisites (Linux, KVM), pinned release install (container), one-time host setup (`sudo ./minicloud-setup.sh` — persistent TUN device, no per-run sudo), `MINICLOUD_*` env var reference, running the artifact test locally, known limitations and skipped subtests, troubleshooting (AppArmor on Ubuntu 24.04+, KVM access, IAM permissions for EBS Direct AMI download)
- Update `AGENTS.md` backends section to mention minicloud

**Definition of Done**:
- [ ] A developer can follow the guide from scratch and run the artifact test locally

**Dependencies**: Phase 4 (CI green first, so the guide documents a working flow).

---

### Phase 6: GCE backend support — In progress

**Importance**: Medium

Minicloud emulates a subset of the GCE Compute API (instances, networks, subnetworks, firewalls, metadata server, image downloads), and #14875 already contains the SCT-side pieces (`sdcm/cluster_gce.py` `use_dns_names`, `sdcm/utils/gce_region.py`, `sdcm/utils/gce_utils.py`, GCE nested-virt in `sct_runner.py`).

**Blockers**:
- Local-ssd NVMe support is missing from the distributed minicloud binary ([QATOOLS-312](https://scylladb.atlassian.net/browse/QATOOLS-312)) — GCE instances come up but `scylla_create_devices` finds no NVMe disks
- GCE label filtering in `instances.list`/aggregated ([QATOOLS-297](https://scylladb.atlassian.net/browse/QATOOLS-297))
- Empty-subnetwork auto-resolve ([QATOOLS-319](https://scylladb.atlassian.net/browse/QATOOLS-319)) — fixed on minicloud master, pending a tagged release

**Definition of Done**:
- [ ] GCE provision test passes against minicloud end-to-end

**Dependencies**: upstream fixes for QATOOLS-312/QATOOLS-297 in an NVMe-capable tagged minicloud release.

## 5. Testing Requirements

### Unit Tests

Implemented in #14875: `unit_tests/unit/test_minicloud.py` (52 tests) covering `MinicloudManager` — container lifecycle, health checks, env-var configuration, endpoint activation/deactivation, failure paths (minicloud unreachable, container start failure).

Additional coverage needed:

| Area | What it verifies |
|------|-----------------|
| Endpoint restore on stop | previous `AWS_ENDPOINT_URL` is restored, not deleted (review finding on #14875) |
| Config mechanism | whichever of `minicloud_endpoint_url` / `MINICLOUD_*` wins the Phase 1 decision is actually honored |

### Integration / CI Tests

| Test | Service | Status |
|------|---------|--------|
| `minicloud-provision-test` Jenkins pipeline (3-node provision, CQL, teardown) | minicloud (QEMU/KVM on sct-runner) | exists, not green |
| `artifacts-minicloud` pipeline (AMI artifact test) | minicloud | exists, pending validation |
| `scripts/test-minicloud-*.sh` CI simulation | minicloud container | passing locally |

### Manual Testing

| Procedure |
|-----------|
| Run the provision test locally against a pinned minicloud release; verify Scylla starts, CQL works, teardown leaves no orphan QEMU processes |
| Run any AWS-backend test with minicloud disabled — verify zero behavioral change |

## 6. Success Criteria

- [ ] All phase Definition of Done items are met (Phases 1–6)
- [ ] AMI resolution works through minicloud's passthrough (real AWS `DescribeImages`/SSM) with no SCT-side routing code — `sdcm/cluster_aws.py` stays untouched
- [ ] Zero behavioral change for all backends when minicloud is not activated

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Minicloud API gaps (missing Describe*/tag operations) block SCT flows | High | Medium | Gaps are audited ([QATOOLS-308](https://scylladb.atlassian.net/browse/QATOOLS-308)) and tracked per-gap in QATOOLS; fixes land upstream in minicloud, never as shims in SCT business logic |
| Running against `scylladb/minicloud:dev` makes results non-reproducible; capability claims drift from the released version | High | Medium | Pin a release tag (v0.1.0+) in `MinicloudManager`/pipeline; state the validated minicloud version in this plan and in test reports; bump the pin deliberately |
| Passthrough actions referencing minicloud-local IDs reach real AWS (spot: `InvalidSubnetID.NotFound`; Describe*/Stop/Start: NotFound) | High | Medium | Keep spot disabled in minicloud configs ([QATOOLS-321](https://scylladb.atlassian.net/browse/QATOOLS-321) blocks the passthrough upstream; [QATOOLS-291](https://scylladb.atlassian.net/browse/QATOOLS-291) implements the local mock); treat NotFound from passthrough Describe* as a known failure signature |
| QEMU NVMe/network fidelity differs from real EC2 (ENA absent, NVMe needs patched QEMU) | High | Low | Hardware-specific subtests get minicloud-aware skip/relaxed paths; ship the patched QEMU in the minicloud image where NVMe fidelity matters |
| AMI first-download via EBS Direct API requires AWS credentials and fails on public/cross-account snapshots | Medium | Low | One-time cost, cached afterwards; document required IAM permissions; CopyImage fallback tracked in [QATOOLS-283](https://scylladb.atlassian.net/browse/QATOOLS-283) |
| Host setup requires sudo (KVM, TUN device) | Medium | Low | One-time `sudo ./minicloud-setup.sh` creates a persistent TUN device — no per-run sudo; runner AMIs/images pre-provision KVM access ([QATOOLS-311](https://scylladb.atlassian.net/browse/QATOOLS-311) covers distribution) |
| Event-system/container shutdown hangs on teardown | Medium | Medium | Hang-proof shutdown implemented in #14875 (`sdcm/sct_events/`); orphan-QEMU check runs at the end of minicloud CI smoke tests |

## Tracking

- SCT integration epic: [QAINFRA-83](https://scylladb.atlassian.net/browse/QAINFRA-83)
- Minicloud ownership/development epic: [QATOOLS-304](https://scylladb.atlassian.net/browse/QATOOLS-304); API-gap audit: [QATOOLS-308](https://scylladb.atlassian.net/browse/QATOOLS-308); release/distribution: [QATOOLS-311](https://scylladb.atlassian.net/browse/QATOOLS-311)
- Implementation PR: [#14875](https://github.com/scylladb/scylla-cluster-tests/pull/14875)
- minicloud repo: <https://github.com/scylladb/minicloud> (issues migrated to Jira QATOOLS, 2026-07-16)
- CI job: `scylla-staging/fruch/oss/minicloud/minicloud-provision-test-test`
