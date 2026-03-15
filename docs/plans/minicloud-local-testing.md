# Minicloud Local Testing Integration

## 1. Problem Statement

Running AMI artifact tests (`artifacts_test.py::ArtifactsTest::test_scylla_service`) requires an AWS account, incurs cloud costs ($0.30–$1.00+ per run for `i4i.large` spot instances), and depends on network connectivity to AWS. This creates friction for local development:

- **Cost**: Every test iteration launches a real EC2 instance (even for verifying basic Scylla service behavior)
- **Latency**: Instance provisioning takes 2–5 minutes before the test logic even starts
- **Access barriers**: Developers need AWS credentials, correct IAM permissions, and VPN/network access
- **CI dependency**: Artifact tests can only run in Jenkins pipelines with cloud access, not on developer machines

[minicloud](https://github.com/scylladb/minicloud) is a local AWS EC2 emulator backed by QEMU/KVM that implements a subset of the EC2 Query API. It can launch real Linux VMs from AMI images locally, with NVMe storage, TAP networking, and IMDS metadata — making it a viable backend for running AMI artifact tests without AWS.

### Key constraint

AMI image resolution (`DescribeImages`, `convert_name_to_ami_if_needed`) and other read-only AWS metadata operations must continue to hit real AWS. Only instance-lifecycle operations (`RunInstances`, `DescribeInstances`, VPC/subnet/security-group setup) should be routed to minicloud. This selective routing is critical because minicloud downloads AMI images via the EBS Direct API on first use, but does not implement `DescribeImages` or image-scanning APIs.

## 2. Current State

### Backend selection

Backend is chosen via `cluster_backend` config parameter in `sdcm/sct_config.py:475`:

```python
cluster_backend: String = SctField(
    description="backend that will be used, aws/gce/azure/oci/docker/xcloud",
)
```

The `init_resources()` method in `sdcm/tester.py:2524` dispatches to backend-specific methods:

```python
if cluster_backend in ("aws", "aws-siren"):
    self.get_cluster_aws(...)
elif cluster_backend == "docker":
    self.get_cluster_docker()
```

### AWS cluster implementation

- `sdcm/cluster_aws.py:81` — `AWSCluster(cluster.BaseCluster)`: provisions instances via `EC2ClientWrapper`
- `sdcm/cluster_aws.py:539` — `AWSNode(cluster.BaseNode)`: wraps `ec2.Instance`, provides SSH access
- `sdcm/cluster_aws.py:1047` — `ScyllaAWSCluster(cluster.BaseScyllaCluster, AWSCluster)`: Scylla-specific DB cluster
- `sdcm/cluster_aws.py:1235` — `LoaderSetAWS(cluster.BaseLoaderSet, AWSCluster)`: loader nodes
- `sdcm/cluster_aws.py:1275` — `MonitorSetAWS(cluster.BaseMonitorSet, AWSCluster)`: monitor nodes

### EC2 client usage

`sdcm/ec2_client.py:55` — `EC2ClientWrapper` creates boto3 clients directly:

```python
class EC2ClientWrapper:
    def __init__(self, timeout=REQUEST_TIMEOUT, region_name=None):
        self._client = self._get_ec2_client(region_name)
        self._resource = boto3.resource("ec2", region_name=region_name)

    def _get_ec2_client(self, region_name=None) -> EC2Client:
        return boto3.client(service_name="ec2", region_name=region_name)
```

There are 40+ direct `boto3.client("ec2", ...)` and `boto3.resource("ec2", ...)` calls scattered across:
- `sdcm/ec2_client.py` — instance lifecycle (RunInstances, spot requests)
- `sdcm/utils/aws_region.py:45` — `AwsRegion` class (VPC/subnet/SG management)
- `sdcm/utils/aws_utils.py:74` — cleanup, tagging, device mappings
- `sdcm/utils/common.py` — AMI lookups (`convert_name_to_ami_if_needed`, `get_ami_images`)
- `sdcm/provision/aws/utils.py:87` — provisioner EC2 clients
- `sdcm/provision/aws/capacity_reservation.py` — capacity reservations
- `sdcm/provision/aws/dedicated_host.py` — dedicated hosts

### AMI artifact test

- `artifacts_test.py:52` — `ArtifactsTest(ClusterTester)`: main test class
- `test-cases/artifacts/ami.yaml` — config: `cluster_backend: 'aws'`, `instance_type_db: 'i4i.large'`, `n_db_nodes: 1`, `n_loaders: 0`, `n_monitor_nodes: 0`
- Test method `test_scylla_service` (`artifacts_test.py:356`) verifies: ENA support, IO params, NVMe write cache, XFS discard, snitch, node health, CQL, cassandra-stress, stop/start/restart, housekeeping, perftune, time sync services

### AMI resolution flow

`sdcm/sct_config.py:2469` resolves AMI names to IDs during config validation:

```python
self[key] = convert_name_to_ami_if_needed(param, tuple(self.region_names))
```

This calls `sdcm/utils/common.py:1449` which uses `boto3.resource("ec2").images.filter()` — a `DescribeImages` call that **must** hit real AWS.

### minicloud capabilities (from README)

- **API**: Subset of EC2 Query API on `localhost:5000` — supports `CreateVpc`, `CreateSubnet`, `CreateSecurityGroup`, `CreateKeyPair`, `RunInstances`, `DescribeInstances`, `DescribeKeyPairs`
- **VMs**: QEMU/KVM with NVMe controllers, TAP networking, IMDS v2 metadata
- **Instance type**: Only `i4i.large` (2 vCPU, 16 GiB RAM; `--lightweight` mode: 1 vCPU, 1.5 GiB)
- **Networking**: Linux bridges per VPC, TAP devices, host connectivity via `minicloud-setup.sh`
- **AMI download**: Uses EBS Direct API to download real AMI images on first use
- **Missing**: `TerminateInstances`, `StopInstances`, `DescribeImages`, EBS volume management
- **Not enforced**: Security group rules (stored but not enforced in v1)

### EC2 API inventory for artifact test flow

The table below lists every EC2 API action SCT calls during the AMI artifact test (`test_scylla_service` with `ami.yaml`: 1 db node, 0 loaders, 0 monitors). This inventory is needed to evaluate minicloud compatibility before implementation begins — each operation must either be supported by minicloud, handled by real AWS (selective routing), or have a minicloud issue filed.

| EC2 API Action | SCT File | Flow Phase | minicloud status |
|----------------|----------|------------|-----------------|
| **Config / AMI resolution — always routed to real AWS** |
| DescribeImages (filter by name/tag) | sdcm/utils/common.py:1494 | config | real AWS (selective routing) |
| DescribeImages (SSM resolve) | sdcm/utils/common.py:1475 | config | real AWS (selective routing) |
| DescribeInstanceTypes | sdcm/utils/aws_utils.py:511 | config | real AWS (selective routing) |
| **VPC / network setup — routed to minicloud** |
| DescribeVpcs | sdcm/utils/aws_region.py:57 | network | supported |
| CreateVpc | sdcm/utils/aws_region.py:73 | network | supported |
| ModifyVpcAttribute (DNS hostnames) | sdcm/utils/aws_region.py:76 | network | **needs investigation** |
| CreateTags (VPC, subnet, SG, IGW, RT) | sdcm/utils/aws_region.py (multiple) | network | **needs investigation** |
| DescribeAvailabilityZones | sdcm/utils/aws_region.py:84 | network | **needs investigation** |
| DescribeInstanceTypeOfferings | sdcm/utils/aws_region.py:89 | network | **needs investigation** |
| DescribeSubnets | sdcm/utils/aws_region.py:120 | network | **needs investigation** |
| CreateSubnet | sdcm/utils/aws_region.py:139 | network | supported |
| ModifySubnetAttribute (public IP, IPv6) | sdcm/utils/aws_region.py:149 | network | **needs investigation** |
| DescribeInternetGateways | sdcm/utils/aws_region.py:218 | network | **needs investigation** |
| CreateInternetGateway | sdcm/utils/aws_region.py:240 | network | **needs investigation** |
| AttachInternetGateway | sdcm/utils/aws_region.py:250 | network | **needs investigation** |
| DescribeRouteTables | sdcm/utils/aws_region.py:183 | network | **needs investigation** |
| CreateRouteTable | sdcm/utils/aws_region.py:295 | network | **needs investigation** |
| CreateRoute (IPv4/IPv6 to IGW) | sdcm/utils/aws_region.py:302 | network | **needs investigation** |
| AssociateRouteTable | sdcm/utils/aws_region.py:189 | network | **needs investigation** |
| DescribeSecurityGroups | sdcm/utils/aws_region.py:336 | network | supported (implied by CreateSecurityGroup) |
| CreateSecurityGroup | sdcm/utils/aws_region.py:364 | network | supported |
| AuthorizeSecurityGroupIngress | sdcm/utils/aws_region.py:374 | network | supported (stored, not enforced) |
| DescribeKeyPairs | sdcm/utils/aws_region.py:673 | network | supported |
| ImportKeyPair | sdcm/utils/aws_region.py:692 | network | supported (via CreateKeyPair) |
| **Instance lifecycle — routed to minicloud** |
| RunInstances | sdcm/cluster_aws.py:183 | instance | supported |
| DescribeInstances | sdcm/ec2_client.py (multiple) | instance | supported |
| CreateTags (instance) | sdcm/ec2_client.py:277 | instance | **needs investigation** |
| AllocateAddress (EIP) | sdcm/cluster_aws.py:818 | instance | skip in minicloud mode (single NIC) |
| AssociateAddress (EIP) | sdcm/cluster_aws.py:821 | instance | skip in minicloud mode (single NIC) |
| **Teardown — routed to minicloud** |
| TerminateInstances | sdcm/cluster_aws.py:990 | teardown | **not supported — file minicloud issue** |
| ReleaseAddress (EIP) | sdcm/cluster_aws.py:975 | teardown | skip in minicloud mode (no EIP) |
| **Not called in artifact test (spot disabled)** |
| RequestSpotInstances | sdcm/ec2_client.py:120 | instance | not needed (on_demand forced) |
| RequestSpotFleet | sdcm/ec2_client.py:164 | instance | not needed (on_demand forced) |
| DescribeSpotInstanceRequests | sdcm/ec2_client.py:174 | instance | not needed (on_demand forced) |
| CancelSpotInstanceRequests | sdcm/ec2_client.py:199 | instance | not needed (on_demand forced) |
| **Not called in artifact test (0 monitors)** |
| No monitoring-specific EC2 calls | — | — | — |

**Key finding**: The test itself (`test_scylla_service`) makes zero EC2 API calls — all Scylla checks are via SSH/CQL. The EC2 surface is entirely in setup and teardown.

**Action items from inventory**:
1. Operations marked **needs investigation** must be tested against minicloud to determine if they work, return stubs, or fail. For unsupported operations, file minicloud issues and evaluate whether SCT can skip them in minicloud mode.
2. `TerminateInstances` is the most critical missing operation — file a minicloud issue as a priority.
3. Network setup has ~27 distinct API calls. Many may already work or return acceptable stubs. A quick smoke test against minicloud (calling `AwsRegion.configure()` with minicloud endpoint) will reveal which ones need work.

## 3. Goals

1. **Run AMI artifact test locally** against minicloud-managed VMs with zero AWS instance costs
2. **Selective endpoint routing**: AMI resolution and image scanning hit real AWS; instance lifecycle operations (`RunInstances`, `DescribeInstances`, VPC/subnet/SG setup) route to minicloud
3. **Reuse existing AWS backend code** — no separate backend class; minicloud is a deployment mode of the `aws` backend
4. **Zero impact on existing backends** — changes are opt-in via configuration; default behavior is unchanged
5. **Developer experience**: a developer can run `uv run sct.py run-test artifacts_test.ArtifactsTest.test_scylla_service --backend aws --config test-cases/artifacts/ami.yaml` with minicloud running locally and a single config override

## 4. Implementation Phases

### Phase 1: Centralize EC2 client creation — Importance: HIGH

**Objective**: Create a single factory function for EC2 boto3 clients/resources so that endpoint routing can be injected in one place instead of patching 40+ call sites.

**Implementation**:
- Add a new module `sdcm/utils/ec2_services.py` with factory functions:
  ```python
  def get_ec2_client(region_name: str, endpoint_url: str | None = None) -> EC2Client:
      return boto3.client("ec2", region_name=region_name, endpoint_url=endpoint_url)

  def get_ec2_resource(region_name: str, endpoint_url: str | None = None) -> EC2ServiceResource:
      return boto3.resource("ec2", region_name=region_name, endpoint_url=endpoint_url)
  ```
- Migrate `EC2ClientWrapper` (`sdcm/ec2_client.py:55`) to use the factory
- Migrate `AwsRegion` (`sdcm/utils/aws_region.py:45`) to use the factory
- Migrate `sdcm/provision/aws/utils.py:87` to use the factory

**Definition of Done**:
- [ ] All EC2 client creation in `sdcm/ec2_client.py`, `sdcm/utils/aws_region.py`, and `sdcm/provision/aws/utils.py` goes through the factory
- [ ] Existing unit tests pass unchanged
- [ ] No behavioral change (endpoint_url defaults to None = real AWS)

**Dependencies**: None

---

### Phase 2: Add minicloud configuration and activation — Importance: HIGH

**Objective**: Add SCT config parameters to enable minicloud mode, define how the minicloud binary is obtained, and add the activation entry point that will be used by Phase 3 and Phase 4 to prepare the environment.

**Implementation**:
- Add to `sdcm/sct_config.py`:
  ```python
  minicloud_endpoint_url: String = SctField(
      description="""EC2 API endpoint URL for minicloud. When set, instance-lifecycle
          operations (RunInstances, DescribeInstances, VPC/subnet/SG management) are
          routed to this endpoint. AMI resolution and image scanning always use real AWS.
          Example: http://localhost:5000""",
      appendable=False,
  )
  ```
- Add default `minicloud_endpoint_url: ''` in `defaults/test_default.yaml`
- Support env var override: `SCT_MINICLOUD_ENDPOINT_URL=http://localhost:5000`
- **Minicloud version sourcing**: Document the expected minicloud version/commit in a pinned reference (e.g. `defaults/minicloud.yaml` or a constant in `sdcm/utils/minicloud.py`). Initially, developers build minicloud from source at a pinned commit. Future work: provide a pre-built binary via GitHub releases or an SCT helper script (`sct.py setup-minicloud`).
- **SCT activation entry point**: Add `sdcm/utils/minicloud.py` with a `prepare_minicloud_environment()` function. This phase implements only the skeleton and the minicloud reachability check (HTTP health check to the endpoint). The actual VPC/subnet/SG preparation logic is implemented in Phase 3, and the selective endpoint routing is implemented in Phase 4. This function is called early in `get_cluster_aws()` before instance provisioning begins.

**Definition of Done**:
- [ ] Parameter is recognized by `SCTConfiguration` and can be set via env var, config file, or CLI
- [ ] Empty string (default) means no minicloud routing
- [ ] `uv run sct.py conf` shows the new parameter
- [ ] Minicloud version/commit is pinned in a documented location
- [ ] `sdcm/utils/minicloud.py` exists with `prepare_minicloud_environment()` skeleton that verifies minicloud reachability
- [ ] Unit test validates parameter loading and reachability check

**Dependencies**: None (can be done in parallel with Phase 1)

---

### Phase 3: VPC/Subnet/SecurityGroup setup via minicloud — Importance: HIGH

**Objective**: Make the AWS region/network setup work against minicloud. This is a prerequisite for all instance-lifecycle operations — VPC, subnet, and security group must exist before `RunInstances` can be called.

**Implementation**:
- Implement the VPC/subnet/SG preparation logic inside `prepare_minicloud_environment()` (skeleton from Phase 2). This function uses `AwsRegion` with its EC2 client pointed at minicloud (via the factory from Phase 1) to create VPC, subnets, security groups, and key pairs. The created resource IDs are stored in the test config so that `AWSCluster` uses them for `RunInstances`.
- `AwsRegion` (`sdcm/utils/aws_region.py:30`) creates VPCs, subnets, security groups, internet gateways, and key pairs — all supported by minicloud
- The existing `create_sct_vpc()`, `create_sct_subnets()`, `create_sct_security_group()`, and `create_sct_key_pair()` methods should work against minicloud's API with minimal changes
- Handle minicloud stubs: `AuthorizeSecurityGroupIngress` stores rules but doesn't enforce them (acceptable for testing)
- **Note**: `AwsRegion.__init__` calls `all_aws_regions(cached=True)` (`sdcm/utils/common.py:473`) to compute VPC CIDR from region index. The `cached=True` path uses a hardcoded region list (no AWS API call), so this is safe with minicloud. However, if the configured `region_name` is not in the hardcoded list, the `.index()` call will raise `ValueError`. Verify that `us-east-1` (the default for minicloud) is in the list.
- Verify that any `AwsRegion` methods calling unsupported minicloud APIs (e.g. `ModifyVpcAttribute` for DNS hostnames, `CreateVpcEndpoint`) are handled gracefully — catch errors and skip non-essential operations when minicloud is active

**Definition of Done**:
- [ ] VPC, subnet, security group, internet gateway, and key pair creation succeeds against minicloud
- [ ] Network interfaces are correctly attached to instances
- [ ] Unsupported VPC operations (DNS attributes, VPC endpoints) fail gracefully in minicloud mode

**Dependencies**: Phase 1, Phase 2 (can run in parallel with Phase 4 — both depend on Phase 1 + 2 but not on each other)

---

### Phase 4: Selective endpoint routing in EC2 factory — Importance: HIGH

**Objective**: When `minicloud_endpoint_url` is configured, route instance-lifecycle EC2 calls to minicloud while keeping AMI/image calls on real AWS.

**Implementation**:
- Extend the factory from Phase 1 to accept a `use_minicloud: bool` parameter:
  ```python
  def get_ec2_client(region_name: str, use_minicloud: bool = False) -> EC2Client:
      endpoint_url = _get_minicloud_endpoint() if use_minicloud else None
      return boto3.client("ec2", region_name=region_name, endpoint_url=endpoint_url)
  ```
- In `EC2ClientWrapper.__init__`, pass `use_minicloud=True` — this class handles `RunInstances`, `DescribeInstances`, spot requests
- In `AwsRegion.__init__`, pass `use_minicloud=True` — this class handles VPC, subnet, SG creation
- In AMI resolution code (`sdcm/utils/common.py:1449` `convert_name_to_ami_if_needed`), do NOT pass `use_minicloud` — these calls always go to real AWS
- AMI lookup functions in `sdcm/sct_config.py:2489` remain unchanged (real AWS)

**Definition of Done**:
- [ ] With `minicloud_endpoint_url` unset, all behavior is identical to current
- [ ] With `minicloud_endpoint_url` set, `EC2ClientWrapper` and `AwsRegion` create clients pointing to minicloud
- [ ] `convert_name_to_ami_if_needed` and `get_ami_images` still use real AWS regardless
- [ ] Unit tests with mocked boto3 verify correct endpoint routing

**Dependencies**: Phase 1, Phase 2 (can run in parallel with Phase 3 — both depend on Phase 1 + 2 but not on each other)

---

### Phase 5: Adapt AWSCluster for minicloud instance lifecycle — Importance: HIGH

**Objective**: Make `AWSCluster` and `AWSNode` work with minicloud's subset of EC2 API.

**Implementation**:
- `AWSCluster._create_on_demand_instances` (`sdcm/cluster_aws.py:150`): minicloud does not support spot instances, capacity reservations, dedicated hosts, or placement groups. When minicloud is active:
  - Force `instance_provision: "on_demand"` (skip spot logic)
  - Skip `add_capacity_reservation_param` and `add_host_id_param`
  - Skip `add_placement_group_name_param`
- `AWSNode`: minicloud VMs are reachable via their private IP from the host (after `minicloud-setup.sh`). The existing SSH logic should work since `AWSNode` already resolves IP from the EC2 instance metadata.
- Skip EIP allocation logic when using minicloud
- Handle missing `TerminateInstances` — **Needs Investigation**: minicloud does not yet support `TerminateInstances`. The graceful error handling must be guarded so it only applies when `minicloud_endpoint_url` is set — real AWS teardown must never silently skip termination. Track a minicloud issue for `TerminateInstances` support and work to implement it there. Workaround: catch `UnsupportedOperation` only in minicloud mode, log warning, document that minicloud process restart cleans up VMs.

**Definition of Done**:
- [ ] `AWSCluster._create_on_demand_instances` successfully calls minicloud's `RunInstances`
- [ ] `AWSNode` resolves IP and establishes SSH to the minicloud VM
- [ ] Spot instance, capacity reservation, dedicated host, and EIP logic is skipped
- [ ] Graceful handling of missing `TerminateInstances` — only when minicloud is active (never on real AWS)

**Dependencies**: Phase 3, Phase 4

---

### Phase 6: AMI artifact test config and end-to-end validation — Importance: HIGH

**Objective**: Create a test configuration for running the AMI artifact test against minicloud and validate end-to-end.

**Implementation**:
- Create `test-cases/artifacts/ami-minicloud.yaml`:
  ```yaml
  root_disk_size_db: 50
  backtrace_decoding: false
  cluster_backend: 'aws'
  instance_type_db: 'i4i.large'
  instance_provision: 'on_demand'
  n_db_nodes: 1
  n_loaders: 0
  n_monitor_nodes: 0
  nemesis_class_name: 'NoOpMonkey'
  region_name: 'us-east-1'
  scylla_linux_distro: 'centos'
  test_duration: 60
  user_prefix: 'artifacts-ami-minicloud'
  minicloud_endpoint_url: 'http://localhost:5000'
  ip_ssh_connections: 'private'
  ```
- Validate test subtests that are expected to work:
  - `check_scylla` (nodetool status, cassandra-stress)
  - `check_cqlsh`
  - `verify_snitch` (should use `Ec2Snitch` since backend is `aws`)
  - `verify_node_health`
  - `check Scylla server after stop/start` and `after restart`
- Subtests expected to need different paths in the artifact test when running under minicloud:
  - `check ENA support` — **Needs Investigation**: QEMU may not expose ENA; may need to skip or use an alternative check
  - `check Scylla IO Params` (IOTuneValidator) — may produce different results on QEMU NVMe; artifact test can have a minicloud-aware path
  - `verify_nvme_write_cache` — QEMU NVMe may not expose write_cache sysfs; skip or adapt
  - `verify_xfs_online_discard_enabled` — depends on VM image setup
  - `check node_exporter liveness` — requires Prometheus; skipped when `n_monitor_nodes: 0`
  - `check perftune` — may differ on QEMU; artifact test can use a relaxed check under minicloud

**Definition of Done**:
- [ ] `artifacts_test.ArtifactsTest.test_scylla_service` runs against minicloud
- [ ] Core subtests pass: Scylla starts, CQL works, cassandra-stress runs, stop/start/restart works
- [ ] Hardware-specific subtests (ENA, NVMe cache, IO params) have minicloud-aware paths with clear messages
- [ ] Documentation: README section or `docs/minicloud-testing.md` with setup instructions

**Dependencies**: Phase 5

---

### Phase 7: Migrate remaining scattered boto3.client("ec2") calls — Importance: MEDIUM

**Objective**: Migrate the remaining direct `boto3.client("ec2", ...)` calls to use the centralized factory, for consistency and future-proofing.

**Implementation**:
- Migrate calls in:
  - `sdcm/cluster_aws.py:817,973` (EIP allocation, network interface management)
  - `sdcm/provision/aws/capacity_reservation.py` (6 calls)
  - `sdcm/provision/aws/dedicated_host.py` (5 calls)
  - `sdcm/utils/common.py` (AMI-related calls — these should NOT use minicloud endpoint)
  - `sdcm/utils/aws_utils.py` (cleanup, tagging)
  - `sdcm/utils/resources_cleanup.py` (5 calls)
- Each call site must be evaluated: does it deal with instance lifecycle (-> minicloud) or metadata/AMI/cleanup (-> real AWS)?

**Definition of Done**:
- [ ] All `boto3.client("ec2")` and `boto3.resource("ec2")` calls go through the factory
- [ ] Each call site correctly chooses minicloud vs real AWS
- [ ] Existing unit tests pass

**Dependencies**: Phase 1

---

### Phase 8: Documentation and developer guide — Importance: MEDIUM

**Objective**: Document minicloud setup, usage, and known limitations for developers.

**Implementation**:
- Create `docs/minicloud-testing.md` covering:
  - Prerequisites (Linux, KVM, QEMU, Rust toolchain for building minicloud)
  - Building and running minicloud (pinned version/commit reference)
  - Running `minicloud-setup.sh` for host connectivity
  - Running the AMI artifact test locally
  - Known limitations and skipped subtests
  - Troubleshooting (AppArmor on Ubuntu 24.04+, KVM access)
- Update `AGENTS.md` backends section to mention minicloud

**Definition of Done**:
- [ ] `docs/minicloud-testing.md` exists with complete setup guide
- [ ] A developer can follow the guide from scratch and run the artifact test

**Dependencies**: Phase 6

## 5. Testing Requirements

### Unit Tests

| Phase | Test | What it verifies |
|-------|------|-----------------|
| 1 | `test_ec2_factory_default_endpoint` | Factory returns client with no endpoint_url by default |
| 1 | `test_ec2_factory_custom_endpoint` | Factory passes endpoint_url to boto3 when provided |
| 2 | `test_minicloud_config_from_env` | `SCT_MINICLOUD_ENDPOINT_URL` is loaded correctly |
| 2 | `test_prepare_minicloud_reachability_check` | `prepare_minicloud_environment()` raises clear error when minicloud is unreachable |
| 3 | `test_vpc_creation_against_minicloud` | `AwsRegion` VPC/subnet/SG creation works with minicloud endpoint |
| 4 | `test_selective_routing_instance_lifecycle` | `EC2ClientWrapper` uses minicloud endpoint when configured |
| 4 | `test_selective_routing_ami_resolution` | `convert_name_to_ami_if_needed` always uses real AWS |
| 5 | `test_create_instances_skips_spot_for_minicloud` | On-demand is forced when minicloud is active |
| 5 | `test_terminate_graceful_when_unsupported` | Teardown doesn't crash when `TerminateInstances` fails (minicloud only) |

### Integration Tests

| Phase | Test | Service |
|-------|------|---------|
| 6 | `test_minicloud_ami_artifact_e2e` | minicloud (QEMU/KVM) |

This test requires minicloud running locally and KVM access. It should be marked `@pytest.mark.integration` with a `skipif` guard for minicloud availability.

### Manual Testing

| Phase | Procedure |
|-------|-----------|
| 6 | Start minicloud with `--lightweight`, run artifact test, verify Scylla starts and CQL works |
| 6 | Run artifact test with `minicloud_endpoint_url` unset — verify zero behavioral change on real AWS |

## 6. Success Criteria

- [ ] AMI artifact test (`test_scylla_service`) core subtests pass against minicloud with zero AWS instance costs
- [ ] AMI resolution still uses real AWS `DescribeImages` (selective routing works)
- [ ] Existing AWS backend tests pass with no behavioral changes when `minicloud_endpoint_url` is unset
- [ ] Developer can set up minicloud and run the artifact test locally following the documentation
- [ ] No new `boto3.client("ec2")` calls are added without going through the centralized factory (enforced by grep in pre-commit or documented convention)

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| minicloud API incompatibilities (missing parameters, different response shapes) | High | Medium | Start with the simplest flow (on-demand, single node, no spot). Add compatibility shims in the factory layer, not in business logic. For non-trivial incompatibilities, raise issues on the [minicloud repo](https://github.com/scylladb/minicloud) and fix them there — only add shims in SCT for things that are easy to emulate locally. |
| Missing `TerminateInstances` causes teardown failures | High | Medium | Catch `UnsupportedOperation` errors in teardown **only when `minicloud_endpoint_url` is set** — real AWS teardown must never silently skip termination. File a minicloud issue for `TerminateInstances` support and work to implement it upstream. Interim workaround: minicloud process restart cleans up VMs. |
| QEMU NVMe behavior differs from real EC2 NVMe | Medium | Low | Hardware-specific subtests (ENA, NVMe cache, IO params, perftune) get different paths in the artifact test when `minicloud_endpoint_url` is set — e.g. relaxed checks, alternative validations, or graceful skips. These are not the core value of local testing. |
| Scattered `boto3.client("ec2")` calls bypass the factory | Medium | Low | Phase 7 migrates remaining calls. Add a grep-based lint check or document the convention. |
| AMI first-download via EBS Direct API requires AWS credentials | Low | Low | This is a one-time cost. Document that developers need `ebs:ListSnapshotBlocks` + `ebs:GetSnapshotBlock` permissions. Cached AMIs are reused across runs. |
| minicloud is under active development — API surface may change | Medium | Medium | Pin to a specific minicloud version/commit in `defaults/minicloud.yaml` or a constant in `sdcm/utils/minicloud.py`. The SCT activation code (Phase 2) checks minicloud reachability at startup and uses `AwsRegion` to prepare the VPC/network environment before any instance operations. Abstract minicloud-specific workarounds behind the factory layer so updates are localized. Future: add an `sct.py setup-minicloud` command that downloads/builds the pinned version. |
