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

### Phase 2: Add minicloud configuration parameters — Importance: HIGH

**Objective**: Add SCT config parameters to enable minicloud mode and specify the endpoint.

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

**Definition of Done**:
- [ ] Parameter is recognized by `SCTConfiguration` and can be set via env var, config file, or CLI
- [ ] Empty string (default) means no minicloud routing
- [ ] `uv run sct.py conf` shows the new parameter
- [ ] Unit test validates parameter loading

**Dependencies**: None (can be done in parallel with Phase 1)

---

### Phase 3: Selective endpoint routing in EC2 factory — Importance: HIGH

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

**Dependencies**: Phase 1, Phase 2

---

### Phase 4: Adapt AWSCluster for minicloud instance lifecycle — Importance: HIGH

**Objective**: Make `AWSCluster` and `AWSNode` work with minicloud's subset of EC2 API.

**Implementation**:
- `AWSCluster._create_on_demand_instances` (`sdcm/cluster_aws.py:150`): minicloud does not support spot instances, capacity reservations, dedicated hosts, or placement groups. When minicloud is active:
  - Force `instance_provision: "on_demand"` (skip spot logic)
  - Skip `add_capacity_reservation_param` and `add_host_id_param`
  - Skip `add_placement_group_name_param`
- `AWSNode`: minicloud VMs are reachable via their private IP from the host (after `minicloud-setup.sh`). The existing SSH logic should work since `AWSNode` already resolves IP from the EC2 instance metadata.
- Skip EIP allocation logic when using minicloud
- Handle missing `TerminateInstances` — **Needs Investigation**: minicloud does not yet support `TerminateInstances`. Teardown may need to kill the minicloud process or use `SIGTERM` on QEMU processes. Workaround: add a `minicloud_skip_terminate` flag or document that teardown requires restarting minicloud.

**Definition of Done**:
- [ ] `AWSCluster._create_on_demand_instances` successfully calls minicloud's `RunInstances`
- [ ] `AWSNode` resolves IP and establishes SSH to the minicloud VM
- [ ] Spot instance, capacity reservation, dedicated host, and EIP logic is skipped
- [ ] Graceful handling of missing `TerminateInstances` (log warning, skip)

**Dependencies**: Phase 3

---

### Phase 5: VPC/Subnet/SecurityGroup setup via minicloud — Importance: HIGH

**Objective**: Make the AWS region/network setup work against minicloud.

**Implementation**:
- `AwsRegion` (`sdcm/utils/aws_region.py:30`) creates VPCs, subnets, security groups, internet gateways — all supported by minicloud
- The existing flow in `sdcm/tester.py:1740` (`get_cluster_aws`) calls `AwsRegion` to set up networking. Since Phase 3 already routes `AwsRegion`'s client to minicloud, this should work with minimal changes
- Handle minicloud stubs: `CreateVpcEndpoint` returns a dummy ID (acceptable), `AuthorizeSecurityGroupIngress` stores rules but doesn't enforce them (acceptable for testing)
- Verify the VPC CIDR used by SCT (`10.x.0.0/16`) doesn't conflict with minicloud's internal CIDR (`172.31.0.0/16`). **Needs Investigation**: check if minicloud enforces CIDR ranges or if SCT's default CIDRs work.

**Definition of Done**:
- [ ] VPC, subnet, security group, internet gateway creation succeeds against minicloud
- [ ] Network interfaces are correctly attached to instances
- [ ] SCT's VPC CIDR allocation works with minicloud (or documented workaround)

**Dependencies**: Phase 3

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
- Subtests expected to be skipped or adapted:
  - `check ENA support` — **Needs Investigation**: QEMU may not expose ENA; may need to skip
  - `check Scylla IO Params` (IOTuneValidator) — may produce different results on QEMU NVMe
  - `verify_nvme_write_cache` — QEMU NVMe may not expose write_cache sysfs
  - `verify_xfs_online_discard_enabled` — depends on VM image setup
  - `check node_exporter liveness` — requires Prometheus; skipped when `n_monitor_nodes: 0`
  - `check perftune` — may differ on QEMU

**Definition of Done**:
- [ ] `artifacts_test.ArtifactsTest.test_scylla_service` runs against minicloud
- [ ] Core subtests pass: Scylla starts, CQL works, cassandra-stress runs, stop/start/restart works
- [ ] Hardware-specific subtests (ENA, NVMe cache, IO params) skip gracefully with clear messages
- [ ] Documentation: README section or `docs/minicloud-testing.md` with setup instructions

**Dependencies**: Phase 4, Phase 5

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
- Each call site must be evaluated: does it deal with instance lifecycle (→ minicloud) or metadata/AMI/cleanup (→ real AWS)?

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
  - Building and running minicloud
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
| 3 | `test_selective_routing_instance_lifecycle` | `EC2ClientWrapper` uses minicloud endpoint when configured |
| 3 | `test_selective_routing_ami_resolution` | `convert_name_to_ami_if_needed` always uses real AWS |
| 4 | `test_create_instances_skips_spot_for_minicloud` | On-demand is forced when minicloud is active |
| 4 | `test_terminate_graceful_when_unsupported` | Teardown doesn't crash when `TerminateInstances` fails |

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
| minicloud API incompatibilities (missing parameters, different response shapes) | High | Medium | Start with the simplest flow (on-demand, single node, no spot). Add compatibility shims in the factory layer, not in business logic. |
| Missing `TerminateInstances` causes teardown failures | High | Medium | Catch `UnsupportedOperation` errors in teardown, log warning, document that minicloud process restart cleans up VMs. Track minicloud issue for `TerminateInstances` support. |
| QEMU NVMe behavior differs from real EC2 NVMe | Medium | Low | Hardware-specific subtests (ENA, NVMe cache, IO params, perftune) skip gracefully with `backend == "aws" and minicloud_endpoint_url` guard. These are not the core value of local testing. |
| VPC CIDR conflicts between SCT defaults and minicloud internals | Medium | Medium | Investigate minicloud CIDR handling. If needed, use a minicloud-specific CIDR in the test config. |
| Scattered `boto3.client("ec2")` calls bypass the factory | Medium | Low | Phase 7 migrates remaining calls. Add a grep-based lint check or document the convention. |
| AMI first-download via EBS Direct API requires AWS credentials | Low | Low | This is a one-time cost. Document that developers need `ebs:ListSnapshotBlocks` + `ebs:GetSnapshotBlock` permissions. Cached AMIs are reused across runs. |
| minicloud is under active development — API surface may change | Medium | Medium | Pin to a specific minicloud version/commit in documentation. Abstract minicloud-specific workarounds behind the factory layer so updates are localized. |
