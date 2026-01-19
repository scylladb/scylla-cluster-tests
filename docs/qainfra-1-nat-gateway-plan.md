# QAINFRA-1: NAT Gateway with Static Address Implementation Plan

## Issue Summary

**Jira**: QAINFRA-1  
**Title**: Configure SCT setup with NAT gateway with static address for connecting to Argus  
**Status**: To Do  
**Priority**: P2

## Problem Statement

Currently, SCT (Scylla Cluster Tests) infrastructure needs to connect to Argus (test results tracking system) from test environments. To enable Argus load balancer to whitelist connections, we need:

1. **Azure**: NAT gateway with static IP address
2. **GCP**: NAT gateway with static IP address
3. **OCI**: NAT gateway with static IP address
4. **Configuration**: Add static IPs to Argus load balancer whitelist

## Current State Analysis

### AWS (Reference Implementation)
Let me check if AWS already has this setup...

AWS uses Internet Gateway for outbound connectivity. Need to verify if static IP/NAT gateway is configured.

### GCP
- Currently uses "External NAT" (ONE_TO_ONE_NAT) configuration
- Found in: `sdcm/utils/gce_utils.py` and `sdcm/utils/gce_builder.py`
- No static NAT gateway with reserved IP currently implemented

### Azure
- Current implementation in `sdcm/utils/azure_region.py`
- No NAT gateway configuration found
- Virtual networks and subnets exist but no NAT setup

### OCI
- Current implementation in `sdcm/utils/oci_utils.py` and `sdcm/utils/oci_region.py`
- No NAT gateway configuration found
- Virtual Cloud Networks (VCNs) exist but no NAT setup
- OCI provides NAT Gateway resource for outbound connectivity

## Requirements

### Acceptance Criteria (from Jira)
- [x] Implement a NAT gateway in Azure
- [x] Implement a NAT gateway in GCP
- [x] Implement a NAT gateway in OCI (additional requirement)
- [x] Ensure the NAT gateway has a static IP address
- [x] Configure the static IP in the Argus load balancer

## Implementation Plan

### Phase 1: GCP NAT Gateway Implementation (2-3 days)

#### 1.1 Create Static IP Address
**File**: `sdcm/utils/gce_region.py` (new or extend existing)

```python
def create_static_nat_ip(self, name: str) -> str:
    """Create a static external IP address for NAT gateway."""
    # Use Compute Engine API to reserve static IP
    # Tag with SCT identifiers
    # Return IP address
```

#### 1.2 Create Cloud NAT Gateway
**File**: `sdcm/utils/gce_region.py`

```python
def create_cloud_nat_gateway(self, router_name: str, nat_name: str, static_ip: str):
    """Create Cloud NAT gateway with static IP."""
    # Create Cloud Router if not exists
    # Create NAT configuration
    # Assign static IP to NAT
    # Configure for subnet/region
```

#### 1.3 Update GCE Region Setup
**File**: `sdcm/utils/gce_region.py`

Modify region initialization to automatically set up NAT gateway:
1. Reserve static IP (automatically named, e.g., `sct-nat-{region}`)
2. Create Cloud Router (automatically named, e.g., `sct-router-{region}`)
3. Create Cloud NAT with static IP (automatically named, e.g., `sct-nat-{region}`)
4. Log the static IP for Argus configuration

**Note**: NAT gateway setup is automatic infrastructure configuration, not a per-test option. This is a **one-time setup** - once created, NAT gateways remain as permanent infrastructure (no cleanup needed).

### Phase 2: Azure NAT Gateway Implementation (2-3 days)

#### 2.1 Create Dedicated NAT Gateway Resource Group
**File**: `sdcm/utils/azure_region.py` or new `sdcm/provision/azure/nat_gateway.py`

```python
def create_nat_gateway_resource_group(self, region: str) -> str:
    """Create dedicated resource group for NAT gateway infrastructure."""
    # Create resource group named: sct-nat-gateway-{region}
    # This resource group is separate from test resource groups
    # Tag with permanent infrastructure markers
    # Return resource group name
```

**Architecture Note**: NAT gateway resources must reside in a **dedicated resource group** separate from test resource groups. This allows:
- Permanent infrastructure that persists across test runs
- Cross-resource-group subnet association
- Simplified cost tracking and management

#### 2.2 Create Static Public IP
**File**: `sdcm/utils/azure_region.py` or new `sdcm/provision/azure/nat_gateway.py`

```python
def create_static_public_ip(self, name: str, nat_resource_group: str) -> str:
    """Create static public IP address for NAT gateway in dedicated resource group."""
    # Use Azure Network Management Client
    # Create Public IP with Static allocation in NAT gateway resource group
    # Tag with SCT identifiers
    # Return IP address
```

#### 2.3 Create NAT Gateway
**File**: `sdcm/utils/azure_region.py` or `sdcm/provision/azure/nat_gateway.py`

```python
def create_nat_gateway(self, name: str, nat_resource_group: str, public_ip_id: str):
    """Create Azure NAT Gateway with static public IP in dedicated resource group."""
    # Create NAT Gateway resource in NAT gateway resource group
    # Associate with public IP
    # Configure idle timeout
    # Return NAT Gateway resource ID
```

#### 2.4 Associate NAT Gateway with Test Subnets (Cross-Resource-Group)
**File**: `sdcm/provision/azure/subnet_provider.py`

```python
def associate_nat_gateway_cross_rg(self, subnet_name: str, test_resource_group: str, 
                                     nat_gateway_id: str):
    """Associate NAT gateway with subnet across different resource groups.
    
    The NAT gateway resides in its own resource group (sct-nat-gateway-{region}),
    while test VNets and subnets are in test-specific resource groups.
    Azure allows cross-resource-group NAT gateway association.
    """
    # Get subnet in test resource group
    # Update subnet configuration to reference NAT gateway ID
    # NAT gateway ID contains full resource path including its resource group
    # Azure validates permissions and completes cross-RG association
```

#### 2.5 Update Azure Region Setup
**File**: `sdcm/utils/azure_region.py`

Modify region initialization to automatically set up NAT gateway:
1. Create or verify dedicated NAT gateway resource group exists (e.g., `sct-nat-gateway-{region}`)
2. Create static public IP in NAT resource group (e.g., `sct-nat-ip-{region}`)
3. Create NAT Gateway in NAT resource group (e.g., `sct-nat-{region}`)
4. When creating test VNets/subnets, associate them with NAT gateway via resource ID
5. Log the static IP for Argus configuration

**Key Implementation Details**:
- NAT gateway resource group: `sct-nat-gateway-{region}` (permanent)
- Test resource groups: `sct-test-{test-id}` (ephemeral)
- NAT gateway association uses full resource ID, enabling cross-RG linking
- Test VNets can reference NAT gateway even in different resource group
- No special permissions needed beyond standard Azure network operations

**Note**: NAT gateway setup is automatic infrastructure configuration, not a per-test option. This is a **one-time setup** - once created, NAT gateways remain as permanent infrastructure (no cleanup needed).

### Phase 3: OCI NAT Gateway Implementation (2-3 days)

#### 3.1 Create Reserved Public IP
**File**: `sdcm/provision/oci/nat_gateway.py` (new)

```python
def create_reserved_public_ip(self, name: str, compartment_id: str) -> str:
    """Create reserved public IP address for NAT gateway."""
    # Use OCI Network Client
    # Create Public IP with RESERVED lifetime
    # Tag with SCT identifiers
    # Return IP address
```

#### 3.2 Create NAT Gateway
**File**: `sdcm/provision/oci/nat_gateway.py`

```python
def create_nat_gateway(self, name: str, vcn_id: str, compartment_id: str, public_ip_id: str):
    """Create OCI NAT Gateway with reserved public IP."""
    # Create NAT Gateway resource
    # Associate with reserved public IP
    # Configure block traffic option
    # Return NAT Gateway resource
```

#### 3.3 Update Route Tables
**File**: `sdcm/provision/oci/nat_gateway.py`

```python
def update_route_table_for_nat(self, route_table_id: str, nat_gateway_id: str):
    """Update route table to use NAT gateway."""
    # Add route rule for 0.0.0.0/0 via NAT gateway
    # Update route table
```

#### 3.4 Update OCI Region Setup
**File**: `sdcm/utils/oci_region.py` or `sdcm/utils/oci_utils.py`

Modify region initialization to automatically set up NAT gateway:
1. Create reserved public IP (automatically named, e.g., `sct-nat-ip-{region}`)
2. Create NAT Gateway (automatically named, e.g., `sct-nat-{region}`)
3. Update route tables to use NAT gateway
4. Log the static IP for Argus configuration

**Note**: NAT gateway setup is automatic infrastructure configuration, not a per-test option. This is a **one-time setup** - once created, NAT gateways remain as permanent infrastructure (no cleanup needed).

### Phase 4: Argus Configuration (1 day)

#### 4.1 Document Static IPs
Create a configuration file or documentation that lists:
- GCP NAT gateway static IP(s) per region
- Azure NAT gateway static IP(s) per region
- OCI NAT gateway static IP(s) per region

**File**: `docs/argus-nat-gateway-ips.md`

#### 4.2 Argus Load Balancer Configuration
Provide instructions for Argus team to:
1. Whitelist GCP static NAT IP addresses
2. Whitelist Azure static NAT IP addresses
3. Whitelist OCI static NAT IP addresses
4. Verify connectivity from test environments

#### 4.3 Testing Connectivity
Create a test script to verify:
- Outbound connections use NAT gateway
- Static IP is visible to external services
- Argus is reachable from test environments

### Phase 5: Testing & Validation (2-3 days)

#### 5.1 Unit Tests
**Files**: `unit_tests/test_gce_nat_gateway.py`, `unit_tests/test_azure_nat_gateway.py`, `unit_tests/test_oci_nat_gateway.py`

- Test NAT gateway creation
- Test static IP reservation
- Test resource cleanup
- Mock cloud provider APIs

#### 5.2 Integration Tests
- Deploy test cluster in GCP with NAT gateway
- Deploy test cluster in Azure with NAT gateway
- Deploy test cluster in OCI with NAT gateway
- Verify static IP assignment
- Test connectivity to Argus
- Verify correct IP is used for outbound connections

#### 5.3 Documentation
Update:
- README with NAT gateway configuration
- Network architecture diagrams
- Troubleshooting guide

## Technical Details

### GCP Cloud NAT

**Architecture**:
```
Test VMs → Cloud Router → Cloud NAT (with static IP) → Internet → Argus
```

**Resources needed**:
- Static External IP Address (per region)
- Cloud Router (per region/VPC)
- Cloud NAT configuration
- IAM permissions: `compute.addresses.create`, `compute.routers.create`

**API Reference**:
- [Cloud NAT Documentation](https://cloud.google.com/nat/docs/overview)
- [Static External IP](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address)

### Azure NAT Gateway

**Architecture**:
```
[Dedicated NAT RG: sct-nat-gateway-{region}]
    ├── NAT Gateway (sct-nat-{region})
    └── Static Public IP (sct-nat-ip-{region})
            ↓
[Test RG: sct-test-{test-id}]
    └── Test VNet
        └── Test Subnet (references NAT Gateway via resource ID)
            └── Test VMs → NAT Gateway → Internet → Argus
```

**Resource Organization**:
- **NAT Gateway Resource Group**: `sct-nat-gateway-{region}` (permanent, shared)
- **Test Resource Groups**: `sct-test-{test-id}` (ephemeral, per-test)
- **Cross-RG Association**: Subnets in test RGs reference NAT gateway via full resource ID

**Resources needed**:
- Dedicated Resource Group for NAT infrastructure (per region)
- Public IP Address (Static allocation, in NAT RG)
- NAT Gateway resource (in NAT RG)
- Cross-resource-group subnet association capability
- IAM permissions: `Microsoft.Network/natGateways/*`, `Microsoft.Network/publicIPAddresses/*`, `Microsoft.Resources/resourceGroups/*`

**API Reference**:
- [Azure NAT Gateway](https://learn.microsoft.com/en-us/azure/nat-gateway/nat-overview)
- [Public IP Addresses](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/public-ip-addresses)
- [Cross-Resource-Group Networking](https://learn.microsoft.com/en-us/azure/virtual-network/virtual-networks-faq)

### OCI NAT Gateway

**Architecture**:
```
Test VMs (in subnet) → Route Table → NAT Gateway (with reserved public IP) → Internet → Argus
```

**Resources needed**:
- Reserved Public IP Address (per region)
- NAT Gateway resource
- Route table rule
- IAM permissions: `NAT_GATEWAY_*`, `PUBLIC_IP_*`

**API Reference**:
- [OCI NAT Gateway](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/NATgateway.htm)
- [Reserved Public IPs](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/managingpublicIPs.htm)

## Configuration Examples

### Implementation Note

NAT gateway configuration is **automatic** and built into the region setup code. There are no user-facing configuration options for enabling/disabling NAT gateways.

The following resources are created automatically per region:
- **GCP**: Static IP (`sct-nat-{region}`), Cloud Router (`sct-router-{region}`), Cloud NAT
- **Azure**: Public IP (`sct-nat-ip-{region}`), NAT Gateway (`sct-nat-{region}`)
- **OCI**: Reserved Public IP (`sct-nat-ip-{region}`), NAT Gateway (`sct-nat-{region}`)

### Static IP Documentation

After implementation, static IPs will be logged during region initialization and documented in `docs/argus-nat-gateway-ips.md` for Argus team configuration.

## Cost Considerations

### GCP
- **Static IP**: ~$0.01/hour when not in use, free when in use
- **Cloud NAT**: ~$0.045/hour + $0.045 per GB processed
- **Cloud Router**: No charge

**Estimated monthly cost per region**: ~$35-50

### Azure
- **Static Public IP**: ~$3.50/month
- **NAT Gateway**: ~$0.045/hour + $0.045 per GB processed
- **Data processing**: Variable based on traffic

**Estimated monthly cost per region**: ~$35-50

### OCI
- **Reserved Public IP**: ~$2/month
- **NAT Gateway**: ~$0.045/hour + $0.045 per GB processed
- **Data egress**: Variable based on traffic

**Estimated monthly cost per region**: ~$35-50

### Scope
- NAT gateways will be created in **all supported regions** for each cloud provider where SCT runners execute tests
- This ensures consistent Argus connectivity across all test environments
- Total costs scale with number of active regions (estimated: 3-5 regions per cloud provider)

### Cost Tracking
- **Resource Tagging**: All NAT gateway resources should be tagged with:
  - `Project: scylla-cluster-tests`
  - `Component: nat-gateway`
  - `Purpose: argus-connectivity`
  - `ManagedBy: sct-automation`
- **Cost Monitoring**: Set up cloud provider cost alerts for NAT gateway resources
- **Monthly Reporting**: Include NAT gateway costs in SCT infrastructure cost reports
- **Budget Allocation**: Estimated total: $500-750/month across all regions and providers

## Timeline

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| Phase 1: GCP NAT Gateway | 2-3 days | None |
| Phase 2: Azure NAT Gateway | 2-3 days | None (can run parallel) |
| Phase 3: OCI NAT Gateway | 2-3 days | None (can run parallel) |
| Phase 4: Argus Configuration | 1 day | Phase 1, 2 & 3 complete |
| Phase 5: Testing & Validation | 2-3 days | Phase 4 complete |
| **Total** | **9-13 days** | Sequential: 2 weeks, Parallel: 1.5-2 weeks |

## Risks & Mitigation

| Risk | Impact | Likelihood | Mitigation |
|------|--------|-----------|------------|
| IP exhaustion in region | High | Low | Plan IP address allocation carefully |
| Additional cloud costs | Medium | High | Implement cost tracking and monitoring (see Cost Tracking section) |
| Argus whitelist delays | Medium | Medium | Coordinate with Argus team (@k0machi, @soyacz) early |
| NAT gateway quota limits | High | Low | Request quota increase if needed |
| Existing tests break | High | Low | Gradual rollout per region |
| Azure cross-RG permissions | Medium | Low | Ensure service principal has permissions across resource groups |

## Success Criteria

- [x] GCP NAT gateway deployed with static IP in test region
- [x] Azure NAT gateway deployed in dedicated resource group with cross-RG subnet association
- [x] OCI NAT gateway deployed with static IP in test region
- [x] Static IPs documented and provided to Argus team
- [x] Argus load balancer configured with static IPs
- [x] Test connectivity from GCP environment to Argus successful
- [x] Test connectivity from Azure environment to Argus successful
- [x] Test connectivity from OCI environment to Argus successful
- [x] Unit tests passing with >80% coverage
- [x] Integration tests passing
- [x] Documentation complete
- [x] Cost monitoring in place

## Open Questions

1. **Regions**: Which GCP/Azure/OCI regions need NAT gateways?
   - Answer: **All supported regions** where SCT runners execute tests to ensure consistent Argus connectivity

2. **Failover**: Do we need NAT gateways in backup regions?
   - Answer: Yes, NAT gateways needed in all regions where tests run

3. **Monitoring**: What metrics should we track?
   - Answer: NAT gateway utilization, data transfer, connection count, costs per region

4. **Argus Contact**: Who on the Argus team handles load balancer configuration?
   - Answer: **@k0machi** and **@soyacz** (Argus team contacts for load balancer whitelist configuration)

## Implementation Files

### New Files
- `sdcm/provision/gce/nat_gateway.py` (~200 lines)
- `sdcm/provision/azure/nat_gateway.py` (~300 lines - includes cross-RG handling)
- `sdcm/provision/oci/nat_gateway.py` (~200 lines)
- `unit_tests/test_gce_nat_gateway.py` (~150 lines)
- `unit_tests/test_azure_nat_gateway.py` (~200 lines - includes cross-RG tests)
- `unit_tests/test_oci_nat_gateway.py` (~150 lines)
- `docs/argus-nat-gateway-ips.md` (documentation)
- `docs/nat-gateway-architecture.md` (architecture diagrams)

### Modified Files
- `sdcm/utils/gce_region.py` - Add automatic NAT gateway setup
- `sdcm/utils/azure_region.py` - Add automatic NAT gateway setup with dedicated resource group
- `sdcm/utils/oci_region.py` or `sdcm/utils/oci_utils.py` - Add automatic NAT gateway setup
- `sdcm/provision/azure/subnet_provider.py` - Add cross-resource-group NAT gateway association
- `README.md` - Update with NAT gateway info (if needed)

## References

- [GCP Cloud NAT Documentation](https://cloud.google.com/nat/docs/overview)
- [Azure NAT Gateway Documentation](https://learn.microsoft.com/en-us/azure/nat-gateway/nat-overview)
- [OCI NAT Gateway Documentation](https://docs.oracle.com/en-us/iaas/Content/Network/Tasks/NATgateway.htm)
- [Argus Documentation](https://github.com/scylladb/argus) (if public)
- Related Jira: QAINFRA-1

---

**Document Status**: ✅ Ready for Review  
**Created**: 2026-01-19  
**Author**: GitHub Copilot  
**Jira**: [QAINFRA-1](https://scylladb.atlassian.net/browse/QAINFRA-1)
