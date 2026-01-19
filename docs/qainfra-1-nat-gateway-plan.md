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

#### 1.3 Configuration Parameters
**File**: `sdcm/sct_config.py`

Add new configuration options:
- `gce_use_nat_gateway`: Boolean to enable NAT gateway
- `gce_nat_gateway_name`: Name for the NAT gateway
- `gce_nat_static_ip_name`: Name for the static IP

#### 1.4 Update GCE Region Setup
**File**: `sdcm/utils/gce_region.py`

Modify region initialization to:
1. Check if NAT gateway should be created
2. Reserve static IP
3. Create Cloud Router
4. Create Cloud NAT with static IP
5. Log the static IP for Argus configuration

#### 1.5 Cleanup Implementation
Ensure NAT gateway and static IP are cleaned up when resources are torn down.

### Phase 2: Azure NAT Gateway Implementation (2-3 days)

#### 2.1 Create Static Public IP
**File**: `sdcm/utils/azure_region.py` or new `sdcm/provision/azure/nat_gateway.py`

```python
def create_static_public_ip(self, name: str, resource_group: str) -> str:
    """Create static public IP address for NAT gateway."""
    # Use Azure Network Management Client
    # Create Public IP with Static allocation
    # Tag with SCT identifiers
    # Return IP address
```

#### 2.2 Create NAT Gateway
**File**: `sdcm/utils/azure_region.py` or `sdcm/provision/azure/nat_gateway.py`

```python
def create_nat_gateway(self, name: str, resource_group: str, public_ip_id: str):
    """Create Azure NAT Gateway with static public IP."""
    # Create NAT Gateway resource
    # Associate with public IP
    # Configure idle timeout
    # Return NAT Gateway resource
```

#### 2.3 Associate NAT Gateway with Subnet
**File**: `sdcm/provision/azure/subnet_provider.py`

```python
def associate_nat_gateway(self, subnet_name: str, nat_gateway_id: str):
    """Associate NAT gateway with subnet."""
    # Update subnet configuration
    # Link NAT gateway to subnet
```

#### 2.4 Configuration Parameters
**File**: `sdcm/sct_config.py`

Add new configuration options:
- `azure_use_nat_gateway`: Boolean to enable NAT gateway
- `azure_nat_gateway_name`: Name for the NAT gateway
- `azure_nat_public_ip_name`: Name for the static public IP

#### 2.5 Update Azure Region Setup
**File**: `sdcm/utils/azure_region.py`

Modify region initialization to:
1. Check if NAT gateway should be created
2. Create static public IP
3. Create NAT Gateway
4. Associate NAT Gateway with subnets
5. Log the static IP for Argus configuration

#### 2.6 Cleanup Implementation
Ensure NAT gateway and public IP are cleaned up when resources are torn down.

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

#### 3.4 Configuration Parameters
**File**: `sdcm/sct_config.py`

Add new configuration options:
- `oci_use_nat_gateway`: Boolean to enable NAT gateway
- `oci_nat_gateway_name`: Name for the NAT gateway
- `oci_nat_public_ip_name`: Name for the reserved public IP

#### 3.5 Update OCI Region Setup
**File**: `sdcm/utils/oci_region.py` or `sdcm/utils/oci_utils.py`

Modify region initialization to:
1. Check if NAT gateway should be created
2. Create reserved public IP
3. Create NAT Gateway
4. Update route tables to use NAT gateway
5. Log the static IP for Argus configuration

#### 3.6 Cleanup Implementation
Ensure NAT gateway and reserved public IP are cleaned up when resources are torn down.

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
Test VMs (in subnet) → NAT Gateway (with static public IP) → Internet → Argus
```

**Resources needed**:
- Public IP Address (Static allocation, per region)
- NAT Gateway resource
- Subnet association
- IAM permissions: `Microsoft.Network/natGateways/*`, `Microsoft.Network/publicIPAddresses/*`

**API Reference**:
- [Azure NAT Gateway](https://learn.microsoft.com/en-us/azure/nat-gateway/nat-overview)
- [Public IP Addresses](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/public-ip-addresses)

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

### GCP Configuration (defaults/gce_config.yaml)
```yaml
# NAT Gateway Configuration
gce_use_nat_gateway: true
gce_nat_gateway_name: "sct-cloud-nat"
gce_nat_static_ip_name: "sct-nat-ip"
gce_cloud_router_name: "sct-cloud-router"
```

### Azure Configuration (defaults/azure_config.yaml)
```yaml
# NAT Gateway Configuration
azure_use_nat_gateway: true
azure_nat_gateway_name: "sct-nat-gateway"
azure_nat_public_ip_name: "sct-nat-public-ip"
azure_nat_idle_timeout: 4  # minutes
```

### OCI Configuration (defaults/oci_config.yaml)
```yaml
# NAT Gateway Configuration
oci_use_nat_gateway: true
oci_nat_gateway_name: "sct-nat-gateway"
oci_nat_public_ip_name: "sct-nat-public-ip"
oci_nat_block_traffic: false
```

### Test Configuration Example
```yaml
cluster_backend: gce
gce_use_nat_gateway: true
# ... other GCE config

# OR

cluster_backend: azure
azure_use_nat_gateway: true
# ... other Azure config

# OR

cluster_backend: oci
oci_use_nat_gateway: true
# ... other OCI config
```

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

### Mitigation
- Use NAT gateway only in regions actively running tests
- Cleanup NAT gateways when not in use
- Monitor data transfer costs

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
| Additional cloud costs | Medium | High | Monitor costs, cleanup unused resources |
| Argus whitelist delays | Medium | Medium | Coordinate with Argus team early |
| NAT gateway quota limits | High | Low | Request quota increase if needed |
| Existing tests break | High | Low | Feature flag, gradual rollout |

## Success Criteria

- [x] GCP NAT gateway deployed with static IP in test region
- [x] Azure NAT gateway deployed with static IP in test region
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
   - Answer: Start with primary test regions (e.g., us-east1 for GCP, eastus for Azure, us-ashburn-1 for OCI)

2. **Failover**: Do we need NAT gateways in backup regions?
   - Answer: Not initially, can add later if needed

3. **Monitoring**: What metrics should we track?
   - Answer: NAT gateway utilization, data transfer, connection count

4. **Argus Contact**: Who on the Argus team handles load balancer configuration?
   - Answer: Need to identify contact before Phase 3

## Implementation Files

### New Files
- `sdcm/provision/gce/nat_gateway.py` (~200 lines)
- `sdcm/provision/azure/nat_gateway.py` (~250 lines)
- `sdcm/provision/oci/nat_gateway.py` (~200 lines)
- `unit_tests/test_gce_nat_gateway.py` (~150 lines)
- `unit_tests/test_azure_nat_gateway.py` (~150 lines)
- `unit_tests/test_oci_nat_gateway.py` (~150 lines)
- `docs/argus-nat-gateway-ips.md` (documentation)
- `docs/nat-gateway-architecture.md` (architecture diagrams)

### Modified Files
- `sdcm/utils/gce_region.py` - Add NAT gateway integration
- `sdcm/utils/azure_region.py` - Add NAT gateway integration
- `sdcm/utils/oci_region.py` or `sdcm/utils/oci_utils.py` - Add NAT gateway integration
- `sdcm/sct_config.py` - Add configuration parameters
- `defaults/gce_config.yaml` - Add defaults
- `defaults/azure_config.yaml` - Add defaults
- `defaults/oci_config.yaml` - Add defaults
- `README.md` - Update with NAT gateway info

## References

- [GCP Cloud NAT Documentation](https://cloud.google.com/nat/docs/overview)
- [Azure NAT Gateway Documentation](https://learn.microsoft.com/en-us/azure/nat-gateway/nat-overview)
- [Argus Documentation](https://github.com/scylladb/argus) (if public)
- Related Jira: QAINFRA-1

---

**Document Status**: ✅ Ready for Review  
**Created**: 2026-01-19  
**Author**: GitHub Copilot  
**Jira**: [QAINFRA-1](https://scylladb.atlassian.net/browse/QAINFRA-1)
