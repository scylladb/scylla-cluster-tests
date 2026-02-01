# Mixed Instance Type Clusters - Usage Examples

This document provides practical examples for configuring clusters with multiple instance types using the unified cluster topology format (Option 3).

---

## Basic Examples

### Example 1: Single DC, Mixed Instance Types

**Use Case**: Test elastic cloud with 2 different instance sizes in one datacenter

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge]
      - [i4i.large, i4i.2xlarge]
      - [i4i.large, i4i.2xlarge]
```

**Result**:
- 1 datacenter (dc1)
- 3 racks
- 6 total nodes (2 per rack)
- Each rack has 1 i4i.large + 1 i4i.2xlarge

**Visual Layout**:
```
DC1
├── Rack 0
│   ├── node0: i4i.large
│   └── node1: i4i.2xlarge
├── Rack 1
│   ├── node2: i4i.large
│   └── node3: i4i.2xlarge
└── Rack 2
    ├── node4: i4i.large
    └── node5: i4i.2xlarge
```

---

### Example 2: Multi-DC with Different Instance Types

**Use Case**: Cross-region replication with different instance families

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge]
      - [i4i.large, i4i.2xlarge]
  - dc: dc2
    racks:
      - [i3.large, i3.2xlarge]
      - [i3.large, i3.2xlarge]
```

**Result**:
- 2 datacenters (dc1, dc2)
- 2 racks per DC
- 8 total nodes (4 per DC)
- DC1 uses i4i instances, DC2 uses i3 instances

---

### Example 3: Asymmetric Cluster

**Use Case**: Test load balancing with different node counts per rack

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge, i4i.4xlarge]  # 3 nodes
      - [i4i.large, i4i.2xlarge]                # 2 nodes
      - [i4i.large]                              # 1 node
```

**Result**:
- 1 datacenter
- 3 racks with different node counts
- 6 total nodes distributed unevenly

---

## Advanced Examples

### Example 4: Verbose Syntax with Regions and Availability Zones

**Use Case**: Explicit control over regions and AZs for multi-region deployment

```yaml
cluster_topology:
  - datacenter:
      name: us-east
      region: us-east-1
    racks:
      - rack:
          id: 0
          az: us-east-1a
        nodes:
          - instance_type: i4i.large
          - instance_type: i4i.2xlarge
      - rack:
          id: 1
          az: us-east-1b
        nodes:
          - instance_type: i4i.large
          - instance_type: i4i.2xlarge

  - datacenter:
      name: us-west
      region: us-west-2
    racks:
      - rack:
          id: 0
          az: us-west-2a
        nodes:
          - instance_type: i4i.xlarge
      - rack:
          id: 1
          az: us-west-2b
        nodes:
          - instance_type: i4i.xlarge
```

**Result**:
- 2 datacenters in different AWS regions
- Explicit AZ placement
- 6 total nodes

---

### Example 5: Four Instance Sizes for Elastic Cloud Testing

**Use Case**: Test resharding and capacity changes across 4 different sizes

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge, i4i.4xlarge, i4i.8xlarge]
      - [i4i.large, i4i.2xlarge, i4i.4xlarge, i4i.8xlarge]
```

**Result**:
- 1 datacenter
- 2 racks
- 8 total nodes (4 per rack)
- Range from i4i.large (2 vCPU) to i4i.8xlarge (32 vCPU)

**Use for**:
- Testing data migration between node sizes
- Validating resharding logic
- Performance testing at different capacities

---

### Example 6: Load Balancing with Capacity Ratios

**Use Case**: Test load balancer with 1:4 capacity ratio

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.4xlarge]
      - [i4i.large, i4i.4xlarge]
      - [i4i.large, i4i.4xlarge]
```

**Result**:
- 6 nodes alternating between small (2 vCPU) and large (16 vCPU)
- Perfect for testing capacity-aware load balancing

---

## Migration Examples

### Before: Legacy Format

```yaml
# Old configuration (still supported during transition)
n_db_nodes: 6
simulated_racks: 3
instance_type_db: 'i4i.large'
```

### After: Unified Topology Format

```yaml
# New configuration - equivalent to above
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.large]
      - [i4i.large, i4i.large]
      - [i4i.large, i4i.large]
```

**Note**: During transition period, both formats are supported. Legacy parameters are ignored if `cluster_topology` is specified.

---

### Before: Multi-DC Legacy Format

```yaml
# Old multi-DC configuration
n_db_nodes: '6 4'
simulated_racks: 3
instance_type_db: 'i4i.large'
region_name: 'us-east-1 us-west-2'
```

### After: Multi-DC Topology

```yaml
# New configuration
cluster_topology:
  - datacenter:
      name: dc1
      region: us-east-1
    racks:
      - [i4i.large, i4i.large]
      - [i4i.large, i4i.large]
      - [i4i.large, i4i.large]

  - datacenter:
      name: dc2
      region: us-west-2
    racks:
      - [i4i.large]
      - [i4i.large]
      - [i4i.large, i4i.large]
```

---

## Use Case Scenarios

### Scenario 1: Elastic Cloud - Capacity Testing

**Goal**: Test how Scylla handles nodes of different capacities

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.xlarge, i4i.2xlarge]
      - [i4i.large, i4i.xlarge, i4i.2xlarge]
```

**Test Plan**:
1. Write data evenly across all nodes
2. Monitor data distribution
3. Test rebalancing when adding/removing nodes
4. Verify load balancer behavior

---

### Scenario 2: Load Balancing - Heterogeneous Nodes

**Goal**: Validate load balancer with different node capacities

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.4xlarge]
      - [i4i.large, i4i.4xlarge]
      - [i4i.large, i4i.4xlarge]
```

**Test Plan**:
1. Configure capacity-aware load balancing
2. Generate uniform load
3. Verify requests distributed proportionally (1:4 ratio expected)
4. Test under various load patterns

---

### Scenario 3: Cost Optimization - Spot/On-Demand Mix

**Goal**: Mix of spot and on-demand instances with different sizes

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.xlarge, i4i.2xlarge]
      - [i4i.large, i4i.xlarge, i4i.2xlarge]

# Combine with instance_provision parameter
instance_provision: mixed  # 70% spot, 30% on-demand
```

**Test Plan**:
1. Deploy mixed instance types with mixed provisioning
2. Simulate spot instance termination
3. Verify cluster resilience
4. Measure cost savings

---

### Scenario 4: Multi-Region Heterogeneous

**Goal**: Different instance generations per region (availability varies)

```yaml
cluster_topology:
  - datacenter:
      name: us-east
      region: us-east-1
    racks:
      - [i4i.large, i4i.2xlarge]
      - [i4i.large, i4i.2xlarge]

  - datacenter:
      name: eu-west
      region: eu-west-1
    racks:
      - [i3.large, i3.2xlarge]  # Different generation available
      - [i3.large, i3.2xlarge]
```

**Test Plan**:
1. Test cross-region replication
2. Verify performance with heterogeneous hardware
3. Test failover scenarios
4. Monitor data consistency

---

## GCE Examples

### Example 7: GCE with Machine Types

```yaml
# For GCE backend
cluster_backend: gce

cluster_topology:
  - datacenter:
      name: dc1
      region: us-central1
    racks:
      - rack:
          id: 0
          az: us-central1-a
        nodes:
          - instance_type: n2-standard-2
          - instance_type: n2-standard-8
      - rack:
          id: 1
          az: us-central1-b
        nodes:
          - instance_type: n2-standard-2
          - instance_type: n2-standard-8
```

---

## Azure Examples

### Example 8: Azure with VM Sizes

```yaml
# For Azure backend
cluster_backend: azure

cluster_topology:
  - datacenter:
      name: dc1
      region: eastus
    racks:
      - [Standard_D2s_v3, Standard_D8s_v3]
      - [Standard_D2s_v3, Standard_D8s_v3]
```

---

## Docker Backend Examples

### Example 9: Local Testing with Docker

```yaml
# For local development
cluster_backend: docker

cluster_topology:
  - dc: dc1
    racks:
      - [scylla, scylla]  # Instance type not used for docker
      - [scylla, scylla]
```

**Note**: For Docker backend, instance_type is ignored, but topology structure is still used for node distribution.

---

## Future Extensions (Planned)

### Example 10: With Disk Configuration (Future)

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - rack:
          id: 0
        nodes:
          - instance_type: i4i.large
            disk_size: 100
            disk_type: gp3
          - instance_type: i4i.2xlarge
            disk_size: 500
            disk_type: gp3
```

---

### Example 11: With Network Configuration (Future)

```yaml
cluster_topology:
  - datacenter:
      name: dc1
      region: us-east-1
      vpc: vpc-12345
    racks:
      - rack:
          id: 0
          az: us-east-1a
          subnet: subnet-abc123
        nodes:
          - instance_type: i4i.large
            private_ip: 10.0.1.10
          - instance_type: i4i.2xlarge
            private_ip: 10.0.1.11
```

---

## Validation Examples

### Valid Configuration ✅

```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large]  # Minimum: 1 node per rack
```

### Invalid Configurations ❌

```yaml
# ERROR: Empty topology
cluster_topology: []

# ERROR: Empty rack
cluster_topology:
  - dc: dc1
    racks:
      - []  # Must have at least one node

# ERROR: Missing racks key
cluster_topology:
  - dc: dc1
    # Missing 'racks' key

# ERROR: Invalid instance type format
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, '']  # Empty string not allowed
```

---

## Common Patterns

### Pattern 1: Uniform Mixed Types
All racks have the same instance type mix:
```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge]
      - [i4i.large, i4i.2xlarge]
      - [i4i.large, i4i.2xlarge]
```

### Pattern 2: Graduated Sizes
Progressive increase in instance sizes:
```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large]
      - [i4i.xlarge]
      - [i4i.2xlarge]
      - [i4i.4xlarge]
```

### Pattern 3: Majority + Minority
Most nodes one size, few nodes different size:
```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.large, i4i.large, i4i.4xlarge]
      - [i4i.large, i4i.large, i4i.large]
      - [i4i.large, i4i.large, i4i.large]
```

---

## Reference

- **Implementation Plan**: See `docs/plans/mixed_instance_types_cluster_support.md`
- **Configuration Docs**: `docs/configuration_options.md`
- **Test Cases**: `test-cases/*.yaml`

---

## Document Version

- **Created**: 2026-02-02
- **Last Updated**: 2026-02-02
- **Format Version**: Option 3 (Unified Cluster Topology)
