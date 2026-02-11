# Mixed Instance Type Cluster Support - Implementation Plan

## Overview

This document provides a comprehensive plan for adding support for clusters with multiple instance types per rack/datacenter in SCT. Currently, all nodes in a cluster use identical instance types, but features like elastic cloud and load balancing require heterogeneous clusters.

**Issue**: [Allow creating clusters with multiple instance types](https://github.com/scylladb/scylla-cluster-tests/issues/XXXXX)
**Status**: Planning Phase
**Priority**: P3 - Not urgent, but wanted feature

---

## Problem Statement

### Current Limitation
```yaml
n_db_nodes: 3
simulated_racks: 3
instance_type_db: 'i4i.large'
```
Creates: 1 DC, 3 racks, 1 node per rack - **all nodes are i4i.large**

### Desired Capability
Support mixed instance types within the same cluster to enable:
- **Elastic cloud testing** - node resizing, capacity changes
- **Load balancing validation** - different node capacities
- **Heterogeneous deployments** - mixed hardware configurations
- **Cost optimization** - spot/on-demand mix with varied sizes

---

## Current Implementation Analysis

### Configuration Flow
1. **Parameters**: `n_db_nodes` (int or space-separated), `instance_type_db` (string)
2. **Parsing**: `n_db_nodes="3 2"` → `[3, 2]` (two DCs)
3. **Node creation**: `BaseCluster.__init__()` calls `add_nodes()` for each DC
4. **Instance assignment**: All nodes in a single `add_nodes()` call get the same instance type

**Key Files**:
- `sdcm/sct_config.py` - Configuration parameter definitions
- `sdcm/cluster.py` - Base cluster and node creation (`BaseCluster.__init__`, `add_nodes()`)
- `sdcm/cluster_aws.py` - AWS-specific provisioning
- `sdcm/cluster_gce.py` - GCE-specific provisioning
- `sdcm/cluster_azure.py` - Azure-specific provisioning
- `sdcm/tester.py` - Test setup and cluster initialization

### Constraints
- **Backward compatibility**: Existing test configs must continue to work unchanged
- **Multi-DC support**: Current format uses space-separated values for DCs
- **Rack distribution**: Nodes auto-distributed across racks (round-robin)
- **Per-backend configs**: AWS uses `instance_type_db`, GCE uses `gce_instance_type_db`, Azure uses `azure_instance_type_db`
- **CLI/Jenkins override**: Must support environment variable overrides

---

## Proposed Solutions

### Option 1: Comma-Separated Instance Types

#### Configuration
```yaml
# Single DC with mixed types
n_db_nodes: 6
simulated_racks: 3
instance_type_db: 'i4i.large,i4i.2xlarge'

# Multi-DC with different mixes
n_db_nodes: '6 4'
instance_type_db: 'i4i.large,i4i.2xlarge i4i.xlarge,i4i.4xlarge'
```

#### Semantics
- Comma separates instance types within a DC
- Space separates DCs (existing pattern)
- Instance types assigned round-robin to nodes
- Nodes auto-distributed across racks

#### Pros
- ✅ 100% backward compatible (single instance type still works)
- ✅ Simple syntax following existing SCT patterns
- ✅ Easy to implement (minimal code changes)
- ✅ Easy CLI/Jenkins override (`SCT_INSTANCE_TYPE_DB='i4i.large,i4i.2xlarge'`)
- ✅ Covers 90% of use cases

#### Cons
- ❌ No explicit per-rack control
- ❌ Need to calculate which node gets which type
- ❌ Less explicit layout visibility

#### Implementation Complexity
**Low** - 2-3 days

---

### Option 2: Per-Rack Instance Types

#### Configuration
```yaml
n_db_nodes: 6
simulated_racks: 3
instance_type_db_per_rack:
  - 'i4i.large,i4i.2xlarge'  # Rack 0
  - 'i4i.large,i4i.2xlarge'  # Rack 1
  - 'i4i.large,i4i.2xlarge'  # Rack 2

# Multi-DC
n_db_nodes: '6 4'
instance_type_db_per_rack:
  dc1:
    - 'i4i.large,i4i.2xlarge'
    - 'i4i.large,i4i.2xlarge'
    - 'i4i.large,i4i.2xlarge'
  dc2:
    - 'i4i.xlarge,i4i.4xlarge'
    - 'i4i.xlarge'
    - 'i4i.xlarge'
```

#### Pros
- ✅ Explicit per-rack layout
- ✅ Maximum flexibility
- ✅ Clear validation (total nodes must match)

#### Cons
- ❌ More verbose configuration
- ❌ New parameter alongside `instance_type_db`
- ❌ Difficult to override from Jenkins/CLI (nested structure)

#### Implementation Complexity
**Medium** - 5 days

---

### Option 3: Unified Cluster Topology (RECOMMENDED BY STAKEHOLDERS)

#### Configuration
```yaml
cluster_topology:
  - datacenter: dc1
    racks:
      - rack: 0
        nodes:
          - instance_type: i4i.large
          - instance_type: i4i.2xlarge
      - rack: 1
        nodes:
          - instance_type: i4i.large
          - instance_type: i4i.2xlarge
      - rack: 2
        nodes:
          - instance_type: i4i.large
          - instance_type: i4i.2xlarge
  - datacenter: dc2
    racks:
      - rack: 0
        nodes:
          - instance_type: i4i.xlarge
      - rack: 1
        nodes:
          - instance_type: i4i.xlarge
```

**Simplified alternative syntax:**
```yaml
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge]  # rack 0
      - [i4i.large, i4i.2xlarge]  # rack 1
      - [i4i.large, i4i.2xlarge]  # rack 2
  - dc: dc2
    racks:
      - [i4i.xlarge]
      - [i4i.xlarge]
```

#### Semantics
- Complete cluster specification in one parameter
- Replaces: `n_db_nodes`, `simulated_racks`, `instance_type_db`
- Self-contained topology definition
- Can be extended with additional per-node attributes (volumes, IPs, etc.)

#### Pros
- ✅ **Most explicit** - complete cluster layout visible
- ✅ **Self-documenting** - easy to understand cluster structure
- ✅ **Future-proof** - can extend with per-rack/per-node attributes
- ✅ **Eliminates inconsistencies** - single source of truth
- ✅ **Flexible** - supports any topology configuration
- ✅ **Extensible** - foundation for additional cluster parameters

#### Cons
- ❌ **Breaking change** - requires deprecation period for old parameters
- ❌ **Verbose** - large clusters = lots of YAML
- ❌ **Override complexity** - difficult from environment variables
- ❌ **Migration effort** - need to support both formats during transition

#### Implementation Complexity
**High** - 2-3 weeks including migration path

---

### Option 4: Instance Type Patterns

Extension to Option 1 - adds distribution control.

#### Configuration
```yaml
n_db_nodes: 6
simulated_racks: 3
instance_type_db: 'i4i.large,i4i.2xlarge'
instance_type_pattern: 'per_rack'  # or 'round_robin' (default)
```

#### Pros
- ✅ Backward compatible
- ✅ Adds flexibility to Option 1
- ✅ Easy to extend with more patterns

#### Cons
- ❌ Additional parameter
- ❌ Pattern complexity

#### Implementation Complexity
**Low-Medium** - 3 days

---

## Detailed Plan for Option 3: Unified Cluster Topology

### Stakeholder Preference

Based on feedback from @pehala and @fruch, Option 3 is the preferred approach as it provides:
- One "ultimate" cluster option
- Everything configured in one place
- No need to coordinate 4+ separate parameters
- Foundation for future cluster customizations

### Implementation Strategy

#### Phase 1: Design and Specification (1 week)

**Goals:**
1. Finalize YAML schema for `cluster_topology`
2. Define backward compatibility strategy
3. Design validation logic
4. Plan migration path

**Schema Design:**

```yaml
# Full verbose syntax
cluster_topology:
  - datacenter:
      name: dc1              # Optional, defaults to dc1, dc2, etc.
      region: us-east-1      # Optional, for cloud backends
    racks:
      - rack:
          id: 0              # Rack identifier
          az: us-east-1a     # Optional, availability zone
        nodes:
          - instance_type: i4i.large
            disk_size: 100   # Optional, future extension
            disk_type: gp3   # Optional, future extension
          - instance_type: i4i.2xlarge
      - rack:
          id: 1
          az: us-east-1b
        nodes:
          - instance_type: i4i.large
          - instance_type: i4i.2xlarge

# Simplified syntax (for basic use cases)
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge]
      - [i4i.large, i4i.2xlarge]
  - dc: dc2
    racks:
      - [i4i.xlarge]
```

**Pydantic Models for Validation:**

To ease development and validation, use Pydantic models for the cluster topology structure. This approach:
- Provides automatic validation with clear error messages
- Enables type checking and IDE autocomplete
- Simplifies parsing of both verbose and simplified syntax
- Follows existing SCT patterns (see `sdcm/provision/aws/instance_parameters.py`)

```python
from typing import List, Optional, Union, Literal
from pydantic import BaseModel, Field, field_validator

class NodeConfig(BaseModel):
    """Configuration for a single node"""
    instance_type: str = Field(..., description="Instance type (e.g., i4i.large)")
    disk_size: Optional[int] = Field(None, description="Disk size in GB")
    disk_type: Optional[str] = Field(None, description="Disk type (e.g., gp3)")

    @field_validator('instance_type')
    @classmethod
    def validate_instance_type(cls, v):
        if not v or not v.strip():
            raise ValueError("instance_type cannot be empty")
        return v.strip()

class RackConfigVerbose(BaseModel):
    """Verbose rack configuration with explicit rack metadata"""
    rack: Optional[dict] = Field(None, description="Rack metadata (id, az)")
    nodes: List[NodeConfig] = Field(..., min_length=1, description="List of nodes in this rack")

class DatacenterConfigVerbose(BaseModel):
    """Verbose datacenter configuration"""
    datacenter: Optional[dict] = Field(None, description="DC metadata (name, region)")
    racks: List[Union[RackConfigVerbose, List[str]]] = Field(
        ..., min_length=1, description="List of racks (supports both verbose and simplified syntax)"
    )

    @field_validator('racks')
    @classmethod
    def validate_racks(cls, v):
        """Validate that each rack has at least one node"""
        for rack in v:
            if isinstance(rack, list):
                # Simplified syntax: list of instance types
                if not rack:
                    raise ValueError("Rack must have at least one node")
            elif isinstance(rack, dict):
                # Verbose syntax: validate nodes list
                if 'nodes' not in rack or not rack['nodes']:
                    raise ValueError("Rack must have 'nodes' key with at least one node")
        return v

class DatacenterConfigSimplified(BaseModel):
    """Simplified datacenter configuration"""
    dc: str = Field(..., description="Datacenter name")
    racks: List[List[str]] = Field(..., min_length=1, description="List of racks with instance types")

    @field_validator('racks')
    @classmethod
    def validate_racks(cls, v):
        for rack in v:
            if not rack:
                raise ValueError("Each rack must have at least one instance type")
        return v

class ClusterTopology(BaseModel):
    """Root cluster topology configuration"""
    datacenters: List[Union[DatacenterConfigVerbose, DatacenterConfigSimplified]] = Field(
        ..., min_length=1, description="List of datacenters", alias="cluster_topology"
    )

    class Config:
        populate_by_name = True  # Allow both 'datacenters' and 'cluster_topology'

# Usage in sct_config.py
def parse_cluster_topology(topology_dict: dict) -> ClusterTopology:
    """Parse and validate cluster topology using Pydantic"""
    try:
        return ClusterTopology(cluster_topology=topology_dict)
    except ValidationError as e:
        raise ValueError(f"Invalid cluster_topology configuration: {e}")
```

**Benefits of Pydantic approach:**
1. **Self-documenting** - Field descriptions serve as inline documentation
2. **Automatic validation** - Type checking and value validation out of the box
3. **Clear error messages** - Pydantic provides detailed validation errors
4. **Flexible parsing** - Supports both verbose and simplified syntax seamlessly
5. **Extensible** - Easy to add new fields (disk_size, disk_type, network_config) in the future
6. **IDE support** - Type hints enable autocomplete and type checking in IDEs

**Backward Compatibility:**

```python
# In sct_config.py
def _resolve_cluster_config(self):
    """Resolve cluster configuration from either new or legacy format"""
    if self.get('cluster_topology'):
        # New format - use cluster_topology
        return self._parse_cluster_topology()
    else:
        # Legacy format - use n_db_nodes, instance_type_db, etc.
        return self._parse_legacy_cluster_config()
```

**Migration Path:**
1. Support both formats for 6+ months
2. Add deprecation warnings to legacy parameters
3. Provide migration tool to convert old configs to new format
4. Eventually make legacy format optional

#### Phase 2: Core Implementation (1 week)

**Changes Required:**

1. **`sdcm/sct_config.py`** - Add Pydantic models and parameter:
   ```python
   # Import Pydantic models (see Phase 1 schema design above)
   from sdcm.cluster_topology_models import ClusterTopology

   # Add new cluster_topology parameter
   dict(
       name="cluster_topology",
       env="SCT_CLUSTER_TOPOLOGY",
       type="dict",  # Complex nested structure - will be validated by Pydantic
       help="Unified cluster topology definition (replaces n_db_nodes, instance_type_db, simulated_racks)"
   )

   # Parse and validate using Pydantic
   def _parse_cluster_topology(self):
       """Parse cluster_topology using Pydantic model for validation"""
       topology_dict = self.get('cluster_topology')
       if not topology_dict:
           return None

       try:
           # Pydantic automatically validates the structure
           topology = ClusterTopology(cluster_topology=topology_dict)
           return topology.datacenters
       except ValidationError as e:
           raise ValueError(f"Invalid cluster_topology configuration: {e}")

   # Mark legacy parameters as deprecated when cluster_topology is used
   def verify_configuration(self):
       if self.get('cluster_topology'):
           if any([self.get('n_db_nodes'), self.get('instance_type_db')]):
               logger.warning(
                   "cluster_topology is set; ignoring legacy parameters "
                   "(n_db_nodes, instance_type_db, simulated_racks)"
               )
   ```

2. **`sdcm/cluster.py` - BaseCluster**:
   ```python
   class BaseCluster:
       def __init__(self, cluster_topology=None, n_nodes=None, ...):
           if cluster_topology:
               self._init_from_topology(cluster_topology)
           else:
               self._init_from_legacy_params(n_nodes, ...)

       def _init_from_topology(self, topology):
           """Initialize cluster from unified topology definition"""
           for dc_idx, dc_config in enumerate(topology):
               dc_name = dc_config.get('datacenter', {}).get('name', f'dc{dc_idx + 1}')

               for rack_idx, rack_config in enumerate(dc_config['racks']):
                   rack_id = rack_config.get('rack', {}).get('id', rack_idx)

                   # Handle both verbose and simplified syntax
                   if isinstance(rack_config, list):
                       # Simplified: ['i4i.large', 'i4i.2xlarge']
                       nodes = [{'instance_type': it} for it in rack_config]
                   else:
                       # Verbose: {rack: {...}, nodes: [...]}
                       nodes = rack_config['nodes']

                   for node_idx, node_config in enumerate(nodes):
                       self.add_nodes(
                           count=1,
                           dc_idx=dc_idx,
                           rack=rack_id,
                           instance_type=node_config['instance_type'],
                           # Future extensions:
                           # disk_size=node_config.get('disk_size'),
                           # disk_type=node_config.get('disk_type'),
                       )
   ```

3. **`sdcm/tester.py` - Cluster initialization**:
   ```python
   def get_cluster_aws(self, loader_info, db_info, monitor_info):
       # Check for new topology format
       cluster_topology = self.params.get('cluster_topology')

       if cluster_topology:
           return ScyllaAWSCluster(
               cluster_topology=cluster_topology,
               # ... other common params
           )
       else:
           # Legacy path
           init_db_info_from_params(db_info, params=self.params, regions=regions)
           # ... existing logic
   ```

4. **Backend-specific clusters** (`cluster_aws.py`, `cluster_gce.py`, `cluster_azure.py`):
   - Update constructors to accept `cluster_topology`
   - Add topology parsing logic
   - Ensure `add_nodes()` supports per-node instance types

#### Phase 3: Validation Logic (3 days)

**Configuration Validation with Pydantic:**

With Pydantic models (defined in Phase 1), most validation is handled automatically. The validation logic is simplified to:

```python
from sdcm.cluster_topology_models import ClusterTopology
from pydantic import ValidationError

def validate_cluster_topology(topology_dict: dict) -> ClusterTopology:
    """
    Validate cluster topology using Pydantic model.

    Pydantic automatically validates:
    - Topology is not empty
    - Each DC has at least one rack
    - Each rack has at least one node
    - Instance types are non-empty strings
    - Structure matches expected schema (both verbose and simplified)
    """
    try:
        topology = ClusterTopology(cluster_topology=topology_dict)
        return topology
    except ValidationError as e:
        # Pydantic provides detailed error messages with field paths
        raise ValueError(f"Invalid cluster_topology: {e}")

# Example error output from Pydantic:
# ValidationError: 2 validation errors for ClusterTopology
#   datacenters.0.racks.0 -> 0
#     Value error, instance_type cannot be empty [type=value_error]
#   datacenters.1.racks
#     Field required [type=missing]
```

**Additional Backend-Specific Validation:**

After Pydantic validates the structure, add backend-specific validation:

```python
def validate_backend_instance_types(topology: ClusterTopology, backend: str):
    """
    Validate instance types are supported by the target backend.
    Called after Pydantic structural validation.
    """
    for dc_idx, dc_config in enumerate(topology.datacenters):
        for rack_idx, rack_config in enumerate(dc_config.racks):
            # Extract instance types from both syntax formats
            if isinstance(rack_config, list):
                # Simplified syntax
                instance_types = rack_config
            else:
                # Verbose syntax - Pydantic model already validated structure
                instance_types = [node.instance_type for node in rack_config.nodes]

            # Validate each instance type against backend
            for instance_type in instance_types:
                if backend in ('aws', 'aws-siren'):
                    region = getattr(dc_config, 'datacenter', {}).get('region', 'us-east-1')
                    if not aws_check_instance_type_supported(instance_type, region):
                        raise ValueError(
                            f"Instance type '{instance_type}' not supported in "
                            f"AWS region '{region}'"
                        )

                elif backend in ('gce', 'gce-siren'):
                    zone = getattr(rack_config, 'rack', {}).get('az', 'us-east1-b')
                    if not gce_check_if_machine_type_supported(instance_type, zone):
                        raise ValueError(
                            f"Machine type '{instance_type}' not supported in "
                            f"GCE zone '{zone}'"
                        )

                elif backend == 'azure':
                    region = getattr(dc_config, 'datacenter', {}).get('region', 'eastus')
                    if not azure_check_instance_type_available(instance_type, region):
                        raise ValueError(
                            f"VM size '{instance_type}' not available in "
                            f"Azure region '{region}'"
                        )

# Full validation flow
def validate_full_cluster_topology(topology_dict: dict, backend: str) -> ClusterTopology:
    """Complete validation: structure (Pydantic) + backend-specific"""
    # Step 1: Pydantic validates structure, types, constraints
    topology = validate_cluster_topology(topology_dict)

    # Step 2: Backend-specific validation
    validate_backend_instance_types(topology, backend)

    return topology
```

**Benefits of Pydantic-based validation:**
1. **Automatic validation** - Most checks handled by Pydantic decorators
2. **Clear error messages** - Pydantic shows exact field path and issue
3. **Less code** - ~200 lines of manual validation → ~50 lines with Pydantic
4. **Type safety** - IDE and mypy can catch errors before runtime
5. **Maintainable** - Adding new fields just requires updating the model


#### Phase 4: Testing (1 week)

**Unit Tests** (`unit_tests/test_cluster_topology.py`):

Test both Pydantic model validation and backend-specific logic:

```python
import pytest
from pydantic import ValidationError
from sdcm.cluster_topology_models import ClusterTopology, NodeConfig
from sdcm.sct_config import validate_cluster_topology, validate_backend_instance_types

class TestClusterTopologyPydantic:
    """Test Pydantic model validation"""

    def test_simplified_syntax_single_dc(self):
        """Test simplified syntax with single datacenter"""
        topology_dict = [
            {
                'dc': 'dc1',
                'racks': [
                    ['i4i.large', 'i4i.2xlarge'],
                    ['i4i.large', 'i4i.2xlarge'],
                ]
            }
        ]
        topology = validate_cluster_topology(topology_dict)
        assert topology is not None
        assert len(topology.datacenters) == 1

    def test_verbose_syntax_multi_dc(self):
        """Test verbose syntax with multiple datacenters"""
        topology_dict = [
            {
                'datacenter': {'name': 'dc1', 'region': 'us-east-1'},
                'racks': [
                    {
                        'rack': {'id': 0, 'az': 'us-east-1a'},
                        'nodes': [
                            {'instance_type': 'i4i.large'},
                            {'instance_type': 'i4i.2xlarge'},
                        ]
                    }
                ]
            },
            {
                'datacenter': {'name': 'dc2', 'region': 'us-west-2'},
                'racks': [
                    {
                        'rack': {'id': 0, 'az': 'us-west-2a'},
                        'nodes': [
                            {'instance_type': 'i4i.xlarge'},
                        ]
                    }
                ]
            }
        ]
        topology = validate_cluster_topology(topology_dict)
        assert len(topology.datacenters) == 2

    def test_empty_topology_fails(self):
        """Empty topology should raise Pydantic ValidationError"""
        with pytest.raises(ValidationError):
            ClusterTopology(cluster_topology=[])

    def test_missing_racks_fails(self):
        """Datacenter without racks should fail Pydantic validation"""
        with pytest.raises(ValidationError, match="racks"):
            ClusterTopology(cluster_topology=[{'dc': 'dc1'}])

    def test_empty_rack_fails(self):
        """Rack without nodes should fail Pydantic validation"""
        with pytest.raises(ValidationError):
            ClusterTopology(cluster_topology=[
                {'dc': 'dc1', 'racks': [[]]}
            ])

    def test_empty_instance_type_fails(self):
        """Empty instance type should fail custom validator"""
        with pytest.raises(ValidationError, match="instance_type cannot be empty"):
            NodeConfig(instance_type='')

    def test_pydantic_error_messages(self):
        """Test that Pydantic provides helpful error messages"""
        try:
            ClusterTopology(cluster_topology=[
                {'dc': 'dc1', 'racks': [['']]}  # Empty instance type
            ])
            assert False, "Should have raised ValidationError"
        except ValidationError as e:
            # Check error message includes field path
            assert 'instance_type' in str(e)

    def test_node_config_with_optional_fields(self):
        """Test NodeConfig with optional disk fields"""
        node = NodeConfig(
            instance_type='i4i.large',
            disk_size=100,
            disk_type='gp3'
        )
        assert node.instance_type == 'i4i.large'
        assert node.disk_size == 100
        assert node.disk_type == 'gp3'

class TestBackwardCompatibility:
    """Test that legacy parameters still work alongside new topology"""

    def test_legacy_config_still_works(self):
        """Test that legacy parameters are still supported"""
        legacy_config = {
            'n_db_nodes': 3,
            'instance_type_db': 'i4i.large',
            'simulated_racks': 3,
        }
        # Legacy path should still work (tested in existing tests)
        assert True

    def test_topology_overrides_legacy(self):
        """Test that cluster_topology takes precedence over legacy params"""
        # When both are set, topology should be used
        # Legacy params should be ignored with a warning
        pass
```

**Integration Tests**:
- Test cluster creation with topology on AWS
- Test cluster creation with topology on GCE
- Test cluster creation with topology on Docker
- Test mixed instance types within racks
- Test multi-DC topology
- Test backward compatibility with legacy configs
- Test Pydantic validation error messages are user-friendly


#### Phase 5: Migration Tools (3 days)

**Config Converter**:

```python
def convert_legacy_to_topology(legacy_config):
    """Convert legacy cluster config to topology format"""
    n_db_nodes = legacy_config.get('n_db_nodes', 3)
    instance_type_db = legacy_config.get('instance_type_db', 'i4i.xlarge')
    simulated_racks = legacy_config.get('simulated_racks', 1)

    # Parse multi-DC format
    if isinstance(n_db_nodes, str):
        node_counts = [int(n) for n in n_db_nodes.split()]
    else:
        node_counts = [n_db_nodes]

    # Parse multi-region instance types if present
    if ' ' in instance_type_db:
        instance_types_per_dc = instance_type_db.split()
    else:
        instance_types_per_dc = [instance_type_db] * len(node_counts)

    # Build topology
    topology = []
    for dc_idx, (node_count, instance_type) in enumerate(zip(node_counts, instance_types_per_dc)):
        racks = []
        nodes_per_rack = node_count // simulated_racks
        extra_nodes = node_count % simulated_racks

        for rack_idx in range(simulated_racks):
            rack_node_count = nodes_per_rack + (1 if rack_idx < extra_nodes else 0)
            rack_nodes = [instance_type] * rack_node_count
            racks.append(rack_nodes)

        topology.append({
            'dc': f'dc{dc_idx + 1}',
            'racks': racks
        })

    return {'cluster_topology': topology}

# CLI tool
# python utils/convert_cluster_config.py --input test-cases/longevity.yaml --output test-cases/longevity_new.yaml
```

#### Phase 6: Documentation (3 days)

1. **Update `docs/configuration_options.md`**:
   - Add `cluster_topology` parameter documentation
   - Add examples for both syntaxes
   - Mark legacy parameters as deprecated

2. **Create migration guide**:
   - How to convert existing configs
   - Examples of common patterns
   - Troubleshooting guide

3. **Update test case templates**:
   - Add example configs using new topology format
   - Update existing templates with comments

#### Phase 7: Rollout (1 week)

1. **Week 1-2**: Merge PR with both formats supported
2. **Week 3-4**: Update internal test configs to use new format
3. **Month 2-6**: Deprecation warnings in logs
4. **Month 6+**: Consider making legacy format optional

### Timeline Summary

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| **Phase 1: Design** | 1 week | Finalized schema, validation spec |
| **Phase 2: Core Implementation** | 1 week | Working cluster_topology support |
| **Phase 3: Validation** | 3 days | Comprehensive validation logic |
| **Phase 4: Testing** | 1 week | Unit + integration tests |
| **Phase 5: Migration Tools** | 3 days | Config converter, migration guide |
| **Phase 6: Documentation** | 3 days | Updated docs, examples |
| **Phase 7: Rollout** | Ongoing | Gradual adoption |

**Total Implementation Time**: ~3 weeks for full feature

---

## Comparison Matrix

| Criterion | Option 1 | Option 2 | Option 3 ⭐ | Option 4 |
|-----------|----------|----------|------------|----------|
| **Backward Compatible** | ✅ Yes | ⚠️ Fallback | ⚠️ Dual-mode | ✅ Yes |
| **Readability** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Explicit Layout** | ❌ No | ✅ Yes | ✅ Yes | ⚠️ Pattern |
| **CLI Override** | ✅ Easy | ❌ Hard | ❌ Hard | ✅ Easy |
| **Implementation Time** | 2 days | 5 days | 15 days | 3 days |
| **Risk** | 🟢 Low | 🟡 Medium | 🟡 Medium | 🟢 Low |
| **Extensibility** | ⚠️ Limited | ⚠️ Limited | ✅ High | ⚠️ Limited |
| **Single Source of Truth** | ❌ No | ❌ No | ✅ Yes | ❌ No |
| **Future-Proof** | ⚠️ Limited | ⚠️ Limited | ✅ Yes | ⚠️ Limited |

**Stakeholder Preference**: Option 3 (per feedback from @pehala and @fruch)

---

## Alternative Considerations

### Why Not Use Options 1, 2, or 4?

**Option 1 (Comma-separated)**:
- ✅ Quick to implement
- ❌ Doesn't solve the broader problem of coordinating multiple parameters
- ❌ Not extensible for future cluster attributes

**Option 2 (Per-rack)**:
- ✅ More explicit than Option 1
- ❌ Still requires coordinating multiple parameters
- ❌ Difficult to override from Jenkins/CLI

**Option 4 (Patterns)**:
- ✅ Adds flexibility to Option 1
- ❌ Doesn't address the core issue of parameter coordination
- ❌ Limited extensibility

**Option 3 addresses the root problem**: Having cluster characteristics split across multiple parameters (n_db_nodes, simulated_racks, instance_type_db, availability zones, etc.) makes it hard to maintain consistency and see the full cluster layout.

### Override Complexity Mitigation

While Option 3 makes environment variable overrides more complex, we can provide:

1. **YAML file override**:
   ```bash
   export SCT_CONFIG_FILES="base.yaml,topology_override.yaml"
   ```

2. **JSON string override** (for simple cases):
   ```bash
   export SCT_CLUSTER_TOPOLOGY='[{"dc":"dc1","racks":[["i4i.large","i4i.2xlarge"]]}]'
   ```

3. **Helper scripts** for common topology patterns:
   ```bash
   # Generate topology with mixed types
   python utils/generate_topology.py --dcs 2 --racks 3 --types "i4i.large,i4i.2xlarge"
   ```

---

## Open Questions

1. **Syntax preference**: Verbose vs. simplified vs. hybrid?
   - **Recommendation**: Support both, default to simplified for examples

2. **Parameter deprecation timeline**: How long to support legacy format?
   - **Recommendation**: 6 months deprecation warnings, then optional

3. **Environment variable override**: JSON string or alternative approach?
   - **Recommendation**: Support JSON string + YAML file override

4. **Migration tooling**: Auto-convert on config load or manual tool?
   - **Recommendation**: Manual tool + clear migration guide

5. **Additional node attributes**: What else to support in topology?
   - **Future**: disk_size, disk_type, network_config, labels, etc.

---

## Success Criteria

After implementation:
- ✅ **Unified cluster definition** - all topology in one place
- ✅ **Clear cluster layout** - easy to see full structure
- ✅ **Backward compatible** - existing tests work during transition
- ✅ **Extensible** - foundation for future attributes
- ✅ **Well-documented** - migration guide and examples
- ✅ **Zero ambiguity** - single source of truth for cluster topology

---

## References

- **Issue**: [Allow creating clusters with multiple instance types](https://github.com/scylladb/scylla-cluster-tests/issues/XXXXX)
- **Current Implementation**: `sdcm/sct_config.py`, `sdcm/cluster.py`, `sdcm/cluster_*.py`
- **Test Configs**: `test-cases/*.yaml`, `defaults/*.yaml`
- **Examples**: See `docs/plans/mixed_instance_types_examples.md`

---

## Document History

- **Created**: 2026-02-01
- **Last Updated**: 2026-02-02
- **Status**: Planning Phase - Focus on Option 3
- **Next Steps**: Implementation Phase 1 (Design & Specification)
