# GCE Provisioning Implementation Plan

## Overview
This document outlines the plan to implement a modern provisioning system for Google Cloud Engine (GCE), similar to the existing implementations for AWS and Azure. The goal is to create a `GceProvisioner` class that implements the `Provisioner` interface and integrates with the existing SCT framework.

## Current State Analysis

### Existing GCE Implementation
- **Location**: `sdcm/cluster_gce.py`
- **Approach**: Direct API calls using `google.cloud.compute_v1` 
- **Pattern**: Inline provisioning within cluster class methods
- **Key files**:
  - `sdcm/cluster_gce.py` - Main cluster implementation with inline provisioning
  - `sdcm/utils/gce_utils.py` - Utility functions for GCE operations
  - `sdcm/provision/gce/kms_provider.py` - Existing KMS provider

### AWS Provisioning Structure (Reference)
- **Location**: `sdcm/provision/aws/`
- **Files**:
  - `provisioner.py` - Main provisioner implementing `InstanceProvisionerBase`
  - `instance_parameters.py` - Instance parameter models
  - `instance_parameters_builder.py` - Builder for instance parameters
  - `utils.py` - AWS-specific utility functions
  - `constants.py` - AWS constants (spot limits, timeouts, etc.)
  - `capacity_reservation.py` - Capacity reservation management
  - `dedicated_host.py` - Dedicated host management
  - `configuration_script.py` - Configuration scripts
- **Pattern**: Uses old-style provisioner interface (`InstanceProvisionerBase`)

### Azure Provisioning Structure (Reference)
- **Location**: `sdcm/provision/azure/`
- **Files**:
  - `provisioner.py` - Main provisioner implementing `Provisioner` abstract class
  - `virtual_machine_provider.py` - VM creation and management
  - `network_interface_provider.py` - Network interface management
  - `ip_provider.py` - IP address management
  - `resource_group_provider.py` - Resource group management
  - `virtual_network_provider.py` - Virtual network management
  - `subnet_provider.py` - Subnet management
  - `network_security_group_provider.py` - Security group management
  - `kms_provider.py` - KMS integration
  - `utils.py` - Azure-specific utilities
- **Pattern**: Uses new-style provisioner interface (`Provisioner` ABC)
- **Integration**: Used with `provision_instances_with_fallback()` in cluster

## Goals

1. **Create GCE Provisioner**: Implement `GceProvisioner` class following the Azure pattern (new-style `Provisioner` interface)
2. **Provider Pattern**: Create modular provider classes for GCE resources (similar to Azure's approach)
3. **Integration**: Integrate with existing `cluster_gce.py` to use the new provisioner
4. **Backward Compatibility**: Ensure existing GCE tests continue to work
5. **Feature Parity**: Support spot instances, multiple regions, and disk configurations

## Implementation Plan

### Phase 1: Core Provisioner Infrastructure

#### 1.1 Create Base Provider Classes
**Location**: `sdcm/provision/gce/`

**New Files to Create**:

1. **`provisioner.py`** - Main GCE provisioner
   - Implement `Provisioner` abstract class interface
   - Methods to implement:
     - `get_or_create_instance()`
     - `get_or_create_instances()`
     - `terminate_instance()`
     - `reboot_instance()`
     - `list_instances()`
     - `cleanup()`
     - `add_instance_tags()`
     - `run_command()`
     - `discover_regions()` (class method)
   - Use composition pattern with provider classes

2. **`instance_provider.py`** - VM instance creation and management
   - `VirtualMachineProvider` class
   - Methods:
     - `get_or_create()` - Create GCE instances
     - `get()` - Get instance by name
     - `list()` - List instances
     - `delete()` - Delete instance
     - `reboot()` - Reboot instance
     - `add_tags()` - Add labels to instance
   - Handle spot vs on-demand instances
   - Support for local SSDs and persistent disks

3. **`network_provider.py`** - Network management
   - `NetworkProvider` class
   - Methods:
     - `get_or_create_network()` - Get or create VPC network
     - `get_or_create_firewall_rules()` - Manage firewall rules
   - Handle network tags for firewall rules

4. **`disk_provider.py`** - Disk management
   - `DiskProvider` class
   - Methods:
     - `create_root_disk_config()` - Boot disk configuration
     - `create_local_ssd_config()` - Local SSD configuration
     - `create_persistent_disk_config()` - Persistent disk configuration
   - Support for different disk types (pd-standard, pd-ssd, local-ssd)

5. **`utils.py`** - GCE-specific utilities
   - Helper functions for:
     - Converting tags to GCE labels format
     - Waiting for operations
     - Zone selection
     - Instance state management
   - Reuse/refactor existing utilities from `sdcm/utils/gce_utils.py`

6. **`constants.py`** - GCE constants
   - Spot instance limits
   - Timeout values
   - Default configurations
   - Instance name constraints

#### 1.2 Update Existing Files

**Files to Modify**:

1. **`sdcm/provision/gce/kms_provider.py`** - Already exists
   - Review and ensure compatibility with new provisioner
   - May need minor updates for integration

2. **`sdcm/provision/__init__.py`** - Register GCE provisioner
   ```python
   from sdcm.provision.gce.provisioner import GceProvisioner
   provisioner_factory.register_provisioner(backend="gce", provisioner_class=GceProvisioner)
   ```

### Phase 2: Cluster Integration

#### 2.1 Refactor cluster_gce.py

**Approach**: Gradual refactoring to use new provisioner

**Changes to `sdcm/cluster_gce.py`**:

1. **Add provisioner initialization**:
   - Create `GceProvisioner` instances for each region
   - Pass to cluster constructor similar to Azure pattern

2. **Refactor `_create_instances()` method**:
   - Replace inline instance creation with provisioner calls
   - Use `InstanceDefinition` for instance specifications
   - Use `provision_instances_with_fallback()` pattern

3. **Update `GCECluster.__init__()`**:
   - Accept `provisioners: List[GceProvisioner]` parameter
   - Store provisioners for later use

4. **Maintain backward compatibility**:
   - Keep existing method signatures
   - Support both old and new initialization paths during transition

#### 2.2 Integration Points

**Files to Update**:

1. **`sdcm/sct_provision/region_definition_builder.py`**
   - Ensure GCE regions are handled correctly
   - May need GCE-specific builder if not already present

2. **Test initialization code** (various test files)
   - Update cluster initialization to create provisioners
   - Follow Azure pattern for consistency

### Phase 3: Testing and Validation

#### 3.1 Unit Tests

**Location**: `unit_tests/provisioner/`

**New Test Files**:

1. **`test_gce_provisioner.py`**
   - Test `GceProvisioner` class methods
   - Mock GCE API calls
   - Test spot/on-demand provisioning
   - Test instance lifecycle operations

2. **`test_gce_instance_provider.py`**
   - Test instance creation
   - Test disk configuration
   - Test network configuration

3. **`fake_gce_service.py`**
   - Mock GCE service for testing
   - Similar to `fake_azure_service.py`

#### 3.2 Integration Tests

**Approach**:
- Run existing GCE tests with new provisioner
- Verify instance creation, deletion, and management
- Test spot instance provisioning
- Test multi-region provisioning

**Test Cases to Validate**:
- Basic cluster provisioning
- Spot instance provisioning with fallback
- Multiple disk types (local SSD, persistent)
- Network configuration
- Tag/label management
- Instance termination and cleanup

### Phase 4: Documentation and Cleanup

#### 4.1 Documentation

**Files to Create/Update**:

1. **`docs/provision_gce.md`** - GCE provisioning guide
   - How to use GCE provisioner
   - Configuration options
   - Examples

2. **`AGENTS.md`** - Update with GCE provisioning info
   - Add to provisioning section
   - Explain GCE-specific patterns

3. **Code documentation**:
   - Add docstrings to all new classes and methods
   - Include usage examples

#### 4.2 Cleanup

1. **Remove/Refactor old code**:
   - Mark deprecated methods in `cluster_gce.py`
   - Gradually remove inline provisioning code
   - Consolidate utility functions

2. **Code review**:
   - Ensure consistent patterns with Azure
   - Follow existing code style
   - Add type hints where missing

## Technical Considerations

### 1. GCE-Specific Features

**Features to Support**:
- **Preemptible/Spot instances**: GCE calls them "preemptible" or "spot"
- **Local SSDs**: Attach NVMe local SSDs to instances
- **Persistent disks**: pd-standard, pd-ssd, pd-balanced
- **Machine types**: Standard, custom, and high-memory types
- **Labels**: GCE uses labels instead of tags (key constraints: lowercase, no special chars)
- **Metadata**: SSH keys, startup scripts, user-data
- **Service accounts**: For GCE API access
- **Network tags**: For firewall rules

### 2. Differences from AWS/Azure

**GCE Specifics**:
1. **Labels vs Tags**: 
   - GCE labels have stricter naming rules (lowercase, max 63 chars)
   - Need normalization function (already exists in cluster_gce.py)

2. **Zones vs Availability Zones**:
   - GCE uses zones (e.g., us-central1-a)
   - Need zone selection logic (already in `gce_utils.py`)

3. **Instance names**:
   - Must be lowercase
   - Max 63 characters
   - Cannot end with hyphen

4. **Disk attachment**:
   - Local SSDs must be attached at instance creation
   - Cannot be attached to running instance

5. **Network configuration**:
   - Network tags for firewall rules
   - Single network per project typically

### 3. Migration Strategy

**Approach**: Phased migration

**Phase 1** (Current PR):
- Create new provisioner infrastructure
- Keep old cluster_gce.py working
- Add tests for new provisioner

**Phase 2** (Follow-up):
- Integrate provisioner with cluster_gce.py
- Support both old and new paths
- Update test initialization

**Phase 3** (Future):
- Remove old provisioning code
- Full migration to new provisioner

## Resource Providers Architecture

### GceProvisioner (Main)
```
GceProvisioner
├── NetworkProvider (manages VPC and firewall)
├── DiskProvider (manages disk configurations)
├── VirtualMachineProvider (manages instances)
└── KmsProvider (already exists, manages encryption keys)
```

### Provider Responsibilities

1. **NetworkProvider**:
   - Get or create network
   - Manage firewall rules
   - Handle network tags

2. **DiskProvider**:
   - Build disk configurations
   - Support multiple disk types
   - Handle disk sizing

3. **VirtualMachineProvider**:
   - Create instances
   - Manage instance state
   - Handle spot/preemptible instances
   - Set labels and metadata

4. **KmsProvider**:
   - Already implemented
   - May need integration updates

## Key APIs and Classes

### GCE API Classes (from google.cloud.compute_v1)
- `InstancesClient` - Instance management
- `DisksClient` - Disk management
- `NetworksClient` - Network management
- `FirewallsClient` - Firewall management

### SCT Interfaces
- `Provisioner` - Abstract base class to implement
- `InstanceDefinition` - Instance specification
- `VmInstance` - Provisioned instance representation
- `PricingModel` - Spot vs on-demand enum

## Dependencies

### Python Packages
- `google-cloud-compute` - Already in use
- `pydantic` - For data models (already used)

### Existing Code to Reuse
- `sdcm/utils/gce_utils.py` - Utility functions
- `sdcm/utils/gce_region.py` - Region management
- `sdcm/provision/helpers/cloud_init.py` - Cloud-init helpers
- `sdcm/keystore.py` - SSH key management

## Success Criteria

1. ✅ `GceProvisioner` implements all `Provisioner` interface methods
2. ✅ Unit tests pass with >80% coverage
3. ✅ Integration tests pass for basic provisioning
4. ✅ Spot instance provisioning works with fallback
5. ✅ Multi-region provisioning works
6. ✅ Disk configuration (local SSD, persistent) works
7. ✅ Labels/tags are properly applied
8. ✅ Instance cleanup works correctly
9. ✅ Documentation is complete
10. ✅ Backward compatibility maintained

## Timeline Estimate

- **Phase 1** (Core Infrastructure): 3-5 days
  - Provisioner class: 1 day
  - Provider classes: 2-3 days
  - Utils and constants: 1 day

- **Phase 2** (Integration): 2-3 days
  - Cluster refactoring: 1-2 days
  - Testing and fixes: 1 day

- **Phase 3** (Testing): 2-3 days
  - Unit tests: 1-2 days
  - Integration tests: 1 day

- **Phase 4** (Documentation): 1-2 days

**Total**: 8-13 days

## Risks and Mitigations

### Risk 1: Breaking existing tests
**Mitigation**: 
- Maintain backward compatibility
- Create new tests before modifying existing code
- Use feature flags if needed

### Risk 2: GCE API differences
**Mitigation**:
- Study existing GCE code carefully
- Test thoroughly with real GCE instances
- Handle GCE-specific errors properly

### Risk 3: Complex state management
**Mitigation**:
- Use provider pattern to isolate concerns
- Add comprehensive error handling
- Log all operations for debugging

## Related PRs and Issues

- **Reference PR**: https://github.com/scylladb/scylla-cluster-tests/pull/4799/files
  - This PR attempted similar work but was not completed
  - Can be used as reference for what was tried before

## Next Steps

After approval of this plan:

1. Create feature branch: `feature/gce-provisioner`
2. Start with Phase 1: Core provisioner infrastructure
3. Implement provider classes one by one
4. Add unit tests as we go
5. Regular commits and progress updates
6. Request review after Phase 1 complete

## Questions for Stakeholders

1. Should we support all disk types (pd-standard, pd-ssd, pd-balanced, local-ssd)?
2. Are there any specific GCE features we must support in v1?
3. Should we maintain full backward compatibility or can we make breaking changes to cluster_gce.py?
4. What's the priority: complete feature parity or basic provisioning first?
5. Should we implement capacity reservation like AWS?

---

**Document Version**: 1.0  
**Last Updated**: 2026-01-05  
**Author**: GitHub Copilot Agent  
**Status**: DRAFT - Awaiting Review
