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

#### 1.3 Definition of Done - Phase 1

**Code Completion Criteria**:
- [ ] All provider classes implemented (`provisioner.py`, `instance_provider.py`, `network_provider.py`, `disk_provider.py`)
- [ ] All utility functions and constants defined
- [ ] GCE provisioner registered with factory
- [ ] All methods in `Provisioner` interface implemented
- [ ] Code passes linting (ruff, autopep8)
- [ ] Type hints added to all new functions and classes

**Unit Test Requirements**:
- [ ] `test_gce_provisioner.py` created with >80% coverage
- [ ] `test_gce_instance_provider.py` created
- [ ] Mock GCE service (`fake_gce_service.py`) implemented
- [ ] All provider methods have unit tests
- [ ] Tests pass with `pytest unit_tests/provisioner/test_gce_*.py`

**Manual Testing - Phase 1**:

1. **Import Verification**:
   ```python
   # Test that provisioner can be imported
   from sdcm.provision.gce.provisioner import GceProvisioner
   from sdcm.provision import provisioner_factory
   # Verify factory registration
   assert 'gce' in provisioner_factory._classes
   ```

2. **Provisioner Instantiation**:
   ```python
   # Test creating provisioner instance
   provisioner = GceProvisioner(
       test_id="test-123",
       region="us-east1",
       availability_zone="b"
   )
   # Verify attributes are set correctly
   assert provisioner.region == "us-east1"
   assert provisioner.availability_zone == "b"
   ```

3. **Provider Classes Initialization**:
   ```python
   # Verify all internal providers are created
   assert provisioner._vm_provider is not None
   assert provisioner._network_provider is not None
   assert provisioner._disk_provider is not None
   ```

**Success Criteria**:
- All code complete and committed
- All unit tests passing
- No linting errors
- Manual import and instantiation tests successful
- Code review completed
- Documentation strings added to all public methods

### Phase 2: Cluster Integration

#### 2.1 Integration with cluster_gce.py

**Approach**: Add new provisioner support WITHOUT breaking existing code

**Changes to `sdcm/cluster_gce.py`**:

1. **Add optional provisioner initialization**:
   - Create `GceProvisioner` instances for each region (optional parameter)
   - Pass to cluster constructor similar to Azure pattern
   - **Default to None** to maintain backward compatibility

2. **Add alternative `_create_instances()` path**:
   - If provisioner is provided, use new provisioner calls
   - If provisioner is None, use existing inline instance creation
   - Both paths must work and be tested

3. **Update `GCECluster.__init__()`**:
   - Accept **optional** `provisioners: List[GceProvisioner] = None` parameter
   - Store provisioners if provided
   - **Keep all existing parameters and defaults unchanged**

4. **Strict backward compatibility**:
   - All existing method signatures unchanged
   - All existing initialization paths work without modification
   - No deprecation warnings
   - Existing tests pass without changes

#### 2.2 Integration Points

**Files to Update**:

1. **`sdcm/sct_provision/region_definition_builder.py`**
   - Ensure GCE regions are handled correctly
   - May need GCE-specific builder if not already present

2. **Test initialization code** (various test files)
   - Update cluster initialization to create provisioners
   - Follow Azure pattern for consistency

#### 2.3 Definition of Done - Phase 2

**Code Completion Criteria**:
- [ ] `cluster_gce.py` updated with optional provisioner support
- [ ] Both old and new code paths work simultaneously
- [ ] No breaking changes to existing API
- [ ] Region definition builder handles GCE
- [ ] Code passes linting

**Unit Test Requirements**:
- [ ] Tests for provisioner-enabled cluster creation
- [ ] Tests for backward compatibility (old path still works)
- [ ] Tests verifying both paths produce same results
- [ ] All existing GCE cluster tests still pass

**Manual Testing - Phase 2**:

1. **Test Old Path (Backward Compatibility)**:
   ```python
   # Verify existing cluster creation still works
   from sdcm.cluster_gce import ScyllaGCECluster

   cluster = ScyllaGCECluster(
       gce_image="scylla-image",
       gce_image_type="pd-ssd",
       gce_image_size=50,
       gce_network="default",
       gce_service=(gce_service, info),
       credentials=credentials,
       # NO provisioners parameter - use old path
       n_nodes=1
   )
   # Should create cluster using old inline provisioning
   nodes = cluster.add_nodes(1)
   assert len(nodes) == 1
   assert nodes[0]._instance is not None
   ```

2. **Test New Path (With Provisioner)**:
   ```python
   # Test cluster creation with new provisioner
   from sdcm.provision.gce.provisioner import GceProvisioner

   provisioner = GceProvisioner(
       test_id="test-123",
       region="us-east1",
       availability_zone="b"
   )

   cluster = ScyllaGCECluster(
       gce_image="scylla-image",
       # ... other params ...
       provisioners=[provisioner],  # Use new path
       n_nodes=1
   )
   nodes = cluster.add_nodes(1)
   assert len(nodes) == 1
   ```

3. **Test Feature Parity**:
   ```bash
   # Run existing GCE test to verify no regression
   pytest unit_tests/test_cluster_gce.py -v

   # Verify spot instances work with provisioner
   # Verify multiple disk types work
   # Verify network tags applied correctly
   # Verify labels/metadata set properly
   ```

4. **Integration Test - Real GCE**:
   ```bash
   # Create actual GCE cluster (small, short-lived)
   hydra run-test longevity_test.LongevityTest.test_custom_time \
     --backend gce \
     --config test-cases/gce-simple-test.yaml \
     n_db_nodes=1 \
     test_duration=10

   # Verify:
   # - Cluster provisions successfully
   # - Instance has correct disk configuration
   # - Network and firewall rules work
   # - Can SSH to nodes
   # - Scylla starts successfully
   ```

**Success Criteria**:
- All existing tests pass without modification
- New provisioner path works correctly
- Both paths tested and verified
- No breaking changes detected
- Integration test on real GCE successful
- Cluster cleanup works properly

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

#### 3.3 Definition of Done - Phase 3

**Code Completion Criteria**:
- [ ] All unit tests written and passing
- [ ] Integration tests written and passing
- [ ] Test coverage >80% for new code
- [ ] All test files properly documented

**Unit Test Requirements**:
- [ ] `test_gce_provisioner.py` - Complete coverage of GceProvisioner
- [ ] `test_gce_instance_provider.py` - VM operations
- [ ] `test_gce_network_provider.py` - Network operations
- [ ] `test_gce_disk_provider.py` - Disk configurations
- [ ] `fake_gce_service.py` - Mock service for all tests
- [ ] Tests run in CI/CD pipeline

**Manual Testing - Phase 3**:

1. **Comprehensive Feature Testing**:
   ```bash
   # Test 1: Spot instance with fallback
   # Create spot instance, simulate preemption, verify fallback to on-demand
   hydra run-test longevity_test.LongevityTest.test_custom_time \
     --backend gce \
     instance_provision='spot' \
     instance_provision_fallback_on_demand=true \
     n_db_nodes=1 \
     test_duration=15
   ```

2. **All Disk Types Test**:
   ```bash
   # Test 2: Verify all disk types work
   # Config with pd-standard root, pd-ssd data, local-ssd
   hydra run-test longevity_test.LongevityTest.test_custom_time \
     --backend gce \
     gce_image_type='pd-ssd' \
     gce_n_local_ssd=2 \
     n_db_nodes=1 \
     test_duration=15
   ```

3. **Multi-Region Test**:
   ```bash
   # Test 3: Multiple regions/zones
   hydra run-test longevity_test.LongevityTest.test_custom_time \
     --backend gce \
     gce_datacenter='us-east1 us-west1' \
     n_db_nodes=3 \
     test_duration=20
   ```

4. **Network and Labels Test**:
   ```python
   # Test 4: Verify network configuration and labels
   # Create cluster, verify:
   # - Firewall rules applied correctly
   # - Network tags present
   # - Labels/metadata set properly
   # - SSH access works
   provisioner = GceProvisioner(test_id="test-net", region="us-east1", availability_zone="b")
   instance_def = InstanceDefinition(
       name="test-instance",
       image_id="scylla-image",
       type="n2-standard-2",
       user_name="ubuntu",
       ssh_key=ssh_key,
       tags={"TestId": "test-net", "NodeType": "scylla-db"}
   )
   instance = provisioner.get_or_create_instance(instance_def, PricingModel.ON_DEMAND)

   # Verify labels
   assert instance.tags["TestId"] == "test-net"

   # Verify can SSH
   result = instance.run_command("echo 'test'")
   assert result.ok

   # Cleanup
   provisioner.cleanup()
   ```

5. **Stress Test - Resource Cleanup**:
   ```python
   # Test 5: Create and destroy multiple times
   # Verify no resource leaks
   for i in range(5):
       provisioner = GceProvisioner(test_id=f"test-{i}", region="us-east1", availability_zone="b")
       instances = provisioner.get_or_create_instances([...], PricingModel.ON_DEMAND)
       # Use instances
       time.sleep(30)
       # Cleanup
       provisioner.cleanup(wait=True)
       # Verify all resources deleted (VMs, disks, IPs if applicable)
   ```

6. **Existing Test Suite**:
   ```bash
   # Test 6: Run full existing GCE test suite
   pytest unit_tests/test_cluster_gce.py -v
   pytest unit_tests/test_gce_*.py -v

   # All should pass without modification
   ```

**Success Criteria**:
- All unit tests passing (>80% coverage)
- All integration tests passing
- Manual tests 1-6 completed successfully
- No resource leaks detected
- Existing test suite passes without changes
- Performance acceptable (provisioning time similar to old method)
- Error handling verified (network errors, quota limits, etc.)

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

1. **Maintain backward compatibility**:
   - **DO NOT** remove or deprecate existing methods in `cluster_gce.py`
   - Keep all current functionality working
   - New provisioner should coexist with existing code
   - Tests using old API must continue to pass

2. **Code review**:
   - Ensure consistent patterns with Azure
   - Follow existing code style
   - Add type hints where missing

#### 4.3 Definition of Done - Phase 4

**Code Completion Criteria**:
- [ ] All documentation files created/updated
- [ ] Code review completed and feedback addressed
- [ ] All docstrings complete
- [ ] Type hints added everywhere
- [ ] No TODO comments remaining

**Documentation Requirements**:
- [ ] `docs/provision_gce.md` created with comprehensive guide
- [ ] `AGENTS.md` updated with GCE provisioning section
- [ ] All classes have docstrings with examples
- [ ] All public methods documented
- [ ] Configuration options documented in comments
- [ ] README updated if needed

**Manual Testing - Phase 4**:

1. **Documentation Verification**:
   ```bash
   # Test 1: Follow documentation to use provisioner
   # A new developer should be able to:
   # - Read docs/provision_gce.md
   # - Create a simple test using GceProvisioner
   # - Understand all configuration options
   # - Successfully provision and cleanup
   ```

2. **Code Quality Check**:
   ```bash
   # Test 2: Run all quality tools
   ruff check sdcm/provision/gce/
   ruff format --check sdcm/provision/gce/
   mypy sdcm/provision/gce/  # if type checking enabled

   # All should pass with no errors
   ```

3. **Final Integration Test**:
   ```bash
   # Test 3: End-to-end realistic test
   # Run a full longevity test using new provisioner
   hydra run-test longevity_test.LongevityTest.test_custom_time \
     --backend gce \
     --config test-cases/longevity/longevity-gce-custom-d1-workload1-hybrid-raid.yaml \
     use_gce_provisioner=true \
     test_duration=60

   # Verify:
   # - All features work (spot, disks, network)
   # - Performance is acceptable
   # - Cleanup is complete
   # - Logs are clean (no warnings about provisioner)
   ```

4. **Backward Compatibility Final Check**:
   ```bash
   # Test 4: Ensure old tests still work
   # Run existing GCE tests without any changes
   hydra run-test artifacts_test.ArtifactsTest.test_gce_image \
     --backend gce

   # Should complete successfully using old path
   ```

5. **Documentation Review**:
   ```markdown
   # Test 5: Review checklist
   - [ ] All code examples in docs are runnable
   - [ ] Configuration options match actual code
   - [ ] Architecture diagrams accurate (if any)
   - [ ] Links to related code are correct
   - [ ] Examples cover common use cases
   - [ ] Troubleshooting section added
   ```

**Success Criteria**:
- All documentation complete and reviewed
- Code quality tools pass
- Final integration tests successful
- Backward compatibility verified
- No outstanding issues or TODOs
- PR ready for final review and merge

**Pre-Merge Checklist**:
- [ ] All 4 phases completed
- [ ] All DoD criteria met for each phase
- [ ] All manual tests documented and passed
- [ ] Code review approved
- [ ] CI/CD pipeline passing
- [ ] Documentation complete
- [ ] No breaking changes
- [ ] Feature complete (all cluster_gce.py features supported)
- [ ] Performance validated
- [ ] Security review completed (if applicable)

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

**Approach**: Single-phase implementation with backward compatibility

**Implementation Strategy**:
- Create new provisioner infrastructure in parallel to existing code
- Keep ALL existing cluster_gce.py functionality working unchanged
- New provisioner available as alternative API (not replacement)
- Both old and new paths must work and be tested
- No removal of existing code - coexistence model
- Feature complete implementation before merge

**Backward Compatibility Requirements**:
1. All existing tests must pass without modification
2. No changes to existing cluster_gce.py public API
3. Existing initialization paths remain functional
4. New provisioner is additive, not replacing existing code

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
6. ✅ **All disk types work** (pd-standard, pd-ssd, pd-balanced, local-ssd)
7. ✅ Labels/tags are properly applied
8. ✅ Instance cleanup works correctly
9. ✅ Documentation is complete
10. ✅ **Full backward compatibility maintained - no breaking changes**
11. ✅ **All features from cluster_gce.py are supported**
12. ✅ **Feature complete before merge** - all functionality working

## Follow-Up Work (Separate Issues)

The following features are deferred to future work and will be tracked in separate issues:

1. **Capacity Reservation** - Similar to AWS implementation
   - Create issue to track this work
   - Not blocking for initial GCE provisioner merge
   - Can be added in future enhancement

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

Plan has been approved with the following requirements:
- ✅ Support all disk types (pd-standard, pd-ssd, pd-balanced, local-ssd)
- ✅ Support all features currently in cluster_gce.py
- ✅ No breaking changes in any phase
- ✅ Implementation must be feature complete before merge

**Ready to Begin Implementation:**

1. Create feature branch: `feature/gce-provisioner` ✓
2. Start with Phase 1: Core provisioner infrastructure
3. Implement provider classes one by one
4. Add unit tests as we go
5. Regular commits and progress updates
6. Complete all phases before requesting merge
7. Create separate issue for capacity reservation (follow-up work)

## Requirements (Based on Stakeholder Feedback)

1. **Disk Types**: Support all disk types (pd-standard, pd-ssd, pd-balanced, local-ssd) ✓
2. **GCE Features**: All features currently supported in `cluster_gce.py` must be supported ✓
3. **Backward Compatibility**: No breaking changes in any phase - full backward compatibility required ✓
4. **Implementation Priority**: Start with basic provisioning, but PR should not be merged until feature complete ✓
5. **Capacity Reservation**: Deferred to follow-up work (separate issue to be created) ✓

---

**Document Version**: 1.1
**Last Updated**: 2026-01-05
**Author**: GitHub Copilot Agent
**Status**: APPROVED - Ready for Implementation
