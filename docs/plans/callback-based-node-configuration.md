# Callback-Based Configuration Plan

## Problem Statement

Currently, the `_add_new_node_in_new_dc()` nemesis operation requires restarting the newly added node to apply custom DC configuration. This happens because:

1. `add_nodes()` creates cloud infrastructure
2. `wait_for_init()` installs Scylla and configures it using `proposed_scylla_yaml`
3. After startup, we modify `scylla.yaml` and `cassandra-rackdc.properties`
4. **Restart required** to apply the changes

### Important Context: Preinstalled Scylla vs Fresh Installation

**This issue only occurs when NOT using preinstalled Scylla images.**

- **Fresh Installation** (e.g., installing from repos): The original bug occurs where `scylla.yaml` doesn't exist until after `wait_for_init()` completes. Accessing it before causes "No such file or directory" errors.

- **SMI-Based Images** (preinstalled Scylla): This issue is NOT problematic because Scylla and its configuration files are already present on the image. The files exist immediately after instance creation, before `wait_for_init()`.

**Testing Requirement**: Both installation methods (fresh install and preinstalled/SMI-based images) must be tested with the callback mechanism to ensure it works correctly in both scenarios.

## Analysis of Other Nemesis Operations

A comprehensive search of the codebase identified all nemesis operations that add nodes:

### Operations That Add Nodes

1. **`_add_new_node_in_new_dc()`** (line 5272) - **AFFECTED** ✗
   - **Issue**: Modifies `scylla.yaml` and `cassandra-rackdc.properties` after `wait_for_init()`
   - **Current fix**: Requires restart to apply DC configuration
   - **Benefit from callback**: Would eliminate restart overhead

2. **`_replace_cluster_node()`** (line 1263) - **NOT AFFECTED** ✓
   - Uses `add_nodes()` → `wait_for_init()` → normal flow
   - No post-initialization config modifications
   - Sets `replacement_host_id` or `replacement_node_ip` BEFORE `wait_for_init()`
   - These properties are used during initialization, not after

3. **`_add_and_init_new_cluster_nodes()`** (line 1324) - **NOT AFFECTED** ✓
   - Standard add nodes flow: `add_nodes()` → `wait_for_init()`
   - No post-initialization config modifications
   - Used by grow/shrink cluster operations

4. **`disrupt_bootstrap_streaming_error()`** (line 5904) - **NOT AFFECTED** ✓
   - Uses `add_nodes()` → monitors bootstrap process
   - No config file modifications
   - Intentionally aborts bootstrap for testing

### Operations That Modify Config Files (Not on New Nodes)

The following operations modify `scylla.yaml` on **existing nodes** (not newly added):

- `disrupt_switch_between_password_authenticator_and_saslauthd_authenticator_and_back()` (line 916)
- `disrupt_toggle_internode_compression()` (line 951, 957)
- `disrupt_toggle_ldap_configuration()` (lines 1206, 1227)
- `disrupt_encryption_at_rest()` (line 4866)

**Conclusion**: These operations are NOT affected by the issue because they:
1. Operate on existing, running nodes (not newly added)
2. Already call `remote_scylla_yaml()` on initialized nodes
3. Explicitly restart nodes after config changes (expected behavior)

### Summary

**Only `_add_new_node_in_new_dc()` is affected by this issue.**

All other node addition operations follow the correct pattern:
- `add_nodes()` → `wait_for_init()` → continue
- No post-initialization config modifications required

The callback mechanism would **only benefit `_add_new_node_in_new_dc()`** but provides a clean architectural pattern that could be leveraged by future nemesis operations if needed.

## Proposed Solution: Configuration Callback Mechanism

### High-Level Design

Introduce a callback mechanism that allows custom configuration to be applied **after installation but before Scylla starts**, eliminating the need for a restart.

### Architecture

```
Current Flow:
add_nodes() → wait_for_init() → [node_setup() → config_setup() → node_startup()] → modify config → restart

Proposed Flow:
add_nodes(config_callback=fn) → wait_for_init() → [node_setup() → config_setup() → **config_callback()** → node_startup()]
```

### Implementation Details

#### 1. Add Callback Parameter to `add_nodes()`

**File**: `sdcm/cluster.py` (BaseCluster)

```python
def add_nodes(
    self,
    count,
    ec2_user_data="",
    dc_idx=0,
    rack=0,
    enable_auto_bootstrap=False,
    instance_type=None,
    config_callback: Optional[Callable[[BaseNode], None]] = None  # NEW
):
    """
    :param config_callback: Optional callback function to configure the node
                           before Scylla starts. Called after config_setup()
                           but before node_startup().
    """
```

#### 2. Store Callback in Node Object

**File**: `sdcm/cluster.py` (BaseNode.__init__)

```python
class BaseNode:
    def __init__(self, ..., config_callback=None):
        # ... existing init code ...
        self.config_callback = config_callback
```

#### 3. Execute Callback in node_setup()

**File**: `sdcm/cluster.py` (BaseScyllaCluster.node_setup)

Insert callback execution after `config_setup()` but before `node_startup()`:

```python
def node_setup(self, node: BaseNode, verbose: bool = False, timeout: int = 3600):
    # ... existing setup code ...

    node.config_setup(append_scylla_args=self.get_scylla_args())

    # NEW: Execute custom configuration callback if provided
    if hasattr(node, 'config_callback') and node.config_callback:
        node.log.info("Executing custom configuration callback")
        try:
            node.config_callback(node)
        except Exception as e:
            node.log.error(f"Configuration callback failed: {e}")
            raise

    self._scylla_post_install(node, install_scylla, nic_devname)
    # ... rest of setup code ...
```

#### 4. Alternative: Hook in wait_for_init_wrap

For better separation of concerns, we could also inject the callback at the orchestration level:

**File**: `sdcm/cluster.py` (wait_for_init_wrap)

```python
def wait_for_init_wrap(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        # ... existing code ...

        # NEW: Extract config_callback from kwargs
        config_callback = kwargs.pop("config_callback", None)

        # Pass to node_setup
        setup_kwargs = {
            k: v for k, v in kwargs.items()
            if k not in ["node_list", "check_node_health", "wait_for_db_logs"]
        }
        if config_callback:
            setup_kwargs["config_callback"] = config_callback
```

### Usage in Nemesis

**File**: `sdcm/nemesis.py` (_add_new_node_in_new_dc)

```python
def _add_new_node_in_new_dc(self, is_zero_node=False) -> BaseNode:
    # Define custom configuration callback
    def configure_new_dc(node: BaseNode):
        """Configure node for new datacenter before Scylla starts"""
        with node.remote_scylla_yaml() as scylla_yml:
            scylla_yml.rpc_address = node.ip_address
            scylla_yml.seed_provider = [
                SeedProvider(
                    class_name="org.apache.cassandra.locator.SimpleSeedProvider",
                    parameters=[{"seeds": self.tester.db_cluster.seed_nodes_addresses}],
                )
            ]

        endpoint_snitch = self.cluster.params.get("endpoint_snitch") or ""
        if endpoint_snitch.endswith("GossipingPropertyFileSnitch"):
            rackdc_value = {"dc": "add_remove_nemesis_dc"}
        else:
            rackdc_value = {"dc_suffix": "_nemesis_dc"}

        with node.remote_cassandra_rackdc_properties() as properties_file:
            properties_file.update(**rackdc_value)

    add_node_func_args = {
        "count": 1,
        "dc_idx": 0,
        "enable_auto_bootstrap": True,
        "disruption_name": self.current_disruption,
        "config_callback": configure_new_dc,  # NEW
        **({"is_zero_node": is_zero_node} if is_zero_node else {}),
    }

    new_node = skip_on_capacity_issues(db_cluster=self.tester.db_cluster)(
        self.cluster.add_nodes
    )(**add_node_func_args)[0]

    # Wait for initialization (callback executes inside)
    self.cluster.wait_for_init(node_list=[new_node], timeout=900, check_node_health=False)

    # NO RESTART NEEDED!
    new_node.wait_node_fully_start()
    self.monitoring_set.reconfigure_scylla_monitoring()
    return new_node
```

## Benefits

1. **Performance**: Eliminates unnecessary node restart (saves ~30-60 seconds)
2. **Cleaner**: Configuration happens at the right place in the lifecycle
3. **Reusable**: Other nemesis operations or tests can use the same mechanism
4. **Safer**: Reduces potential race conditions from restart timing

## Edge Cases & Considerations

### Preinstalled Scylla vs Fresh Installation

The callback mechanism must handle both installation scenarios:

**Fresh Installation** (installing from repos):
```python
def node_setup(self, node: BaseNode, ...):
    install_scylla = True
    if self.params.get("use_preinstalled_scylla") and node.is_scylla_installed(...):
        install_scylla = False

    if install_scylla:
        self._scylla_install(node)  # Installs Scylla, creates config files
    else:
        self._wait_for_preinstalled_scylla(node)  # Config files already exist

    # Execute callback regardless of installation method
    # At this point, scylla.yaml exists in BOTH cases
    if hasattr(node, 'config_callback') and node.config_callback:
        node.config_callback(node)
```

**Key Points**:
- **Fresh install**: Config files created during `_scylla_install()`, callback executes after
- **Preinstalled/SMI**: Config files already exist, callback can modify them immediately
- **Testing**: Both scenarios must be tested to ensure callback works correctly

The callback executes whether Scylla was just installed or was preinstalled.

### Callback Failure Handling

Exceptions from the callback must propagate up to the nemesis code to ensure clear failure messages and proper error handling.

**Exception Propagation Flow**:
```
callback raises exception
  → node_setup() fails and propagates exception
  → wait_for_init() catches and re-raises with context
  → nemesis code receives clear error message
```

**Implementation Requirements**:

1. **No exception catching in callback execution**: Let exceptions bubble up naturally
   ```python
   # In node_setup()
   if hasattr(node, 'config_callback') and node.config_callback:
       node.log.info("Executing custom configuration callback")
       # NO try/except here - let exceptions propagate
       node.config_callback(node)
   ```

2. **wait_for_init() propagates failures**: The existing `wait_for_init_wrap` decorator already handles this:
   - Catches exceptions from `node_setup()`
   - Stores exception details with traceback
   - Re-raises or reports failures to nemesis code
   - Ensures nemesis receives clear error messages

3. **Alternative: Explicit failure checking**: If needed, add a method to check callback success:
   ```python
   # Option: Check callback status after wait_for_init
   try:
       self.cluster.wait_for_init(node_list=[new_node], ...)
   except NodeSetupFailed as e:
       # Exception message includes callback failure details
       self.log.error(f"Node setup failed, likely due to config callback: {e}")
       raise
   ```

**Key Points**:
- **No silent failures**: Callback exceptions MUST propagate to nemesis
- **Clear error messages**: Exception messages should indicate callback failure
- **Existing infrastructure**: `wait_for_init_wrap` already handles exception propagation
- **Logging**: Add debug logging before callback execution for troubleshooting
- **No rollback needed**: If callback fails, node_setup fails, node won't start

### Backward Compatibility

- **Optional parameter**: `config_callback` is optional, existing code continues to work
- **No breaking changes**: All existing tests/nemesis operations unaffected

## Test Plan

### Unit Tests

**Location**: `unit_tests/test_node_config_callback.py`

**Test Cases**:
1. **Callback execution**: Verify callback is executed during node_setup
2. **Optional callback**: Verify node_setup works without callback (backward compatibility)
3. **Error handling**: Verify callback exceptions propagate and fail node_setup
4. **Correct parameters**: Verify callback receives the correct node object
5. **Execution order**: Verify callback executes after config_setup but before node_startup
6. **Fresh install**: Test callback with nodes installed from repos
7. **Preinstalled Scylla**: Test callback with SMI-based/preinstalled images

### Integration Tests

**Location**: `unit_tests/test_nemesis_with_callback.py`

**Test Cases**:
1. **No restart required**: Verify new node in DC doesn't require restart with callback
2. **Config modification**: Verify callback successfully modifies scylla.yaml
3. **Properties modification**: Verify callback successfully modifies cassandra-rackdc.properties
4. **Nemesis flow**: Verify complete nemesis operation with callback mechanism
5. **Fresh install scenario**: Test with `use_preinstalled_scylla=False`
6. **Preinstalled scenario**: Test with `use_preinstalled_scylla=True` (SMI-based images)

### Functional Tests

**Location**: `functional_tests/test_callback_dc_config.py`

**Test Cases**:
1. **End-to-end flow**: Add a node with custom DC configuration using callback
   - Verify node starts successfully
   - Verify configuration is applied correctly
   - Verify no restart is needed
   - Verify node joins cluster in correct DC
2. **Preinstalled Scylla**: Test callback works with preinstalled Scylla nodes (SMI-based images)
3. **Fresh installation**: Test callback works with nodes installed from repos
4. **Multi-node**: Test adding multiple nodes simultaneously with callbacks
3. **Properties modification**: Verify callback successfully modifies cassandra-rackdc.properties
4. **Nemesis flow**: Verify complete nemesis operation with callback mechanism

### Functional Tests

**Location**: `functional_tests/test_callback_dc_config.py`

**Test Cases**:
1. **End-to-end flow**: Add a node with custom DC configuration using callback
   - Verify node starts successfully
   - Verify configuration is applied correctly
   - Verify no restart is needed
   - Verify node joins cluster in correct DC
2. **Preinstalled Scylla**: Test callback works with preinstalled Scylla nodes
3. **Multi-node**: Test adding multiple nodes simultaneously with callbacks

### Performance Tests

**Location**: `performance_tests/test_callback_performance.py`

**Test Cases**:
1. **Time comparison**: Compare node addition time with and without restart
   - Expected: Callback approach saves 30-60 seconds per node
2. **Resource usage**: Monitor CPU/memory during callback execution vs restart
3. **Scalability**: Test callback performance with multiple simultaneous node additions

## Migration Plan

### Phase 1: Implementation
1. Add callback parameter to `add_nodes()` signature
2. Store callback in node object during initialization
3. Execute callback in `node_setup()` after `config_setup()`
4. Add comprehensive unit tests

### Phase 2: Nemesis Updates
1. Update `MultiDcAddRemoveNemesis._add_new_node_in_new_dc()` to use callback
2. Remove restart logic
3. Test with MultiDC nemesis tests

### Phase 3: Documentation & Rollout
1. Update developer documentation
2. Add usage examples
3. Consider migrating other nemesis operations that modify config post-init

## Alternative Approaches Considered

### 1. Override proposed_scylla_yaml

**Approach**: Modify `node.proposed_scylla_yaml` before `wait_for_init()`

**Pros**:
- No new mechanism needed
- Uses existing property system

**Cons**:
- `proposed_scylla_yaml` is a computed property, not directly settable
- Requires complex changes to the property computation logic
- Less flexible for runtime configuration

### 2. Pre-configure via add_nodes parameters

**Approach**: Pass DC config as parameters to `add_nodes()`

**Pros**:
- Simple parameter passing
- No callback complexity

**Cons**:
- Doesn't scale to complex configurations
- Each new config option requires new parameter
- Less flexible than callback

### 3. Post-init hook in wait_for_init

**Approach**: Add a hook parameter to `wait_for_init()` instead of `add_nodes()`

**Pros**:
- Clearer separation of concerns

**Cons**:
- Callback is farther from node creation
- Requires passing callback through multiple layers

## Conclusion

The callback-based approach provides:
- ✅ **No restart required** - saves time and complexity
- ✅ **Clean architecture** - configuration happens at right lifecycle point
- ✅ **Backward compatible** - optional callback doesn't break existing code
- ✅ **Reusable** - other operations can leverage the same mechanism
- ✅ **Testable** - clear unit and integration test strategy

**Recommended**: Implement Phase 1 first with comprehensive tests, then migrate nemesis operations in Phase 2.
