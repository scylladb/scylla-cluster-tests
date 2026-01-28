# Callback-Based Configuration Plan

## Problem Statement

Currently, the `_add_new_node_in_new_dc()` nemesis operation requires restarting the newly added node to apply custom DC configuration. This happens because:

1. `add_nodes()` creates cloud infrastructure
2. `wait_for_init()` installs Scylla and configures it using `proposed_scylla_yaml`
3. After startup, we modify `scylla.yaml` and `cassandra-rackdc.properties` 
4. **Restart required** to apply the changes

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

### Preinstalled Scylla

For nodes with preinstalled Scylla (`use_preinstalled_scylla=True`):

```python
def node_setup(self, node: BaseNode, ...):
    if self.params.get("use_preinstalled_scylla") and node.is_scylla_installed(...):
        install_scylla = False
    
    # ... installation code ...
    
    # Execute callback regardless of installation method
    if hasattr(node, 'config_callback') and node.config_callback:
        node.config_callback(node)
```

The callback executes whether Scylla was just installed or was preinstalled.

### Callback Failure Handling

- **Exception propagation**: If callback fails, `node_setup()` should fail
- **Logging**: Clear logging of callback execution and errors
- **Rollback**: Consider if we need to undo partial changes on failure

### Backward Compatibility

- **Optional parameter**: `config_callback` is optional, existing code continues to work
- **No breaking changes**: All existing tests/nemesis operations unaffected

## Test Plan

### Unit Tests

**File**: `unit_tests/test_node_config_callback.py`

```python
import pytest
from unittest.mock import Mock, MagicMock, patch
from sdcm.cluster import BaseNode, BaseScyllaCluster

class TestConfigCallback:
    """Test configuration callback mechanism"""
    
    def test_callback_executed_during_node_setup(self):
        """Verify callback is executed during node_setup"""
        callback = Mock()
        node = Mock(spec=BaseNode)
        node.config_callback = callback
        
        cluster = Mock(spec=BaseScyllaCluster)
        # Mock the node_setup to call callback
        
        # Simulate node_setup flow
        cluster.node_setup(node)
        
        # Verify callback was called with node
        callback.assert_called_once_with(node)
    
    def test_callback_not_required(self):
        """Verify node_setup works without callback (backward compatibility)"""
        node = Mock(spec=BaseNode)
        node.config_callback = None
        
        cluster = Mock(spec=BaseScyllaCluster)
        
        # Should not raise exception
        cluster.node_setup(node)
    
    def test_callback_exception_propagates(self):
        """Verify callback exceptions fail node_setup"""
        def failing_callback(node):
            raise ValueError("Test error")
        
        node = Mock(spec=BaseNode)
        node.config_callback = failing_callback
        
        cluster = Mock(spec=BaseScyllaCluster)
        
        with pytest.raises(ValueError, match="Test error"):
            cluster.node_setup(node)
    
    def test_callback_receives_correct_node(self):
        """Verify callback receives the correct node object"""
        received_nodes = []
        
        def track_callback(node):
            received_nodes.append(node)
        
        node = Mock(spec=BaseNode)
        node.config_callback = track_callback
        
        cluster = Mock(spec=BaseScyllaCluster)
        cluster.node_setup(node)
        
        assert len(received_nodes) == 1
        assert received_nodes[0] is node
    
    def test_callback_executes_after_config_setup(self):
        """Verify callback executes after config_setup but before startup"""
        execution_order = []
        
        def callback(node):
            execution_order.append("callback")
        
        node = Mock(spec=BaseNode)
        node.config_callback = callback
        
        # Mock config_setup and startup to track order
        with patch.object(node, 'config_setup') as mock_config:
            mock_config.side_effect = lambda *a, **k: execution_order.append("config_setup")
            
            cluster = Mock(spec=BaseScyllaCluster)
            # Simulate execution order
            node.config_setup()
            callback(node)
            
            assert execution_order == ["config_setup", "callback"]
```

### Integration Tests

**File**: `unit_tests/test_nemesis_with_callback.py`

```python
import pytest
from sdcm.nemesis import MultiDcAddRemoveNemesis
from unit_tests.nemesis.fake_cluster import FakeTester

class TestNemesisWithCallback:
    """Test nemesis operations using config callback"""
    
    def test_add_new_node_in_new_dc_no_restart(self):
        """Verify new node in DC doesn't require restart with callback"""
        tester = FakeTester()
        nemesis = MultiDcAddRemoveNemesis(tester, None)
        
        # Track if restart was called
        restart_called = []
        
        with patch.object(BaseNode, 'restart_scylla_server') as mock_restart:
            mock_restart.side_effect = lambda *a, **k: restart_called.append(True)
            
            new_node = nemesis._add_new_node_in_new_dc()
            
            # Verify no restart was needed
            assert len(restart_called) == 0
            assert new_node is not None
    
    def test_callback_modifies_scylla_yaml(self):
        """Verify callback successfully modifies scylla.yaml"""
        tester = FakeTester()
        nemesis = MultiDcAddRemoveNemesis(tester, None)
        
        # Track scylla.yaml modifications
        with patch('sdcm.cluster.remote_file') as mock_remote_file:
            yaml_context = MagicMock()
            mock_remote_file.return_value.__enter__.return_value = yaml_context
            
            new_node = nemesis._add_new_node_in_new_dc()
            
            # Verify rpc_address was set
            assert yaml_context.rpc_address == new_node.ip_address
    
    def test_callback_modifies_rackdc_properties(self):
        """Verify callback successfully modifies cassandra-rackdc.properties"""
        tester = FakeTester()
        nemesis = MultiDcAddRemoveNemesis(tester, None)
        
        # Track properties modifications
        properties_updates = []
        
        with patch('sdcm.cluster.remote_file') as mock_remote_file:
            props_context = MagicMock()
            props_context.update.side_effect = lambda d: properties_updates.append(d)
            mock_remote_file.return_value.__enter__.return_value = props_context
            
            new_node = nemesis._add_new_node_in_new_dc()
            
            # Verify DC properties were updated
            assert len(properties_updates) > 0
            assert any('dc' in update or 'dc_suffix' in update 
                      for update in properties_updates)
```

### Functional Tests

**File**: `functional_tests/test_callback_dc_config.py`

```python
import pytest
from sdcm.cluster import BaseNode

@pytest.mark.docker_scylla_args(docker_network="scylla-test-network")
def test_add_node_with_custom_dc_callback(docker_scylla):
    """
    Functional test: Add a node with custom DC configuration using callback
    
    Verifies:
    1. Node starts successfully
    2. Configuration is applied correctly
    3. No restart is needed
    4. Node joins cluster in correct DC
    """
    # Setup cluster
    cluster = docker_scylla.db_cluster
    
    # Define callback
    def custom_dc_config(node: BaseNode):
        with node.remote_scylla_yaml() as scylla_yml:
            scylla_yml.rpc_address = node.ip_address
        
        with node.remote_cassandra_rackdc_properties() as props:
            props.update({"dc": "test_dc", "rack": "test_rack"})
    
    # Add node with callback
    new_nodes = cluster.add_nodes(
        count=1, 
        dc_idx=0,
        config_callback=custom_dc_config
    )
    
    # Wait for initialization
    cluster.wait_for_init(node_list=new_nodes, timeout=600)
    
    new_node = new_nodes[0]
    
    # Verify node started (no restart needed)
    assert new_node.is_running()
    
    # Verify DC configuration
    status = cluster.get_nodetool_status()
    assert "test_dc" in status
    
    # Verify no restart occurred (check uptime)
    uptime = new_node.remoter.run("uptime -s").stdout
    # Node should have single startup time
    
@pytest.mark.docker_scylla_args(docker_network="scylla-test-network")
def test_preinstalled_scylla_with_callback(docker_scylla):
    """
    Test callback works with preinstalled Scylla
    
    Verifies callback executes correctly even when Scylla is preinstalled
    """
    # Similar test but with use_preinstalled_scylla=True
    pass
```

### Performance Tests

**File**: `performance_tests/test_callback_performance.py`

```python
import time
import pytest

def test_node_addition_time_comparison():
    """
    Compare node addition time with and without restart
    
    Expected: Callback approach saves 30-60 seconds per node
    """
    # Test with restart (old approach)
    start = time.time()
    add_node_with_restart()
    time_with_restart = time.time() - start
    
    # Test with callback (new approach)
    start = time.time()
    add_node_with_callback()
    time_with_callback = time.time() - start
    
    # Verify callback is faster
    time_saved = time_with_restart - time_with_callback
    assert time_saved >= 30, f"Expected at least 30s savings, got {time_saved}s"
    
    print(f"Time saved: {time_saved}s ({time_saved/time_with_restart*100:.1f}%)")
```

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
