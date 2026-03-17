---
status: draft
domain: k8s
created: 2026-01-24
last_updated: 2026-03-15
owner: fruch
---

# K8s Multitenancy Dict-based Configuration Plan

## Problem Statement

Currently, multitenancy configuration options with `k8s_multitenancy_supported=True` only accept lists to specify per-tenant values. This requires users to maintain order alignment between the list index and tenant number, which is error-prone and lacks clarity about which tenant gets which value.

### Current Behavior

```yaml
# Current: List-based configuration (index-dependent)
nemesis_interval: [5, 7]           # tenant-1 gets 5, tenant-2 gets 7
nemesis_class_name: [
    "FakeNemesisClassNameForTenant1",
    "FakeNemesisClassNameForTenant2"
]
```

### Problems with Current Approach
1. **Index-dependent mapping**: Users must maintain strict order alignment
2. **No explicit tenant identification**: Values are implicitly assigned by position
3. **Error-prone for large tenant counts**: Easy to misalign values with wrong tenant
4. **Limited readability**: Cannot tell which tenant gets which value at a glance

## Goals

1. **Support dict-based configuration** with explicit tenant keys (e.g., `{"tenant1": value1, "tenant2": value2}`)
2. **Maintain backward compatibility** with existing list-based format
3. **Improve configuration readability** by making tenant-value associations explicit
4. **Validate dict keys** against tenant count/naming conventions

## Proposed Solution

### New Configuration Format

```yaml
# New: Dict-based configuration (explicit tenant mapping)
nemesis_interval:
  tenant1: 5
  tenant2: 7

nemesis_class_name:
  tenant1: "FakeNemesisClassNameForTenant1"
  tenant2: "FakeNemesisClassNameForTenant2"

# Also support for complex nested values
stress_cmd:
  tenant1:
    - "stress_cmd_part1"
    - "stress_cmd_part2"
  tenant2:
    - "stress_cmd_single"
```

### Implementation Steps

#### Step 1: Create Generic Multitenancy Type

**File**: `sdcm/sct_config.py` (around lines 176-240)

Create a single generic type `MultitenantValue[T]` that wraps any existing type to add dict support. The presence of this type will automatically indicate multitenancy support, eliminating the need for `k8s_multitenancy_supported=True`.

```python
from typing import TypeVar, Generic, get_origin, get_args

T = TypeVar('T')

class MultitenantValueMarker:
    """Marker class to identify MultitenantValue types at runtime."""
    pass

def multitenancy_value_validator(value: T | dict[str, T]) -> T | dict[str, T]:
    """
    Validator that passes through dict values for multitenancy support.
    The inner type validation is handled by the wrapped type's own validator.
    """
    if isinstance(value, dict):
        # Dict values are passed through as-is for multitenancy
        # Individual value validation happens in _validate_value
        return value
    # Non-dict values are returned as-is for the inner type's validator
    return value

# Generic type that adds dict support to any existing type
# Usage: MultitenantValue[IntOrList], MultitenantValue[StringOrList], etc.
def MultitenantValue(inner_type):
    """
    Type wrapper that:
    1. Adds dict[str, T] support to any type T
    2. Automatically marks the field as k8s_multitenancy_supported
    """
    return Annotated[
        inner_type | dict[str, inner_type],
        BeforeValidator(multitenancy_value_validator),
        MultitenantValueMarker(),  # Marker for runtime detection
    ]
```

**Auto-detection in `sct_field`**: Modify `sct_field()` to detect `MultitenantValue` types and automatically set `k8s_multitenancy_supported`:

```python
def sct_field(*args, **kwargs):
    kwargs.setdefault("default", None)
    assert "env" in kwargs
    extra = {k: v for k, v in kwargs.items() if k in ("env", "appendable")}
    # Note: k8s_multitenancy_supported is no longer needed here - detected from type
    kwargs.setdefault("json_schema_extra", extra)
    for key in extra:
        kwargs.pop(key, None)
    return Field(*args, **kwargs)

def is_multitenant_field(field: Field) -> bool:
    """Check if a field uses MultitenantValue type by looking for the marker."""
    if hasattr(field, 'metadata'):
        for meta in field.metadata:
            if isinstance(meta, MultitenantValueMarker):
                return True
    # Also check annotation for MultitenantValueMarker
    if hasattr(field, 'annotation'):
        origin = get_origin(field.annotation)
        if origin is Annotated:
            for arg in get_args(field.annotation):
                if isinstance(arg, MultitenantValueMarker):
                    return True
    return False
```

This approach:
- **No duplication**: Single generic wrapper for all types
- **Composable**: Works with existing `IntOrList`, `StringOrList`, `BooleanOrList` types
- **Type-safe**: Preserves inner type validation
- **Self-documenting**: Using `MultitenantValue[T]` automatically enables multitenancy support
- **Removes boilerplate**: No need for `k8s_multitenancy_supported=True` flag

#### Step 2: Update Field Type Annotations

**File**: `sdcm/sct_config.py`

Update all 19 fields with `k8s_multitenancy_supported=True` to wrap existing types with `MultitenantValue`:

| Field Name | Current Type | New Type |
|------------|--------------|----------|
| `nemesis_class_name` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `nemesis_interval` | `IntOrList` | `MultitenantValue[IntOrList]` |
| `nemesis_sequence_sleep_between_ops` | `IntOrList` | `MultitenantValue[IntOrList]` |
| `nemesis_during_prepare` | `BooleanOrList` | `MultitenantValue[BooleanOrList]` |
| `nemesis_seed` | `IntOrList` | `MultitenantValue[IntOrList]` |
| `nemesis_add_node_cnt` | `IntOrList` | `MultitenantValue[IntOrList]` |
| `space_node_threshold` | `IntOrList` | `MultitenantValue[IntOrList]` |
| `nemesis_filter_seeds` | `BooleanOrList` | `MultitenantValue[BooleanOrList]` |
| `stress_cmd` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `round_robin` | `BooleanOrList` | `MultitenantValue[BooleanOrList]` |
| `stress_cmd_w` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `stress_cmd_r` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `stress_cmd_m` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `stress_cmd_read_disk` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `stress_cmd_cache_warmup` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `prepare_write_cmd` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `nemesis_selector` | `StringOrList` | `MultitenantValue[StringOrList]` |
| `nemesis_exclude_disabled` | `BooleanOrList` | `MultitenantValue[BooleanOrList]` |
| `nemesis_multiply_factor` | `IntOrList` | `MultitenantValue[IntOrList]` |

**Example field definition:**

```python
# Before: Required explicit flag
nemesis_interval: IntOrList = sct_field(
    description="""Nemesis sleep interval to use if None provided specifically in the test""",
    env="SCT_NEMESIS_INTERVAL",
    k8s_multitenancy_supported=True,  # <-- No longer needed
)

# After: Type automatically indicates multitenancy support
nemesis_interval: MultitenantValue[IntOrList] = sct_field(
    description="""Nemesis sleep interval to use if None provided specifically in the test""",
    env="SCT_NEMESIS_INTERVAL",
    # k8s_multitenancy_supported is auto-detected from MultitenantValue type
)
```

#### Step 3: Remove `is_k8s_multitenant_value` and Simplify Validation

**File**: `sdcm/sct_config.py` (method `_validate_value`, lines ~3270-3301)

**Remove** the dynamic `is_k8s_multitenant_value` flag entirely. Instead, use `is_multitenant_field()` to detect multitenancy support from the type annotation.

**Changes:**
1. Remove `field.json_schema_extra["is_k8s_multitenant_value"] = False` initialization
2. Remove `field.json_schema_extra["is_k8s_multitenant_value"] = True` assignment
3. Remove `k8s_multitenancy_supported` from `sct_field()` extra keys
4. Use `is_multitenant_field(field)` to detect multitenancy support

```python
def _validate_value(self, field_name: str, field: Field):
    # ...existing validator extraction code...

    param_value = self.get(field_name)

    # Handle dict-based multitenancy - validate each value
    if (
        self.get("cluster_backend").startswith("k8s")
        and self.get("k8s_tenants_num") > 1
        and is_multitenant_field(field)  # <-- Use type-based detection
        and isinstance(param_value, dict)
        and len(param_value) > 1
    ):
        for tenant_key, tenant_value in param_value.items():
            try:
                from_env_func(tenant_value)
            except Exception as ex:
                raise ValueError(f"failed to validate {field_name}[{tenant_key}]") from ex
        return

    # Handle list-based multitenancy - validate each value
    if (
        self.get("cluster_backend").startswith("k8s")
        and self.get("k8s_tenants_num") > 1
        and is_multitenant_field(field)  # <-- Use type-based detection
        and isinstance(param_value, list)
        and len(param_value) > 1
    ):
        for list_element in param_value:
            try:
                from_env_func(list_element)
            except Exception as ex:
                raise ValueError(f"failed to validate {field_name}") from ex
        return

    # Regular single-value validation
    from_env_func(param_value)
```

**Rationale**: The `is_k8s_multitenant_value` flag and `k8s_multitenancy_supported` are no longer needed. Instead, we detect multitenancy support directly from the `MultitenantValue[T]` type annotation using `is_multitenant_field()`.

#### Step 4: Update Tenant Parameter Processing

**File**: `sdcm/utils/k8s_operator/multitenant_common.py` (function `get_tenants`, lines ~106-133)

Modify the parameter processing loop to:
1. Use `is_multitenant_field()` helper to detect multitenancy support from type
2. Check value type (dict or list) at runtime
3. Handle both list and dict formats

```python
from sdcm.sct_config import is_multitenant_field

# Process multitenant parameters if present
for field_name, field in test_class_instance.params.model_fields.items():
    param_name = field_name
    param_value = test_class_instance.params.get(param_name)

    # Check if field supports multitenancy via MultitenantValue type
    if not is_multitenant_field(field):
        continue

    # Handle dict-based configuration
    if isinstance(param_value, dict) and len(param_value) > 1:
        LOGGER.debug("Process multitenant dict option '%s'. Value: %s", param_name, param_value)
        for i, tenant in enumerate(tenants):
            tenant_key = f"tenant{i + 1}"  # or derive from tenant object
            if tenant_key in param_value:
                current_param_value = param_value[tenant_key]
                # Apply stress_cmd single-element list unwrapping if needed
                if (param_name.startswith("stress_")
                    and isinstance(current_param_value, list)
                    and len(current_param_value) == 1):
                    current_param_value = current_param_value[0]
                tenant.params[param_name] = current_param_value
                tenant.db_cluster.params[param_name] = current_param_value

    # Handle list-based configuration
    elif isinstance(param_value, list) and len(param_value) > 1:
        LOGGER.debug("Process multitenant list option '%s'. Value: %s", param_name, param_value)
        for i, tenant in enumerate(tenants):
            current_param_value = param_value[i]
            # Apply stress_cmd single-element list unwrapping if needed
            if (param_name.startswith("stress_")
                and isinstance(current_param_value, list)
                and len(current_param_value) == 1):
                current_param_value = current_param_value[0]
            tenant.params[param_name] = current_param_value
            tenant.db_cluster.params[param_name] = current_param_value
```

#### Step 5: Add Tests

**File**: `unit_tests/test_utils__operator__multitenant_common.py`

Add new test methods:

- `test_multitenant_longevity_class_with_dict_options()`
- `test_multitenant_performance_class_with_dict_options()`

**New test data file**: `unit_tests/test_data/test_config/multitenant/dict_option_values.yaml`

```yaml
test_duration: 300
user_prefix: 'longevity-scylla-operator-3h-multitenant'
n_db_nodes: 4
scylla_version: "5.0.2"
k8s_tenants_num: 2

# Dict-based stress options
stress_cmd:
  tenant1:
    - "fake__stress_cmd__tenant1__part1"
    - "fake__stress_cmd__tenant1__part2"
  tenant2:
    - "fake__stress_cmd__tenant2__single"

# Dict-based nemesis options
nemesis_class_name:
  tenant1: "FakeNemesisClassNameForTenant1"
  tenant2: "FakeNemesisClassNameForTenant2"

nemesis_interval:
  tenant1: 5
  tenant2: 7

nemesis_during_prepare:
  tenant1: false
  tenant2: true
```

## Design Considerations

### 1. Tenant Key Naming Convention

**Recommendation**: Use `tenant{N}` convention (e.g., `tenant1`, `tenant2`)

This aligns with `k8s_tenants_num` configuration and `current_cluster_index` in the code.

### 2. Backward Compatibility

Both formats will be supported:
- **List format**: `[value1, value2]` - mapped by index
- **Dict format**: `{tenant1: value1, tenant2: value2}` - mapped by key

### 3. Validation Rules

- Dict must have entries matching `k8s_tenants_num`
- Missing tenant keys should raise validation error
- Extra tenant keys should raise validation error

### 4. Mixed Format Support

**Not recommended**: Mixing list and dict formats in the same config file should be allowed (each field independently), but not within a single field value.

## Files to Modify

| File | Changes |
|------|---------|
| `sdcm/sct_config.py` | Add `MultitenantValue` type with `MultitenantValueMarker`, add `is_multitenant_field()` helper, update field annotations, remove `is_k8s_multitenant_value` and `k8s_multitenancy_supported` flags |
| `sdcm/utils/k8s_operator/multitenant_common.py` | Update `get_tenants` to use `is_multitenant_field()` for detection, handle dict values |
| `unit_tests/test_utils__operator__multitenant_common.py` | Add dict-based configuration tests |
| `unit_tests/test_data/test_config/multitenant/dict_option_values.yaml` | New test data file |

## Testing Strategy

1. **Unit tests**: Verify dict parsing and tenant assignment
2. **Validation tests**: Ensure proper error messages for invalid dict keys
3. **Backward compatibility tests**: Confirm list-based configs still work
4. **Integration tests**: End-to-end multitenant test with dict configuration

## Further Considerations

1. **Tenant naming convention:** Should tenant keys be `tenant1`, `tenant2`, etc. (aligned with `k8s_tenants_num`)? Or allow arbitrary string keys?
2. **Validation strictness:** Should dict keys be validated to exactly match tenant count (raise error on missing/extra keys)?
3. **Documentation update:** Update `docs/configuration_options.md` with the new dict format examples.
