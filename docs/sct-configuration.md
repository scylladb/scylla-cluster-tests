# SCT Configuration System

This document explains the SCT (Scylla Cluster Tests) configuration system, how it works, and how to add new configuration options.

## Overview

SCT uses a **Pydantic-based configuration system** that provides:
- **Type safety**: All configuration options are strongly typed
- **Automatic validation**: Pydantic validates types and values
- **Self-documenting**: Type hints and descriptions make the config clear
- **Flexible input formats**: Support for multiple input types (strings, lists, dicts)
- **Multiple sources**: Merge configurations from YAML files, environment variables, and defaults

## Configuration Architecture

### Configuration Class

The `SCTConfiguration` class in `sdcm/sct_config.py` is a Pydantic `BaseModel` that defines all available configuration options:

```python
from pydantic import BaseModel
from sdcm.sct_config import SctField, StringOrList, IntOrList

class SCTConfiguration(BaseModel):
    test_duration: int = SctField(
        description="Test duration in minutes",
    )

    n_db_nodes: IntOrList = SctField(
        description="Number of database nodes in each DC",
    )

    stress_cmd: StringOrList = SctField(
        description="Stress command(s) to run",
    )
```

### Configuration Sources (Priority Order)

SCT merges configurations from multiple sources in this order (later sources override earlier ones):

1. **Default values** - Defined in `defaults/` directory
   - `test_default.yaml` - Common defaults
   - `aws_config.yaml`, `gce_config.yaml`, etc. - Backend-specific defaults

2. **Test case YAML files** - Specified via `--config` or `SCT_CONFIG_FILES`
   - Example: `test-cases/longevity/longevity-10gb-3h.yaml`
   - Multiple files can be specified and will be merged

3. **Environment variables** - Prefixed with `SCT_`
   - Example: `SCT_SCYLLA_VERSION=5.4.0`
   - Format: `SCT_<FIELD_NAME_UPPERCASE>`

4. **Command line arguments** - When using hydra
   - Example: `--backend aws`

## Type System

SCT uses specialized types that provide automatic conversion and validation:

### Core Types

#### StringOrList
Accepts either a string or list of strings, with automatic conversion:

```yaml
# Single string (becomes list with one element in multitenant contexts)
stress_cmd: "cassandra-stress write n=1000000"

# List of strings
stress_cmd:
  - "cassandra-stress write n=1000000"
  - "cassandra-stress read n=1000000"

# Evaluable expression (starting with !)
stress_cmd: "!get_stress_cmd()"
```

**Use cases**: Commands, file paths, package lists

#### IntOrList
Accepts integer or list of integers, with space-separated string parsing:

```yaml
# Single integer
n_db_nodes: 3

# Space-separated string (for multi-DC)
n_db_nodes: "3 3"  # → [3, 3]

# List of integers
n_db_nodes: [3, 3, 0]
```

**Use cases**: Node counts, timeouts, thresholds

#### BooleanOrList
Accepts boolean or list of booleans, with string conversion:

```yaml
# Single boolean
nemesis_during_prepare: true

# Space-separated string
nemesis_during_prepare: "true false"  # → [true, false]

# String values (yes/no/1/0)
nemesis_during_prepare: "yes"  # → true
```

**Use cases**: Feature flags, per-tenant boolean settings

#### Enum and Literal (Choices)
For configuration options with a fixed set of allowed values, use `Literal`:

```python
from typing import Literal

class SCTConfiguration(BaseModel):
    ip_ssh_connections: Literal["public", "private", "ipv6"] = SctField(
        description="Type of IP to use for SSH connections",
    )

    instance_provision: Literal["spot", "on_demand", "spot_fleet", "spot_low_price"] = SctField(
        description="AWS instance provisioning method",
    )
```

```yaml
# In YAML configuration
ip_ssh_connections: "public"
instance_provision: "spot"
```

**Benefits**:
- Type-safe: Only valid choices are accepted
- Auto-completion in IDEs
- Clear error messages for invalid values
- Self-documenting: choices are visible in the type definition

**Use cases**: Backend selection, provisioning types, connection modes, log transport types

#### Sub-Pydantic Models
For nested configuration structures, use Pydantic BaseModel subclasses:

```python
class SCTConfiguration(BaseModel):
    # DictOrStrOrPydantic allows dict, string, or Pydantic BaseModel instances
    append_scylla_yaml: DictOrStrOrPydantic = SctField(
        description="Additional scylla.yaml configuration",
    )
```

```yaml
# As a dictionary
append_scylla_yaml:
  experimental_features:
    - udf
    - alternator-streams

# Or reference a Python expression
append_scylla_yaml: "!get_scylla_yaml_config()"
```

**Benefits**:
- Nested validation: Each sub-model validates its own fields
- Reusable structures: Define once, use in multiple places
- Type safety for complex configurations
- Supports both dict and Pydantic instances for flexibility

**Use cases**: YAML configurations, nested settings, complex data structures

### Multitenant Support

#### MultitenantValue[T]
Wraps any type to add dictionary-based multitenant configuration support.

**Old format** (list-based, ambiguous):
```yaml
# Hard to tell: is this multiple commands or multiple tenants?
stress_cmd: [["cmd1 part1", "cmd1 part2"], ["cmd2 part1", "cmd2 part2"]]
```

**New format** (dict-based, explicit):
```yaml
# Clear: each tenant gets specific commands
stress_cmd:
  tenant1: ["cmd1 part1", "cmd1 part2"]
  tenant2: ["cmd2 part1", "cmd2 part2"]
```

Fields using `MultitenantValue`:
- **Nemesis**: `nemesis_class_name`, `nemesis_selector`, `nemesis_interval`, `nemesis_seed`, etc.
- **Stress commands**: `stress_cmd`, `stress_cmd_w`, `stress_cmd_r`, `stress_cmd_m`, `prepare_write_cmd`, etc.
- **Other**: `space_node_threshold`, `round_robin`

See the full list in the [PR description](../README.md).

## Adding New Configuration Options

### Step 1: Add Field to SCTConfiguration

Edit `sdcm/sct_config.py` and add your field to the `SCTConfiguration` class:

```python
class SCTConfiguration(BaseModel):
    # ... existing fields ...

    my_new_option: str = SctField(
        description="Description of what this option does",
    )
```

### Step 2: Choose the Appropriate Type

Select the type based on your needs:

| Type | Use When |
|------|----------|
| `str` | Single string value |
| `int` | Single integer value |
| `bool` | Single boolean value |
| `StringOrList` | String or list of strings |
| `IntOrList` | Integer or list of integers |
| `BooleanOrList` | Boolean or list of booleans |
| `Literal["choice1", "choice2", ...]` | Fixed set of allowed string values (choices/enum) |
| `dict` | Dictionary/mapping |
| `DictOrStrOrPydantic` | Dict, string, or Pydantic BaseModel for nested configs |
| `MultitenantValue[T]` | Type T but with multitenant support |

### Step 3: Add Default Value

**MANDATORY**: All configuration options must have a default value defined in the YAML files in `defaults/`. Do not define defaults in the code - they must be in the configuration files for visibility and maintainability.

Add the default value to the appropriate file in `defaults/`:

```yaml
# In defaults/test_default.yaml
my_new_option: "default_value"
```

Or for backend-specific defaults:
```yaml
# In defaults/aws_config.yaml
my_new_option: "aws_specific_value"
```

**Important**: Even if a field can be `None`, explicitly set it to `null` in the defaults file rather than relying on code-level defaults.

### Step 4: Update Documentation

The documentation in `docs/configuration_options.md` is **auto-generated** from the Pydantic model.

To regenerate the documentation, run:

```bash
# Via pre-commit (runs all checks including doc generation)
uv run sct.py pre-commit

# Or directly generate only the configuration documentation
uv run python3 -c "from sdcm.sct_config import SCTConfiguration; SCTConfiguration.dump_help_config_markdown()"
```

This will update `docs/configuration_options.md` with your new option.

### Step 5: Add Validation (Optional)

If you need custom validation, add a Pydantic validator:

```python
from pydantic import field_validator

class SCTConfiguration(BaseModel):
    my_new_option: int = SctField(
        description="Must be between 1 and 100",
    )

    @field_validator('my_new_option')
    @classmethod
    def validate_my_new_option(cls, value):
        if value and not (1 <= value <= 100):
            raise ValueError("my_new_option must be between 1 and 100")
        return value
```

### Complete Example

```python
# In sdcm/sct_config.py
from typing import Literal

class SCTConfiguration(BaseModel):
    # ... existing fields ...

    custom_timeout: IntOrList = SctField(
        description="""
            Timeout in seconds for custom operation.
            Can be a single value or list for multi-DC.
        """,
    )

    enable_custom_feature: bool = SctField(
        description="Enable the custom feature",
    )

    custom_mode: Literal["fast", "balanced", "thorough"] = SctField(
        description="Operating mode for custom feature",
    )

    custom_commands: MultitenantValue(StringOrList) = SctField(
        description="Custom commands to run (supports multitenancy)",
    )

    @field_validator('custom_timeout')
    @classmethod
    def validate_custom_timeout(cls, value):
        if isinstance(value, list):
            for v in value:
                if v < 0:
                    raise ValueError("Timeout cannot be negative")
        elif value and value < 0:
            raise ValueError("Timeout cannot be negative")
        return value
```

```yaml
# In defaults/test_default.yaml
custom_timeout: 300
enable_custom_feature: false
custom_mode: "balanced"
custom_commands: "echo 'default command'"
```
# In defaults/test_default.yaml
custom_timeout: 300
enable_custom_feature: false
custom_commands: "echo 'default command'"
```

## Usage Examples

### Basic Configuration

```bash
# Set via environment variable
export SCT_SCYLLA_VERSION=5.4.0
export SCT_N_DB_NODES=3

# Run test with config file
hydra run-test longevity_test.LongevityTest.test_custom_time \
  --backend aws \
  --config test-cases/longevity/longevity-10gb-3h.yaml
```

### Multiple Configuration Files

Configuration files are merged left-to-right (later files override earlier ones):

```bash
# Base config + overrides + network config
hydra run-test longevity_test.LongevityTest.test_custom_time \
  --backend aws \
  --config test-cases/longevity/longevity-10gb-3h.yaml \
  --config my-overrides.yaml \
  --config configurations/network_config/test_communication_public.yaml
```

### Multi-DC Configuration

```yaml
# Use space-separated strings for multi-DC
n_db_nodes: "3 3 0"  # 3 nodes in DC1, 3 in DC2, 0 in DC3
n_loaders: "2 2"     # 2 loaders in each DC
```

### Multitenant Configuration

```yaml
# Dict-based multitenant config
stress_cmd:
  tenant1: "cassandra-stress write n=1000000"
  tenant2: "cassandra-stress write n=5000000"

nemesis_interval:
  tenant1: 5
  tenant2: 10
```

## Accessing Configuration in Tests

```python
class MyTest(ClusterTester):
    def test_example(self):
        # Access via attribute (recommended)
        duration = self.params.test_duration

        # Access via get() method (returns None if not set)
        version = self.params.get('scylla_version')

        # WARNING: config.get() with default parameter does NOT work like dict.get()!
        # Do NOT use: timeout = self.params.get('custom_timeout', 300)
        # This creates hidden defaults in code instead of explicit YAML defaults.
        # Instead, define the default in defaults/test_default.yaml and access directly:
        timeout = self.params.custom_timeout  # Will use YAML default

        # Check if value exists (truthiness check)
        if self.params.get('enable_custom_feature'):
            self.run_custom_feature()
```

## Configuration Validation

The configuration is validated automatically when loaded:

```python
# This happens automatically during test initialization
params = SCTConfiguration()
params.verify_configuration()  # Validates all fields
```

Validation errors will be raised with clear messages:

```
ValidationError: 1 validation error for SCTConfiguration
n_db_nodes
  Value error, n_db_nodes must be greater than 0 [type=value_error, ...]
```

## Directory Structure

```
scylla-cluster-tests/
├── defaults/                    # Default configuration values
│   ├── test_default.yaml       # Common defaults for all tests
│   ├── aws_config.yaml         # AWS-specific defaults
│   ├── gce_config.yaml         # GCE-specific defaults
│   ├── azure_config.yaml       # Azure-specific defaults
│   └── docker_config.yaml      # Docker-specific defaults
├── test-cases/                  # Test case configurations
│   ├── longevity/
│   ├── performance/
│   └── ...
├── configurations/              # Reusable configuration fragments
│   ├── network_config/         # Network configurations
│   ├── nemesis/                # Nemesis configurations
│   └── ...
└── sdcm/
    └── sct_config.py           # Main configuration class
```

## Best Practices

1. **Use appropriate types**: Choose `StringOrList`, `IntOrList`, or `BooleanOrList` when you need flexibility
2. **Add validation**: Use Pydantic validators for complex validation logic
3. **Document thoroughly**: Add clear descriptions to all fields
4. **Always set defaults in YAML**: All configuration options MUST have defaults defined in `defaults/test_default.yaml` or backend-specific files - never in code
5. **Test your changes**: Run `uv run sct.py unit-tests` to ensure your changes work
6. **Update docs**: Run pre-commit to regenerate `docs/configuration_options.md`

## Migration from Old System

The Pydantic-based system replaces the old dict-based configuration:

**Old way** (dict-based):
```python
config_options = [
    dict(name="test_duration", env="SCT_TEST_DURATION", type=int, help="..."),
]

# Access
duration = config['test_duration']
```

**New way** (Pydantic-based):
```python
class SCTConfiguration(BaseModel):
    test_duration: int = SctField(description="...")

# Access (both work)
duration = config.test_duration      # Attribute access
duration = config.get('test_duration')  # Dict-style access (backward compatible)
```

## Troubleshooting

### Issue: Configuration not loading
**Solution**: Check that your YAML file is valid and the field name matches exactly (case-sensitive)

### Issue: Type validation errors
**Solution**: Ensure the value type matches the field type. Use quotes for strings, no quotes for numbers/booleans

### Issue: Environment variable not working
**Solution**: Ensure it's prefixed with `SCT_` and the name is in uppercase (e.g., `SCT_TEST_DURATION`)

### Issue: Multitenant config not working
**Solution**: Use dict format with `tenant1`, `tenant2` keys, or ensure the field has `MultitenantValue` type

## See Also

- [Configuration Options Reference](configuration_options.md) - Auto-generated list of all options
- [AGENTS.md](../AGENTS.md) - Development guidelines including config best practices
