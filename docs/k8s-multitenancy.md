# K8s Multitenancy Configuration

This document describes how to configure per-tenant values for k8s multitenant tests in SCT.

## Overview

When running k8s multitenant tests (with `k8s_tenants_num > 1`), certain configuration options support per-tenant values. This allows you to specify different configurations for each tenant in your test.

Configuration options that support multitenancy are marked in the [configuration options documentation](configuration_options.md) with:
> *(supports k8s multitenancy)*

## Configuration Formats

### Shared Value (All Tenants)

When you want all tenants to use the same value, simply provide a single value:

```yaml
k8s_tenants_num: 3

# All tenants will use the same stress command
stress_cmd: "cassandra-stress write cl=QUORUM duration=30m -rate threads=100"

# All tenants will use the same nemesis
nemesis_class_name: "SisyphusMonkey"
nemesis_interval: 5
```

### Per-Tenant Values (Dict Format) - Recommended

Use the dict format with `tenant1`, `tenant2`, etc. keys to specify different values for each tenant:

```yaml
k8s_tenants_num: 3

# Different stress commands per tenant
stress_cmd:
  tenant1: "cassandra-stress write cl=QUORUM duration=30m -rate threads=50"
  tenant2: "cassandra-stress write cl=QUORUM duration=30m -rate threads=100"
  tenant3: "cassandra-stress write cl=QUORUM duration=30m -rate threads=150"

# Different nemesis configurations per tenant
nemesis_class_name:
  tenant1: "SisyphusMonkey"
  tenant2: "ChaosMonkey"
  tenant3: "NoOpMonkey"

nemesis_interval:
  tenant1: 5
  tenant2: 10
  tenant3: 15
```

### Per-Tenant Values with Lists

When a tenant needs multiple commands (e.g., multiple stress commands), use a list as the value:

```yaml
k8s_tenants_num: 2

stress_cmd:
  tenant1:
    - "cassandra-stress write cl=QUORUM n=1000000 -rate threads=50"
    - "cassandra-stress write cl=QUORUM n=1000000 -rate threads=100"
  tenant2:
    - "cassandra-stress read cl=QUORUM duration=30m -rate threads=50"
```

### Mixed Configuration

You can mix shared and per-tenant configurations in the same test:

```yaml
k8s_tenants_num: 2

# Shared across all tenants
prepare_write_cmd: "cassandra-stress write cl=ALL n=1000000 -rate threads=10"

# Per-tenant stress commands
stress_cmd:
  tenant1: "cassandra-stress write cl=QUORUM duration=30m -rate threads=100"
  tenant2: "cassandra-stress read cl=QUORUM duration=30m -rate threads=100"

# Per-tenant nemesis
nemesis_class_name:
  tenant1: "SisyphusMonkey"
  tenant2: "NoOpMonkey"

# Shared nemesis interval
nemesis_interval: 5
```


## Notes

1. **Tenant keys must match**: The tenant keys (`tenant1`, `tenant2`, etc.) must be sequential starting from 1 and match the `k8s_tenants_num` value.

2. **All tenants must be specified**: When using dict format, you must provide values for all tenants (tenant1 through tenantN where N = k8s_tenants_num).

3. **Type consistency**: Each tenant's value should match the expected type for that configuration option.
