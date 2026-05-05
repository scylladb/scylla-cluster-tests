# Cross-Cloud Instance Sizing

SCT uses constraint-based instance selection to automatically resolve hardware requirements to cloud-specific instance types. Instead of memorizing instance type names across providers, you describe what you need and the system picks the best match.

## Quick Start

### In a test config YAML

```yaml
# Specify hardware requirements — resolved per cloud at runtime
db_instance_type:
  vcpu: 8
  memory: ">16gb"

loader_instance_type:
  vcpu: 4

monitor_instance_type:
  vcpu: 2
```

When you run this config with `--backend aws`, it resolves to:
- **db**: `i8g.2xlarge` (ARM, 8 vCPU, 64 GB, NVMe)
- **loader**: `c6i.xlarge` (4 vCPU, 8 GB)
- **monitor**: `t3.large` (2 vCPU, 8 GB)

The same config with `--backend gce` resolves to:
- **db**: `z3-highmem-8` (8 vCPU, 64 GB, local SSD)
- **loader**: `e2-standard-4` (4 vCPU, 16 GB)
- **monitor**: `n2-highmem-4` (4 vCPU, 32 GB)

### Via environment variable

```bash
# Flat string format for env vars
export SCT_DB_INSTANCE_TYPE='vcpu=16,memory=>32gb'
export SCT_LOADER_INSTANCE_TYPE='vcpu=4'
export SCT_MONITOR_INSTANCE_TYPE='vcpu=2'
```

### Literal instance type (backward compatible)

If you already know the exact instance type you want, use it directly — no resolution happens:

```yaml
db_instance_type: 'i4i.2xlarge'
```

## Constraint Syntax

### YAML dict format (in config files)

```yaml
db_instance_type:
  vcpu: 8              # exact match
  memory: ">16gb"      # minimum 16 GB
  disk: "500gb-2tb"    # range: 500 GB to 2 TB local disk
  arch: "arm64"        # optional: force architecture
```

### Flat string format (for env vars)

```bash
SCT_DB_INSTANCE_TYPE='vcpu=8,memory=>16gb,disk=500gb-2tb,arch=arm64'
```

Both formats are equivalent and produce identical results.

## Constraint Reference

| Field | Description | Operators | Units | Default if omitted |
|-------|-------------|-----------|-------|--------------------|
| `vcpu` | Number of virtual CPUs | exact, `>`, `<`, `>=`, range | plain int | No filter |
| `memory` | RAM size | exact, `>`, `<`, `>=`, range | `gb`, `tb` | No filter |
| `disk` | Local disk size (per disk) | exact, `>`, `<`, `>=`, range | `gb`, `tb` | db role: must have local disk |
| `arch` | CPU architecture | exact only | `arm64`, `x86_64` | aws=`arm64`, others=`x86_64` |

### Operator examples

| Syntax | Meaning |
|--------|---------|
| `vcpu: 16` | Exactly 16 vCPUs |
| `memory: ">32gb"` | More than 32 GB |
| `memory: ">=32gb"` | 32 GB or more |
| `memory: "<64gb"` | Less than 64 GB |
| `disk: "500gb-2tb"` | Between 500 GB and 2 TB |

## Selection Algorithm

When you specify constraints, the system:

1. **Filters by cloud** — only considers instances for the active backend
2. **Filters by architecture** — uses `arch` constraint if specified, otherwise cloud default (aws=arm64, others=x86_64)
3. **Filters by preferred family** — each role has preferred instance families per cloud (see below)
4. **Applies constraints** — removes instances that don't satisfy vcpu/memory/disk requirements
5. **Sorts by**: family preference order → price (cheapest first) → vCPU count (ascending)
6. **Returns first match** — deterministic, always the same result for the same input

### Preferred Families

| Role | AWS | GCE | Azure | OCI |
|------|-----|-----|-------|-----|
| db | i8g (ARM), i4i (x86 fallback) | z3-highmem | Standard_L*s_v4 | DenseIO.E5.Flex |
| loader | c6i | e2-standard, e2-highcpu | Standard_F*s_v2 | VM.Standard3.Flex |
| monitor | t3, m6i | n2-highmem | Standard_D*_v4 | VM.Standard.E4.Flex |

### Implicit constraints

- **db role**: Automatically requires `local_disk_count > 0` (NVMe/local SSD) unless explicitly overridden
- **AWS db**: Defaults to ARM (`i8g`) architecture; use `arch: "x86_64"` to get `i4i` family

## Examples

### Performance test — large DB nodes

```yaml
db_instance_type:
  vcpu: 16
  memory: ">64gb"

loader_instance_type:
  vcpu: 16

monitor_instance_type:
  vcpu: 4
```

Resolves on AWS to: `i8g.4xlarge` / `c6i.4xlarge` / `m6i.xlarge`

### Force x86 architecture on AWS

```yaml
db_instance_type:
  vcpu: 8
  arch: "x86_64"
```

Resolves to `i4i.2xlarge` instead of the default `i8g.2xlarge`.

### Minimal CI test

```yaml
db_instance_type:
  vcpu: 2

loader_instance_type:
  vcpu: 2

monitor_instance_type:
  vcpu: 2
```

### Multi-role config (full example)

```yaml
# test-cases/longevity/longevity-100gb-4h.yaml
db_instance_type:
  vcpu: 8
  memory: ">32gb"

loader_instance_type:
  vcpu: 8

monitor_instance_type:
  vcpu: 2

n_db_nodes: 4
n_loaders: 2
n_monitor_nodes: 1
```

### Environment variable override

```bash
# Override just the DB instance for a quick test with bigger nodes
export SCT_DB_INSTANCE_TYPE='vcpu=16,memory=>64gb'
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws --config test-cases/longevity/longevity-100gb-4h.yaml
```

## Error Handling

If no instance matches your constraints, you get a clear error:

```
NoMatchingInstanceError: No instance found for role='db', cloud='aws' satisfying:
  - vcpu: 1024  (no instance has >= 1024 vCPUs)
  Candidates considered: i8g family (max 64 vCPUs)
  Suggestion: reduce vcpu constraint or check available families
```

## CLI Tools

### show-prices

Display available instances with pricing for a role:

```bash
$ uv run sct.py show-prices --cloud aws --role db

AWS DB Instances (preferred family: i8g)
┌──────────────────┬──────┬───────────┬──────────┬──────────┬─────────────┐
│ Instance Type    │ vCPU │ Memory GB │ Disk GB  │ Arch     │ $/hour      │
├──────────────────┼──────┼───────────┼──────────┼──────────┼─────────────┤
│ i8g.large        │    2 │        16 │      468 │ arm64    │ $0.15       │
│ i8g.xlarge       │    4 │        32 │      937 │ arm64    │ $0.31       │
│ i8g.2xlarge      │    8 │        64 │    1,875 │ arm64    │ $0.62       │
│ i8g.4xlarge      │   16 │       128 │    3,750 │ arm64    │ $1.24       │
│ i8g.8xlarge      │   32 │       256 │    7,500 │ arm64    │ $2.48       │
│ i8g.16xlarge     │   64 │       512 │   15,000 │ arm64    │ $4.96       │
└──────────────────┴──────┴───────────┴──────────┴──────────┴─────────────┘
```

### translate-size

Show how constraints resolve per cloud:

```bash
$ uv run sct.py translate-size --config test-cases/longevity/longevity-100gb-4h.yaml

Constraint: db_instance_type = {vcpu: 8, memory: ">32gb"}

  AWS:   i8g.2xlarge    (8 vCPU, 64 GB, 1875 GB NVMe, arm64, $0.62/hr)
  GCE:   z3-highmem-8   (8 vCPU, 64 GB, 2x375 GB SSD, x86_64)
  Azure: Standard_L8s_v4 (8 vCPU, 64 GB, 1x1788 GB NVMe, x86_64)
  OCI:   DenseIO.E5.Flex:4:64 (4 oCPU/8 vCPU, 64 GB, local NVMe)

Constraint: loader_instance_type = {vcpu: 8}

  AWS:   c6i.2xlarge    (8 vCPU, 16 GB, x86_64, $0.34/hr)
  GCE:   e2-standard-8  (8 vCPU, 32 GB, x86_64)
  Azure: Standard_F8s_v2 (8 vCPU, 16 GB, x86_64)
  OCI:   VM.Standard3.Flex:4:32 (4 oCPU/8 vCPU, 32 GB)
```

### update-catalog

Refresh the instance catalog from live cloud APIs:

```bash
# Update all clouds
uv run sct.py update-catalog

# Update specific cloud
uv run sct.py update-catalog --cloud aws
```

## Catalog Management

Instance specs and pricing live in `data/instance_catalog/`:

```
data/instance_catalog/
├── aws.yaml       # i8g, i4i, c6i, m6i, t3
├── gce.yaml       # z3-highmem, e2-standard, e2-highcpu, n2-highmem
├── azure.yaml     # Standard_L, Standard_F, Standard_D
└── oci.yaml       # DenseIO.E5, VM.Standard3, VM.Standard.E4
```

Catalog files are generated by `sct.py update-catalog` (queries cloud pricing APIs) and can also be manually edited for new families.

## Migration Guide

### Migrating existing test configs

If your config currently uses cloud-specific instance types:

**Before** (cloud-specific, one backend only):
```yaml
instance_type_db: 'i4i.2xlarge'
gce_instance_type_db: 'z3-highmem-16'
azure_instance_type_db: 'Standard_L16s_v4'
```

**After** (single line, works on all backends):
```yaml
db_instance_type:
  vcpu: 8
  memory: ">16gb"
```

### How to migrate

1. Look at the instance type you're currently using and note its vCPU/memory specs
2. Use `show-prices` to see what's available:
   ```bash
   uv run sct.py show-prices --cloud aws --role db
   ```
3. Write constraints that match your hardware needs
4. Verify resolution with `translate-size`:
   ```bash
   uv run sct.py translate-size --config your-test.yaml
   ```
5. Confirm the resolved types match what you had before (or better)

### Common migration patterns

**DB nodes** — typically need local NVMe storage. Just specify vCPU count:
```yaml
# Was: instance_type_db: 'i4i.2xlarge' (8 vCPU, 64 GB, NVMe)
# Now: system picks i8g.2xlarge (ARM, cheaper, same specs)
db_instance_type:
  vcpu: 8
```

**DB nodes — force x86** (if your workload requires it):
```yaml
db_instance_type:
  vcpu: 8
  arch: "x86_64"
# Resolves to i4i.2xlarge (same as before)
```

**Loader nodes** — compute-optimized, no local disk needed:
```yaml
# Was: instance_type_loader: 'c6i.2xlarge' (8 vCPU)
loader_instance_type:
  vcpu: 8
```

**Monitor nodes** — minimal resources:
```yaml
# Was: instance_type_monitor: 't3.large' (2 vCPU)
monitor_instance_type:
  vcpu: 2
```

## Supported Parameters

All 5 roles support constraint syntax:

| Parameter | Role | Description |
|-----------|------|-------------|
| `db_instance_type` | db | Database nodes |
| `oracle_instance_type` | db_oracle | Oracle comparison nodes |
| `zero_token_instance_type` | zero_token | Zero-token nodes |
| `loader_instance_type` | loader | Stress tool nodes |
| `monitor_instance_type` | monitor | Monitoring stack |

## Backends

| Backend | Constraint resolution |
|---------|----------------------|
| `aws` | Full resolution from catalog |
| `gce` | Full resolution from catalog |
| `azure` | Full resolution from catalog |
| `oci` | Full resolution from catalog |
| `docker` | Silently skipped (no instance types) |
| `baremetal` | Silently skipped (pre-existing cluster) |
| `k8s-*` | Silently skipped (pod resources managed separately) |
