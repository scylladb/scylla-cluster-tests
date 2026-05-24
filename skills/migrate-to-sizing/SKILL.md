---
name: migrate-to-sizing
description: >-
  Migrate SCT test configs and pipeline jobs from literal instance_type params to
  constraint-based sizing_db/sizing_loader/sizing_monitor. Use when converting
  instance_type_db, gce_instance_type_db, azure_instance_type_db, instance_type_loader,
  instance_type_monitor to sizing constraints. Covers identifying instance specs,
  choosing vcpu/memory/arch/disk constraints, running sizing preview, and handling
  multi-DC or special params like nemesis_grow_shrink_instance_type.
---

# Migrate Test Configs to Constraint-Based Sizing

Convert literal per-cloud instance type parameters to cloud-agnostic sizing constraints.

## Essential Principles

### Map Instance Specs to Constraints

Every literal instance type has specific vCPU, memory, and disk characteristics. The sizing constraint must capture the **intent** (what resources the test needs) not the specific instance. Use the minimum constraints that would resolve to an equivalent or better instance across all clouds.

### DB Role Always Gets Local Disk Implicitly

The sizing system adds `local_disk_count > 0` automatically for db/db_oracle/zero_token roles. Never specify `local_disk_count` in sizing_db — it's redundant. Only specify `local_disk_gb` when you need to differentiate between disk tiers (e.g., i7ie's 5000 GB vs i7i's 1875 GB).

### Pin Architecture Only When Required

Only add `arch: x86_64` when the original config used Intel-only families (i7i, i7ie, i3en) AND the test specifically needs x86. If the original used i4i/i8g/i8ge (which have both arch variants or ARM), don't pin arch — let the resolver pick the best match per cloud.

### Loader and Monitor Use Defaults Unless Overridden

`sizing_loader` and `sizing_monitor` have defaults in `defaults/test_default.yaml` (loader: vcpu 4, memory >=8; monitor: vcpu 2, memory >=8). Only add sizing_loader/sizing_monitor to a test config if it needs MORE than the default.

### Keep Special Params As-Is

Some params like `nemesis_grow_shrink_instance_type` are AWS-specific operational parameters, not sizing constraints. Leave them as literal instance types.

## Working from Jenkins Pipeline Files

The preferred workflow is to migrate based on a specific Jenkins pipeline job. The `sizing preview` command accepts Jenkinsfile paths directly:

```bash
# Preview directly from a pipeline file — extracts test_config automatically
uv run python sct.py sizing preview jenkins-pipelines/oss/tier1/longevity-twcs-48h.jenkinsfile
```

This uses `parse_jenkinsfile()` to extract the `test_config` YAML paths and runs preview on them. Start from the pipeline, not the config file.

## Documenting No-Match Clouds

When a sizing constraint produces "No match" for a specific cloud (commonly OCI due to min 16 vCPUs in catalog), add a YAML comment on the `sizing_db:` line:

```yaml
sizing_db:  # no OCI match (min 16 vCPUs in OCI catalog)
  vcpu: 8
  memory: '>=64'
```

For multiple clouds:

```yaml
sizing_db:  # no GCE/OCI match (min 16 vCPUs in OCI catalog, min 8 in GCE z3)
  vcpu: 2
  memory: '>=16'
```

This helps future readers understand that the "No match" is expected and not a bug in the constraints.

## Lowering Memory Constraints for OCI DenseIO Compatibility

OCI DenseIO.E5 Flex shapes have a fixed memory ratio of 12 GB/OCPU (vs 16 GB/OCPU on AWS i-series or GCE z3). This means at the same vCPU count, OCI provides less memory:

| vCPUs | OCI DenseIO.E5 Memory | AWS i8g Memory | GCE z3 Memory |
|-------|----------------------|----------------|---------------|
| 16    | 96 GB                | 128 GB         | 128 GB        |
| 32    | 192 GB               | 256 GB         | 256 GB        |
| 64    | 384 GB               | 512 GB         | 512 GB        |

When `memory: '>=128'` with `vcpu: 16` causes OCI "No match", lower the memory constraint to match OCI's ratio (vcpus × 6 GB, since 1 OCPU = 2 vCPUs and memory = 12 GB/OCPU):

```yaml
sizing_db:
  vcpu: 16
  # OCI DenseIO.E5 at 16 vCPUs provides only 96 GB (12 GB/OCPU fixed ratio).
  # Lowered from >=128 to >=96 so OCI can match; other clouds still pick >=128 naturally.
  memory: '>=96'
```

Other clouds will still resolve to their natural >=128 GB instances because their preferred families don't have a 96 GB option at 16 vCPUs — they jump straight to 128 GB.

**When to lower**: vcpu >= 16 AND the memory constraint exceeds OCI's `vcpus × 6` ratio.

**When NOT to lower**: vcpu <= 8 — OCI's smallest DenseIO shape is 16 vCPUs, so "No match" is unavoidable regardless of memory. Just document with `# no OCI match`.

## When to Use

- Converting a test-case YAML from `instance_type_db`/`gce_instance_type_db`/`azure_instance_type_db` to `sizing_db`
- Converting `instance_type_loader`/`gce_instance_type_loader`/`azure_instance_type_loader` to `sizing_loader`
- Converting `instance_type_monitor` to `sizing_monitor`
- Given a Jenkins pipeline job, identifying which test-case YAML to migrate
- Verifying migration correctness via `sizing preview`

## When NOT to Use

- Adding new instance types to the catalog (that's catalog maintenance)
- Modifying the sizing resolver logic itself
- Working on k8s-specific instance configurations (sizing ignores k8s for now)
- Changing `nemesis_grow_shrink_instance_type` or other non-sizing instance params

## Migration Process

See [migrate-config.md](workflows/migrate-config.md) for the full step-by-step workflow.

### Quick Reference: Instance Spec Lookup

To look up instance specs, use the sizing catalog commands:

```bash
# Show all instances in a cloud catalog
uv run python sct.py sizing catalog --cloud aws

# Resolve constraints to find matching instances
uv run python sct.py sizing resolve --vcpu 8 --memory 64 --role db

# Preview what a config file resolves to across all clouds
uv run python sct.py sizing preview <config-file>
```

The catalogs are in `data/instance_catalog/` (aws.yaml, gce.yaml, azure.yaml, oci.yaml).

### Quick Reference: Constraint Format

```yaml
sizing_db:
  vcpu: 8              # exact match (== 8)
  memory: '>=64'       # minimum 64 GB
  arch: x86_64         # optional: pin to Intel (only if needed)
  local_disk_gb: '>=5000'  # optional: pin disk size (only to differentiate tiers)

sizing_loader:
  vcpu: 16             # override default (4)
  memory: '>=16'       # override default (>=8)

sizing_monitor:
  vcpu: 4              # override default (2)
  memory: '>=16'       # override default (>=8)
```

### Decision Tree: When to Pin Architecture

```
Was the original instance Intel-only (i7i, i7ie, i3en)?
├── YES → Does the test NEED Intel specifically?
│   ├── YES (e.g., testing Intel-specific features) → add arch: x86_64
│   └── NO (just happened to be Intel) → still add arch: x86_64 to preserve behavior
└── NO (i4i, i8g, i8ge, or mixed) → do NOT pin arch
```

### Decision Tree: When to Pin Disk Size

```
Does the original instance have notably large disk (>= 2500 GB per disk)?
├── YES → Is there a smaller-disk instance with same vCPU/memory/arch?
│   ├── YES → add local_disk_gb: '>=XXXX' to differentiate
│   └── NO → don't pin (resolver will find the right one)
└── NO → don't pin
```

## Handling Multi-DC Configs

Multi-DC configs use space-separated node counts like `n_db_nodes: '4 4'`. The sizing preview command currently errors on these. When migrating multi-DC configs:
1. The migration itself is the same (replace instance_type with sizing constraints)
2. Preview validation must be skipped or the preview tool must be fixed to handle multi-DC

## Verification

After migration, run:
```bash
uv run python sct.py sizing preview <config-file>
```

Check that:
- DB instances resolve to storage-optimized instances with local disk on all clouds
- Loader instances resolve to compute-optimized instances without local disk
- Monitor instances resolve to general-purpose or burstable instances
- No "not in catalog" warnings for the sizing-resolved instances
- Estimated costs are reasonable compared to the original instances

## Reference Index

| File | Content |
|------|---------|
| [migrate-config.md](workflows/migrate-config.md) | Step-by-step migration workflow |

## Success Criteria

- [ ] All `instance_type_*` and `gce_instance_type_*` and `azure_instance_type_*` params removed
- [ ] Replaced with appropriate `sizing_db`/`sizing_loader`/`sizing_monitor` constraints
- [ ] `gce_n_local_ssd_disk_db` removed (handled by resolver)
- [ ] Architecture pinned only for Intel-only originals
- [ ] Disk size pinned only when needed to differentiate tiers
- [ ] `sizing preview` resolves without errors (except known multi-DC limitation)
- [ ] Any "No match" clouds documented with YAML comment next to the sizing block
- [ ] Special params (nemesis_grow_shrink_instance_type) left untouched
