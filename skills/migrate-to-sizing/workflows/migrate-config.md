# Migrate a Test Config to Constraint-Based Sizing

Step-by-step process for converting a pipeline job's test config from literal instance types to sizing constraints.

## Phase 1: Identify the Test Config

**Entry criteria**: You have a Jenkins pipeline job name or path.

1. Find the Jenkinsfile in `jenkins-pipelines/` (e.g., `jenkins-pipelines/oss/tier1/`)
2. Look for `test_config` or `--config` references to identify the YAML file(s)
3. Open the test-case YAML file

**Exit criteria**: You have the test-case YAML path and can read its contents.

## Phase 2: Extract Current Instance Types

**Entry criteria**: You have the test-case YAML open.

1. Find all `instance_type_*` parameters:
   - `instance_type_db` (AWS db instance)
   - `gce_instance_type_db` (GCE db instance)
   - `azure_instance_type_db` (Azure db instance)
   - `instance_type_loader` (AWS loader)
   - `gce_instance_type_loader` (GCE loader)
   - `azure_instance_type_loader` (Azure loader)
   - `instance_type_monitor` (AWS monitor)
2. Also note `gce_n_local_ssd_disk_db` if present (will be removed)
3. Look up each instance's specs (vCPU, memory, disk, arch) using the reference table in SKILL.md or AWS/GCE/Azure docs

**Exit criteria**: You have a table of all instance types and their specs.

## Phase 3: Determine Sizing Constraints

**Entry criteria**: You have instance specs for all roles.

For each role (db, loader, monitor):

### DB Role
1. Take the **minimum vCPU** across all cloud variants as the `vcpu` value
   - Exception: if GCE/Azure have MORE vCPUs just because the exact match wasn't available, use the AWS vCPU count as the intent
2. Set `memory` to `'>={aws_memory_gb}'` (use AWS instance as the reference)
3. Pin `arch: x86_64` if the AWS instance is from an Intel-only family (i7i, i7ie, i3en)
4. Pin `local_disk_gb: '>={value}'` only if needed to differentiate from smaller-disk instances with the same vCPU/memory/arch
5. Do NOT set `local_disk_count` (implicit for db role)

### Loader Role
1. If the loader matches the default (vcpu 4, memory >=8), omit sizing_loader entirely
2. Otherwise, set `vcpu` and `memory` based on the AWS loader instance specs

### Monitor Role
1. If the monitor matches the default (vcpu 2, memory >=8), omit sizing_monitor entirely
2. Otherwise, set `vcpu` and `memory` based on the AWS monitor instance specs

**Exit criteria**: You have the sizing constraint YAML block for each role that needs overriding.

## Phase 4: Apply the Migration

**Entry criteria**: You have the sizing constraints determined.

1. Remove all `instance_type_*`, `gce_instance_type_*`, `azure_instance_type_*` params for the roles being migrated
2. Remove `gce_n_local_ssd_disk_db` if present
3. Add the `sizing_db`/`sizing_loader`/`sizing_monitor` blocks in their place
4. Keep any special params like `nemesis_grow_shrink_instance_type` unchanged

**Exit criteria**: The YAML file has sizing constraints instead of literal instance types.

## Phase 5: Verify with Preview

**Entry criteria**: The YAML file is saved with sizing constraints.

1. Run: `uv run python sct.py sizing preview <config-file>`
2. Verify:
   - AWS db resolves to a storage-optimized instance (i8ge, i8g, i7ie, i7i, or i4i family)
   - GCE db resolves to z3-highmem-* (storage) or shows cross-cloud estimation
   - Azure db resolves to Standard_L*s_v3 or Standard_L*as_v3
   - Loader resolves to compute instances (c6i, e2-standard, Standard_F*s_v2)
   - Monitor resolves to general-purpose (t3, e2-standard, Standard_D*)
   - No unexpected "not in catalog" errors for resolved instances
3. Compare estimated costs — they should be in the same ballpark as the original instances

**Known limitation**: Multi-DC configs (`n_db_nodes: '4 4'`) will error in preview. The migration is still correct; preview just can't parse the space-separated node count.

**Exit criteria**: Preview output shows reasonable instance resolution across all clouds.

## Phase 6: Handle Edge Cases

### Instance not in catalog
If the original instance type isn't in the AWS catalog (e.g., i3en, c5n, c7i), the sizing system can still resolve because it matches on constraints, not instance names. The constraint captures the intent.

### GCE used n2-highmem with local SSD attached
The `gce_n_local_ssd_disk_db` parameter attached local SSDs to n2-highmem instances. With sizing, the resolver picks z3-highmem instances which have built-in local storage. Remove `gce_n_local_ssd_disk_db`.

### Test uses both instance_type_db AND a configuration fragment
Check if a `configurations/*.yaml` fragment also sets instance types. If so, migrate the fragment too, or ensure the test-case values override correctly.

### Multiple test configs in one pipeline
Some pipelines use `--config a.yaml --config b.yaml`. Later configs override earlier ones. Migrate both but be aware of override semantics.
