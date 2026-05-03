---
status: draft
domain: config
created: 2026-05-03
last_updated: 2026-05-03
owner: fruch
---

# Cross-Cloud Instance Sizing Abstraction

## 1. Problem Statement

SCT test configurations use cloud-specific instance type parameters (`instance_type_db` for AWS,
`gce_instance_type_db` for GCE, `azure_instance_type_db` for Azure, `oci_instance_type_db` for
OCI). This creates two problems:

1. **Duplication**: Every test YAML must repeat equivalent sizing for each cloud provider. PR #14439
   documents that 185 of 234 test configs with AWS sizing lack GCE equivalents. Adding OCI and
   Azure makes this a 4x duplication problem.

2. **No portable sizing language**: There is no way to say "give me an 8-vCPU, 64 GB, local-SSD
   database node" in a cloud-agnostic way. Each cloud uses its own naming scheme (`i4i.2xlarge`,
   `z3-highmem-16`, `Standard_L8s_v3`, `DenseIO.E5.Flex`), forcing test authors to look up
   equivalences manually and reviewers to validate them with external documentation.

**This plan introduces a T-shirt sizing model** (e.g. `large`, `2xlarge`, `xlarge`) with
per-role config parameters (`db_instance_type`, `loader_instance_type`, `monitor_instance_type`)
that map bidirectionally to concrete cloud instance types. Test YAMLs can use either the abstract
size or a cloud-specific override.

## 2. Current State

### Instance Type Configuration

Each cloud backend has dedicated config parameters in `sdcm/sct_config.py`:

| Role | AWS | GCE | Azure | OCI |
|------|-----|-----|-------|-----|
| DB | `instance_type_db` (L968) | `gce_instance_type_db` (L1236) | `azure_instance_type_db` (L1272) | `oci_instance_type_db` (L1310) |
| Loader | `instance_type_loader` (L962) | `gce_instance_type_loader` (L1098) | `azure_instance_type_loader` (L1266) | `oci_instance_type_loader` (L1296) |
| Monitor | `instance_type_monitor` (L965) | `gce_instance_type_monitor` (L1107) | `azure_instance_type_monitor` (L1269) | `oci_instance_type_monitor` (L1303) |

### Default Instance Types per Cloud

From `defaults/` YAML files:

| Role | AWS | GCE | Azure | OCI |
|------|-----|-----|-------|-----|
| DB | *(none — set per test)* | `n2-highmem-8` | `Standard_L8s_v3` | *(none)* |
| Loader | `c6i.xlarge` | `e2-standard-2` | `Standard_F4s_v2` | `VM.Standard3.Flex:4:32` |
| Monitor | `t3.large` | `n2-highmem-8` | `Standard_D2_v4` | `VM.Standard.E4.Flex:4:32` |

### GCE Disk Parameters

GCE requires explicit disk config alongside instance type:
- `gce_root_disk_type_db` (default: `pd-ssd`)
- `gce_n_local_ssd_disk_db` (default: 4)
- `gce_setup_hybrid_raid` (default: false)

These have no AWS/Azure/OCI equivalents — they're baked into the instance family choice.

### Existing Configuration Fragments

`configurations/gce/` has per-instance-type fragments: `n2-highmem-8.yaml` through
`n2-highmem-64.yaml`, plus `z3-highmem-8-highlssd.yaml`. No equivalent fragments exist for AWS,
Azure, or OCI.

### Config Validation

`sdcm/sct_config.py` method `_instance_type_validation()` (L3670) validates
`nemesis_grow_shrink_instance_type` per-backend. The backend-required parameter groups are defined
at L2379:

```python
"aws": ["region_name", "instance_type_db"],
"gce": ["gce_datacenter", "gce_instance_type_db"],
```

### No Existing Abstraction

There is no T-shirt size concept, no cross-cloud mapping, and no translation layer anywhere in
the codebase. PR #14439 proposes a static mapping document (`docs/gce-instance-type-mapping.md`)
but not a programmatic translation.

## 3. Goals

1. Define a **T-shirt sizing vocabulary** using AWS-style size suffixes (`large`, `xlarge`,
   `2xlarge`, `4xlarge`, `8xlarge`, `16xlarge`) as values for per-role config parameters.

2. Create a **bidirectional mapping module** (`sdcm/utils/cloud_sizes.py`) that translates:
   - Size + role + cloud -> concrete cloud instance type (+ disk params where needed)
   - Concrete cloud instance type -> size (for reporting / display)

3. Each cloud provider has a **default instance family** for DB nodes:
   - AWS: `i8g` family (Graviton/ARM, default) and `i4i` family (x86 fallback)
   - GCE: `z3-highmem` family (local NVMe, optimized for storage)
   - Azure: `Standard_L*s_v4` family (storage-optimized v4)
   - OCI: `DenseIO.E5` family

   The mapping module automatically selects the correct family based on architecture
   (ARM vs x86), detected from the AMI/image or overridden explicitly.

4. Add **new config parameters** `db_instance_type`, `loader_instance_type`,
   `monitor_instance_type` that accept abstract size values (`large`, `2xlarge`, etc.).
   When set, they resolve to the correct cloud-specific type at config load time.
   Cloud-specific overrides still take precedence.

5. **Zero breaking changes**: All existing cloud-specific parameters continue to work unchanged.
   The abstract sizing is opt-in.

6. **AWS param naming alignment** (separate phase): Add `aws_instance_type_db`,
   `aws_instance_type_loader`, `aws_instance_type_monitor` as aliases for the current
   unprefixed `instance_type_db`, `instance_type_loader`, `instance_type_monitor` — making
   the naming consistent across all clouds. The unprefixed names continue to work.

6. This plan supersedes the static mapping approach proposed in PR #14439. PR #14439's
   `docs/gce-instance-type-mapping.md` was never merged — the programmatic mapping here
   replaces that approach entirely.

### Demo: How It Works

A typical longevity test YAML today (6+ lines of cloud-specific sizing):

```yaml
# Current: must specify every cloud explicitly
instance_type_db: 'i4i.2xlarge'              # AWS
gce_instance_type_db: 'z3-highmem-16'        # GCE
gce_n_local_ssd_disk_db: 4
gce_root_disk_type_db: 'pd-ssd'
azure_instance_type_db: 'Standard_L16s_v4'   # Azure
oci_instance_type_db: 'DenseIO.E5.Flex:8:128' # OCI

instance_type_loader: 'c6i.2xlarge'
gce_instance_type_loader: 'e2-standard-8'
azure_instance_type_loader: 'Standard_F8s_v2'
oci_instance_type_loader: 'VM.Standard3.Flex:8:64'

instance_type_monitor: 't3.large'
gce_instance_type_monitor: 'n2-highmem-4'
azure_instance_type_monitor: 'Standard_D2_v4'
oci_instance_type_monitor: 'VM.Standard.E4.Flex:2:16'
```

Becomes (3 lines, all clouds resolved automatically):

```yaml
# New: abstract sizing — automatically resolves per backend
db_instance_type: '2xlarge'
loader_instance_type: 'medium'
monitor_instance_type: 'small'
```

When running with `--backend gce`, the config system resolves:
- `db_instance_type: '2xlarge'` → `gce_instance_type_db: 'z3-highmem-16'` + `gce_n_local_ssd_disk_db: 4` + `gce_root_disk_type_db: 'pd-ssd'`
- `loader_instance_type: 'medium'` → `gce_instance_type_loader: 'e2-standard-8'`
- `monitor_instance_type: 'small'` → `gce_instance_type_monitor: 'n2-highmem-4'`

When running with `--backend aws`:
- `db_instance_type: '2xlarge'` → `instance_type_db: 'i4i.2xlarge'`
- `loader_instance_type: 'medium'` → `instance_type_loader: 'c6i.2xlarge'`
- `monitor_instance_type: 'small'` → `instance_type_monitor: 't3.large'`

Cloud-specific overrides still win — if a test needs a specific non-default type:

```yaml
db_instance_type: '2xlarge'          # used for all clouds...
instance_type_db: 'i3en.2xlarge'     # ...except AWS, which uses this instead
```

### Future Integration: Mixed Instance Type Clusters (PR #13427)

> **NOTE**: PR #13427 has not yet landed. This section describes a *future* integration
> opportunity, not a deliverable of this plan. No implementation work for `cluster_topology`
> is included in Phases 1-5.

PR [#13427](https://github.com/scylladb/scylla-cluster-tests/pull/13427) proposes a
`cluster_topology` config for heterogeneous clusters with different instance types per rack/DC.
If/when that PR lands, it currently requires cloud-specific instance types in rack definitions:

```yaml
# PR #13427 current syntax — cloud-specific
cluster_topology:
  - dc: dc1
    racks:
      - [i4i.large, i4i.2xlarge]       # AWS only
      - [i4i.large, i4i.large]
```

With cross-cloud sizing, this could become backend-agnostic:

```yaml
# Future possibility — works on any cloud
cluster_topology:
  - dc: dc1
    racks:
      - [large, 2xlarge]
      - [large, large]
```

The mapping module from Phase 1 provides the translation layer that `cluster_topology` could call
to resolve abstract sizes to the active backend's concrete types. This is a natural future
integration point — no work is required in this plan.

## 4. Implementation Phases

### Phase 1 — Sizing Data Model and Mapping Module

**Scope:** New module `sdcm/utils/cloud_sizes.py`, unit tests

**Description:**

Create the core data model and mapping registry. The module defines:

```python
@dataclass
class InstanceSpec:
    """Concrete instance specification for one cloud."""
    instance_type: str          # e.g. "i4i.2xlarge"
    vcpus: int                  # e.g. 8
    memory_gb: int              # e.g. 64
    local_ssd_count: int = 0    # e.g. 4 (GCE local SSDs)
    root_disk_type: str = ""    # e.g. "pd-ssd" (GCE)
    extra_params: dict = field(default_factory=dict)  # cloud-specific overrides

@dataclass
class SizeMapping:
    """One abstract size mapped to all clouds."""
    aws: InstanceSpec
    gce: InstanceSpec
    azure: InstanceSpec
    oci: InstanceSpec

# Registry: Dict[str, Dict[str, SizeMapping]]
# e.g. SIZING["db"]["2xlarge"] -> SizeMapping(aws=..., gce=..., azure=..., oci=...)
```

**Default DB family mappings** (`db_instance_type` values):

AWS defaults to `i8g` (Graviton/ARM). When the image architecture is x86, the module
automatically switches to `i4i`. This detection uses the existing `get_arch_from_instance_type()`
/ `_get_normalized_arch()` logic in `sct_config.py` (L3351).

| `db_instance_type` | AWS ARM (`i8g`) | AWS x86 (`i4i`) | GCE (`z3-highmem`) | Azure (`Standard_L*s_v4`) | OCI (`DenseIO.E5`) |
|--------------------|-----------------|-----------------|---------------------|---------------------------|---------------------|
| `large` | `i8g.large` | `i4i.large` | `z3-highmem-4` | `Standard_L4s_v4` | `DenseIO.E5.Flex:2:32` |
| `xlarge` | `i8g.xlarge` | `i4i.xlarge` | `z3-highmem-8` | `Standard_L8s_v4` | `DenseIO.E5.Flex:4:64` |
| `2xlarge` | `i8g.2xlarge` | `i4i.2xlarge` | `z3-highmem-16` | `Standard_L16s_v4` | `DenseIO.E5.Flex:8:128` |
| `4xlarge` | `i8g.4xlarge` | `i4i.4xlarge` | `z3-highmem-32` | `Standard_L32s_v4` | `DenseIO.E5.Flex:16:256` |
| `8xlarge` | `i8g.8xlarge` | `i4i.8xlarge` | `z3-highmem-48` | `Standard_L64s_v4` | `DenseIO.E5.Flex:32:512` |
| `16xlarge` | `i8g.16xlarge` | `i4i.16xlarge` | `z3-highmem-88` | `Standard_L80s_v4` | `DenseIO.E5.Flex:64:1024` |

> **Needs Investigation:** Validate exact `i8g.*` shapes exist in all required AWS regions.
> Validate GCE `z3-highmem-*` shapes exist in all required zones.
> Validate Azure `Standard_L*s_v4` shapes and OCI `DenseIO.E5.Flex` shapes/limits in target
> regions. The vCPU/memory columns above are approximate — confirm from official docs.

**Default Loader family mappings** (`loader_instance_type` values):

| `loader_instance_type` | AWS | GCE | Azure | OCI |
|------------------------|-----|-----|-------|-----|
| `small` | `c6i.xlarge` | `e2-standard-4` | `Standard_F4s_v2` | `VM.Standard3.Flex:4:32` |
| `medium` | `c6i.2xlarge` | `e2-standard-8` | `Standard_F8s_v2` | `VM.Standard3.Flex:8:64` |
| `large` | `c6i.4xlarge` | `e2-standard-16` | `Standard_F16s_v2` | `VM.Standard3.Flex:16:128` |
| `xlarge` | `c6i.16xlarge` | `e2-highcpu-32` | `Standard_F32s_v2` | `VM.Standard3.Flex:32:256` |

**Default Monitor family mappings** (`monitor_instance_type` values):

| `monitor_instance_type` | AWS | GCE | Azure | OCI |
|-------------------------|-----|-----|-------|-----|
| `small` | `t3.large` | `n2-highmem-4` | `Standard_D2_v4` | `VM.Standard.E4.Flex:2:16` |
| `medium` | `m6i.xlarge` | `n2-highmem-8` | `Standard_D4_v4` | `VM.Standard.E4.Flex:4:32` |
| `large` | `m6i.2xlarge` | `n2-highmem-16` | `Standard_D8_v4` | `VM.Standard.E4.Flex:8:64` |

Key API functions:

```python
def resolve_size(role: str, size: str, cloud: str, arch: str = "arm") -> InstanceSpec:
    """('db', '2xlarge', 'aws', 'arm') -> InstanceSpec(instance_type='i8g.2xlarge', ...)
       ('db', '2xlarge', 'aws', 'x86') -> InstanceSpec(instance_type='i4i.2xlarge', ...)
       For non-AWS clouds, arch is ignored (only one family per cloud)."""

def identify_size(cloud: str, instance_type: str) -> tuple[str, str] | None:
    """('gce', 'z3-highmem-16') -> ('db', '2xlarge') (reverse lookup, None if unknown)"""

def get_cloud_params(role: str, spec: InstanceSpec, cloud: str) -> dict:
    """InstanceSpec -> dict of sct_config param names and values for that cloud."""
```

The `arch` parameter defaults to `"arm"` (Graviton). The caller (`_resolve_instance_sizes` in
`sct_config.py`) determines arch from the AMI/image configuration using the existing
`get_arch_from_instance_type()` / `_get_normalized_arch()` logic, or from the explicit
`instance_type_db` if set.

**Definition of Done:**
- [ ] `sdcm/utils/cloud_sizes.py` exists with `InstanceSpec`, `SizeMapping`, `resolve_size`,
  `identify_size`, `get_cloud_params`.
- [ ] Full mapping tables for DB (6 sizes), Loader (4 sizes), Monitor (3 sizes) across 4 clouds.
- [ ] `unit_tests/unit/test_cloud_sizes.py` with parametrized tests covering all mappings in both
  directions (size->cloud, cloud->size).
- [ ] `uv run sct.py pre-commit` passes.

**QA Scenarios:**
```
Scenario: Forward resolution — all clouds, all roles
  Tool: Bash
  Command: uv run python -c "from sdcm.utils.cloud_sizes import resolve_size; spec = resolve_size('db', '2xlarge', 'aws', 'arm'); assert spec.instance_type == 'i8g.2xlarge', spec.instance_type"
  Expected: No assertion error. Also test ('db', '2xlarge', 'aws', 'x86') → i4i.2xlarge,
            gce→z3-highmem-16, azure→Standard_L16s_v4, oci→DenseIO.E5.Flex:8:128.

Scenario: Reverse lookup
  Tool: Bash
  Command: uv run python -c "from sdcm.utils.cloud_sizes import identify_size; r = identify_size('gce', 'z3-highmem-16'); assert r == ('db', '2xlarge'), r"
  Expected: No assertion error.

Scenario: Unknown instance type returns None
  Tool: Bash
  Command: uv run python -c "from sdcm.utils.cloud_sizes import identify_size; assert identify_size('aws', 'x99.nonexistent') is None"
  Expected: No assertion error.

Scenario: Unit tests pass
  Tool: Bash
  Command: uv run python -m pytest unit_tests/unit/test_cloud_sizes.py -x -q --no-header --tb=short -n0
  Expected: All tests pass, 0 failures.
```

**Dependencies:** None.

**Importance:** High — foundational for all subsequent phases.

---

### Phase 2 — Config Integration: `db_instance_type`, `loader_instance_type`, `monitor_instance_type`

**Scope:** `sdcm/sct_config.py`, `defaults/test_default.yaml`

**Description:**

Add three new config parameters:

```python
db_instance_type: String = SctField(
    name="db_instance_type",
    type=str,
    default="",
    help="Abstract instance size for DB nodes (e.g. '2xlarge', 'large'). "
         "Resolves to cloud-specific instance type based on active backend. "
         "Cloud-specific overrides (instance_type_db, gce_instance_type_db, etc.) "
         "take precedence when explicitly set.")

loader_instance_type: String = SctField(...)
monitor_instance_type: String = SctField(...)
```

**Resolution logic** (in a new method `_resolve_instance_sizes`, called **early in `__init__`**
— after config files and env vars are merged but **before** the existing AWS arch/AMI selection
logic at L2594-2794 and before `verify_configuration()`). This ensures abstract sizes are
expanded into cloud-specific params before any downstream logic that reads those params.

To distinguish "user explicitly set `instance_type_db`" from "it came from `defaults/aws_config.yaml`",
introduce a new `_user_provided_keys: set[str]` attribute populated during `__init__` as config
files and env vars are merged (in the loops that call `_load_environment_variables()` and
`merge_dicts_append_strings()`). Each key set by a user config file or `SCT_*` env var is added
to this set. Only skip abstract-size resolution if the cloud-specific param is in
`_user_provided_keys`, not merely present from backend defaults.

```
For each role (db, loader, monitor):
  1. If cloud-specific param was EXPLICITLY set by user config/env -> use it (no change)
  2. Elif {role}_instance_type is set -> determine arch (from AMI config or default 'arm')
     -> resolve via cloud_sizes.resolve_size(role, size, cloud, arch)
     -> populate the cloud-specific param for the active backend
     -> populate the cloud-specific param for the active backend
     -> for GCE, also set gce_n_local_ssd_disk_{role}, gce_root_disk_type_{role}
  3. Else -> fall through to existing defaults (no change)
```

This means a test YAML can be simplified from:

```yaml
# Before: 6 lines, one per cloud + GCE disk params
instance_type_db: 'i4i.2xlarge'
gce_instance_type_db: 'z3-highmem-16'
gce_n_local_ssd_disk_db: 4
gce_root_disk_type_db: 'pd-ssd'
azure_instance_type_db: 'Standard_L16s_v4'
oci_instance_type_db: 'DenseIO.E5.Flex:8:128'
```

To:

```yaml
# After: 1 line, all clouds resolved automatically
db_instance_type: '2xlarge'
```

**Definition of Done:**
- [ ] Three new `SctField` entries in `sct_config.py`.
- [ ] `_resolve_instance_sizes()` method resolves abstract sizes to cloud-specific params.
- [ ] Cloud-specific overrides still take precedence over abstract sizes.
- [ ] Existing tests in `unit_tests/unit/test_config.py` still pass.
- [ ] New unit tests for resolution logic (abstract size alone, abstract + override, no abstract).
- [ ] `uv run sct.py pre-commit` passes.

**QA Scenarios:**
```
Scenario: Abstract size resolves for AWS backend
  Tool: Bash
  Command: uv run python -m pytest unit_tests/unit/test_config.py -x -q --no-header --tb=short -n0 -k "test_resolve_instance_sizes_aws"
  Expected: Test passes — verifies that _resolve_instance_sizes() with db_instance_type='2xlarge'
            and backend='aws' populates instance_type_db='i8g.2xlarge'.

Scenario: Cloud-specific override takes precedence
  Tool: Bash
  Command: uv run python -m pytest unit_tests/unit/test_config.py -x -q --no-header --tb=short -n0 -k "test_resolve_instance_sizes_explicit_override"
  Expected: Test passes — verifies that when instance_type_db is in _user_provided_keys,
            abstract size is NOT applied.

Scenario: GCE resolution also sets disk params
  Tool: Bash
  Command: uv run python -m pytest unit_tests/unit/test_config.py -x -q --no-header --tb=short -n0 -k "test_resolve_instance_sizes_gce_disk_params"
  Expected: Test passes — verifies gce_instance_type_db, gce_n_local_ssd_disk_db, gce_root_disk_type_db
            all populated from abstract size.

Scenario: All resolution unit tests pass
  Tool: Bash
  Command: uv run python -m pytest unit_tests/unit/test_config.py -x -q --no-header --tb=short -n0 -k "resolve_instance_sizes"
  Expected: All tests pass, 0 failures.
```

**Dependencies:** Phase 1.

**Importance:** High — enables the new config parameter for test YAMLs.

---

### Phase 3 — CLI Helpers: `sct.py show-sizes` and `sct.py translate-size`

**Scope:** `sct.py`, new CLI subcommands

**Description:**

Add two utility subcommands for developers:

1. `sct.py show-sizes` — print the full mapping table in a readable format.
   Optionally filter by role (`--role db`) or cloud (`--cloud gce`).

2. `sct.py translate-size <instance_type>` — given a cloud-specific instance type, print the
   abstract size and equivalents on other clouds.

   ```
   $ sct.py translate-size i8g.2xlarge
   i8g.2xlarge (AWS/ARM) = db 2xlarge
     AWS (x86): i4i.2xlarge
     GCE:       z3-highmem-16 (16 vCPU / 128 GB, 4 local SSDs)
     Azure:     Standard_L16s_v4
     OCI:       DenseIO.E5.Flex:8:128
   ```

3. `sct.py translate-size --config <test-case.yaml> [--backend <cloud>]` — **config transformation
   mode**. Reads a test config YAML and outputs a fully-expanded version where all abstract
   sizes (`db_instance_type`, `loader_instance_type`, `monitor_instance_type`) are resolved to
   the concrete cloud-specific instance types (and any associated cloud-specific settings like
   `gce_n_local_ssd_disk_db`).

   If `--backend` is given, only that cloud is expanded. If omitted, expands for ALL backends,
   outputting a multi-backend config or one file per backend.

   ```
   $ sct.py translate-size --config test-cases/longevity/longevity-100gb-4h.yaml --backend gce
   # Output: the YAML with abstract sizes replaced by GCE-specific values
   # e.g. db_instance_type: 2xlarge → gce_instance_type_db: z3-highmem-16
   #                                   gce_n_local_ssd_disk_db: 4
   #                                   gce_root_disk_type_db: pd-ssd

   $ sct.py translate-size --config test-cases/longevity/longevity-100gb-4h.yaml
   # Output: shows expansion for each backend (aws, gce, azure, oci)
   ```

   This is useful for:
   - Migrating existing test configs to abstract sizes (validate the mapping is correct)
   - Reviewing what concrete types a config would resolve to on each cloud
   - CI validation that abstract configs resolve correctly

**Definition of Done:**
- [ ] `sct.py show-sizes` prints full mapping table.
- [ ] `sct.py translate-size <type>` prints cross-cloud equivalents.
- [ ] `sct.py translate-size --config <yaml> [--backend <cloud>]` expands abstract sizes to cloud-specific params.
- [ ] All three modes have `--help` text.
- [ ] `uv run sct.py pre-commit` passes.

**QA Scenarios:**
```
Scenario: show-sizes prints table
  Tool: Bash
  Command: uv run sct.py show-sizes --role db --cloud aws
  Expected: Output contains i8g.large, i8g.xlarge, ... i8g.16xlarge in a readable table.

Scenario: translate-size single type
  Tool: Bash
  Command: uv run sct.py translate-size i8g.2xlarge
  Expected: Output shows "db 2xlarge" and equivalents for GCE (z3-highmem-16), Azure (Standard_L16s_v4), OCI.

Scenario: translate-size config mode
  Tool: Bash
  Command: uv run sct.py translate-size --config unit_tests/test_data/test_abstract_sizing.yaml --backend gce
  Expected: Output is a YAML with abstract sizes expanded to gce_instance_type_db, gce_n_local_ssd_disk_db, etc.
  Note: unit_tests/test_data/test_abstract_sizing.yaml is a fixture file created in Phase 3
        that uses db_instance_type/loader_instance_type/monitor_instance_type fields.

Scenario: translate-size unknown type
  Tool: Bash
  Command: uv run sct.py translate-size x99.nonexistent
  Expected: Clear error message "Unknown instance type: x99.nonexistent".
```

**Dependencies:** Phase 1.

**Importance:** Medium — developer convenience for test authoring and review.

---

### Phase 4 — Migrate Longevity Test Configs

**Scope:** `test-cases/longevity/` YAML files

**Description:**

For a representative subset of longevity tests (start with 10-15 high-traffic configs), replace
the cloud-specific instance type parameters with the new `db_instance_type` /
`loader_instance_type` / `monitor_instance_type` parameters.

Keep cloud-specific overrides only where a test intentionally uses a non-standard instance
(e.g. a performance test requiring a specific AWS type).

**Definition of Done:**
- [ ] 10-15 longevity YAMLs migrated to use `db_instance_type` etc.
- [ ] `./utils/lint_test_cases.sh` passes for all modified files.
- [ ] Spot-check 3 configs: `./sct.py conf -b aws <path>` and `./sct.py conf -b gce <path>` both
  resolve to the expected instance types.
- [ ] `uv run sct.py pre-commit` passes.

**QA Scenarios:**
```
Scenario: Migrated config resolves on AWS
  Tool: Bash
  Command: uv run sct.py conf -b aws test-cases/longevity/<migrated-config>.yaml | grep instance_type_db
  Expected: Output shows concrete AWS instance type (e.g. i8g.2xlarge), not the abstract size.

Scenario: Same config resolves on GCE
  Tool: Bash
  Command: uv run sct.py conf -b gce test-cases/longevity/<migrated-config>.yaml | grep gce_instance_type_db
  Expected: Output shows GCE type (e.g. z3-highmem-16).

Scenario: Lint passes
  Tool: Bash
  Command: ./utils/lint_test_cases.sh
  Expected: Exit code 0, no errors.
```

**Dependencies:** Phase 2.

**Importance:** Medium — proves the approach on the most common test category.

---

### Phase 5 — Documentation and YAML Comment Convention

**Scope:** `docs/`, `AGENTS.md`

**Description:**

- Add `docs/cross-cloud-sizing.md` documenting the T-shirt sizing model, the full mapping table,
  how to use `db_instance_type` etc. in test YAMLs, and how to add new sizes or cloud families.
- Update `AGENTS.md` to reference the new sizing system.
- Define the annotation convention: `# 2xlarge: 8 vCPU / 64 GB` comments on instance lines.
- This plan supersedes the static mapping approach from PR #14439 (which was never merged).
  The programmatic mapping is now the single source of truth.

**Definition of Done:**
- [ ] `docs/cross-cloud-sizing.md` exists with usage guide and full mapping table.
- [ ] `AGENTS.md` updated with cross-cloud sizing convention.
- [ ] `uv run sct.py pre-commit` passes.

**QA Scenarios:**
```
Scenario: Documentation file exists and has required sections
  Tool: Bash
  Command: grep -c "## Usage\|## Mapping Table\|## Adding New Sizes" docs/cross-cloud-sizing.md
  Expected: At least 3 matches (all sections present).

Scenario: AGENTS.md references sizing system
  Tool: Bash
  Command: grep -c "cloud_sizes\|cross-cloud sizing" AGENTS.md
  Expected: At least 1 match.
```

**Dependencies:** Phases 1-2.

**Importance:** Medium — essential for adoption but not blocking further migration work.

## 5. Testing Requirements

### Unit Tests
- `unit_tests/unit/test_cloud_sizes.py`: Parametrized tests for all `resolve_size` and
  `identify_size` mappings (4 clouds x 13 sizes = 52 forward + 52 reverse = 104 test cases).
- `unit_tests/unit/test_config.py`: New tests for `_resolve_instance_sizes()` method covering:
  - Abstract size resolves correctly per backend.
  - Cloud-specific override takes precedence over abstract size.
  - Empty abstract size falls through to existing defaults.
  - Invalid abstract size raises a clear error.
  - GCE resolution populates disk params alongside instance type.

### Config Load Linting
- `./utils/lint_test_cases.sh` must pass after every YAML-editing phase.
- Spot-check migrated configs with `./sct.py conf -b <backend> <path>` for each cloud.

### Manual Verification
- After Phase 4, run at least one longevity test on GCE with a migrated config to confirm the
  resolved instance type is correct.
- Use `sct.py translate-size` to verify bidirectional mapping for the 5 most common instance types
  across each cloud.

## 6. Success Criteria

- Any test YAML can express sizing with a single `db_instance_type: '2xlarge'` line and have it
  resolve correctly for AWS, GCE, Azure, and OCI.
- `resolve_size("db", "2xlarge", "gce")` returns `z3-highmem-16` (not `n2-highmem-16`).
- Cloud-specific overrides still work unchanged — zero breaking changes.
- `sct.py translate-size i8g.2xlarge` outputs equivalents for all 4 clouds.
- `sct.py translate-size --config test-cases/longevity/longevity-100gb-4h.yaml --backend gce` outputs fully-expanded GCE config.
- The mapping module is the single source of truth — no hand-maintained mapping documents needed.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `z3-highmem-*` shapes unavailable in some GCE zones | Medium | High | Validate shape availability in target zones during Phase 1. Fall back to `n2-highmem-*` for zones without `z3` support and document the exception. |
| Azure `Standard_L*s_v4` shapes not available in all regions | Medium | Medium | Validate during Phase 1. The v4 family is newer; if unavailable, fall back to `Standard_L*s_v3` with a documented note. |
| OCI `DenseIO.E5` flex shapes have different OCPU/memory ratios than assumed | Medium | Medium | Validate exact specs from OCI docs during Phase 1. OCI flex shapes allow custom OCPU:memory ratios — document the chosen ratio. |
| Abstract sizes don't cover all existing test configs (custom/unusual instance types) | Low | Low | Cloud-specific overrides remain supported. Abstract sizing is opt-in, not mandatory. |
| Config resolution order creates subtle bugs | Medium | High | Extensive unit tests for precedence logic. The rule is simple: explicit cloud param > abstract size > default. |
| Performance tests need exact hardware match, not "closest equivalent" | Medium | Medium | Performance test YAMLs can continue using cloud-specific overrides. The abstract size is a convenience, not a mandate. |
| Interaction with PR #14439 (AWS/GCE parity plan) | High | Low | This plan supersedes the static mapping approach in PR #14439. The two can coexist: PR #14439 can add GCE params to existing YAMLs now, and this plan provides the abstraction to simplify them later. |
| Integration with PR #13427 (mixed instance type clusters) | Medium | Low | PR #13427's `cluster_topology` can use `resolve_size()` from this plan's mapping module to accept abstract sizes in rack definitions. No blocking dependency — the plans complement each other and can be integrated after both land. |
