---
status: complete
domain: config
created: 2026-05-13
last_updated: 2026-06-25
owner: null
---

# Constraint-Based Instance Sizing

## Implementation Status

> **Status**: Implementation complete in PR #14576.
>
> All 32 review comments from PR #14517 are resolved in the implementation.
>
> ### Key Deviations from Original Plan
>
> | Plan | Actual Implementation |
> |------|----------------------|
> | CLI: `show-prices`, `translate-size`, `show-sizes` | CLI: `sizing catalog`, `sizing resolve`, `sizing preview` (grouped under `sizing` subcommand) |
> | Config key: `db_instance_type` | Config key: `instance_type_db` (matches existing SCT naming) |
> | Env var: `SCT_DB_INSTANCE_TYPE.vcpu=8` | Env var: `SCT_INSTANCE_TYPE_DB.VCPU=8` |
> | 9 separate commits | 3 commits (core + config extraction + agnostic defaults) |
> | `sdcm/utils/cloud_sizes.py` removal (Task 13) | Deferred — old code still present, not yet removed |
> | CLI in `sct.py` | CLI extracted to `sct_sizing.py` (separate module) |
> | Preferred families in catalog YAML files | Preferred families in `sizing_config.yaml` (separate config) |
> | OCI pricing hardcoded | OCI pricing fetched live from Oracle API with lazy caching |
> | GCE catalog: z3-highmem only | GCE catalog: z3-highmem + n2-highmem (with disk variants 1-24 SSDs) |
> | DenseIO memory: 16 GB/OCPU | DenseIO memory: 12 GB/OCPU (corrected from Oracle docs) |
> | Tier1 configs: all migrated uniformly | 3 configs had memory lowered for OCI compatibility; 7 with vcpu≤8 left with `# no OCI match` |
>
> ### Files Created (not in plan)
>
> - `sct_sizing.py` — extracted sizing CLI (plan had it in `sct.py`)
> - `data/instance_catalog/sizing_config.yaml` — role constraints, preferred families, sort order
> - `sdcm/utils/instance_catalog.py` — catalog model + loader (as planned)
> - `sdcm/utils/instance_matcher.py` — constraint parser + matching engine (as planned)
> - `sdcm/utils/catalog_generator.py` — catalog generators for all 4 clouds (as planned)
>
> ### Test Coverage
>
> - `unit_tests/unit/test_instance_matcher.py` — 40 test functions
> - `unit_tests/unit/test_instance_catalog.py` — 14 test functions
> - `unit_tests/integration/test_catalog_generator.py` — integration tests for API generation

## TL;DR

> **Quick Summary**: Replace T-shirt sizing (`db_instance_type: '2xlarge'`) with constraint-based selection (`db_instance_type: {vcpu: 16, memory: ">20gb"}`). An instance catalog per cloud (populated from APIs + curated) is matched against user constraints, selecting the best instance from preferred families by price.
>
> **Deliverables**:
> - Instance catalog module with per-cloud type specs (vcpu/mem/disk/price)
> - Catalog generation script querying AWS/GCE/Azure/OCI pricing APIs
> - Constraint parser supporting exact, range, and comparison operators
> - Matcher selecting best instance per role/cloud from catalog
> - Updated `sct_config.py` wiring (backward-compat: literal strings still passthrough)
> - CLI: `sct.py show-prices`, updated `show-sizes`/`translate-size`
> - Migration of existing test configs
> - User-facing documentation
>
> **Estimated Effort**: Large
> **Parallel Execution**: YES — 4 waves
> **Critical Path**: Task 1 → Task 3 → Task 6 → Task 9 → Task 11 → Final

---

## Context

### Original Request
Replace the T-shirt sizing system (roydahan/pehala review feedback on PR #14490) with a constraint-based selector where users specify requirements (vcpu, memory, disk) and the system resolves to the best matching instance type per cloud provider.

### Interview Summary
**Key Discussions**:
- T-shirt sizes removed entirely; constraint-based only
- YAML dict syntax in configs, dot-notation for env vars (`SCT_DB_INSTANCE_TYPE.vcpu=16`)
- Catalog: generated from live cloud APIs + manual curations, current families only
- Selection: preferred family per role (price-based), with CLI price display
- Constraints: exact (`vcpu: 16`), ranges (`vcpu: "8-16"`, `memory: "10gb-30gb"`), comparisons (`memory: ">20gb"`), plain int for memory/disk means minimum GB, arch (`arch: "arm64"` or `arch: "x86"` — optional, overrides cloud default)

**Research Findings**:
- Existing `InstanceSpec` already has vcpus, memory_gb, local_ssd_count
- `_resolve_instance_sizes()` in sct_config.py handles resolution at config-load time
- PR #14490 has 3 commits implementing T-shirt system (to be replaced)
- AWS Pricing API accessible from us-east-1; GCE uses `gcloud compute machine-types list`; Azure has Retail Prices REST API

### Metis Review
**Identified Gaps** (addressed):
- Backward compat for literal strings: added as passthrough detection
- Docker/baremetal backends: silently skip constraint resolution
- No-match error handling: explicit error message naming unsatisfied constraint
- Multi-role (oracle/zero_token): all roles get constraint syntax
- Determinism: same constraints + catalog → always same instance (no randomness)
- Local disk as implicit constraint for db role: YES, db role implies `local_disk: true`
- Tie-breaking: preferred family first, then cheapest within family

---

## Work Objectives

### Core Objective
Enable cloud-agnostic instance selection via hardware constraints (vcpu, memory, disk) instead of arbitrary T-shirt size names, with automated catalog generation and price-based selection.

### Concrete Deliverables
- `sdcm/utils/instance_catalog.py` — catalog data model + loader
- `sdcm/utils/instance_matcher.py` — constraint parser + matching engine
- `sdcm/utils/catalog_generator.py` — script to populate catalog from cloud APIs
- `data/instance_catalog/` — generated + curated catalog files (YAML per cloud)
- Updated `sdcm/sct_config.py` — wiring constraint resolution
- Updated `sct.py` — CLI commands
- Updated test configs — migrated from T-shirt to constraints
- `docs/cross-cloud-sizing.md` — updated user guide

### Definition of Done
- [x] `{vcpu: 8, memory: ">16gb"}` on AWS resolves to `i8g.2xlarge` (ARM preferred) and all downstream works
- [x] `SCT_DB_INSTANCE_TYPE.vcpu=16` and `SCT_DB_INSTANCE_TYPE.memory=32` env vars parse correctly via dot-notation
- [x] `SCT_DB_INSTANCE_TYPE.vcpu=8` with `SCT_DB_INSTANCE_TYPE.arch=x86` forces x86 on AWS (overrides arm64 default)
- [x] `instance_type_db: 'i4i.large'` still works (literal passthrough)
- [x] `sct.py show-prices --cloud aws --role db` shows pricing table
- [x] 100% unit test coverage on matcher logic
- [x] Integration tests verify API-based catalog generation

### Must Have
- Backward compatibility: literal instance type strings still work as passthrough
- All 5 roles supported: db, db_oracle, zero_token, loader, monitor
- vcpu is mandatory in constraint dicts
- Constraint types: exact value, min (> or plain int), max (<), range (min-max), arch selector (arm64|arm|x86_64|x86)
- vcpu supports range syntax (`"8-16"`)
- Plain int for memory/disk means minimum GB
- Deterministic selection (same input → same output)
- Docker/baremetal backends skip resolution with info log
- Clear error messages when no instance matches

### Must NOT Have (Guardrails)
- No spot/preemptible pricing — on-demand only
- No cross-region auto-selection — region is fixed by other config
- No GPU/accelerator constraints
- No auto-detection of instance availability in region (future work)
- No new cloud API dependencies in test runtime hot path — catalog loaded at startup only
- No changes to downstream provisioning code — output is still a string instance type

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed.

### Test Decision
- **Infrastructure exists**: YES (pytest + unit_tests/)
- **Automated tests**: TDD
- **Framework**: pytest (existing)
- **Split**: Unit tests for parsing/matching logic, integration tests for API catalog generation

### QA Policy
Every task includes agent-executed QA scenarios.
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Library/Module**: Use Bash (python REPL) — import, call functions, compare output
- **CLI**: Use interactive_bash (tmux) — run command, validate output
- **API/Backend**: Use Bash (curl/python) — call cloud APIs, verify catalog generation

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Foundation — no dependencies):
├── Task 1: Instance catalog data model [quick]
├── Task 2: Constraint parser module [deep]
├── Task 3: Catalog file format + loader [quick]
├── Task 4: Preferred families config [quick]

Wave 2 (Core logic — depends on Wave 1):
├── Task 5: Matching engine (depends: 1, 2, 4) [deep]
├── Task 6: Catalog generator — AWS (depends: 1, 3) [unspecified-high]
├── Task 7: Catalog generator — GCE (depends: 1, 3) [unspecified-high]
├── Task 8: Catalog generator — Azure + OCI (depends: 1, 3) [unspecified-high]

Wave 3 (Integration — depends on Wave 2):
├── Task 9: Wire into sct_config.py (depends: 5) [deep]
├── Task 10: CLI commands (depends: 5, 6-8) [unspecified-high]
├── Task 11: Migration of test configs (depends: 9) [quick]

Wave 4 (Docs + cleanup):
├── Task 12: User documentation (depends: 9, 10) [writing]
├── Task 13: Remove old T-shirt sizing code (depends: 11) [quick]

Wave FINAL (4 parallel reviews):
├── F1: Plan compliance audit (oracle)
├── F2: Code quality review (unspecified-high)
├── F3: Real QA execution (unspecified-high)
├── F4: Scope fidelity check (deep)
→ Present results → Get explicit user okay
```

### Dependency Matrix

| Task | Depends On | Blocks | Wave |
|------|-----------|--------|------|
| 1 | — | 5, 6, 7, 8 | 1 |
| 2 | — | 5 | 1 |
| 3 | — | 6, 7, 8 | 1 |
| 4 | — | 5 | 1 |
| 5 | 1, 2, 4 | 9, 10 | 2 |
| 6 | 1, 3 | 10 | 2 |
| 7 | 1, 3 | 10 | 2 |
| 8 | 1, 3 | 10 | 2 |
| 9 | 5 | 11 | 3 |
| 10 | 5, 6-8 | 12 | 3 |
| 11 | 9 | 12, 13 | 3 |
| 12 | 9, 10 | — | 4 |
| 13 | 11 | — | 4 |

### Agent Dispatch Summary

- **Wave 1**: 4 tasks — T1,T3,T4 → `quick`, T2 → `deep`
- **Wave 2**: 4 tasks — T5 → `deep`, T6-T8 → `unspecified-high`
- **Wave 3**: 3 tasks — T9 → `deep`, T10 → `unspecified-high`, T11 → `quick`
- **Wave 4**: 2 tasks — T12 → `writing`, T13 → `quick`
- **FINAL**: 4 tasks — F1 → `oracle`, F2-F3 → `unspecified-high`, F4 → `deep`

---

## TODOs

- [ ] 1. Instance catalog data model

  **What to do**:
  - Create `sdcm/utils/instance_catalog.py`
  - Define `@dataclass InstanceTypeInfo`: instance_type (str), cloud (str), family (str), vcpus (int), memory_gb (float), local_disk_gb (float), local_disk_count (int), arch (str: "x86_64"|"arm64"), price_per_hour (float|None)
  - Define `InstanceCatalog` class: loads from YAML files, provides `query(cloud, **constraints) -> list[InstanceTypeInfo]`
  - Define catalog file format: `data/instance_catalog/{cloud}.yaml` — list of instance type entries

  **Must NOT do**:
  - No cloud API calls in this task — just the data model and file loader
  - No matching logic — that's Task 5

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 2, 3, 4)
  - **Blocks**: Tasks 5, 6, 7, 8
  - **Blocked By**: None

  **References**:
  - `sdcm/utils/cloud_sizes.py:42-70` — existing InstanceSpec dataclass to evolve from
  - `defaults/` — YAML file loading patterns used in SCT

  **Acceptance Criteria**:
  - [ ] Test file: `unit_tests/unit/test_instance_catalog.py`
  - [ ] `pytest unit_tests/unit/test_instance_catalog.py` → PASS

  **QA Scenarios**:
  ```
  Scenario: Load catalog from YAML file
    Tool: Bash (python)
    Steps:
      1. Create temp YAML with 3 instance entries
      2. Load with InstanceCatalog.from_file(path)
      3. Assert len(catalog.instances) == 3
      4. Assert first entry has correct vcpus/memory
    Expected Result: All fields correctly deserialized
    Evidence: .sisyphus/evidence/task-1-load-catalog.txt

  Scenario: Empty catalog file
    Tool: Bash (python)
    Steps:
      1. Load empty YAML file
      2. Assert catalog.instances == []
    Expected Result: No error, empty list
    Evidence: .sisyphus/evidence/task-1-empty-catalog.txt
  ```

  **Commit**: YES
  - Message: `feature(sizing): add instance catalog data model`
  - Files: `sdcm/utils/instance_catalog.py`, `unit_tests/unit/test_instance_catalog.py`

- [ ] 2. Constraint parser module

  **What to do**:
  - Create constraint parsing in `sdcm/utils/instance_matcher.py`
  - Parse dict format: `{"vcpu": 16, "memory": 32, "disk": 500, "arch": "arm64"}`
  - Plain int for memory/disk means minimum GB (e.g. `memory: 32` → `memory >= 32 GB`)
  - vcpu is **mandatory** — raise ValueError if missing
  - vcpu supports exact int (`8`) or range string (`"8-16"`)
  - Supported operators for memory/disk: exact (`32`), gt (`">32"`), lt (`"<32"`), gte (`">=32"`), range (`"500-2048"`)
  - Arch accepts `arm64`, `arm` (→arm64), `x86_64`, `x86` (→x86_64) — optional, uses cloud default if omitted
  - Output: list of `Constraint(field, operator, value)` objects
  - Handle edge cases: whitespace, missing fields (except vcpu)
  - **No flat string parser** — env vars use SCT's native dot-notation (`SCT_DB_INSTANCE_TYPE.vcpu=8`)

  **Must NOT do**:
  - No matching against catalog — just parsing
  - No sct_config integration
  - No flat string parser — dot-notation handles env vars natively

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 3, 4)
  - **Blocks**: Task 5
  - **Blocked By**: None

  **References**:
  - No direct codebase reference — new module
  - Python `re` module for parsing expressions

  **Acceptance Criteria**:
  - [ ] Test file: `unit_tests/unit/test_instance_matcher.py`
  - [ ] `pytest unit_tests/unit/test_instance_matcher.py -k parse` → PASS (20+ parametrized cases)

  **QA Scenarios**:
  ```
  Scenario: Parse YAML dict constraints
    Tool: Bash (python)
    Steps:
      1. parse_constraints({"vcpu": 16, "memory": 32})
      2. Assert 2 constraints returned
      3. Assert vcpu constraint: field="vcpus", op="eq", value=16
      4. Assert memory constraint: field="memory_gb", op="gte", value=32
    Expected Result: Correct Constraint objects, plain int = minimum
    Evidence: .sisyphus/evidence/task-2-parse-dict.txt

  Scenario: Parse vcpu range
    Tool: Bash (python)
    Steps:
      1. parse_constraints({"vcpu": "8-16", "memory": 32})
      2. Assert vcpu constraint: field="vcpus", op="range", value=(8, 16)
    Expected Result: Range parsed correctly
    Evidence: .sisyphus/evidence/task-2-parse-range.txt

  Scenario: Missing vcpu raises ValueError
    Tool: Bash (python)
    Steps:
      1. parse_constraints({"memory": 32}) → expect ValueError("vcpu is mandatory")
    Expected Result: Clear error message
    Evidence: .sisyphus/evidence/task-2-missing-vcpu.txt

  Scenario: Arch shorthands normalized
    Tool: Bash (python)
    Steps:
      1. parse_constraints({"vcpu": 8, "arch": "arm"})
      2. Assert arch constraint value == "arm64"
      3. parse_constraints({"vcpu": 8, "arch": "x86"})
      4. Assert arch constraint value == "x86_64"
    Expected Result: Shorthands expanded
    Evidence: .sisyphus/evidence/task-2-arch-shorthands.txt
  ```

  **Commit**: YES
  - Message: `feature(sizing): add constraint parser with range and arch shorthand support`
  - Files: `sdcm/utils/instance_matcher.py`, `unit_tests/unit/test_instance_matcher.py`

- [ ] 3. Catalog file format and loader

  **What to do**:
  - Create `data/instance_catalog/` directory
  - Create initial curated catalog files: `aws.yaml`, `gce.yaml`, `azure.yaml`, `oci.yaml`
  - Populate with current families we use (from existing SIZING dict data):
    - AWS: i8g, i7i, i4i, c6i, m6i, t3
    - GCE: z3-highmem, e2-standard, e2-highcpu, n2-highmem
    - Azure: Standard_L*s_v4, Standard_F*s_v2, Standard_D*_v4
    - OCI: DenseIO.E5.Flex, VM.Standard3.Flex, VM.Standard.E4.Flex
  - Each entry: instance_type, family, vcpus, memory_gb, local_disk_gb, local_disk_count, arch, price_per_hour (null initially)
  - Add `preferred_families` section per role in each file

  **Must NOT do**:
  - No API calls — manual curation from existing SIZING dict
  - No pricing data yet (null/0) — filled by Task 6-8

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 4)
  - **Blocks**: Tasks 6, 7, 8
  - **Blocked By**: None

  **References**:
  - `sdcm/utils/cloud_sizes.py:80-220` — existing SIZING dict with all instance types and specs

  **Acceptance Criteria**:
  - [ ] `data/instance_catalog/aws.yaml` has all i8g/i7i/i4i/c6i/m6i/t3 entries
  - [ ] YAML files are valid and loadable by Task 1's InstanceCatalog

  **QA Scenarios**:
  ```
  Scenario: Catalog files are valid YAML
    Tool: Bash (python)
    Steps:
      1. yaml.safe_load each file in data/instance_catalog/
      2. Assert no parse errors
      3. Assert each has "instances" key with list
    Expected Result: All 4 files load cleanly
    Evidence: .sisyphus/evidence/task-3-valid-yaml.txt
  ```

  **Commit**: YES
  - Message: `feature(sizing): add curated instance catalog data files`
  - Files: `data/instance_catalog/*.yaml`

- [ ] 4. Preferred families configuration

  **What to do**:
  - Define preferred family ordering per role per cloud in catalog files or separate config
  - Structure: `{role: {cloud: [family1, family2, ...]}}` — ordered by preference (first = best)
  - Defaults:
    - db/aws: ["i8g", "i7i", "i4i"] (ARM preferred, x86 fallback via i7i then i4i)
    - db/gce: ["z3-highmem"]
    - db/azure: ["Standard_L"]
    - db/oci: ["DenseIO.E5"]
    - loader/aws: ["c6i"]
    - loader/gce: ["e2-standard", "e2-highcpu"]
    - monitor/aws: ["t3", "m6i"]
  - Include arch preference per cloud (aws defaults to arm64, others x86_64)

  **Must NOT do**:
  - No matching logic — just the config data

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 3)
  - **Blocks**: Task 5
  - **Blocked By**: None

  **References**:
  - `sdcm/utils/cloud_sizes.py:80-90` — current family choices per role

  **Acceptance Criteria**:
  - [ ] Config loadable from file
  - [ ] All current roles/clouds have family preferences defined

  **QA Scenarios**:
  ```
  Scenario: Load preferred families
    Tool: Bash (python)
    Steps:
      1. Load config
      2. Assert db/aws first family is "i8g"
      3. Assert loader/aws first family is "c6i"
    Expected Result: Correct ordering
    Evidence: .sisyphus/evidence/task-4-families.txt
  ```

  **Commit**: grouped with Task 3

- [ ] 5. Matching engine

  **What to do**:
  - In `sdcm/utils/instance_matcher.py`, add `select_instance(catalog, role, cloud, constraints, arch=None) -> InstanceTypeInfo`
  - Algorithm:
    1. Filter catalog by cloud
    2. Filter by arch: if `arch` constraint specified, use it; otherwise use cloud default (aws=arm64, others=x86_64)
    3. Filter by preferred families for the role (ordered)
    4. Apply constraint filters (vcpu, memory, disk)
    5. Sort remaining by: family preference order → price → vcpu (ascending)
    6. Return first match (deterministic)
  - If no match: raise `NoMatchingInstanceError` with details of unsatisfied constraints
  - For db role: implicitly add `local_disk_count > 0` unless user explicitly sets `local_disk: false`

  **Must NOT do**:
  - No sct_config wiring — standalone module
  - No cloud API calls

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: Tasks 9, 10
  - **Blocked By**: Tasks 1, 2, 4

  **References**:
  - `sdcm/utils/cloud_sizes.py:240-280` — existing `resolve_size()` for comparison
  - `sdcm/utils/instance_catalog.py` (Task 1) — data model
  - `sdcm/utils/instance_matcher.py` (Task 2) — constraint parser

  **Acceptance Criteria**:
  - [ ] `pytest unit_tests/unit/test_instance_matcher.py -k select` → PASS (15+ cases)
  - [ ] Deterministic: same input always same output
  - [ ] NoMatchingInstanceError raised with descriptive message

  **QA Scenarios**:
  ```
  Scenario: Select db instance on AWS with vcpu constraint
    Tool: Bash (python)
    Steps:
      1. Load catalog with i8g.large(2vcpu), i8g.2xlarge(8vcpu), i8g.4xlarge(16vcpu)
      2. select_instance(catalog, "db", "aws", {"vcpu": 8})
      3. Assert result.instance_type == "i8g.2xlarge"
    Expected Result: Exact match returned
    Evidence: .sisyphus/evidence/task-5-select-exact.txt

  Scenario: Range constraint selects smallest fitting
    Tool: Bash (python)
    Steps:
      1. select_instance(catalog, "db", "aws", {"vcpu": ">4", "memory": "16gb-64gb"})
      2. Assert result is i8g.2xlarge (8vcpu, 64gb) — smallest that fits
    Expected Result: Smallest instance satisfying all constraints
    Evidence: .sisyphus/evidence/task-5-select-range.txt

  Scenario: No match raises clear error
    Tool: Bash (python)
    Steps:
      1. select_instance(catalog, "db", "aws", {"vcpu": 1024})
      2. Expect NoMatchingInstanceError
      3. Assert error message contains "vcpu: 1024"
    Expected Result: Descriptive error
    Evidence: .sisyphus/evidence/task-5-no-match.txt
  ```

  **Commit**: YES
  - Message: `feature(sizing): add instance matching engine with price-based selection`
  - Files: `sdcm/utils/instance_matcher.py`, `unit_tests/unit/test_instance_matcher.py`

- [ ] 6. Catalog generator — AWS

  **What to do**:
  - Create `sdcm/utils/catalog_generator.py` with `generate_aws_catalog(families, region) -> list[InstanceTypeInfo]`
  - Use `boto3` `describe_instance_types` to get vcpu/memory/disk specs
  - Use AWS Pricing API (`get_products`) to get on-demand hourly pricing
  - Filter to specified families only (i8g, i7i, i4i, c6i, m6i, t3)
  - Output: write `data/instance_catalog/aws.yaml`
  - CLI entry: `sct.py update-catalog --cloud aws`

  **Must NOT do**:
  - No matching logic
  - No GCE/Azure/OCI — separate tasks

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5, 7, 8)
  - **Blocks**: Task 10
  - **Blocked By**: Tasks 1, 3

  **References**:
  - `sdcm/utils/aws_utils.py` — existing boto3 usage patterns
  - AWS Pricing API: `us-east-1` region required

  **Acceptance Criteria**:
  - [ ] Integration test: `unit_tests/integration/test_catalog_generator.py::test_aws`
  - [ ] Generated YAML matches expected format

  **QA Scenarios**:
  ```
  Scenario: Generate AWS catalog for i8g family
    Tool: Bash (python)
    Preconditions: AWS credentials available
    Steps:
      1. generate_aws_catalog(["i8g"], region="us-east-1")
      2. Assert result contains i8g.large, i8g.xlarge, etc.
      3. Assert each has vcpus > 0, memory_gb > 0, price_per_hour > 0
    Expected Result: Valid catalog entries with pricing
    Evidence: .sisyphus/evidence/task-6-aws-catalog.txt
  ```

  **Commit**: YES
  - Message: `feature(sizing): add AWS catalog generator with pricing`
  - Files: `sdcm/utils/catalog_generator.py`, `unit_tests/integration/test_catalog_generator.py`

- [ ] 7. Catalog generator — GCE

  **What to do**:
  - Add `generate_gce_catalog(families, project, zone) -> list[InstanceTypeInfo]`
  - Use `gcloud compute machine-types list` or `google-cloud-compute` Python SDK
  - Use GCE Billing API or `gcloud billing` for pricing
  - Filter to: z3-highmem, e2-standard, e2-highcpu, n2-highmem
  - Output: `data/instance_catalog/gce.yaml`

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5, 6, 8)
  - **Blocks**: Task 10
  - **Blocked By**: Tasks 1, 3

  **References**:
  - `sdcm/utils/gce_utils.py` — existing GCE patterns

  **Acceptance Criteria**:
  - [ ] Integration test: `test_catalog_generator.py::test_gce`

  **QA Scenarios**:
  ```
  Scenario: Generate GCE catalog for z3-highmem
    Tool: Bash (python)
    Preconditions: GCE credentials available
    Steps:
      1. generate_gce_catalog(["z3-highmem"], project="...", zone="us-east1-b")
      2. Assert z3-highmem-4, z3-highmem-8, etc. present
      3. Assert local_disk_count > 0 for z3 family
    Expected Result: Valid entries with disk info
    Evidence: .sisyphus/evidence/task-7-gce-catalog.txt
  ```

  **Commit**: grouped with Task 6

- [ ] 8. Catalog generator — Azure + OCI

  **What to do**:
  - Add `generate_azure_catalog(families, region)` — Azure Retail Prices REST API (no SDK needed)
  - Add `generate_oci_catalog(families, compartment)` — OCI SDK or REST
  - Azure families: Standard_L, Standard_F, Standard_D
  - OCI families: DenseIO.E5, VM.Standard3, VM.Standard.E4
  - Output: `data/instance_catalog/azure.yaml`, `data/instance_catalog/oci.yaml`

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 5, 6, 7)
  - **Blocks**: Task 10
  - **Blocked By**: Tasks 1, 3

  **References**:
  - `sdcm/utils/azure_utils.py` — existing Azure patterns
  - Azure Retail Prices: `https://prices.azure.com/api/retail/prices`

  **Acceptance Criteria**:
  - [ ] Integration test: `test_catalog_generator.py::test_azure`, `test_catalog_generator.py::test_oci`

  **QA Scenarios**:
  ```
  Scenario: Generate Azure catalog
    Tool: Bash (python)
    Steps:
      1. generate_azure_catalog(["Standard_L"], region="eastus")
      2. Assert Standard_L4s_v4, Standard_L8s_v4, etc. present
    Expected Result: Valid entries with pricing
    Evidence: .sisyphus/evidence/task-8-azure-catalog.txt
  ```

  **Commit**: grouped with Task 6

- [ ] 9. Wire into sct_config.py

  **What to do**:
  - Modify `_resolve_instance_sizes()` to detect constraint syntax:
    - If value is a `dict` → parse as constraints, call `select_instance()`
    - If value is a `str` that looks like an instance type (contains `.` or `-`) → passthrough (backward compat)
    - If backend is docker/baremetal → skip resolution with info log
    - Env var dot-notation (`SCT_DB_INSTANCE_TYPE.vcpu=8`) already builds dicts natively — no custom parsing needed
  - Load `InstanceCatalog` at config init (from `data/instance_catalog/`)
  - Resolved instance type flows into existing params (`instance_type_db`, `gce_instance_type_db`, etc.)
  - All 5 roles: db, db_oracle, zero_token, loader, monitor

  **Must NOT do**:
  - No changes to downstream provisioning — output is still a string
  - No changes to `verify_configuration()` beyond validating constraint syntax

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 3
  - **Blocks**: Tasks 11, 12
  - **Blocked By**: Task 5

  **References**:
  - `sdcm/sct_config.py:3076-3151` — existing `_resolve_instance_sizes()` to replace
  - `sdcm/sct_config.py:3090-3105` — `_primary_param_overrides` for zero_token naming

  **Acceptance Criteria**:
  - [ ] `pytest unit_tests/unit/test_config.py -k resolve_instance` → PASS
  - [ ] Literal string `instance_type_db: 'i4i.large'` still works unchanged
  - [ ] Dict `db_instance_type: {vcpu: 8}` resolves to correct type

  **QA Scenarios**:
  ```
  Scenario: Constraint dict resolves to correct instance
    Tool: Bash (python)
    Steps:
      1. Create config with db_instance_type: {vcpu: 8, memory: ">16gb"}
      2. Load SCTConfiguration with backend=aws
      3. Assert self["instance_type_db"] == "i8g.2xlarge"
    Expected Result: Resolved to ARM 8-vcpu instance
    Evidence: .sisyphus/evidence/task-9-constraint-resolve.txt

  Scenario: Literal string passthrough
    Tool: Bash (python)
    Steps:
      1. Create config with db_instance_type: "i4i.large"
      2. Load SCTConfiguration with backend=aws
      3. Assert self["instance_type_db"] == "i4i.large"
    Expected Result: No catalog lookup, direct passthrough
    Evidence: .sisyphus/evidence/task-9-passthrough.txt

  Scenario: Docker backend skips resolution
    Tool: Bash (python)
    Steps:
      1. Create config with db_instance_type: {vcpu: 8}, backend=docker
      2. Load SCTConfiguration
      3. Assert no error, instance_type_db unchanged
    Expected Result: Silent skip
    Evidence: .sisyphus/evidence/task-9-docker-skip.txt
  ```

  **Commit**: YES
  - Message: `feature(sizing): wire constraint-based selector into sct_config`
  - Files: `sdcm/sct_config.py`, `unit_tests/unit/test_config.py`

- [ ] 10. CLI commands

  **What to do**:
  - Add `sct.py show-prices --cloud <cloud> --role <role> [--family <family>]` — tabular pricing display
  - Add `sct.py update-catalog [--cloud <cloud>]` — runs catalog generator scripts
  - Update `sct.py show-sizes` to show constraint-based entries
  - Update `sct.py translate-size` to accept constraint format and show resolved types per cloud

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Task 9 if no dependency conflicts)
  - **Parallel Group**: Wave 3
  - **Blocks**: Task 12
  - **Blocked By**: Tasks 5, 6-8

  **References**:
  - `sct.py:2774-2900` — existing `show-sizes` and `translate-size` commands

  **Acceptance Criteria**:
  - [ ] `sct.py show-prices --cloud aws --role db` outputs table with price column
  - [ ] `sct.py update-catalog --cloud aws` generates valid catalog file

  **QA Scenarios**:
  ```
  Scenario: show-prices displays table
    Tool: interactive_bash (tmux)
    Steps:
      1. Run: uv run sct.py show-prices --cloud aws --role db
      2. Assert output contains "i8g" and "$" price column
    Expected Result: Formatted table with pricing
    Evidence: .sisyphus/evidence/task-10-show-prices.txt
  ```

  **Commit**: YES
  - Message: `feature(sizing): add show-prices CLI and update show-sizes/translate-size`
  - Files: `sct.py`

- [ ] 11. Migrate test configs

  **What to do**:
  - Update longevity test configs (12 files in `test-cases/longevity/`) from T-shirt to constraint syntax
  - Map: `db_instance_type: '2xlarge'` → `db_instance_type: {vcpu: 8, memory: ">16gb"}`
  - Map: `loader_instance_type: 'small'` → `loader_instance_type: {vcpu: 4}`
  - Map: `monitor_instance_type: 'small'` → `monitor_instance_type: {vcpu: 2}`
  - Verify each migrated config resolves to same instance types as before

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 3
  - **Blocks**: Tasks 12, 13
  - **Blocked By**: Task 9

  **References**:
  - `test-cases/longevity/` — 12 migrated config files

  **Acceptance Criteria**:
  - [ ] All configs resolve to same instance types as the old T-shirt mapping
  - [ ] `sct.py translate-size --config <file> --backend aws` shows expected types

  **QA Scenarios**:
  ```
  Scenario: Migrated config resolves identically
    Tool: Bash
    Steps:
      1. For each migrated file, run translate-size and capture output
      2. Compare against expected instance types from old SIZING dict
    Expected Result: 1:1 match
    Evidence: .sisyphus/evidence/task-11-migration-verify.txt
  ```

  **Commit**: YES
  - Message: `refactor(sizing): migrate test configs from T-shirt to constraint syntax`
  - Files: `test-cases/longevity/*.yaml`

- [ ] 12. User documentation

  **What to do**:
  - Rewrite `docs/cross-cloud-sizing.md` for constraint-based system
  - Sections: Overview, Syntax (YAML dict + env var), Constraint types, Selection algorithm, CLI tools, Examples, Migration guide
  - Include mapping table: "old T-shirt → new constraints"
  - Include **Demo & Quick Start** section:
    - Minimal example: `db_instance_type: {vcpu: 8}` → what happens on each cloud
    - Env var example: `export SCT_DB_INSTANCE_TYPE='vcpu=16,memory=>32gb'`
    - Arch override example: `db_instance_type: {vcpu: 8, arch: "x86_64"}` forces i4i on AWS
    - Multi-role example showing db + loader + monitor in one config file
    - `sct.py show-prices --cloud aws --role db` output screenshot/example
    - `sct.py translate-size --config my-test.yaml` showing per-cloud resolution
    - Error case: what happens when no instance matches (error message example)
  - Include **Constraint Reference** section:
    - Table of all constraint fields: vcpu, memory, disk, arch
    - Operators per field: exact, >, <, >=, range
    - Units: gb, tb (auto-converted)
    - Defaults when omitted (arch per cloud, local_disk for db role)
  - Include **Selection Algorithm** section (user-facing explanation):
    - How preferred families work
    - How tie-breaking works (family order → price → vcpu ascending)
    - How to inspect what was selected: `translate-size` output
  - Include **Catalog Management** section:
    - How to update catalog: `sct.py update-catalog`
    - Where catalog files live: `data/instance_catalog/`
    - How to add a new instance family manually

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Task 13)
  - **Parallel Group**: Wave 4
  - **Blocks**: None
  - **Blocked By**: Tasks 9, 10

  **Acceptance Criteria**:
  - [ ] Doc covers all constraint types with examples
  - [ ] Migration table present

  **Commit**: YES
  - Message: `docs(sizing): update cross-cloud sizing guide for constraint-based selection`
  - Files: `docs/cross-cloud-sizing.md`

- [ ] 13. Remove old T-shirt sizing code

  **What to do**:
  - Remove `SIZING` dict from `sdcm/utils/cloud_sizes.py`
  - Remove `resolve_size()`, `identify_size()`, `SizeMapping` — replaced by catalog + matcher
  - Keep `get_cloud_params()` if still needed, or inline into new matcher
  - Update imports across codebase
  - Remove old `unit_tests/unit/test_cloud_sizes.py`

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Task 12)
  - **Parallel Group**: Wave 4
  - **Blocks**: None
  - **Blocked By**: Task 11

  **Acceptance Criteria**:
  - [ ] No references to `resolve_size` or `SIZING` remain
  - [ ] All tests pass after removal

  **Commit**: YES
  - Message: `refactor(sizing): remove deprecated T-shirt sizing code`
  - Files: `sdcm/utils/cloud_sizes.py` (gutted), `unit_tests/unit/test_cloud_sizes.py` (removed)

---

## Final Verification Wave

- [ ] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists. For each "Must NOT Have": search codebase for forbidden patterns. Check evidence files exist.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | VERDICT`

- [ ] F2. **Code Quality Review** — `unspecified-high`
  Run ruff + pytest. Review all changed files for type safety, error handling, unused imports. Check no AI slop.
  Output: `Build [PASS/FAIL] | Tests [N pass/N fail] | VERDICT`

- [ ] F3. **Real QA** — `unspecified-high`
  Execute every QA scenario from every task. Test cross-task integration. Save evidence.
  Output: `Scenarios [N/N pass] | VERDICT`

- [ ] F4. **Scope Fidelity Check** — `deep`
  Verify 1:1 match between plan spec and actual implementation. Flag scope creep.
  Output: `Tasks [N/N compliant] | VERDICT`

---

## Commit Strategy

- **1**: `feature(sizing): add instance catalog data model` — instance_catalog.py
- **2**: `feature(sizing): add constraint parser with range/comparison support` — instance_matcher.py (parser part)
- **3**: `feature(sizing): add instance matching engine with price-based selection` — instance_matcher.py (matcher part)
- **4**: `feature(sizing): add catalog generation scripts for AWS/GCE/Azure/OCI` — catalog_generator.py + data/
- **5**: `feature(sizing): wire constraint-based selector into sct_config` — sct_config.py
- **6**: `feature(sizing): add show-prices CLI and update show-sizes/translate-size` — sct.py
- **7**: `refactor(sizing): migrate test configs from T-shirt to constraint syntax` — test-cases/
- **8**: `docs(sizing): update cross-cloud sizing guide for constraint-based selection` — docs/
- **9**: `refactor(sizing): remove deprecated T-shirt sizing code` — cloud_sizes.py cleanup

---

## Success Criteria

### Verification Commands
```bash
python -m pytest unit_tests/unit/test_instance_catalog.py -v  # catalog model tests
python -m pytest unit_tests/unit/test_instance_matcher.py -v  # parser + matcher tests
python -m pytest unit_tests/unit/test_config.py -v  # integration with sct_config
python -m pytest unit_tests/integration/test_catalog_generator.py -v  # API tests
uv run sct.py show-prices --cloud aws --role db  # CLI works
```

### Final Checklist
- [x] All "Must Have" present
- [x] All "Must NOT Have" absent
- [x] All tests pass
- [x] Literal string backward compat verified
- [x] All 5 roles work with constraint syntax
- [x] Docker backend silently skips resolution
