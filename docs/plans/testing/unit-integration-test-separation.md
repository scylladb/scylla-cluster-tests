---
status: in_progress
domain: testing
created: 2026-03-23
last_updated: 2026-03-23
owner: null
---

# Unit/Integration Test Directory Separation Plan

## Problem Statement

SCT's 1697 tests (1487 unit, 210 integration) live in a flat `unit_tests/` directory. Unit and
integration tests are separated by `@pytest.mark.integration` markers, but they share a single
`conftest.py` that must accommodate both test types. This causes fixture-scoping problems:

1. **Fake fixtures leak into integration tests.** `unit_tests/conftest.py:338` defines
   `fake_remoter` as `autouse=True`, which activates for every test — including integration tests
   that need real SSH connections. A `pytest_collection_modifyitems` hook (line 473) selectively
   injects `mock_cloud_services` only for non-integration tests, but this is fragile and
   non-obvious.

2. **Docker fixtures pollute unit test collection.** Docker fixtures (`docker_scylla`,
   `docker_scylla_2`, `docker_vector_store`) at lines 230–335 import Docker-related modules
   (`RemoteDocker`, `VectorStoreSetDocker`, `subprocess`) during collection even when running unit
   tests that never use them.

3. **Mixed fixture scoping is fragile.** The root conftest contains both session-scoped fakes
   (`fake_provisioner`, `fake_region_definition_builder`) and function-scoped Docker fixtures.
   Adding new fixtures requires careful consideration of which test type they target, with no
   structural enforcement.

4. **The `pytest_collection_modifyitems` hook is a workaround.** It exists solely because both
   test types share a conftest. Separating the directories eliminates the need for the hook
   entirely — `mock_cloud_services` becomes a plain `autouse=True` fixture scoped to
   `unit_tests/unit/conftest.py`.

**Markers remain the source of truth** for classifying tests. The directory separation addresses
fixture dependency isolation and eliminates the hook workaround. `sct.py` commands continue using
`-m "not integration"` / `-m "integration"` filtering unchanged.

## Current State

### Test Inventory (verified against master `0a8e8407b`)

| Category | Count | Notes |
|----------|-------|-------|
| Total tests | 1697 | 139 files |
| Unit tests (`-m "not integration"`) | 1487 | 119 files contain at least one unit test |
| Integration tests (`-m "integration"`) | 210 | 29 files contain at least one integration test |
| Purely integration files | 19 | All tests in file are marked integration |
| Mixed files | 10 | Contains both unit and integration tests |
| Purely unit files | ~110 | No integration markers |

### Purely Integration Files (19 files — move directly)

`test_alternator_streams_kcl.py` (1), `test_aws_services.py` (8), `test_base_version.py` (21),
`test_cassandra_harry.py` (1), `test_cassandra_stress_thread.py` (8),
`test_config_get_version_based_on_conf.py` (36), `test_cql_stress_cassandra_stress_thread.py` (3),
`test_gemini_thread.py` (4), `test_kafka.py` (2), `test_ndbench_thread.py` (4),
`test_oci_utils_integration.py` (10), `test_python_driver.py` (2), `test_run_cqlsh.py` (3),
`test_ssh_none_auth.py` (2), `test_sync.py` (1), `test_s3_remote_uploader.py` (1),
`test_utils_database_query_utils.py` (1), `test_utils_issues.py` (31), `test_vector_store.py` (3),
`test_ycsb_thread.py` (4)

### Mixed Files (10 files — need splitting)

| File | Unit | Integration |
|------|------|-------------|
| `test_version_utils.py` | 314 | 12 |
| `test_cluster.py` | 69 | 14 |
| `test_config.py` | 60 | 24 |
| `test_latte_thread.py` | 54 | 4 |
| `test_events.py` | 33 | 1 |
| `test_scylla_bench_thread.py` | 18 | 3 |
| `test_find_ami_equivalent.py` | 6 | 4 |
| `test_dedicated_hosts.py` | 6 | 1 |
| `test_azure_vm_provider_reboot.py` | 3 | 1 |
| `test_base_version.py` | 0 | 21 |

### Key Framework Files

- **`unit_tests/conftest.py`** (508 lines) — Contains all fixtures for both test types:
  - Lines 338–346: `fake_remoter` (autouse, function-scoped) — sets `FakeRemoter` for ALL tests
  - Lines 349–436: `mock_cloud_services` (session-scoped) — mocks AWS/GCE/Azure/KeyStore
  - Lines 439–441: `fake_provisioner` (session, autouse)
  - Lines 444–446: `fake_region_definition_builder` (session, autouse)
  - Lines 473–484: `pytest_collection_modifyitems` — injects `mock_cloud_services` into
    non-integration tests; this hook is the workaround that will be removed
  - Lines 107–110: `events` / `events_function_scope` fixtures (shared)
  - Lines 230–335: Docker fixtures (`docker_scylla`, `docker_scylla_2`, `docker_vector_store`)

- **`sct.py`** (lines 1602–1640) — CLI commands. Will **not** change:
  - `unit-tests`: `pytest unit_tests/ -m "not integration"` with `-n2`
  - `integration-tests`: `pytest unit_tests/ -m "integration"` with `-n4 --dist loadgroup`

- **`unit_tests/lib/`** — Shared fake utilities used by both test types:
  `fake_events.py`, `fake_provisioner.py`, `fake_region_definition_builder.py`, `fake_remoter.py`,
  `alternator_utils.py`, `events_utils.py`, `mock_remoter.py`, `remoter_recorder.py`, `s3_utils.py`

- **`unit_tests/dummy_remote.py`** — `LocalNode` and `LocalScyllaClusterDummy`, used by both test
  types.

- **Subdirectories** (all purely unit tests): `k8s/`, `nemesis/`, `provisioner/`, `rest/`,
  `vector_store/`

- **Data directories** (many tests use `__file__`-relative paths):
  `unit_tests/test_data/`, `unit_tests/test_configs/`

### Stale Artifacts (from previous incomplete attempt)

- `unit_tests/unit/` — Empty subdirectories (k8s, nemesis, provisioner, rest), untracked
- `unit_tests/integration/` — Partial `test_config.py` and `__pycache__`, untracked

## Goals

1. **Eliminate the `pytest_collection_modifyitems` hook** by moving fake fixtures into
   `unit_tests/unit/conftest.py` as `autouse=True`, where conftest scoping enforces the boundary
   structurally.
2. **Isolate Docker fixtures** in `unit_tests/integration/conftest.py` so unit test collection
   never imports Docker-related modules.
3. **Separate test files by type** into `unit_tests/unit/` and `unit_tests/integration/`
   subdirectories, splitting mixed files so each side has clean fixture dependencies.
4. **Keep markers as source of truth** — all `@pytest.mark.integration` markers are preserved;
   `sct.py` commands continue using `-m` filtering unchanged.
5. **Maintain all 1697 tests passing** at every commit — no regressions at any phase.

## Implementation Phases

This plan is implemented as a single PR with one commit per phase. The conftest split (Phase 4)
is deliberately ordered after the file moves (Phases 2–3) so that the hook removal is safe: by
the time the hook is removed, all pure unit and integration files are already in their respective
directories and will receive the correct fixtures via conftest scoping.

---

### Phase 1: Cleanup and Directory Structure

**Importance**: Critical
**Description**: Remove stale artifacts from the previous attempt, create `unit_tests/unit/` and
`unit_tests/integration/` with `__init__.py` files, and create symlinks for `__file__`-relative
path resolution. No test files move yet; no conftest changes yet.

**Deliverables**:
- Stale `unit_tests/unit/` empty dirs and `unit_tests/integration/` removed
- `unit_tests/unit/__init__.py` and `unit_tests/integration/__init__.py` created
- `unit_tests_data_dir` fixture added to `unit_tests/conftest.py` — returns
  `Path(__file__).parent / "test_data"` (i.e., always `unit_tests/test_data/`
  regardless of which subdirectory the calling test lives in). Tests that currently
  use `Path(__file__).parent / "test_data"` should migrate to this fixture; module-level
  usages (constants defined at import time) are updated to use an explicit
  `Path(__file__).parent.parent / "test_data"` (one extra `.parent` to reach
  `unit_tests/` from `unit_tests/unit/` or `unit_tests/integration/`) at move time.

**Definition of Done**:
- [ ] Stale directories removed
- [ ] Both `__init__.py` files exist
- [ ] `unit_tests_data_dir` fixture present in parent `unit_tests/conftest.py`
- [ ] `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/ --collect-only -q`
      collects 1697 tests (unchanged)
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 2: Move Purely Unit Test Files

**Importance**: Critical
**Description**: Move ~110 purely unit test files from `unit_tests/` to `unit_tests/unit/` using
`git mv`. This includes root-level `test_*.py` files with no integration markers, plus
subdirectories `k8s/`, `nemesis/`, `provisioner/`, `rest/`, `vector_store/`. The root conftest
and its hook remain unchanged at this stage — pytest's hierarchical conftest discovery means the
parent conftest still applies to all tests under `unit_tests/`.

This commit will have a large file-count diff but each change is a mechanical `git mv` with no
logic changes. Split into two commits if needed: one for root-level files, one for subdirectories.

Update `sct.py:145` which imports `FakeTester` from `unit_tests.nemesis.fake_cluster` — this
path changes to `unit_tests.unit.nemesis.fake_cluster`.

In the same commit, fix all `__file__`-relative paths in the moved files that pointed at
`unit_tests/test_data/`. Two patterns exist:

- **Fixture-injectable** (function body): migrate to the `unit_tests_data_dir` fixture from
  the shared conftest.
- **Module-level constants** (defined at import time, cannot use fixtures):
  `Path(__file__).parent / "test_data"` becomes
  `Path(__file__).parent.parent / "test_data"`. Affected files:
  `test_scylla_yaml_builders.py` (`BASE_FOLDER`), `test_gemini_thread.py`
  (`GEMINI_SCHEMA_PATH`), `test_aws_services.py` (`MOTO_AMIS_PATH`).

Also fix `__file__`-relative paths that traverse upward past `unit_tests/`:
- `test_clean_k8s_clusters.py`: `parent.parent / "utils"` → `parent.parent.parent / "utils"`
- `provisioner/test_gce_region_definition_builder.py`: `parent.parent.parent / "defaults"` →
  `parent.parent.parent.parent / "defaults"` (moves to `unit_tests/unit/provisioner/`)

**Dependencies**: Phase 1

**Deliverables**:
- All purely-unit test files under `unit_tests/unit/`
- Subdirectories moved: `k8s/`, `nemesis/`, `provisioner/`, `rest/`, `vector_store/`
- `sct.py` FakeTester import updated

**Definition of Done**:
- [ ] All purely-unit files live under `unit_tests/unit/`
- [ ] `sct.py` import updated and verified:
      `python -c "from unit_tests.unit.nemesis.fake_cluster import FakeTester"`
- [ ] All `__file__`-relative paths to `unit_tests/test_data/` updated in moved files
- [ ] `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/ --collect-only -q`
      collects 1697 tests
- [ ] `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/ -m "not integration" --collect-only -q`
      collects 1487 tests
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 3: Move Purely Integration Test Files

**Importance**: Critical
**Description**: Move 19 purely-integration test files from `unit_tests/` to
`unit_tests/integration/` using `git mv`. All `@pytest.mark.integration` markers are preserved.
The root conftest hook still applies at this stage.

**Dependencies**: Phase 1

**Deliverables**:
- 19 files moved to `unit_tests/integration/`

**Definition of Done**:
- [ ] All 19 purely-integration files live under `unit_tests/integration/`
- [ ] All `@pytest.mark.integration` markers preserved
- [ ] `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/ -m "integration" --collect-only -q`
      collects 210 tests
- [ ] Total test count remains 1697
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 4: Split conftest.py and Remove the Hook

**Importance**: Critical
**Description**: Split the 508-line root `conftest.py` into three files and remove the
`pytest_collection_modifyitems` hook. This is safe at this point because all pure unit and
integration files are already in their respective directories.

**Split**:

- **Parent** (`unit_tests/conftest.py`): shared fixtures only — `events`,
  `events_function_scope`, `prom_address`, `params`, `fixture_cleanup_continuous_events_registry`,
  `pytest_sessionfinish`, `pytest_runtest_logreport`, `create_ssl_dir`. The
  `pytest_collection_modifyitems` hook and all fake/Docker fixtures are removed from here.

- **Unit** (`unit_tests/unit/conftest.py`): `fake_remoter` (autouse), `mock_cloud_services`
  (session, **autouse** — no longer needs the hook), `fake_provisioner` (session, autouse),
  `fake_region_definition_builder` (session, autouse). Imports scoped to what these fixtures need.

- **Integration** (`unit_tests/integration/conftest.py`): `docker_scylla`, `docker_scylla_2`,
  `docker_vector_store`, and helper functions `configure_scylla_node`, `mock_remote_scylla_yaml`,
  `create_ssl_dir` (or import from parent). Docker-related imports.

**Dependencies**: Phases 2, 3

**Deliverables**:
- `unit_tests/conftest.py` reduced to shared fixtures (~120 lines)
- `unit_tests/unit/conftest.py` with fake fixtures, `mock_cloud_services` as autouse (~160 lines)
- `unit_tests/integration/conftest.py` with Docker fixtures (~250 lines)
- `pytest_collection_modifyitems` hook deleted

**Definition of Done**:
- [ ] `pytest_collection_modifyitems` hook does not exist anywhere in the codebase
- [ ] `mock_cloud_services` is `autouse=True` in `unit_tests/unit/conftest.py`
- [ ] `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/ -m "not integration" -x -q`
      passes (1487 tests)
- [ ] Integration tests do not receive `fake_remoter` or `mock_cloud_services`
- [ ] `uv run sct.py pre-commit` passes

---

### Phase 5: Split Mixed Files and Update Documentation

**Importance**: Important
**Description**: Split the 10 mixed files so that unit tests live in `unit_tests/unit/` and
integration tests in `unit_tests/integration/`. For each file, use `git mv` for the larger
portion (typically unit) and create a new file for the smaller portion. Shared helpers (classes,
fixtures, constants) stay in the unit file and are imported from the integration file — they are
not duplicated.

**Easy splits** (small integration portion):
- `test_events.py` (33 unit, 1 integration)
- `test_dedicated_hosts.py` (6 unit, 1 integration)
- `test_azure_vm_provider_reboot.py` (3 unit, 1 integration)
- `test_scylla_bench_thread.py` (18 unit, 3 integration)
- `test_latte_thread.py` (54 unit, 4 integration)
- `test_find_ami_equivalent.py` (6 unit, 4 integration)

**Hard splits** (complex sharing):
- `test_version_utils.py` (314 unit, 12 integration) — integration tests call real S3/repos
- `test_config.py` (60 unit, 24 integration) — integration tests use real AMI resolution
- `test_cluster.py` (69 unit, 14 integration) — integration tests use Docker containers
- `test_base_version.py` (0 unit, 21 integration) — move directly to integration/

**Documentation updates**:
- Update `AGENTS.md` test organization section to reflect the new structure
- Update `writing-unit-tests` and `writing-integration-tests` skill files

**Dependencies**: Phase 4

**Deliverables**:
- All 10 mixed files split; original files removed from `unit_tests/`
- Unit parts in `unit_tests/unit/`, integration parts in `unit_tests/integration/`
- Shared helpers accessible via imports, not duplicated
- `AGENTS.md` and skill files updated

**Definition of Done**:
- [ ] No test files remain at `unit_tests/` root level (only conftest, `__init__.py`, `lib/`,
      `dummy_remote.py`, `test_data/`, `test_configs/`)
- [ ] All `@pytest.mark.integration` markers preserved on integration tests
- [ ] Total test count remains 1697
- [ ] No shared test classes duplicated
- [ ] `uv run sct.py unit-tests` and `uv run sct.py integration-tests` work correctly
- [ ] `AGENTS.md` reflects new directory structure
- [ ] `uv run sct.py pre-commit` passes

## Testing Requirements

### Unit Tests

- After every commit, verify total count is unchanged:
  `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/ --collect-only -q` → 1697
- After Phase 4: verify unit tests pass with fakes, no Docker imports triggered:
  `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/unit/ -x -q`
- Marker filtering still works: `uv run sct.py unit-tests` collects 1487 tests

### Integration Tests

- After Phase 3: verify integration count:
  `UV_PROJECT_ENVIRONMENT=.venv uv run python -m pytest unit_tests/integration/ -m "integration" --collect-only -q`
  → 210 tests (growing to full 210 after Phase 5)
- After Phase 4: verify integration tests do NOT receive `fake_remoter` or `mock_cloud_services`
- `uv run sct.py integration-tests` works correctly (requires Docker)

### Manual Testing

- `uv run sct.py unit-tests` — verify 1487 tests discovered via `-m "not integration"` from all
  of `unit_tests/`
- `uv run sct.py integration-tests` — verify 210 tests discovered via `-m "integration"`
- `sct.py unit-tests -t unit/test_config.py` — verify specific-file targeting works with the
  new subdirectory paths

## Success Criteria

All Definition of Done items across phases are met. Additionally:

1. No test is lost — total count remains 1697 across `unit_tests/unit/` and
   `unit_tests/integration/`
2. `pytest_collection_modifyitems` hook is gone — fixture boundaries are enforced by directory
   structure, not code
3. `sct.py` commands work unchanged — markers remain the primary discovery mechanism
4. Unit tests under `unit_tests/unit/` get fake fixtures automatically via conftest scoping; no
   Docker imports triggered during unit test collection
5. Integration tests under `unit_tests/integration/` get Docker fixtures; `fake_remoter` and
   `mock_cloud_services` are not injected

## Risk Mitigation

### Risk: `__file__`-relative path breakage
**Likelihood**: High
**Impact**: Tests using `Path(__file__).parent / "test_data"` or traversals like
`parent.parent / "utils"` fail after the move because the file depth changes.
**Mitigation**: Two strategies applied at move time (Phase 2 and 3):
1. **Fixture-injectable usages** (inside test functions/fixtures): migrate to the
   `unit_tests_data_dir` fixture added in Phase 1, which always returns
   `unit_tests/test_data/` regardless of caller location.
2. **Module-level constants** (import-time, cannot use fixtures): update the
   `parent` chain directly — add one `.parent` for each level of nesting added by the
   move. Affected files are identified in the Phase 2 deliverables.
   Verify after each file move that the path resolves correctly before committing.

### Risk: Import path breakage for `sct.py`
**Likelihood**: High
**Impact**: `sct.py:145` imports `FakeTester` from `unit_tests.nemesis.fake_cluster`. Moving
`nemesis/` to `unit_tests/unit/nemesis/` breaks this import.
**Mitigation**: Update the import in Phase 2 and verify:
`python -c "from unit_tests.unit.nemesis.fake_cluster import FakeTester"`

### Risk: Conftest fixture scope conflicts
**Likelihood**: Medium
**Impact**: Session-scoped autouse fixtures in `unit_tests/unit/conftest.py` (e.g.,
`mock_cloud_services`) may interact unexpectedly with parent conftest fixtures when running all
tests together (`pytest unit_tests/`).
**Mitigation**: Run the full suite with `pytest unit_tests/` after Phase 4 to verify no fixture
conflicts. Ensure no fixture name collisions between parent and child conftest files.

### Risk: Mixed file split breaks shared state
**Likelihood**: Medium
**Impact**: Helper classes shared between unit and integration tests (e.g., `DummyScyllaCluster`
in `test_cluster.py`) become inaccessible after splitting.
**Mitigation**: Keep shared helpers in the unit file and import them from the integration file.
Alternatively, extract to `unit_tests/lib/`. Do not duplicate.

### Risk: Hook removal breaks tests still at `unit_tests/` root
**Likelihood**: Low (by design — resolved by phase ordering)
**Impact**: Tests that were at `unit_tests/` root and relied on the hook to receive
`mock_cloud_services` would fail if the hook is removed before they are moved.
**Mitigation**: Phase ordering prevents this: the hook is removed only in Phase 4, after all pure
unit files are already under `unit_tests/unit/` (Phase 2) where `mock_cloud_services` is autouse.
Mixed files not yet moved (handled in Phase 5) still live at root with the old conftest active —
but by Phase 4 all root-level test files are mixed files, and they will be handled in Phase 5
immediately after.

**Needs Investigation**: Verify that `test_config.py`, `test_cluster.py`, `test_version_utils.py`,
and the other mixed files still work correctly between Phase 4 (hook removed) and Phase 5
(files split) since they remain at root level during that window. Root-level tests are covered by
parent conftest (shared fixtures), but `mock_cloud_services` autouse is now only in
`unit_tests/unit/conftest.py`. The unit tests in mixed files at root will lose `mock_cloud_services`
between Phase 4 and Phase 5. **Resolution**: Run Phase 4 and Phase 5 in the same PR but consecutive
commits; do not merge between these two phases.

### Risk: `sct.py -t` option with new subdirectory paths
**Likelihood**: Low
**Impact**: `sct.py unit-tests -t test_config.py` prepends `unit_tests/` to the path, resolving
to `unit_tests/test_config.py` which won't exist after Phase 5.
**Mitigation**: Document in the PR description that `-t` requires the subdirectory prefix after
the migration: `sct.py unit-tests -t unit/test_config.py`. Consider updating the `sct.py` help
text to reflect this.

## Related Plans

- [MiniCloud Local Testing](https://github.com/scylladb/scylla-cluster-tests/pull/14009)
  (`pending_pr`, #14009) — Testing infrastructure plan; may need path updates if merged
  concurrently.

## PR History

| Phase | Commit | Status |
|-------|--------|--------|
| Phase 1: Cleanup and directory structure | `be83737fb` | Done |
| Phase 2: Move purely unit test files | `b6d44820e` | Done |
| Phase 3: Move purely integration test files | `9f2c3237d` | Done |
| Phase 4: Split conftest.py and remove hook | `d1b8d6764` | Done |
| Phase 5: Split mixed files + docs | — | In progress |
