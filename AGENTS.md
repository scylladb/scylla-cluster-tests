# AGENTS.md

This file provides guidance to LLM Agents when working with code in this repository.

## Repository Overview

Scylla Cluster Tests (SCT) is a comprehensive test framework for ScyllaDB database, designed to test high-load scenarios on physical/virtual servers. The framework automatically provisions test infrastructure including Scylla clusters, loader machines, and monitoring servers.

## Development Environment Setup

### Local Development with uv (Recommended for direct Python work)
```bash
uv python install
uv venv
export UV_PROJECT_ENVIRONMENT=.venv
uv sync
```

> **⚠️ PROTECTED FILE — `.python-version`**
> Never commit changes to `.python-version`. This file is managed by the project maintainers and
> controls the Python version used across CI and all developer environments. If you need a different
> Python version locally to work around a dependency build failure, make the change temporarily in
> your local workspace **without staging or committing it**. Instead, add a PR comment describing
> the compatibility issue (e.g. "fastavro 1.11.1 fails to build on Python 3.14; tests were run
> locally with Python 3.12") so maintainers can decide whether to update the pinned version.

### Using Hydra (Docker/Podman container environment)
```bash
# Install hydra
sudo ./install-hydra.sh

# Enter containerized environment
hydra bash
```

## Common Development Commands

#### Run unit tests
```bash
# All unit tests
uv run sct.py unit-tests

# Specific unit test
uv run sct.py unit-tests -t test_config.py
```

#### Run integration tests
```bash
uv run sct.py integration-tests
```

### Code Quality and Linting

```bash
# Run pre-commit checks (includes autopep8, ruff, and other linters)
uv run sct.py pre-commit

# Run specific linters
ruff check --fix --preview .
autopep8 -i -j 2 <file>
```

### Running SCT

```bash
export SCT_SCYLLA_VERSION=2025.3.0
export SCT_USE_MGMT=false
uv run sct.py run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml
# or with hydra (without need to install python dependencies):
./docker/env/hydra.sh run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml
```

#### Cluster Reuse (for faster development iterations)
Before running SCT run below command (only when previously left cluster alive):
```bash
# Reuse existing cluster
export SCT_REUSE_CLUSTER=$(cat ~/sct-results/latest/test_id)
```

## High-Level Architecture

### Test Organization

**Test Types:**
- `*_test.py` - Main test modules (longevity, performance, upgrade, artifacts, etc.)
- `functional_tests/` - Kubernetes operator and other functional tests
- `unit_tests/` - Unit and integration tests for SCT components

**Unit Tests Directory Structure (`unit_tests/`):**
- `unit_tests/unit/` - Pure unit tests (~110 files). These get `fake_remoter` and
  `mock_cloud_services` autouse fixtures automatically via `unit_tests/unit/conftest.py`.
  Subdirectories: `k8s/`, `nemesis/`, `provisioner/`, `rest/`, `vector_store/`.
- `unit_tests/integration/` - Integration tests (~29 files) that require external services
  (Docker, AWS, GCE, Azure). Docker fixtures (`docker_scylla`, etc.) are in
  `unit_tests/integration/conftest.py`.
- `unit_tests/lib/` - Shared test utilities: `fake_remoter.py`, `fake_events.py`,
  `fake_provisioner.py`, `dummy_remote.py`, `s3_utils.py`, etc.
- `unit_tests/test_data/` and `unit_tests/test_configs/` - Shared test data files
- `unit_tests/conftest.py` - Shared fixtures (events, params, prom_address)

**Key Test Categories:**
1. **Longevity Tests** - Long-running stability tests with various nemesis operations
2. **Performance Tests** - Throughput and latency regression tests
3. **Upgrade Tests** - Rolling upgrade and version migration tests
4. **Artifacts Tests** - Package and image deployment verification
5. **Manager Tests** - Scylla Manager operation tests
6. **K8S Functional Tests** - Kubernetes operator tests

### Core Components

**Main Framework (`sdcm/`):**

| Area | Key Files | Purpose |
|------|-----------|---------|
| Cluster management | `cluster.py`, `cluster_aws.py`, `cluster_gce.py`, `cluster_azure.py`, `cluster_docker.py`, `cluster_k8s/` | Base classes and backend-specific implementations |
| Test base | `tester.py` | Base test class, setup/teardown, reporting |
| Nemesis (chaos) | `nemesis/` package — see [docs/nemesis.md](docs/nemesis.md) | `NemesisBaseClass`, `NemesisRunner`, `NemesisRegistry`, auto-discovery |
| Configuration | `sct_config.py` | Parameter handling; precedence: CLI > env vars > config files > defaults |
| Stress tools | `stress/`, `*_thread.py` | Wrappers for cassandra-stress, scylla-bench, YCSB, gemini, latte |
| Remote execution | `remote/` | SSH, Docker, and K8s command runners |
| Provisioning | `provision/` | Cloud-specific infra provisioning (AWS, Azure, GCE) |
| Monitoring | `monitorstack/`, `prometheus.py`, `db_stats.py` | Prometheus/Grafana metrics, log collection |
| Events | `sct_events/` | Event generation, filtering, and test flow control |
| REST clients | `rest/` | Scylla storage service, compaction manager, RAFT API |
| Manager | `mgmt/` | Scylla Manager CLI and operations (backup, repair) |
| Utilities | `utils/` | Cloud utils, version utils, Docker utils, and more |

**Configuration System:**
- `test-cases/` - Test configuration YAML files
- `configurations/` - Reusable configuration fragments
- `defaults/` - Default configuration values
- Configuration precedence: CLI args > Environment vars > Config files > Defaults

**Backends Supported:**
- `aws` - Amazon Web Services (primary backend)
- `gce` - Google Cloud Platform
- `azure` - Microsoft Azure
- `docker` - Local Docker development
- `k8s-eks`, `k8s-gke` - Kubernetes on cloud providers
- `k8s-local-kind` - Local Kubernetes with kind
- `baremetal` - Pre-existing clusters

### Nemesis Operations

Nemesis are chaos operations that test database resilience. For a comprehensive guide, see [docs/nemesis.md](docs/nemesis.md).

**Architecture:**
- `NemesisBaseClass` — Abstract base for individual disruptions (a.k.a. "Monkeys"). Each subclass sets boolean flags and implements `disrupt()`.
- `NemesisRunner` — Orchestrator that contains all `disrupt_*` methods (the actual disruption logic), handles node selection, metrics, and error reporting.
- `NemesisRegistry` — Discovery mechanism that filters nemesis using boolean flag expressions (e.g. `"not disruptive"`, `"topology_changes and not limited"`).
- `NemesisNodeAllocator` — Thread-safe singleton preventing conflicting nemesis on the same node.

**Common nemesis categories:**
- Node operations (stop/start, reboot, terminate, decommission)
- Network disruptions (block, delay, partition)
- Disk operations (fill disk, corrupt data)
- Schema changes (add/drop columns, modify tables)
- Load operations (memory stress, CPU stress)

### Monitoring and Reporting

**Monitoring Stack:**
- Prometheus + Grafana for metrics
- Elasticsearch for logs
- Custom SCT monitoring dashboards

**Test Results:**
- Results stored in `~/sct-results/`
- Argus integration for test tracking
- Email reports for performance tests
- Parallel timeline reports for debugging

## Testing Best Practices

### Test Configuration
- Start with existing test cases in `test-cases/` as templates
- Use configuration fragments from `configurations/` for common settings
- Network configs are in `configurations/network_config/`

### Development Workflow
1. Create feature branch from master
2. Run unit tests locally: `uv run sct.py unit-tests`
3. Test with docker backend first: `--backend docker`
4. Use cluster reuse for faster iteration
5. Run pre-commit before commit, and after commit: `uv run sct.py pre-commit`

### Debugging SCT Tests
- Check logs in `~/sct-results/latest/`

### Code Style Guidelines

**IMPORTANT: Avoid inline imports at all cost**

- All imports at the top of the file, NOT inside functions/methods
- **Exception:** Only for hard-to-overcome cyclic dependencies (add comment above the import explaining why)

**Import grouping standard:**
1. Built-in modules (e.g., `import os`, `from typing import List`)
2. Third-party modules (e.g., `import pytest`, `from boto3 import client`)
3. Internal modules (e.g., `from sdcm.cluster import BaseNode`)

Separate each group with a blank line.
Within each group, sort imports alphabetically.

**HTTP/Curl Conventions:**
- All `remoter.run("curl ...")` calls must use `curl_with_retry()` from `sdcm/utils/curl.py`
- All `requests` calls must go through a session with retry adapter (pattern: `sdcm/rest/rest_client.py`)
- Localhost calls may use `retry=0` but must still use the utility for consistent `--connect-timeout`
- Document any exceptions with `# no-retry: <reason>` comments

### Method Signature Changes (Override Safety)

**CRITICAL**: SCT uses deep class hierarchies (e.g., `BaseCluster` -> `AWSCluster` -> `MonitorSetEKS`).
When modifying a method signature in a parent class:

1. Search for ALL overrides: `grep -rn "def <method_name>" sdcm/ unit_tests/`
2. Update every override to accept and forward the new parameter
3. Update test stubs in `unit_tests/` that override the same method (e.g., `dummy_remote.py`, `test_cluster.py`)
4. If using `**kwargs` in overrides, verify the parameter actually reaches `super()`

Failure to do this causes runtime `TypeError` that isn't caught by linting or unit tests.
See PR #14113 for a real-world example of this failure pattern (caused by PR #13445 missing an override in `cluster_k8s/eks.py`).

High-risk methods with 5+ overrides: `add_nodes`, `_create_instances`, `wait_for_init`, `destroy`.

### Unit Testing Guidelines

**IMPORTANT: Use pytest style, NOT unittest.TestCase**

All unit tests in `unit_tests/` should follow pytest conventions:

**❌ DON'T:** `class TestMyFeature(unittest.TestCase)` with `setUp()` and `self.assertEqual()`

**✅ DO:** Use pytest functions, fixtures, and simple `assert` statements

**Key Principles:**

1. **Use fixtures for setup/teardown** - `@pytest.fixture` to avoid code repetition, access via function parameters
2. **Use parametrize for multiple test cases** - `@pytest.mark.parametrize("input,expected", [...])` for efficient variation testing
3. **Scope fixtures appropriately** - Use `scope="module"` for expensive setup, `scope="function"` (default) for clean state
4. **Use conftest.py for shared fixtures** - Place common fixtures in `unit_tests/conftest.py`, auto-discovered by pytest
5. **Prefer simple assertions** - Use `assert x == y` not `self.assertEqual(x, y)`

## Documentation Standards

- **Docstrings**: Google Python format (Args, Returns, Raises sections)
- **Config parameters**: Include type, default, valid range, and example. See [docs/sct-configuration.md](docs/sct-configuration.md)
- **Code comments**: Explain "why" not "what" — focus on intent and non-obvious constraints
- **Breaking changes**: Bold formatting, before/after examples, migration paths

## Environment Variables

Critical environment variables:
- `SCT_CLUSTER_BACKEND` - Backend to use (aws, gce, azure, docker, etc.)
- `SCT_CONFIG_FILES` - Configuration files to use
- `SCT_SCYLLA_VERSION` - Scylla version to test
- `SCT_REUSE_CLUSTER` - Test ID to reuse existing cluster
- `SCT_ENABLE_ARGUS` - Enable/disable Argus reporting
- `SCT_IP_SSH_CONNECTIONS` - Use 'public' for local development
- `AWS_PROFILE` - AWS profile for authentication (if using OKTA)

## Common Issues and Solutions

### Python Version Compatibility

`.python-version` is pinned to a specific version (currently `3.14`) and **must never be modified in a commit**.
If a dependency fails to build with the pinned version (e.g. `fastavro` wheel not yet available for `3.14`):

1. Temporarily override it in your local workspace only: `echo "3.12" > .python-version` (do **not** stage this file)
2. Create the venv and run tests: `uv sync && .venv/bin/python -m pytest …`
3. Restore the original value: `git checkout -- .python-version`
4. Add a comment to the PR explaining the compatibility issue so a maintainer can update
   the pinned version if needed.

### AWS/Cloud Authentication
- Use OKTA for AWS access (preferred)
- Or configure AWS CLI: `aws configure`
- For GCE: Set up gcloud CLI
- For Azure: Configure az CLI

### Network Configuration
- For local development with cloud backend (aws, gce, azure) use `configurations/network_config/test_communication_public.yaml` and `export SCT_IP_SSH_CONNECTIONS=public`

### Docker/Podman Issues
- Ensure user is in docker group: `sudo usermod -aG docker $USER`
- For podman, configure registries in `~/.config/containers/registries.conf`
- Some operations requiring sudo won't work in rootless podman

## Jenkins Pipeline Structure

Jenkins pipelines are in `jenkins-pipelines/` organized by:
- `oss/` - Scylla tests
- `operator/` - Kubernetes operator tests
- `performance/` - Performance regression tests
- `manager/` - Scylla Manager tests

Pipeline utilities are in `vars/` directory.

## Skills

Modular, task-specific guidance for AI agents lives in the `skills/` directory. Each skill is a self-contained directory with a `SKILL.md` entry point and optional `references/` and `workflows/` subdirectories.

| Skill | Description | Path |
|-------|-------------|------|
| code-review | Guides AI-assisted code review of SCT PRs. Use when reviewing diffs, checking override compatibility, verifying import conventions, or assessing backend impact. | `skills/code-review/SKILL.md` |
| designing-skills | Guides the design and structuring of AI agent skills for SCT. Use when creating new skills, reviewing existing skills, or editing SKILL.md files. | `skills/designing-skills/SKILL.md` |
| fix-backport-conflicts | Fix inline merge conflict markers in backport PRs. Use when a backport PR has unresolved conflict markers or a cherry-pick produced merge conflicts. | `skills/fix-backport-conflicts/SKILL.md` |
| profiling-sct-code | Profile Python code in SCT to find CPU, memory, and concurrency bottlenecks. Use when a test or operation is unexpectedly slow or memory usage grows unbounded. | `skills/profiling-sct-code/SKILL.md` |
| writing-plans | Use when asked to generate an implementation plan, draft a plan, or design a phased feature rollout for SCT. | `skills/writing-plans/SKILL.md` |
| writing-unit-tests | Guides writing and debugging unit tests for SCT using pytest. Use when creating test files in unit_tests/, adding test cases, or mocking external services. | `skills/writing-unit-tests/SKILL.md` |
| writing-integration-tests | Guides writing integration tests that interact with real external services. Use when creating tests requiring Docker, AWS, GCE, Azure, OCI, or Kubernetes backends. | `skills/writing-integration-tests/SKILL.md` |
| commit-summary | Generate weekly commit summary reports for the SCT repository | `skills/commit-summary/SKILL.md` |
| writing-nemesis | Guides writing new nemesis (chaos disruptions). Use when creating NemesisBaseClass subclasses or implementing disruption logic. | `skills/writing-nemesis/SKILL.md` |

When creating a new skill, follow the process in `skills/designing-skills/workflows/create-a-skill.md`.

## Implementation Plans

I have strict standards for feature planning. You can find the full guidelines in `docs/plans/INSTRUCTIONS.md`.

**Rule:** When I ask you to "generate an implementation plan" or "draft a plan", you MUST read `docs/plans/INSTRUCTIONS.md` and follow the structure defined there. Do not apply this format to regular coding questions.

**Plan types:** The `writing-plans` skill supports two formats:
- **Full plans** (7-section, `docs/plans/`): For multi-phase work, 1K+ LOC. Requires YAML frontmatter, MASTER.md registration, and progress.json tracking. PRs with a `plans` label use this format.
- **Mini-plans** (4-section, `docs/plans/mini-plans/`): For single-PR changes under ~1K LOC. No frontmatter, no registration. Disposable after merge.

The skill routes automatically based on the PR `plans` label, user input, or task size estimate.
