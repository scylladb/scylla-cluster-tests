# Plan Frontmatter Fields

Every plan file in `docs/plans/` must start with YAML frontmatter. This document defines the fields, valid values, and conventions.

---

## Required Fields

```yaml
---
status: draft
domain: framework
created: 2026-01-15
last_updated: 2026-03-15
owner: null
---
```

### `status`

Current lifecycle state of the plan.

| Value | Meaning | When to Use |
|-------|---------|-------------|
| `draft` | Plan written, not yet started | Default for new plans |
| `approved` | Reviewed and approved for implementation | After team review/sign-off |
| `in_progress` | Active implementation underway | At least one phase has started |
| `blocked` | Implementation blocked | External dependency or issue prevents progress |
| `complete` | All phases implemented and verified | All DoD items checked off |
| `pending_pr` | Plan exists in an open PR, not yet merged | Used only in MASTER.md/progress.json for unmerged plans |

### `domain`

The functional area of the codebase this plan affects. Must be one of:

| Domain Key | Covers | Codebase Areas |
|-----------|--------|----------------|
| `cluster` | Cluster management, node lifecycle, backends | `sdcm/cluster*.py`, `sdcm/provision/` |
| `nemesis` | Chaos engineering, disruptors | `sdcm/nemesis.py`, `sdcm/nemesis_registry.py` |
| `stress-tools` | Load generators | `sdcm/stress/`, `*_thread.py` |
| `monitoring` | Metrics, dashboards, reporting | `sdcm/monitorstack/`, `sdcm/reporting/` |
| `events` | Event system | `sdcm/sct_events/` |
| `ci-cd` | Jenkins pipelines, Groovy libs | `jenkins-pipelines/`, `vars/` |
| `config` | Configuration system | `sdcm/sct_config.py`, `defaults/`, `test-cases/` |
| `k8s` | Kubernetes operator, K8s backends | `sdcm/cluster_k8s/` |
| `framework` | Core framework internals | `sdcm/cluster.py`, `sdcm/tester.py` |
| `ai-tooling` | AI skills, agent guidance | `skills/`, `AGENTS.md` |
| `remote` | Remote execution | `sdcm/remote/` |
| `testing` | Unit/integration test infrastructure | `unit_tests/` |

### `created`

Date the plan was first written. Format: `YYYY-MM-DD`. Derive from `git log --diff-filter=A` if adding frontmatter to an existing plan.

### `last_updated`

Date of the most recent substantive change. Format: `YYYY-MM-DD`. Update this when modifying plan content or status.

### `owner`

GitHub username of the person responsible for driving implementation. Set to `null` if unassigned.

---

## Corresponding Tracking Files

When creating or updating a plan's frontmatter, also update:

1. **`docs/plans/MASTER.md`** — Add/update the plan's row in the correct domain table
2. **`docs/plans/progress.json`** — Add/update the plan's entry with matching status

See [update-plan-status.md](../workflows/update-plan-status.md) for the step-by-step workflow.
