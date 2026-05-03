# SCT Implementation Plans — Master Index

![Progress Roadmap](assets/progress-roadmap.svg)

This is the central index for all implementation plans in the SCT project.
Plans are grouped by domain and tracked with status metadata.

For plan writing guidelines, see [INSTRUCTIONS.md](INSTRUCTIONS.md).

## Status Legend

| Status | Meaning |
|--------|---------|
| `draft` | Plan written, not yet approved or started |
| `approved` | Plan reviewed and approved for implementation |
| `in_progress` | Active implementation underway |
| `blocked` | Implementation blocked by dependency or issue |
| `complete` | All phases implemented and verified |
| `pending_pr` | Plan exists in an open PR, not yet merged |

## Plans by Domain

### Cluster — Cluster management, node lifecycle, backends

| Plan | Status | File / PR |
|------|--------|-----------|
| Docker Cleanup for All Backends | `draft` | [docker-cleanup-all-backends.md](infrastructure/docker-cleanup-all-backends.md) |
| GCE Provisioning | `draft` | [gce-provisioning.md](infrastructure/gce-provisioning.md) |
| Amazon EMR Spark Migrator | `pending_pr` | [#13909](https://github.com/scylladb/scylla-cluster-tests/pull/13909) |
| Source-Destination Clusters | `pending_pr` | [#13908](https://github.com/scylladb/scylla-cluster-tests/pull/13908) |
| Cassandra Cluster Support | `draft` | [cassandra-cluster-support.md](infrastructure/cassandra-cluster-support.md) |
| SSH Key Decoupling | `draft` | [ssh-key-decoupling.md](infrastructure/ssh-key-decoupling.md) |
| Multi-Cloud Provisioning Resilience | `draft` | [multi-cloud-provisioning-resilience.md](infrastructure/multi-cloud-provisioning-resilience.md) |

### Nemesis — Chaos engineering, disruptors

| Plan | Status | File / PR |
|------|--------|-----------|
| Nemesis Rework (Nemesis 2.0) | `in_progress` | [nemesis-rework.md](nemesis/nemesis-rework.md) |
| Nemesis Extraction Phase 3 | `in_progress` | [nemesis-extraction.md](nemesis/nemesis-extraction.md) |

### Stress Tools — Load generators

| Plan | Status | File / PR |
|------|--------|-----------|

### CI/CD — Jenkins pipelines, Groovy libs

| Plan | Status | File / PR |
|------|--------|-----------|
| Jenkins Pipeline Cluster Reuse | `draft` | [jenkins-reuse-cluster.md](jenkins/jenkins-reuse-cluster.md) |
| Jenkins Pipeline Config Linter | `draft` | [jenkins-pipeline-config-linter.md](jenkins/jenkins-pipeline-config-linter.md) |
| Jenkins Uno-Choice Billing Project | `draft` | [jenkins-uno-choice-billing-project.md](jenkins/jenkins-uno-choice-billing-project.md) |
| Centralized Trigger Matrix | `draft` | [centralized-trigger-matrix.md](jenkins/centralized-trigger-matrix.md) |
| i8g Performance Jobs Migration | `draft` | [i8g-performance-jobs-migration.md](i8g-performance-jobs-migration.md) |

### Config — Configuration system

| Plan | Status | File / PR |
|------|--------|-----------|
| Resilient Test Config Dependencies | `pending_pr` | [#13982](https://github.com/scylladb/scylla-cluster-tests/pull/13982) |
| Typed Config Access Migration | `pending_pr` | [#13878](https://github.com/scylladb/scylla-cluster-tests/pull/13878) |
| SCT Config Validation and Lazy Images | `draft` | [sct-config-validation-and-lazy-images.md](sct-config-validation-and-lazy-images.md) |
| SCT Config Follow-up Refactoring | `pending_pr` | [#13845](https://github.com/scylladb/scylla-cluster-tests/pull/13845) |
| Config Type Normalization | `draft` | [config-type-normalization.md](config/config-type-normalization.md) |

### K8s — Kubernetes operator, K8s backends

| Plan | Status | File / PR |
|------|--------|-----------|
| K8s Multitenancy Dict Config | `draft` | [k8s-multitenancy-dict-config.md](config/k8s-multitenancy-dict-config.md) |

### Framework — Core framework internals

| Plan | Status | File / PR |
|------|--------|-----------|
| Health Check Optimization | `draft` | [health-check-optimization.md](infrastructure/health-check-optimization.md) |
| Full Version Tag Lookup | `draft` | [full-version-tag-lookup.md](config/full-version-tag-lookup.md) |
| Keystore Improvements | `pending_pr` | [#14055](https://github.com/scylladb/scylla-cluster-tests/pull/14055) |

### AI Tooling — AI skills, agent guidance

| Plan | Status | File / PR |
|------|--------|-----------|
| AI Skills Framework | `draft` | [ai-skills-framework.md](ai-skills-framework.md) |
| PR Review Taxonomy Analysis | `in_progress` | [pr-review-taxonomy-analysis.md](pr-review-taxonomy-analysis.md) |

### Testing — Unit/integration test infrastructure

| Plan | Status | File / PR |
|------|--------|-----------|
| MiniCloud Local Testing | `pending_pr` | [#14009](https://github.com/scylladb/scylla-cluster-tests/pull/14009) |
| Unit/Integration Test Separation | `draft` | [unit-integration-test-separation.md](testing/unit-integration-test-separation.md) |

## Cross-Plan Dependencies

| Dependent Plan | Depends On | Relationship |
|---------------|------------|--------------|
| Nemesis Extraction Phase 3 | Nemesis Rework | Phase 3 continues the extraction started in Nemesis 2.0 |
| SCT Config Follow-up Refactoring | SCT Config Validation and Lazy Images | Follow-up work after initial config validation |
| Typed Config Access Migration | SCT Config Follow-up Refactoring | Type safety layer on top of refactored config |

## Domain Coverage Gaps

The following domains have **no plans** currently:

| Domain | Covers | Codebase Areas |
|--------|--------|----------------|
| `monitoring` | Metrics, dashboards, reporting | `sdcm/monitorstack/`, `sdcm/reporting/` |
| `events` | Event system | `sdcm/sct_events/` |
| `remote` | Remote execution | `sdcm/remote/` |

These gaps are informational — not every domain needs an active plan.
