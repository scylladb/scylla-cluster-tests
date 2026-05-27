---
name: labeling-pipelines
description: >-
  Guides labeling and linting test-case YAML files in SCT with test_metadata
  sections. Use when bulk-adding test_metadata to multiple test cases, running
  lint-test-docs to find coverage gaps, or auditing label accuracy across a
  directory. Triggers: "label pipelines", "add metadata to test cases",
  "bulk document tests", "find missing test_metadata", "lint test docs".
---

# Labeling Pipelines for SCT

Bulk-add or audit `test_metadata:` labels across test-case YAML files.

## Workflow: Add test_metadata to a test case

1. Read the test-case YAML to understand what it does
2. Check `test_duration` (minutes) → pick `duration_class` (short <6h, medium 6-24h, long >24h)
3. Check `nemesis_class_name` → add to `nemesis_labels`
4. Check `stress_cmd*` fields → add tools to `stress_tools`
5. Check `server_encrypt`, `n_db_nodes` → add to `features`
6. Check `cluster_backend` → add to `supported_backends` (or leave null for all)
7. Write a 2-4 sentence `description` explaining what the test validates
8. Run the `lint-test-docs` CLI with `--test-case-file` flag pointing to your file to verify

## Complete Example

```yaml
test_metadata:
  description: >-
    Basic longevity test running cassandra-stress write workload at QUORUM
    consistency for ~4 hours on a 6-node single-DC cluster with SisyphusMonkey
    nemesis. Validates cluster stability under moderate write load with chaos.
  test_type: longevity
  tier: tier1
  duration_class: short
  supported_backends:
    - aws
    - gce
    - azure
  stress_tools:
    - cassandra-stress
  workload: write
  nemesis_labels:
    - SisyphusMonkey
  features: []
```

## Workflow: Find coverage gaps

Run the `lint-test-docs` CLI command with `--missing-only` flag to list all
test-case YAMLs that have no `test_metadata:` section.

## Workflow: Lint all test cases

Run `lint-test-docs` with no flags. Exit code 1 if any errors (missing required
fields, invalid values). Warnings (cross-reference mismatches) do not fail the run.

## Auto-fix

Run `lint-test-docs` with `--fix` flag. Auto-corrects: `duration_class` from
`test_duration`, `stress_tools` from `stress_cmd*` fields, `multi-dc` feature
from `n_db_nodes`, `tls-ssl` from `server_encrypt`.

## Tier Selection Guide

| Tier | When to use |
|------|-------------|
| `sanity` | Runs on every commit, <1h, basic smoke |
| `tier1` | Weekly core regression, 3-24h |
| `tier2` | Extended regression, run less frequently |
| `release` | Mandatory for release qualification |
| `ondemand` | Investigation, niche, or expensive tests |

## Valid Field Values

See `references/taxonomy-values.md` for the full list of valid values per field.
The authoritative source is always `docs/pipeline-labels/taxonomy.yaml`.

## Validation Rules Quick Reference

| Rule | Check | Severity |
|------|-------|----------|
| TD-001 | test_metadata section exists | error |
| TD-002 | description >20 chars | error |
| TD-003 | all values valid per pydantic | error |
| TD-004 | test_type matches directory | warning |
| TD-005 | nemesis_labels includes nemesis_class_name | warning |
| TD-006 | stress_tools matches stress_cmd* | warning |
| TD-007 | supported_backends includes cluster_backend | warning |
| TD-008 | duration_class matches test_duration | warning |
| TD-009 | features consistent with config | warning |
| TD-010 | tier appropriate for duration_class | warning |
| TD-011 | supported_backends includes pipeline backend | warning |
