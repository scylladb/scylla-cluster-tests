---
name: labeling-pipelines
description: >-
  Guides labeling and linting test-case YAML files in SCT with test_metadata
  sections. Use when bulk-adding test_metadata to multiple test cases, running
  lint-test-docs to find coverage gaps, auto-fixing duration_class or
  stress_tools mismatches, or auditing label accuracy across a directory.
  Covers the lint-test-docs CLI, auto-fix flags, and batch workflows.
---

# Labeling Pipelines for SCT

Bulk-add or audit `test_metadata:` labels across test-case YAML files.

## Workflow: Add test_metadata to a test case

1. Read the test-case YAML to understand what it does
2. Check `test_duration` (minutes) → pick `duration_class`
3. Check `nemesis_class_name` → add to `nemesis_labels`
4. Check `stress_cmd*` fields → add tools to `stress_tools`
5. Check `server_encrypt`, `n_db_nodes` → add to `features`
6. Check `cluster_backend` → add to `supported_backends` (or leave null for all)
7. Write a 2-4 sentence `description` explaining what the test validates
8. Run `uv run sct.py lint-test-docs --test-case-file <path>` to verify

## Workflow: Find coverage gaps

```bash
uv run sct.py lint-test-docs --missing-only
```

Lists all test-case YAMLs that have no `test_metadata:` section.

## Workflow: Lint all test cases

```bash
uv run sct.py lint-test-docs
```

Exit code 1 if any errors (missing required fields, invalid values).
Warnings (cross-reference mismatches) do not fail the run.

## Auto-fix

```bash
uv run sct.py lint-test-docs --fix
```

Auto-corrects: `duration_class` from `test_duration`, `stress_tools` from
`stress_cmd*` fields, `multi-dc` feature from `n_db_nodes`, `tls-ssl` from
`server_encrypt`.

## Tier Selection Guide

| Tier | When to use |
|------|-------------|
| `sanity` | Runs on every commit, <1h, basic smoke |
| `tier1` | Weekly core regression, 3-24h |
| `release` | Mandatory for release qualification |
| `ondemand` | Investigation, niche, or expensive tests |

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
