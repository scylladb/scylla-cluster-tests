---
name: reviewing-pipeline-docs
description: >-
  Guides reviewing and generating test_metadata sections for SCT test-case
  YAML files. Use when adding or auditing test_metadata to a test-case YAML,
  checking that description, tier, test_type, stress_tools, nemesis_labels,
  and features are accurate and complete. Covers the TestMetadata pydantic
  model, taxonomy values, cross-reference rules, and the lint-test-docs CLI.
---

# Reviewing Pipeline Docs for SCT

Add or audit `test_metadata:` sections in test-case YAML files.

## What is test_metadata?

Every test-case YAML in `test-cases/` should have a `test_metadata:` section
validated by `sdcm/test_metadata.py` (pydantic `TestMetadata` model).
It flows to Argus at test runtime via `submit_sct_run()`.

## Taxonomy Reference

Authorized values live in `docs/pipeline-labels/taxonomy.yaml`.

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `description` | str | **yes** | 2-4 sentences, >20 chars |
| `test_type` | Literal | no | matches test-cases/ subdir |
| `tier` | Literal | no | sanity/tier1/release/ondemand |
| `duration_class` | Literal | no | short(<6h)/medium(6-24h)/long(>24h) |
| `supported_backends` | list[str] | no | null = all backends |
| `stress_tools` | list[str] | no | cassandra-stress, scylla-bench, etc. |
| `workload` | Literal | no | write/read/mixed/etc. |
| `nemesis_labels` | list[str] | no | nemesis class names used |
| `features` | list[str] | no | tls-ssl, multi-dc, cdc, etc. |

## Duration Class Thresholds

`test_duration` in SCT config is in **minutes**:
- `short`: `test_duration < 360` (under 6 hours)
- `medium`: `360 <= test_duration <= 1440` (6–24 hours)
- `long`: `test_duration > 1440` (over 24 hours)

## Example

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

## Cross-Reference Rules

When filling in metadata, cross-check against the YAML config:
- `nemesis_labels` must include `nemesis_class_name` value
- `stress_tools` must include tools found in `stress_cmd*` fields
- `duration_class` must match `test_duration` (in minutes)
- `features` must include `multi-dc` if `n_db_nodes` has multiple values
- `features` must include `tls-ssl` if `server_encrypt: true`
- `supported_backends` must include `cluster_backend` if set

## Linting

```bash
# Validate all test cases
uv run sct.py lint-test-docs

# Validate a single file
uv run sct.py lint-test-docs --test-case-file test-cases/longevity/longevity-10gb-3h.yaml

# Show only files missing test_metadata
uv run sct.py lint-test-docs --missing-only
```
