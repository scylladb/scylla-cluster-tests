---
name: reviewing-pipeline-docs
description: >-
  Guides reviewing and generating test_metadata sections for SCT test-case
  YAML files. Use when adding or auditing test_metadata to a test-case YAML,
  checking that description, tier, test_type, stress_tools, nemesis_labels,
  and features are accurate and complete. Triggers: "add test_metadata",
  "audit test YAML", "fill in pipeline labels", "document test case",
  "run lint-test-docs", "review test_metadata". Covers the TestMetadata
  pydantic model, taxonomy values, cross-reference rules, and validation.
---

# Reviewing Pipeline Docs for SCT

Add or audit `test_metadata:` sections in test-case YAML files.

## Workflow (numbered steps)

1. **Read** the test-case YAML to understand its purpose (stress commands, nemesis, duration, backends)
2. **Check** if `test_metadata:` already exists — if so, audit it against the config fields below
3. **Cross-reference** each metadata field against the actual YAML config (see rules below)
4. **Fill in** missing or incorrect fields using values from the taxonomy
5. **Validate** by running the lint-test-docs CLI on the target file
6. **Fix** any errors/warnings reported by the linter

## What is test_metadata?

Every test-case YAML in `test-cases/` should have a `test_metadata:` section
validated by the TestMetadata pydantic model in `sdcm/test_metadata.py`.
It flows to Argus at test runtime.

## Field Summary

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `description` | str | **yes** | 2-4 sentences, >20 chars |
| `test_type` | Literal | no | matches test-cases/ subdir |
| `tier` | Literal | no | sanity/tier1/tier2/release/ondemand |
| `duration_class` | Literal | no | see references for thresholds |
| `supported_backends` | list | no | null = all backends |
| `stress_tools` | list | no | tool names from stress commands |
| `workload` | Literal | no | write/read/mixed/scan/counter |
| `features` | list | no | tls-ssl, multi-dc, cdc, etc. |
| `team_ownership` | Literal | no | QA team responsible for maintaining this test |

Note: `nemesis_labels` appears in older examples but is not currently a validated
`TestMetadata` field (no taxonomy/pydantic support) — omit it from new
`test_metadata:` blocks rather than add unenforced data.

For the full list of valid values per field, see `references/taxonomy-values.md`.
The authoritative source is always `docs/pipeline-labels/taxonomy.yaml`.

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
  features: []
  team_ownership: core-test-infra
```

## Cross-Reference Rules

When filling in metadata, cross-check against the YAML config:
- `stress_tools` must include tools found in `stress_cmd*` fields
- `duration_class` must match `test_duration` (in minutes): short <360, medium 360-1440, long >1440
- `features` must include `multi-dc` if `n_db_nodes` has multiple values
- `features` must include `tls-ssl` if `server_encrypt: true`
- `supported_backends` must include `cluster_backend` if set

## Linting

Use the `lint-test-docs` CLI command (via sct.py) to validate:
- No flags: validates all test cases, exits 1 on errors
- With a file path argument: validates a single file
- With missing-only flag: shows only files missing test_metadata
