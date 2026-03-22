---
name: code-review
description: >-
  Guides AI-assisted code review of SCT pull requests. Use when reviewing
  a PR, checking a diff for correctness, evaluating method signature changes
  across class hierarchies, verifying override compatibility, checking import
  conventions, error handling patterns, backend impact, test coverage, or
  provision label requirements. Covers inheritance safety, polymorphic method
  audits, and SCT-specific review criteria.
---

# Code Review for SCT

Review SCT pull requests for correctness, safety, and convention compliance.

## Essential Principles

### Override Safety Is the Highest-Priority Check

**When a method signature changes on a base class, every subclass override MUST be updated.**

SCT uses deep class hierarchies (e.g., `BaseCluster` -> `AWSCluster` -> `MonitorSetEKS`). A signature change in a parent class that isn't propagated to all overrides causes a runtime `TypeError` — invisible to linting, invisible in the diff, caught only in production. This is the single most dangerous class of bug in SCT because it passes all static checks and all existing tests.

Real incident: PR #13445 added `ami_id` to `AWSCluster._create_instances()` but missed `MonitorSetEKS._create_instances()` in `cluster_k8s/eks.py`. The bug survived multiple review rounds and broke EKS monitor provisioning at runtime (PR #14113 fixed it).

**Why this must be check #1**: LLMs process diffs, not the full class hierarchy. Without explicit instruction to search for overrides outside the diff, no reviewer — human or AI — will find them.

### Look Beyond the Diff

**The diff shows what changed, not what broke.**

Many SCT bugs come from code that was NOT modified but should have been. When reviewing:
- A method signature change -> search for ALL overrides
- A new parameter threaded through a call chain -> verify every link in the chain
- A configuration option added -> check `defaults/` for missing default values
- A backend-specific change -> check if other backends need the same change

### Convention Violations Are Bugs

**SCT enforces strict conventions because inconsistency causes real failures in a multi-backend test framework.**

Import ordering, error handling patterns, test style — these aren't style preferences. Inline imports cause circular dependency failures in production. `unittest.TestCase` breaks pytest fixture injection. Wrong provision labels mean backend regressions go untested.

### Review What's Missing, Not Just What's Present

**The most valuable review comment is about code that should exist but doesn't.**

Ask: Is there a test for this change? Is there a default value for this config option? Are there other backends that need the same fix? Is the docstring updated? Missing code is harder to spot than wrong code, and it's where AI reviewers add the most value.

## When to Use

- Reviewing a PR diff for SCT repository
- Checking whether a method signature change breaks subclass overrides
- Verifying import conventions in new or modified Python files
- Assessing backend impact of infrastructure code changes
- Evaluating whether a PR needs provision test labels
- Checking for missing tests, missing defaults, or missing docstrings
- Reviewing nemesis operations or cluster configuration changes

## When NOT to Use

- Writing new code from scratch (use relevant implementation skills)
- Writing unit tests (use `writing-unit-tests` skill)
- Writing integration tests (use `writing-integration-tests` skill)
- Creating implementation plans (use `writing-plans` skill)
- Fixing backport conflicts (use `fix-backport-conflicts` skill)

## Review Checks (Quick Reference)

Detailed checklist with examples in [review-checklist.md](references/review-checklist.md).
Real incident catalog in [common-issues.md](references/common-issues.md).

### Check 1: Override & Inheritance Safety

**Trigger**: Any PR that adds, removes, or changes parameters on a method definition.

1. Search for ALL overrides: `grep -rn "def <method_name>" sdcm/ unit_tests/`
2. For each override NOT in the diff -> **FLAG as potential breakage**
3. Verify new params are accepted AND forwarded to `super()`
4. Check test stubs in `unit_tests/` that mock or override the same method

**High-risk polymorphic methods** (sorted by override count):

| Method | Overrides | Key files to check |
|---|---|---|
| `add_nodes` | 18+ | All `cluster_*.py`, `cluster_k8s/`, `unit_tests/` stubs |
| `_create_instances` | 5+ | `cluster_aws.py`, `cluster_gce.py`, `cluster_azure.py`, `cluster_oci.py`, `cluster_k8s/eks.py` |
| `_create_on_demand_instances` | 2+ | `cluster_aws.py` subclasses |
| `_create_spot_instances` | 2+ | `cluster_aws.py` subclasses |
| `wait_for_init` | 5+ | All backend cluster files |
| `destroy` | 5+ | All backend cluster and node files |

### Check 2: Import Conventions

**Trigger**: Any new or modified `import` statement.

- All imports at top of file, never inside functions
- Three groups separated by blank lines: stdlib, third-party, internal
- Alphabetically sorted within each group
- Only exception: cyclic dependency with explanatory comment

### Check 3: Error Handling

**Trigger**: Any `try/except`, `raise`, or error handling code.

- No empty `catch(e) {}` or bare `except: pass`
- Use `silence()` context manager over bare try/except where appropriate
- Custom exceptions should subclass appropriate SCT base exceptions
- Error messages should include enough context for debugging

### Check 4: Test Coverage

**Trigger**: Any non-trivial code change.

- New public functions/methods should have corresponding tests in `unit_tests/`
- Tests must use pytest style, not `unittest.TestCase`
- Tests must use fixtures, not `setUp`/`tearDown`
- Parametrize when testing multiple input variations

### Check 5: Configuration Changes

**Trigger**: Any change to `sdcm/sct_config.py` or config-related files.

- New config options MUST have defaults in `defaults/test_default.yaml` or backend-specific files
- Config parameter must have proper type, description, and example
- Pre-commit hook updates `docs/configuration_options.md` automatically

### Check 6: Backend Impact

**Trigger**: Any change to files in `sdcm/cluster_*.py`, `sdcm/provision/`, or `sdcm/utils/*_utils.py`.

- Verify the correct provision test labels are requested
- Check if the same change is needed for other backends
- Backend file -> label mapping in [review-checklist.md](references/review-checklist.md)

### Check 7: Commit Message Format

**Trigger**: Every PR.

- Conventional Commits format: `type(scope): subject`
- Valid types: `ci`, `docs`, `feature`, `fix`, `improvement`, `perf`, `refactor`, `revert`, `style`, `test`, `unit-test`, `build`, `chore`
- Scope minimum 3 chars, subject 10-120 chars, body minimum 30 chars

## Reference Index

| File | Content |
|------|---------|
| [review-checklist.md](references/review-checklist.md) | Full review checklist with per-check details and examples |
| [common-issues.md](references/common-issues.md) | Catalog of real incidents with root cause analysis and prevention |

| Workflow | Purpose |
|----------|---------|
| [review-a-pr.md](workflows/review-a-pr.md) | Step-by-step process for reviewing an SCT pull request |

## Success Criteria

A complete code review:

- [ ] Override safety checked — all method signature changes verified against subclass overrides
- [ ] Import conventions verified — no inline imports, correct grouping
- [ ] Error handling reviewed — no empty catches, appropriate patterns used
- [ ] Test coverage assessed — new logic has corresponding tests
- [ ] Configuration defaults verified — new options have defaults
- [ ] Backend impact evaluated — correct provision labels requested
- [ ] Commit message validated — Conventional Commits format followed
- [ ] Missing code identified — defaults, docstrings, tests, other backends
