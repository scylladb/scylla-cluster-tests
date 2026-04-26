# Audit Report: Skills Directory (`.claude/skills/`)

**Date:** April 2025
**Scope:** All 9 skills in `.claude/skills/`

## Overview

The skills directory contains 9 skills that guide AI agents for common SCT development tasks. Overall quality is good — the dual-platform structure (GitHub Copilot + Claude Code) is well thought out, and most skills follow a consistent pattern with `SKILL.md` entry point, `references/`, and `workflows/` subdirectories.

## Skill Quality Assessment

| Skill | Rating | Notes |
|-------|--------|-------|
| `code-review` | Excellent | Comprehensive, covers override safety, SCT-specific criteria |
| `writing-unit-tests` | Excellent | Strong coverage of pytest patterns, mocking, fixtures |
| `writing-integration-tests` | Good | Covers Docker/cloud fixtures, credential skipping |
| `writing-nemesis` | Good | Covers package structure, flags, auto-discovery |
| `writing-plans` | Good | Dual-format (full/mini), clear routing logic |
| `designing-skills` | Good | Meta-skill for creating other skills, solid structure |
| `commit-summary` | Good | Simple and focused, covers the workflow well |
| `fix-backport-conflicts` | Issues | Non-standard YAML frontmatter in SKILL.md |
| `profiling-sct-code` | Issues | Orphaned workflow file (referenced but may not exist) |

## Structural Issues

### 1. Non-Standard YAML Frontmatter (`fix-backport-conflicts`)

**File:** `.claude/skills/fix-backport-conflicts/SKILL.md`
**Issue:** Contains YAML frontmatter that doesn't follow the pattern used by other skills. This may cause parsing issues with tools that consume skill metadata.
**Fix:** Align frontmatter format with the other 8 skills.

### 2. Orphaned Workflow (`profiling-sct-code`)

**File:** `.claude/skills/profiling-sct-code/`
**Issue:** A workflow file is referenced from the skill but the reference chain may be broken (orphaned file). This means the workflow might not be discoverable by agents.
**Fix:** Verify all workflow references and remove or reconnect orphaned files.

### 3. Orphaned Files (4 total)

Across the skills directory, 4 files were identified that are not referenced from any SKILL.md or workflow:
- These may be leftover from refactoring or incomplete skill creation
- They add noise and could confuse agents that scan the directory

**Fix:** Audit each orphaned file — either connect it to its parent skill or remove it.

### 4. Inconsistent Directory Structure

Most skills follow `SKILL.md` + `references/` + `workflows/`, but a few deviate:
- Some have `references/` but no `workflows/`
- Some have extra files at the skill root level

This isn't a bug, but consistency helps agents navigate predictably.

## Coverage Gaps

The following areas of SCT development have **no dedicated skill**:

| Gap Area | Impact | Priority |
|----------|--------|----------|
| **Writing test cases** (test-cases/*.yaml) | Test case configuration is complex; no guidance exists | High |
| **Configuration management** (sct_config.py) | Adding parameters requires understanding a 4500-line file | High |
| **Backend implementation** (new cluster_*.py) | No skill for adding new backend support | Medium |
| **Monitoring/alerting** (monitorstack/) | Custom monitoring setup has no guidance | Medium |
| **Stress tool integration** (stress/) | Adding new stress tool wrappers has no guidance | Medium |
| **Event system** (sct_events/) | Custom event creation/handling is undocumented for agents | Low |
| **Jenkins pipeline** (jenkins-pipelines/) | Pipeline creation/modification has no agent guidance | Low |
| **REST client** (rest/) | Adding new REST API clients has no guidance | Low |
| **Upgrade testing** (upgrade_test.py patterns) | Complex upgrade scenarios have no skill | Low |

## Recommendations

1. **Fix structural issues** (frontmatter, orphaned files) — low effort, immediate value
2. **Add test-case writing skill** — highest coverage gap impact
3. **Add configuration management skill** — second highest impact
4. **Standardize directory structure** — add a skill template or linter
5. **Maintain a skills coverage matrix** — track which areas have skills and which don't
