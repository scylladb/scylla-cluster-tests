# PR Review Taxonomy — Executive Summary

**Date:** March 2026
**Scope:** 11,312 PRs · 28,519 review comments · 7 years (2019 Q1 – 2026 Q1)

---

## Key Finding

**AI-authored PRs generate 2–42× more review comments per category than human-authored PRs.**
The biggest gaps are in dead code removal (42×), test quality (5.7×), readability (3.7×), and scope hygiene (3.9×). This means reviewers spend disproportionate time on AI PRs — time that well-designed skills and automated checks could save.

## Top Review Burdens (by 7-year volume)

| # | Category | Total | Trend | Actionable? |
|---|----------|------:|-------|-------------|
| 1 | **U8 Directive** — direct instructions to author | 6,098 | Persistent | Reduce by improving skill pre-checks |
| 2 | **U9 Process** — backport, CI, merge strategy | 3,189 | Persistent | Automate with pipeline tooling |
| 3 | **U10 Opinion** — subjective assessments | 2,443 | Persistent | Codify conventions to reduce ambiguity |
| 4 | **U4 Discussion** — context sharing, acks | 2,269 | Persistent | Non-actionable (healthy signal) |
| 5 | **U3 Question** — clarification requests | 1,936 | Persistent | Reduce with better docs/comments |

These universal categories account for the bulk of review volume. The SCT-specific categories below are where targeted skills have the most impact:

| # | Category | Total | Trend | AI:Human Ratio |
|---|----------|------:|-------|:--------------:|
| 1 | **T11 Config System** | 1,044 | Persistent | 2.5× |
| 2 | **T15 Safety/Retry** (timeouts, long_running) | 652 | Persistent | 0.8× |
| 3 | **T14 Security** (secrets, injection) | 406 | Persistent | 1.2× |
| 4 | **T1 Test Quality** (pytest style, fixtures) | 338 | **Growing ⬆** | **5.7×** |
| 5 | **T5 Scope Hygiene** (out-of-scope changes) | 317 | Persistent | **3.9×** |
| 6 | **T9 Unused/Dead Code** | 223 | Declining ⬇ | **42×** |
| 7 | **T7 Missing Tests** | 214 | Persistent | **3×** |
| 8 | **T3 AI Code Quality** | 63 | **Growing ⬆** | 9× |

## What's Getting Worse

Two categories are **growing** — meaning review comments are increasing quarter over quarter:

1. **T1 Test Quality** — 119 comments in Q1 2026 alone (35% of 7-year total in one quarter). Reviewers repeatedly flag: wrong test style (unittest.TestCase instead of pytest), missing fixtures, missing parametrize.

2. **T3 AI Code Quality** — 33 comments in Q1 2026 (52% of all-time). Direct quotes: *"As always with claude generated PRs I think the tests are low quality"*, *"@copilot this is wrong solution"*.

## Who Reviews What

| Reviewer | Primary Focus | Total Comments |
|----------|---------------|:--------------:|
| **fruch** | Discussion, Directives, Config System | 10,296 |
| **vponomaryov** | Discussion, Process, Scope Hygiene | 4,031 |
| **soyacz** | Discussion, Process, Safety/Retry | 3,458 |
| **roydahan** | Directives, Questions, Opinions | 2,955 |
| **fgelcer** | Discussion, Directives, Process | 2,955 |
| **Copilot** (bot) | Process, Readability, Config System | 1,908 |

## Recommended Actions

### 1. Create `code-review` Skill (HIGH priority)

A checklist-driven skill covering the top 7 SCT-specific categories. Based on 1,500+ historical review comments, this skill would pre-check PRs before human review.

**Target categories:** Test Quality (338), Scope Hygiene (317), Missing Tests (214), Logic Correctness (140), Missing Documentation (137), Error Handling (98), Code Structure (36).

### 2. Enhance AI Code Quality Gates (HIGH priority)

AI-authored PRs have dramatically higher review rates across all categories. The skill should include AI-specific checks: no dead imports, no unittest.TestCase, no mutable default arguments, no commented-out code.

### 3. Automate What's Already Declining (LOW priority)

T9 (Unused/Dead Code) is already declining thanks to CodeQL automation. T8 (Plan Quality) is declining as plan conventions mature. No new skills needed — existing automation is working.

---

*Full report: `scripts/review_analysis/reports/pr-review-taxonomy-report.md`*
*Data: 118MB cache in `scripts/review_analysis/cache/` (upload to S3 with `./upload_cache.sh`)*
