---
marp: true
theme: default
paginate: true
size: 16:9
style: |
  section {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
  }
  h1 {
    color: #1a1a2e;
  }
  h2 {
    color: #16213e;
    border-bottom: 2px solid #e94560;
    padding-bottom: 8px;
  }
  table {
    font-size: 0.75em;
    width: 100%;
  }
  th {
    background-color: #16213e;
    color: white;
  }
  .highlight {
    color: #e94560;
    font-weight: bold;
  }
  .growing {
    color: #e94560;
  }
  .declining {
    color: #0f9b58;
  }
  .persistent {
    color: #f4a261;
  }
  footer {
    font-size: 0.6em;
    color: #666;
  }
---

<!-- _class: lead -->

# 📊 SCT PR Review Taxonomy
## 7 Years of Code Review Data — What It Tells Us

**March 2026**
11,312 PRs · 28,519 Comments · 2019–2026

---

## Why This Analysis?

- We've accumulated **7 years** of PR review comments
- Goal: use **data** to guide AI skill design and review automation
- Pipeline: Collect → Classify → Analyze → Report

### What We Measured

| Metric | Value |
|--------|-------|
| PRs analyzed | 11,312 |
| Review comments | 28,519 |
| Categories | 27 (11 universal + 16 SCT-specific) |
| Time span | 2019 Q1 – 2026 Q1 |
| Active reviewers | 50+ |

---

## The Big Picture: Where Review Time Goes

| # | Category | 7yr Total | What It Is |
|---|----------|----------:|------------|
| 1 | **Directives** | 6,098 | "please fix/add/remove/change..." |
| 2 | **Process** | 3,189 | Backport, CI, merge strategy |
| 3 | **Opinions** | 2,443 | "I think...", "probably..." |
| 4 | **Discussion** | 2,269 | Context sharing, acks, "done" |
| 5 | **Questions** | 1,936 | "Why is this...?", "Does this...?" |
| 6 | **Suggestions** | 1,557 | "Instead of X, use Y" |
| 7 | **Readability** | 1,133 | Naming, formatting, simplification |
| 8 | **Config System** | 1,044 | SCT config validation, defaults |

> **60%** of review comments are directives, process, or discussion — not code quality.

---

## What's Growing ⬆ (Needs Attention)

### T1: Test Quality — **338 total, 119 in Q1 2026 alone**

> *"Dont use classes, if you need tests grouping uses module + separate files"*
> *"This should be a fixture, instead of having to call it in every test"*

Reviewers keep flagging: `unittest.TestCase` → pytest, missing fixtures, missing parametrize.

### T3: AI Code Quality — **63 total, 33 in Q1 2026**

> *"As always with claude generated PRs I think the tests are low quality"*
> *"@copilot this is wrong solution"*

More than half of all-time AI quality complaints happened in the last quarter.

---

## AI vs Human PRs — The Data

234 AI-authored PRs vs 11,078 human-authored PRs.

| Category | Human | AI | **AI Multiple** |
|----------|------:|---:|:--------------:|
| Unused/Dead Code | 0.2% | 8.4% | **42×** |
| AI Code Quality | 0.2% | 1.8% | **9×** |
| Test Quality | 0.9% | 5.1% | **5.7×** |
| Missing Docs | 0.4% | 1.7% | **4.3×** |
| Scope Hygiene | 0.9% | 3.5% | **3.9×** |
| Readability | 3.1% | 11.6% | **3.7×** |
| Config System | 3.2% | 8.0% | **2.5×** |
| Directives | 17.0% | 37.8% | **2.2×** |

**AI PRs get less praise** (0.3% vs 0.9%) and **fewer questions** (3.1% vs 5.8%).
Reviewers don't discuss — they just issue directives.

---

## SCT-Specific Categories: Full Breakdown

| Category | Total | Trend | Top Reviewer |
|----------|------:|:-----:|:-------------|
| T11 Config System | 1,044 | → Persistent | fruch (392) |
| T15 Safety/Retry | 652 | → Persistent | fruch (135) |
| T14 Security | 406 | → Persistent | fruch (76) |
| T1 Test Quality | 338 | ⬆ **Growing** | fruch (113) |
| T5 Scope Hygiene | 317 | → Persistent | fruch (82) |
| T10 Consistency | 281 | → Persistent | Copilot (70) |
| T9 Dead Code | 223 | ⬇ Declining | fgelcer (16) |
| T7 Missing Tests | 214 | → Persistent | vponomaryov (43) |
| T13 Logic Correctness | 140 | → Persistent | soyacz (26) |
| T2 Missing Docs | 137 | → Persistent | Copilot (31) |
| T6 Error Handling | 98 | ↔ Intermittent | fruch (30) |
| T3 AI Code Quality | 63 | ⬆ **Growing** | fruch (26) |

---

## Reviewer Specialization Matrix

| Reviewer | #1 Focus | #2 Focus | #3 Focus | Total |
|----------|----------|----------|----------|------:|
| **fruch** | Discussion | Directives | Opinions | 10,296 |
| **vponomaryov** | Discussion | Process | Directives | 4,031 |
| **soyacz** | Discussion | Process | Directives | 3,458 |
| **roydahan** | Directives | Questions | Opinions | 2,955 |
| **fgelcer** | Discussion | Directives | Process | 2,955 |
| **bentsi** | Directives | Questions | Process | 2,172 |
| **Copilot** | Process | Directives | Readability | 1,908 |
| **pehala** | Opinions | Directives | Questions | 1,744 |

> **fruch** accounts for **28%** of all review comments.

---

## What Real Reviewers Say

### Config System (T11) — 1,044 comments
> *"we shouldn't have empty help values"*
> *"shouldnt this be configurable?"*

### Scope Hygiene (T5) — 317 comments
> *"This far out of scope for this PR, needs to be removed"*
> *"how taking this comment out, is related to this PR?"*

### Logic Correctness (T13) — 140 comments
> *"this code is wrong, seeds need to be a comma separated list"*
> *"If we fail to count them, shouldn't we throw an exception?"*

### Error Handling (T6) — 98 comments
> *"does not use try/finally around the yield, so event_stop will not be called"*

---

## What's Already Working ✅

### Declining Categories (automation is helping)

- **T9 Unused/Dead Code** — handled by CodeQL and github-code-quality bot
- **Formatting** — handled by pre-commit (ruff, autopep8)
- **Commit messages** — handled by commitlint

### Lesson: Automation works. Invest in skills for what can't be linted.

---

## Recommended Actions

### 1. Create `code-review` Skill 🔴 HIGH

Checklist covering top 7 categories (1,500+ historical comments).
Pre-checks PRs before human review.

**Covers:** Test Quality, Scope Hygiene, Missing Tests, Logic Correctness, Missing Docs, Error Handling, Code Structure.

### 2. AI-Specific Quality Gates 🔴 HIGH

AI PRs need stricter pre-review:
- No `unittest.TestCase` (use pytest)
- No dead imports or commented-out code
- No mutable default arguments
- Scope check: only files mentioned in the issue

### 3. Leave declining categories alone 🟢 LOW

T9 and T8 are improving. Don't invest in skills for them.

---

<!-- _class: lead -->

# Next Steps

1. **Review executive summary** — feedback welcome
2. **Code review skill** — in progress, data-backed checklist
3. **Re-run classification** — with expanded 27-category taxonomy
4. **Quarterly updates** — `./run.sh --no-collect` every 3 months

---

## Appendix: Pipeline Architecture

```
┌─────────┐    ┌──────────┐    ┌──────────┐    ┌────────┐
│ collect  │───▶│ classify │───▶│ analyze  │───▶│ report │
│ (gh API) │    │ (regex)  │    │ (trends) │    │ (Marp) │
└─────────┘    └──────────┘    └──────────┘    └────────┘
   7 hrs          5 min           1 min          1 min
   11K PRs        taxonomy.yaml   quarterly      Mermaid
```

**Cache:** 118MB (11,313 raw + 11,312 classified JSON files)
**Taxonomy:** 27 categories with regex patterns
**Report:** Markdown with Mermaid charts

*Source: `scripts/review_analysis/` in PR #14146*
