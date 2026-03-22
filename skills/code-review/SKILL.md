---
name: code-review
description: >-
  Guides SCT code review by checking PRs against 7 data-backed categories from
  11,312 PRs and 28,519 review comments. Use when asked to review code, run a
  code review checklist, pre-review a PR, check what reviewers would flag, or
  audit changes for common SCT issues. Covers test quality, scope hygiene,
  missing tests, logic correctness, documentation, error handling, and code
  structure. AI-authored PRs receive 2-42x more review comments than
  human-authored PRs in these categories.
---

# Code Review for SCT

Apply 7 data-backed review categories to catch the issues SCT reviewers flag most often.

## Essential Principles

### 1. Check Test Quality First (T1 — 338 occurrences, GROWING)

**#1 review category because SCT's test suite is the product — bad test structure is a bug.**

Tests must use pytest functions (not classes), fixtures (not setUp), and `@pytest.mark.parametrize`
for variations. Classes are only acceptable when grouping requires shared state that fixtures cannot
provide. Every violation is a review comment.

> "Dont use classes, if you need tests grouping uses module + separate files"
> "This should be a fixture, instead of having to call it in every test"
> "can be made into one test with parametrization probably"

### 2. Enforce Scope Hygiene (T5 — 317 occurrences, PERSISTENT)

**PRs that bundle unrelated changes hide intent, inflate review burden, and create merge conflicts.**

Every change in a PR must be traceable to the PR's stated goal. Unrelated fixes, cleanups, or
refactors — however small — belong in separate PRs. Reviewers block PRs with out-of-scope changes,
not just comment on them.

> "This far out of scope for this PR, needs to be removed and put into its own PR"
> "how taking this comment out, is related to this PR?"

### 3. Demand Unit Tests for New Logic (T7 — 214 occurrences, PERSISTENT)

**New parsing, config handling, and utility logic without tests creates permanent blind spots.**

If a function parses input, transforms data, validates config, or implements non-trivial logic, it
needs a unit test. Reviewers specifically check for missing tests when they see new functions in
`sdcm/`, `utils/`, or config paths.

> "there isn't a unit test asserting the parsing behavior"
> "Do you think we could unit test this as well?"

### 4. Verify Correctness at Boundaries (T13 — 140 occurrences, PERSISTENT)

**Wrong data types and formats silently corrupt test runs — they don't raise exceptions.**

Check: Are list values rendered as comma-separated strings (not Python lists) where APIs expect
strings? Do functions return the right type? Are edge cases handled (empty input, zero count,
single item)? Returning a wrong default (e.g., `0` instead of raising) can make tests pass
incorrectly.

> "this code is wrong, seeds need to be a comma separated list of addresses, not an actual list"
> "If we fail to count them, shouldn't we throw an exception? By returning zero the test might pass"

### 5. Require Documentation for Non-Obvious Code (T2 — 137 occurrences, PERSISTENT)

**Empty help text and missing 'why' comments make maintenance impossible at scale.**

Config parameters in `sct_config.py` must have non-empty `description` fields. Magic numbers,
limits, and thresholds must have inline comments explaining the reasoning. Functions with
non-obvious behavior need Google-style docstrings.

> "write the descriptions for those, we shouldn't have empty help values"
> "why max is 10? what makes it reasonable? please add clear comment about the why"

## When to Use

- Reviewing a PR before submitting it ("pre-review my changes")
- Auditing someone else's PR for common SCT review patterns
- Answering "what would reviewers flag in this change?"
- Running a structured review checklist on a diff
- Checking AI-generated code before committing (AI PRs get 2-42x more comments)
- Validating that new config parameters are properly documented

## When NOT to Use

- Writing a new unit test — use the `writing-unit-tests` skill instead
- Fixing merge conflicts in a backport PR — use the `fix-backport-conflicts` skill
- Profiling slow code — use the `profiling-sct-code` skill
- Writing an implementation plan — use the `writing-plans` skill

## Quick Reference: Review Checklist

| Category | ID | Frequency | What to Check |
|----------|----|-----------|---------------|
| Test Quality | T1 | 338 — GROWING | No `TestCase` classes; fixtures not setUp; parametrize for variations |
| Scope Hygiene | T5 | 317 — PERSISTENT | Every change maps to PR goal; no cleanup bundled in |
| Missing Tests | T7 | 214 — PERSISTENT | New parsing/config/utility logic has unit tests in `unit_tests/` |
| Logic Correctness | T13 | 140 — PERSISTENT | Data types match API contracts; edge cases handled; no silent zero-returns |
| Documentation | T2 | 137 — PERSISTENT | Config descriptions non-empty; magic numbers commented; docstrings present |
| Error Handling | T6 | 98 — INTERMITTENT | try/finally around yields; error events raised; inputs validated |
| Code Structure | T4 | 36 — PERSISTENT | No inner functions; no mutable defaults; constants centralized |

### AI-Authored Code: Heightened Review

AI-written PRs in SCT receive significantly more review comments than human-authored PRs:

| Category | AI Rate | Human Rate | Multiplier |
|----------|---------|------------|------------|
| Unused/Dead Code | 8.4% | 0.2% | 42x |
| Readability | 11.6% | 3.1% | 3.7x |
| Test Quality | 5.1% | 0.9% | 5.7x |
| Scope Hygiene | 3.5% | 0.9% | 3.9x |

Apply all 7 checklist categories rigorously to AI-generated code.

## Reference Index

| File | Content |
|------|---------|
| [test-quality-checklist.md](references/test-quality-checklist.md) | T1 + T7: pytest conventions, fixture patterns, parametrize, when to add tests |
| [scope-and-structure.md](references/scope-and-structure.md) | T5 + T4: PR scope enforcement, inner functions, mutable defaults, config centralization |
| [correctness-and-safety.md](references/correctness-and-safety.md) | T13 + T6 + T2: data format bugs, error handling, try/finally, docstrings, config help |

## Success Criteria

A reviewed SCT PR should:

- [ ] All tests use pytest functions — no `unittest.TestCase` or `setUp` methods
- [ ] Fixtures replace repeated setup code; `@pytest.mark.parametrize` for variations
- [ ] Every change is traceable to the PR's stated goal — no bundled unrelated changes
- [ ] New parsing, config, or utility functions have unit tests in `unit_tests/`
- [ ] List/dict values passed to APIs are in the correct serialized format (not raw Python objects)
- [ ] Edge cases handled — empty inputs, zero counts, single-item collections
- [ ] All `sct_config.py` parameters have non-empty `description` fields
- [ ] Magic numbers and limits have inline comments explaining "why"
- [ ] `try/finally` used around context manager yields to guarantee cleanup
- [ ] No inner (nested) functions unless strictly necessary
- [ ] No mutable default arguments (no `def f(x=[]): ...`)
- [ ] All imports at the top of the file — no inline imports

## Trigger Eval Queries

### Should Trigger This Skill

- "Review this PR for common issues"
- "What would reviewers flag in this change?"
- "Pre-review my code before I submit"
- "Run a code review checklist on my changes"
- "Check this diff for SCT review patterns"
- "Is my PR ready to submit?"
- "What's typically flagged in SCT PRs?"

### Should NOT Trigger This Skill

- "Write a unit test for config parsing" → use `writing-unit-tests`
- "Fix the merge conflict in this backport PR" → use `fix-backport-conflicts`
- "Profile why this test is slow" → use `profiling-sct-code`
- "Create an implementation plan for this feature" → use `writing-plans`
- "How do I mock AWS in a unit test?" → use `writing-unit-tests`
