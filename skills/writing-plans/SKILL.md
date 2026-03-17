---
name: writing-plans
description: >-
  Use when asked to generate an implementation plan, draft a plan, save a plan,
  or design a feature rollout for the SCT repository. Supports two formats:
  full 7-section plans for multi-phase work (1K+ LOC, tracked in MASTER.md)
  and lightweight mini-plans for single-PR changes (under 1K LOC, stored in
  docs/plans/mini-plans/). Routes automatically based on PR plans label,
  user input, or task size estimate.
---

# Writing Implementation Plans for SCT

Create well-structured implementation plans that follow SCT's 7-section format and enable incremental, PR-scoped development.

## Essential Principles

### Authoritative Source

**`docs/plans/INSTRUCTIONS.md` is the authoritative source for plan structure.**

This skill supplements — not replaces — the official instructions. Always read `docs/plans/INSTRUCTIONS.md` before writing a plan. If there is a conflict between this skill and `INSTRUCTIONS.md`, follow `INSTRUCTIONS.md`.

### Code-Verified Current State

**The Current State section must reference real files, classes, and methods.**

Use file-reading tools to inspect actual code before writing Current State. Do not guess or hallucinate file names. Every path mentioned must resolve to an existing file in the repository.

### PR-Scoped Phases

**Each implementation phase should be scoped to a single Pull Request.**

Large phases that span multiple PRs are hard to review and test. Break work into atomic phases with clear Definition of Done criteria. Within a PR, split logically distinct changes into separate commits for easier review. Order phases by dependency — foundational refactoring before feature implementation.

### Measurable Goals

**Goals and success criteria must be measurable, not vague.**

"Improve performance" is not a goal. "Reduce health check time from 120 minutes to under 12 minutes for 60-node clusters" is a goal. Include numbers, thresholds, or verifiable conditions.

### Investigation Over Assumption

**Mark unclear requirements as "Needs Investigation" instead of assuming.**

When information is missing or ambiguous, explicitly flag it. Incorrect assumptions in a plan propagate through the entire implementation and cause rework.

### Every Plan Must Have Status Metadata

**All plans require YAML frontmatter with status, domain, and dates.**

Plans without metadata are invisible to the tracking system. Every plan file must start with frontmatter specifying `status`, `domain`, `created`, `last_updated`, and `owner` fields. This enables `MASTER.md` and `progress.json` to reflect accurate state. See [frontmatter-fields.md](references/frontmatter-fields.md) for valid values.

## Plan Type Routing

When the user asks to "save a plan", "draft a plan", or similar, determine the plan type **before writing anything**.

### Decision Tree

```
1. User explicitly said big/small?       -> Use that
2. Working on a PR with `plans` label?   -> FULL plan
3. Working on a PR without `plans` label? -> MINI-plan
4. No PR context?                        -> Ask user, or estimate: >=1K LOC -> FULL, <1K LOC -> MINI
```

### Step 1: Ask the User (Preferred)

When explicitly asked to create/save a plan, ask:

> "Is this a big plan (multi-phase, 1K+ LOC, needs tracking) or a small plan (single PR, under ~1K LOC)?"

The user's answer is authoritative. If they say "small" -> mini-plan. If they say "big" -> full plan.

**When user says "big":** If already working on a PR, add the `plans` label immediately:
```bash
gh pr edit <number> --add-label "plans"
```
If a PR hasn't been opened yet, include a reminder in the plan output: "Remember to add the `plans` label when the PR is created." The agent should also add the label automatically when it later creates the PR during that session.

### Step 2: PR Label Check (When Working on an Existing PR)

If the agent is already working on a PR (e.g., from `gh pr checkout`), check for the `plans` label:

```bash
gh pr view <number> --json labels --jq '.labels[].name' | grep -q '^plans$'
```

- PR has `plans` label -> FULL plan (7-section, `docs/plans/`)
- PR does NOT have `plans` label -> MINI-plan (4-section, `docs/plans/mini-plans/`)

### Step 3: Fallback Heuristic (Only if No User Answer and No PR Context)

Estimate the scope from the task description:

- **Likely big:** Multiple new files, new config parameters, multiple backends affected, needs CI changes
- **Likely small:** Single file change, bug fix, test addition, refactoring one module
- **When in doubt, ask the user** rather than guessing

## When to Use

- When asked to "generate an implementation plan", "draft a plan", or "save a plan"
- When designing a multi-phase feature rollout for SCT (-> full plan)
- When a complex change needs to be broken into PR-scoped steps (-> full plan)
- When documenting a refactoring strategy before implementation (-> full plan)
- When creating a roadmap for a new SCT capability (-> full plan)
- When reviewing or improving an existing plan
- When a small, single-PR change benefits from lightweight planning (-> mini-plan)
- When working on a PR without a `plans` label and want to document approach (-> mini-plan)

## When NOT to Use

- For trivial changes that need no planning at all — just implement directly
- For regular coding questions — answer them without plan structure
- For documentation-only changes — edit the relevant file directly
- For updating an existing plan's status — edit the plan file directly

## The 7-Section Structure

Every SCT plan follows this structure. See [plan-templates.md](references/plan-templates.md) for detailed templates and examples.

| # | Section | Purpose | Key Rule |
|---|---------|---------|----------|
| 1 | Problem Statement | What and why | Include measurable pain points |
| 2 | Current State | What exists today | MUST reference real files (verify with tools) |
| 3 | Goals | What success looks like | Numbered, measurable objectives |
| 4 | Implementation Phases | How to get there | PR-scoped (≤200 LOC), Importance levels, DoD per phase |
| 5 | Testing Requirements | How to verify | Unit, integration, and manual testing planned upfront |
| 6 | Success Criteria | How to know it's done | References DoD items; add plan-level criteria only if needed |
| 7 | Risk Mitigation | What could go wrong | Likelihood, impact, mitigation for each risk |

## Plan File Conventions

| Convention | Rule |
|-----------|------|
| Location | `docs/plans/` directory |
| Filename | kebab-case, descriptive (e.g., `health-check-optimization.md`) |
| Format | Markdown with `# Plan Title` as first line |
| Archiving | Move completed plans to `docs/plans/archive/` |

## SCT-Specific Considerations

When writing plans for SCT, consider these domain-specific areas:

| Area | What to Include |
|------|----------------|
| **Backends** | Which backends are affected? (AWS, GCE, Azure, Docker, K8S, Baremetal) |
| **Configuration** | New parameters in `sdcm/sct_config.py` with defaults in `defaults/test_default.yaml` |
| **Nemesis** | Impact on chaos operations in `sdcm/nemesis.py` and `sdcm/nemesis_registry.py` |
| **Monitoring** | Changes to Prometheus metrics, Grafana dashboards, or Argus reporting |
| **CI/CD** | Jenkins pipeline changes in `jenkins-pipelines/` |
| **Provision labels** | Which provision test labels to add (`provision-aws`, `provision-gce`, etc.) |
| **MASTER.md** | Register the plan in `docs/plans/MASTER.md` under the correct domain |
| **progress.json** | Add an entry to `docs/plans/progress.json` with plan metadata |
| **Related Plans** | Check MASTER.md for existing plans in the same domain that may overlap |

## Mini-Plan Format

Mini-plans are a lightweight 4-section format for small changes. See [mini-plan-template.md](references/mini-plan-template.md) for the full template and example.

| Aspect | Full Plan | Mini-Plan |
|--------|-----------|-----------|
| Sections | 7 | 4 (Problem, Approach, Files, Verification) |
| Frontmatter | Required | None |
| MASTER.md | Required | None |
| progress.json | Required | None |
| Location | `docs/plans/<domain>/` | `docs/plans/mini-plans/` |
| Filename | `kebab-case-name.md` | `YYYY-MM-DD-kebab-case-name.md` |
| Lifecycle | Tracked until archived | Disposable after PR merge or 30 days |

## Anti-Pattern Quick Reference

See [anti-patterns.md](references/anti-patterns.md) for the full catalog with before/after examples and the quick-reference table.

## Reference Index

| File | Content |
|------|---------|
| [plan-templates.md](references/plan-templates.md) | Complete plan skeleton, section-by-section templates with SCT-specific examples |
| [anti-patterns.md](references/anti-patterns.md) | Common plan writing mistakes with before/after fixes |
| [frontmatter-fields.md](references/frontmatter-fields.md) | YAML frontmatter field definitions, valid values, and domain taxonomy |
| [mini-plan-template.md](references/mini-plan-template.md) | Mini-plan 4-section template with example and rules |

| Workflow | Purpose |
|----------|---------|
| [create-a-plan.md](workflows/create-a-plan.md) | 5-phase process for writing a full implementation plan from scratch |
| [create-a-mini-plan.md](workflows/create-a-mini-plan.md) | 3-phase lightweight workflow for writing a mini-plan |
| [update-plan-status.md](workflows/update-plan-status.md) | 3-phase workflow for updating plan status across frontmatter, MASTER.md, and progress.json |

## Supporting Documents

| Document | Role |
|----------|------|
| `docs/plans/INSTRUCTIONS.md` | Authoritative source for plan structure and guidelines |
| `docs/plans/infrastructure/health-check-optimization.md` | Reference example: performance optimization plan |
| `docs/plans/nemesis/nemesis-rework.md` | Reference example: large-scale refactoring plan |

## Success Criteria

### Full Plan

- [ ] Passes `uv run sct.py pre-commit` without formatting issues
- [ ] Follows the 7-section structure from `docs/plans/INSTRUCTIONS.md`
- [ ] Has a Problem Statement with measurable pain points
- [ ] Has a Current State section with verified file/class/method references
- [ ] Has numbered, measurable goals
- [ ] Has PR-scoped implementation phases (≤200 LOC each) with Importance levels and Definition of Done
- [ ] Includes a documentation update phase or deliverable (docs, config regeneration, guides)
- [ ] Has testing requirements covering unit, integration, and manual testing
- [ ] Has success criteria that reference DoD items (no duplication)
- [ ] Has risk mitigation with likelihood, impact, and mitigation for each risk
- [ ] Marks unclear requirements as "Needs Investigation"
- [ ] Is saved in `docs/plans/` with a kebab-case filename
- [ ] Has valid YAML frontmatter (status, domain, created, last_updated, owner)
- [ ] Is registered in `docs/plans/MASTER.md` under the correct domain
- [ ] Has a corresponding entry in `docs/plans/progress.json`

### Mini-Plan

- [ ] Has exactly 4 sections: Problem, Approach, Files to Modify, Verification
- [ ] Has no YAML frontmatter
- [ ] All file paths in "Files to Modify" are code-verified (or marked as new)
- [ ] Verification checklist has concrete, runnable checks
- [ ] Includes `uv run sct.py pre-commit` in verification
- [ ] Is saved as `docs/plans/mini-plans/YYYY-MM-DD-kebab-case-name.md`
- [ ] Is NOT registered in MASTER.md or progress.json
