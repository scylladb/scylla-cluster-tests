---
name: writing-plans
description: >-
  Guides the creation of implementation plans for the SCT repository
  following the 7-section structure (Problem Statement, Current State,
  Goals, Implementation Phases, Testing Requirements, Success Criteria,
  Risk Mitigation). Use when asked to generate an implementation plan,
  draft a plan, or design a phased feature rollout.
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

Large phases that span multiple PRs are hard to review and test. Break work into atomic phases with clear Definition of Done criteria. Order phases by dependency — foundational refactoring before feature implementation.

### Measurable Goals

**Goals and success criteria must be measurable, not vague.**

"Improve performance" is not a goal. "Reduce health check time from 120 minutes to under 12 minutes for 60-node clusters" is a goal. Include numbers, thresholds, or verifiable conditions.

### Investigation Over Assumption

**Mark unclear requirements as "Needs Investigation" instead of assuming.**

When information is missing or ambiguous, explicitly flag it. Incorrect assumptions in a plan propagate through the entire implementation and cause rework.

## When to Use

- When asked to "generate an implementation plan" or "draft a plan"
- When designing a multi-phase feature rollout for SCT
- When a complex change needs to be broken into PR-scoped steps
- When documenting a refactoring strategy before implementation
- When creating a roadmap for a new SCT capability
- When reviewing or improving an existing plan

## When NOT to Use

- For small, single-PR changes that don't need a plan — just implement directly
- For regular coding questions — answer them without plan structure
- For documentation-only changes — edit the relevant file directly
- For bug fixes that have an obvious solution — file a PR instead
- For updating an existing plan's status — edit the plan file directly

## The 7-Section Structure

Every SCT plan follows this structure. See [plan-templates.md](references/plan-templates.md) for detailed templates and examples.

| # | Section | Purpose | Key Rule |
|---|---------|---------|----------|
| 1 | Problem Statement | What and why | Include measurable pain points |
| 2 | Current State | What exists today | MUST reference real files (verify with tools) |
| 3 | Goals | What success looks like | Numbered, measurable objectives |
| 4 | Implementation Phases | How to get there | PR-scoped, ordered by dependency, DoD per phase |
| 5 | Testing Requirements | How to verify | Unit, integration, manual, performance |
| 6 | Success Criteria | How to know it's done | Measurable outcomes, not vague "it works" |
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

## Anti-Pattern Quick Reference

The most common mistakes. Full catalog with before/after examples in [anti-patterns.md](references/anti-patterns.md).

| ID | Anti-Pattern | One-Line Fix |
|----|-------------|-------------|
| PAP-1 | Too much code in the plan | Use short snippets for interfaces only; describe behavior in prose |
| PAP-2 | Missing diagrams for complex interactions | Add ASCII/Mermaid diagrams for 3+ component interactions |
| PAP-3 | Overly granular phases | Describe deliverables, not line-by-line changes |
| PAP-4 | Vague or unmeasurable goals | Add measurable criteria or verifiable conditions |
| PAP-5 | Conflicting requirements | Cross-check goals, phases, and risks for consistency |
| PAP-6 | Missing code references in Current State | Cite specific file paths, verify they exist |
| PAP-7 | Phases without Definition of Done | Add checkbox criteria to every phase |
| PAP-8 | Monolithic phase spanning multiple PRs | Split into single-PR-scoped phases |

## Reference Index

| File | Content |
|------|---------|
| [plan-templates.md](references/plan-templates.md) | Complete plan skeleton, section-by-section templates with SCT-specific examples |
| [anti-patterns.md](references/anti-patterns.md) | Common plan writing mistakes with before/after fixes |

| Workflow | Purpose |
|----------|---------|
| [create-a-plan.md](workflows/create-a-plan.md) | 5-phase process for writing an implementation plan from scratch |

## External References

| Document | Role |
|----------|------|
| `docs/plans/INSTRUCTIONS.md` | Authoritative source for plan structure and guidelines |
| `docs/plans/health-check-optimization.md` | Reference example: performance optimization plan |
| `docs/plans/nemesis-rework.md` | Reference example: large-scale refactoring plan |

## Success Criteria

A well-written SCT implementation plan:

- [ ] Follows the 7-section structure from `docs/plans/INSTRUCTIONS.md`
- [ ] Has a Problem Statement with measurable pain points
- [ ] Has a Current State section with verified file/class/method references
- [ ] Has numbered, measurable goals
- [ ] Has PR-scoped implementation phases with Definition of Done
- [ ] Has testing requirements covering unit, integration, and manual testing
- [ ] Has measurable success criteria (not vague)
- [ ] Has risk mitigation with likelihood, impact, and mitigation for each risk
- [ ] Marks unclear requirements as "Needs Investigation"
- [ ] Is saved in `docs/plans/` with a kebab-case filename
- [ ] Passes `uv run sct.py pre-commit` without formatting issues
