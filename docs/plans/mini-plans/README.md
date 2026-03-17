# Mini-Plans

Lightweight implementation plans for small changes (under ~1K LOC, single PR).

## When to Use

Use a mini-plan instead of a full 7-section plan when:
- The change fits in a single PR
- Estimated LOC is under ~1K
- No multi-phase tracking is needed
- The PR does NOT have a `plans` label

## Format

Mini-plans use a 4-section format: Problem, Approach, Files to Modify, Verification. No YAML frontmatter, no MASTER.md registration, no progress.json entry.

See `skills/writing-plans/references/mini-plan-template.md` for the template.

## Naming Convention

Files are named `YYYY-MM-DD-kebab-case-name.md` (ISO dates sort chronologically).

Example: `2026-03-17-add-retry-logic-to-health-check.md`

## Cleanup Policy

Mini-plans are disposable. Delete them after the related PR is merged or after 30 days, whichever comes first.
