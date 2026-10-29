---
name: discovering-standards
description: >-
  Discovers the coding standards of SCT from its configuration files, its
  documentation, and its code. Owns docs/standards/, the docs/INDEX.md index,
  and REVIEW.md, the review guide for reviewers and the code-review skill.
  Use when the user asks to discover, refresh, or audit the standards, to regenerate docs/INDEX.md or REVIEW.md,
  or when the standards drift from the code. Not for one new convention (use
  updating-standards) or for a review of a pull request (use code-review).
argument-hint: "[domain]"
---

# Discovering Standards

Find the conventions that SCT already follows. Write them to
`docs/standards/<domain>/<topic>.md`, index them in `docs/INDEX.md`, and
generate `REVIEW.md` from them.

## Essential Principles

### Evidence, Not Taste

Each standard comes from a source that a reader can check. The source is a
configuration rule, a stated rule in the documentation, or a pattern that
most of the code follows. A standard without evidence is an opinion, and reviewers will not
apply it the same way.

### Keep What Exists

Read the existing `docs/standards/` first. Add to a topic file and keep its
other sections. A rerun must not drop a standard that the team agreed on.

### Name the Sources

Each topic file ends with a `Sources:` line. When a source changes, a reader
finds the standard that must change too.

## Phase 1: Read the Sources

**Entry criteria**: the user wants the standards discovered or refreshed.

Read the sources in this order. When the arguments name a domain, read only
the sources of that domain.

1. Configuration: `pyproject.toml`, `.pre-commit-config.yaml`,
   `commitlint.config.js`, `.github/workflows/`, `Jenkinsfile`.
2. Documentation: `AGENTS.md`, `CLAUDE.md`, `.github/copilot-instructions.md`,
   `docs/contribution-handbook.md`, the convention files under `docs/` such as
   `docs/http-retry-conventions.md`, and the `skills/*/SKILL.md` files. Take
   only the stated rules: "always", "never", "must", "use", "prefer". Do not
   infer a rule from one example.
3. Code: count a pattern with `git grep -l` over `sdcm/` and `unit_tests/`.
   A pattern is a standard when most of the files follow it.

**Exit criteria**: each finding has a domain, a name, a one-line rule, and
its evidence.

## Phase 2: Score and Propose

**Entry criteria**: Phase 1 complete.

1. Score each finding. A configuration rule scores 70 to 85. A documented
   rule scores 80 to 92. A code pattern scores 60 to 88, by the share of files
   that follow it. Add 10 when a second source agrees, up to 100.
2. Merge two findings with the same domain and name. Flag two findings that
   contradict each other, with both sources.
3. Use the domains that exist under `docs/standards/`: `global`, `python`,
   `testing`, and `config`. Add a domain only when a group of findings fits
   none of them.
4. Show one table for each domain: name, score, sources, and rule. Ask the
   user which findings to write, and which side of each contradiction holds.

**Exit criteria**: the user has approved the findings to write.

## Phase 3: Write the Standards

**Entry criteria**: Phase 2 complete.

1. Group the approved findings of a domain into topic files, as in
   `python/coding-style.md`. A file opens with `## <Topic>`. Each standard is
   one `### <Name>` section with one to ten lines of rule and, when it helps,
   one example under ten lines.
2. End each file with a `Sources:` line.
3. Write `docs/INDEX.md` in the shape of
   [index-template.md](references/index-template.md). The paragraph of each
   entry names every practice in the file.
4. Generate `REVIEW.md` at the repository root from
   [review-template.md](references/review-template.md). Keep the three passes.
   Fill each pass from the standards and from the checks of
   `skills/code-review/SKILL.md` that find bugs that the linters miss.
5. Write the prose in ASD-STE100 Simplified Technical English.

**Exit criteria**: the topic files, `docs/INDEX.md`, and `REVIEW.md` agree.

## Phase 4: Report

**Entry criteria**: Phase 3 complete.

1. List the files written and the findings dropped, with the reason for each
   drop.
2. List the contradictions and the side that holds. A contradiction in a
   source file, as in `.github/copilot-instructions.md`, needs a fix there
   too.
3. Run `uv run sct.py pre-commit`, and suggest a commit.

**Exit criteria**: the user has the report.

## When NOT to Use

- One new convention from a review or a conversation. Use
  `updating-standards`.
- A review of a pull request. Use `code-review`, which reads `REVIEW.md`.
