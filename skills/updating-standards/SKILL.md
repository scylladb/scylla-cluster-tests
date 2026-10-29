---
name: updating-standards
description: >-
  Adds or changes one coding standard of SCT in docs/standards/, from a
  description or from the conversation, and updates its entry in
  docs/INDEX.md and, when the review must check it, REVIEW.md. Use when the
  user states a convention, says "add this as a standard" or "remember this
  rule for the repo", when a review settles a convention, or when the same
  fix repeats across files. Not for a full discovery of all the standards
  (use discovering-standards).
argument-hint: "[the convention, in one sentence]"
---

# Updating a Standard

Write one convention into the standard file that holds its topic. Then
update the entry of that file in `docs/INDEX.md`.

## Phase 1: Find the Convention

**Entry criteria**: the user wants a standard added or changed.

1. Take the convention from the arguments.
2. When they are empty, take it from the recent conversation. Look for a rule
   that the user stated, a fix that repeated, or a review comment that
   settled a question. When neither gives one, ask the user.
3. When `docs/standards/` does not exist, stop and point the user to
   `discovering-standards`.

**Exit criteria**: the convention is one clear rule with its source.

## Phase 2: Write It

**Entry criteria**: Phase 1 complete.

1. List `docs/standards/*/*.md`. Pick the file whose topic holds the
   convention, and read it. When two files fit, or none, ask the user.
   Give three options: an existing file, a new file, or a new domain.
2. Write the convention as one `### <Name>` section with one to ten lines of
   rule. Add one example under ten lines when it helps. When a section on the
   same rule exists, change that section. Start a new file with
   `## <Topic>`.
3. Add the source to the `Sources:` line at the end of the file.
4. Update the entry of the file in `docs/INDEX.md`, in the shape of
   `skills/discovering-standards/references/index-template.md`. The
   paragraph names every practice that the file now holds. Add the entry and
   the domain heading when they are absent.
5. A reviewer can need to check the convention on each pull request. Then
   add one question for it to the right pass of `REVIEW.md`.
6. Write the prose in ASD-STE100 Simplified Technical English.

**Exit criteria**: the standard, the index entry, and `REVIEW.md` agree.

## Phase 3: Report

**Entry criteria**: Phase 2 complete.

1. Show the section, the index entry, and the `REVIEW.md` line when you
   added one.
2. Run `uv run sct.py pre-commit`, and suggest a commit.

**Exit criteria**: the user has seen the change.

## When NOT to Use

- A discovery of all the standards. Use `discovering-standards`.
- A rule that applies to one change only. Put it in the spec of that change.
