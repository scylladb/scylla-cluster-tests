# Claude review of SCT pull requests on request

**Jira:** SCT-1457

## Problem

SCT has no Claude review that a contributor can request on a pull request.
The `claude.yml` workflow runs Claude on each `@claude` mention, but it has no
review mode. It takes the comment as a free task, and it has write access to
the repository.

The `code-review` skill in `skills/` holds the SCT checks. Its CI output is
one top-level comment with no inline comments, so findings do not attach to
lines and cannot be resolved.

A stronger review method exists only as a personal skill, `/fruch-review`, in
one maintainer's `~/.claude/skills`. CI and other contributors cannot use it.
It reads whole files and callers, checks scope and the spec, tags each finding
with a severity, and posts one review with inline comments.

## Who it affects

- Contributors who want a review before a maintainer looks at the pull
  request.
- Maintainers, who now review without a first pass that applies `REVIEW.md`.
- The AI reviewer trial of SCT-1393, which cannot include Claude.

## Evidence

The automatic Claude review was removed in commit `4129f1ba75`:

> Remove the claude-code-review.yml workflow that ran a review on every PR.
> Reviews are now opt-in: mention @claude on a PR to request one (handled by
> the existing claude.yml workflow).

`claude.yml` gives the job `contents: write` and passes the comment to Claude
as the task. It has no review prompt.

`skills/code-review/workflows/review-a-pr.md`, Phase 6:

> Post a **single top-level PR comment** using `gh pr comment`. Do NOT post
> inline review comments or use the GitHub review API.

On pull request #16900, a `/fruch-review` run found that the Qodo
configuration kept all Qodo findings out of review threads, so the trial
could not measure Qodo. CodeRabbit and Qodo reviewed the same pull request and
did not find it.

## Definition of done

- A repository skill holds the review method. Claude Code on a laptop and
  Claude in CI use the same skill. `REVIEW.md` stays the only source of the
  rules.
- An org member comments `@claude review` on a pull request. One Claude
  review appears, with inline comments that carry a severity tag, within the
  `REVIEW.md` volume rule.
- The review job cannot push, approve, request changes, or run code from the
  pull request.
- The spec records, for Copilot, Codex, and the Gemini CLI action, whether
  each can use the same skill, how it starts, and what it costs.

## Out of scope

- An automatic review on each pull request.
- Workflows for Copilot, Codex, or Gemini. This change only records whether
  they can reuse the skill.
- Checks against other repositories, such as scylladb or scylla-pkg.
- The how-to comment, the handbook table, and the trial of SCT-1393. They
  change after pull request #16900 merges.
