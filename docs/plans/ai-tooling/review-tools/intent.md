# One review source for the AI pull request reviewers

**Jira:** SCT-1393

## Problem

SCT uses three AI reviewers on pull requests: CodeRabbit, Qodo, and GitHub
Copilot. Each one reads a different set of rules.

- CodeRabbit reads `REVIEW.md` and `docs/standards/`. Its configuration also
  restates the same rules in eleven path instructions.
- Qodo has no review instructions. Its configuration only sets the trigger.
- Copilot reads `.github/copilot-instructions.md`, which restates the rules in
  its own words. One of them contradicts the standard: it tells the author to
  squash the commits, and the standard tells the author to push each review
  fix as a new commit.
- `AGENTS.md` restates the code style, override safety, and unit test rules.
  CodeRabbit and Copilot read `AGENTS.md` and `.github/copilot-instructions.md`
  by default, so each copy reaches the review.

When a rule changes in one place, the other copies stay old, and the
reviewers give different advice.

The reviewers also post output that we do not use: lint findings that
pre-commit already reports, docstring and unit test generation, pre-merge
checks on the title and the description, effort estimates, and related issue
lists. This output hides the findings that block a merge.

No reviewer runs automatically, and the authors do not request them. The
contribution handbook says that CodeRabbit and Copilot review each pull
request automatically. That is not true.

## Who it affects

The SCT contributors who open pull requests, the maintainers who review them,
and the agents that read `AGENTS.md` and `.github/copilot-instructions.md`.

## Evidence

The review rules live in six places: `AGENTS.md`,
`.github/copilot-instructions.md`, `docs/contribution-handbook.md`, the
`code-review` skill, `docs/standards/`, and `REVIEW.md`
([PR #16699 review thread](https://github.com/scylladb/scylla-cluster-tests/pull/16699#discussion_r4451916188)).

`.coderabbit.yaml`:

```yaml
  auto_review:
    enabled: false
```

`.pr_agent.toml`:

```toml
[github_app]
# Commands run automatically when a PR is opened or updated. Empty = none.
pr_commands = []
```

`docs/contribution-handbook.md`, section 4.3:

> **Automated code review** — PRs are reviewed automatically by CodeRabbit and
> GitHub Copilot.

The reviews and comments on the last 100 pull requests (2026-11-08) have no
author from CodeRabbit, Qodo, or Copilot.

## Definition of done

- CodeRabbit, Qodo, and Copilot read the same review source: `REVIEW.md` and
  `docs/standards/`. No reviewer configuration restates a rule.
- `AGENTS.md` and `.github/copilot-instructions.md` index the standards, and
  do not restate them.
- The three reviewers follow the same volume rule: the blockers first, three
  to five findings, and the small findings grouped.
- CodeRabbit and Qodo post no output outside the three passes of `REVIEW.md`.
- Each new pull request gets one comment that tells the author how to request
  each reviewer.
- One reviewer runs automatically. The spec records how we chose it, with the
  data.
- The handbook states the trigger of each reviewer correctly.

## Out of scope

- A new AI reviewer.
- A change to the review rules themselves.
- A merge gate that depends on a reviewer.
- The `code-review` skill. It keeps the detailed checks and the incidents
  behind them, and it already points to `REVIEW.md`.
