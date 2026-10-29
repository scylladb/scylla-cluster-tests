# REVIEW.md Template

`REVIEW.md` at the repository root is the review guide. People read it, the
`code-review` skill reads it, and an automated reviewer that looks for a
`REVIEW.md` reads it. Keep it short: it lists the questions, and the
`code-review` skill holds the detail.

Fill each pass with questions, one line each. Take Pass 1 from the
`code-review` checks that find the bugs that the linters and the unit tests
miss. Take Pass 2 from section 7 of `docs/contribution-handbook.md`. Take
Pass 3 from the development flow and from `docs/standards/`.

```markdown
# Review guide

This guide applies to each pull request in SCT, from a person or from an
agent. A reviewer works through the three passes in order.

The `code-review` skill in `skills/code-review/` holds the detailed checks
and the real incidents behind them. The standards are in
[docs/INDEX.md](docs/INDEX.md).

## The outcome of a pass

A review states an outcome for each pass: `pass`, or the findings of that
pass. A pass with no outcome did not run. Report the three outcomes together,
in order, so that a reader sees which pass found what.

## Pass 1 — Bugs and logic

- Does the change do what the pull request description says?
- Which input makes it fail? Name one.
- <The SCT checks for bugs that the linters miss.>

## Pass 2 — Security

- Does the diff carry a credential, a token, a private key, or an internal
  host name?
- <The security-sensitive areas of SCT.>

## Pass 3 — Compliance

- When the pull request carries an idea directory in
  `docs/plans/<feature>/<idea>/`, does the change stay inside its spec?
- Does a bug fix state its root cause in the commit message and the pull
  request description?
- Does the change follow the standards in `docs/standards/`?
- <The commit and label rules.>

## Volume

Report each finding that changes the code. Group the small findings in one
comment. A review with twenty small findings hides the one that matters.

Findings do not block a merge alone. A maintainer approves the merge.
```
