# Development flow skills in SCT

**Jira:** SCT-1379

## Problem

Large SCT pull requests, and pull requests that change the SCT architecture,
often reach the reviewers with no written problem and no design. The
reviewer then rebuilds the intent from the diff, and the review checks the
code against nothing.

SCT also has no index of its coding standards. The rules are spread over
`AGENTS.md`, `.github/copilot-instructions.md`,
`docs/contribution-handbook.md`, and the skills. There is no short review
guide that people and agents share.

The QA Tools team uses this flow in its own repositories. SCT has
contributors from many teams, and the flow must work for each of them with no
setup.

## Who it affects

The SCT contributors who write big changes, the maintainers who review them,
and the agents that work in the repository.

## Evidence

The QA Tools team uses the `qatools-sdlc` plugin in most of its repositories.
SCT has no `docs/standards/`, no `docs/INDEX.md`, and no `REVIEW.md`.

## What good looks like

- An agent in a fresh SCT checkout, with no plugin installed, writes an
  intent, a spec, and a plan in `docs/plans/<feature>/<idea>/`.
- The agent proposes them for a big or architectural change, and does not
  force them on a small one.
- `docs/standards/`, `docs/INDEX.md`, and `REVIEW.md` describe the SCT
  conventions, and skills keep them current.

## Out of scope

- A separate root cause document for a bug fix. The root cause goes in the
  commit message and the pull request description.
- A tool that files Jira issues.
- Any hook or gate that forces the flow.
