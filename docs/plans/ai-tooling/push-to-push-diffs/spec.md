# Push-to-push diffs for review rounds on SCT pull requests

**Jira:** SCT-1395

**Status:** built

## Overview

Two workflows show the diff of each force-push to a pull request.
`pr-range-diff-record` runs on each push, with no token, and records the
number of the pull request. `pr-range-diff` runs after it from `master`. When
the push is a force-push, it runs `git range-diff` between the old series and
the new series. Each series starts at its own merge-base with the base
branch, so the commits that a rebase brings from `master` do not show. The
workflow posts the result as one comment on the pull request. The commits
standard tells the author to fold each review fix into its logical commit and
force-push. The reviewer reads the comment to check the review round. Each
later force-push also gets a comment, so the commits that merge are the
commits that the reviewers read.

The fetch, the range-diff, the format, and the post steps are a port of
`.github/workflows/pr-range-diff.yaml` from scylladb/scylladb, commit
`b97a8e03965e`.

## Constraints

- Each trigger runs with no Actions event policy. From 2026-12-02, GitHub
  blocks `pull_request_target` in public repositories unless a policy allows
  it.
- A `pull_request` run takes its workflow file from the pull request. Its
  output is untrusted. The job with the write token runs from `master`, and
  takes the old head and the new head from GitHub, not from that output.
- The job with the write token fetches git objects only. It does not check
  out or run a file from the pull request.
- Commit titles and diff lines come from the author of the pull request. The
  comment must not let them add links, images, mentions, or HTML.
- GitHub limits a comment to 65536 characters.
- The job uses only the default `GITHUB_TOKEN`, and no repository secret.

## Design

```mermaid
flowchart TD
    P["Push to a pull request"] --> REC["pr-range-diff-record<br/>(pull_request: synchronize, no permissions)<br/>upload the PR number"]
    REC --> WR["pr-range-diff from master<br/>(workflow_run: completed)"]
    WR --> L["Check the PR number.<br/>Find the force-push event of the PR<br/>whose new head is the head of the run"]
    L --> E{"Event found?"}
    E -- no --> N[Stop, no comment]
    E -- yes --> F["Fetch old head, new head, base branch tip<br/>blobless, no checkout"]
    F --> R["git range-diff<br/>old_base..old_head new_base..new_head"]
    R --> C{"A commit of the<br/>author changed?"}
    C -- no --> RO["Comment:<br/>Rebased only, no commit changed"]
    C -- yes --> FM["Format: list of changed commits,<br/>one collapsed diff for each modified commit"]
    FM --> S{"Over 60 KB?"}
    S -- no --> PC[Post one comment]
    S -- yes --> T["Post a cut comment with a link<br/>and write the full text to the job summary"]
```

`old_base` is the merge-base of the tip of the base branch and the old head.
`new_base` is the merge-base of the tip of the base branch and the new head.
The tip is at or after each commit that the author rebased on, so each
merge-base is the start of the series of the author.

`pr-range-diff` is one job with these steps:

| Step | Does |
|---|---|
| Find | Download the artifact, check that it holds a number, read the base branch from `pulls/<number>`, and find the force-push event in the GraphQL timeline of the pull request |
| Fetch | `git init`, then `git fetch --filter=blob:none` of the two heads and the base branch from the base repository |
| Compute | `git range-diff --no-color --creation-factor=80` to a text file, with Python hunk headers |
| Format | An inline Python step parses the text file and writes the comment body |
| Post | `gh pr comment` with the body file |

The formatter stays inline in the workflow file and stays near the scylla
text. A later fix in scylla then applies with a small diff.

The workflow runs for each author, and for draft pull requests. It lives on
`master` only. `workflow_run` takes the workflow file from the default
branch for a pull request to any branch, so it also runs for the backport
pull requests to the release branches. The base of each series is the tip of
the base branch of the pull request. The pull request that adds the workflow
carries the `backport/none` label.

Each force-push gets a new comment, and each comment stays as posted. The
timeline of the pull request then holds one diff for each review
round, next to the review comments of that round.

| Condition | Behavior |
|---|---|
| The push only adds commits | The pull request has no force-push event with this head. No comment. GitHub shows the new commits |
| The artifact holds no number | The job fails. The pull request gets no comment |
| The force-push only rebases | The comment says `Rebased only; no commit changed.` |
| The comment is over 60 KB | The comment keeps what fits, closes its open blocks, and links to the job summary. The job summary holds the full text |
| The fetch of a SHA fails | The job fails in the Actions tab. A `workflow_run` job adds no check to the pull request. The pull request gets no comment |
| `gh pr comment` fails | The job fails. The workflow log holds the full range-diff |
| The job runs longer than 10 minutes | GitHub stops it |

## Contracts

### Inputs

The artifact `pr-push` of the `Record PR push for range-diff` run, one file:

```
number      # the PR number, untrusted: the job accepts only digits
```

The `workflow_run` event, type `completed`, and the GitHub API:

```
github.event.workflow_run.conclusion     # success, or the job does not run
github.event.workflow_run.id             # the run that holds the artifact
github.event.workflow_run.head_sha       # new head SHA
repos/<repo>/pulls/<number> .base.ref    # base branch
HeadRefForcePushedEvent                  # old head SHA, from the event whose
  { beforeCommit afterCommit }           #   afterCommit is the new head
github.server_url, github.repository, github.run_id
```

Permissions:

```yaml
# pr-range-diff-record
permissions: {}
# pr-range-diff, job range-diff
permissions:
  actions: read
  contents: read
  pull-requests: write
```

### Outputs

The comment on the pull request:

````markdown
### Changes since previous version ([`<old 10>`](<commit url>) → [`<new 10>`](<commit url>))

- `2/5` **modified**: [<commit title>](<commit url>)
- `5/5` **new**: [<commit title>](<commit url>)
- `–` **dropped**: [<commit title>](<commit url>)

<details><summary><b>2/5 <commit title></b> (<N> lines changed)</summary>

```diff
<range-diff of the commit pair>
```
</details>
````

The list holds only the commits that changed. The formatter escapes Markdown
and HTML in each commit title, and breaks each `@` with a zero-width space.
The fence of each diff block is longer than each backtick run in the diff.

`docs/standards/global/commits-and-pull-requests.md`, section "One Logical
Change per Commit":

> Each commit is one coherent step that a reviewer can read alone. During a
> review, fold each fix into the commit that it amends, and force-push. The
> `pr-range-diff` workflow posts the changes of each force-push as a comment
> on the pull request. The reviewer reads this comment to check the
> review round.

`.github/copilot-instructions.md`, section "Commit History and Squashing".
This text is the whole section:

> Keep one logical change per commit. During a review, fold each fix into
> the commit that it amends, and force-push the branch. When you cannot
> force-push, give the reviewer the hash of the folded branch. The
> `pr-range-diff` workflow shows the reviewer what changed. See
> `docs/standards/global/commits-and-pull-requests.md`.

`docs/development-flow.md`, section "Review", one more paragraph:

> During a review, the author folds each fix into its commit and
> force-pushes. The `pr-range-diff` workflow posts the changes of each
> force-push as a comment on the pull request.

## Risks

| Risk | Response |
|---|---|
| `git range-diff` fetches each blob that it needs in a separate request. A long series with many files can take minutes | The job stops after 10 minutes and fails in the Actions tab. The author can still link a local `git range-diff` |
| A pull request with many force-pushes gets many comments | The diffs are collapsed. The reviewers report in the first weeks if the comments hide the discussion |
| Bots that rebase their pull requests get a comment for each rebase. The backport bot force-pushes its conflict fixes | A rebase gets the one line `Rebased only; no commit changed.` A conflict fix gets a diff that shows the fix |
| A pull request from the fork of a first-time contributor waits for a maintainer to approve its `pull_request` runs | The comment comes after the approval |
| A rename of `pr-range-diff-record` stops the comments, with no error | A comment in each file names the coupling |
| A fix that rewrites most of a commit makes `git range-diff` pair no commits. The comment then shows the commit as dropped and new, with no diff of the fix | The creation factor of 80 pairs a small commit with a one-line fix. The title of the new commit matches the dropped one |
| A change to the comment format of `git range-diff` breaks the parser | The job runs on `ubuntu-latest`, as in scylla. The raw range-diff stays in the workflow log |

## Decisions

- SCT ports the range-diff workflow of scylla core. It is the only option
  that hides the commits that a rebase brings from `master`, and scylla runs
  it on each pull request. (user)
- A `pull_request` run records the PR number, and a `workflow_run` job from
  `master` posts the comment. The comment then needs no Actions event policy
  for `pull_request_target`. The job takes the old head from the force-push
  event on GitHub, so a changed recorder cannot choose the commits, and two
  quick pushes each keep their comment. (review)
- The port differs from scylla in six points: the trigger, it runs on
  drafts, it takes the tip of the base branch as the base, it sets Python
  hunk headers, it pairs commits with a creation factor of 80, and a failed
  `git range-diff` fails the job. (spec)
- The workflow comments on draft pull requests. The standard opens each pull
  request as a draft, and reviews start on drafts. (spec)
- The workflow runs for each author and for pull requests to each branch.
  A backport pull request with a conflict fix gets the diff of the fix.
  (spec)
- Each force-push gets its own comment. Each review round keeps its diff in
  the timeline. (spec)
- A force-push that only rebases gets a short comment. The reviewer learns
  that there is nothing to read again. (spec)
- The formatter stays inline. It is a near copy of the scylla formatter,
  which runs on each scylla pull request. Its checks are a local run of the
  workflow steps and a run in a fork, on a pull request between two branches
  of the fork. A pull request from a fork to SCT runs it only after the
  merge, because `workflow_run` takes the file from `master`. (spec)
- The Copilot section holds the rule and a link to the standard. One
  source holds the rule. (spec)
- The Copilot agent force-pushes the folded branch. When it cannot
  force-push, it gives the hash of the folded branch to the reviewer. (spec)
