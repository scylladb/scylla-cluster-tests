## Commits and Pull Requests

### Conventional Commits

Write each commit subject as `type(scope): subject`. commitlint checks it.

- `type` is one of `ci`, `docs`, `feature`, `fix`, `improvement`, `perf`,
  `refactor`, `revert`, `style`, `test`, `unit-test`, `build`, `chore`.
- `scope` is required, with at least 3 characters.
- The subject has 10 to 120 characters and no trailing period. The header
  stays under 100 characters.
- The body is required, with at least 30 characters, after a blank line.
  Each body line stays under 120 characters.

### One Logical Change per Commit

Each commit is one coherent step that a reviewer can read alone. During a
review, fold each fix into the commit that it amends, and force-push. The
`pr-range-diff` workflow posts the changes of each force-push as a comment
on the pull request. The reviewer reads this comment to check the
review round.

### Root Cause of a Bug

A bug fix states its root cause in the commit body and in the pull request
description. Give the mechanism, the code path, and the evidence that ties
the cause to the symptom.

### Pre-commit Before Each Commit

Run `uv run sct.py pre-commit` before each commit. Do not commit code that
fails it. When a hook changes files, review the changes and run it again.

### Generated Files

The pre-commit hooks generate `configurations/nemesis/` and
`jenkins-pipelines/oss/nemesis/` from `sdcm/nemesis/` and the pipeline
templates. Commit the generated files that your change causes, as with a new
nemesis or a template change. The hooks can also change files that your
change does not explain. Do not commit those files. Tell the reviewers about
them.

The `update-conf-docs` hook writes `docs/configuration_options.md` and the
group pages in `docs/configuration_options/`. Commit each file that it
changes.

### Pull Request Description

Open each pull request as a draft. Fill the template in
`.github/pull_request_template.md` with the tests that ran, and their Argus or
Jenkins links. List the manual tests that a reviewer must run. Link the flow
directory under `docs/plans/` when the change has a spec.

### Jira Reference in the Pull Request

When the pull request finishes a Jira issue, write `Fixes: SCT-xxxx` on its
own line in the description. Use this form only:

- The Jira sync (`.github/workflows/call_jira_sync.yml`) reads the
  description and the commit messages. It moves an issue to Done only when a
  closing keyword comes right before the key.
- The backport workflow makes a Jira backport sub-task for each `Fixes` key.
  It ignores `Closes:` and `Resolves:`, which close the issue but leave the
  backports without a sub-task.

Both ignore the title, so `[SCT-xxxx]` in the title does nothing. They also
ignore `Jira: SCT-xxxx`, `Refs: SCT-xxxx`, `Tracked in SCT-xxxx` and a key in a
sentence. A pull request that uses only these forms merges and leaves the
issue open.

- The pull request finishes the issue: `Fixes: SCT-xxxx`.
- The pull request is one part of the issue: `Refs: SCT-xxxx`, in the
  description and in the commit messages. A `Fixes:` in one of its commits
  closes the unfinished issue. Close the issue by hand when the last part
  merges.
- An example in a description or a commit message never puts a real key
  after a closing keyword. The sync acts on it, also inside an HTML comment or
  backticks. Write `SCT-xxxx`.
- Never put a word that ends in a closing keyword right before a key. The sync
  matches the end of any word, so `hotfix: SCT-xxxx` or `prefix SCT-xxxx`
  closes the issue.
- Never put a closing keyword before a GitHub issue or pull request number
  that this pull request does not fix. GitHub closes that one on merge.

### Labels

Each pull request to `master` carries a backport label: `backport/none`, or
a `backport/X.Y` label for each release branch that needs the change. Add a
`test-provision-<backend>` label, as in `test-provision-aws`, for each
backend whose provisioning code the change touches.
`skills/code-review/references/review-checklist.md` maps the files to the
backends.

Sources: `commitlint.config.js`, `.github/workflows/pr-range-diff.yaml`,
`docs/contribution-handbook.md` sections 2.4, 2.6, 3.2, 4.4,
`.github/copilot-instructions.md`, `AGENTS.md`.
