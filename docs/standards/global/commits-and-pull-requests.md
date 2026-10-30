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

Each commit is one coherent step that a reviewer can read alone. Squash fixup
commits before the review.

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
Jenkins links. List the manual tests that a reviewer must run. Link the idea
directory under `docs/plans/` when the change has a spec.

### Labels

Each pull request to `master` carries a backport label: `backport/none`, or
a `backport/X.Y` label for each release branch that needs the change. Add a
`test-provision-<backend>` label, as in `test-provision-aws`, for each
backend whose provisioning code the change touches.
`skills/code-review/references/review-checklist.md` maps the files to the
backends.

Sources: `commitlint.config.js`, `docs/contribution-handbook.md` sections 2.4,
2.6, 3.2, 4.4, `.github/copilot-instructions.md`, `AGENTS.md`.
