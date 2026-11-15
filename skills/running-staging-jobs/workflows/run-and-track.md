# Run and Track Staging Jobs

Step-by-step process for proving one SCT PR on staging Jenkins.

## Phase 1: Choose the jobs

**Entry**: The PR is open and its head branch is pushed.

1. List the changed files: `gh pr diff <N> --name-only`.
2. Map them to jenkinsfiles with the table in [SKILL.md](../SKILL.md#pick-jobs-by-what-the-change-exercises). Pick the smallest set that runs every changed code path once.
3. If the PR head repo is your fork and the PR targets an upstream stacked branch, compare `git ls-remote origin <branch>` with `git ls-remote upstream <branch>`. Jenkins checks out the fork, so push the head there if they differ.
4. Write the list, and one line per job saying why it was chosen, in the PR description. Reviewers judge the coverage from this list.

**Exit**: A list of jenkinsfile paths under `jenkins-pipelines/`, each tied to a part of the diff.

## Phase 2: Generate the job copies

**Entry**: The job list from Phase 1.

1. Dry run first: `uv run python staging_trigger.py generate --pr <N> -n <jenkinsfile> [...]`.
2. Check that the printed `SCM` is the PR head repo and branch.
3. Run it again without `-n`.
4. Wait until each new job has registered its parameters (the job page shows "Build with Parameters", or Jenkins MCP `get_item_parameters` returns them). Until then, the job defines no parameters to receive the trigger's values.

**Exit**: Each job exists at `scylla-staging/<user>/<jenkinsfile dir>/<name>-test` and has parameters.

## Phase 3: Trigger

**Entry**: Jobs exist and have parameters.

1. Decide the parameters: `scylla_version` (`master:latest`, or `branch-X:latest` on a release branch), `region`, plus anything the PR changes. The last build's values and the preset override the jenkinsfile (see SKILL.md, "Pass every parameter that matters").
2. Group the jobs by preset (artifacts, longevity, manager, perf, dtest), one `trigger` call per group.
3. Trigger in the background, because the command polls Jenkins for minutes:
   ```bash
   uv run python staging_trigger.py trigger --pr <N> --update-pr \
       -s scylla_version=master:latest -s <key>=<value> <dir>/<name>-test [...]
   ```
   `--update-pr` appends a `- [ ] :clock1: [job #build](url)` line per build under `### Testing` in the PR body.
4. If a throwaway build #1 is running without parameters, abort it. The requested build is #2.

**Exit**: Each job has a queued or running build, listed in the PR.

## Phase 4: Verify the build is the one you meant

**Entry**: Builds are running.

1. Open each build's parameters (build page "Parameters", or Jenkins MCP `get_build_parameters`). Every `-s` key must be there with its value.
2. Check the first lines of the console log: the checked-out commit must be the PR head SHA (`gh pr view <N> --json headRefOid`).
3. If either check fails, stop the build before `Run SCT stages` starts creating cloud resources, fix the cause (see the pitfalls table in SKILL.md), and trigger again.

**Exit**: Every running build uses the PR head and the intended parameters.

## Phase 5: Track and diagnose

**Entry**: Verified builds are running.

1. Wait for a terminal result. Don't poll in a tight loop: the durations of the job's earlier builds in Jenkins tell how long to wait.
2. For a failure, read the console log from the first `ERROR` or traceback, not the last one. Teardown errors after a failure are noise. The Argus run linked from the build has the events and the node logs.
3. Classify the root cause:
   - **Caused by the PR**: fix it in the PR (squash into the commit that introduced it), push, and trigger only that job again.
   - **Infrastructure or a stale parameter**: fix the parameter and trigger again. Nothing changes in the PR.
   - **Known issue in ScyllaDB or SCT, not related to the PR**: link the existing issue next to the build. Don't retry until it passes.

**Exit**: Each job has a final build that passed, or failed with a root cause that is linked and not related to the PR.

## Phase 6: Record the result

**Entry**: Every job has a final build.

1. In the PR `### Testing` section, mark each final build `- [x] ✅` or `- [ ] ❌` with a one-line reason and a link to the issue.
2. Remove the superseded builds, or move them under one "Earlier attempts" line with their root cause. Reviewers need the final state, not the history.

**Exit**: The PR description shows one final build per chosen job.
