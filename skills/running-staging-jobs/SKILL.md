---
name: running-staging-jobs
description: >-
  Guides proving an SCT pull request on real Jenkins before merge, by running
  per-PR copies of the affected pipelines under scylla-staging with
  staging_trigger.py. Use when asked to "run staging for PR N", "validate this
  PR on Jenkins", "trigger the artifacts or longevity job on my branch", or
  when a staging build ran with wrong parameters, an old revision, or the wrong
  backend. Covers picking the jobs that exercise the change, generate and
  trigger without interactive prompts, checking the build got the intended
  parameters, and recording results in the PR Testing section. Not for local
  runs (hydra, minicloud), creating release-branch job trees, or editing
  production Jenkins jobs.
---

# Running Staging Jobs

Prove that a PR works on the real Jenkins pipelines it changes, before it merges.

The output is a PR whose `### Testing` section lists, for each pipeline the change touches, a staging build that ran the PR's code with the intended parameters, and its result.

## Essential Principles

### Pick jobs by what the change exercises

A staging run proves only the code paths that the chosen pipelines execute.
Pick the jenkinsfiles from the diff, not by habit:

| The PR changes | Run |
|---|---|
| A jenkinsfile or `vars/*.groovy` it calls | That jenkinsfile (one per changed pipeline type) |
| A test-case YAML under `test-cases/` | A jenkinsfile whose `test_config` uses that YAML (`grep -rl <yaml> jenkins-pipelines/`) |
| Provisioning for one backend (`sdcm/provision/`, `sdcm/cluster_<backend>.py`) | A short job on that backend, for example `oss/artifacts/*` or `oss/longevity/longevity-10gb-3h*` |
| Package install or node setup | The artifacts jobs of the affected distros and architectures |
| Shared framework code with no backend in it | One short longevity job (`oss/longevity/longevity-10gb-3h`, AWS) |

If nothing in the table fits, or unit tests already cover the change, say so in the PR and skip staging. A staging build costs cloud money and about an hour.

### Never let the CLI go interactive

`staging_trigger.py` is interactive by default. In an agent shell (no TTY), a prompt aborts the command.
Always pass the jenkinsfiles to `generate`, the job names to `trigger`, `--pr` (or `-b` together with `--repo`), and `--update-pr` or `--no-update-pr`.

### The build is the evidence, not the trigger output

`trigger` reports what it requested, and Jenkins can still run something else:
- The job runs the branch on the PR's head repo. If that is your fork and the PR lives on an upstream branch, the fork's branch with the same name can be months old.
- Parameters that the job does not define yet are dropped. The trigger output warns about it with `Dropping parameters that <job> does not define`, but the build still starts.

So before trusting a build, open its parameters and the commit it checked out.

### Pass every parameter that matters

`trigger` does not use the jenkinsfile defaults. It merges, last one winning:

1. the parameters of the job's last build
2. the preset (`utils/staging_trigger/constants.py`): longevity, manager and perf set `region=eu-west-1` and `scylla_version=master:latest`, and artifacts clears `scylla_version`
3. the `-s` values

So a `-s` value for a key the preset doesn't set sticks to the next build, even when not passed again.
A `-s` value for a preset key does not stick: the next trigger without it goes back to the preset, for example a backport run without `-s scylla_version` runs master.
The preset is detected from the first job name and applied to every job in the call, so mixing artifacts and longevity jobs clears `scylla_version` for the longevity one.
Run one `trigger` per preset (or force it with `-p`), and pass `scylla_version`, `region`, `backend` and any parameter the PR changes with `-s`.

## When to Use

- "Run staging for PR 16761" or "validate this PR on Jenkins" before asking for merge
- A PR changes a jenkinsfile, `vars/*.groovy`, provisioning, or node setup, and unit tests can't run that path
- A staging build ran old code, missed a `-s` parameter, or ran on the wrong backend
- Several backport PRs of one change need the same jobs, each on its own branch

## When NOT to Use

- Running a test on your machine: use `hydra`, or the `downscaling-for-minicloud` skill for minicloud
- Creating the job tree for a new release branch: use `hydra create-test-release-jobs`
- Changing production jobs or trigger schedules: edit `jenkins-pipelines/` and `configurations/triggers/` in a PR
- A change covered by unit tests alone: write it in the PR and skip staging

## Quick Reference

```bash
# 1. Create the per-PR job copies (folder: scylla-staging/<user>/<jenkinsfile dir>/, name suffix -test)
uv run python staging_trigger.py generate --pr <N> oss/artifacts/artifacts-ami

# 2. Trigger them (blocks for minutes, run in the background)
uv run python staging_trigger.py trigger --pr <N> --update-pr \
    -s scylla_version=master:latest oss/artifacts/artifacts-ami-test

# Check the job path first: a dry run against a wrong path still "succeeds"
uv run python staging_trigger.py -f scylla-staging/<user>/oss/artifacts trigger -l "artifacts*"
```

For several backports of one PR, give each its own job copies with `generate --suffix -bp2026-3-test`, because a job is bound to one branch.

### Common Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Command prints `Aborted!` right away | It prompted for input | Pass the pipelines or jobs, `--pr`, and `--update-pr` or `--no-update-pr` |
| Build #1 has no parameters, runs on AWS with `email_recipients=qa@` | First build of a new job only registers the parameters | Wait for the parameters to appear on the job, abort #1, and let the queued #2 run |
| `trigger` returns HTTP 400 on a new job | Build #1 failed to compile the pipeline, so the job has no parameters | Fix the jenkinsfile, delete the job, then `generate` again |
| `Dropping parameters that ... does not define` in the trigger output | The job does not define that parameter yet | Wait for the build to register the parameters, stop it before `Run SCT stages`, then trigger again |
| Build ran old code | The fork branch with the PR's name is stale | Push the PR head to the fork branch, then trigger again |
| Region or another value differs from the jenkinsfile | The preset (`region=eu-west-1`) or the last build's value overrides it | Pass the parameter with `-s` |
| Longevity job ran with an empty `scylla_version` | Triggered in one call after an artifacts job, so it got the artifacts preset | One `trigger` call per preset, or `-p` |
| AMI job: `AMIs ... not found` | Release `scylla_version` without the `branch-` prefix | `-s scylla_version=branch-2026.3:latest` |
| Job not found | Wrong `-f` folder, or `-f` given after the subcommand | `-f scylla-staging/<user>/<dir>` goes before `trigger`. For `generate`, use the bare `scylla-staging/<user>` |

## Workflow

The step-by-step process, with entry and exit criteria, is in [run-and-track.md](workflows/run-and-track.md).

## Success Criteria

- [ ] Every chosen job maps to a part of the diff, and the mapping is written in the PR
- [ ] Each counted build ran the PR head commit with the intended parameters
- [ ] Each failure has a root cause, either fixed in the PR or linked to an existing issue
- [ ] The PR `### Testing` section shows the final build of each job, with ✅ or ❌
