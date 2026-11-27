# Less Groovy, one runnable Python layer for CI (proposal)

> This is a proposal of ideas to experiment with. It is not a committed roadmap.
> Each idea in `spec.md` comes with a small experiment and a way to stop.

## Problem

SCT keeps a large part of its CI logic in Groovy and in bash heredocs that Groovy builds.
Nobody can unit-test that logic, run it on a laptop, or call it from an AI agent or from
GitHub Actions. The same logic is copied into many places, and the copies drift apart.

The pain points, in order of size:

1. **Copied `SCT_*` export blocks.** Each `vars/` step turns Jenkins parameters into
   `SCT_*` variables with its own copy of the code. A new parameter must be added in many
   files, and the copies do not agree.
2. **Shell quoting.** The steps build bash strings in Groovy and pass them through
   `hydra.sh`, which runs `eval`. Empty values, spaces, and JSON break.
3. **Groovy CPS traps.** Serialization errors, `JsonSlurperClassic`, CPS scoping, and the
   JENKINS-41929 "build #1 only loads the parameters" bug.
4. **The stage sequence and its recovery are written about ten times.** About ten pipelines
   each implement argus → runner → provision → test → collect logs → clean → argus → email,
   and the `post { always }` recovery.
5. **A new backend or region touches many `vars/` files.**
6. **Logic that only Jenkins can run.** Runner sizing, builder label choice, and the
   PR-label matrix. No test compiles `vars/`. No command replays a job locally.
7. **Fragile state between stages.** Loose files (`sct_runner_ip`, `./latest`) carry the
   state. A stale `sct_runner_ip` sends commands to an old runner.
8. **Agents cannot drive a job from end to end.** No CLI or skill covers
   "plan → run locally → run on Jenkins → watch → read the result".
9. **The minicloud lifecycle exists three times.** Groovy, the shell scripts, and the
   `downscaling-for-minicloud` skill each implement preflight → start → firewalld zone →
   provision → test → collect → clean → stop. Local runs hit host traps (the kvm group,
   memlock, a firewalld reload) that only the Groovy preflight checks.
10. **Fast PR checks use a cloud builder.** pre-commit, the unit tests, collect-only, and
    `lint-pipelines` run on an AWS Jenkins builder. GitHub Actions runs no tests.
11. **The hydra image version is a counter committed into the PR.** Two PRs can take the
    same number. Backports conflict on `docker/env/version` and `uv.lock`. The build has no
    cache, runs for amd64 only, and uses `pull_request_target` with the push secrets.
    The 8 helper images under `docker/` are built by hand.

## Who it affects

- Every SCT developer who changes a pipeline, adds a backend or a region, or runs
  minicloud locally.
- The maintainers of the release branches, who backport pipeline and hydra image changes.
- AI agents that must run or debug an SCT job.
- All Jenkins jobs (1,173 jenkinsfiles), because they all run the `vars/` steps.

## Evidence

Size, measured on master on 2026-11-11:

- `vars/`: 52 files, about 9,000 lines. The root `Jenkinsfile`: 616 lines.
- 194 commits changed `vars/` or `Jenkinsfile` from 2025-11-01 to 2026-11-11.
- `export SCT_` lines in Groovy: `runSctTest` 65, `provisionResources` 53,
  `perfSearchBestConfigParallelPipeline` 40, `artifactsPipeline` 34, `managerPipeline` 33,
  `estimateTestCost` 20, `runCleanupResource` 16, `jepsenPipeline` 13. About 312 in total.
- Minicloud lifecycle: `startMinicloud.groovy` 196, `minicloudPreflight.groovy` 170,
  `minicloudReclaim.groovy` 178, `stopMinicloud.groovy` 111, `scripts/run-minicloud-test.sh`
  170, `run-minicloud-provision-resources.sh` 58, `run-minicloud-clean-resources.sh` 99,
  skill `run-script-template.md` 216 and `triage-local-runs.md` 129.
- `.github/scripts/backport_hydra_fixup.py`: 330 lines that only repair hydra version
  conflicts in backports.

Commits that fixed each problem (subjects verbatim):

```
085c527fbf fix(ci-oci): export SCT_OCI_INSTANCE_TYPE_DB in runSctTest.groovy
055c63401b fix(pipeline): pass billing_project parameter to performance tests
1b9c582e09 fix(jenkins): correct typo in gemini_seed parameter reference
e8ad5182eb fix(simpleTrigger): drop duplicated post_behavior_k8s_cluster param
a8fa464ec4 fix(jenkins-pipelines): quote email_recipients exactly once to fix multi-address crash
26f917412a fix: remove JsonOutput.toJson() from gce_datacenter and use single-quoted shell exports
2fc7e93dba fix(jenkins): quote arguments passed through hydra.sh to preserve spaces
85be2c2ff7 fix(jenkins): omit --test-name when the pipeline has no test_name
a5d58184ac fix(cleanSctRunners): omit empty --runner-ip flag and guard set-runner-tags (SCT-607)
4fb2804be6 fix(jenkins): add pipefail to prevent tee from swallowing errors
5461607f24 fix(jenkins): read PR labels outside CPS to avoid NotSerializableException
c2169d9765 fix(jenkins-pipeline): avoid MethodClosure serialization in parallel execution
9231f0460b fix(groovy): switch to use JsonSlurperClassic
2c94f7ffcc fix(pipelines): add try/finally cleanup and fix CPS scoping in perfRegressionParallelPipeline
5df7192a4f fix(manager-pipeline): abort build#1 to work around JENKINS-41929 params loading bug
6ea4ca3976 improvement(sct): enable log collection when provision fails
dc39403f65 fix(xcloud): clean runner instances when test runs on xcloud backend
b7930fe25d ci(backend): add full support of Oracle Cloud (OCI) in groovy files
2eeb3fbb3a ci(oci): add 'oci' label to the vars/getJenkinsLabels.groovy
7237120eee fix(getJenkinsLabels): add missing oci_region placeholder
cf50fd7746 fix(jenkins): anchor the duration regexes in 'getJobTimeouts()'
0dd6e831bf chore(hydra): create image 1.124-PR13589-85ca070
3aaea35aef Revert "chore(hydra): create image 1.124-PR13589-85ca070"
0a00f66c55 chore(hydra): create image 1.135-PR14571-8041fd5
de201e94ff Revert "chore(hydra): create image 1.135-PR14571-8041fd5"
666fd253ab ci(backport): drop master's hydra commit from backports and re-lock uv.lock
```

From `longevityPipeline.groovy` and the other pipelines: a stale `sct_runner_ip` file sends
the next command to an old runner, so each pipeline deletes it by hand.

A precedent that worked: the trigger logic moved from Groovy and XML into tested Python in
`sdcm/utils/trigger_matrix/`. `triggerMatrixPipeline.groovy` now mostly calls
`hydra trigger-matrix`.

## Definition of done

This is a proposal, so "done" means "worth continuing". An experiment is worth continuing
when its result shows one of these:

- A `vars/` step gets its `SCT_*` environment from one tested Python mapping, and the
  mapping gives the same variables as the Groovy that it replaces.
- A developer or an agent runs the steps of a Jenkins job, or a minicloud run, with one
  local command, and Jenkins runs the same command.
- A pipeline fix lands in Python with a unit test, not in Groovy.
- The PR checks give the same results on GitHub Actions as on Jenkins, in less time.
- A hydra image change needs no version commit, and its backport has no version conflict.
- No release branch breaks. A branch changes only when a backport is chosen for it.

Signals to watch over time: the Groovy line count, the `export SCT_` count, and the number
of Groovy fix commits.

## Out of scope

- A rewrite of the 1,173 jenkinsfiles. They keep their entry points and arguments.
- Leaving Jenkins. Jenkins stays the trigger for cron, the parameters UI, the builders,
  and throttling.
- The SCT test logic itself (`sdcm/`, the tests, the nemesis).
- VM images: the SCT runners, the Jenkins builders, and the minicloud guests.
- New Jenkins jobs. The experiments reuse the existing jobs and the staging folders.
- A decision for or against Dagger. `spec.md` compares both options and leaves the choice open.
