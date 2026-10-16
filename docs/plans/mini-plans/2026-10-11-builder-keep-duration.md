# Mini-Plan: Jenkins Builders: Run Owner, Keep Duration and Timeouts

**Date:** 2026-10-11
**Estimated LOC:** ~60 (PR 1) + ~250 (PR 2) + ~80 (PR 3)
**Related PR:** #16431 (this plan)
**Jira:** [SCT-1210](https://scylladb.atlassian.net/browse/SCT-1210), related [RELENG-1014](https://scylladb.atlassian.net/browse/RELENG-1014)

## Problem

Every SCT Jenkins builder launches with `keep=alive`: the AWS builder ASGs propagate it to their
instances and the GCE instance template carries it as a label. OCI builders launch with no tags.
If Jenkins loses track of a builder, nothing ever cleans it up.
Cloud cleanup already understands `keep=<hours>`, counted from instance launch, but builders never
carry one.

OCI builders are invisible to `clean_oci` entirely. It only looks at defined tags in the `sct`
namespace with `CreatedBy=SCT`, while `tagBuilder` writes freeform tags with lowercased keys.
`--freeform-tags` also replaces the instance's whole freeform set on every call.

Resources are not attributed to the person who started the run. `tagBuilder` takes the owner only
from the `requested_by_user` parameter or a manual-start cause. Anything else is tagged
`RunByUser=jenkins`:
- SCT CI on a pull request: the builder, and the integration-test SCT runner.
- Builds started by an upstream job without `requested_by_user`.
- Timer runs of the trigger configs that set no owner: `sanity`, `scylla-doctor-gating` and
  `pgo-offline-installer`.

The CI provision tests are the exception: they overwrite `BUILD_USER_ID` with the PR author's
GitHub login, per stage.

A finished build releases its builder whatever the `keep` value is. A builder outlives its build only
when the build hangs, and builds can hang because many steps have no timeout. RELENG-1014: SCT PR
job PR-14882 #2 held a CI builder for 12 days. Its pipeline-level `post` step
(`postTestSummaryComment`, a `hydra.sh` call) lost its durable-task directory across a controller
restart and retried forever. Nothing around it had a timeout.
- 17 of the 20 builder pipelines have no pipeline-level timeout, including `Jenkinsfile` and
  all the test pipelines.
- None of the shared helpers that run shell steps has a timeout of its own: `tagBuilder`,
  `dockerLogin`, `cleanSctRunners`, `postTestSummaryComment`, `checkoutQaInternal`. Each is called
  without one in several places, including the `post`/`finally` cleanup fallbacks.
- `hydra-show-monitor` and `hydra-show-jepsen-results` have no timeout at all.
- manager, jepsen, rollingUpgrade and perfRegressionParallel pass `collectLogsTimeout` /
  `resourceCleanupTimeout` to their post fallbacks unguarded. When 'Get test duration' failed they
  are 0, which aborts immediately; longevity guards it with `?: 30`.

## Approach

Three PRs. They are independent, and PR 1 and PR 3 can merge first:
- **PR 1, owner attribution:** `tagBuilder` logic only, with no infra step.
- **PR 2, keep duration:** pipelines, builder templates and OCI tags, then the rollout below.
- **PR 3, bounded builds:** timeouts, so a hung build releases its builder. `keep` only covers the
  cloud side: had cleanup terminated the RELENG-1014 builder, the build would still have looped on
  an offline agent. The two are complementary.

### PR 1: the owner is whoever started the run, manually or automatically

`tagBuilder.getRunningUserId` already decides the builder's `RunByUser`. It becomes the single
owner resolution for the whole build, first match wins:
1. the `requested_by_user` parameter;
2. the user who started the build manually;
3. the pull request author, for CI;
4. the owner of the upstream build that triggered this one;
5. `jenkins`, only when no person is behind the run at all.

`tagBuilder` tags the builder with that owner, and exports it for the rest of the build as the
requested-by user. SCT's `get_username` gives that variable top priority, and hydra forwards it into
its container, so the SCT runner, cluster nodes and Argus run all carry the same owner as the
builder.
- Code that sets its own user, such as the CI provision tests' per-stage `BUILD_USER_ID` override,
  calls `tagBuilder.getRunningUserId` directly instead.
- The three trigger configs without an owner get `requested_by_user`, like `tier1` and
  `perf-regression` already have.
- **Needs Investigation:** a PR author is a GitHub login, while other owners are scylladb.com user
  names. Decide whether to map logins to user names (and from what source), or to accept the
  GitHub login as is.

### PR 2: keep duration

Lifecycle, the same on AWS, GCE and OCI:
builder launches with `keep=6` -> build starts, `tagBuilder` sets `keep = ceil(uptime_h) + 6` ->
pipeline computes its timeouts, `tagBuilder` sets
`keep = ceil(uptime_h) + ceil(runnerTimeout / 60) + 5` -> build ends, builder idles and Jenkins
scales it in -> if Jenkins lost it, cloud cleanup terminates it once `launch + keep` passes.

- **Keep formula.** Cleanup counts keep from launch time, and AWS and OCI builders are reused
  across builds, so the time the builder has already been up is added. Uptime comes from the
  builder itself, so every cloud works the same way.
  - `runnerTimeout` is the budget `getJobTimeouts` already returns: test run, log collection,
    cleanup and email.
  - The +5 hours covers the 4-hour cap on the Provision Resources stage plus 1 hour of slack.
  - Lowering a keep the previous build raised is intended: builders have one executor, so the
    running build is the only tenant.
  - Example: a 3-hour longevity on a fresh builder gets keep 13; a 3-day longevity gets 83.
- **`tagBuilder` owns the keep.** It accepts an optional budget in minutes. With no budget it writes
  the "+6" value; with a budget it writes the formula. It also always writes `keep_action=terminate`.
  The cloud-detection result is cached for the build, so the second call is a single tagging call.
- **Pipelines that call `getJobTimeouts`** call `tagBuilder` again right after it, passing
  `runnerTimeout`. byoLongevity and artifacts pass the sum of their own stage timeouts.
- **Non-test jobs that run on a builder** call `tagBuilder` too. Without this, a reused builder keeps
  whatever short keep the previous build left. Each job passes a budget only when it can run past
  the 6-hour start-of-build value. All of them already load the shared library.

  | Job | Builder label | Today | Change |
  |-----|---------------|-------|--------|
  | CI `Jenkinsfile` (PR checks, provision tests) | `v3-CI` | tags at start, no keep | pass a budget: its stage timeouts add up to more than 6 h (two 300-minute provision waits) |
  | `triggerMatrixPipeline` (7 `*-trigger` jobs) | `v3-CI` | no tagging | start call with its 6-hour pipeline timeout as the budget |
  | `createTestJobPipeline` (`create-sct-test-jobs-for-branch`) | v4 ASG | no tagging | start call with its 360-minute budget |
  | `sdReleaseGatingPipeline` (`sd-release-gating`) | v4 ASG | no tagging | start call; its 180-minute timeout fits in the default |
  | `qa/hydra-clean-runner-instances`, `hydra-clean-test-resources`, `hydra-cleanup-cloud`, `hydra-show-monitor`, `hydra-show-jepsen-results`, `trigger-matrix-arch-audit` | `v3-CI` | no tagging | start call. They run for minutes; the `show-*` jobs hand off to a new SCT runner and return. |
  | `simpleTrigger` (4 sanity triggers) | `agent none` | n/a | nothing: it never holds a builder |

  `perfRegressionParallelPipelinebyRegion` and `perfSimpleQueryOfflineInstallerTrigger{Arm,X86}`
  have no caller in the repo. They are left out here and can be deleted separately.
- **Launch defaults** become `keep=6` + `keep_action=terminate` on all three clouds:
  - AWS: the ASG tags. Re-running the configure command also updates the tags of an ASG that
    already exists; today it only updates the instance policy. The launch template's own
    `keep=alive` tag stays: it protects the template, not instances.
  - GCE: labels and metadata. Instance templates can't be modified, so the builder version is
    bumped.
  - OCI: the Jenkins plugin template launches builders with `sct.*` defined tags: `NodeType`,
    `RunByUser`, `keep`, `keep_action`, `billing_project`, `CreatedBy=SCT`.
- **OCI tagging matches SCT's own OCI resources.** `tagBuilder` reads the instance's defined tags,
  merges the new values into the `sct` namespace, and writes them back. Other namespaces such as
  `Oracle-Tags` are kept, and key case is kept. `billing_project` is added to the namespace keys
  SCT creates. With `CreatedBy=SCT` present, `clean_oci` applies the same creation-time + keep rule
  as AWS and GCE.
- **Tag alignment.** GCE builders currently report `billing_project=sct-runners`; it becomes
  `sct-builders` like AWS.
- **Rollout order.** Every jenkinsfile loads the shared library from its own branch, but all
  branches share the same builders.
  - Merging this PR changes no live builder; the defaults apply only when
    `hydra configure-jenkins-builders` runs.
  - Run it only after the `tagBuilder` change is backported to every active release branch.
    Otherwise a release-branch build on the old `tagBuilder` is terminated 6 hours after its
    builder launched.
  - The `aws-sct-builders-eu-west-1-v3-CI` ASG is not created by this repo. It gets the same tags
    by hand, or its jobs move to v4 and it is retired. **Needs Investigation:** which of the two.

### PR 3: every builder-bound step has a timeout

- **Timeouts live in the helpers.** `tagBuilder`, `dockerLogin`, `cleanSctRunners`,
  `postTestSummaryComment` and `checkoutQaInternal` each wrap their own shell work in a `timeout`
  sized to that work. Every caller is covered at once, including the post and `finally` fallbacks,
  with no change at the call sites. `cleanSctRunners` gets the longest one, since it tears the
  runner down. `tagBuilder`'s IMDS `curl` calls also get `--max-time`.
- **Pipeline-level backstop where the budget is static:**
  - `Jenkinsfile`: the sum of its stage timeouts. That is the same number PR 2 passes to
    `tagBuilder` as its budget.
  - `createTestJobPipeline`: 360 minutes.
  - The `qa/hydra-*` jobs and `trigger-matrix-arch-audit`: a short fixed timeout.
- **Test pipelines** get no pipeline-level timeout: their duration is known only after
  'Get test duration', and `options` is fixed before that. Every step in them is bounded by its
  stage timeout or by the helper timeouts above, so the build as a whole is bounded too.
- **Zero-timeout guard.** The four pipelines missing it get longevity's `?: 30`.
- **Stuck-build alert, out of repo:** RELENG-1014 also asks for an alert on builds running past N
  hours. This plan leaves that to RELENG.

## Files to Modify

PR 1:
- `vars/tagBuilder.groovy` -- `getRunningUserId` adds the PR author and upstream owner; `tagBuilder` exports the requested-by user
- `Jenkinsfile` -- provision tests use `tagBuilder.getRunningUserId` instead of their per-stage `BUILD_USER_ID` override
- `configurations/triggers/sanity.yaml`, `configurations/triggers/scylla-doctor-gating.yaml`, `configurations/triggers/pgo-offline-installer.yaml` -- set `requested_by_user`

PR 2:
- `vars/tagBuilder.groovy` -- optional budget parameter, keep computation, cached cloud detection, OCI defined-tag merge
- `vars/longevityPipeline.groovy`, `vars/managerPipeline.groovy`, `vars/jepsenPipeline.groovy`, `vars/rollingUpgradePipeline.groovy`, `vars/rollingOperatorUpgradePipeline.groovy`, `vars/perfRegressionParallelPipeline.groovy`, `vars/perfSearchBestConfigParallelPipeline.groovy` -- second `tagBuilder` call after `getJobTimeouts`
- `vars/byoLongevityPipeline.groovy`, `vars/artifactsPipeline.groovy` -- pass their own stage-timeout budget
- `Jenkinsfile` -- pass its stage-timeout budget to the existing `tagBuilder` call
- `vars/triggerMatrixPipeline.groovy`, `vars/createTestJobPipeline.groovy` -- start-of-build `tagBuilder` call with their pipeline timeout as the budget
- `vars/sdReleaseGatingPipeline.groovy`, `jenkins-pipelines/qa/hydra-*.jenkinsfile`, `jenkins-pipelines/qa/trigger-matrix-arch-audit.jenkinsfile` -- start-of-build `tagBuilder` call
- `sdcm/utils/aws_builder.py` -- ASG launch tags `keep=6`; update tags of existing ASGs
- `sdcm/utils/gce_builder.py` -- template `keep=6`, `keep_action=terminate`, `billing_project=sct-builders`, version bump
- `sdcm/utils/builder_setup_groovy/oci_jenkind_plugin_config.groovy.tmpl` -- launch with `sct.*` defined tags
- `sdcm/utils/oci_builder.py` -- pass the launch tags to the template
- `sdcm/provision/oci/constants.py` -- add `billing_project` to the namespace keys
- `unit_tests/unit/` -- tests for the AWS ASG tags, GCE template labels and OCI template tags

PR 3:
- `vars/tagBuilder.groovy`, `vars/dockerLogin.groovy`, `vars/cleanSctRunners.groovy`, `vars/postTestSummaryComment.groovy`, `vars/checkoutQaInternal.groovy` -- internal `timeout`
- `Jenkinsfile`, `vars/createTestJobPipeline.groovy`, `jenkins-pipelines/qa/hydra-*.jenkinsfile`, `jenkins-pipelines/qa/trigger-matrix-arch-audit.jenkinsfile` -- pipeline-level `options { timeout }`
- `vars/managerPipeline.groovy`, `vars/jepsenPipeline.groovy`, `vars/rollingUpgradePipeline.groovy`, `vars/perfRegressionParallelPipeline.groovy` -- `?: 30` guard on the post fallback timeouts

## Verification

- [ ] PR 1: on a PR CI run, the builder, the integration-test SCT runner and the provision-test resources all carry the PR author as `RunByUser`
- [ ] PR 1: a timer run of `sanity` and a run started by an upstream job carry their owner, not `jenkins`
- [ ] PR 2: Unit tests for the three builder classes assert `keep=6` and `keep_action=terminate` in launch tags, and that an existing ASG gets its tags updated
- [ ] PR 2: Staging longevity build on an AWS builder: the instance shows `keep=6`, then the "+6" value at build start, then the formula value after "Get test duration"
- [ ] PR 2: Same check on a GCE builder, and on an OCI builder where the values appear under `sct.*` defined tags and `Oracle-Tags` is still present
- [ ] PR 2: A CI `Jenkinsfile` run on the PR shows the budget-based keep on its `v3-CI` builder
- [ ] PR 2: A staging trigger-matrix run and one `qa/hydra-*` run each raise the keep on their builder
- [ ] PR 2: Every jenkinsfile whose agent is a builder label reaches `tagBuilder`: grep for the builder labels and check each match
- [ ] PR 2: `clean_oci` in dry-run lists an OCI builder whose keep has expired, and skips one whose keep has not
- [ ] PR 2: Before running `configure-jenkins-builders`: the backports are merged on all active release branches
- [ ] PR 3: grep every `sh` and helper call in the builder pipelines; each is inside a stage, pipeline or helper timeout
- [ ] PR 3: a CI run where `postTestSummaryComment`'s step is made to hang fails at its timeout instead of holding the builder
- [ ] `uv run sct.py pre-commit` passes
