# Mini-Plan: Jenkins Builders: Run Owner, Keep Duration and Timeouts

**Date:** 2026-10-11
**Estimated LOC:** ~60 (PR 1) + ~60 (PR 2) + ~80 (PR 3) + ~120 (PR 4)
**Related PR:** #16431 (this plan), #16544 (builder `billing_project`, promoter account)
**Jira:** [SCT-1210](https://scylladb.atlassian.net/browse/SCT-1210), related [RELENG-1014](https://scylladb.atlassian.net/browse/RELENG-1014), [SCT-1303](https://scylladb.atlassian.net/browse/SCT-1303)

## Problem

Every SCT Jenkins builder launches with `keep=alive`: the AWS builder ASGs propagate it to their
instances and the GCE instance template carries it as a label. OCI builders launch with no tags,
and `clean_oci` only looks at SCT's defined tags, so it never sees them. If Jenkins fails to stop a
job or release its builder, nothing ever cleans the builder up. This is a rare edge case, so the
safety net should be simple.

AWS and OCI builders are also reused across builds (EC2 Fleet `maxTotalUses 10`; OCI until 6 idle
minutes, with no cap). GCE builders already serve one build each. Reuse is to stop regardless:
a build should start on a fresh builder.

Resources are not attributed to the person who started the run. `tagBuilder` takes the owner only
from the `requested_by_user` parameter or a manual-start cause. Anything else is tagged
`RunByUser=jenkins`:
- SCT CI on a pull request: the builder, and the integration-test SCT runner.
- Builds started by an upstream job without `requested_by_user`.

The CI provision tests are the exception: before `runSctTest` they set the build-wide
`BUILD_USER_ID` to the PR author's GitHub login.

A finished build releases its builder whatever the `keep` value is. A builder outlives its build only
when the build hangs, and builds can hang because many steps have no timeout. RELENG-1014: SCT PR
job PR-14882 #2 held a CI builder for 12 days. Its pipeline-level `post` step
(`postTestSummaryComment`, a `hydra.sh` call) lost its durable-task directory across a controller
restart and retried forever. Nothing around it had a timeout.
- 17 of the 20 builder pipelines have no pipeline-level timeout, including `Jenkinsfile` and
  every test pipeline except `byoLongevityPipeline`.
- None of the shared helpers that run shell steps has a timeout of its own: `tagBuilder`,
  `dockerLogin`, `cleanSctRunners`, `postTestSummaryComment`, `checkoutQaInternal`. Each is called
  without one in several places, including the `post`/`finally` cleanup fallbacks.
- `hydra-show-monitor` and `hydra-show-jepsen-results` have no timeout at all.
- When 'Get test duration' fails, the log-collection and cleanup timeouts are left unusable. In
  longevity and rollingUpgrade they stay 0, and `timeout(time: 0)` aborts at once. In
  manager, jepsen, perfRegressionParallel, perfSearchBestConfig and rollingOperatorUpgrade they are
  undefined in `call()`. Only longevity's post fallbacks guard against it, with `?: 30`.

## Approach

Four PRs. PR 1, PR 2 and PR 3 are independent; PR 4 follows PR 2.
- **PR 1, owner attribution:** `tagBuilder` plus dropping the `Jenkinsfile` override, with no infra
  step. Backported like any fix.
- **PR 2, keep phase 1:** one build per builder, and builders launch with a one-week keep instead
  of `alive`. Builder configs only: no pipeline change, no backport.
- **PR 3, bounded builds:** timeouts, so a hung build releases its builder.
- **PR 4, keep phase 2:** each build lowers its builder's keep to its own time budget.

Alternative considered for the keep: reconciliation (soyacz). The scheduled cleanup would compare
the cloud's builders with the Jenkins node list and terminate builders Jenkins doesn't know. It is
faster, and never touches a builder Jenkins knows. It was not chosen for now: it is new code that
needs the Jenkins API and terminates instances, does nothing while Jenkins is down, and gives no
bound for a build hung on a live node. Once reuse stops, the keep approach needs none of that.

### PR 1: the owner is whoever started the run, manually or automatically

Builds on #16544 (SCT-1303), which already skips the promoter account, exports the requested-by
user in the inline run-test blocks and tags builders with `params.billing_project`.
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
- The CI provision tests' `BUILD_USER_ID = CHANGE_AUTHOR` assignments are removed:
  `get_username` reads the exported requested-by user first, so they have no effect anymore.
- Trigger configs need no change: a value in their `defaults` would reach every child job and win
  over the real starter or upstream owner. Only `cron_triggers[].params` may set it, as `tier1`
  and `perf-regression` do.
- **Needs Investigation:** a PR author is a GitHub login, while other owners are scylladb.com user
  names. Decide whether to map logins to user names (and from what source), or to accept the
  GitHub login as is.

### PR 2: one build per builder, one-week default keep

- **No reuse.** AWS: `maxTotalUses 1`. GCE is already one-shot. OCI: **Needs Investigation**, since
  the plugin template exposes no single-use setting. A builder's launch time is now its build's
  start time, so a keep counted from launch is a keep counted from the build.
- **Launch with `keep=168` and `keep_action=terminate`** on all three clouds:
  - AWS: the ASG tags. Re-running the configure command also updates the tags of an ASG that
    already exists; today it only updates the instance policy. The launch template's own
    `keep=alive` tag stays: it protects the template, not instances.
  - GCE: the template labels and metadata. Instance templates can't be modified, so the builder
    version is bumped.
  - OCI: the plugin template launches builders with `sct.*` defined tags: `NodeType=builder`,
    `keep`, `keep_action`, `CreatedBy=SCT`. All four keys already exist in the namespace, so
    `clean_oci` sees builders with no namespace change.
- **One week** covers every test on master except `scale-450-500` (21 days). Its jenkinsfile passes
  a longer keep through a `builder_keep` pipeline parameter, which `tagBuilder` writes at build start.
- **Cost of no reuse:** every build waits for a fresh builder to boot, a few minutes, as GCE builds
  already do.
- **Rollout:**
  1. Run `configure-jenkins-builders -c gce` from the PR 2 branch before merging. It adds the
     new-version template and Jenkins cloud next to the current one, because both names include
     the version.
  2. Merge PR 2, including the `getJenkinsLabels` change to the new GCE label, and backport that
     one-line change.
  3. Run `configure-jenkins-builders` for AWS and OCI.
  4. Remove the old GCE template and cloud once no branch requests its label.
  - Release-branch builds need no other backport: a one-week keep is longer than any of their
    tests.
  - The `aws-sct-builders-eu-west-1-v3-CI` ASG is not created by this repo. It gets the same tags
    and use limit by hand, or its jobs move to v4 and it is retired. **Needs Investigation:** which
    of the two.

### PR 3: every builder-bound step has a timeout

- **Timeouts live in the helpers.** `tagBuilder`, `dockerLogin`, `cleanSctRunners`,
  `postTestSummaryComment` and `checkoutQaInternal` each wrap their own shell work in a `timeout`
  sized to that work. Every caller is covered at once, including the post and `finally` fallbacks,
  with no change at the call sites. `cleanSctRunners` gets the longest one, since it tears the
  runner down. `tagBuilder`'s IMDS `curl` calls also get `--max-time`.
- **Pipeline-level backstop where the budget is static:**
  - `Jenkinsfile`: the sum of its stage timeouts.
  - `createTestJobPipeline`: 360 minutes.
  - The `qa/hydra-*` jobs and `trigger-matrix-arch-audit`: a short fixed timeout.
- **Test pipelines** get no pipeline-level timeout: their duration is known only after
  'Get test duration', and `options` is fixed before that. Instead every step in them gets a bound:
  stage timeouts, the helper timeouts above, and a short `timeout` around the remaining bare `sh`
  steps, the status echo in `post`/`finally` and the `sh "exit 1"` in the BYO stage `post`.
- **Zero-timeout guard.** The test pipelines initialise `collectLogsTimeout` and
  `resourceCleanupTimeout` to 30 instead of 0, in the script binding (no `def`), as longevity and
  rollingUpgrade already declare them. That covers the main stages and the fallbacks in one place,
  and replaces longevity's `?: 30`. manager, jepsen, perfRegressionParallel and perfSearchBestConfig
  declare them with `def`, which hides them from `call()`, so a failed 'Get test duration' there
  ends in an undefined-variable error. rollingOperatorUpgrade doesn't declare them at all.
- **Stuck-build alert, out of repo:** RELENG-1014 also asks for an alert on builds running past N
  hours. This plan leaves that to RELENG.

### PR 4: lower the keep to each build's budget

With no reuse and a one-week default, this phase only shortens how long a lost builder can live.
- After 'Get test duration', `tagBuilder` sets `keep = ceil(runnerTimeout / 60) + 6`. That is the
  same formula as the SCT runner's keep. The +6 hours covers the stages outside `runnerTimeout`, up
  to 350 minutes on Azure: Estimate Test Cost, BYO Scylladb, Create SCT Runner, Provision Resources
  and Finish Argus Test Run.
- Sub-test `node(builder.label)` blocks (perfRegressionParallel, rollingUpgrade,
  perfSearchBestConfig, artifacts) run on their own builders and make the same call. Cloud
  detection is per call, with no build-wide cache, so each call tags its own builder.
- Jobs that don't call it keep the one-week default: the non-test jobs, and release branches
  without the backport. No rollout order is needed; a missing call only means a longer keep.
- OCI: `tagBuilder` writes `keep` into the instance's `sct` defined tags with a read, merge, write,
  keeping other namespaces such as `Oracle-Tags`.

## Files to Modify

PR 1:
- `vars/tagBuilder.groovy` -- `getRunningUserId` adds the PR author and upstream owner; `tagBuilder` exports the requested-by user
- `Jenkinsfile` -- drop the `BUILD_USER_ID = CHANGE_AUTHOR` assignments

PR 2:
- `sdcm/utils/aws_builder.py` -- `maxTotalUses 1`; ASG launch tags `keep=168`; update tags of existing ASGs
- `sdcm/utils/gce_builder.py` -- template `keep=168`, `keep_action=terminate`, version bump
- `vars/getJenkinsLabels.groovy` -- GCE labels to the new template version
- `sdcm/utils/builder_setup_groovy/oci_jenkind_plugin_config.groovy.tmpl`, `sdcm/utils/oci_builder.py` -- launch with `sct.*` defined tags; single use
- `vars/tagBuilder.groovy`, `vars/longevityPipeline.groovy`, `jenkins-pipelines/oss/scale/scale-450-500-cluster-test.jenkinsfile` -- `builder_keep` override
- `unit_tests/unit/` -- tests for the AWS ASG tags and use limit, GCE template labels and OCI template tags

PR 3:
- `vars/tagBuilder.groovy`, `vars/dockerLogin.groovy`, `vars/cleanSctRunners.groovy`, `vars/postTestSummaryComment.groovy`, `vars/checkoutQaInternal.groovy` -- internal `timeout`
- `Jenkinsfile`, `vars/createTestJobPipeline.groovy`, `jenkins-pipelines/qa/hydra-*.jenkinsfile`, `jenkins-pipelines/qa/trigger-matrix-arch-audit.jenkinsfile` -- pipeline-level `options { timeout }`
- `vars/longevityPipeline.groovy`, `vars/managerPipeline.groovy`, `vars/jepsenPipeline.groovy`, `vars/rollingUpgradePipeline.groovy`, `vars/rollingOperatorUpgradePipeline.groovy`, `vars/perfRegressionParallelPipeline.groovy`, `vars/perfSearchBestConfigParallelPipeline.groovy` -- initialise the log/cleanup timeouts to 30 in the script binding, without `def`; `timeout` around the bare `sh` steps in `post`/`finally` and the BYO stage `post`

PR 4:
- `vars/tagBuilder.groovy` -- optional budget parameter writes `ceil(budget / 60) + 6`; OCI defined-tag merge
- `vars/longevityPipeline.groovy`, `vars/managerPipeline.groovy`, `vars/jepsenPipeline.groovy`, `vars/rollingUpgradePipeline.groovy`, `vars/rollingOperatorUpgradePipeline.groovy`, `vars/perfRegressionParallelPipeline.groovy`, `vars/perfSearchBestConfigParallelPipeline.groovy`, `vars/artifactsPipeline.groovy` -- `tagBuilder(runnerTimeout)` after `getJobTimeouts` and at the start of each sub-test `node()` block

## Verification

- [ ] PR 1: on a PR CI run, the builder, the integration-test SCT runner and the provision-test resources all carry the PR author as `RunByUser`
- [ ] PR 1: a run started by an upstream job carries the upstream build's owner, not `jenkins`
- [ ] PR 2: unit tests assert `keep=168` and `keep_action=terminate` in the launch tags of all three builder classes, and `maxTotalUses 1` on AWS
- [ ] PR 2: after configure, a new builder on each cloud shows `keep=168` (OCI under `sct.*`), and the second build on the same label gets a new builder
- [ ] PR 2: `clean_oci` in dry-run lists OCI builders and keeps them while their week runs
- [ ] PR 2: a GCE build on master and on a release branch lands on the new template version
- [ ] PR 3: grep every `sh` and helper call in the builder pipelines; each is inside a stage, pipeline or helper timeout
- [ ] PR 3: a CI run where `postTestSummaryComment`'s step is made to hang fails at its timeout instead of holding the builder
- [ ] PR 4: a staging longevity build shows `keep=168`, then `ceil(runnerTimeout / 60) + 6` after 'Get test duration', on AWS, GCE and OCI
- [ ] PR 4: in a perfRegressionParallel build, each sub-test builder carries its own lowered keep
- [ ] `uv run sct.py pre-commit` passes
