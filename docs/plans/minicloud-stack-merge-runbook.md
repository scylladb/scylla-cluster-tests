# minicloud stack — merge runbook

How to land [#15617](https://github.com/scylladb/scylla-cluster-tests/pull/15617) on its own and
restack the rest, one PR at a time. Nothing here merges more than one PR.

Written 2026-08-10 before the first merge; updated as the merges landed with what they actually
did. Steps 1 and 2 are complete (15617 and 15618 are on master) — Step 3 is the remaining one.

## The stack

| PR | branch | base | commits of its own | topology it adds |
|---|---|---|---|---|
| [#15617](https://github.com/scylladb/scylla-cluster-tests/pull/15617) | `feature/minicloud-integration` | **merged to `master`** | 9 | core SCT support, local `scripts/run-minicloud-test.sh` |
| [#15618](https://github.com/scylladb/scylla-cluster-tests/pull/15618) | `feature/minicloud-pipelines` | **merged to `master`** | 8 | Jenkins jobs on a nested-virt **sct-runner** |
| [#15668](https://github.com/scylladb/scylla-cluster-tests/pull/15668) | `feature/minicloud-local-agent` | `feature/minicloud-pipelines` | 2 | the **local KVM agent** alternative |

All three head branches live on `scylladb/scylla-cluster-tests` (required for native GitHub
stacking). [#15669](https://github.com/scylladb/scylla-cluster-tests/pull/15669) (bounded builder
log collection) was split out of #15618 and targets `master` directly — one commit,
`utils/upload_sct_coredump.sh` + `vars/collectBuilderLogs.groovy` +
`vars/collectTestCoredumps.groovy`, no file in common with any PR in this stack, so it is
**independent and mergeable in any order** (it is currently 12 commits behind master).

## Why 15617 can merge alone

Its tree references nothing that arrives later. Checked by name, `pr/15617` contains no reference
to `startMinicloud`, `stopMinicloud`, `minicloudPreflight`, `minicloudReclaim`,
`jenkins-pipelines/oss/minicloud/*.jenkinsfile`, `configurations/minicloud/{aws,gce}.yaml`, or
`test-cases/longevity/longevity-minicloud-10gb-1h.yaml`:

```bash
for f in startMinicloud stopMinicloud minicloudPreflight minicloudReclaim \
         configurations/minicloud/aws.yaml longevity-minicloud-10gb-1h; do
    echo "$f -> $(git grep -l -F "$f" pr/15617 -- ':!docs/plans' | tr '\n' ' ')"
done
```

What it *does* land standalone: `sdcm/utils/minicloud/`, the `minicloud_*` config options, the
mandatory `configurations/minicloud.yaml` overlay, `sct.py start-minicloud`,
`scripts/run-minicloud-test.sh` (`-f ami|repo|provision|upgrade|scale`), and 128 unit tests. Every
new behaviour is inert unless an endpoint variable activates minicloud, so master is unaffected
for anyone not running it.

The reverse is **not** true: 15618's own commits touch ten files 15617 also touches — `sct.py`,
`sdcm/tester.py`, `sdcm/sct_runner.py`, `sdcm/utils/aws_region.py`, `docs/minicloud.md`, and
`sdcm/utils/minicloud/{bootstrap,config,gcp,manager,networking}.py`. 15618 and 15668 therefore stay
stacked and are restacked after each merge; they cannot be reordered ahead of 15617.

## Step 1 — merge 15617 (done 2026-08-10)

Merged by rebase; its 9 commits are on master as `7e4f498419`..`b214d08781`, every patch replayed
unchanged — `git range-diff af8b09ced2~9..af8b09ced2 7e4f498419^..b214d08781` shows all nine as
`=`. Do not check with a plain `git diff` against the PR head: master had moved 19 files under the
PR by merge time, two of them files this PR also touches, so the *trees* differ even though the
*patches* do not. Gates it had to clear:

- the four required contexts: `jenkins/precommit`, `jenkins/unittests`,
  `jenkins/lint_test_cases`, `label`
- **one maintainer approval** (`required_approving_review_count: 1`). All 13 review threads
  (8 from dimakr, 5 from the copilot bot) were answered and resolved, each fix re-verified present
  in the head rather than taken from the reply.

Merge with **Rebase and merge**: master has zero merge commits in its last 200 and no squash
`(#NNNNN)` suffixes in its last 100.

Do not touch 15618/15668 until this is in.

**A GitHub *stack object* is a separate thing from the base refs, and it blocks the merge.** These
PRs were bound into stack #15619 (size 3); while it existed, GitHub refused even to retarget a
member — `Cannot change the base branch because the pull request is part of a stack` — and the
merge button stayed unavailable on 15617 despite its base already being `master`. Dissolve it
first, which leaves every base ref untouched:

```bash
gh api graphql -f query='{repository(owner:"scylladb",name:"scylla-cluster-tests"){
    pullRequest(number:15617){stack{number size}}}}'   # read the stack number
gh stack unstack 15619
```

Reversible with `gh stack link 15617 15618 15668` if the UI grouping is wanted back. Admins can
then merge past a missing approval with `gh pr merge 15617 --rebase --admin` (`enforce_admins` is
false on `master`).

## Step 2 — restack 15618 onto master (done 2026-08-10; merged 2026-08-11)

Rebase was clean and the resulting diff is 8 commits / 32 files, as predicted. Two caveats that
only showed up in the doing are recorded below: the precommit failure, and the branch-name clash.

```bash
git fetch upstream master \
    'refs/pull/15618/head:refs/remotes/pr/15618' \
    'refs/pull/15668/head:refs/remotes/pr/15668' --force

# 15617's head as it was at merge time — not the SHA it had when this runbook was written, which
# is why it is read rather than remembered. Capture it BEFORE the branch is deleted, or recover
# it from the PR's merged-commit reference afterwards.
OLD_BASE=af8b09ced2

# Use a scratch branch name: `git checkout -B feature/minicloud-pipelines` fails with
# "already used by worktree at ..." if that branch is checked out in another worktree, and the
# rebase then silently runs on whatever branch you were on.
git checkout -B restack18 pr/15618
git rebase --onto upstream/master $OLD_BASE
git rev-list --count upstream/master..HEAD    # expect 8

git push --force-with-lease=feature/minicloud-pipelines:$(git ls-remote upstream \
    refs/heads/feature/minicloud-pipelines | cut -f1) upstream HEAD:feature/minicloud-pipelines
```

Pin `--force-with-lease` to that separately-read SHA rather than the bare form; a chained fetch
re-arms the bare version and it stops protecting against a concurrent push.

Then, on the PR: confirm the base is `master` (GitHub usually retargets it automatically; if not,
`gh pr edit 15618 --base master`), and check the diff is 8 commits / ~1365 insertions across 32
files — not 40+ files, which would mean the rebase did not take.

**`jenkins/precommit` does not necessarily clear on its own here — always read it after the
restack.** The stacking failure and a real failure look identical from the outside, and on 15618
the first was hiding the second. Its console showed both at once:

```
fatal: ambiguous argument 'origin/master..HEAD': unknown revision or path not in the working tree.
ruff-format..............................................................Failed
```

The first line is the stacking artifact — Jenkins' github-branch-source derives `baseHash` from a
merge ref it cannot reach for a stacked PR, so `origin/master..HEAD` does not resolve. The second
was genuine: two files in 15618's own commits failed `ruff format --check` while passing at master.
So check the changed files directly rather than trusting the base change to fix it:

```bash
.venv/bin/ruff format --check $(git diff --name-only upstream/master..HEAD | grep '\.py$')
.venv/bin/ruff check $(git diff --name-only upstream/master..HEAD | grep '\.py$')
```

Fix with `git commit --fixup=<sha>` per owning commit, then
`GIT_SEQUENCE_EDITOR=true git rebase -i --autosquash upstream/master` — no fixup commits left in
the history — and restack the PR above it again on the new head.

Merged 2026-08-11 by rebase; its 8 commits are on master as `72f889859b`..`ae5f85a892`, again with
every patch replayed unchanged (`git range-diff` all `=`). The only end-to-end evidence noted in
the PR is the staging AMI artifact job (#24); the GCE image job and the longevity job were not
linked.

## Step 3 — restack 15668 onto master

Same shape, now that 15618 has landed:

```bash
# 15618's head at merge time — read from the PR's merged headRefOid, not remembered. 15668's two
# commits sit exactly on it (`git rev-parse pr/15668~2` gives the same SHA).
OLD_BASE=1729aa537e

git checkout -B restack68 pr/15668
git rebase --onto upstream/master $OLD_BASE
git rev-list --count upstream/master..HEAD    # expect 2

git push --force-with-lease=feature/minicloud-local-agent:$(git ls-remote upstream \
    refs/heads/feature/minicloud-local-agent | cut -f1) upstream HEAD:feature/minicloud-local-agent

# Note: 15668 must be restacked again after ANY force-push to 15618, not just after its merge.
```

15668 also carries the one acknowledged blocker for the whole effort: no Jenkins node serves
`minicloud-kvm-builders-v1` yet, so the local-agent path cannot be run end-to-end until a spider
is labelled. That is a reason to land 15617 and 15618 without it, not a reason to hold them.

## If a restack conflicts

It did not in the dry run, but master moves. The conflict will be in one of the ten shared files
listed above, and in every case the resolution is "keep master's version of 15617's hunk, reapply
15618's addition on top" — 15618 only adds to what 15617 introduced. Re-run
`.venv/bin/python -m pytest unit_tests/unit/minicloud/ -q` after resolving; it should stay at 128
passed for 15617's set plus 15618's `test_networking.py` / `test_region_enumeration.py`.

## Checks worth repeating at each step

```bash
# unit tests for the minicloud package
.venv/bin/python -m pytest unit_tests/unit/minicloud/ -q

# config resolution both backends (15617)
./sct.py conf -b aws '["test-cases/minicloud-provision-test.yaml", "configurations/minicloud.yaml"]'
./sct.py conf -b gce '["test-cases/minicloud-provision-test.yaml", "configurations/minicloud.yaml"]'

# jenkinsfile linting (15618/15668)
./sct.py lint-pipelines --pipeline-dir jenkins-pipelines/oss/minicloud
```

`git commit` needs `.venv/bin` on `PATH` in this repo, or the `language: system` pre-commit hooks
fail with "Executable not found".
