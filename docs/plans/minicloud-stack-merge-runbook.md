# minicloud stack — merge runbook

How to land [#15617](https://github.com/scylladb/scylla-cluster-tests/pull/15617) on its own and
restack the rest, one PR at a time. Nothing here merges more than one PR.

Verified 2026-08-10 against `upstream/master` = `db9425d6e1`.

## The stack

| PR | branch | base today | commits of its own | topology it adds |
|---|---|---|---|---|
| [#15617](https://github.com/scylladb/scylla-cluster-tests/pull/15617) | `feature/minicloud-integration` | `master` | 8 | core SCT support, local `scripts/run-minicloud-test.sh` |
| [#15618](https://github.com/scylladb/scylla-cluster-tests/pull/15618) | `feature/minicloud-pipelines` | `feature/minicloud-integration` | 8 | Jenkins jobs on a nested-virt **sct-runner** |
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

## Step 1 — merge 15617

Remaining gates, as of writing:

- `jenkins/precommit` ✅ (build #21)
- `continuous-integration/jenkins/pr-head` — must go green
- **one maintainer approval** — `reviewDecision` is `REVIEW_REQUIRED`. All 13 review threads
  (8 from dimakr, 5 from the copilot bot) are answered and resolved; each fix was re-verified
  present in `139b5499db`.

Merge with **Rebase and merge**. Master has zero merge commits in its last 200 and no squash
`(#NNNNN)` suffixes in its last 100, so the convention is a rebase that replays the 8 commits
individually.

Do not touch 15618/15668 until this is in — GitHub auto-retargets 15618 to `master` when
`feature/minicloud-integration` is deleted, and that retarget alone leaves its diff wrong (it will
show 15617's changes again until the rebase below).

## Step 2 — restack 15618 onto master

Dry-run verified clean, and the resulting tree is byte-identical to today's `pr/15618`, under both
rebase-merge and squash-merge semantics.

```bash
git fetch upstream master \
    'refs/pull/15618/head:refs/remotes/pr/15618' \
    'refs/pull/15668/head:refs/remotes/pr/15668' --force

# The old base commit — 15617's head as it was before the merge. Capture it BEFORE deleting
# the branch, or read it from the PR's "merged commit" reference afterwards.
OLD_BASE=139b5499db

git checkout -B feature/minicloud-pipelines pr/15618
git rebase --onto upstream/master $OLD_BASE
git rev-list --count upstream/master..HEAD    # expect 8

git push --force-with-lease=feature/minicloud-pipelines:$(git rev-parse pr/15618) \
    upstream feature/minicloud-pipelines
```

Pin `--force-with-lease` to that separately-read SHA rather than the bare form; a chained fetch
re-arms the bare version and it stops protecting against a concurrent push.

Then, on the PR: confirm the base is `master` (GitHub usually retargets it automatically; if not,
`gh pr edit 15618 --base master`), and check the diff is 8 commits / ~1365 insertions across 32
files — not 40+ files, which would mean the rebase did not take.

**`jenkins/precommit` should flip from fail to pass here.** It fails today only because Jenkins'
github-branch-source derives `baseHash` from a merge ref it cannot reach for a stacked PR; a
`master`-based PR has a reachable one. If it still fails after the restack, that is a real failure
and needs reading.

Review and merge 15618 on its own. Note in the PR that the only end-to-end evidence is the staging
AMI artifact job (#24); the GCE image job and the longevity job have not been linked.

## Step 3 — restack 15668 onto master

Same shape, after 15618 lands:

```bash
OLD_BASE=fb0626590d      # 15618's head before its merge

git checkout -B feature/minicloud-local-agent pr/15668
git rebase --onto upstream/master $OLD_BASE
git rev-list --count upstream/master..HEAD    # expect 2

git push --force-with-lease=feature/minicloud-local-agent:$(git rev-parse pr/15668) \
    upstream feature/minicloud-local-agent
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
