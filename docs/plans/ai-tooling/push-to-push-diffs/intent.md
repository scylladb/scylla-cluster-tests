# Push-to-push diffs for review rounds on SCT pull requests

**Jira:** SCT-1395

## Problem

During a review, the commits standard asks the author to push each fix as a
new commit. The reviewer then sees the fix. Before the merge, the author must
fold each fixup commit into the commit that it amends.

This rule makes extra work for the author. The author arranges the history
two times: one time for the review, and one time for the merge. The fold needs
a force-push after the approval, and the reviewers do not check the folded
history. When the author forgets the fold, the fixup commits go to `master`.

The author can fold each fix into its logical commit right away and
force-push. The reviewer then needs another way to find the fixes. GitHub
links each force-push to a compare view of the old head and the new head.
After a rebase, this view also shows the changes that came from `master`.

## Who it affects

- The authors of SCT pull requests, most of whom push from forks.
- The SCT maintainers and reviewers, who check each review round.
- The agents that follow `docs/standards/`.

## Evidence

`docs/standards/global/commits-and-pull-requests.md`, section "One Logical
Change per Commit":

> Each commit is one coherent step that a reviewer can read alone. During a
> review, push each fix as a new commit, so that the reviewer sees the fix.
> Fold each fixup commit into the commit that it amends before the merge.

Pull request #16699, review comment by dimakr
(https://github.com/scylladb/scylla-cluster-tests/pull/16699#discussion_r4445633308):

> This PR does not follow this standard yet. 8 of its 12 commits are
> fix-ups. So they should be folded into the commit they amend, before
> merging the PR.

The author answered
(https://github.com/scylladb/scylla-cluster-tests/pull/16699#discussion_r4451819143):

> true. I changed the rule in 8df5f0146: during a review, each fix goes in a
> new commit, so the reviewer sees it. We'll fold the fixups of this PR into
> their commits after the reviewers accept it.

Review comment by fruch
(https://github.com/scylladb/scylla-cluster-tests/pull/16699#discussion_r4451986653):

> something to followup, about the documented flow of the sdlc
>
> I think we should have better way to show the changes for review then
> fixup commits, it create extra work.
> I've seen the scylla core start something to automate show the diffs
> between pushes - maybe it can fit better for this purpose ?

Pull request #16699 merged with 17 commits. Its review fixes were not folded,
for example `docs(standards): fold fixup commits before the merge [SCT-1379]`
and `docs(skills): rename the idea documents to the flow documents
[SCT-1379]`.

98 of the last 100 merged SCT pull requests came from forks
(`gh pr list --state merged --limit 100 --json isCrossRepository`, on
2026-11-12).

scylladb/scylladb commit `e59d8b67c652`, "github: post range-diff on PR
force-push":

> When a PR series is force-pushed, reviewers have no easy way to see what
> changed since the version they reviewed: GitHub's compare link mixes in
> everything that landed on the base branch if the series was rebased.

## Definition of done

- Each force-push to an SCT pull request gets a readable diff against the
  previous push, as a comment or a link on the pull request. The reviewer
  does not need a local checkout.
- This works for pull requests from forks.
- A rebase on a newer `master` alone shows no change in the commits of the
  author.
- The diff does not run code from the pull request with write access or with
  repository secrets.
- The commits standard tells the author to fold each review fix into its
  logical commit before the push. `docs/development-flow.md` agrees with
  it.
- The commits that merge are the commits that the reviewers approved.

## Out of scope

- The merge method of SCT pull requests.
- Changes in other repositories, such as `scylladb/scylladb`.
- The AI reviewers (CodeRabbit, Qodo, Copilot, Claude) and how they read the
  diff. SCT-1393 and SCT-1394 cover them.
- Stacked pull requests and the `stack-sync` skill.
