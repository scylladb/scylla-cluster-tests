---
name: updating-runner-images
description: >-
  Guides rolling out a new SCT runner image version and the Jenkins builders made from it,
  across AWS, GCE, Azure and OCI. Use when bumping SctRunner.VERSION in sdcm/sct_runner.py,
  changing install_prereqs, running create-runner-image or configure-jenkins-builders,
  bumping the AwsBuilder, GceBuilder or OciBuilder VERSION, updating the builder labels in
  vars/getJenkinsLabels.groovy, or shipping a kernel or security fix to the runners and
  builders. Also use when a runner from a new image does not boot or does not answer SSH.
  Not for test node images (Scylla AMIs), for the minicloud builders, or for a test that
  fails on a runner that boots fine.
---

# Updating SCT Runner Images

Roll a new runner image to all four clouds, prove it boots, then move the Jenkins builders onto it.

## Essential Principles

### Every backend is its own gate

Build one image per cloud, boot a runner from it, reboot it, and SSH in again before anything
points at it. The clouds boot differently (cloud-init datasources, network stacks, firewall
packages), so a pass on three clouds says nothing about the fourth: a unit that booted on
GCE, AWS and Azure hung the OCI boot forever.

### Same version, same image: check for a stale one first

`create-runner-image` does nothing when an image with the current version already exists. An
image left by an unmerged branch, or by a build you are redoing, makes the build a silent no-op
that leaves the old content in place. List the images of the version before you build, and
delete only the ones you made (check the creation date, in UTC).

### Builders follow the image through a new builder version

Builders are created from the runner image, and Jenkins finds them by labels that carry the
builder version. A new image therefore needs new `VERSION`s in the builder classes and the
matching labels in `vars/getJenkinsLabels.groovy`, in one commit, so a branch switches
image and labels together.

### Old builders stay until every branch moves

`configure-jenkins-builders` adds new clouds and templates next to the old ones. The release
branches keep their old labels until their backports merge, so never remove the old builders as
part of the rollout.

### Jenkins changes need the user's go

`configure-jenkins-builders` changes the shared Jenkins configuration. Stop before it, and
before deleting any image, and show what will change.

## When to Use

- A change to `SctRunner.install_prereqs` or anything else baked into the runner image
- A kernel, package or security fix that has to reach the runners and the builders
- Building runner images with `create-runner-image` for one or more clouds
- Moving the Jenkins builders to a new image with `configure-jenkins-builders`
- Bumping builder versions and the labels in `vars/getJenkinsLabels.groovy`
- A runner from a new image that never answers SSH

## When NOT to Use

- Building Scylla test node images (AMIs, GCE images): that is the Scylla packaging flow
- The minicloud KVM builders: they are configured outside this flow
- A test that fails on a runner that boots and answers SSH: debug the test
- Changing only a test's runner instance type: that is test configuration, not the image

## Phases

The full commands are in [rollout.md](workflows/rollout.md).

| Phase | What | Gate |
|-------|------|------|
| 1 | Change the image code, bump `SctRunner.VERSION` | Unit tests pass |
| 2 | List existing images of the version on every cloud | No stale image, or the user approved deleting it |
| 3 | `create-runner-image` per cloud | Log has "All packages successfully installed" and no traceback |
| 4 | Boot, check, reboot, check one runner per cloud | All four pass, user's go |
| 5 | Bump builder versions and the groovy labels, one commit | Label cross-check finds no missing label |
| 6 | `configure-jenkins-builders` per cloud | User's go, then one builder per cloud comes up |
| 7 | Run a test job on each new label, add the backport labels | Jobs pass on the new builders |

## Where Things Live

| What | Where |
|------|-------|
| Image version, `install_prereqs` | `sdcm/sct_runner.py` (`SctRunner.VERSION`) |
| AWS builders (and FIPS, derived from it) | `sdcm/utils/aws_builder.py` (`AwsBuilder`, `AwsFipsCiBuilder`) |
| GCE builders, one set per project | `sdcm/utils/gce_builder.py`, projects from `SUPPORTED_PROJECTS` |
| OCI builders | `sdcm/utils/oci_builder.py` |
| Labels the pipelines ask for | `vars/getJenkinsLabels.groovy` |
| Short manual description of the flow | `docs/sct-runners.md` |

## Reference Index

| File | Content |
|------|---------|
| [rollout.md](workflows/rollout.md) | The seven phases with exact commands, entry and exit criteria |
| [troubleshooting.md](references/troubleshooting.md) | Failures seen in past rollouts, how to diagnose each, and the fix |

## Success Criteria

- [ ] No stale image was reused: every image of the version was built in this rollout
- [ ] One runner per cloud booted, answered SSH, survived a reboot, and was terminated
- [ ] Builder versions and groovy labels changed in one commit, cross-check clean
- [ ] `configure-jenkins-builders` ran only after the user's go
- [ ] A test job ran on each new label
- [ ] Old builders are still registered for the branches that have not moved yet
