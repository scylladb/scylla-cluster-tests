# Jenkins Builders

Builders are the Jenkins agents that SCT pipelines run on. `hydra configure-jenkins-builders`
creates them from code:

| Cloud | Code | Created in the cloud | Jenkins cloud | Label |
|---|---|---|---|---|
| AWS | `sdcm/utils/aws_builder.py` | launch template + ASG per region | EC2 Fleet | `aws-sct-builders-<region>-<VERSION>-asg` |
| GCE | `sdcm/utils/gce_builder.py` | instance template per project and region | Compute Engine | `<project>-builders-<region>-template-<VERSION>` |
| OCI | `sdcm/utils/oci_builder.py`, `builder_setup_groovy/oci_jenkind_plugin_config.groovy.tmpl` | nothing | OCI Compute | `oci-sct-builders-<region>-<VERSION>` |

Pipelines pick a label through `vars/getJenkinsLabels.groovy`. Each branch loads `vars/` from its
own checkout, but all branches share the builders. That one fact drives the procedure below: the
label is the only switch a branch controls, so a new label is how a change reaches one branch
and not the others.

Running configure needs:
- AWS credentials for the SCT account. Configure reads the Jenkins admin credentials
  (`jenkins.json`) and the GCP and OCI credentials from KeyStore.
- The cloud's `prepare-regions` already done (VPC, subnets, security groups).

Configure is idempotent per version: it creates what is missing and replaces the Jenkins cloud
of the same name. It never deletes an older version.

## How builders behave

- One build per builder: the plugins stop using a builder after its build (`maxTotalUses 1`;
  GCE `oneShot`) and terminate it once idle.
- Builders launch with `keep=168` and `keep_action=terminate`. If Jenkins loses track of a builder,
  `hydra-cleanup-cloud` terminates it a week after launch. OCI builders carry these as `sct.*`
  defined tags with `CreatedBy=SCT`, which is what `clean_oci` reads.
- A test longer than a week raises its builder's keep with the `builder_keep` longevity pipeline
  parameter (in hours), which `tagBuilder` writes at build start. `scale-450-500` does this.
- `aws-sct-builders-eu-west-1-v3-CI` (CI `Jenkinsfile`, trigger-matrix, `qa/*` jobs) was not created
  by this code, and is not covered by the above.

## Changing builder configuration

Any change to how builders launch is a new version, never an in-place change. This covers the
image, instance type, tags, use limits, labels and plugin settings. Re-running configure on an
existing version changes the builders of every branch at once, including release branches that
never got the change.

1. **Bump `VERSION`** of each builder class you change (`AwsBuilder`, `AwsFipsCiBuilder`,
   `GceBuilder`, `OciBuilder`). Names, launch templates, ASGs, instance templates and Jenkins
   clouds all include the version, so the new ones are created next to the old ones.
2. **Point `vars/getJenkinsLabels.groovy` at the new labels** in the same PR.
3. **Before merging, run configure from the PR branch** for each cloud you changed:
   ```shell
   hydra configure-jenkins-builders -c aws   # also creates the CI and FIPS ASGs
   hydra configure-jenkins-builders -c gce
   hydra configure-jenkins-builders -c oci
   ```
   This only adds resources. Nothing requests the new labels yet except the PR branch itself.
4. **Test from the PR branch:** run a short job on each changed cloud (for example
   `longevity-10gb-3h`, and `longevity-10gb-3h-gce` for GCE). Check that it lands on a new-version
   builder and that the builder has the expected tags.
5. **Merge.** Master moves to the new builders, and release branches stay on the old ones.
6. **Release branches opt in by backport.** Backport the `getJenkinsLabels` change (and anything
   it depends on) to a branch only when that branch should move.
7. **Retire the old version** once no active branch requests its labels:
   ```shell
   git grep -n '<old label>' upstream/master upstream/branch-<x.y> -- vars/getJenkinsLabels.groovy
   ```
   Then remove the old Jenkins clouds (Manage Jenkins → Clouds) and the old cloud resources:
   - AWS: the ASGs (`aws autoscaling delete-auto-scaling-group --force-delete`) and the
     `aws-sct-builders-<old VERSION>` launch template, in every region.
   - GCE: the instance templates, in every project and region.

### Example: bigger AWS builders, and a new default tag

Say AWS builders need 4 vCPUs and 16 GiB instead of 2 and 8 GiB, and every builder should carry
a new `team=qa` tag.

1. In `sdcm/utils/aws_builder.py`:
   - set `NUM_CPUS = 4` and `MemoryMiB` to 16384 in `mixed_instances_policy`;
   - add `{"Key": "team", "Value": "qa", "PropagateAtLaunch": True}` to
     `auto_scaling_group_tags`;
   - bump `VERSION` from `v5` to `v6`.

   The same change elsewhere: GCE sets `machine_type` and `labels` in
   `GceBuilder._create_launch_template`; OCI sets `NUM_OCPUS`, `MEMORY_GB`, `oci_shape` and the
   `tagList` in the groovy template. On OCI a new tag key must also exist in `SCT_TAG_KEYS`
   (`sdcm/provision/oci/constants.py`), and `prepare-regions -c oci` must have created it.
2. In `vars/getJenkinsLabels.groovy`, replace `-v5-asg` with `-v6-asg`.
3. From the PR branch: `hydra configure-jenkins-builders -c aws`. This creates
   `aws-sct-builders-v6`, the `...-v6-...` ASGs and their Jenkins clouds. The v5 ASGs keep
   serving everyone.
4. From the PR branch, run `longevity-10gb-3h`. The build should land on a `v6` builder with
   4 vCPUs and the `team=qa` tag.
5. Merge. Master builds now use v6.
6. Backport the `getJenkinsLabels` change to the release branches that should get the bigger
   builders.
7. When `git grep -- -v5-asg` finds nothing on any active branch, delete the v5 Jenkins clouds,
   the v5 ASGs and the `aws-sct-builders-v5` launch template.

Don't re-run configure with a changed instance type but the same `VERSION`. Configure updates the
instance requirements of an existing ASG in place, so the change would reach every branch at once.

### Why each step

- **New version, not in place:** an ASG, an instance template and a Jenkins cloud each serve every
  branch that requests their label. An in-place change hits all of them on their next build, with
  no review of what it does to a release branch's tests. GCE instance templates can't be edited
  at all.
- **Configure before merge:** the merge switches master's labels. If no cloud serves a label
  yet, every master job on it waits in the queue (see below).
- **Backport only the labels:** builder code runs only at configure time. Branches need just the
  label change, plus any `vars/` change the new builders rely on.
- **Retire last:** a release branch build may still ask for an old label months later.

### What failure looks like

- Builds stuck in the queue with "There are no nodes with the label ...": configure wasn't run
  for that version (or for that region or project), or its Jenkins cloud was removed while a
  branch still requested it.
- A builder that runs with no build on it for days: cleanup should terminate it after its keep.
  If it doesn't, check its `keep`, `keep_action` and (OCI) `CreatedBy` tags.
- `configure -c oci` fails with `NoSuchMethodException`: the OCI plugin's
  `BaremetalCloudAgentTemplate` constructor changed after a plugin upgrade. Match the
  argument list in the template to the installed version.

A new SCT runner image follows the same steps. Create the images first; see
[sct-runners.md](sct-runners.md).
