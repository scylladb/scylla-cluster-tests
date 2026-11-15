# Runner Image Rollout

Eight phases, from the code change to test jobs on the new builders and the retirement of the old
ones. Run every command from the repository root.

## Phase 1: Change the image and bump its version

**Entry:** the change to bake into the image is known (a package, a kernel, a systemd unit).

1. Change `SctRunner.install_prereqs()` in `sdcm/sct_runner.py`. The script runs as root on the
   image builder over SSH. Keep long scripts readable by building them in Python and passing them
   base64-encoded (`printf %s <b64> | base64 -d | bash`), which avoids a second layer of quoting.
2. Bump `SctRunner.VERSION` (for example `1.20` to `1.21`). `AwsFipsSctRunner.VERSION` derives
   from it, so it moves by itself.
3. If the change adds a systemd unit that starts early in boot, read
   [troubleshooting.md](../references/troubleshooting.md) on boot ordering first. An ordering mistake
   there passes on some clouds and hangs the boot on others.
4. Run the unit tests of what you changed and `uv run python -m pytest unit_tests/unit/test_sct_runner.py`.

**Exit:** tests pass, the change is committed on a branch.

## Phase 2: Check for a stale image of the version

**Entry:** Phase 1 is done.

`create-runner-image` skips the build when an image of the version exists, so a leftover image
silently wins.

1. List the images of the version on each cloud:
   - AWS: `describe_images` with `Owners=["self"]` and tags `Name=sct-runner-<version>`,
     `Version=<version>` in `eu-west-2`.
   - GCE: image `sct-runner-<version with dashes>` in each project of `SUPPORTED_PROJECTS`.
   - Azure: gallery version `sct-runner/<version>.0` in gallery `SCT_IMAGES`.
   - OCI: images named `sct-runner-<version with dashes>` in `us-ashburn-1`, `us-phoenix-1` and
     `eu-frankfurt-1`.

   `SctRunner._image(ImageType.SOURCE)` from `get_sct_runner(...)` returns exactly the image the
   build checks for, on every cloud.
2. For every image found: check its creation date (in UTC, GCE returns Pacific time) and which branch
   made it. Ask the user before deleting it. Delete only images nothing uses: no builder template,
   instance configuration or launch template points at them.
3. On AWS delete the AMI's snapshots with the AMI.

**Exit:** no image of the version exists, or each one that exists was built from this change.

## Phase 3: Build the images

**Entry:** Phase 2 is done.

Run the builds in parallel and keep each log:

```bash
uv run sct.py create-runner-image -c aws -r eu-west-2 -z a
uv run sct.py create-runner-image -c gce -r us-east1 -z b
SCT_GCE_PROJECT=gcp-local-ssd-latency uv run sct.py create-runner-image -c gce -r us-east1 -z b
uv run sct.py create-runner-image -c azure -r eastus -z a
uv run sct.py create-runner-image -c oci -r us-ashburn-1 -z a
```

1. In each log, look for `All packages successfully installed.` and then the image line
   (`Image 'ami-...' is available`, `Image created: ocid1.image...`, `SCT Image Builder instance stopped.`).
2. A traceback after the install line means the image step failed. The builder instance
   `sct-runner-<version>-builder-<region>` is left behind: delete it before you retry, or the retry
   fails on the name.
3. A build that prints `No need to copy SCT Runner image since it already exists` with no install
   step reused an image: go back to Phase 2.

**Exit:** five logs with the install line and an image, no traceback.

## Phase 4: Boot gate, one runner per cloud

**Entry:** Phase 3 is done.

1. Create a runner from each image. `create-runner-instance` waits for SSH and fails with
   `Unable to SSH to <ip>! Exiting...` when the runner never answers:

   ```bash
   uv run sct.py create-runner-instance -c aws -r eu-west-2 -z a -t <test id> -d 60
   uv run sct.py create-runner-instance -c gce -r us-east1 -z b -t <test id> -d 60
   uv run sct.py create-runner-instance -c azure -r eastus -z a -t <test id> -d 60
   uv run sct.py create-runner-instance -c oci -r us-ashburn-1 -z a -t <test id> -d 60
   ```

   Use a different made-up test id per cloud, so each runner is easy to find and clean.
2. Check each runner. On AWS, GCE and Azure use `uv run sct.py ssh-cmd -t <test id> -P '<command>'`.
   On OCI `ssh-cmd` does not find the instance; use
   `ssh -i ~/.ssh/scylla_test_id_ed25519 ubuntu@<ip> '<command>'`.
   Check at least:
   - `uname -r`: the expected kernel
   - `systemctl is-active` and `is-enabled` of every unit the change added
   - `docker --version`, `java -version`
   - `systemctl is-active sct-jenkins-ssh-sync` and a non-empty `/home/jenkins/.ssh/authorized_keys`:
     Jenkins connects to the builders through them
3. Reboot (`sudo systemctl reboot`), wait for SSH with a new `/proc/sys/kernel/random/boot_id`, and
   run the same checks. The first boot runs cloud-init's new-instance path, the reboot runs the
   normal path, and a regression can sit in either.
4. A runner that never answers SSH: read its serial console before you terminate it, see
   [troubleshooting.md](../references/troubleshooting.md).
5. Terminate each runner by its IP:

   ```bash
   uv run sct.py clean-runner-instances --runner-ip <ip> --force -b <cloud>
   ```

6. Report the result per cloud to the user and wait for the go.

**Exit:** four runners passed before and after reboot, all terminated, the user said go.

## Phase 5: Bump the builders and the labels

**Entry:** the user approved Phase 4.

1. Bump `VERSION` in `AwsBuilder` and `AwsFipsCiBuilder` (`vN-fibs`), `GceBuilder` and `OciBuilder`.
   `AwsCiBuilder` takes its version from `AwsBuilder`.
2. Change every builder label in `vars/getJenkinsLabels.groovy` to the new versions. Labels the
   builders create:
   - AWS: `aws-sct-builders-<region>-<version>-asg`, FIPS `aws-sct-builders-us-east-1-<version>-CI-FIPS`
   - GCE: `gcp-<project>-builders-<region>-template-<version>` (the groovy builds `gcp-<project>` from `gce_project`)
   - OCI: `oci-sct-builders-<region>-<version>`
3. Move the pinned labels too. The PR job, the hydra utility jobs and the trigger matrix ask for
   `aws-sct-builders-eu-west-1-<version>-CI` (made by `AwsCiBuilder`) directly, and freestyle job
   XMLs set `<assignedNode>`, all outside `getJenkinsLabels.groovy`, so neither the groovy map nor
   the Phase 6 label check sees them:

   ```bash
   git grep -n 'sct-builders-' -- Jenkinsfile jenkins-pipelines vars ':!vars/getJenkinsLabels.groovy'
   ```

   Move every hit to the new version. Jenkins doesn't reload a job XML from the repository: after
   Phase 6, change that job's live config in Jenkins too.

4. Commit the builder versions and all the labels together.
5. Phase 6 must finish on every cloud before this commit merges. Master jobs ask for the new labels
   as soon as it merges, and a label without a cloud or template makes them wait forever.

**Exit:** one commit with the builder versions, the groovy labels and the pinned labels.

## Phase 6: Configure the Jenkins builders

**Entry:** Phase 5 is pushed and the user approved changing Jenkins.

The PR job asks for the new `-CI` label of Phase 5 step 3, so it stays queued until this phase
creates the AWS CI builder. That wait is expected: the job starts by itself once the builder
exists, and doubles as the check for the CI label.

```bash
uv run sct.py configure-jenkins-builders -c aws
uv run sct.py configure-jenkins-builders -c gce
uv run sct.py configure-jenkins-builders -c oci
```

The GCE command loops over every project in `SUPPORTED_PROJECTS` by itself, so each project needs its
image from Phase 3 first.

1. Azure has no builders (`configure-jenkins-builders -c azure` raises): Azure jobs run on AWS
   builders in `us-east-1`.
2. Don't skip OCI. It is the easiest cloud to forget: no Jenkins node carries an OCI label until a
   job asks for one, so nothing looks missing until OCI jobs hang (see
   [troubleshooting.md](../references/troubleshooting.md)).
3. The OCI step copies the image into each region that lacks it (phoenix and frankfurt), so it
   runs longer than the others. It loops over `get_all_regions("oci")`, tries every region, and
   exits non-zero with the list of failed ones; rerun with `-r <region>` for each.
4. Leave the old clouds and templates in place.
5. Check that Jenkins serves every label. OCI and GCE builders are on-demand clouds, so no node
   carries a new label until a job runs on it; the command asks the Jenkins label API for a node
   or a cloud instead:

   ```bash
   uv run sct.py check-jenkins-builder-labels
   ```

   It exits 1 and lists the labels of `vars/getJenkinsLabels.groovy` that nothing serves, which
   also catches a typo in a label. Check a release branch's copy with `--groovy-file`. It doesn't
   read the pinned labels of Phase 5 step 3: the queued PR job picking up a builder confirms
   the CI label, and the next run of each job with an XML pin confirms its label.

**Exit:** the new clouds and templates exist in Jenkins next to the old ones,
`check-jenkins-builder-labels` passes, and the PR job left the queue.

## Phase 7: Validate on Jenkins and backport

**Entry:** Phase 6 is done.

1. Run one existing test job per backend from the PR branch, so it picks up the new labels. Reuse
   existing jobs, pass the branch as a parameter, and confirm the build ran on a new builder (its
   node name carries the new builder version).
2. Add the backport labels for every live branch: the branches pick up the labels, and therefore
   the new builders, only when their backport merges.
3. After every branch moved, retire the old builders with Phase 8.

**Exit:** a job passed on each new label, backport labels are set.

## Phase 8: Retire the old builder versions

**Entry:** every backport of Phase 7 merged. Old versions pile up when nobody retires them (in
November 2026 Jenkins still had AWS v3 and v4 next to v5 and v6), so retire every version that
no live branch asks for, not only the one this rollout replaced.

This phase changes Jenkins and deletes cloud resources: show the list of what goes and wait for
the user's go before step 3. It changes no code in the repository, except a job XML pin that
step 1 finds.

1. Prove that no live branch asks for the old version. The live branches are master and the
   branch behind each `backport/<name>` label
   (`gh label list -R scylladb/scylla-cluster-tests --search backport/`). Skip `backport/none` and
   the `-done` labels. `backport/<year>.<n>` is `branch-<year>.<n>`, and the `manager-*` and
   `perf-*` labels are branches of the same name; skip a label whose branch
   `git ls-remote --heads upstream <branch>` doesn't list.
   For each one, with `<old>` the AWS version (`v5`), GCE (`v10`) and OCI (`v3`) in turn:

   ```bash
   git fetch upstream
   git grep -nE -- '-<old>(-fibs)?-(asg|CI)|-template-<old>"|-<old>["'\'']' upstream/<branch> -- vars jenkins-pipelines Jenkinsfile
   ```

   A hit in a jenkinsfile or in `vars/` means that branch's backport has not merged: stop. A hit
   in a job `.xml` is a pin no rollout moved (in November 2026 `devops-cloud-resources-report.xml`
   still asked for AWS v3): move it to the current version, in the repository and in the job's
   live config, before you go on. A daily job rarely shows in the queue check of step 2. The pins
   live outside `vars/`, so `jenkins-pipelines` and `Jenkinsfile` are part of the search. Also run
   `uv run sct.py check-jenkins-builder-labels --groovy-file <file>` on each branch's copy of
   `vars/getJenkinsLabels.groovy` after the removal (step 6).
2. Prove that Jenkins has no running build and no queued item on the old labels: in the Jenkins
   script console, list `Jenkins.instance.computers` whose `node.labelString` has the old version,
   and `Jenkins.instance.queue.items` whose `assignedLabel` has it. Both must be empty. A builder
   still busy runs to the end of its build; wait for it.
3. Remove the old clouds from Jenkins. Print the matches first, then remove them and save:

   ```groovy
   def old = ~/^(aws-.+-qa-builder-v5(-fibs)?-\d+(-CI(-FIPS)?)?|gce-.+-qa-builder-v10|oci-compute-oci-.+-qa-builder-v3-\d+)$/
   def gone = Jenkins.instance.clouds.findAll { it.name ==~ old }
   println gone*.name                       // dry run: check this list with the user
   // Jenkins.instance.clouds.removeAll(gone); Jenkins.instance.save()
   ```

   The OCI credentials `oci-sct-user-<region>` are shared by every version: keep them.
4. AWS, in each region of `AWS_SUPPORTED_REGIONS`:
   - Auto scaling groups `aws-<region>-qa-builder-<old>-1`, plus `aws-eu-west-1-qa-builder-<old>-1-CI`
     and `aws-us-east-1-qa-builder-<old>-fibs-1-CI-FIPS`. Check that each group has no instance
     (`aws autoscaling describe-auto-scaling-groups`), then
     `aws autoscaling delete-auto-scaling-group --auto-scaling-group-name <name> --region <region>`.
   - Launch templates `aws-sct-builders-<old>` and, in `us-east-1`, `aws-sct-builders-<old>-fibs`:
     `aws ec2 delete-launch-template --launch-template-name <name> --region <region>`.
   - Keep `SCT-builder-sg`: every builder version uses it.
5. GCE, in each project of `SUPPORTED_PROJECTS` and each region of `SUPPORTED_REGIONS`:
   `gcloud compute instance-templates delete gce-<project>-<region>-qa-builder-<old> --project <project id> --quiet`.
   The firewall rules and the `sct-builder` network tag are shared by every version: keep them.
6. OCI has nothing left in the cloud: a version is only its Jenkins cloud. The builder subnet and
   its security list are shared: keep them.
7. Keep the runner images. A branch's `SctRunner.VERSION` still creates its runners from its image
   after its builders moved. Delete a runner image only when no live branch has that
   `SctRunner.VERSION` and no builder template, launch template or instance configuration points
   at it, in a separate change with the user's go.
8. Check that Jenkins still serves every label each live branch asks for: run
   `check-jenkins-builder-labels` on each branch's groovy file, and run the queued-build check of
   step 2 again an hour later.

**Exit:** no Jenkins cloud, auto scaling group, launch template or instance template of the old
versions is left, and `check-jenkins-builder-labels` passes for every live branch.
