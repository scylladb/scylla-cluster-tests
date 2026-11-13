# Runner Image Rollout

Seven phases, from the code change to test jobs on the new builders. Run every command from the
repository root.

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
2. Change every builder label in `vars/getJenkinsLabels.groovy` to the new versions. Labels the
   builders create:
   - AWS: `aws-sct-builders-<region>-<version>-asg`, FIPS `aws-sct-builders-us-east-1-<version>-CI-FIPS`
   - GCE: `gcp-<project>-builders-<region>-template-<version>` (the groovy builds `gcp-<project>` from `gce_project`)
   - OCI: `oci-sct-builders-<region>-<version>`
3. Cross-check: compute the labels from the builder classes and their region lists
   (`AWS_SUPPORTED_REGIONS`, `SUPPORTED_PROJECTS` x `SUPPORTED_REGIONS`, `get_all_regions("oci")`)
   and confirm each label in the groovy file is one of them. `lint-pipelines` does not parse the
   groovy files, so nothing else catches a typo.
4. Commit the builder versions and the labels together.
5. Phase 6 must finish on every cloud before this commit merges. Master jobs ask for the new labels
   as soon as it merges, and a label without a cloud or template makes them wait forever.

**Exit:** one commit, cross-check finds no label the builders don't create.

## Phase 6: Configure the Jenkins builders

**Entry:** Phase 5 is pushed and the user approved changing Jenkins.

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
   runs longer than the others. It loops over `get_all_regions("oci")`; a failed region is logged
   and skipped, not fatal, so read the whole log and rerun with `-r <region>` for any that failed.
4. Leave the old clouds and templates in place.
5. Check Jenkins, not the command's exit code. OCI and GCE builders are on-demand clouds, so
   `get_all_nodes` lists no node for a new label until a job runs on it. List the clouds and
   their template labels in the Jenkins script console:

   ```groovy
   Jenkins.get().clouds.each { c ->
     def labels = c.metaClass.respondsTo(c, "getTemplates") ? c.templates*.labelString : []
     println("${c.name}: ${labels}")
   }
   ```

   Every label of `vars/getJenkinsLabels.groovy` must appear: the OCI ones as clouds
   `oci-compute-oci-<region>-qa-builder-<version>-1` with label `oci-sct-builders-<region>-<version>`,
   one per OCI region. AWS groups appear as EC2 fleet clouds.

**Exit:** the new clouds and templates exist in Jenkins next to the old ones, and every label in
the groovy file has one.

## Phase 7: Validate on Jenkins and backport

**Entry:** Phase 6 is done.

1. Run one existing test job per backend from the PR branch, so it picks up the new labels. Reuse
   existing jobs, pass the branch as a parameter, and confirm the build ran on a new builder (its
   node name carries the new builder version).
2. Add the backport labels for every live branch: the branches pick up the labels, and therefore
   the new builders, only when their backport merges.
3. After every branch moved, the old builders can be removed in a separate change.

**Exit:** a job passed on each new label, backport labels are set.
