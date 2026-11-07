# Runner Rollout Troubleshooting

Failures seen in real rollouts, how each showed up, and what fixed it.

## Runner never answers SSH, the boot hangs

**Symptom:** `create-runner-instance` ends with `Unable to SSH to <ip>! Exiting...` after
`ConnectTimeout` errors (a timeout, not a refused connection).

**Diagnose:** read the serial console before you terminate the instance.
- OCI: `capture_console_history` then `get_console_history_content` on the compute client.
- GCE: `get_serial_port_output` on the instances client.
- AWS: `get_console_output`.

A console that ends with `Job <unit>/start running (... / no limit)` repeating is a systemd job
that never finishes.

**Seen (CVE-2026-98432, runner 1.20):** `cloud-init-network.service` ran for over an hour on OCI.
The image had a new unit with `Before=network-pre.target` and default dependencies. Default
dependencies order a unit after `sysinit.target`. cloud-init's network stage runs before
`sysinit.target` and, on networkd systems, runs `systemctl start systemd-networkd-wait-online`.
networkd waits for `network-pre.target`, which waited for the unit, which waited for
`sysinit.target`: a deadlock. GCE, AWS and Azure booted because cloud-init-local leaves
`.skip-network` there and the network stage skips the wait.

**Fix:** a unit that runs before the network sets `DefaultDependencies=no` with
`Before=shutdown.target` and `Conflicts=shutdown.target`, like Debian's `nftables.service`. Check
in a container with systemd as PID 1:
`systemctl show -p After <unit>` must not list `sysinit.target` or `basic.target`.

## The build did nothing

**Symptom:** `create-runner-image` ends quickly with `No need to copy SCT Runner image since it
already exists` and no `All packages successfully installed.`

**Cause:** an image of the version already exists, often from an unmerged branch that bumped to
the same version. The build checks only the name and version.

**Fix:** Phase 2 of [rollout.md](../workflows/rollout.md): find the image, confirm nothing uses it,
delete it with the user's go, rebuild.

## GCE image step fails with "disk is already being used"

**Symptom:** `400 POST .../global/images: The disk resource '.../disks/sct-runner-<v>-builder-us-east1'
is already being used by '.../instances/sct-runner-<v>-builder-us-east1'`.

**Cause:** the image insert started before the builder instance finished stopping. Older code
didn't wait for the stop operation.

**Fix:** make sure `GceSctRunner._stop_image_builder_instance()` waits for the operation
(`wait_for_extended_operation`). Delete the leftover builder instance, then rerun the build.

## AWS configure fails with "'NoneType' object has no attribute 'subnet_id'"

**Symptom:** `configure-jenkins-builders -c aws` stops in one region at `create_auto_scaling_group`.
The regions before it are done.

**Cause:** AWS added a zone to the region (eu-west-2d) and SCT has no subnet there, so
`sct_subnet()` returned None for it.

**Fix:** make sure `create_auto_scaling_group()` uses only the zones that have an SCT subnet, then
rerun the command. It updates the groups that already exist and goes on from the failed region.

## A new builder label waits with "All nodes of label ... are offline"

**Symptom:** the first job on a new label waits a few minutes.

**Cause:** a new auto scaling group starts at zero instances, and the EC2 fleet plugin starts one
on demand. This is normal for the first job; a wait longer than about 10 minutes is not.

**Fix:** none for a short wait. For a long one, check the group's activity in the EC2 console for
launch errors (subnet, instance type capacity, launch template).

## `ssh-cmd` reports values from your own machine

**Symptom:** the runner "has" your laptop's kernel or Java version.

**Cause:** `sct.py ssh-cmd` runs `ssh ... <command>` through a local shell, so `$(...)` and `$VAR`
expand locally first.

**Fix:** wrap the command in single quotes inside the argument: `-P "'echo \$(uname -r)'"`, or put
the checks in a script and run that.

## `ssh-cmd` or `clean-runner-instances --test-id` finds nothing

**Symptom:** no runner found for a runner you just created.

**Cause:** `ssh-cmd` doesn't search OCI, and the test id lookup doesn't match every cloud's tags.

**Fix:** use the IP: plain `ssh -i ~/.ssh/scylla_test_id_ed25519 ubuntu@<ip>` and
`clean-runner-instances --runner-ip <ip> --force -b <cloud>`.

## A groovy label points at builders that are never created

**Symptom:** a job waits forever for a node with its label.

**Cause:** the label isn't one `configure-jenkins-builders` creates: a typo, or a GCE project the
groovy maps to that isn't in `SUPPORTED_PROJECTS` (`gce_project: gcp` maps to
`gcp-skilled-adapter-452`).

**Fix:** run the label cross-check of Phase 5 and fix the label, or configure that project too.
