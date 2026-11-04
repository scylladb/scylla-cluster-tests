# Monitor setup changes only what a shared host lacks

**Jira:** SCT-1215

## Problem

The baremetal backend sets up a loader or monitor host as if SCT owns it.
Lab hosts are not like that. They already run Docker, system services, and
other Jenkins jobs, and SCT does not create or destroy them.

The monitor setup has these problems on such a host:

- It always runs the `get.docker.com` script. On a host with Fedora's
  `moby-engine`, the script fails three times and stops the run, although
  Docker already works.
- It always restarts Docker. On a shared host, the restart stops the
  containers of other jobs.
- It needs `wget` to download scylla-monitoring.
- It installs `scylla-monitoring` in `~/sct-monitoring` and deletes that
  directory first. A second monitor on the same host deletes the install and
  the data of the first one.
- When the stack does not start, it runs `kill-all.sh`. That script removes
  every container with the default names of the stack, also the stack of
  another run.
- On RHEL-like hosts, it turns off the firewall, which flushes the iptables
  rules of Docker, and it upgrades the SSH packages.
- The cleanup restores the package and service baseline on every baremetal
  host. On a shared loader or monitor host, the restore removes the packages
  that other jobs installed since the baseline. With two runs on one host,
  the first run to finish restores the baseline under the other run.

The workaround is manual host prep: a `~/PREPARED-MONITOR` marker file and
`wget` installed by hand. Each new lab host needs the same prep.

## Who it affects

Baremetal runs whose loader and monitor are on lab hosts, such as the
SCT-1215 longevity job on `monster` (Fedora 43). Lab builders as baremetal
test nodes (#16342) have the same hosts.

## Evidence

[sct1215-baremetal-shared-host-test #2](https://jenkins.scylladb.com/job/scylla-staging/job/fruch/job/performance_staging/job/sct1215-baremetal-shared-host-test/2/),
monitor setup on `monster`, which runs `moby-engine`:

```
< t:2026-10-13 10:00:26,002 f:file_logger.py  l:108  c:sdcm.sct_events.file_logger p:ERROR > Docker installation failed, retrying in 10 seconds...
< t:2026-10-13 10:00:26,002 f:file_logger.py  l:108  c:sdcm.sct_events.file_logger p:ERROR > Docker installation failed, retrying in 10 seconds...
< t:2026-10-13 10:00:26,002 f:file_logger.py  l:108  c:sdcm.sct_events.file_logger p:ERROR > Docker installation failed after 3 attempts.
```

The manual prep on `monster` before the next run, from the SCT-1215 Jira
comment of 2026-10-13:

| Change | Command |
| --- | --- |
| Marker so SCT skips its Docker install | `touch /jenkins/PREPARED-MONITOR` |
| Installed wget | `dnf install -y wget` |

## Definition of done

- On a host with every prerequisite present, setup installs no package and
  runs no pip or Docker install command.
- On a host that lacks only `unzip`, setup installs only `unzip`.
- The monitor setup does not use `wget`.
- On a baremetal host, setup does not install Docker, change the `docker`
  group, restart Docker, change the firewall, or upgrade the SSH packages.
  Setup never runs `kill-all.sh` there.
- On a baremetal host where Docker is missing, or the SSH user cannot use
  it, setup fails and tells what the host needs.
- On a cloud VM that the run created, setup installs a missing Docker as
  today.
- Two runs' monitors on one baremetal host use different directories.
- After the cleanup, no container and no directory of the monitoring stack
  of the run stays on a baremetal monitor host. The stack of another run
  stays.
- The cleanup of a baremetal loader or monitor host removes and reinstalls
  no package. The cleanup of a DB host restores its baseline as today.
- Setup of a baremetal loader or monitor host leaves the apt-daily timers
  as it found them.
- The SCT-1215 job passes on `monster` without `~/PREPARED-MONITOR` and
  without `wget`.
- The baremetal docs page lists what SCT installs and changes on a loader
  and monitor host, and what the cleanup removes.

## Out of scope

- The checks that fail a host before the first image pull (Docker version,
  disk space, ports). See `docs/plans/baremetal/host-preflight/`.
- Other changes of the common node setup on shared hosts: vector installed
  as a host service, the `sshd_config` edits, and the coredump limits. They
  stay after the run, and the docs page lists them.
- Scylla Manager on a shared monitor host.
- The removal of `~/PREPARED-MONITOR` support. Setup still honours the
  marker.
