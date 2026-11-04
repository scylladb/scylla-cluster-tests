# Preflight check of loader and monitor hosts

**Jira:** SCT-1215

## Problem

A loader or monitor host that cannot run the test fails late, in the middle
of the setup or of the test, with an error that does not name the cause:

- Docker keeps its images on the root filesystem. When that filesystem is
  full, an image pull fails. When the Grafana pull fails, Grafana does not
  start, and the test waits 40 minutes on the dashboard upload.
- The monitoring stack runs on the host network and needs fixed ports. A
  host service that holds one of them stops the stack. On Fedora, the
  Cockpit web console holds 9090, the Prometheus port.
- SCT does not check which Docker the host runs. An old Docker or the
  Podman `docker` shim fails later, on the first container.
- With `use_mgmt: true`, the monitor host gets Scylla Manager, a running
  Scylla, and their ports and timers. On a shared host, a second run
  rewrites the Manager config of the first run.

## Who it affects

Baremetal runs on lab hosts, such as the SCT-1215 longevity job on
`monster`. On cloud VMs that the run created, the same failures are rare.

## Evidence

[sct1215-baremetal-shared-host-test #4](https://jenkins.scylladb.com/job/scylla-staging/job/fruch/job/performance_staging/job/sct1215-baremetal-shared-host-test/4/),
`docker pull` of the ycsb image on the 15 GB root disk of `monster`:

```
E               failed to copy: failed to send write: write /var/lib/containerd/io.containerd.content.v1.content/ingest/6f2156e0be439cf7e25d1eaf837941ce68147efd7a481c2f26dc121ff033b32b/data: no space left on device
```

The SCT-1215 Jira comment of 2026-10-13, builds #2 and #3:

> Port 9090 (Prometheus) is taken by Cockpit, Fedora's web admin console.

> Grafana did not start: the Grafana image pull failed with
> `write /var/lib/containerd/...`, because monster's root disk (15 GB, where
> Docker stores images) was full. The test then waited on dashboard upload
> for 40 min.

## Definition of done

- Before the first image pull, setup checks each loader and monitor host.
  A host that fails a check stops the setup with a message that names what
  is wrong and what the host needs.
- `moby-engine` on Fedora passes. A Docker older than 20.10, or the Podman
  shim, fails before a container starts. SCT never upgrades Docker.
- A host without enough free disk space for the Docker images fails before
  the first pull. The message names the path, the free space, and the
  minimum. Setup first removes the images that no container uses, once.
  It never removes an image that a container uses.
- A monitor host with a monitoring port in use fails before the stack
  starts. The message names the port and the process.
- A host with more than one role is checked one time, against the sum of
  the disk minimums of its roles.
- On a baremetal host, a missing Docker, or one that the SSH user cannot
  use, fails here and names the fix.
- A baremetal configuration with `use_mgmt: true` fails the validation.
- Each disk minimum is a configuration option, listed in
  `docs/configuration_options.md`.
- The baremetal docs page lists the host requirements that the check
  enforces.

## Out of scope

- Moving the monitoring stack to free ports when a default port is in use.
- A budget that limits the image cache on lab hosts.
- Installing or upgrading Docker on a baremetal host.
- Scylla Manager on a shared monitor host.
