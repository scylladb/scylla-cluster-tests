---
status: draft
domain: cluster
created: 2026-10-13
last_updated: 2026-10-28
owner: fruch
---

# Baremetal Setup on Shared, Pre-configured Hosts

Each phase names its POC PR, and the manual tests list the SCT-1215 runs. The same information, with the merge
order, is on the overview page [Baremetal Shared Hosts](https://claude.ai/artifact/14QF6zQCQJRDJfrocfaexs)
(ScyllaDB organization only).

## 1. Problem Statement

The baremetal backend assumes the hosts are fresh machines SCT can configure however it likes.
Lab hosts are not like that: they already run Docker, system services and other Jenkins jobs, and
SCT does not create or destroy them.

SCT-1215 ran the loader and monitor of a longevity test on the lab host `monster` (Fedora 43),
with one DB node in AWS. Every failure came from the monitor and loader setup, not from the test:

- **Docker install conflicts with the host's Docker.** The monitor setup always runs the
  `get.docker.com` script. `monster` runs Fedora's `moby-engine`; the script failed 3 times and
  stopped the run, although Docker already worked.
- **A missing tool stops the run.** `wget` was not installed and the monitoring download needs it.
- **Fixed ports clash with the host's services.** Fedora's Cockpit web console listens on 9090,
  which Prometheus needs. Log collection then reached Cockpit's TLS endpoint instead of Prometheus.
- **node_exporter is installed over an existing one.** Fedora's `prometheus-node-exporter`
  already served 9100. SCT installed its own copy for both the loader and the monitor; both
  failed to bind and stayed behind as failed units.
- **The root disk filled up mid-pull.** Docker keeps images on the 15 GB root filesystem. The
  Grafana pull failed with a write error, Grafana never started, and the test waited 40 minutes on
  dashboard upload before it was aborted.
- **The setup restarts Docker.** On a shared host this kills other jobs' containers. It did not
  happen in SCT-1215 only because the Docker install failed first.
- **Connectivity is all or nothing.** `ip_ssh_connections` is global, so the one DB node in AWS,
  reached over its public address, forced reverse SSH tunnels on the lab hosts next to the runner
  too. The loader and the monitor shared one host, and the second node killed the tunnel the first
  had just opened. SSH to the lab host needed an SCT key added by hand.

The workaround was manual host prep: a `~/PREPARED-MONITOR` marker file, installing `wget`,
disabling Cockpit, removing images by hand. Each new lab host needs the same prep, and nobody
knows the list.

## 2. Current State

**Monitor setup** (`sdcm/cluster.py:BaseMonitorSet`):

- `install_scylla_monitoring_prereqs` has three paths:
  - With `~/PREPARED-MONITOR` present it skips everything.
  - On the formal monitor image (`is_formal_monitor_image`) it skips the package and Docker
    install, but still fixes the home directory's ownership, unpacks the bundled monitoring
    archive, adds the user to the `docker` group, restarts Docker and logs in to Docker Hub.
  - Otherwise it installs the packages, runs `get.docker.com` with retries (plus a Rocky Linux
    fallback), pip-installs `pyyaml` and `psutil` with `--break-system-packages` when needed, adds
    the user to the `docker` group and always runs `systemctl restart docker`.
- On RHEL-like monitors, setup also turns off the firewall (`disable_firewall`, which flushes
  iptables, Docker's rules included) and upgrades the SSH packages (`upgrade_ssh_packages`).
- With `use_mgmt` (on by default) the monitor host gets Scylla Manager and a running Scylla for
  its backend.
- `download_scylla_monitoring` fetches the scylla-monitoring archive with `wget` into
  `$HOME/sct-monitoring`, deleting that directory first; the monitoring data lives under it too.
  Two monitors on one host therefore share it.
- When the stack fails to start, setup runs a bare `kill-all.sh`, which removes every container
  with the stack's default names, whoever started them, and tries again.
- `start_scylla_monitoring` calls `start-all.sh` without port options, so the stack takes its
  default ports. `start-all.sh` already accepts `-g` (Grafana), `-p` (Prometheus), `-m`
  (Alertmanager), `--loki-port` and `--promtail-port`.
- `__init__` sets `grafana_port = 3000` and `prometheus_port = 9090` as constants.

**node_exporter:**

- `sdcm/node_exporter_setup.py:NodeExporterSetup.install` always downloads node_exporter 1.8.2 and
  writes its own `node_exporter.service`.
- It is called from `BaseLoaderSet.node_setup` and from `install_scylla_monitoring_prereqs`.
- `BaseMonitorSet.configure_scylla_monitoring` and `reconfigure_scylla_monitoring` build the scrape
  targets as `<address>:9100`.

**Baremetal hosts:**

- `sdcm/cluster_baremetal.py:PhysicalMachineNode` and `PhysicalHostCleanup` handle hosts that
  outlive the run.
- `sdcm/utils/resources_cleanup.py:clean_resources_baremetal` cleans up afterwards: it frees SSH
  tunnel ports and undoes the Scylla disk setup.
- Nothing checks or limits Docker's disk use, and nothing removes images a run pulled.

**Related plans:**

- *Lab builders as baremetal test nodes* (PR #16342, pending) reserves lab hosts through Jenkins
  and adds a pre-run host check for SSH key, sudo and Docker. This plan is the other half: making
  SCT's own setup safe on those hosts.
- *Docker Cleanup for All Backends* (`docker-cleanup-all-backends.md`) cleans the runner's
  containers. It does not limit image disk use on test hosts.
- *Baremetal jobs on the local agent* (PR #16445) runs the test on the lab host's Jenkins agent,
  next to the lab hosts. It also carries the SCT-1215 job used for the manual tests below.
- *SCT-1275* reaches lab hosts through the lab access service instead of direct public SSH. It blocks SSH
  port forwarding, so the runner reaches lab ports through nested SSH over its session or
  through TCP resources of that service. Phase 4 reaches lab hosts through it for SSH; reaching
  the monitor's ports from outside the lab is a follow-up.

## 3. Goals

1. A Fedora or Ubuntu host that already runs Docker becomes a working loader and monitor without
   SCT-specific prep: no marker file, no extra packages.
2. Setup never restarts, installs or upgrades Docker on a baremetal host, never stops a container
   or deletes files it did not create, and leaves the host's firewall and packages alone beyond
   the missing prerequisites.
3. A host that can't run the test (old Docker, short disk, a taken monitoring port) fails before
   any image pull or container start, saying what is wrong and what is needed.
4. An existing node_exporter on a host is scraped instead of replaced.
5. Cloud backends behave as today: default ports, same install path.
6. A run can mix hosts next to the runner with hosts it reaches over public addresses: only the
   latter get reverse SSH tunnels, and lab hosts are reached without an SCT key on them.

## 4. Implementation Phases

Phases 1, 3 and 4 are independent. Phase 2 builds on phase 1. Phase 5 comes last. Phase 4's
lab-access part needs the SCT-1275 lookup.

Phases 1 to 3 apply to hosts SCT does not own: baremetal hosts. On VMs the run created, setup keeps
doing what it does today, except that it installs only the missing prerequisites (phase 1).

```mermaid
graph LR
    P1["1 Install only what is missing"] --> P2["2 Preflight check"]
    P3["3 Reuse existing node_exporter"]
    P4["4 Connectivity per role, lab hosts by name"]
    SCT1275["SCT-1275 lab access lookup"] -.-> P4
    P2 & P3 & P4 --> P5["5 Docs"]
```

### Phase 1: Install only what is missing, change nothing else

**Importance**: Critical

**POC**: #16535

**Description**:

- The monitor prerequisites check for each tool and install only the missing ones. The tools are
  `curl`, `unzip`, `python3`, and the `yaml` and `psutil` Python modules that scylla-monitoring's
  scripts import.
- Docker is a host requirement on baremetal. Setup never installs it there, never adds the user
  to the `docker` group and never restarts it; a missing Docker, or one the SSH user can't use,
  fails the preflight check (phase 2).
- On VMs the run created, setup installs Docker when it is missing and adds the user to the
  `docker` group, restarting Docker only when it installed it. The formal monitor image path stays
  as it is today, restart included.
- The scylla-monitoring download uses curl with retries, like the node_exporter download already
  does, so `wget` is no longer needed.
- On baremetal, each run gets its own monitoring install and data directory, so a second monitor
  on the host can't delete the first one's. A stack that fails to start fails the setup; the
  restart path that runs a bare `kill-all.sh` is not used there.
- On baremetal, setup does not turn off the firewall or upgrade the SSH packages. Letting the
  monitor scrape the hosts is a host requirement (phase 5).
- `~/PREPARED-MONITOR` is no longer needed. It stays honoured so existing images and hosts behave
  the same; removing it is a follow-up once no image relies on it.

**Definition of Done**:

- [ ] On a host with everything present, setup runs no package manager, pip or Docker commands.
- [ ] On a host missing only `unzip`, setup installs only `unzip`.
- [ ] `wget` is not used anywhere in the monitor setup.
- [ ] On baremetal, setup runs no Docker install, group change, Docker restart, firewall change or
      SSH package upgrade, and never runs `kill-all.sh`.
- [ ] Two runs' monitors on one baremetal host use different directories.
- [ ] Unit tests cover the "all present", "one missing" and "Docker unusable by the user" cases,
      on baremetal and on a VM the run created.

**Dependencies**: none.

### Phase 2: Preflight check of loader and monitor hosts

**Importance**: Critical

**POC**: #16538

**Description**:

- Before the first image pull, setup checks each loader and monitor host and fails right away,
  naming what is wrong and what is needed, instead of failing mid-test. The check runs once per
  host: a host with several roles is checked once, against all of them.
  - **Docker:** the running server is Docker Engine 20.10 or newer, of any packaging
    (`docker-ce`, `moby-engine`, distro `docker.io`), and not the Podman `docker` shim. 20.10 is
    the oldest line the supported distros still ship. SCT never upgrades Docker: on a shared host
    that restarts the daemon under other jobs. On baremetal, Docker missing, or not usable by the
    SSH user, fails here too, naming the fix (install Docker, or add the user to `docker`).
  - **Disk:** the free space where Docker keeps images is at least a minimum. With the
    containerd image store that is `/var/lib/containerd`, not `DockerRootDir`. When space is
    short, setup removes the images no container uses, once per host, and checks again; that
    keeps the image cache in bounds without a separate cleanup step. The minimums are per role,
    and a host with several roles needs their sum (13 GB for a loader and monitor). They are
    config options:
    - monitor hosts: 10 GB. The monitoring stack images measured on `monster` take about 5 GB,
      and unpacked about twice that;
    - loader hosts: 3 GB. The stress tool images take about 1.5 GB. Loaders on AWS and OCI have a
      20 GB root disk, so the monitor's minimum would fail healthy loaders.
  - **Ports, monitor hosts:** the monitoring stack's ports are free. The stack runs on the host
    network, so every port it opens binds on the host: Grafana 3000, Prometheus 9090,
    Alertmanager 9093 and its gossip port 9094, Loki 3100 and its gRPC port 9095, Promtail 9080
    and its syslog port 1514, and the Grafana renderer 8081. A taken port fails setup naming the
    port and the process holding it. Lab host owners keep those ports free; Fedora's Cockpit,
    which holds 9090, is disabled on lab hosts by agreement.
- **Manager, baremetal:** config validation rejects `use_mgmt: true` with the baremetal backend.
  Manager brings a running Scylla, its ports and its timers onto the monitor host, and a second
  run would rewrite the first one's Manager config. Manager on shared hosts is a follow-up.
- Backends: the check runs wherever SCT sets up a loader or monitor host itself, cloud VMs
  included, where it is cheap and should never fire. It is skipped on the docker and
  k8s-local-kind backends, whose monitoring stack runs on the runner, and for loaders on
  Kubernetes, which are pods.

**Definition of Done**:

- [ ] `moby-engine` on Fedora passes; a too-old Docker or the Podman shim fails before any
      container starts.
- [ ] A host short on Docker disk space fails before any pull, with a message naming the path
      and both sizes; removing unused images never removes an image a container uses.
- [ ] A monitor host with 9090 taken fails before the stack starts, naming the port and process.
- [ ] A host that is both loader and monitor is checked once, against the summed minimum, and
      pruned at most once.
- [ ] Missing Docker on baremetal fails here, naming the fix.
- [ ] A baremetal config with `use_mgmt: true` fails validation.

**Dependencies**: Phase 1.

### Phase 3: Reuse an existing node_exporter

**Importance**: Important

**POC**: #16539

**Description**:

- Before installing, setup checks port 9100 for a node_exporter (a `/metrics` response that
  contains `node_exporter_build_info`). The probe runs from the monitor host against the address
  Prometheus will scrape, so an exporter bound to loopback or behind the host's firewall doesn't
  pass. If one answers, SCT skips the install and scrapes it.
  - The distro build may have a different service name and collector set. Dashboards read the
    same metric names, so the extra or missing collectors are acceptable.
- If something else holds 9100, SCT installs its node_exporter on a free port. The scrape targets
  for that host then use that port instead of `:9100`.
- If the loader and monitor share a host, the second node finds the first one's node_exporter
  and reuses it.

**Definition of Done**:

- [ ] A host with a distro node_exporter on 9100 gets no SCT node_exporter and is scraped.
- [ ] A host with 9100 taken by another service gets SCT's node_exporter on another port, and
      Prometheus scrapes it there.
- [ ] No failed `node_exporter.service` is left on a host after the run.
- [ ] An exporter the monitor can't reach at the scrape address is not reused.

**Dependencies**: none.

### Phase 4: Connectivity per role, and lab hosts by name

**Importance**: Important

**POC**: #16573 (per role), #16574 (by name)

**Description**:

- Each role in the baremetal hosts config (`db_nodes`, `loader_nodes`, `monitor_nodes`) can set
  its own `ip_ssh_connections`, falling back to the run's. Lab hosts next to the runner are then
  `private` (no tunnels, logs straight to the runner) while a cloud DB node stays `public`. Today
  the value is read run-wide, so each reader takes one of these:
  - the node's own: its SSH address, reverse tunnels and the stale-port release, the log
    destination it ships to, and its saslauthd config. Readers with no node in hand today take
    the value as an argument;
  - the DB role's: the LDAP address written to scylla.yaml, and the CQL driver's fallback
    address;
  - each role's own, read from the hosts config: log collection, which lists the hosts per role.
  - Every other reader keeps the run-wide value.
- Hosts shared by several nodes free stale reverse-tunnel ports once per run, so the second node
  does not kill the tunnel the first one just opened.
- A node can name its host in the lab access service (`access`). SCT then opens SSH where the
  internal lab access module says, and the host needs no SCT key. `public_ip` becomes optional for
  lab hosts, defaulting to `private_ip`.
- **Needs Investigation:** the lab access service lets clients in with SSH's `none` method only,
  so the lookup has to return no key for those hosts (as the xcloud lookup already does). Confirm
  that with the owners of SCT's SSH client.

**Definition of Done**:

- [ ] A run with a `public` DB node and `private` lab hosts opens tunnels for the DB node only.
- [ ] A loader and a monitor sharing a host both ship logs for the whole run.
- [ ] A lab host with `access` set is reached through the lab access service, with no SCT key on it.

**Dependencies**: none; the `access` part needs the SCT-1275 lookup.

### Phase 5: Baremetal host documentation

**Importance**: Important

**Description**:

- A new doc page for baremetal hosts. It covers:
  - what a host must provide: SSH access, sudo, Docker 20.10 or newer usable by the SSH user,
    free disk for Docker's images, free monitoring ports on monitor hosts (the phase 2 checks),
    and a firewall that lets the monitor scrape the loaders' and its own node_exporter;
  - one SCT monitor per host, and `use_mgmt: false`;
  - what SCT installs or changes on it, and what cleanup undoes;
  - the hosts config: per-role `ip_ssh_connections` and `access`.
- Regenerate `docs/configuration_options.md` for the new options.

**Definition of Done**:

- [ ] The page lists every host requirement and every host change SCT makes, matching phases 1
      to 4.
- [ ] `docs/configuration_options.md` includes the new options.

**POC**: #16554

**Dependencies**: Phases 1 to 4.

### Follow-ups, not in this plan

- **Monitor access from outside the lab.** When SCT runs off the lab network, it cannot reach the
  monitor's ports. Reaching them through the lab access service waits on SCT-1275's choice between
  nested SSH forwards and TCP resources. Not needed while SCT runs on the lab host's agent.
  POC: #16555 (one address lookup for the monitoring stack).
- **Moving monitoring ports instead of failing.** If keeping the ports free proves too strict for
  some hosts, the stack could start on free ports. That means leaving the host network
  (`start-all.sh` refuses `-l` with port options), renamed containers, and every port user,
  log collection included, reading the real ports. POC from SCT-1215: #16547 and #16548. Start
  from:
  - the callers that pass only the monitor's host and rely on Prometheus' default port: the
    tester's Prometheus stats, the SLA and network nemeses, the scan operation thread, the
    latency helpers, the SLA-per-user test, and log collection's monitoring snapshot. A grep for
    literal ports doesn't find them;
  - `stop_scylla_monitoring`'s bare `kill-all.sh`, which would stop another stack's default-named
    containers;
  - scylla-monitoring's `--stack` doesn't help: on the host network it still binds the default
    ports, and off it, stack 2's Alertmanager maps 9042, Scylla's CQL port.
- **Manager on shared hosts.** Running Manager, and the Scylla it needs, on a monitor host other
  runs share: per-run ports and config, or a Manager host of its own.

## 5. Testing Requirements

**Unit tests** (`unit_tests/unit/`), per phase:

- Phase 1: the prerequisite check with fake remoter output, for "all present", "partly missing"
  and "Docker present but unusable", on baremetal and on a VM; the per-run monitoring directory.
- Phase 2: Docker version parsing (`docker-ce`, `moby-engine`, the Podman shim, missing Docker);
  free-space parsing and storage-location detection, with and without the prune freeing enough,
  and the summed minimum for a shared host; a taken monitoring port reported with its process;
  `use_mgmt` rejected on baremetal.
- Phase 3: node_exporter detection for distro exporter, other service, free port, and an
  exporter the monitor can't reach.
- Phase 4: per-role settings over the run's, tunnels only for `public` nodes, tunnel ports freed
  once per shared host, and the SSH address taken from the lab access lookup.

**Manual tests:**

SCT-1215 ran the staging job on `monster`, with the DB node in AWS:

- #4: first run to reach the stress commands; per-role connectivity and the node_exporter reuse
  worked. It failed pulling a stress image: the 15 GB root disk was full.
- #6: first passing run, after the root disk was grown, log collection included.
- #7: passed with Cockpit on, using the moved-ports POC (now a follow-up).
- #8: passed from one branch merging all the POC PRs, on the fork's scylla-qa-internal branch.
  `~/PREPARED-MONITOR` was still on the host, so phase 1's install path was not exercised.

Still to run:

- Repeat the SCT-1215 staging job on `monster` after removing the manual prep: delete
  `~/PREPARED-MONITOR` and leave the distro node_exporter running. The run must reach the stress
  commands and finish with logs collected.
- The same job with Cockpit turned on: it must fail in preflight, naming port 9090.
- A provision test on AWS (`provision-aws` label) to confirm cloud behaviour is unchanged.

## 6. Success Criteria

- All phase DoD items are met.
- The SCT-1215 job passes on `monster` with no SCT-specific host prep. The host still has to meet
  the requirements phase 5 documents (SSH access, sudo, Docker 20.10 or newer, free disk for
  Docker's images, free monitoring ports).
- The hosts list no failed SCT units after the run.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Skipping the Docker restart leaves a cloud VM where the user's new `docker` group is not active | Medium | Setup fails on first `docker` call | The SSH session is already reconnected after the group change; keep the restart on cloud VMs whenever setup installed Docker, and on the formal monitor image |
| Two runs' monitors land on one host | Low | The second run fails in preflight on the taken ports | Intended: the docs say one SCT monitor per host, and per-run directories keep the first run's data safe |
| A lab host turns Cockpit (or another service) back on | Medium | Runs on that host fail in preflight | The failure names the port and process, so the host owner can act; moving ports instead is a listed follow-up |
| The distro node_exporter lacks a collector a dashboard panel uses | Low | Empty panel | Acceptable on lab hosts; list the known gaps in the phase 5 doc |
| Pruning unused images removes other jobs' cached images. On a host that runs short of space, jobs keep pruning each other's caches, not just in a race | Medium | Other jobs re-pull their images | Accepted: only unused images are removed, pulls are idempotent, and the prune runs only when space is short. SCT can't tell its own pulls from another job's, so it doesn't try |
| The minimum Docker version or a free-space minimum is too strict | Medium | Runs fail on hosts that would work | Minimums come from measured needs; the failure names both values, and each minimum is a config option |

**Rollback:** each phase is its own PR and can be reverted alone. The preflight free-space
minimums are config options; setting one to 0 turns that check off without a code change.
