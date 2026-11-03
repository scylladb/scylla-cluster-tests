# Baremetal backend

## Overview

The baremetal backend runs a test on hosts that already exist: lab machines, or VMs created outside SCT.
SCT neither creates nor destroys them. It connects over SSH, sets up what the run needs, and undoes what would break
the next run.

Such hosts are usually shared: they run their own services, other Jenkins jobs, and earlier SCT runs left things on
them. SCT therefore changes as little as it can, and never restarts or reinstalls something that already works.

A host can play more than one role. A lab host can be both the loader and the monitor of a run, with the DB nodes on
other hosts or in a cloud.

## Hosts config

The hosts are listed in a JSON file named by `s3_baremetal_config`. SCT reads `<name>.json` from the working
directory when it exists, otherwise `<name>.json` from the KeyStore (Secrets Manager, `sct/<name>.json`).

```json
{
  "db_nodes": {
    "username": "ubuntu",
    "ip_ssh_connections": "public",
    "node_list": [{"public_ip": "54.77.53.161", "private_ip": "10.4.13.82"}]
  },
  "loader_nodes": {
    "username": "jenkins",
    "ip_ssh_connections": "private",
    "node_list": [{"private_ip": "10.0.0.60", "access": "monster"}]
  },
  "monitor_nodes": {
    "username": "jenkins",
    "ip_ssh_connections": "private",
    "node_list": [{"private_ip": "10.0.0.60", "access": "monster"}]
  }
}
```

- `username`: the SSH user. It needs passwordless sudo.
- `ip_ssh_connections` (optional): how the runner reaches the hosts of this role. It overrides the run's
  `ip_ssh_connections`:
  - `private`: direct connections, and logs go straight to the runner. Use it for hosts on the runner's network.
  - `public`: the hosts' public address, with reverse SSH tunnels carrying the logs back. Use it for hosts the runner
    reaches but which can't reach the runner, such as a cloud VM when SCT runs in the lab.
- `public_ip` (optional): defaults to `private_ip`, since lab hosts often have no other address.
- `access` (optional): the host's name in the lab access service. SCT then opens SSH where that service says instead
  of to the host's address, and the host needs no SCT key. The lookup lives in scylla-qa-internal (SCT-1275).

## Configuration

```yaml
cluster_backend: baremetal
s3_baremetal_config: baremetal_lab_hosts
user_credentials_path: '~/.ssh/scylla_test_id_ed25519'
ip_ssh_connections: public  # default for the roles that don't set their own
```

| Option | Default | Meaning |
|--------|---------|---------|
| `docker_min_free_space_gb_loader` | 3 | Free GB a loader's Docker image store needs before the run pulls images |
| `docker_min_free_space_gb_monitor` | 10 | The same for a monitor host |

`0` turns the check off. A host with several roles needs the sum: 13 GB for a loader and monitor.

`use_mgmt` defaults to `false` on baremetal, and `true` fails config validation: Manager would bring a running Scylla,
its ports and its timers onto the shared monitor host.

## What a host must provide

- SSH as `username`, with the key in `user_credentials_path`, or an `access` name.
- Passwordless sudo for that user.
- Loader and monitor hosts:
  - Docker Engine 20.10 or newer, any packaging (`docker-ce`, `moby-engine`, distro `docker.io`), running and usable
    by the SSH user without sudo (the user is in the `docker` group). The Podman `docker` shim is rejected. SCT doesn't
    install Docker or change the group on these hosts.
  - Free space where Docker keeps images, at least the minimum above, summed over the host's roles. With the
    containerd image store (the default since Docker 29, and Fedora's `moby-engine`) that is `/var/lib/containerd`,
    not `/var/lib/docker`.
  - A firewall that lets the monitor scrape the host's node_exporter, at the address and port SCT finds (see below).
- Monitor hosts: the monitoring stack's ports free, since SCT starts it on the host network: Grafana 3000 and its
  renderer 8081, Prometheus 9090, Alertmanager 9093 and its gossip port 9094, Loki 3100 and its gRPC port 9095,
  Promtail 9080 and its syslog port 1514. Fedora's Cockpit holds 9090 by default, so it is disabled on lab hosts.
  Since the ports are fixed, a host runs one SCT monitor at a time: a second run's monitor fails the check.
- DB hosts: the disks for Scylla, unless `use_preinstalled_scylla` is set.

Setup checks the loader and monitor requirements before the first image pull (the preflight check), and fails
naming what is wrong and what is needed. The missing monitoring prerequisites and node_exporter are installed before
that check. A host with several roles is checked as one host. SCT does not upgrade
Docker on these hosts: an upgrade restarts the daemon under other jobs' containers.

## What SCT changes on a host

**Every host**

The node setup every SCT backend runs also runs on baremetal hosts, whatever their role:

- Installs the log shipper (vector, with the default `logs_transport: vector`) as a host service, pointed at the run's
  log collector. A later run on the host points it at its own collector.
- The first time on a host, the same script edits `/etc/ssh/sshd_config` (`MaxSessions`, `MaxStartups`,
  `LoginGraceTime`, `ClientAlive*`), restarts sshd, and writes `/etc/security/limits.d/20-nproc.conf`.
- Installs `rsync`.
- Refreshes the package cache: `apt-get clean` and `apt-get update`, or `yum clean all`.
- Marks where the run starts in the host's journal, since one journal holds every run.

**Loader and monitor hosts**

- Doesn't disable the host's apt-daily timers: the host's owner decides how it updates.
- Loaders: install EPEL on RHEL-like hosts other than Fedora, append the coredump limits to
  `/etc/security/limits.d/20-coredump.conf` on every run, and log in to Docker Hub, which writes the SSH user's
  `~/.docker/config.json`.
- Installs only the monitoring prerequisites that are missing: `curl`, `unzip`, `python3`, `pip3`, and the `yaml` and
  `psutil` Python modules. With everything present it installs no package and runs no pip command; the package cache
  refresh above still runs.
- Doesn't install, restart or reconfigure Docker, change the firewall, or upgrade the SSH packages.
- Removes Docker images no container uses when the free space is below the minimum, at most once per host, and fails
  before any pull if that is not enough. Images a container uses, other jobs' included, are never removed.
- node_exporter: a node_exporter already serving one of ports 9100-9119 at the address the monitor scrapes is reused,
  whatever its version and collectors (for example Fedora's `prometheus-node-exporter`). One bound only to loopback
  doesn't answer there and isn't reused. Otherwise SCT installs its own on the first free port there. Loader and
  monitor scrape targets use that port.

**Monitor hosts**

- Each run gets its own monitoring install and data directory, `~/sct-monitoring-<test id>`, so a second run can't
  delete the first one's.
- The run's stack is the containers with a mount under that directory. A restart stops only those, never the stack's
  `kill-all.sh`, which removes every container with the stack's names. A stack that fails to start fails the setup.

**DB hosts**

- Sets up the Scylla disks.

## What cleanup undoes

`hydra clean-resources` cleans up the hosts of the node types the post behavior asks to clean:

- Frees the reverse SSH tunnel ports the run bound, for `public` hosts.
- Every host: stops the journal followers a run left running on the host.
- Monitor hosts: removes the run's monitoring stack containers and its `~/sct-monitoring-<test id>` directory, after
  log collection took the data. Another run's stack is left alone.
- DB hosts: restores the package baseline, and starts the baseline's services that are stopped. On a host the run
  installed Scylla on, it also undoes the Scylla disk setup and removes the files the install left.
- Loader and monitor hosts keep their packages: other jobs share them, so restoring a baseline would remove their
  packages too. The prerequisites SCT installed stay for the next run, and so does SCT's own node_exporter (the
  `node_exporter` user, `/usr/local/bin/node_exporter` and `node_exporter.service`), which the next run reuses.

A run repeats the tunnel and disk steps at its start, for a host whose cleanup did not run. It doesn't remove a
monitoring stack left by an earlier run whose cleanup did not run: that stack keeps the ports, and the next run's
preflight check fails on them. Clean it up with `hydra clean-resources` and the earlier run's test id, which is the
suffix of its `~/sct-monitoring-<test id>` directory on the host.

## Known gaps

- A distro node_exporter may lack collectors SCT's own enables or disables; dashboard panels fed by a missing
  collector stay empty.
- The SCT test runner, when it runs on a lab host's Jenkins agent, keeps its own containers (vector) on that host;
  they are not part of the host cleanup.
