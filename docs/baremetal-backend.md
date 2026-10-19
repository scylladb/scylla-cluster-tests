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
    "node_list": [{"public_ip": "10.0.0.60", "private_ip": "10.0.0.60", "sdm_resource": "lab-monster"}]
  },
  "monitor_nodes": {
    "username": "jenkins",
    "ip_ssh_connections": "private",
    "node_list": [{"public_ip": "10.0.0.60", "private_ip": "10.0.0.60", "sdm_resource": "lab-monster"}]
  }
}
```

- `username`: the SSH user. It needs passwordless sudo.
- `ip_ssh_connections` (optional): how the runner reaches the hosts of this role. It overrides the run's
  `ip_ssh_connections`:
  - `private`: direct connections, and logs go straight to the runner. Use it for hosts on the runner's network.
  - `public`: the hosts' public address, with reverse SSH tunnels carrying the logs back. Use it for hosts the runner
    reaches but which can't reach the runner, such as a cloud VM when SCT runs in the lab.
- `sdm_resource` (optional): the host's StrongDM SSH resource. SCT then opens SSH through a StrongDM client container
  next to it, instead of to the host's address, and the host needs no SCT key. This needs the lab SDM lookup from
  scylla-qa-internal and the `sdm_lab.json` KeyStore secret with the service account (SCT-1275).

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
| `baremetal_docker_image_budget_gb` | 20 | GB of unused Docker images a loader or monitor host keeps after cleanup |

`0` turns each of them off.

## What a host must provide

- SSH as `username`, with the key in `user_credentials_path`, or an `sdm_resource`.
- Passwordless sudo for that user.
- Loader and monitor hosts:
  - Docker Engine 20.10 or newer, any packaging (`docker-ce`, `moby-engine`, distro `docker.io`). The Podman `docker`
    shim is rejected.
  - Free space where Docker keeps images, at least the minimum above. With the containerd image store (the default
    since Docker 29, and Fedora's `moby-engine`) that is `/var/lib/containerd`, not `/var/lib/docker`.
- DB hosts: the disks for Scylla, unless `use_preinstalled_scylla` is set.

SCT does not upgrade Docker on these hosts: an upgrade restarts the daemon under other jobs' containers. A too old
Docker fails the setup with the version found and the version needed, so the host owner can upgrade it.

## What SCT changes on a host

**Loader and monitor hosts**

- Installs only the monitoring prerequisites that are missing: `curl`, `unzip`, `python3`, `pip3`, and the `yaml` and
  `psutil` Python modules. With everything present it runs no package manager or pip command.
- Adds the SSH user to the `docker` group when only root can use Docker. Docker itself is not restarted.
- Removes Docker images no container uses when the free space is below the minimum, and fails before any pull if that
  is not enough.
- node_exporter: a node_exporter already serving one of ports 9100-9119 is reused, whatever its version and collectors
  (for example Fedora's `prometheus-node-exporter`). Otherwise SCT installs its own on the first free port there.
  Loader and monitor scrape targets use that port.

**Monitor hosts**

- Each of Grafana (3000), Prometheus (9090), Alertmanager (9093), Loki (3100) and Promtail (9080) whose default port
  is taken moves to the next free port, and the run logs the move. For example, Fedora's Cockpit holds 9090, so
  Prometheus starts on 9091.
- Once a port moves, the stack runs on its own Docker network instead of the host network, and the moved containers
  are named `<name>-<port>` (`aprom-9091`). SCT reads the ports from there, including log collection after the test.

**DB hosts**

- Sets up the Scylla disks, and marks where the run starts in the host's journal, since one journal holds every run.

## What cleanup undoes

`hydra clean-resources` cleans up the hosts of the node types the post behavior asks to clean:

- Frees the reverse SSH tunnel ports the run bound, for `public` hosts.
- Undoes the Scylla disk setup of DB hosts.
- Keeps the unused Docker images on loader and monitor hosts within `baremetal_docker_image_budget_gb`, removing the
  least recently pulled first. Images a container uses, other jobs' included, are never removed.

A run repeats the tunnel and disk steps at its start, for a host whose cleanup did not run.

## Known gaps

- A distro node_exporter may lack collectors SCT's own enables or disables; dashboard panels fed by a missing
  collector stay empty.
- The SCT test runner, when it runs on a lab host's Jenkins agent, keeps its own containers (vector) on that host;
  they are not part of the host cleanup.
