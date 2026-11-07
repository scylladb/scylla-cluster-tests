# Reuse a node_exporter that the host runs

**Jira:** SCT-1215

## Problem

Setup always installs its own node_exporter on loader and monitor hosts, on
port 9100. Lab hosts often run a node_exporter already, such as Fedora's
`prometheus-node-exporter`. The node_exporter of SCT then cannot bind 9100.
It stays on the host as a failed `node_exporter.service` unit, and a second
copy runs for nothing.

When a service that is not a node_exporter holds 9100, SCT has no exporter
on the host, and Prometheus scrapes the wrong service.

## Who it affects

Baremetal runs on lab hosts, such as the SCT-1215 longevity job on
`monster`. When the loader and the monitor share one host, both roles
install a copy, and both copies fail.

## Evidence

The SCT-1215 Jira comment of 2026-10-13, manual prep on `monster`:

| Change | Command |
| --- | --- |
| Removed SCT's failed node_exporter unit (Fedora's `prometheus-node-exporter` still serves 9100) | `rm /etc/systemd/system/node_exporter.service; systemctl daemon-reload` |

## Definition of done

- A host with a node_exporter on 9100 gets no node_exporter from SCT, and
  Prometheus scrapes the one that runs.
- A node_exporter that does not answer at the address that Prometheus
  scrapes, for example one that listens only on loopback, is not reused.
- A host with 9100 in use by another service gets the node_exporter of SCT
  on another port. Prometheus scrapes it there.
- A loader and a monitor on one host use one node_exporter.
- A reused monitor continues to scrape its exporters on the ports that
  setup found.
- No failed `node_exporter.service` stays on a host after the run.
- The baremetal docs page tells that SCT reuses an exporter, and that the
  firewall of the host must let the monitor scrape it.

## Out of scope

- A check that the firewall between the monitor and the host lets the scrape
  through. That is a host requirement.
- The collector set of a distro node_exporter. Dashboards read the same
  metric names, so missing or extra collectors are acceptable.
