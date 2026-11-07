# Connectivity per role, and lab hosts by name

**Jira:** SCT-1215

## Problem

`ip_ssh_connections` is one value for the whole run. A run that mixes lab
hosts next to the runner with a cloud DB node, reached over its public
address, must set it to `public`. Then every node gets reverse SSH tunnels,
also the lab hosts that the runner reaches directly.

When the loader and the monitor share one host, the setup of the second node
frees the tunnel ports of the host as left over from an earlier run. It
stops the tunnel that the first node just opened, and the first node stops
shipping its logs.

SSH to a lab host needs an SCT key that somebody adds to the host by hand.
The lab access service (SCT-1275) gives SSH to lab hosts by name with no key
on the host, but SCT cannot use it.

## Who it affects

Baremetal runs that mix lab hosts with cloud nodes, such as the SCT-1215 job:
loader and monitor on `monster`, one DB node in AWS. Every lab host that SCT
uses needs the manual key.

## Evidence

The SCT-1215 Jira comment of 2026-10-13, build #3:

> With loader and monitor on the same host, the monitor node's setup killed
> the reverse SSH tunnel the loader had just opened, as if it was left over
> from an earlier run.

The SCT-1215 Jira comment of 2026-10-18, run 4:

> each role in the baremetal hosts JSON (`db_nodes`, `loader_nodes`,
> `monitor_nodes`) can set its own `ip_ssh_connections`. Before, it was a
> single global setting, so one cloud DB node forced reverse tunnels on every
> node

The SCT-1215 Jira comment of 2026-10-18, build #7:

> the SCT key added to monster by hand is still needed until SDM SSH
> (SCT-1275) lands.

## Definition of done

- A run with a `public` DB node and `private` lab hosts opens reverse
  tunnels for the DB node only.
- A loader and a monitor on one host both ship their logs for the whole run.
- A lab host that the hosts config names in the lab access service is
  reached through that service, by the run and by its cleanup, with no SCT
  key on the host.
- A host reached through the lab access service gets no reverse tunnels,
  also on a `public` role, and ships its logs directly.
- A role that does not set `ip_ssh_connections` uses the value of the run,
  so existing hosts configs work as today.
- The baremetal docs page tells how to set `ip_ssh_connections` per role and
  how to name a host in the lab access service.

## Out of scope

- Access to the monitoring ports from outside the lab network. That waits on
  the SCT-1275 choice between nested SSH forwards and TCP resources.
- The internal lookup of the lab access service. It lives in
  scylla-qa-internal.
- `ip_ssh_connections` per role on backends other than baremetal.
