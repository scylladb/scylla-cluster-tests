# Longevity tests

## Test operations

On a high level overview, the test operations are:

### Setup

1) Provision a Cluster DB, with the specified number of nodes (the number
   of nodes can be specified through the config file, or the test writer can
   set a specific number depending on the test needs).

2) Provision a set of loader nodes. They will be the ones to initiate
   cassandra stress, and possibly other database stress inducing activities.

3) Provision a set of monitoring nodes. They will run prometheus [3], to
   store metrics information about the database cluster, and also grafana [4],
   to let the user see real time dashboards of said metrics while the test is
   running. This is very useful in case you want to run the test suite and keep
   watching the behavior of each node.

4) Wait until the loaders are ready (SSH up and cassandra-stress is present)

5) Wait until the DB nodes are ready (SSH up and DB services are up, port 9042
   occupied)

6) Wait until the monitoring nodes are ready.

### Actual test

1) Loader nodes execute cassandra stress on the DB cluster (optional)

2) If configured, a Nemesis class, will execute periodically, introducing some
   disruption activity to the cluster (stop/start a node, destroy data, kill
   scylla processes on a node). the nemesis starts after an interval, to give
   cassandra-stress on step 1 to stabilize

Keep in mind that the suite libraries are flexible, and will allow you to
set scenarios that differ from this base one.

## Nemesis Error Detection

By default, nemesis operations now automatically fail if ERROR or CRITICAL events (such as coredumps, database errors, or backtraces) occur during their execution. This helps identify problematic nemesis operations and maintain accurate test statistics.

### Configuration

The feature is controlled by the `nemesis_fail_on_error_events` parameter:

```yaml
nemesis_fail_on_error_events: true  # Default - nemesis fails on ERROR/CRITICAL events
# or
nemesis_fail_on_error_events: false  # Disable - nemesis ignores error events
```

You can also set it via environment variable:
```bash
export SCT_NEMESIS_FAIL_ON_ERROR_EVENTS=false
```

### Behavior

When enabled (default), if ERROR or CRITICAL events occur during a nemesis operation:
- The nemesis will be marked as **Failed** instead of Succeeded
- A detailed error message will list the events (types, timestamps, brief descriptions)
- Full event details are available in the nemesis `full_traceback` field for debugging

Example error output:
```
Nemesis failed due to 2 ERROR event(s), 1 CRITICAL event(s) during execution

Event Details:
CRITICAL Events:
  1. [2026-01-20 13:00:10] CoreDumpEvent: Scylla crashed on node...

ERROR Events:
  1. [2026-01-20 13:00:08] DatabaseLogEvent: Connection timeout occurred...
  2. [2026-01-20 13:00:09] BacktraceEvent: Stack trace captured...
```

### When to Disable

You may want to disable this feature (`nemesis_fail_on_error_events: false`) when:
- Testing error recovery scenarios where errors are expected
- Running tests specifically designed to verify behavior under error conditions
- Debugging tests where you want nemesis to complete despite errors

### Parallel Nemesis Support

The error detection system correctly handles parallel/concurrent nemesis scenarios by:
- Only counting events that occurred during the specific nemesis execution time window
- Excluding events attributed to other running nemeses
- Respecting all configured event filters

This ensures each nemesis only fails for events truly related to its own execution.
