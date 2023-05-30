# Longevity tests

## Test operations

On a high level overview, the test operations are:

### Setup

1) Instantiate a Cluster DB, with the specified number of nodes (the number
   of nodes can be specified through the config file, or the test writer can
   set a specific number depending on the test needs).

2) Instantiate a set of loader nodes. They will be the ones to initiate
   cassandra stress, and possibly other database stress inducing activities.

3) Instantiate a set of monitoring nodes. They will run prometheus [3], to
   store metrics information about the database cluster, and also grafana [4],
   to let the user see real time dashboards of said metrics while the test is
   running. This is very useful in case you want to run the test suite and keep
   watching the behavior of each node.

4) Wait until the loaders are ready (SSH up and cassandra-stress is present)

5) Wait until the DB nodes are ready (SSH up and DB services are up, port 9042
   occupied)

6) Wait until the monitoring nodes are ready. If you are following the job log,
   you will see a message with the address you can point your browser to while
   the test is executing ::

### Actual test

1) Loader nodes execute cassandra stress on the DB cluster (optional)

2) If configured, a Nemesis class, will execute periodically, introducing some
   disruption activity to the cluster (stop/start a node, destroy data, kill
   scylla processes on a node). the nemesis starts after an interval, to give
   cassandra-stress on step 1 to stabilize

Keep in mind that the suite libraries are flexible, and will allow you to
set scenarios that differ from this base one.
