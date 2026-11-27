# Per-datacenter, per-rack, and per-node DB cluster layout

**Jira:** SCT-528

## Problem

A test can not make a DB cluster whose nodes differ. All the DB nodes of a
test get the same instance type, the same disks, and the same image. The
options that shape the cluster are flat and global: `n_db_nodes`,
`simulated_racks`, `availability_zone`, `instance_type_db` (or its GCE, Azure,
and OCI forms), `sizing_db`, `root_disk_size_db`, and the
`data_volume_disk_*` options. `n_db_nodes` can differ per datacenter. Every
other option applies to every node.

The layout of the cluster is also hard to read. A reader must combine four or
more options, from the test case and from the Jenkins job, to know how many
nodes each rack has and where each rack is.

Some per-node overrides exist, but only for nodes that are added later:
`zero_token_instance_type_db`, `nemesis_grow_shrink_instance_type`, and
`instance_type_db_target`. Each one is a separate option for one use.

## Who it affects

- Tests of features that need racks or nodes of different sizes, such as
  elastic cloud, tablets load balancing, and mixed instance families.
- Test authors who need different disks or different images inside one
  cluster.
- Reviewers of test cases, who must rebuild the layout from many options.

## Evidence

GitHub issue scylladb/scylla-cluster-tests#10835, "Allow creating clusters
with multiple instance types":

> Currently, clusters are created with all instances being of the same type.
>
> But for features related to elastic cloud or load balancing for example,
> racks with different instance types are required.

From the same issue, a maintainer:

> I would like to see one "ultimate" cluster option which would enable us to
> set everything up without needing to set 4 options (nodes, racks,
> availability zones, etc) but this would make it much harder to override
> from jenkins.

On `upstream/master`, 2026-11-05, `instance_type_db` and `sizing_db` hold one
value for all DB nodes. The node-creation path accepts an instance type for
each call, but the configuration has no way to give one for each node.

## Definition of done

- A test case can describe the DB cluster as datacenters, racks in each
  datacenter, and nodes in each rack, in one option.
- Each node can have its own instance type or its own sizing constraints, its
  own disks, and its own image. Each rack can have its own availability zone.
- A reader can see the full layout of the DB cluster in that one option.
- A value that the layout does not give comes from the flat option, so a
  Jenkins override of a flat option still works.
- A test case that does not use the new option provisions the same cluster as
  today.
- A wrong layout fails at configuration load, with the path of the wrong
  value in the error.
- The provisioned cluster matches the layout on AWS and GCE.

## Out of scope

- Loader nodes, monitor nodes, and other roles.
- The K8S backends, which get nodes from node pools, and the xcloud backend.
- Removing or deprecating the flat options.
- A tool that converts existing test cases.
- Per-node network settings, tags, and spot or on-demand choice.
