# One registry for the supported GCE regions

## Problem

SCT keeps the list of GCE regions that it supports in five places:

- `SUPPORTED_REGIONS` in `sdcm/utils/gce_utils.py`, which the builder setup reads.
- `SUPPORTED_GCE_REGIONS` in `sdcm/provision/gce/constants.py`, which the
  region fallback reads.
- `MINICLOUD_GCE_REGIONS` in `sdcm/utils/minicloud/addressing.py`, which the
  minicloud network setup reads.
- The `gce` entry of `supported_regions_by_provider` in
  `vars/getJenkinsLabels.groovy`, which picks the region and the builder label
  of a pipeline. The `gce-<region>` builder labels in the same file repeat the
  list again.
- The description of `gce_datacenter`, which the configuration documentation
  shows.

A person who adds or removes a region must find and change all of them. When
one copy is missed, the parts of SCT disagree about which regions exist. For
example, the region fallback can move a cluster to a region that has no
builder label, or a pipeline can pick a region that the fallback never uses.

SCT also does not check `gce_datacenter` against the list. A wrong region
passes the configuration check and fails later, during the provisioning. The
AWS backend checks its regions when it loads the configuration.

The rest of the original plan (PR #14543) is in master: the capacity error
classification, the upfront zone filter, the zone fallback, the region
fallback, and the Capacity Advisor ranking. This change covers only the
region list.

## Who it affects

- The maintainers of the GCE infrastructure, who add and remove regions.
- Every GCE pipeline, through the region and builder label that
  `getJenkinsLabels` picks.
- GCE test runs with `fallback_to_next_region`, which pick their target
  region from the list.
- Users who set `gce_datacenter` by hand.

## Evidence

Review comment on PR #14543:

> we still gonna have a list of supported regions, that we have configured
> SCT on, and we have runner configuration for, it should be expose to the
> pipelines in some way, so there would be one place to define the supported
> regions across everything (groovy, gce_builders.py, gce_region.py)

The comment above `MINICLOUD_GCE_REGIONS` says "Same regions as
getJenkinsLabels". Nothing checks that statement.

## Definition of done

- One file in the repository defines the supported GCE regions.
- The Python code and the Jenkins shared library read the regions from that
  file. No other copy of the list remains.
- The GCE builder labels in `getJenkinsLabels` come from the same file.
- A configuration with a `gce_datacenter` region that is not in the file
  fails when SCT loads the configuration, with a message that lists the
  supported regions.
- A unit test fails when a supported region has no minicloud subnet index or
  no secondary subnet index.
- The existing GCE test cases and pipelines run with no change.

## Out of scope

- The AWS, Azure, and OCI region lists.
- The region list of `hydra prepare-regions -c gce`, which prepares every GCE
  region.
- The order of the minicloud and secondary subnet regions. The order sets the
  subnet CIDRs and stays as it is.
- Changes to the zone and region fallback logic.
