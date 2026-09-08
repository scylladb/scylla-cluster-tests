# Minicloud

[← All configuration options](configuration_options.md)

Minicloud is an AWS-API-compatible environment rather than a cloud of its own: it runs with
`cluster_backend: aws` and an endpoint override, and these options control the local minicloud
service.

**14 options.** Jump to: [minicloud_container_cpus](#minicloud_container_cpus) · [minicloud_container_memory](#minicloud_container_memory) · [minicloud_container_name](#minicloud_container_name) · [minicloud_docker_image](#minicloud_docker_image) · [minicloud_endpoint_url](#minicloud_endpoint_url) · [minicloud_gcs_bucket](#minicloud_gcs_bucket) · [minicloud_keep_alive](#minicloud_keep_alive) · [minicloud_lightweight](#minicloud_lightweight) · [minicloud_lightweight_memory](#minicloud_lightweight_memory) · [minicloud_lightweight_vcpus](#minicloud_lightweight_vcpus) · [minicloud_regions](#minicloud_regions) · [minicloud_s3_passthrough_buckets](#minicloud_s3_passthrough_buckets) · [minicloud_skip_memory_check](#minicloud_skip_memory_check) · [minicloud_state_dir](#minicloud_state_dir)


## **minicloud_container_cpus** / SCT_MINICLOUD_CONTAINER_CPUS

Cap the minicloud container's CPU allowance, in docker --cpus form (e.g. '8' or '7.5'). Empty means no limit

**default:** N/A

**type:** str (appendable)


## **minicloud_container_memory** / SCT_MINICLOUD_CONTAINER_MEMORY

Cap the minicloud container's memory (e.g. '32GiB'). Empty means no docker limit, so the container can consume the whole host. Setting it also makes this, rather than the host's free memory, the budget the preflight guest-memory gate measures against

**default:** N/A

**type:** str (appendable)


## **minicloud_container_name** / SCT_MINICLOUD_CONTAINER_NAME

Name of the minicloud docker container. Change it to run two emulators on one host — a second run under the same name force-removes the first one's container

**default:** minicloud

**type:** str (appendable)


## **minicloud_docker_image** / SCT_MINICLOUD_DOCKER_IMAGE

Explicit minicloud image override. Empty means the renovate-managed default from defaults/docker_images/minicloud/ (exposed as stress_image.minicloud)

**default:** N/A

**type:** str (appendable)


## **minicloud_endpoint_url** / SCT_MINICLOUD_ENDPOINT_URL

EC2 API endpoint URL for minicloud. When set, SCT adapts for minicloud limitations (no spot, no EIP, graceful TerminateInstances). Example: http://localhost:5000

**default:** N/A

**type:** str


## **minicloud_gcs_bucket** / SCT_MINICLOUD_GCS_BUCKET

GCS bucket for minicloud GCE image staging. Empty means derive <project>-minicloud-staging and create it on demand

**default:** N/A

**type:** str (appendable)


## **minicloud_keep_alive** / SCT_MINICLOUD_KEEP_ALIVE

Leave the minicloud container running after the test instead of tearing it down (CI sets this so separate provision/test/collect/clean stages reach the same container)

**default:** False

**type:** bool


## **minicloud_lightweight** / SCT_MINICLOUD_LIGHTWEIGHT

Enable lightweight mode for minicloud deployments

**default:** True

**type:** bool


## **minicloud_lightweight_memory** / SCT_MINICLOUD_LIGHTWEIGHT_MEMORY

Memory allocation for lightweight minicloud deployments

**default:** 4GiB

**type:** str (appendable)


## **minicloud_lightweight_vcpus** / SCT_MINICLOUD_LIGHTWEIGHT_VCPUS

vCPUs per guest in lightweight mode. Scylla runs one shard per vCPU, so this multiplies with minicloud_lightweight_memory across every guest in the test — raise it only on a host with cores to spare

**default:** 1

**type:** int


## **minicloud_regions** / SCT_MINICLOUD_REGIONS

Narrow the AWS regions minicloud prepares (default: every SCT-supported region; each costs ~2s at start-up)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


## **minicloud_s3_passthrough_buckets** / SCT_MINICLOUD_S3_PASSTHROUGH_BUCKETS

S3 buckets minicloud proxies to real AWS (keystore, job artifacts, downloads). Backend-independent: GCE runs reach S3 for the same content

**default:** scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com

**type:** str | list[str] → list[str] (appendable)


## **minicloud_skip_memory_check** / SCT_MINICLOUD_SKIP_MEMORY_CHECK

Skip the conservative host-memory preflight gate — for development machines whose owner knows the workload's real footprint; an oversized test then dies mid-run as a container OOM kill (exit 137)

**default:** False

**type:** bool


## **minicloud_state_dir** / SCT_MINICLOUD_STATE_DIR

Where minicloud keeps its image cache, per-instance disks and minicloud.log — tens of GiB. Empty means ~/.cache/minicloud; point it at a bigger disk or a CI workspace

**default:** N/A

**type:** str (appendable)
