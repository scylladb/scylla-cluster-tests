# Minicloud

[← All configuration options](../configuration_options.md)

Minicloud is an AWS-API-compatible environment rather than a cloud of its own: it runs with
`cluster_backend: aws` and an endpoint override, and these options control the local minicloud
service.

**15 options.**


<a id="minicloud_container_cpus"></a>

## **minicloud_container_cpus** / SCT_MINICLOUD_CONTAINER_CPUS

Cap the minicloud container's CPU allowance, in docker --cpus form (e.g. '8' or '7.5'). Empty means no limit

**default:** N/A

**type:** str (appendable)


<a id="minicloud_container_memory"></a>

## **minicloud_container_memory** / SCT_MINICLOUD_CONTAINER_MEMORY

Cap the minicloud container's memory (e.g. '32GiB'). Empty means no docker limit, so the container can consume the whole host. Setting it also makes this, rather than the host's free memory, the budget the preflight guest-memory gate measures against

**default:** N/A

**type:** str (appendable)


<a id="minicloud_container_name"></a>

## **minicloud_container_name** / SCT_MINICLOUD_CONTAINER_NAME

Name of the minicloud docker container. Change it to run two emulators on one host — a second run under the same name force-removes the first one's container

**default:** minicloud

**type:** str (appendable)


<a id="minicloud_docker_image"></a>

## **minicloud_docker_image** / SCT_MINICLOUD_DOCKER_IMAGE

Explicit minicloud image override. Empty means the renovate-managed default from defaults/docker_images/minicloud/ (exposed as [`stress_image`](stress-commands-and-load-generation.md#stress_image).minicloud)

**default:** N/A

**type:** str (appendable)


<a id="minicloud_endpoint_url"></a>

## **minicloud_endpoint_url** / SCT_MINICLOUD_ENDPOINT_URL

EC2 API endpoint URL for minicloud. When set, SCT adapts for minicloud limitations (no spot, no EIP, graceful TerminateInstances). Example: http://localhost:5000

**default:** N/A

**type:** str


<a id="minicloud_gcs_bucket"></a>

## **minicloud_gcs_bucket** / SCT_MINICLOUD_GCS_BUCKET

GCS bucket for minicloud GCE image staging. Empty means derive <project>-minicloud-staging and create it on demand

**default:** N/A

**type:** str (appendable)


<a id="minicloud_keep_alive"></a>

## **minicloud_keep_alive** / SCT_MINICLOUD_KEEP_ALIVE

Leave the minicloud container running after the test instead of tearing it down (CI sets this so separate provision/test/collect/clean stages reach the same container)

**default:** False

**type:** bool


<a id="minicloud_lightweight"></a>

## **minicloud_lightweight** / SCT_MINICLOUD_LIGHTWEIGHT

Enable lightweight mode for minicloud deployments

**default:** True

**type:** bool


<a id="minicloud_lightweight_memory"></a>

## **minicloud_lightweight_memory** / SCT_MINICLOUD_LIGHTWEIGHT_MEMORY

Memory allocation for lightweight minicloud deployments

**default:** 4GiB

**type:** str (appendable)


<a id="minicloud_lightweight_vcpus"></a>

## **minicloud_lightweight_vcpus** / SCT_MINICLOUD_LIGHTWEIGHT_VCPUS

vCPUs per guest in lightweight mode. Scylla runs one shard per vCPU, so this multiplies with [`minicloud_lightweight_memory`](#minicloud_lightweight_memory) across every guest in the test — raise it only on a host with cores to spare

**default:** 1

**type:** int


<a id="minicloud_regions"></a>

## **minicloud_regions** / SCT_MINICLOUD_REGIONS

Narrow the AWS regions minicloud prepares (default: every SCT-supported region; each costs ~2s at start-up)

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="minicloud_s3_passthrough_buckets"></a>

## **minicloud_s3_passthrough_buckets** / SCT_MINICLOUD_S3_PASSTHROUGH_BUCKETS

S3 buckets minicloud proxies to real AWS (keystore, job artifacts, downloads). Backend-independent: GCE runs reach S3 for the same content

**default:** scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com

**type:** str | list[str] → list[str] (appendable)


<a id="minicloud_scylla_reserve_memory"></a>

## **minicloud_scylla_reserve_memory** / SCT_MINICLOUD_SCYLLA_RESERVE_MEMORY

Extra memory reserved by scylla-server for the guest OS on lightweight minicloud guests, passed as --reserve-memory (for example, '3G'). Without this option, Scylla usually keeps about ~1.5GiB on guests below ~22GiB RAM, which may be too small for sshd and SCT helper tools; then sshd cannot fork and one-shot commands can be OOM-killed. Empty (default) means do not pass this option. Because this memory comes from Scylla own budget, tests must opt in. Ignored if [`append_scylla_args`](scylla-installation-and-configuration.md#append_scylla_args) already contains --memory or --reserve-memory

**default:** N/A

**type:** str (appendable)


<a id="minicloud_skip_memory_check"></a>

## **minicloud_skip_memory_check** / SCT_MINICLOUD_SKIP_MEMORY_CHECK

Skip the conservative host-memory preflight gate — for development machines whose owner knows the workload's real footprint; an oversized test then dies mid-run as a container OOM kill (exit 137)

**default:** False

**type:** bool


<a id="minicloud_state_dir"></a>

## **minicloud_state_dir** / SCT_MINICLOUD_STATE_DIR

Where minicloud keeps its image cache, per-instance disks and minicloud.log — tens of GiB. Empty means ~/.cache/minicloud; point it at a bigger disk or a CI workspace

**default:** N/A

**type:** str (appendable)
