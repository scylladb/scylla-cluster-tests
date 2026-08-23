# Docker backend specifics

### Overview
SCT supports multiple backends for deploying and testing Scylla clusters, with the Docker backend being a practical choice for local development and testing.<br>
That said, there are a few differences and specifics to be aware of when using the Docker backend, compared to other backends like AWS, GCE, or Azure.

## Running Nemeses
Not all nemeses are supported or run successfully on the Docker backend.<br>
The full list of supported, unsupported or failing nemeses (due to know issues) can be found on the [Individual Nemesis status on Docker backend](docker-backend-nemesis.md) page.

## Simulated racks require Scylla 2026.1 or newer
On the cloud backends a rack maps onto a real availability zone. Containers have no such thing, so on
the Docker backend the rack is passed to the Scylla image entrypoint as `--dc=datacenter1
--rack=RACK<n>` when the container is created, and the entrypoint writes `cassandra-rackdc.properties`
before Scylla first boots. Those arguments were added in **Scylla 2026.1**; an older entrypoint
forwards them to the Scylla binary, which rejects them and exits, so the container never comes up.

`simulated_racks` defaults to 3 for every backend, and racks take effect only when the test-case has
**more than one DB node** — a single-node cluster stays in one rack, and `endpoint_snitch` is left
alone, so nothing reads the rack. That combination is therefore accepted on any Scylla version.

Racks that would take effect on a pre-2026.1 image fail when the configuration is built, before any
container is created:

```
ValueError: simulated_racks=3 is not supported on Scylla 2025.1.0 (docker backend): the --dc/--rack
entrypoint arguments were added in 2026.1.0-dev. Use a 2026.1+ image or set simulated_racks: 1.
```

Either run a 2026.1+ image, or set `simulated_racks: 1` in the test-case if it has no interest in
racks. Branch versions such as `master:latest` are assumed new enough.

Note that a multi-node Docker test which does not mention `simulated_racks` inherits 3 and therefore
does get racks on a 2026.1+ image — rack-aware CQL routing, `GossipingPropertyFileSnitch` and one
rack per node — which is how the rack-aware code paths get cheap local coverage.

## Monitoring stack is on the Docker host machine
SCT does not support creating a dedicated monitoring node when using the Docker backend. As a result, the monitoring stack is installed directly on the host machine, not on a dedicated Docker instance.

## Scylla-manager is not installed
SCT installs Scylla-manager on a monitoring node, which is not supported in the Docker backend. Therefore, Scylla-manager is not installed when using this backend.

## Starting DB node instances with specific resources footprint
By default, Scylla on containerized DB instances is started with the following CPU and RAM configuration:
- `smp` parameter is set to 1
- `memory` parameter is not set

The default values are sufficient for simple test configurations. But for more complex scenarios the following issues may arise:
- if the `memory` parameter is not set or does not limit the memory usage per instance, RAM of the local machine can be exhausted (especially with multiple DB instances in the test configuration), causing the test to fail
- if the default `smp` parameter value is used, DB instance containers may become overloaded during stress commands execution, leading to test failures

To prevent these issues, the `smp` and `memory` parameters should be set according to the configuration and/or load profile of the test scenario.<br>
This can be done by setting the appropriate values through the `append_scylla_args` SCT config parameter in the test configuration file. For example:
```bash
append_scylla_args: '--smp 2 --memory 2G'
```

## SSL/TLS certificates
The Docker backend supports `client_encrypt` and `server_encrypt`, but certificate generation and
the file transfer that installs them work differently than on cloud backends — there is no SSH, so
files move as in-memory tar streams into the container.<br>
See [SSL/TLS on the Docker backend](docker-backend-ssl.md) for the certificate flow, the
trailing-slash copy semantics, and troubleshooting notes.

## Executing a longevity test on the Docker backend in Jenkins
SCT longevity tests can be executed on the Docker backend in Jenkins, with AWS serving as the cloud provider for SCT runner instance.<br>
This setup allows simulating test execution as if it was running on a local machine. The SCT runner instance in AWS operates as an all-in-one setup, hosting the loaders, DB nodes and monitoring stack.
