# Docker backend specifics

### Overview
SCT supports multiple backends for deploying and testing Scylla clusters, with the Docker backend being a practical choice for local development and testing.<br>
That said, there are a few differences and specifics to be aware of when using the Docker backend, compared to other backends like AWS, GCE, or Azure.

## Running Nemeses
Not all nemeses are supported or run successfully on the Docker backend.<br>
The full list of supported, unsupported or failing nemeses (due to know issues) can be found on the [Individual Nemesis status on Docker backend](docs/docker-backend-nemesis.md) page.

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

## Executing a longevity test on the Docker backend in Jenkins
SCT longevity tests can be executed on the Docker backend in Jenkins, with AWS serving as the cloud provider for SCT runner instance.<br>
This setup allows simulating test execution as if it was running on a local machine. The SCT runner instance in AWS operates as an all-in-one setup, hosting the loaders, DB nodes and monitoring stack.
