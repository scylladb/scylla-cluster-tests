# Docker backend

[← All configuration options](configuration_options.md)

Running the cluster as local Docker containers.

**2 options.**


## **docker_image** / SCT_DOCKER_IMAGE

Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from [`scylla_version`](scylla-installation-and-configuration.md#scylla_version)

**default:** N/A

**type:** str (appendable)


## **docker_network** / SCT_DOCKER_NETWORK

Local docker network to use, if there's need to have db cluster connect to other services running in docker

**default:** N/A

**type:** str (appendable)
