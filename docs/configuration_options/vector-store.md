# Vector Store

[← All configuration options](../configuration_options.md)

The Vector Store service under test alongside Scylla.

**9 options.**


<a id="n_vector_store_nodes"></a>

## **n_vector_store_nodes** / SCT_N_VECTOR_STORE_NODES

Number of vector store nodes (0 = VS is disabled)

**default:** 0

**type:** int


<a id="vector_store_docker_image"></a>

## **vector_store_docker_image** / SCT_VECTOR_STORE_DOCKER_IMAGE

Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from [`vector_store_version`](#vector_store_version)

**default:** scylladb/vector-store

**type:** str (appendable)


<a id="vector_store_port"></a>

## **vector_store_port** / SCT_VECTOR_STORE_PORT

TCP port the Vector Store service listens on for its API.

**default:** 6080

**type:** int


<a id="vector_store_scylla_port"></a>

## **vector_store_scylla_port** / SCT_VECTOR_STORE_SCYLLA_PORT

ScyllaDB connection port for Vector Store

**default:** 9042

**type:** int


<a id="vector_store_threads"></a>

## **vector_store_threads** / SCT_VECTOR_STORE_THREADS

Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)

**default:** 0

**type:** int


<a id="vector_store_scylla_username"></a>

## **vector_store_scylla_username** / SCT_VECTOR_STORE_SCYLLA_USERNAME

Username for Vector Store to authenticate with ScyllaDB. When set, SCT creates this role and a service level on the ScyllaDB cluster, and attaches the service level to the role. With CassandraAuthorizer, SCT also grants VECTOR_SEARCH_INDEXING on all keyspaces to the role. Requires PasswordAuthenticator and the superuser in [`append_scylla_yaml`](scylla-installation-and-configuration.md#append_scylla_yaml) (auth_superuser_name, auth_superuser_salted_password). configurations/auth_cassandra.yaml sets all of them.

**default:** N/A

**type:** str (appendable)


<a id="vector_store_scylla_password"></a>

## **vector_store_scylla_password** / SCT_VECTOR_STORE_SCYLLA_PASSWORD

Password for the Vector Store ScyllaDB user

**default:** N/A

**type:** str (appendable)


<a id="vector_store_service_level_shares"></a>

## **vector_store_service_level_shares** / SCT_VECTOR_STORE_SERVICE_LEVEL_SHARES

Shares for the Vector Store service level (default: 1000)

**default:** 1000

**type:** int


<a id="vector_store_version"></a>

## **vector_store_version** / SCT_VECTOR_STORE_VERSION

Vector Store version / docker image tag

**default:** N/A

**type:** str (appendable)
