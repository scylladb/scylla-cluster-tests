# Vector Store

[← All configuration options](configuration_options.md)

The Vector Store service under test alongside Scylla.

**6 options.** Jump to: [n_vector_store_nodes](#n_vector_store_nodes) · [vector_store_docker_image](#vector_store_docker_image) · [vector_store_port](#vector_store_port) · [vector_store_scylla_port](#vector_store_scylla_port) · [vector_store_threads](#vector_store_threads) · [vector_store_version](#vector_store_version)


## **n_vector_store_nodes** / SCT_N_VECTOR_STORE_NODES

Number of vector store nodes (0 = VS is disabled)

**default:** 0

**type:** int


## **vector_store_docker_image** / SCT_VECTOR_STORE_DOCKER_IMAGE

Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from vector_store_version

**default:** scylladb/vector-store

**type:** str (appendable)


## **vector_store_port** / SCT_VECTOR_STORE_PORT

TCP port the Vector Store service listens on for its API.

**default:** 6080

**type:** int


## **vector_store_scylla_port** / SCT_VECTOR_STORE_SCYLLA_PORT

ScyllaDB connection port for Vector Store

**default:** 9042

**type:** int


## **vector_store_threads** / SCT_VECTOR_STORE_THREADS

Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)

**default:** 0

**type:** int


## **vector_store_version** / SCT_VECTOR_STORE_VERSION

Vector Store version / docker image tag

**default:** N/A

**type:** str (appendable)
