# Scylla Cluster Tests (SCT): Loaders and Stress Commands

This document explains how to configure and use loaders and stress commands effectively in Scylla Cluster Tests (SCT). It primarily focuses on Longevity Tests (`longevity_test.LongevityTest.test_custom_time`), although the concepts apply similarly to other tests, which might include additional parameters (refer to specific test code/documentation for details).

---

## Overview of Loaders

Loaders are specialized machines used to generate workloads on Scylla clusters. They run stress tools such as:

- [cassandra-stress](https://github.com/scylladb/cassandra-stress/) (java driver) - the most commonly used stress tool, simulating various workloads on Scylla with predefined columns count, row size and user profiles for custom ones.
- [scylla-bench](https://github.com/scylladb/scylla-bench/) (gocql driver) - a stress tool designed for Scylla, providing more advanced features especially for testing large partitions.
- [gemini](https://github.com/scylladb/gemini/) (gocql driver) - tool for data consistency validation across various schemas and mixed workloads (read, write, delete). Verifies data integrity against 'oracle' cluster.
- [latte](https://github.com/scylladb/latte/) (rust driver) - highly customizable rust-based benchmarking tool for ScyllaDB and Cassandra.
Mostly used for testing complex and realistic customer scenarios with controlled disruptions and specific data flows. More about it [the article](https://www.scylladb.com/2025/07/01/latte-benchmarking/).
- [cql-stress](https://github.com/scylladb/cql-stress/) (rust driver) - `rust` based equivalent of `cassandra-stress` and `scylla-bench` - supports the same command line options.
- [ycsb](https://github.com/scylladb/ycsb/) (java driver) - a tool supporting ScyllaDB Alternator API, used for testing ScyllaDB in AWS DynamoDB compatible mode.

Loaders execute these stress tools within Docker containers (Docker images specified in `defaults/docker_images`). They help simulate realistic database loads and measure performance and reliability.

---

## How Loaders Operate

The stress load is initiated usually via methods such as:

- `sdcm.tester.ClusterTester.run_stress_thread`
- `sdcm.utils.loader_utils.LoaderUtilsMixin.assemble_and_run_all_stress_cmd`

### Stress Command Parameters

Stress commands are specified using these parameters:

- **`stress_cmd`**: Used primarily in longevity tests.
- **`stress_cmd_<w|r|m>`**: Used in performance tests, with `<w|r|m>` representing write, read, or mixed workloads.

These parameters accept either a single string or a list of strings, each representing a distinct stress command.

### Adjustments to Stress Commands

Stress commands provided by the user undergo the following modifications before execution:

- **Node targeting**: IP addresses of all nodes in the cluster are appended automatically.
- **Duration adjustment**: Modified according to the test’s `stress_duration` and `prepare_stress_duration` parameters, if present.
- **HDR Histogram**: if `use_hdrhistogram` is set to `true`, proper arguments are added and SCT handles collection HDR histograms. SCT supports HDR histograms for the `cassandra-stress`, `cql-stress` and `latte`. The HDR histograms support for the `ycsb` benchmarking tool is [being worked on](https://github.com/scylladb/scylla-cluster-tests/pull/10927). The scylla-bench tool does support HDR histograms itself, but not covered by SCT due to lack of use cases.
- **Authentication**: Adjusted based on LDAP parameters (`use_ldap`, `are_ldap_users_on_scylla`) and `authenticator_user` and `authenticator_password`.
- **Encryption**: Included when `client_encrypt` is enabled, considering additional params like `peer_verification` and `client_encrypt_mtls`.
- **Multi-DC tests**: Loader targets nodes within the same datacenter.
- **Rack awareness**: Enabled if `rack_aware_loader` is `true` (stress command targets nodes on the same rack as loader is).

### Execution Modes

Commands are executed in two possible modes based on the `round_robin` setting:

- **Round Robin (`true`)**: Each stress command runs once on a loader picked up using the `round_robin` selection approach.
- **Parallel Execution (`false`)**: All loaders run the command simultaneously. If `stress_multiplier` (or specific variants like `stress_multiplier_<w|r|m>`) is greater than 1, commands execute multiple parallel instances on each loader. **Avoid this mode** due to data overwrites, which can lead to incorrect data size estimation and increased compaction load.

---

## Preloading Data

Before executing main stress tests, loaders can populate the cluster with initial data using the `prepare_write_cmd` parameter. This is essential for validating read operations. This parameter also accepts single or multiple commands and follows similar execution logic.

---

## Adjusting Load Size

To effectively measure cluster performance, ensure loaders deliver sufficient stress:

- **`n_loaders`**: Specifies the number of loader machines.
- **`instance_type_loader`**: Defines the loader's hardware specification (use CPU-optimized instances for better results). Search test configuration files for examples (depends on cloud provider: `instance_type_loader` for AWS, `azure_instance_type_loader` for Azure, `gce_instance_type_loader` for GCE)
- **k8s_n_loader_pods_per_cluster**: Number of loader pods per loader cluster.
- **k8s_loader_run_type** : Defines how the loader pods must run. `static` (default) run stress command on the constantly existing idle pod having reserved resources, perf-oriented). `dynamic` run stress command in a separate pod as main thread and get logs in a separate retryable API call not having resource reservations.


---

## Test-specific Parameters

Here are common parameters for longevity and performance tests:

### Longevity Tests

- **`pre_create_schema`**:
  - If `true`, schemas are created before stress execution.
  - Also applies to artifact tests.

- **`keyspace_num`**:
  - Multiplies the stress commands to create multiple keyspaces.
  - Typically used alongside `pre_create_schema`.

- **`batch_size`**:
  - Specifically used in `longevity_test.LongevityTest.test_batch_custom_time`.
  - Defines the number of stress commands executed in a single batch.

---

## Guidelines for Crafting Effective Stress Commands

When creating stress commands, consider the following guidelines:

- **Estimate Data Size**:
  - Example: In `cassandra-stress`, data size is calculated as `row_size * num_rows`.
  - Row size example: `-col 'size=FIXED(128) n=FIXED(8)'` equals approximately 1 KB per row.

- **Avoid Parallel Overwrites**:
  - Do not set `round_robin` to `false` with multiple loaders for write or mixed workloads, as this causes parallel data overwrites and incorrect data-size estimation, increasing compaction load unrealistically.

- **Optimize Loader Count and Size**:
  - Prefer fewer loaders with more resources to minimize overhead and improve performance.

- **Monitor Loader Performance**:
  - Regularly check loader OS metrics in Grafana to ensure adequate load generation without performance bottlenecks or wasted resources.

- **Monitor ScyllaDB Performance**:
  - Use Grafana dashboards to track ScyllaDB performance metrics, ensuring the cluster handles the stress load effectively.
  - verify latencies, throughput, load and find bottlenecks when issues are found (can be CPU, network or disk bound).
---
