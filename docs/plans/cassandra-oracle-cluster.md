# Cassandra Oracle Cluster for Gemini Tests

## Problem Statement

Gemini tests compare a Scylla "test cluster" against an "oracle cluster" to verify data consistency. Today, the oracle cluster is **another Scylla instance** (`ScyllaAWSCluster` with `node_type="oracle-db"`). This defeats the original purpose of Gemini — comparing Scylla against a **reference Cassandra implementation** to catch Scylla-specific bugs. Using Scylla-vs-Scylla means any bug present in both clusters is invisible.

Additionally, oracle cluster support is **AWS-only**. The `get_cluster_gce()`, `get_cluster_azure()`, and `get_cluster_docker()` methods in `sdcm/tester.py` have no `mixed_scylla` or `mixed` handling, so Gemini oracle tests cannot run on GCE, Azure, or Docker backends.

This plan introduces a **modern Cassandra cluster** (Cassandra 4.1 or 5.0) as a first-class oracle backend for Gemini tests, across all cloud providers and Docker.

## Current State

### Oracle Cluster Provisioning (AWS only)

The oracle cluster is created in `sdcm/tester.py:1847-1857` when `db_type == "mixed_scylla"`:

```python
# sdcm/tester.py:1847-1857
elif db_type == "mixed_scylla":
    self.test_config.mixed_cluster(True)
    return ScyllaAWSCluster(
        ec2_ami_id=self.params.get("ami_id_db_oracle").split(),
        ec2_ami_username=self.params.get("ami_db_scylla_user"),
        ec2_instance_type=self.params.get("instance_type_db_oracle"),
        ec2_block_device_mappings=db_info["device_mappings"],
        n_nodes=[self.params.get("n_test_oracle_db_nodes")],
        node_type="oracle-db",
        **(common_params | {"user_prefix": user_prefix + "-oracle"}),
    )
```

Key observations:
- Uses `ScyllaAWSCluster` — the oracle is a Scylla node, not Cassandra
- Config parameters: `ami_id_db_oracle`, `oracle_scylla_version`, `instance_type_db_oracle`, `n_test_oracle_db_nodes` (defined in `sdcm/sct_config.py:517-519, 603-606, 954-955, 980-981`)
- Defaults in `defaults/test_default.yaml:121-124`: `oracle_scylla_version: '2024.1'`, `n_test_oracle_db_nodes: 1`

### Legacy CassandraAWSCluster (obsolete)

A `CassandraAWSCluster` exists in `sdcm/cluster_aws.py:1145-1232` but is effectively dead code:
- Hard-coded to Cassandra **2.1.15** (circa 2014) via user-data: `--version community --release 2.1.15`
- Installs `openjdk-6-jdk` (end-of-life)
- Uses the old DataStax AMI provisioning mechanism
- Not used by any current Gemini test configuration
- Only reachable via `db_type: "cassandra"` or `db_type: "mixed"` paths (`sdcm/tester.py:1874-1878`)

### GeminiStressThread

`sdcm/gemini_thread.py:65-160` drives the Gemini binary. The oracle cluster is passed as a constructor argument:

```python
# sdcm/gemini_thread.py:71
oracle_cluster: ScyllaAWSCluster | CassandraAWSCluster | None
```

The thread generates `--oracle-cluster` and `--test-cluster` flags with comma-separated CQL IPs. Gemini itself is **database-agnostic** — it speaks CQL and works equally well with Cassandra or Scylla as oracle. No protocol-level changes are needed in the Gemini thread.

### Backend Coverage Gap

| Backend | Test cluster | Oracle cluster | Gap |
|---------|-------------|---------------|-----|
| AWS | `ScyllaAWSCluster` | `ScyllaAWSCluster` (oracle-db) | Uses Scylla, not Cassandra |
| GCE | `ScyllaGCECluster` | **None** | No `mixed_scylla` in `get_cluster_gce()` (`sdcm/tester.py:1549-1658`) |
| Azure | `ScyllaAzureCluster` | **None** | No `mixed_scylla` in `get_cluster_azure()` (`sdcm/tester.py:1660`) |
| Docker | `ScyllaDockerCluster` | **None** | No oracle support in `get_cluster_docker()` (`sdcm/tester.py:1918-1953`) |

### Log Collection Gap

The `collect_logs()` method in `sdcm/tester.py:4220-4305` iterates over a hardcoded list of cluster/collector pairs. It collects logs from `self.db_cluster` using `ScyllaLogCollector` but does **not** collect logs from `self.cs_db_cluster` (the oracle cluster). This means oracle node logs are silently lost during test teardown.

The existing `ScyllaLogCollector` in `sdcm/logcollector.py:863-929` is Scylla-specific:
- Collects Scylla systemd journal entries (`scylla-server.service`, `scylla-jmx.service`, etc.)
- Collects `scylla.yaml`, `io_properties.yaml`, Scylla-specific coredumps
- Collects Scylla Manager agent config
- None of these apply to a Cassandra node

Cassandra log locations differ fundamentally:
- **System log:** `/var/log/cassandra/system.log` (not journalctl)
- **Debug log:** `/var/log/cassandra/debug.log`
- **GC log:** `/var/log/cassandra/gc.log.*`
- **Configuration:** `/etc/cassandra/cassandra.yaml`, `/etc/cassandra/cassandra-env.sh`
- **JVM options:** `/etc/cassandra/jvm11-server.options` or `/etc/cassandra/jvm17-server.options`
- **Heap dumps:** configurable, typically `/var/lib/cassandra/`
- **Thread dumps:** via `nodetool sjk` or `jstack`

### Gemini Test Configs

All Gemini test configs in `test-cases/gemini/` use `db_type: mixed_scylla` with AWS-specific instance types (e.g., `instance_type_db_oracle: 'i4i.8xlarge'`). Example from `test-cases/gemini/gemini-basic-3h.yaml`:

```yaml
db_type: mixed_scylla
n_test_oracle_db_nodes: 1
instance_type_db_oracle: 'i4i.8xlarge'
```

## Goals

1. **Extend oracle support with Cassandra** — add Apache Cassandra 4.1 or 5.0 as a Gemini oracle option alongside the existing Scylla oracle, fulfilling Gemini's original design intent
2. **Support all backends** — Cassandra oracle clusters on AWS, GCE, Azure, and Docker
3. **Introduce a new `db_type: "mixed_cassandra"`** — to clearly differentiate from the existing `mixed_scylla` flow, allowing both to coexist
4. **Minimize custom image building** — use official Docker images and standard apt packages rather than maintaining custom AMIs/images per Cassandra version
5. **Keep the existing `mixed_scylla` flow intact** — no regressions to current Gemini tests
6. **Fix oracle log collection** — oracle node logs (both `mixed_scylla` and `mixed_cassandra`) are currently not collected during teardown (pre-existing bug); this plan fixes it for both flows

## Cassandra Image Strategy

### Docker Backend (Development / CI)

**Recommended: Official Apache Cassandra Docker image**

- Image: `cassandra:4.1` or `cassandra:5.0` from [Docker Hub](https://hub.docker.com/_/cassandra)
- These are the official images maintained by the Apache Cassandra project
- Configuration via environment variables: `CASSANDRA_CLUSTER_NAME`, `CASSANDRA_SEEDS`, `CASSANDRA_DC`, `CASSANDRA_RACK`, `CASSANDRA_NUM_TOKENS`, `CASSANDRA_ENDPOINT_SNITCH`, `MAX_HEAP_SIZE`, `HEAP_NEWSIZE`
- CQL port 9042 is exposed by default
- No installation step needed — just pull and run

**Why this is the best starting point:**
- Fastest iteration cycle for development
- No cloud cost during development
- Identical Cassandra binary to what runs in cloud
- Can validate the full Gemini flow locally before running on cloud

### AWS Backend

**Option A (Recommended): Generic Ubuntu AMI + Cassandra tarball install**

- Use the same base AMI as loaders (`ami_id_loader`) or a clean Ubuntu 22.04/24.04 AMI
- Install Cassandra in `node_setup()` via the official tarball or apt repository:
  ```
  # apt-based (Cassandra 4.1)
  curl -o /etc/apt/trusted.gpg.d/cassandra.asc https://downloads.apache.org/cassandra/KEYS
  echo "deb https://debian.cassandra.apache.org 41x main" | sudo tee /etc/apt/sources.list.d/cassandra.sources.list
  sudo apt-get update && sudo apt-get install -y cassandra

  # For Cassandra 5.0, use the 50x distribution:
  echo "deb https://debian.cassandra.apache.org 50x main" | sudo tee /etc/apt/sources.list.d/cassandra.sources.list
  ```
- **Pros:** No custom AMI maintenance, always gets latest patch version, reuses existing AMI infrastructure
- **Cons:** Slower node_setup (~2-5 min install), requires JDK installation (Cassandra 4.x needs JDK 11, Cassandra 5.x needs JDK 11 or 17)

The oracle cluster is typically 1 node, so the 2-5 minute install overhead is negligible compared to test durations of 3-10 hours. No pre-baked images will be maintained — apt install with retry logic is sufficient.

### GCE Backend

**Option A (Recommended): Generic image + Cassandra install**

- Use a base Debian/Ubuntu GCE image (the same approach as AWS Option A)
- Install Cassandra via apt or tarball during `node_setup()`
- GCE image parameter: `gce_image_db_cassandra_oracle`

**Option B: GCE Marketplace**

- Cassandra images exist on GCE Marketplace from Bitnami and others
- **Not recommended:** Marketplace images have non-standard layouts, licensing concerns, and version lag

### Azure Backend

**Option A (Recommended): Generic VM image + Cassandra install**

- Use a standard Ubuntu 22.04 Azure VM image
- Install Cassandra during `node_setup()` via apt or tarball
- Azure image parameter: `azure_image_db_cassandra_oracle`

**Option B: Azure Marketplace**

- Similar to GCE — marketplace images exist but are not recommended for the same reasons

### Summary: Image Strategy Per Backend

| Backend | Image Source | Config Parameter | Install Method |
|---------|------------|-----------------|----------------|
| Docker | `cassandra:4.1` or `cassandra:5.0` (Docker Hub) | `docker_image_cassandra_oracle` | Pull & run (no install needed) |
| AWS | Ubuntu 22.04 base AMI | `ami_id_db_cassandra_oracle` (or reuse loader AMI) | apt install during `node_setup()` |
| GCE | Ubuntu/Debian base GCE image | `gce_image_db_cassandra_oracle` (or reuse loader image) | apt install during `node_setup()` |
| Azure | Ubuntu 22.04 Azure VM image | `azure_image_db_cassandra_oracle` (or reuse loader image) | apt install during `node_setup()` |

## Implementation Phases

### Phase 1: Docker Cassandra Oracle Cluster

**Objective:** Run Gemini with a real Cassandra oracle on the Docker backend for local development and CI.

**Dependencies:** None — this is the foundation.

**Implementation:**

1. Create `CassandraDockerCluster` class in `sdcm/cluster_docker.py`:
   - Extend the existing Docker cluster infrastructure
   - Use the official `cassandra:4.1` (or `cassandra:5.0`) Docker image
   - Configure via environment variables (`CASSANDRA_CLUSTER_NAME`, `CASSANDRA_SEEDS`, etc.)
   - Implement `node_setup()` to wait for CQL port readiness
   - Implement `wait_for_init()` to verify the Cassandra node is accepting CQL connections
   - Override `get_node_cql_ips()` if the base class implementation doesn't work for Cassandra nodes

2. Add `db_type: "mixed_cassandra"` handling to `get_cluster_docker()` in `sdcm/tester.py:1918`:
   - When `db_type == "mixed_cassandra"`, create a `ScyllaDockerCluster` for the test cluster and a `CassandraDockerCluster` for `self.cs_db_cluster`
   - Pass the Cassandra oracle to Gemini via the existing `oracle_cluster` parameter

3. Add configuration parameters to `sdcm/sct_config.py`:
   - `docker_image_cassandra_oracle`: Docker image for Cassandra oracle (default: `cassandra`)
   - `cassandra_oracle_version`: Cassandra version tag (default: `4.1`)
   - Reuse existing `n_test_oracle_db_nodes` for node count

4. Update `GeminiStressThread` type annotation in `sdcm/gemini_thread.py:71`:
   - Change `oracle_cluster` type to `BaseCluster | None` (the current `ScyllaAWSCluster | CassandraAWSCluster | None` is unnecessarily narrow)

5. Create a test config: `test-cases/gemini/gemini-basic-cassandra-oracle-docker.yaml`

**Note:** Phases 1 and 2 should land in the same PR or immediately sequential PRs. If Phase 1 is merged and Phase 2 is delayed, the codebase will have non-mixin `CassandraDockerCluster` as permanent state.

**Definition of Done:**
- [ ] `CassandraDockerCluster` starts a Cassandra container, waits for CQL, and is reachable
- [ ] Gemini test runs with `--backend docker` using Cassandra oracle
- [ ] Gemini successfully compares Scylla (test) vs Cassandra (oracle) reads
- [ ] Unit test verifying `cassandra.yaml` generation and config parameter wiring
- [ ] Integration test verifying `CassandraDockerCluster` starts and CQL port is reachable

---

### Phase 2: Base `CassandraCluster` Mixin

**Objective:** Create a backend-agnostic `CassandraCluster` mixin (analogous to `BaseScyllaCluster`) that encapsulates Cassandra-specific setup logic.

**Dependencies:** Phase 1 (validates the Docker approach).

**Implementation:**

1. Create `BaseCassandraCluster` mixin in `sdcm/cluster.py` (or a new `sdcm/cluster_cassandra.py`):
   - Cassandra-specific `node_setup()` logic: JDK installation, Cassandra installation (apt/tarball), `cassandra.yaml` configuration
   - Method to generate `cassandra.yaml` with correct seeds, cluster name, listen address, RPC address, snitch configuration
   - Health check: verify `nodetool status` shows UN (Up/Normal) for all nodes
   - Credential handling compatible with Gemini's `--oracle-username`/`--oracle-password`
   - **No nemesis support needed** — the oracle cluster only needs to stay stable

2. Refactor `CassandraDockerCluster` from Phase 1 to inherit from `BaseCassandraCluster` (for shared configuration logic)

3. Define the Cassandra version strategy:
   - Support Cassandra 4.1.x (LTS, JDK 11) and 5.0.x (latest, JDK 11/17)
   - Version controlled by `cassandra_oracle_version` config parameter
   - JDK version auto-selected based on Cassandra major version

**Definition of Done:**
- [ ] `BaseCassandraCluster` mixin extracts common Cassandra configuration logic
- [ ] `CassandraDockerCluster` refactored to use the mixin
- [ ] Docker Gemini tests still pass after refactoring

---

### Phase 3: AWS Cassandra Oracle Cluster

**Objective:** Run Gemini with a Cassandra oracle on AWS.

**Dependencies:** Phase 2 (shared Cassandra setup logic).

**Implementation:**

1. Create `CassandraAWSCluster` class (replace or rename the legacy one in `sdcm/cluster_aws.py:1145`):
   - Inherit from both `BaseCassandraCluster` and `AWSCluster`
   - Use a base Ubuntu AMI (configurable via `ami_id_db_cassandra_oracle`, defaults to the loader AMI)
   - `node_setup()`: install JDK + Cassandra via apt, generate `cassandra.yaml`, start service
   - `wait_for_init()`: wait for CQL readiness on all oracle nodes

2. Add `db_type: "mixed_cassandra"` handling to `get_cluster_aws()` in `sdcm/tester.py:1830`:
   ```python
   elif db_type == "mixed_cassandra":
       self.test_config.mixed_cluster(True)
       return CassandraAWSCluster(
           ec2_ami_id=self.params.get("ami_id_db_cassandra_oracle").split(),
           ec2_ami_username="ubuntu",
           ec2_instance_type=self.params.get("instance_type_db_oracle"),
           n_nodes=[self.params.get("n_test_oracle_db_nodes")],
           node_type="oracle-db",
           ...
       )
   ```

3. Add config parameters to `sdcm/sct_config.py`:
   - `ami_id_db_cassandra_oracle`: AMI for Cassandra oracle nodes (default: empty, meaning use loader AMI)
   - Reuse `instance_type_db_oracle` and `n_test_oracle_db_nodes`
   - Note: the existing `instance_type_db_oracle` defaults (e.g., `i4i.8xlarge`) are Scylla-centric (NVMe-optimized). Cassandra as oracle is predominantly memory-bound and does not require NVMe instances — test configs for Cassandra oracle may use smaller/cheaper instance types

4. Update the three KMS skip checks in `sdcm/tester.py` (lines ~981, ~1038, ~1097) to also skip when `db_type == "mixed_cassandra"`, matching the existing `mixed_scylla` exemption. Without this, KMS setup will be attempted against Cassandra nodes and fail.

5. Create test config: `test-cases/gemini/gemini-basic-cassandra-oracle-aws.yaml`

**Definition of Done:**
- [ ] Cassandra oracle cluster provisions on AWS from a base Ubuntu AMI
- [ ] Cassandra installs and starts during node_setup
- [ ] Gemini test runs on AWS with Cassandra oracle, comparing Scylla vs Cassandra
- [ ] Legacy `CassandraAWSCluster` (Cassandra 2.1.15) removed along with the unused `db_type: "cassandra"` and `db_type: "mixed"` code paths

---

### Phase 4: GCE and Azure Cassandra Oracle Clusters

**Objective:** Extend Cassandra oracle support to GCE and Azure backends.

**Dependencies:** Phase 3 (proven install-on-boot pattern from AWS).

**Implementation:**

1. **GCE:**
   - Create `CassandraGCECluster(BaseCassandraCluster, GCECluster)` in `sdcm/cluster_gce.py`
   - Add `mixed_cassandra` handling to `get_cluster_gce()` in `sdcm/tester.py:1549`
   - Config: `gce_image_db_cassandra_oracle` (or reuse loader image), `gce_instance_type_db_oracle`

2. **Azure:**
   - Create `CassandraAzureCluster(BaseCassandraCluster, AzureCluster)` in `sdcm/cluster_azure.py`
   - Add `mixed_cassandra` handling to `get_cluster_azure()` in `sdcm/tester.py:1660`
   - Config: `azure_image_db_cassandra_oracle` (or reuse loader image), `azure_instance_type_db_oracle`

3. Create backend-specific test configs under `test-cases/gemini/`

**Definition of Done:**
- [ ] Cassandra oracle cluster provisions on GCE from a base image
- [ ] Cassandra oracle cluster provisions on Azure from a base image
- [ ] Gemini tests pass on both GCE and Azure with Cassandra oracle

---

### Phase 5: Cassandra Oracle Log Collection

**Objective:** Ensure Cassandra oracle node logs are collected during test teardown and uploaded to S3 alongside other cluster logs.

**Dependencies:** Phase 2 (BaseCassandraCluster mixin exists).

**Implementation:**

1. Create `CassandraLogCollector` class in `sdcm/logcollector.py`:
   - Inherit from `LogCollector` (same base as `ScyllaLogCollector`)
   - Define Cassandra-specific `log_entities`:
     ```python
     log_entities = [
         FileLog(name="cassandra_system.log",
                 command="cat /var/log/cassandra/system.log",
                 search_locally=True),
         FileLog(name="cassandra_debug.log",
                 command="cat /var/log/cassandra/debug.log",
                 search_locally=True),
         FileLog(name="cassandra_gc.log",
                 command="cat /var/log/cassandra/gc.log.0.current",
                 search_locally=True),
         CommandLog(name="cassandra.yaml",
                    command="cat /etc/cassandra/cassandra.yaml"),
         CommandLog(name="cassandra-env.sh",
                    command="cat /etc/cassandra/cassandra-env.sh"),
         CommandLog(name="jvm-server.options",
                    command="cat /etc/cassandra/jvm11-server.options 2>/dev/null || "
                           "cat /etc/cassandra/jvm17-server.options 2>/dev/null || echo 'not found'"),
         CommandLog(name="nodetool_status",
                    command="nodetool status 2>/dev/null || echo 'nodetool unavailable'"),
         CommandLog(name="nodetool_info",
                    command="nodetool info 2>/dev/null || echo 'nodetool unavailable'"),
         CommandLog(name="nodetool_tpstats",
                    command="nodetool tpstats 2>/dev/null || echo 'nodetool unavailable'"),
         CommandLog(name="nodetool_compactionstats",
                    command="nodetool compactionstats 2>/dev/null || echo 'nodetool unavailable'"),
         CommandLog(name="cpu_info", command="cat /proc/cpuinfo"),
         CommandLog(name="mem_info", command="cat /proc/meminfo"),
         CommandLog(name="dmesg.log", command="sudo dmesg -P"),
         CommandLog(name="systemctl.status",
                    command="sudo systemctl status --all --full --no-pager"),
     ]
     ```
   - Set `cluster_log_type = "cassandra-oracle-cluster"` and `cluster_dir_prefix = "cassandra-oracle-cluster"`

2. For Docker backend: adapt log collection paths
   - Docker Cassandra images may use `/var/log/cassandra/` inside the container or output to stdout
   - The `CassandraLogCollector` should handle both cases, or the Docker variant can override log paths
   - Consider also collecting `docker logs <container_id>` output as a fallback

3. Register `CassandraLogCollector` in `collect_logs()` in `sdcm/tester.py:4239-4293`:
   - Add a new entry to the `clusters` tuple:
     ```python
     {
         "name": "cassandra_oracle",
         "nodes": self.cs_db_cluster and self.cs_db_cluster.nodes
                  if isinstance(self.cs_db_cluster, BaseCassandraCluster) else None,
         "collector": CassandraLogCollector,
         "logname": "cassandra_oracle_log",
     },
     ```
   - Add `"cassandra_oracle_log": ""` to the `logs_dict` initialization

4. Add `CassandraLogCollector` to imports in `sdcm/tester.py`

5. Ensure the log collector works for the `mixed_scylla` flow too:
   - When oracle is a Scylla node (`mixed_scylla`), the existing `ScyllaLogCollector` should handle it
   - Add a separate entry for `cs_db_cluster` using `ScyllaLogCollector` when `db_type == "mixed_scylla"` (this is a **pre-existing bug** — oracle Scylla logs are also not collected today)

**Definition of Done:**
- [ ] `CassandraLogCollector` collects `system.log`, `debug.log`, `gc.log`, `cassandra.yaml`, and nodetool outputs
- [ ] Cassandra oracle logs appear in the `collected_logs/` directory and are uploaded to S3
- [ ] Log collection works for both Docker and cloud backends
- [ ] Log collection for `mixed_scylla` oracle is also fixed (pre-existing gap)
- [ ] Unit test verifying `CassandraLogCollector` log entity definitions

---

### Phase 6: Cassandra Configuration Tuning and Hardening

**Objective:** Ensure the Cassandra oracle is stable and appropriately configured for its role as a Gemini oracle.

**Dependencies:** Phase 3 (at minimum AWS working).

**Implementation:**

1. Cassandra `cassandra.yaml` tuning for oracle role:
   - `num_tokens: 256` (explicitly set to match Scylla's token distribution — note that Cassandra 4.1+ defaults to `num_tokens: 16`, not 256; this mismatch doesn't affect oracle correctness since the oracle owns all tokens in its own ring, but matching Scylla's distribution is preferred)
   - `concurrent_reads: 32`, `concurrent_writes: 32` (oracle sees lower load)
   - `commitlog_sync: periodic` with `commitlog_sync_period_in_ms: 10000`
   - `memtable_heap_space_in_mb` and `memtable_offheap_space_in_mb` sized for the instance
   - `endpoint_snitch: SimpleSnitch` (single-DC oracle) or `GossipingPropertyFileSnitch` (multi-DC)
   - Disable features not needed for oracle: `enable_materialized_views: false`, `enable_sasi_indexes: false`

2. JVM tuning (`jvm11-server.options` or `jvm17-server.options`):
   - Heap size: 8-16 GB depending on instance type (oracle is typically a beefy single node)
   - G1GC tuning for stable latency

3. Monitoring integration:
   - Expose Cassandra JMX metrics for the SCT monitoring stack
   - **Needs Investigation:** Determine if the existing Prometheus/Grafana monitoring can scrape Cassandra metrics (likely needs a JMX exporter sidecar)

4. Graceful shutdown handling in SCT teardown

**Definition of Done:**
- [ ] Cassandra oracle is stable through 10-hour Gemini test runs
- [ ] No OOM or GC pause issues on the oracle node
- [ ] Oracle node metrics visible in monitoring (or documented as out-of-scope for Phase 6)

---

### Phase 7: Migrate Existing Gemini Tests

**Objective:** Transition existing Gemini test configurations from `mixed_scylla` to `mixed_cassandra`.

**Dependencies:** Phase 5 (stable Cassandra oracle).

**Implementation:**

1. For each test config in `test-cases/gemini/`:
   - Create a parallel `*-cassandra-oracle.yaml` variant with `db_type: mixed_cassandra`
   - Run both variants in CI to compare results
   - Once validated, consider making `mixed_cassandra` the default for new Gemini tests

2. Update Gemini Jenkins pipelines in `jenkins-pipelines/` to include Cassandra oracle jobs

3. Update documentation

**Definition of Done:**
- [ ] All Gemini test configs have `mixed_cassandra` variants
- [ ] CI runs both `mixed_scylla` and `mixed_cassandra` Gemini tests
- [ ] A follow-up issue is filed to track `mixed_scylla` deprecation timeline

## Testing Requirements

### Unit Tests

- `CassandraDockerCluster` creation and configuration generation
- `BaseCassandraCluster` mixin: `cassandra.yaml` generation for various scenarios (single node, multi-node, different versions)
- Config parameter validation for new Cassandra oracle parameters
- `GeminiStressThread` command generation with Cassandra oracle cluster type

### Integration Tests

- Docker: Full Gemini test cycle — provision Scylla + Cassandra, run Gemini, verify results
- This is the primary validation gate before moving to cloud backends

### Cloud Validation (Manual / CI)

- AWS: Provision Cassandra oracle, run `gemini-basic-3h` equivalent
- GCE/Azure: Same validation after Phase 4

### Log Collection Tests

- Unit test: `CassandraLogCollector` defines correct log entities for Cassandra paths
- Integration test (Docker): Verify `system.log`, `cassandra.yaml`, and nodetool output are collected from Cassandra oracle container
- Integration test (cloud): Verify logs are uploaded to S3 as `cassandra-oracle-cluster-*` archives

### Regression Tests

- Existing `mixed_scylla` Gemini tests must continue to pass unchanged
- The legacy `db_type: "cassandra"` and `db_type: "mixed"` paths should be tested or explicitly deprecated

## Success Criteria

1. Gemini tests can run with a **real Apache Cassandra** oracle cluster
2. Docker backend works for fast local development iteration
3. AWS backend works for production Gemini test runs
4. GCE and Azure backends work for multi-cloud testing
5. No regression to existing `mixed_scylla` Gemini tests
6. Cassandra oracle is stable for 10+ hour test runs
7. Configuration is simple — a single `db_type: mixed_cassandra` switch activates the Cassandra oracle
8. Cassandra oracle logs (`system.log`, `debug.log`, `gc.log`, config files, nodetool output) are collected during teardown and uploaded to S3

## Risk Mitigation

### Risk: Cassandra CQL compatibility differences

- **Impact:** Gemini might hit CQL features that behave differently between Scylla and Cassandra, causing false positives
- **Mitigation:** This is actually the **desired outcome** — Gemini is meant to find these differences. However, Gemini's `--cql-features` flag can be used to restrict to a common CQL subset. Start with `--cql-features normal` and expand.

### Risk: Cassandra performance insufficient for oracle role

- **Impact:** Oracle node becomes a bottleneck, slowing Gemini throughput
- **Mitigation:** Oracle nodes are typically single beefy instances (e.g., `i4i.8xlarge`). Cassandra on a single large node can handle the oracle workload. Gemini's `--concurrency` can be tuned down if needed. The oracle sees lower load than the test cluster (RF=1 vs RF=3).

### Risk: JDK installation failures on cloud VMs

- **Impact:** `node_setup()` fails due to apt repository issues or JDK download failures
- **Mitigation:** Pin specific JDK versions. Add retry logic to apt operations (already common in SCT).

### Risk: Legacy `CassandraAWSCluster` conflicts

- **Impact:** Naming conflict with the existing `CassandraAWSCluster` (Cassandra 2.1.15)
- **Mitigation:** Remove the legacy `CassandraAWSCluster` class entirely — it has not been used for years and can be safely dropped. The `db_type: "cassandra"` and `db_type: "mixed"` paths should also be cleaned up.

### Risk: Monitoring gap for Cassandra nodes

- **Impact:** No visibility into Cassandra oracle health during test runs
- **Mitigation:** Cassandra exposes metrics via JMX. A JMX-to-Prometheus exporter (e.g., `jmx_exporter` Java agent) can be deployed alongside Cassandra. This can be deferred to Phase 6.

## Open Questions

1. **Cassandra version policy:** Should we support only one Cassandra version (e.g., 5.0) or multiple? Supporting one simplifies maintenance. Supporting both 4.1 and 5.0 gives flexibility.
2. **Multi-node oracle:** Current oracle is single-node (RF=1). Is there a need for multi-node Cassandra oracle clusters? Single-node is simpler and sufficient for consistency checking.
3. **Cassandra authentication:** Should the Cassandra oracle run with authentication enabled (matching Scylla test cluster) or without (simpler setup)?
