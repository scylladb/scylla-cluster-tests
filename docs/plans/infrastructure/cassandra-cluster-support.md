---
status: draft
domain: cluster
created: 2026-03-08
last_updated: 2026-03-19
owner: fruch
---
# Cassandra Cluster Support in SCT

## Problem Statement

SCT has **no way to provision a standalone Apache Cassandra cluster** as a first-class cluster type. The existing `CassandraAWSCluster` in `sdcm/cluster_aws.py` is dead code pinned to Cassandra 2.1.15 (circa 2014) and is not wired into any active test flow.

This gap matters beyond any single use case. A two-cluster setup (Cassandra + ScyllaDB) is useful for:

- **Compatibility validation** — running workloads against both databases side-by-side to catch Scylla-specific regressions
- **Migration testing** — verifying that data and queries behave identically when moving from Cassandra to Scylla
- **Gemini oracle testing** — using Cassandra as the reference oracle for Gemini consistency checks (the original motivation for Gemini)
- **Performance comparison** — automated benchmarking of Scylla vs Cassandra under identical workloads; this has historically been done manually for blog posts and customer-facing materials, making it error-prone and hard to reproduce

### Why Gemini Is the Entry Point

Gemini is currently the **only multi-cluster configuration in SCT**, which makes it the natural place to introduce dual-cluster support. Gemini's original design intent was to compare Scylla against a **reference Cassandra implementation** to catch Scylla-specific bugs. Today, however, the oracle cluster is **another Scylla instance** (`ScyllaAWSCluster` with `node_type="oracle-db"`), which defeats this purpose — any bug present in both clusters is invisible.

Additionally, oracle cluster support is **AWS-only**. The `get_cluster_gce()`, `get_cluster_azure()`, and `get_cluster_docker()` methods in `sdcm/tester.py` have no `mixed_scylla` or `mixed` handling, so multi-cluster tests cannot run on GCE, Azure, or Docker backends.

This plan introduces a **modern Cassandra cluster** (Cassandra 4.1 or 5.0) as a first-class SCT cluster type, with Gemini oracle as the initial use case to validate the implementation across all cloud providers and Docker.

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

The legacy class has a `_cassandra_yaml()` context manager at approximately line 1200 that reads `/etc/cassandra/cassandra.yaml` and updates only the `seeds` stanza — all other configuration is whatever was baked into the DataStax AMI. This is a dead end architecturally: the AMI is gone, the JDK is EOL, and the API does not compose with SCT's modern provisioning pipeline.

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

### ScyllaYaml Provisioning Pipeline (Reference Architecture)

Understanding how Scylla's config pipeline works is essential context for designing the Cassandra equivalent. The pipeline for `scylla.yaml` is:

1. `ScyllaYaml` — a Pydantic model (`sdcm/provision/scylla_yaml/scylla_yaml.py`) representing every recognized parameter with types and defaults
2. `ScyllaYamlClusterAttrBuilder` — sets cluster-wide fields (cluster name, seeds, snitch, RF defaults)
3. `ScyllaYamlNodeAttrBuilder` — sets per-node fields (listen address, broadcast address, DC/rack)
4. `remote_scylla_yaml()` context manager — SSH-reads the existing `/etc/scylla/scylla.yaml`, deserializes into the Pydantic model, applies both builders, re-serializes, and writes back to the node

This is a powerful, type-safe pipeline. However, it exists because Scylla is a test target that needs fine-grained, version-aware configuration control during every test. Cassandra as an oracle cluster does not need that level of control — it needs to be configured once at install time and then left alone.

## Goals

1. **Provision a standalone Cassandra cluster as a first-class SCT cluster type** — introduce a `CassandraCluster` hierarchy (backed by `BaseCassandraCluster`) that SCT can provision independently of any specific test tool, replacing the dead legacy `CassandraAWSCluster`
2. **Support all backends** — Cassandra clusters on AWS, GCE, Azure, OCI, and Docker, using the same install-on-boot pattern as other cluster types
3. **Minimize custom image building** — use official Docker images and standard apt packages rather than maintaining custom AMIs/images per Cassandra version
4. **Keep the existing `mixed_scylla` flow intact** — no regressions to current Gemini tests
5. **Fix oracle log collection** — oracle node logs (both `mixed_scylla` and `mixed_cassandra`) are currently not collected during teardown (pre-existing bug); this plan fixes it for both flows
6. **Use cases enabled by the Cassandra cluster:**
   - 6a. **Gemini oracle** — wire the new Cassandra cluster into Gemini's `--oracle-cluster` flow via a new `db_type: "mixed_cassandra"`, fulfilling Gemini's original design intent
   - 6b. **Migration testing with Spark Migrator** — provision a Cassandra cluster as a data source for Spark Migrator to validate Cassandra-to-Scylla migration workflows end-to-end

## Cassandra vs ScyllaDB: Configuration and Operations Analysis

This section documents the fundamental differences between Cassandra and ScyllaDB from SCT's perspective. Understanding these differences is required to implement a correct `BaseCassandraCluster` mixin and to make informed decisions about configuration architecture.

### A. Configuration File Mapping

Cassandra and ScyllaDB have completely different configuration file sets. The table below maps each Cassandra file to its SCT handling strategy:

| Cassandra File | Purpose | SCT Equivalent | Handling Strategy |
|---|---|---|---|
| `/etc/cassandra/cassandra.yaml` | Main database config | `/etc/scylla/scylla.yaml` via `ScyllaYaml` builder | Template pattern: inject dynamic values via sed/Python at node_setup time |
| `/etc/cassandra/logback.xml` | Logging config (log levels, file rotation, appenders) | Built-in Scylla logging via systemd | Static template file; SCT substitutes `${LOG_DIR}` for the log directory path |
| `/etc/cassandra/jvm-server.options` | Base JVM flags (GC type, debugging, security) | N/A (Scylla is C++, uses Seastar reactor) | Template file; SCT substitutes computed heap values |
| `/etc/cassandra/jvm11-server.options` | Java 11-specific JVM flags | N/A | Required for Cassandra 4.1 (JDK 11); absent on 5.0-only nodes |
| `/etc/cassandra/jvm17-server.options` | Java 17-specific JVM flags | N/A | Required for Cassandra 5.0 (JDK 17); present but may be absent on 4.1 |
| `/etc/cassandra/cassandra-env.sh` | Dynamic JVM settings computed from system RAM | N/A | Template with `MAX_HEAP_SIZE` and `HEAP_NEWSIZE` substituted by SCT |
| `/etc/cassandra/cassandra-rackdc.properties` | DC/Rack topology for `GossipingPropertyFileSnitch` | `cassandra-rackdc.properties` (same file, same format) | Reuse existing SCT snitch handling logic; write DC/rack from SCT config |

**Dynamic vs static parameters per file:**

`cassandra.yaml` contains both cluster-wide fields (set once, same on every node) and per-node fields (set individually per node):
- **Cluster-wide (static after cluster creation):** `cluster_name`, `num_tokens`, `authenticator`, `authorizer`, `endpoint_snitch`, `concurrent_reads`, `concurrent_writes`, `commitlog_sync`, `commitlog_sync_period_in_ms`
- **Per-node (must be injected per node at setup time):** `listen_address`, `broadcast_address`, `broadcast_rpc_address`, `seed_provider.seeds`

`jvm-server.options` and `cassandra-env.sh` contain system-dependent values that must be computed at node_setup time from the node's actual RAM:
- `MAX_HEAP_SIZE` — computed from total available RAM
- `HEAP_NEWSIZE` — computed as a fraction of `MAX_HEAP_SIZE`

`logback.xml` can be static (same file across all nodes) with only the log directory path substituted.

`cassandra-rackdc.properties` must be per-node because each node has its own DC and rack assignment.

### B. Configuration Architecture Decision: Build vs Template vs Env Var

Three approaches exist for injecting Cassandra configuration:

**Option 1: Builder pattern (like ScyllaYaml + builders)**

Build a `CassandraYaml` Pydantic model and corresponding `CassandraYamlClusterAttrBuilder` / `CassandraYamlNodeAttrBuilder`. After install, SSH to each node, read the installed `/etc/cassandra/cassandra.yaml`, deserialize into the model, apply builders, and write back.

Pros: type-safe, catches unknown parameters, clean separation of cluster vs node concerns.
Cons: `cassandra.yaml` has over 200 documented parameters; maintaining a full Pydantic model for a stable oracle cluster that does not need fine-grained control is significant over-engineering. Cassandra's config schema also changes between 4.1 and 5.0 (the `cassandra_latest.yaml` split in 5.0 changes defaults). The builder approach requires constant model updates to track Cassandra releases.

**Option 2: Template pattern**

Ship a minimal `cassandra-oracle.yaml.j2` Jinja2 template (or a plain-text template with `sed` substitution points) that covers only the parameters SCT needs to control. During `node_setup()`, render the template with per-node values (listen address, seeds, cluster name) and upload it to `/etc/cassandra/cassandra.yaml`, overwriting the installed default.

Pros: simple, transparent, minimal maintenance surface. The template explicitly documents which parameters SCT manages. Changes to the template are visible as diffs.
Cons: if the installed `cassandra.yaml` has important defaults that the template omits, those defaults are lost. Mitigation: keep the template as a superset of the installed defaults by basing it on the official Cassandra 4.1 release defaults and only overriding the dynamic fields.

**Option 3: Docker environment variable pattern**

The official `cassandra:4.1` Docker image's entrypoint script reads `CASSANDRA_*` environment variables and injects them into `cassandra.yaml` using sed. This pattern is already supported by the Docker image and requires zero additional code for basic configuration.

Pros: zero config code for Docker backend — the image handles it. Cons: only works on Docker. Cannot be used on cloud VMs where the image entrypoint is not available.

**Recommendation:**

Use the **template pattern for cloud backends** (AWS, GCE, Azure) and the **env var pattern for Docker**.

Rationale for rejecting the full builder pattern: Cassandra is a stable oracle cluster, not a test target. It does not need the same level of per-test configuration variation that Scylla does. The `ScyllaYaml` builder exists because every SCT test may need to tune dozens of Scylla parameters depending on the test type, nemesis, and backend. Cassandra as an oracle only needs to be configured to join the cluster, be reachable on port 9042, and have reasonable memory settings. A 40-parameter template covers this completely. Building and maintaining a 200-parameter Pydantic model would double the maintenance cost of this feature for no practical benefit.

The template lives in `sdcm/templates/cassandra/cassandra-oracle.yaml.j2` and is checked into the repo. It is rendered with a Python `string.Template` or Jinja2 substitution during `node_setup()`. The rendered file replaces `/etc/cassandra/cassandra.yaml` on the node.

### C. Logging Architecture

The logging subsystem is one of the most significant operational differences between Scylla and Cassandra.

**Scylla logging:**
- Scylla logs to the systemd journal (`scylla-server.service`)
- `ScyllaLogCollector` collects logs via `journalctl -u scylla-server.service --no-pager -o json` and via the `CommandLog` class
- Log collection requires no specific log file paths — journalctl is always available
- No log rotation configuration is needed (journald handles rotation)

**Cassandra logging on cloud VMs:**
- Cassandra uses logback as its logging framework (XML-configured at `/etc/cassandra/logback.xml`)
- Log files live at `/var/log/cassandra/`:
  - `system.log` — primary operational log, equivalent to Scylla's journal
  - `debug.log` — verbose debug output; disabled by default in production configs but enabled by default in the packaged `logback.xml`
  - `gc.log.0.current` — JVM garbage collection log (G1GC output); rotated automatically
- The `cassandra` system user owns these files; collection requires `sudo` or the file ACLs must allow the SSH user to read them
- Log rotation is configured in `logback.xml` via `RollingFileAppender`; the default policy rotates at 20MB and keeps 20 archived files

**Cassandra logging on Docker:**
- The official `cassandra:4.1` image writes logs to stdout (Docker log capture)
- The image also writes `/var/log/cassandra/system.log` inside the container via a bind-mount-compatible path
- For Docker backend, SCT should collect logs via `docker logs <container>` (captured by `CommandLog`) plus any bind-mounted `/var/log/cassandra/` directory
- The `logback.xml` inside the Docker image configures a `ConsoleAppender` in addition to the file appenders

**SCT log collection strategy per backend:**

| Backend | Collection Method | Files/Sources |
|---|---|---|
| AWS, GCE, Azure (cloud VM) | SSH + `FileLog`/`CommandLog` | `/var/log/cassandra/system.log`, `debug.log`, `gc.log.0.current`, `/etc/cassandra/cassandra.yaml`, `/etc/cassandra/cassandra-env.sh`, `/etc/cassandra/jvm11-server.options` or `jvm17-server.options`, `nodetool status`, `nodetool info`, `nodetool tpstats` |
| Docker | `docker logs <id>` + container exec | stdout via `docker logs`, optionally `/var/log/cassandra/` if bind-mounted |

**logback.xml role in SCT:** SCT does not need to modify `logback.xml` for normal oracle operation. The default log level (INFO for `system.log`, DEBUG for `debug.log`) is appropriate. If log verbosity becomes excessive for 10-hour runs, the template for `logback.xml` can set `debug.log` to WARN level. For now, the default packaged `logback.xml` is used unchanged.

### D. Key cassandra.yaml Parameters (Dynamic)

The following parameters in `cassandra.yaml` must be set dynamically at node_setup time because they depend on the cluster topology, node IP addresses, or test configuration. These are the only parameters the SCT template needs to inject:

| Parameter | Source | Notes |
|---|---|---|
| `cluster_name` | SCT test ID or `cassandra_cluster_name` config param | Identifies the cluster; must match across all nodes in a multi-node setup |
| `seed_provider[0].parameters[0].seeds` | Comma-separated list of first N node IPs from provisioning | Seeds are only needed for joining; once joined, Gossip takes over |
| `listen_address` | Node private IP from SCT node object | The address Cassandra binds to for inter-node communication |
| `rpc_address` | `'0.0.0.0'` (always) | Bind CQL to all interfaces; required for cross-subnet access |
| `broadcast_address` | Node public IP if `ip_ssh_connections == 'public'`, else private IP | Advertised to other nodes; must be reachable from all cluster nodes |
| `broadcast_rpc_address` | Same as `broadcast_address` | Advertised CQL address; must be reachable from Gemini loaders |
| `native_transport_port` | Always `9042` | CQL port; matches Scylla default |
| `endpoint_snitch` | `SimpleSnitch` for single-DC, `GossipingPropertyFileSnitch` for multi-DC | Determines rack-awareness and DC topology |
| `num_tokens` | `cassandra_num_tokens` config param, default `16` | Cassandra 4.1+ default is 16; override to 256 only if needed |
| `data_file_directories` | Backend-dependent; default `/var/lib/cassandra/data` | NVMe path if available; otherwise default |
| `commitlog_directory` | Backend-dependent; default `/var/lib/cassandra/commitlog` | Should be on fast storage if possible |
| `authenticator` | `AllowAllAuthenticator` (default) or `PasswordAuthenticator` | Must match test cluster auth settings if Gemini uses credentials |
| `authorizer` | `AllowAllAuthorizer` (default) or `CassandraAuthorizer` | Same as authenticator: match test cluster |

**Parameters intentionally left at Cassandra defaults (not injected by SCT):**
- `concurrent_reads`, `concurrent_writes` — Cassandra auto-tunes based on CPU count
- `memtable_heap_space_in_mb` — leave at Cassandra's default auto-sizing
- `compaction_throughput_mb_per_sec` — leave at default
- All repair, streaming, and row cache settings — defaults are appropriate for an oracle

**Template rendering approach:**

```python
# In BaseCassandraCluster.generate_cassandra_yaml(node):
template_vars = {
    "cluster_name": self.name,
    "seeds": ",".join(n.ip_address for n in self.seed_nodes),
    "listen_address": node.ip_address,
    "rpc_address": "0.0.0.0",
    "broadcast_address": node.external_address,
    "broadcast_rpc_address": node.external_address,
    "endpoint_snitch": self._snitch_class(),
    "num_tokens": self.params.get("cassandra_num_tokens") or 16,
    "data_file_directories": self._data_directories(node),
    "commitlog_directory": self._commitlog_directory(node),
    "authenticator": self._authenticator(),
    "authorizer": self._authorizer(),
}
template_path = Path(__file__).parent / "templates/cassandra/cassandra-oracle.yaml.j2"
rendered = Template(template_path.read_text()).substitute(template_vars)
node.remoter.send_files(StringIO(rendered), "/etc/cassandra/cassandra.yaml", sudo=True)
```

### E. JVM Memory Model

ScyllaDB uses the Seastar reactor and manages its own memory directly — it takes a configured fraction of total RAM (typically 70-80%) and runs with no garbage collector. Cassandra runs on the JVM with a traditional heap + GC model, which requires explicit sizing.

**Why JVM heap sizing matters for SCT:**

If the JVM heap is not explicitly sized, Cassandra uses its bundled `cassandra-env.sh` auto-sizing script, which computes `MAX_HEAP_SIZE` from available RAM. The auto-sizing is usually acceptable, but on cloud VMs with unusual memory configurations (e.g., 768GB RAM instances where the script caps at 8GB), it can produce suboptimal results. SCT should always inject explicit heap values computed from the node's actual RAM.

**Heap sizing formula:**

```python
def compute_jvm_heap_mb(total_ram_mb: int) -> tuple[int, int]:
    """Compute JVM heap size for Cassandra oracle.

    Returns (MAX_HEAP_SIZE_MB, HEAP_NEWSIZE_MB).

    Rules:
    - Heap should be ~50% of total RAM.
    - Hard cap at 31744 MB (31 GB): above this, the JVM cannot use
      compressed object pointers (CompressedOops), which significantly
      increases memory overhead for object references.
    - HEAP_NEWSIZE (young generation) should be ~25% of heap size.
    - For G1GC (recommended for Cassandra 4.1+), the young gen size is
      mostly managed by the GC itself; HEAP_NEWSIZE is a hint only.
    """
    max_heap_mb = min(int(total_ram_mb * 0.5), 31744)
    heap_newsize_mb = int(max_heap_mb * 0.25)
    return max_heap_mb, heap_newsize_mb
```

**G1GC recommendation:**

Cassandra 4.1 and later recommend G1GC over CMS (which was removed in Java 14). The `jvm11-server.options` file should include:

```
-XX:+UseG1GC
-XX:G1RSetUpdatingPauseTimePercent=5
-XX:MaxGCPauseMillis=300
-XX:InitiatingHeapOccupancyPercent=70
-XX:+ParallelRefProcEnabled
-XX:+UnlockDiagnosticVMOptions
-XX:G1SummarizeRSetStatsPeriod=1
```

These flags are taken from the official Cassandra 4.1 distribution and are appropriate for an oracle cluster under moderate load.

**Memory budget for a Cassandra oracle node:**

On a typical oracle instance (e.g., 64 GB RAM):
- JVM heap: 31 GB (capped by CompressedOops limit)
- Off-heap: Cassandra uses direct ByteBuffers for SSTable I/O caches; typically 2-4 GB
- OS page cache: remaining RAM; important for SSTable read performance
- JVM overhead (metaspace, code cache, threads): ~1-2 GB

**SCT implementation:** Compute heap values in `BaseCassandraCluster._compute_heap_sizes(node)` by SSHing to the node and running `cat /proc/meminfo | grep MemTotal`, then calling `compute_jvm_heap_mb()`. Inject the computed values into `/etc/cassandra/cassandra-env.sh` by uncommenting and setting `MAX_HEAP_SIZE` and `HEAP_NEWSIZE` variables (they are present but commented out in the default `cassandra-env.sh`).

### F. Health Check Mapping

Cassandra and Scylla share `nodetool` as the primary cluster management tool and CQL as the client protocol, but differ in how SCT monitors service health:

| Check | Scylla approach | Cassandra approach | Difference |
|---|---|---|---|
| Service status | `systemctl status scylla-server` | `systemctl status cassandra` | Service name differs; unit file is `cassandra.service` not `scylla-server.service` |
| CQL connectivity | TCP connect to port 9042 | TCP connect to port 9042 | Same port, same protocol |
| Node operational state | `nodetool status` shows `UN` (Up/Normal) | `nodetool status` shows `UN` | Same output format — both use Cassandra-compatible nodetool |
| Cluster readiness | `nodetool describecluster` | `nodetool describecluster` | Identical — Cassandra invented this interface |
| Schema agreement | `nodetool describecluster` → `Schema versions` | Same | Same |
| JMX management | Not primary (REST API preferred) | Primary management interface on port 7199 | Cassandra relies heavily on JMX; SCT does not use JMX directly but must not block port 7199 |
| REST/HTTP | Scylla has a full REST API on port 10000 | No HTTP API | SCT's `StorageServiceClient` will not work against Cassandra — must not be called on oracle nodes |

**Implications for `BaseCassandraCluster.check_node_health()`:**

The health check sequence for a Cassandra node:
1. Wait for `systemctl is-active cassandra` to return `active`
2. Wait for TCP port 9042 to accept connections (with retry/timeout)
3. Run `nodetool status` and verify the node appears as `UN`
4. Optionally: attempt a CQL `SELECT now() FROM system.local` as a liveness check

The `check_node_health()` method in `BaseScyllaCluster` publishes `ClusterHealthValidatorEvent` instances. `BaseCassandraCluster` should follow the same event pattern to integrate with SCT's event monitoring pipeline.

**JMX port 7199:** Cassandra's nodetool communicates with the local Cassandra process via JMX on port 7199. This port must not be firewalled on oracle nodes. SCT's firewall disable step (which runs before service start in `BaseScyllaCluster.node_setup()`) already handles this by disabling the OS firewall entirely on cloud VMs, so no special handling is needed.

### G. Paths and Constants

New constants to add to `sdcm/paths.py`:

```python
# Cassandra
CASSANDRA_YAML_PATH = "/etc/cassandra/cassandra.yaml"
CASSANDRA_ENV_SH_PATH = "/etc/cassandra/cassandra-env.sh"
CASSANDRA_LOGBACK_PATH = "/etc/cassandra/logback.xml"
CASSANDRA_RACKDC_PATH = "/etc/cassandra/cassandra-rackdc.properties"
CASSANDRA_LOG_DIR = "/var/log/cassandra"
CASSANDRA_DATA_DIR = "/var/lib/cassandra/data"
CASSANDRA_COMMITLOG_DIR = "/var/lib/cassandra/commitlog"
```

These mirror the existing `SCYLLA_YAML_PATH = "/etc/scylla/scylla.yaml"` convention.

## Cassandra Image Strategy

### Docker Backend (Development / CI)

**Recommended: Official Apache Cassandra Docker image**

- Image: `cassandra:4.1` or `cassandra:5.0` from Docker Hub (official Apache Cassandra project images)
- Configuration via environment variables: `CASSANDRA_CLUSTER_NAME`, `CASSANDRA_SEEDS`, `CASSANDRA_DC`, `CASSANDRA_RACK`, `CASSANDRA_NUM_TOKENS`, `CASSANDRA_ENDPOINT_SNITCH`, `MAX_HEAP_SIZE`, `HEAP_NEWSIZE`
- The image entrypoint (`docker-entrypoint.sh`) performs sed substitutions on `/etc/cassandra/cassandra.yaml` using the env vars before starting Cassandra
- CQL port 9042 is exposed by default
- No installation step needed — just pull and run

**Docker env var to cassandra.yaml field mapping:**

| Docker env var | cassandra.yaml field | Notes |
|---|---|---|
| `CASSANDRA_CLUSTER_NAME` | `cluster_name` | Required; must match across all nodes |
| `CASSANDRA_SEEDS` | `seed_provider.seeds` | Comma-separated IPs of seed nodes |
| `CASSANDRA_LISTEN_ADDRESS` | `listen_address` | Set to container IP or `auto` |
| `CASSANDRA_BROADCAST_ADDRESS` | `broadcast_address` | Set for multi-host Docker setups |
| `CASSANDRA_RPC_ADDRESS` | `rpc_address` | Default `0.0.0.0` in the image |
| `CASSANDRA_DC` | `dc` in `cassandra-rackdc.properties` | Only when using `GossipingPropertyFileSnitch` |
| `CASSANDRA_RACK` | `rack` in `cassandra-rackdc.properties` | Only when using `GossipingPropertyFileSnitch` |
| `CASSANDRA_ENDPOINT_SNITCH` | `endpoint_snitch` | Default `SimpleSnitch` in the image |
| `CASSANDRA_NUM_TOKENS` | `num_tokens` | Default `16` in Cassandra 4.1 image |
| `MAX_HEAP_SIZE` | Injects into `cassandra-env.sh` | Set explicitly; e.g., `4G` for dev |
| `HEAP_NEWSIZE` | Injects into `cassandra-env.sh` | Set explicitly; e.g., `800M` for dev |

**Why this is the best starting point:**
- Fastest iteration cycle for development
- No cloud cost during development
- Identical Cassandra binary to what runs in cloud
- Can validate the full Gemini flow locally before running on cloud

### AWS Backend

**Recommended: Generic Ubuntu AMI + Cassandra apt install**

- Use the same base AMI as loaders (`ami_id_loader`) or a clean Ubuntu 22.04/24.04 AMI
- Install Cassandra in `node_setup()` via the official apt repository:

  ```bash
  # Cassandra 4.1 (JDK 11):
  sudo apt-get install -y openjdk-11-jdk
  curl -o /etc/apt/trusted.gpg.d/cassandra.asc \
      https://downloads.apache.org/cassandra/KEYS
  echo "deb https://debian.cassandra.apache.org 41x main" \
      | sudo tee /etc/apt/sources.list.d/cassandra.sources.list
  sudo apt-get update && sudo apt-get install -y cassandra

  # Cassandra 5.0 (JDK 17):
  sudo apt-get install -y openjdk-17-jdk
  echo "deb https://debian.cassandra.apache.org 50x main" \
      | sudo tee /etc/apt/sources.list.d/cassandra.sources.list
  sudo apt-get update && sudo apt-get install -y cassandra
  ```

- Pros: No custom AMI maintenance, always gets the latest patch version, reuses existing AMI infrastructure
- Cons: Slower node_setup (2-5 minutes for install), requires JDK installation

The oracle cluster is typically 1 node, so the 2-5 minute install overhead is negligible compared to test durations of 3-10 hours. No pre-baked images will be maintained.

### GCE Backend

Use a base Debian/Ubuntu GCE image (the same approach as AWS). Install Cassandra via apt during `node_setup()`. GCE image parameter: `gce_image_db_cassandra_oracle`.

### Azure Backend

Use a standard Ubuntu 22.04 Azure VM image. Install Cassandra during `node_setup()` via apt. Azure image parameter: `azure_image_db_cassandra_oracle`.

### Summary: Image Strategy Per Backend

| Backend | Image Source | Config Parameter | Install Method |
|---------|------------|-----------------|----------------|
| Docker | `cassandra:4.1` or `cassandra:5.0` (Docker Hub) | `docker_image_cassandra_oracle` | Pull and run (no install needed) |
| AWS | Ubuntu 22.04 base AMI | `ami_id_db_cassandra_oracle` (or reuse loader AMI) | apt install during `node_setup()` |
| GCE | Ubuntu/Debian base GCE image | `gce_image_db_cassandra_oracle` (or reuse loader image) | apt install during `node_setup()` |
| Azure | Ubuntu 22.04 Azure VM image | `azure_image_db_cassandra_oracle` (or reuse loader image) | apt install during `node_setup()` |

## Implementation Phases

### Phase 1: Docker Cassandra Cluster

**Objective:** Provision a standalone Cassandra cluster on the Docker backend. As a validation step, wire it into Gemini as an oracle to run a full multi-cluster test cycle locally and in CI.

**Dependencies:** None — this is the foundation.

**Implementation:**

**Step 1: Create `CassandraDockerCluster` in `sdcm/cluster_docker.py`**

The class extends the existing Docker cluster infrastructure and uses the official `cassandra:4.1` image. Configuration is done entirely through Docker environment variables (no template injection needed for Docker — the image entrypoint handles substitution):

```python
class CassandraDockerNode(DockerNode):
    """A Cassandra node running in a Docker container."""

    def wait_db_up(self, verbose=True, timeout=600):
        """Wait for CQL port to be ready."""
        self.wait_for_port(9042, timeout=timeout)
        # Additionally verify nodetool reports UN
        self.run("nodetool status", timeout=60)


class CassandraDockerCluster(BaseCassandraCluster, DockerCluster):
    """Cassandra cluster on Docker backend for development and CI."""

    node_container_image = "cassandra"
    node_container_name = "cassandra-oracle"

    def _cassandra_env_vars(self, node) -> dict:
        """Compute Docker env vars for the Cassandra container."""
        seeds = ",".join(n.ip_address for n in self.seed_nodes)
        max_heap, heap_new = compute_jvm_heap_mb(self._get_node_ram_mb(node))
        return {
            "CASSANDRA_CLUSTER_NAME": self.name,
            "CASSANDRA_SEEDS": seeds,
            "CASSANDRA_LISTEN_ADDRESS": node.ip_address,
            "CASSANDRA_BROADCAST_ADDRESS": node.ip_address,
            "CASSANDRA_RPC_ADDRESS": "0.0.0.0",
            "CASSANDRA_BROADCAST_RPC_ADDRESS": node.ip_address,
            "CASSANDRA_ENDPOINT_SNITCH": "SimpleSnitch",
            "CASSANDRA_NUM_TOKENS": str(self.params.get("cassandra_num_tokens") or 16),
            "MAX_HEAP_SIZE": f"{max_heap}M",
            "HEAP_NEWSIZE": f"{heap_new}M",
        }
```

The Docker image's entrypoint reads these env vars and performs sed substitution into `/etc/cassandra/cassandra.yaml` before launching the JVM. No separate `cassandra.yaml` template injection is needed for Docker.

**Step 2: Add `db_type: "mixed_cassandra"` to `get_cluster_docker()` in `sdcm/tester.py:1918`:**

```python
elif db_type == "mixed_cassandra":
    self.test_config.mixed_cluster(True)
    return CassandraDockerCluster(
        docker_image=self.params.get("docker_image_cassandra_oracle"),
        docker_image_tag=self.params.get("cassandra_oracle_version"),
        node_key_file=cluster.Setup.KEY_PATH,
        n_nodes=[self.params.get("n_test_oracle_db_nodes")],
        node_type="oracle-db",
        user_prefix=self.params.get("user_prefix") + "-cassandra-oracle",
        params=self.params,
        node_container_name="cassandra-oracle",
    )
```

**Step 3: Add config parameters to `sdcm/sct_config.py`:**

```python
docker_image_cassandra_oracle: str = SctField(
    description="Docker image name for Cassandra oracle cluster.",
    # Default: "cassandra" (official Docker Hub image)
)
cassandra_oracle_version: str = SctField(
    description="Version tag for the Cassandra oracle Docker image or apt package.",
    # Default: "4.1"
)
cassandra_num_tokens: int = SctField(
    description="num_tokens value to configure in cassandra.yaml for oracle nodes.",
    # Default: 16 (Cassandra 4.1+ default)
)
```

Add defaults in `defaults/test_default.yaml`:
```yaml
docker_image_cassandra_oracle: 'cassandra'
cassandra_oracle_version: '4.1'
cassandra_num_tokens: 16
```

**Step 4: Widen `GeminiStressThread` type annotation in `sdcm/gemini_thread.py:71`:**

```python
# Before:
oracle_cluster: ScyllaAWSCluster | CassandraAWSCluster | None

# After:
oracle_cluster: BaseCluster | None
```

The Gemini thread only calls `oracle_cluster.get_node_cql_ips()` (or equivalent) to build the `--oracle-cluster` flag. Any `BaseCluster` subclass that implements this method works.

**Step 5: Create test config `test-cases/gemini/gemini-basic-cassandra-oracle-docker.yaml`:**

```yaml
db_type: mixed_cassandra
n_db_nodes: 1
n_test_oracle_db_nodes: 1
cassandra_oracle_version: '4.1'
docker_image_cassandra_oracle: 'cassandra'
# Loaders use cassandra-stress or gemini binary
stress_cmd: '...'
```

**Note on sequencing:** Phases 1 and 2 should land in the same PR or immediately sequential PRs. If Phase 1 is merged and Phase 2 is delayed, the codebase will have a non-mixin `CassandraDockerCluster` as permanent state, which creates technical debt.

**Definition of Done:**
- [ ] `CassandraDockerCluster` starts a Cassandra container, waits for CQL, and is reachable on port 9042
- [ ] Gemini test runs with `--backend docker` using Cassandra oracle (`db_type: mixed_cassandra`)
- [ ] Gemini successfully compares Scylla (test) vs Cassandra (oracle) reads with no unexpected errors
- [ ] Unit test verifying Docker env var mapping (`_cassandra_env_vars()`) for single-node and multi-node clusters
- [ ] Integration test verifying `CassandraDockerCluster` starts and CQL port is reachable

---

### Phase 2: Base `BaseCassandraCluster` Mixin

**Objective:** Create a backend-agnostic `BaseCassandraCluster` mixin (analogous to `BaseScyllaCluster`) that encapsulates Cassandra-specific setup logic. This is the core abstraction that makes the Cassandra cluster a reusable, standalone SCT cluster type rather than a Gemini-specific construct.

**Dependencies:** Phase 1 (validates the Docker approach and establishes the env var pattern).

**Implementation:**

**Step 1: Decide the file location**

Options:
- Add `BaseCassandraCluster` to `sdcm/cluster.py` alongside `BaseScyllaCluster` — consistent with the existing pattern, one location for all base cluster types
- Create a new `sdcm/cluster_cassandra.py` — cleaner separation, avoids making `cluster.py` even larger

Recommended: `sdcm/cluster_cassandra.py`. The file will be moderate in size (500-700 lines) and is self-contained. It imports from `sdcm/cluster.py` but does not belong in it.

**Step 2: Implement `BaseCassandraCluster` mixin**

The mixin contains:

```python
class BaseCassandraCluster:
    """Backend-agnostic mixin for Cassandra cluster nodes.

    Provides Cassandra-specific node_setup(), configuration generation,
    and health check logic. Intended to be mixed in with a backend-specific
    cluster class (AWSCluster, GCECluster, DockerCluster).
    """

    # Subclasses must set this to the cassandra version being used
    cassandra_version: str = "4.1"

    def node_setup(self, node, verbose=False, timeout=3600):
        """Full Cassandra node setup sequence for cloud VM backends.

        This method is NOT called for Docker backend (Docker uses the image
        entrypoint instead). For Docker, override node_setup() to only wait
        for CQL readiness.

        Steps:
        1. Disable OS firewall (iptables/ufw) — same as ScyllaCluster
        2. Install JDK (version depends on cassandra_version)
        3. Add official Cassandra apt repository
        4. apt-get install cassandra (with retry)
        5. Stop cassandra service (it auto-starts after install)
        6. Render and upload cassandra.yaml template
        7. Render and upload cassandra-env.sh with computed heap sizes
        8. Set cassandra-rackdc.properties if using GossipingPropertyFileSnitch
        9. Restart cassandra service
        10. Wait for CQL readiness (port 9042 + nodetool status UN)
        """
        ...

    def _jdk_version(self) -> int:
        """Return required JDK major version for the configured Cassandra version."""
        major = int(self.cassandra_version.split(".")[0])
        return 17 if major >= 5 else 11

    def _install_jdk(self, node):
        """Install the correct JDK version for this Cassandra release."""
        jdk_version = self._jdk_version()
        node.remoter.run(
            f"sudo apt-get install -y openjdk-{jdk_version}-jdk",
            retry=3,
        )

    def _add_cassandra_apt_repo(self, node):
        """Add the official Apache Cassandra apt repository."""
        major = int(self.cassandra_version.split(".")[0])
        minor = int(self.cassandra_version.split(".")[1])
        dist_name = f"{major}{minor}x"  # e.g., "41x" for 4.1, "50x" for 5.0
        cmds = [
            "curl -s -o /etc/apt/trusted.gpg.d/cassandra.asc "
            "https://downloads.apache.org/cassandra/KEYS",
            f'echo "deb https://debian.cassandra.apache.org {dist_name} main" '
            f"| sudo tee /etc/apt/sources.list.d/cassandra.sources.list",
            "sudo apt-get update",
        ]
        for cmd in cmds:
            node.remoter.run(cmd, retry=3)

    def _install_cassandra(self, node):
        """Install Cassandra package with retry logic."""
        node.remoter.run("sudo apt-get install -y cassandra", retry=5, timeout=300)
        # Stop auto-started service so we can configure before first real start
        node.remoter.run("sudo systemctl stop cassandra", ignore_status=True)

    def generate_cassandra_yaml(self, node) -> str:
        """Render the cassandra.yaml template for the given node."""
        ...

    def _compute_heap_sizes(self, node) -> tuple[int, int]:
        """Compute JVM heap sizes from node's actual available RAM."""
        result = node.remoter.run("grep MemTotal /proc/meminfo")
        total_kb = int(result.stdout.split()[1])
        total_mb = total_kb // 1024
        return compute_jvm_heap_mb(total_mb)

    def _configure_cassandra_env(self, node):
        """Inject computed heap sizes into cassandra-env.sh."""
        max_heap, heap_new = self._compute_heap_sizes(node)
        # cassandra-env.sh has MAX_HEAP_SIZE and HEAP_NEWSIZE commented out;
        # sed uncomments and sets them
        node.remoter.run(
            f"sudo sed -i 's/#MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE=\"{max_heap}M\"/' "
            f"{CASSANDRA_ENV_SH_PATH}",
        )
        node.remoter.run(
            f"sudo sed -i 's/#HEAP_NEWSIZE=.*/HEAP_NEWSIZE=\"{heap_new}M\"/' "
            f"{CASSANDRA_ENV_SH_PATH}",
        )

    def _write_rackdc_properties(self, node):
        """Write cassandra-rackdc.properties for topology-aware snitches."""
        dc = node.dc_idx_name or "datacenter1"
        rack = node.rack_name or "rack1"
        content = f"dc={dc}\nrack={rack}\n"
        node.remoter.send_files(StringIO(content), CASSANDRA_RACKDC_PATH, sudo=True)

    def wait_for_init(self, node_list=None, verbose=False, timeout=1200, check_node_ssh_connect=True):
        """Wait for all Cassandra nodes to reach UN state."""
        ...

    def check_node_health(self, node, retries=3):
        """Verify a Cassandra node is healthy (UN state, CQL reachable)."""
        ...
```

**Step 3: Refactor `CassandraDockerCluster` from Phase 1**

After `BaseCassandraCluster` is implemented, `CassandraDockerCluster` inherits from it and overrides `node_setup()` to skip the apt install steps (the Docker image handles installation):

```python
class CassandraDockerCluster(BaseCassandraCluster, DockerCluster):
    def node_setup(self, node, verbose=False, timeout=600):
        # Docker: image is already installed; just wait for CQL readiness
        node.wait_db_up(timeout=timeout)
```

**Step 4: Define the Cassandra version strategy**

- Cassandra 4.1.x (LTS): JDK 11, apt repo `41x`, supported indefinitely
- Cassandra 5.0.x (latest): JDK 17, apt repo `50x`, newer features (SAI, trie memtables, vector type)
- Version controlled by `cassandra_oracle_version` config parameter
- JDK version auto-selected in `_jdk_version()` based on the major version number

**Definition of Done:**
- [ ] `BaseCassandraCluster` mixin is in `sdcm/cluster_cassandra.py` with all methods listed above
- [ ] `CassandraDockerCluster` refactored to inherit from `BaseCassandraCluster`
- [ ] Docker Gemini tests (from Phase 1) still pass after refactoring
- [ ] Unit tests for `generate_cassandra_yaml()` covering single-node, multi-node, and multi-DC cases
- [ ] Unit tests for `_compute_heap_sizes()` with various RAM sizes including the 31 GB cap edge case

---

### Phase 3: AWS Cassandra Cluster

**Objective:** Provision a standalone Cassandra cluster on AWS using the install-on-boot pattern. Validate via Gemini oracle (`db_type: mixed_cassandra`) to confirm the cluster integrates correctly with SCT's multi-cluster test flow.

**Dependencies:** Phase 2 (shared Cassandra setup logic in `BaseCassandraCluster`).

**Implementation:**

**Step 1: Create `CassandraAWSCluster` in `sdcm/cluster_aws.py`**

The legacy `CassandraAWSCluster` (Cassandra 2.1.15, DataStax AMI) is removed and replaced:

```python
class CassandraAWSCluster(BaseCassandraCluster, AWSCluster):
    """Cassandra cluster provisioned on AWS EC2.

    Installs Cassandra during node_setup() via the official apt repository.
    Uses a standard Ubuntu base AMI (no custom Cassandra-specific AMI needed).
    """

    def node_setup(self, node, verbose=False, timeout=3600):
        """Install and configure Cassandra on an AWS EC2 node.

        Full sequence:
        1. Wait for SSH to be ready (inherited from AWSCluster)
        2. Disable OS firewall (iptables-legacy flush + ufw disable)
        3. Refresh apt cache (sudo apt-get update, retry=3)
        4. Install JDK (openjdk-11-jdk for Cassandra 4.1, openjdk-17-jdk for 5.0)
        5. Add Apache Cassandra apt repository (curl KEYS + add sources.list entry)
        6. apt-get update (picks up Cassandra repo)
        7. apt-get install cassandra (retry=5 — apt is flaky on fresh VMs)
        8. sudo systemctl stop cassandra (auto-starts after install; stop for config)
        9. Render cassandra.yaml template and upload to /etc/cassandra/cassandra.yaml
        10. Configure cassandra-env.sh with computed heap sizes
        11. Write cassandra-rackdc.properties if using GossipingPropertyFileSnitch
        12. sudo systemctl enable cassandra && sudo systemctl start cassandra
        13. Wait for CQL port 9042 to accept connections (retry with 10s sleep, max 120 attempts)
        14. Verify nodetool status shows UN for this node
        """
        self._disable_firewall(node)
        self._install_jdk(node)
        self._add_cassandra_apt_repo(node)
        self._install_cassandra(node)
        self._configure_cassandra_yaml(node)
        self._configure_cassandra_env(node)
        self._configure_rackdc(node)
        self._start_cassandra_service(node)
        node.wait_db_up(timeout=timeout)
```

**Apt retry pattern:**

Apt on fresh AWS EC2 instances is frequently flaky during the first few minutes (unattended-upgrades holds the dpkg lock, DNS takes time to propagate, etc.). All apt commands use the `retry=N` parameter on `node.remoter.run()` with at least 3 retries for update and 5 for install.

**JDK version selection:**

```python
def _install_jdk(self, node):
    """Install the required JDK. Cassandra 4.x needs JDK 11; Cassandra 5.x needs JDK 17.

    On Ubuntu 22.04, openjdk-11-jdk and openjdk-17-jdk are both in the
    official apt repository and do not require additional repositories.
    """
    jdk_version = self._jdk_version()
    node.remoter.run(
        f"sudo apt-get install -y openjdk-{jdk_version}-jdk-headless",
        retry=3,
        timeout=300,
    )
    # Verify installation
    node.remoter.run(f"java -version 2>&1 | grep '{jdk_version}'")
```

**Step 2: Add `db_type: "mixed_cassandra"` to `get_cluster_aws()` in `sdcm/tester.py:1830`:**

```python
elif db_type == "mixed_cassandra":
    self.test_config.mixed_cluster(True)
    return CassandraAWSCluster(
        ec2_ami_id=self.params.get("ami_id_db_cassandra_oracle").split()
                   or self.params.get("ami_id_loader").split(),
        ec2_ami_username="ubuntu",
        ec2_instance_type=self.params.get("instance_type_db_oracle"),
        ec2_block_device_mappings=db_info["device_mappings"],
        n_nodes=[self.params.get("n_test_oracle_db_nodes")],
        node_type="oracle-db",
        cassandra_version=self.params.get("cassandra_oracle_version"),
        **(common_params | {"user_prefix": user_prefix + "-cassandra-oracle"}),
    )
```

**Step 3: New config parameters in `sdcm/sct_config.py`:**

```python
ami_id_db_cassandra_oracle: str = SctField(
    description="""
        AMI ID for Cassandra oracle cluster nodes on AWS.
        Defaults to empty string, which causes CassandraAWSCluster to fall back
        to the loader AMI (ami_id_loader) — a standard Ubuntu image.
        Specify an explicit AMI only if a custom image with pre-installed
        Cassandra is desired for faster node_setup().
        Example: ami_id_db_cassandra_oracle: 'ami-0abcd1234ef567890'
    """,
)
```

Add to `defaults/test_default.yaml`:
```yaml
ami_id_db_cassandra_oracle: ''
```

Note: `instance_type_db_oracle` is reused. Existing defaults (e.g., `i4i.8xlarge`) are NVMe-optimized, which is Scylla-centric. For Cassandra as oracle, a memory-optimized instance like `r6i.2xlarge` (64 GB RAM, 8 vCPU) is more appropriate. Gemini test configs for `mixed_cassandra` should override `instance_type_db_oracle` to a memory-optimized type.

**Step 4: Update KMS skip checks in `sdcm/tester.py`**

Three locations check `db_type == "mixed_scylla"` to skip KMS setup (approximately lines 981, 1038, 1097). Each must be updated to also skip for `"mixed_cassandra"`:

```python
# Before:
if db_type not in ("mixed_scylla", "scylla", ...):
    self._setup_kms(...)

# After:
if db_type not in ("mixed_scylla", "mixed_cassandra", "scylla", ...):
    self._setup_kms(...)
```

Without this, SCT attempts to set up KMS encryption on Cassandra oracle nodes (which have no Scylla KMS integration) and fails.

**Step 5: Remove the legacy `CassandraAWSCluster` (Cassandra 2.1.15) and the `db_type: "cassandra"` and `db_type: "mixed"` dead code paths**

The legacy class and the unreachable code paths (`sdcm/tester.py:1874-1878`) should be deleted in this phase. File a separate cleanup issue if the deletion is too risky to include in the same PR.

**Step 6: Create test config `test-cases/gemini/gemini-basic-cassandra-oracle-aws.yaml`**

```yaml
db_type: mixed_cassandra
n_db_nodes: 3
n_test_oracle_db_nodes: 1
cassandra_oracle_version: '4.1'
instance_type_db_oracle: 'r6i.2xlarge'  # memory-optimized; Cassandra is JVM-based
ami_id_db_cassandra_oracle: ''           # use loader AMI fallback
```

**Definition of Done:**
- [ ] Cassandra oracle cluster provisions on AWS from a base Ubuntu AMI
- [ ] Cassandra installs and starts during `node_setup()` without errors
- [ ] Gemini test runs on AWS with Cassandra oracle, comparing Scylla vs Cassandra
- [ ] Legacy `CassandraAWSCluster` (Cassandra 2.1.15) removed along with the unused `db_type: "cassandra"` and `db_type: "mixed"` code paths
- [ ] KMS skip check updated for `mixed_cassandra` path

---

### Phase 4: GCE and Azure Cassandra Clusters

**Objective:** Extend Cassandra cluster support to GCE and Azure backends, completing multi-cloud coverage for the new cluster type.

**Dependencies:** Phase 3 (proven install-on-boot pattern from AWS).

**Implementation:**

**GCE (`sdcm/cluster_gce.py`):**

```python
class CassandraGCECluster(BaseCassandraCluster, GCECluster):
    """Cassandra cluster provisioned on Google Compute Engine."""

    def node_setup(self, node, verbose=False, timeout=3600):
        """GCE node_setup follows the same sequence as CassandraAWSCluster.node_setup().
        GCE-specific differences:
        - Base image is a GCE image (Debian or Ubuntu), not an AMI
        - Package mirror is closer (GCE runs in Google's network) so apt is faster
        - Firewall: GCE VMs may use GCP firewall rules rather than iptables;
          the generic iptables flush still works but may be a no-op
        """
        # Implementation identical to CassandraAWSCluster.node_setup()
        # BaseCassandraCluster._disable_firewall() handles OS-level firewall;
        # GCE firewall rules are managed via Terraform/API separately
        ...
```

Add `mixed_cassandra` handling to `get_cluster_gce()` in `sdcm/tester.py:1549`:

```python
elif db_type == "mixed_cassandra":
    self.test_config.mixed_cluster(True)
    return CassandraGCECluster(
        gce_image=self.params.get("gce_image_db_cassandra_oracle")
                  or self.params.get("gce_image"),
        gce_image_username=self.params.get("gce_image_username"),
        gce_instance_type=self.params.get("instance_type_db_oracle"),
        n_nodes=[self.params.get("n_test_oracle_db_nodes")],
        node_type="oracle-db",
        cassandra_version=self.params.get("cassandra_oracle_version"),
        **(common_params | {"user_prefix": user_prefix + "-cassandra-oracle"}),
    )
```

New config parameters: `gce_image_db_cassandra_oracle` (defaults to empty string, falls back to `gce_image`).

**Azure (`sdcm/cluster_azure.py`):**

```python
class CassandraAzureCluster(BaseCassandraCluster, AzureCluster):
    """Cassandra cluster provisioned on Azure Virtual Machines."""
    ...
```

Add `mixed_cassandra` handling to `get_cluster_azure()` in `sdcm/tester.py:1660`. New config parameter: `azure_image_db_cassandra_oracle`.

**Test configs:**

- `test-cases/gemini/gemini-basic-cassandra-oracle-gce.yaml`
- `test-cases/gemini/gemini-basic-cassandra-oracle-azure.yaml`

**Definition of Done:**
- [ ] `CassandraGCECluster` provisions a Cassandra oracle on GCE from a base image
- [ ] `CassandraAzureCluster` provisions a Cassandra oracle on Azure from a base image
- [ ] Gemini tests pass on both GCE and Azure with `db_type: mixed_cassandra`
- [ ] New config parameters have defaults in `defaults/test_default.yaml`

---

### Phase 5: Cassandra Oracle Log Collection

**Objective:** Ensure Cassandra oracle node logs are collected during test teardown and uploaded to S3 alongside other cluster logs. Also fix the pre-existing gap where `mixed_scylla` oracle logs are not collected.

**Dependencies:** Phase 2 (`BaseCassandraCluster` mixin exists for `isinstance` checks).

**Implementation:**

**Step 1: Create `CassandraLogCollector` in `sdcm/logcollector.py`**

```python
class CassandraLogCollector(LogCollector):
    """Log collector for Cassandra oracle clusters.

    Collects Cassandra system logs, debug logs, GC logs, configuration files,
    and nodetool diagnostic output from each oracle node.
    """

    cluster_log_type = "cassandra-oracle-cluster"
    cluster_dir_prefix = "cassandra-oracle-cluster"

    log_entities = [
        # Primary operational log (equivalent to Scylla's journal)
        FileLog(
            name="cassandra_system.log",
            command="cat /var/log/cassandra/system.log",
            search_locally=True,
        ),
        # Verbose debug log (large — last 100K lines only to avoid huge archives)
        CommandLog(
            name="cassandra_debug.log",
            command="tail -n 100000 /var/log/cassandra/debug.log 2>/dev/null || echo 'debug.log not found'",
        ),
        # GC log (current rotation file)
        CommandLog(
            name="cassandra_gc.log",
            command="cat /var/log/cassandra/gc.log.0.current 2>/dev/null || "
                    "ls /var/log/cassandra/gc.log* 2>/dev/null || echo 'gc.log not found'",
        ),
        # Configuration files
        CommandLog(
            name="cassandra.yaml",
            command="cat /etc/cassandra/cassandra.yaml",
        ),
        CommandLog(
            name="cassandra-env.sh",
            command="cat /etc/cassandra/cassandra-env.sh",
        ),
        CommandLog(
            name="jvm-server.options",
            command=(
                "cat /etc/cassandra/jvm11-server.options 2>/dev/null || "
                "cat /etc/cassandra/jvm17-server.options 2>/dev/null || "
                "echo 'jvm*-server.options not found'"
            ),
        ),
        CommandLog(
            name="logback.xml",
            command="cat /etc/cassandra/logback.xml",
        ),
        CommandLog(
            name="cassandra-rackdc.properties",
            command="cat /etc/cassandra/cassandra-rackdc.properties 2>/dev/null || echo 'not found'",
        ),
        # Operational diagnostics
        CommandLog(
            name="nodetool_status",
            command="nodetool status 2>/dev/null || echo 'nodetool unavailable'",
        ),
        CommandLog(
            name="nodetool_info",
            command="nodetool info 2>/dev/null || echo 'nodetool unavailable'",
        ),
        CommandLog(
            name="nodetool_tpstats",
            command="nodetool tpstats 2>/dev/null || echo 'nodetool unavailable'",
        ),
        CommandLog(
            name="nodetool_compactionstats",
            command="nodetool compactionstats 2>/dev/null || echo 'nodetool unavailable'",
        ),
        CommandLog(
            name="nodetool_gcstats",
            command="nodetool gcstats 2>/dev/null || echo 'nodetool unavailable'",
        ),
        # System diagnostics
        CommandLog(name="cpu_info", command="cat /proc/cpuinfo"),
        CommandLog(name="mem_info", command="cat /proc/meminfo"),
        CommandLog(name="disk_info", command="df -h && lsblk"),
        CommandLog(name="dmesg.log", command="sudo dmesg -P"),
        CommandLog(
            name="systemctl_cassandra_status",
            command="sudo systemctl status cassandra --no-pager -l",
        ),
        CommandLog(
            name="cassandra_service_journal",
            command="sudo journalctl -u cassandra --no-pager -n 5000 2>/dev/null || echo 'journald not available'",
        ),
    ]
```

**Step 2: Docker log collection strategy**

For Docker backend, `CassandraLogCollector` uses a different approach because Cassandra writes to stdout (captured by Docker) in addition to files inside the container:

```python
class CassandraDockerLogCollector(CassandraLogCollector):
    """Log collector for Docker-based Cassandra oracle clusters.

    Docker Cassandra images write to stdout (docker logs) AND to
    /var/log/cassandra/ inside the container.
    This collector captures both sources.
    """

    log_entities = CassandraLogCollector.log_entities + [
        # Docker logs capture stdout (includes all log output in Docker mode)
        CommandLog(
            name="docker_logs",
            command="docker logs {container_id} 2>&1 | tail -n 50000",
        ),
    ]
```

The `container_id` substitution requires passing the container ID to the log entity. This can be done by overriding `collect_logs()` in the Docker variant to inject the container ID into the command string before executing.

**Step 3: Register `CassandraLogCollector` in `sdcm/tester.py:4239-4293`**

In `collect_logs()`, add a new entry to the `clusters` collection tuple:

```python
# Existing entries (unchanged):
("db_cluster", ScyllaLogCollector, "db_log"),
("loaders", LoaderLogCollector, "loader_log"),
("monitors", MonitorLogCollector, "monitor_log"),

# New entries:
# Cassandra oracle logs (mixed_cassandra flow)
("cs_db_cluster",
 CassandraLogCollector if isinstance(self.cs_db_cluster, BaseCassandraCluster) else None,
 "cassandra_oracle_log"),

# Scylla oracle logs — FIXES PRE-EXISTING BUG where mixed_scylla oracle logs are dropped
("cs_db_cluster",
 ScyllaLogCollector if isinstance(self.cs_db_cluster, BaseScyllaCluster) else None,
 "scylla_oracle_log"),
```

Add `"cassandra_oracle_log": ""` and `"scylla_oracle_log": ""` to the `logs_dict` initialization.

**Step 4: Handle the case where `cs_db_cluster` is None**

When `db_type` is not a multi-cluster type (e.g., plain `scylla`), `self.cs_db_cluster` is None. The `isinstance` checks handle this: `isinstance(None, BaseCassandraCluster)` returns False, so no log collection is attempted for oracle nodes when there is no oracle cluster.

**Definition of Done:**
- [ ] `CassandraLogCollector` collects `system.log`, GC log, `cassandra.yaml`, `cassandra-env.sh`, and nodetool outputs
- [ ] Cassandra oracle logs appear in the `collected_logs/` directory and are uploaded to S3
- [ ] Log collection works for both Docker (via `docker logs`) and cloud VM backends (via SSH + file cat)
- [ ] Log collection for `mixed_scylla` oracle is also fixed (pre-existing gap — no Scylla oracle logs collected today)
- [ ] Unit test verifying `CassandraLogCollector.log_entities` list covers the expected file paths
- [ ] Integration test (Docker): `system.log`, `cassandra.yaml`, and nodetool output are collected from a live Cassandra oracle container

---

### Phase 6: Cassandra Configuration Tuning and Hardening

**Objective:** Ensure the Cassandra oracle is stable and appropriately configured for its role as a Gemini oracle under multi-hour test runs.

**Dependencies:** Phase 3 (at minimum AWS working with basic configuration).

**Implementation:**

**Step 1: `cassandra.yaml` tuning for oracle role**

The oracle cluster template should set the following explicitly (not left to Cassandra defaults):

```yaml
# Explicitly set — oracle does not need aggressive tokenization
num_tokens: 16

# Commit log sync: periodic is safer for an oracle (batched is higher risk)
commitlog_sync: periodic
commitlog_sync_period_in_ms: 10000

# Disable features not needed for a consistency oracle
# (reduces risk of compatibility issues with older Scylla CQL behavior)
enable_materialized_views: false
enable_sasi_indexes: false

# Compaction throughput: reduce to minimize I/O interference with Gemini workload
compaction_throughput_mb_per_sec: 16

# Concurrent ops: reduce for oracle (it receives lower load than the test cluster)
concurrent_reads: 32
concurrent_writes: 32
concurrent_counter_writes: 16

# Snitch: SimpleSnitch for single-DC, GossipingPropertyFileSnitch for multi-DC
endpoint_snitch: SimpleSnitch  # override via template variable

# Heap space limits match computed heap (redundant but explicit)
# These are set via cassandra-env.sh, not cassandra.yaml in modern Cassandra
```

**Step 2: JVM options template**

Create `sdcm/templates/cassandra/jvm11-server.options.j2` (checked into repo):

```
# G1GC configuration for Cassandra 4.1 oracle
-XX:+UseG1GC
-XX:G1RSetUpdatingPauseTimePercent=5
-XX:MaxGCPauseMillis=300
-XX:InitiatingHeapOccupancyPercent=70
-XX:+ParallelRefProcEnabled
-XX:+UnlockDiagnosticVMOptions
-XX:G1SummarizeRSetStatsPeriod=1

# GC logging (written to /var/log/cassandra/gc.log.*)
-Xlog:gc=info,heap*=trace,age*=debug,safepoint=info,promotion*=trace:file=/var/log/cassandra/gc.log:time,uptime,pid,tid:filecount=10,filesize=10m

# Heap settings injected via cassandra-env.sh (MAX_HEAP_SIZE, HEAP_NEWSIZE)
# Do not set -Xms or -Xmx here; cassandra-env.sh takes precedence

# Off-heap (direct ByteBuffers for SSTable I/O)
-XX:MaxDirectMemorySize=${max_direct_memory}m
```

`max_direct_memory` is computed as `total_ram_mb - max_heap_mb - 2048` (reserve 2 GB for OS and JVM overhead), with a floor of 1024 MB.

**Step 3: Heap sizing integration**

`BaseCassandraCluster._configure_cassandra_env()` (defined in Phase 2) computes heap sizes and injects them into `cassandra-env.sh`. This is the canonical location for `MAX_HEAP_SIZE` and `HEAP_NEWSIZE` in Cassandra's configuration model.

Full formula for a production Cassandra oracle node:

```python
def compute_jvm_heap_mb(total_ram_mb: int) -> tuple[int, int]:
    """Compute JVM heap size for Cassandra oracle.

    Returns (MAX_HEAP_SIZE_MB, HEAP_NEWSIZE_MB).
    """
    # 50% of RAM for heap, capped at 31744 MB (31 GB = CompressedOops limit)
    max_heap_mb = min(int(total_ram_mb * 0.5), 31744)
    # 25% of heap for young generation (G1GC hint; G1 adjusts dynamically)
    heap_newsize_mb = int(max_heap_mb * 0.25)
    return max_heap_mb, heap_newsize_mb
```

Example results:
- 16 GB RAM: heap = 8192 MB, new gen = 2048 MB
- 64 GB RAM: heap = 31744 MB (capped), new gen = 7936 MB
- 768 GB RAM: heap = 31744 MB (capped), new gen = 7936 MB

**Step 4: Monitoring integration (best-effort)**

Cassandra exposes metrics via JMX on port 7199. The existing SCT monitoring stack (Prometheus + Grafana) does not natively scrape JMX. Options:

- Deploy `prometheus-jmx-exporter` as a Java agent alongside Cassandra: `-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=7070:/opt/jmx_exporter/cassandra.yaml`
- The JMX exporter config for Cassandra is available from the prometheus community (Cassandra mixin at `prometheus-community/helm-charts`)
- This is **optional for Phase 6** — the oracle cluster's health is monitored via `nodetool status` and CQL liveness checks. JMX metric scraping is a nice-to-have for deeper performance analysis

Needs Investigation: Can the existing `MonitorSet` in SCT be extended to scrape a JMX exporter endpoint alongside the existing Prometheus scrape targets? Or does a new `CassandraMonitorSet` need to be created? Defer to Phase 6 follow-up.

**Step 5: Graceful shutdown**

During SCT teardown, Cassandra nodes should be stopped before termination. Add `BaseCassandraCluster.destroy()` that runs `sudo systemctl stop cassandra` on each node before the underlying cloud resource is terminated. For Docker, `docker stop` handles this automatically.

**Definition of Done:**
- [ ] Cassandra oracle is stable through 10-hour Gemini test runs without OOM or excessive GC pauses
- [ ] JVM heap sizing formula is implemented in `compute_jvm_heap_mb()` and unit-tested
- [ ] `jvm11-server.options.j2` template is committed to `sdcm/templates/cassandra/`
- [ ] Monitoring integration status is documented (implemented or explicitly deferred with issue filed)

---

### Phase 7: Migrate Existing Gemini Tests

**Objective:** Transition existing Gemini test configurations from `mixed_scylla` to `mixed_cassandra` to give Gemini a genuine cross-implementation oracle.

**Dependencies:** Phase 5 (stable Cassandra oracle with log collection on all backends).

**Implementation:**

For each test config in `test-cases/gemini/`:
1. Create a parallel `*-cassandra-oracle.yaml` variant with `db_type: mixed_cassandra`
2. Run both variants in CI to compare results (false positives from CQL compatibility differences are expected initially; use `--cql-features` to restrict scope if needed)
3. Once validated, consider making `mixed_cassandra` the default for new Gemini tests (do not change existing tests — keep both variants running)

Update Gemini Jenkins pipelines in `jenkins-pipelines/` to include Cassandra oracle jobs alongside existing `mixed_scylla` jobs.

Update SCT documentation: `docs/gemini.md` (if it exists) to document `db_type: mixed_cassandra` and explain when to use Cassandra vs Scylla as oracle.

**Definition of Done:**
- [ ] All Gemini test configs have `mixed_cassandra` variants
- [ ] CI runs both `mixed_scylla` and `mixed_cassandra` Gemini tests
- [ ] A follow-up issue is filed to track `mixed_scylla` deprecation timeline if `mixed_cassandra` proves reliable

## Testing Requirements

### Unit Tests

**Configuration generation:**
- `BaseCassandraCluster.generate_cassandra_yaml()`: test that the rendered YAML contains the correct `listen_address`, `seeds`, `cluster_name`, and `endpoint_snitch` for various inputs (single-node, multi-node, public IP vs private IP mode)
- `compute_jvm_heap_mb()`: test boundary conditions — 8 GB RAM, 16 GB, 64 GB, 768 GB (should cap at 31744 MB)
- `BaseCassandraCluster._jdk_version()`: test that Cassandra 4.1 returns 11 and Cassandra 5.0 returns 17

**Docker env var mapping:**
- `CassandraDockerCluster._cassandra_env_vars()`: test that all required env vars are present and correctly derived from node IPs and cluster config

**Config parameter validation:**
- New SCT config params (`docker_image_cassandra_oracle`, `cassandra_oracle_version`, `ami_id_db_cassandra_oracle`, etc.) have correct types and defaults
- `GeminiStressThread` generates a valid `--oracle-cluster` flag when `oracle_cluster` is a `CassandraDockerCluster` instance

### Integration Tests

**Docker (primary validation gate):**
- Start a `CassandraDockerCluster` (1 node) and verify CQL port 9042 is reachable
- Start a full Gemini setup (Scylla test cluster + Cassandra oracle + Gemini binary) and run a 5-minute Gemini workload — verify no consistency errors and no crashes
- This test should run in CI on every PR that touches `sdcm/cluster_cassandra.py` or `sdcm/cluster_docker.py`

**Log collection (Docker):**
- Start a `CassandraDockerCluster`, run the `CassandraLogCollector`, verify that `cassandra.yaml` and nodetool output are collected
- Verify that collected files are non-empty and parseable

### Cloud Validation (Manual / CI)

- **AWS:** Provision a Cassandra oracle on AWS (base Ubuntu AMI), run `gemini-basic-3h` equivalent with `db_type: mixed_cassandra`. Verify the cluster provisions in under 10 minutes and passes the full test.
- **GCE/Azure:** Same validation after Phase 4.

### Log Collection Tests

- Unit test: `CassandraLogCollector.log_entities` contains entries for all expected file paths (`system.log`, `debug.log`, `cassandra.yaml`, `jvm*-server.options`, nodetool commands)
- Integration test (Docker): verified above
- Integration test (cloud): verify logs are uploaded to S3 as `cassandra-oracle-cluster-*.tar.zst` archives alongside the main cluster logs

### Regression Tests

- Existing `mixed_scylla` Gemini tests must continue to pass unchanged after every phase
- The `GeminiStressThread` type annotation change (Phase 1) must not break any existing test that passes a `ScyllaAWSCluster` as oracle
- Removing the legacy `CassandraAWSCluster` (Cassandra 2.1.15) in Phase 3 must be verified not to break any CI job (confirm no job uses `db_type: "cassandra"` or `db_type: "mixed"`)

## Success Criteria

1. SCT can **provision a standalone Apache Cassandra cluster** as a first-class cluster type on all supported backends (Docker, AWS, GCE, Azure)
2. Docker backend works for fast local development iteration — developers can run a Gemini test with Cassandra oracle on a laptop without cloud credentials
3. AWS backend provisions a Cassandra oracle from a base Ubuntu AMI in under 10 minutes, with no manual steps
4. GCE and Azure backends work for multi-cloud coverage (Phases 4+)
5. Gemini tests can use Cassandra as a genuine cross-implementation oracle (`db_type: mixed_cassandra`), with no regression to existing `mixed_scylla` tests
6. Cassandra oracle is stable for 10+ hour test runs with no OOM kills or JVM crashes
7. Configuration is simple — a single `db_type: mixed_cassandra` switch activates the Cassandra cluster as Gemini oracle, with sensible defaults for all other parameters
8. Cassandra cluster logs (`system.log`, `debug.log`, `gc.log`, config files, nodetool output) are collected during teardown and uploaded to S3
9. The legacy dead code (`CassandraAWSCluster` Cassandra 2.1.15, `db_type: "cassandra"`, `db_type: "mixed"`) is removed from the codebase

## Risk Mitigation

### Risk: Cassandra CQL compatibility differences

- **Likelihood:** High — Cassandra and Scylla have known CQL differences (lightweight transactions behavior, counter handling, secondary index semantics, certain system table schemas)
- **Impact:** Gemini reports false-positive consistency errors that are not real Scylla bugs
- **Mitigation:** This is actually the **desired outcome** for genuine incompatibilities — Gemini is meant to surface differences. However, for known benign differences, Gemini's `--cql-features` flag restricts the workload to a common CQL subset. Start with `--cql-features normal` and expand scope incrementally. Maintain a list of known differences in the test config comments.

### Risk: Cassandra performance insufficient for oracle role

- **Likelihood:** Low — Cassandra on a single large node handles typical Gemini oracle load well
- **Impact:** Oracle node becomes a bottleneck, slowing Gemini throughput and producing timeout errors
- **Mitigation:** Oracle nodes are single beefy instances (e.g., `r6i.2xlarge` with 64 GB RAM). The oracle sees lower load than the test cluster (RF=1 vs RF=3 for the test cluster, plus Gemini sends all reads to both clusters but only the writes are the real load). Gemini's `--concurrency` can be tuned down if the oracle falls behind. Monitor oracle response times via `nodetool tpstats` in the collected logs.

### Risk: JDK installation failures on cloud VMs

- **Likelihood:** Medium — apt operations on fresh VMs are known to be flaky due to unattended-upgrades lock contention, DNS propagation delays, and mirror instability
- **Impact:** `node_setup()` fails during apt install, causing the test to fail before it starts
- **Mitigation:** All apt operations use `retry=N` with exponential backoff (already standard in SCT). Pin specific JDK minor versions if a specific JDK release proves problematic. Add a pre-check that verifies `apt-get` is not locked before running install commands.

### Risk: Legacy `CassandraAWSCluster` naming conflicts

- **Likelihood:** Low — the class name is reused but the old class is being removed
- **Impact:** Naming conflict if removal is deferred; confusion in code history
- **Mitigation:** Remove the legacy `CassandraAWSCluster` class entirely in Phase 3 — it has not been used for years. The `db_type: "cassandra"` and `db_type: "mixed"` paths should be cleaned up at the same time. If removal is deferred for any reason, rename the legacy class to `_LegacyCassandraAWSCluster` to make it clearly obsolete.

### Risk: Monitoring gap for Cassandra oracle nodes

- **Likelihood:** High — the monitoring gap is certain (JMX vs Prometheus)
- **Impact:** No visibility into Cassandra oracle health during test runs beyond manual log inspection
- **Mitigation:** Basic health monitoring via `nodetool status` and CQL liveness checks provides sufficient operational visibility for Phase 1-3. JMX-to-Prometheus exporter integration is planned for Phase 6. For 10-hour test runs, the collected logs (particularly `nodetool tpstats` and `gc.log`) provide post-hoc visibility into any oracle performance issues.

### Risk: Cassandra 5.0 configuration format changes

- **Likelihood:** High for teams wanting to run Cassandra 5.0 as oracle
- **Impact:** `cassandra.yaml` configuration has a parallel `cassandra_latest.yaml` in Cassandra 5.0 that contains different defaults and new parameters (SAI indexes, trie memtable, unified compaction strategy, vector data type, Azure snitch). The JDK requirement changes from 11-compatible to 17-only. The `jvm11-server.options` file is absent; `jvm17-server.options` is the only JVM options file.
- **Mitigation:** Target Cassandra 4.1 first (Phase 1-3). Treat Cassandra 5.0 support as a separate iteration, gated by a `cassandra_oracle_version: '5.0'` config parameter. The `BaseCassandraCluster._jdk_version()` method already handles the JDK version selection. The YAML template difference requires a version-conditional template selection: if `cassandra_version.startswith("5.")`, use `cassandra-oracle-5.0.yaml.j2` (based on 5.0 defaults) rather than `cassandra-oracle.yaml.j2` (based on 4.1 defaults). The 5.0 template must account for the fact that `cassandra.yaml` in 5.0 no longer includes some 4.1 parameters that were deprecated (e.g., `enable_user_defined_functions`).

### Risk: Multi-node Cassandra oracle bootstrap ordering

- **Likelihood:** Medium for multi-node oracle clusters
- **Impact:** If multiple nodes attempt to bootstrap simultaneously and the seed node has not yet fully initialized, bootstrap can fail with "Cannot add to gossip ring" errors
- **Mitigation:** `BaseCassandraCluster.node_setup()` handles multi-node clusters by waiting for the first node (the seed) to fully reach `UN` state before invoking `node_setup()` on subsequent nodes. This matches the Cassandra bootstrap protocol: seeds must be running before non-seeds join. SCT's `wait_for_init()` coordinates this sequencing by calling `node_setup()` on seed nodes first, waiting for CQL readiness, then starting non-seed nodes.

### Risk: Docker networking for multi-node Cassandra oracle

- **Likelihood:** Medium for multi-node Docker setups
- **Impact:** Docker containers on the default bridge network may not reach each other via hostname, causing Cassandra gossip failures
- **Mitigation:** Use a custom Docker network (`docker network create cassandra-oracle`) and connect all oracle containers to it. Seed addresses use the Docker container IP (not hostname) to avoid DNS resolution issues. For Phase 1, single-node Docker oracle sidesteps this issue entirely.

## Open Questions

1. **Cassandra version policy:** Support only Cassandra 4.1 (simpler, stable LTS) or also 5.0 from the start? Recommendation: 4.1 first, 5.0 as a separate iteration gated by `cassandra_oracle_version: '5.0'` and a dedicated template.

2. **Multi-node oracle:** Current oracle is single-node (RF=1). A multi-node Cassandra oracle would increase fault tolerance and allow testing RF>1 scenarios, but adds complexity (bootstrap ordering, seed coordination). Defer multi-node oracle to a follow-up plan unless a specific test requirement demands it.

3. **Cassandra authentication:** Should the oracle run with `AllowAllAuthenticator` (simpler, no credential management) or `PasswordAuthenticator` (matches test cluster if auth is enabled)? Initial implementation: `AllowAllAuthenticator`. If Gemini tests with authentication are needed, the `cassandra_oracle_authenticator` config parameter controls this. The Gemini binary supports `--oracle-username` and `--oracle-password` flags.

4. **JMX exporter deployment:** Should SCT deploy the Prometheus JMX exporter as a Java agent alongside Cassandra, or leave oracle metrics visibility as a manual post-hoc log inspection task? This affects Phase 6 scope. If the JMX exporter is deployed, it requires SCT to download the exporter JAR during `node_setup()`, which adds to the install time and introduces an external dependency.

5. **`BaseCassandraCluster` location:** `sdcm/cluster_cassandra.py` (new file, cleaner separation) vs `sdcm/cluster.py` (consistent with `BaseScyllaCluster` location). Recommendation: `sdcm/cluster_cassandra.py` — the file will be large enough to warrant separation, and placing it in `cluster.py` would make an already large file even larger.

6. **Template format:** Jinja2 templates (already used elsewhere in SCT for other config generation) vs Python `string.Template` (standard library, no dependency). Recommendation: `string.Template` for the cassandra.yaml template — the substitution requirements are simple (flat key-value substitution, no loops or conditionals), and avoiding a Jinja2 dependency for this use case keeps the implementation lightweight.
