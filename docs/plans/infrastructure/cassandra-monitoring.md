---
status: draft
domain: infrastructure
created: 2026-03-24
last_updated: 2026-03-24
owner: null
---

# Cassandra Monitoring with Prometheus Exporter

## 1. Problem Statement

Cassandra clusters provisioned by SCT have no metrics collection. When running longevity,
performance, or preload tests against Cassandra (as oracle or standalone), operators cannot
observe JVM heap usage, read/write latencies, compaction progress, or cache hit rates.
Scylla clusters automatically get full monitoring via scylla-monitoring + node_exporter,
but Cassandra nodes export zero Prometheus metrics today.

**Pain points:**
- No visibility into Cassandra health during multi-hour preload tests (500GB–5TB)
- Cannot diagnose Cassandra-side bottlenecks (GC pauses, memtable flushes, compaction backlog)
- No historical metrics for post-test analysis via Grafana
- Cassandra oracle cluster in Gemini tests is a black box

## 2. Current State

**Monitoring stack** (`sdcm/cluster.py:6900–7220`, `sdcm/monitorstack/__init__.py`):
- Prometheus scrape config generated in `configure_scylla_monitoring()` (cluster.py:6900)
- Targets added via `scrape_configs.append(dict(job_name=..., static_configs=[...]))`
- Target YAML files stored in `{monitoring_conf_dir}/<service>_servers.yml`
- Grafana dashboards loaded from scylla-monitoring repo + SCT's `data_dir/`

**Node exporter** (`sdcm/node_exporter_setup.py`):
- `NodeExporterSetup.install(node)` — downloads binary, creates systemd service
- Pattern: download tar → extract → create service user → systemd unit → enable+start
- Port 9100, scraped by Prometheus as `node_exporter` job

**Cassandra node setup** (`sdcm/cluster_cassandra.py:326–343`):
- `BaseCassandraCluster.node_setup()`: install JDK → apt install cassandra → configure yaml → start
- No exporter installation step exists
- Cassandra JVM args configured in `_configure_cassandra_env()` (cassandra-env.sh)

**Cassandra metrics today:** Zero — no exporter, no scrape targets, no dashboards.

## 3. Goals

1. Install a Cassandra Prometheus exporter on every Cassandra node during `node_setup()`
2. Add Cassandra exporter scrape targets to Prometheus automatically
3. Provide at least one Grafana dashboard showing: latencies, throughput, JVM heap, compaction, cache
4. Make exporter installation configurable (on/off) via SCT config parameter
5. Support both AWS and Docker backends

## 4. Implementation Phases

### Phase 1: Exporter Installation (Importance: MUST)

**Exporter choice: Criteo cassandra_exporter (standalone)**

Rationale: Standalone daemon is simpler to install/debug than a JVM agent, doesn't
require modifying Cassandra JVM args (which risks breaking startup), and can be
restarted independently. Port 8080 (configurable). Active maintenance (v2.3.8, 2026).

Alternative considered: Instaclustr cassandra-exporter (JVM agent, port 9500) — faster
metrics but requires JVM restart and tighter coupling with Cassandra version. Can
switch later if performance matters.

**Steps:**
1. Create `sdcm/cassandra_exporter_setup.py` following `node_exporter_setup.py` pattern
   - Download JAR from GitHub releases
   - Create config YAML (JMX host, port, metrics filters)
   - Create systemd service unit
   - Enable and start service
2. Add `install_cassandra_exporter` config param to `sdcm/sct_config.py` (default: `true`)
3. Call `CassandraExporterSetup.install(node)` at end of `BaseCassandraCluster.node_setup()`
4. For Docker backend: either bundle in image or install in `CassandraDockerCluster.node_setup()`

**Definition of Done:**
- [ ] Exporter runs on Cassandra node after `node_setup()`, metrics visible at `:8080/metrics`
- [ ] Config param `install_cassandra_exporter` controls installation
- [ ] Unit test for setup class

### Phase 2: Prometheus Integration (Importance: MUST)

**Steps:**
1. In `configure_scylla_monitoring()` (cluster.py:6900), detect Cassandra clusters and add scrape config:
   ```python
   cassandra_targets = [f"{n.ip_address}:8080" for n in cassandra_nodes]
   scrape_configs.append(dict(
       job_name="cassandra_exporter",
       honor_labels=True,
       static_configs=[dict(targets=cassandra_targets)],
   ))
   ```
2. Add `cassandra_exporter_servers.yml` target file generation in `reconfigure_scylla_monitoring()`
3. Handle mixed clusters (Scylla + Cassandra oracle) — both exporters active

**Definition of Done:**
- [ ] Prometheus scrapes Cassandra metrics when Cassandra cluster exists
- [ ] `cassandra_exporter_servers.yml` generated with correct targets
- [ ] Mixed cluster test (Gemini) has both Scylla and Cassandra targets

### Phase 3: Grafana Dashboard (Importance: SHOULD)

**Steps:**
1. Download pre-built dashboard from Grafana Labs (ID 14070 — "Cassandra Dashboard by ORAMAD")
   - Covers: OS metrics, JVM heap, cache, CQL, compaction, latency, keyspace sizes
   - Tested with Cassandra 4.x
   - May need metric name adjustments for Criteo exporter format
2. If metric names don't match, create a custom dashboard JSON in `data_dir/cassandra-monitoring-dashboard.json`
   with panels for: read/write latency, throughput, JVM heap, compaction pending, cache hit rate
3. Load dashboard in `add_sct_dashboards_to_grafana()` or via monitoring stack setup
4. Add dashboard to SCT's `restore_sct_dashboards()` flow

**Definition of Done:**
- [ ] Grafana shows Cassandra metrics dashboard after test setUp
- [ ] Dashboard has panels for: latency, throughput, JVM, compaction, cache
- [ ] Dashboard works for both standalone and oracle Cassandra clusters

### Phase 4: Documentation (Importance: SHOULD)

**Steps:**
1. Add `install_cassandra_exporter` to `docs/configuration_options.md` (auto-generated)
2. Document monitoring setup for Cassandra in implementation plan progress
3. Update `docs/plans/infrastructure/cassandra-cluster-support.md` progress

**Definition of Done:**
- [ ] Config option documented
- [ ] `uv run sct.py pre-commit` passes (regenerates docs)

## 5. Testing Requirements

**Unit tests:**
- Test `CassandraExporterSetup.install()` with `FakeRemoter` (mock download + systemd commands)
- Test Prometheus config generation includes cassandra_exporter job when Cassandra cluster exists
- Test config generation excludes cassandra_exporter when `install_cassandra_exporter: false`

**Integration tests (manual):**
- Docker test: verify exporter metrics at `localhost:8080/metrics`
- AWS test: verify Prometheus scrapes Cassandra targets
- Verify Grafana dashboard loads and shows data

**Automated:**
- `uv run sct.py unit-tests -t test_cluster_cassandra.py`
- `uv run sct.py pre-commit`

## 6. Success Criteria

- [ ] All Phase 1–2 DoD items verified
- [ ] Cassandra metrics visible in Prometheus during AWS provision test
- [ ] At least one Grafana dashboard shows Cassandra data
- [ ] No regression in existing Scylla monitoring

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Criteo exporter metric names don't match dashboard | Medium | Medium | Test with 14070 dashboard first; create custom dashboard if needed |
| Exporter fails to start (JMX connection issue) | Low | Low | Exporter install is best-effort; `ignore_status=True` on service start |
| Exporter adds overhead to Cassandra node | Low | Low | Standalone daemon uses separate process; minimal CPU impact |
| Docker backend can't reach JMX port | Medium | Medium | For Docker, expose JMX port or use agent mode instead |
| Dashboard ID 14070 removed from Grafana Labs | Low | Low | Download JSON and store in `data_dir/` as fallback |
