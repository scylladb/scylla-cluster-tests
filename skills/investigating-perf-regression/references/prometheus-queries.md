# Prometheus Queries for Performance Regression Investigation

Exact queries for the Prometheus HTTP API used during performance regression investigations. All queries use the `query_range` endpoint.

## API Endpoint

```
POST http://<MONITOR_HOST>:9090/api/v1/query_range
```

Parameters:
- `query` -- PromQL expression
- `start` -- ISO 8601 timestamp (e.g., `2026-04-26T05:53:28Z`)
- `end` -- ISO 8601 timestamp
- `step` -- Resolution (use `60s` for overview, `20s` for detailed timeline)

## curl Template

```bash
curl -s --max-time 15 'http://<HOST>:9090/api/v1/query_range' \
  --data-urlencode 'query=<PROMQL>' \
  --data-urlencode 'start=<ISO_START>' \
  --data-urlencode 'end=<ISO_END>' \
  --data-urlencode 'step=60s'
```

## Python Parsing Script

Use this pattern to parse query results into human-readable summaries:

```python
import json, sys
data = json.load(sys.stdin)
if data['status'] == 'success':
    for r in data['data']['result']:
        inst = r['metric'].get('instance', '?')
        vals = [float(v[1]) for v in r['values'] if v[1] != 'NaN']
        if vals:
            print(f'{inst}: min={min(vals):.1f} max={max(vals):.1f} avg={sum(vals)/len(vals):.1f}')
```

For timestamped spike detection:

```python
import json, sys
from datetime import datetime, timezone
data = json.load(sys.stdin)
if data['status'] == 'success':
    for r in data['data']['result']:
        inst = r['metric'].get('instance', '?')
        vals = [(v[0], float(v[1])) for v in r['values'] if v[1] != 'NaN' and float(v[1]) > THRESHOLD]
        for ts, v in vals:
            t = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M:%S')
            print(f'  {t}: {v:.0f}')
```

## Primary Metrics

### 1. P99 Read Latency (Server-Side)

The `rlatencyp99` metric is a pre-computed summary created by Prometheus relabel rules from `scylla_storage_proxy_coordinator_read_latency_summary`. Values are in **microseconds**.

```
max(rlatencyp99) by (instance)
```

To convert to milliseconds in the parsing script, divide by 1000.

For per-shard breakdown (more detail but more data):

```
rlatencyp99{instance!=""}
```

### 2. Queued Reads

The most important indicator of read admission throttling. Values represent the current queue depth (gauge).

```
max(scylla_database_queued_reads) by (instance)
```

**Thresholds:** 0-10 normal, 10-50 mild pressure, 50+ significant, 500+ severe.

### 3. Cache Row Evictions

Rate of cache row evictions per second. High rates indicate memory pressure forcing the cache to shrink.

```
sum(rate(scylla_cache_row_evictions[1m])) by (instance)
```

### 4. Reactor Utilization

Average CPU utilization across all shards as a percentage (0-100).

```
avg(scylla_reactor_utilization) by (instance)
```

### 5. Cache Hit Rate

Fraction of reads served from cache. Should be very close to 1.0 (100%) for read-only workloads.

```
1 - sum(rate(scylla_cache_row_misses[1m])) by (instance) / sum(rate(scylla_cache_reads[1m])) by (instance)
```

## Secondary Metrics

### 6. LSA Memory Compaction

Rate of log-structured allocator memory compaction in bytes/sec. High rates indicate memory fragmentation pressure.

```
sum(rate(scylla_lsa_memory_compacted[1m])) by (instance)
```

### 7. IO Queue Delays

Cumulative I/O delay per I/O class. The rate gives seconds-of-delay-per-second-of-wall-time.

```
max(rate(scylla_io_queue_total_delay_sec[1m])) by (instance, class)
```

### 8. SSTable Read Queue Overloads

Count of times the SSTable read queue was overloaded. Any non-zero rate is a problem.

```
sum(rate(scylla_database_sstable_read_queue_overloads[1m])) by (instance)
```

### 9. Disk Queue Length

Current number of I/O operations queued at the disk level per I/O class.

```
max(scylla_io_queue_disk_queue_length) by (instance, class)
```

### 10. TCP Retransmissions

Rate of TCP segment retransmissions. High rates indicate network instability.

```
rate(node_netstat_Tcp_RetransSegs[1m])
```

### 11. Read Timeouts

Counter of coordinator-level read timeouts. Should be 0 for healthy runs.

```
sum(scylla_storage_proxy_coordinator_read_timeouts) by (instance)
```

## Admission Control & Memory Metrics

### 12. Active Reads

Current number of reads being actively processed (not queued). When this hits the internal limit (~100/shard), additional reads are queued.

```
max(scylla_database_active_reads) by (instance)
```

**Interpretation:** Compare offending nodes (hitting 93-94) vs healthy nodes (3-22). If a node is consistently at the limit, it's saturated.

### 13. Transport Requests Blocked by Memory

Counter of CQL requests that were blocked due to transport memory pressure. Zero means the bottleneck is NOT at the CQL connection layer.

```
max(scylla_transport_requests_blocked_memory) by (instance)
```

### 14. Memory Allocated vs Total (per shard)

Shows memory headroom. If allocated/total > 99%, the node has no buffer for load spikes.

```
scylla_memory_allocated_memory{shard="0"}
scylla_memory_total_memory{shard="0"}
```

### 15. Transport Connections Blocked/Shed

Indicates if connections are being rejected or throttled at the CQL layer.

```
max(scylla_transport_connections_blocked) by (instance)
max(scylla_transport_connections_shed) by (instance)
```

### 16. Compaction Manager Active Compactions

Number of active compactions during the step. Non-zero during a performance step can steal IO/CPU.

```
sum(scylla_compaction_manager_compactions) by (instance)
```

## Metric Discovery

If a metric name doesn't return data, list all available metric names:

```bash
curl -s 'http://<HOST>:9090/api/v1/label/__name__/values' | \
  python3 -c "import json,sys; [print(n) for n in json.load(sys.stdin)['data'] if 'keyword' in n]"
```

Replace `keyword` with relevant terms: `timeout`, `queue`, `stall`, `compaction`, `cache`, `latency`, `evict`.
