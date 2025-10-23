# Performance Tests

## HDR investigate utility

The hdr_investigate utility is essential for performance analysis because it allows users to scan HDR (High Dynamic Range) histogram files
with fine-grained time intervals, rather than only looking at overall or coarse-grained metrics.
By analyzing latency metrics (such as P99) in smaller intervals, the tool helps pinpoint the exact time windows where latency spikes occur.
This makes it possible to correlate these spikes with specific events or Scylla processes, enabling users to identify which Scylla process
or operation is causing performance problems.
This targeted approach greatly improves the efficiency and accuracy of performance troubleshooting in distributed database environments.

Key features:
- Supports multiple stress tools and operations (READ/WRITE).
- Can fetch HDR files from Argus by test ID or use a local folder.
- Allows specifying the time window and scan interval for analysis.
- Reports intervals where P99 latency exceeds a user-defined threshold.

Usage example:

```bash
hydra hdr-investigate \
  --stress-operation READ \
  --throttled-load true \
  --test-id 8732ecb1-7e1f-44e7-b109-6d789b15f4b5 \
  --start-time "2025-09-14\ 20:45:18" \
  --duration-from-start-min 30
```

Main options:
- --test-id: Test run identifier (fetches logs from Argus if --hdr-folder is not provided).
- --stress-tool: Name of the stress tool (cassandra-stress, scylla-bench, or latte) (default: cassandra-stress).
- --stress-operation: Operation type (READ or WRITE).
- --throttled-load: Whether the load was throttled (True or False).
- --start-time: Start time for analysis (format: YYYY-MM-DD\ HH:MM:SS).
- --duration-from-start-min: Duration in minutes to analyze from the start time.
- --error-threshold-ms: P99 latency threshold in milliseconds (default: 10).
- --hdr-summary-interval-sec: Interval in seconds for summary scan (default: 600).
- --hdr-folder: Path to local folder with HDR files (optional).

This utility is useful for performance engineers and developers investigating latency issues in distributed database clusters.
