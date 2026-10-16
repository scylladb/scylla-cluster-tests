# Mini-Plan: Report Topology Barrier Stalls as Events (SCT-1188)

**Date:** 2026-10-08
**Estimated LOC:** ~120
**Related Issue:** SCT-1188

## Problem

Scylla logs a warning with an inline backtrace when a node holds a topology version past expiry
and delays the topology barrier:

```
2026-05-12T12:15:59.522 longevity-mv-si-4d-2026-2-db-node-1317bbfa-6 !WARNING | scylla[6531] [shard 13:sl:d] token_metadata - topology version 1372 held for 4.567 [s] past expiry, released at: 0x194b7ff 0x3526010 ... /opt/scylladb/libreloc/libc.so.6+0x72463 /opt/scylladb/libreloc/libc.so.6+0xf55eb
```

Current master builds (2026.4.0~dev) add a `(BuildId: <hex>)` suffix to the line. Sometimes an
extra raw address appears after the libc frames.

Observed in Argus run `554235c7-9cdf-4bc8-acb6-6e07acec84a7` (longevity-mv-si-4days-streaming, 6
nodes, 28h): 352 occurrences across all 6 nodes, up to 126 per hour in bursts. All were
`!WARNING` lines. Hold times: 146 between 1 and 5 s, 94 between 5 and 30 s, 6 between 30 and
120 s, and 106 at 120 s or more (max 300 s).

SCT drops these lines. They match the generic `WARNING` subtype, which has `SUPPRESS`
severity, so no event is published and the backtrace is not decoded. Reactor stalls and
oversized allocations already go through the event and decoding path. Topology barrier stalls
need the same treatment.

## Approach

Flow: log line → new `TOPOLOGY_BARRIER_STALL` event matched → inline backtrace after
`released at:` collected → event queued for decoding → published with decoded backtrace.

- **New event subtype.** Add `DatabaseLogEvent.TOPOLOGY_BARRIER_STALL` in
  `sdcm/sct_events/database.py`. It matches `token_metadata - topology version \d+ held for`.
- **Ordering.** The new subtype must come before `WARNING` in `SYSTEM_ERROR_EVENTS` because
  the first matching pattern wins. Otherwise the `SUPPRESS` rule drops the line. Use the same
  placement and comment as `OVERSIZED_ALLOCATION`.
- **Severity.** Severity depends on the hold time, the same way as `ReactorStalledMixin`. Add
  the constant `TOLERABLE_TOPOLOGY_BARRIER_STALL = 10` (seconds) next to
  `TOLERABLE_REACTOR_STALL`. A new mixin reads `held for N [s]` from the line. A stall at or
  above the constant is `ERROR`. A shorter one keeps the default `WARNING`. If the duration
  can't be parsed, the event keeps the default and a warning is logged. The max severity in
  `defaults/severities.yaml` is `ERROR`; the registry refuses any event type that has no entry
  there. Tests can lower it with `max_events_severities`. In the run above, 114 of the 352
  stalls would have been `ERROR`.
- **Inline backtrace marker.** `DbLogReader` treats only `backtrace:` and `report: at` as
  markers for one-line backtraces. Add `released at:` as a third marker. The existing token
  filter (`0x…` or `scylladb/lib…`) keeps both address forms and the trailing raw address. It
  drops the `(BuildId: …)` suffix.
- **Decoding is verified.** Two real stall lines from the run above were tokenized the same way
  `DbLogReader` does, with `released at:` added as a marker. Both decoded fully through
  `backtrace.scylladb.com` in about 3 s each. The decoded trace shows where the topology version
  was released, for example `repair_meta::prepare_sstables_for_incremental_repair` or
  `service::abstract_read_executor`. The build-id from the `starting ...` line matches the
  `BuildId` suffix, so no new build-id handling is needed. The 352 stalls in the run had only 16
  distinct raw backtraces, and the decode cache is keyed by raw backtrace. So decoding them all
  would take about 16 service calls.
- **Decoding control.** `backtrace_stall_decoding=false` also skips decoding for
  `TOPOLOGY_BARRIER_STALL`, the same as `REACTOR_STALLED`, because the option covers stalls.
  Update the option's description and regenerate the config docs.
  `backtrace_decoding_disable_regex` already covers the new type by name.

- **Config gate.** New boolean `topology_barrier_stall_events`, default `false` in
  `defaults/test_default.yaml`. When false, `get_system_error_events_patterns()` omits the
  `TOPOLOGY_BARRIER_STALL` pattern, so the line falls through to the `WARNING` suppress rule as
  before. `Cluster` passes the filtered list to `DbLogReader`. Test cases run by the
  tier1 trigger matrix (`configurations/triggers/tier1.yaml`) set it to `true`; all others keep the default.

Out of scope: an `event_counter.py` stat handler, an `issue_by_keyword.yaml` entry, and new
config parameters.

## Files to Modify

- `sdcm/sct_events/database.py`: `TOLERABLE_TOPOLOGY_BARRIER_STALL` constant, the
  duration-based severity mixin, the new subtype with its regex, and its place in
  `SYSTEM_ERROR_EVENTS` before `WARNING`
- `unit_tests/unit/test_sct_events_database.py`: severity test at the threshold and just below
  it, like the existing `REACTOR_STALLED` test
- `defaults/severities.yaml`: `DatabaseLogEvent.TOPOLOGY_BARRIER_STALL: ERROR`
- `sdcm/db_log_reader.py`: `DbLogReader` accepts `released at:` as an inline backtrace marker;
  the `backtrace_stall_decoding` skip covers the new type
- `sdcm/sct_config/mixins/monitoring.py`: new `topology_barrier_stall_events` option; update the
  `backtrace_stall_decoding` description
- `defaults/test_default.yaml`: `topology_barrier_stall_events: false`
- `sdcm/sct_events/database.py`: `get_system_error_events_patterns()` drops the stall pattern
  unless the option is enabled
- `sdcm/cluster.py`: `start_db_log_reader_thread` passes the filtered patterns
- `test-cases/**` run by the tier1 trigger matrix: `topology_barrier_stall_events: true`
- `docs/configuration_options/monitoring-events-and-reporting.md`: regenerated
- `unit_tests/test_data/system_topology_barrier_stall.log` (new): real lines from the Argus run,
  one with the `(BuildId: …)` suffix and one with a trailing raw address, plus a few neutral lines
- `unit_tests/unit/test_cluster.py`: new test next to `test_search_one_line_backtraces`

## Verification

- [ ] New test: reading the fixture publishes one `TOPOLOGY_BARRIER_STALL` event per stall
      line. Each event's `raw_backtrace` contains the line's addresses and libc frames and
      does not contain `BuildId`. A published event also shows that the line is matched before
      the `WARNING` suppress rule.
- [ ] A stall held for `TOLERABLE_TOPOLOGY_BARRIER_STALL` seconds is `ERROR`; one held for
      slightly less is `WARNING`
- [ ] With the option off the stall line matches the suppressed `WARNING` pattern; with it on it
      matches `TOPOLOGY_BARRIER_STALL`; other patterns are unchanged either way
- [ ] `uv run python -m pytest unit_tests/unit/test_cluster.py unit_tests/unit/test_sct_events_database.py -v`
- [ ] `uv run sct.py pre-commit` passes
