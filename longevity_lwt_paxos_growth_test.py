#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2024 ScyllaDB

"""
SCYLLADB-2122: Reproduce paxos ($paxos) table growth under LWT load, where the
paxos table grows much larger than the base table it protects.

Mechanism:
  1. LWT-write a large number of UNIQUE primary keys (INSERT IF NOT EXISTS). Each
     successful CAS leaves a $paxos entry retained for paxos_grace_seconds,
     independent of what happens to the base row.
  2. DELETE those rows with a REGULAR (non-LWT) DELETE. This empties the base
     table but does NOT touch the colocated $paxos entries.
  Result: base table shrinks, $paxos keeps every key -> on-disk ratio grows past 1:1.

TTL / time relationship (the lever that makes this diverge within a short run):
  base default_time_to_live (300s) < test duration (~1h) << paxos_grace_seconds (10d)
  Base rows expire within the run; by the reclaim phase (~20-30 min after Phase 1
  writes) both TTL and gc_grace_seconds (300s) have elapsed, so a plain major
  compaction purges them from disk (tombstone_gc=immediate needs no repair), while
  $paxos entries carry paxos_grace_seconds (10d) and are nowhere near expiry ->
  on-disk paxos:base ratio climbs > 1:1.

Test methods:
  - test_paxos_growth (RECOMMENDED): LWT-write unique keys + plain DELETE.
    Phase 1 (populate): INSERT IF NOT EXISTS over a large UNIQUE key space -> CAS
      succeeds -> $paxos fills with one entry per key.
    Phase 2 (delete): regular (non-LWT) DELETE over the same keys -> base rows
      tombstoned, $paxos entries persist.
    Phase 3 (reclaim): wait past base TTL + gc_grace_seconds, run major compaction
      (tombstone_gc=immediate needs no repair) -> base table empties on disk while
      $paxos stays full (paxos_grace_seconds=10d, nowhere near expiry) -> ratio
      maximized.

  - test_custom_time: Cyclic batch approach (legacy, kept for comparison)
    Each cycle: INSERT 1000, DELETE 900, observe ratio
    Problem: CAS always succeeds with large key space -> ratio pins at 1:1

Schema/stress profile: data_dir/cs_paxos_growth_lwt.yaml (2 partition-key + 2
clustering-key + 3 value columns; column names are arbitrary). Cluster shape,
instance types and per-phase durations are configured in
test-cases/longevity/longevity-lwt-paxos-growth-{tablets,vnodes}.yaml.
"""

import threading
import time

from longevity_test import LongevityTest


class LWTPaxosGrowthTest(LongevityTest):
    """Reproduce SCYLLADB-2122: paxos table growth with large keys + insert/delete cycles.

    Orchestrates write/delete cycles in Python while running background SERIAL reads
    from YAML config (stress_read_cmd) to observe latency impact.
    """

    KEYSPACE = "ks_paxos_growth"
    TABLE = "event_history"
    REPORT_INTERVAL_MIN = 5
    PROFILE = "/tmp/cs_paxos_growth_lwt.yaml"

    # Cycle parameters
    BATCH_SIZE = 1000  # insert 1000 unique keys per cycle
    DELETE_COUNT = 900  # delete 900 of them per cycle
    WRITE_THREADS = 400
    DELETE_THREADS = 400
    STRESS_DURATION_MIN = 60  # total stress duration

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._monitor_stop_event = threading.Event()
        self._monitor_thread = None
        self._prev_paxos_drops: dict[str, dict[str, int]] = {}

    def test_custom_time(self):
        """LWT paxos growth test: write/delete cycles with large keys."""
        self._log_test_parameters()
        self._start_paxos_monitor()

        stress_queue = []
        try:
            # Run first insert cycle to create keyspace/table schema before starting reads
            self._run_first_cycle()

            # Start background SERIAL reads (from YAML stress_read_cmd) to observe issues
            # Started AFTER first cycle so that keyspace/table already exists
            stress_read_cmd = self.params.get("stress_read_cmd")
            if stress_read_cmd:
                cmds = stress_read_cmd if isinstance(stress_read_cmd, list) else [stress_read_cmd]
                for cmd in cmds:
                    self.log.info("Starting background SERIAL read: %s", cmd)
                    read_threads = self.run_stress_thread(
                        stress_cmd=cmd,
                        round_robin=self.params.get("round_robin"),
                    )
                    stress_queue.append(read_threads)

            # Foreground: remaining write/delete cycles (cycle 2+)
            self._run_write_delete_cycles(start_cycle=2)
        finally:
            self._stop_paxos_monitor()
            # Wait for background reads to finish
            for stress in stress_queue:
                try:
                    self.verify_stress_thread(stress)
                except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                    self.log.warning("Background read stress finished with error: %s", exc)
            self.log.info("=== FINAL PAXOS REPORT ===")
            self._report_table_sizes_all_nodes()
            self._log_table_properties()

    def test_paxos_growth(self):
        """LWT-write + plain-DELETE regime: reproduce $paxos > base table ratio.

        Mechanism: LWT-write a large number of UNIQUE primary keys
        (INSERT IF NOT EXISTS) so each successful CAS
        leaves a paxos_state entry retained for paxos_grace_seconds (10 days). Then
        DELETE those rows with a REGULAR (non-LWT) DELETE: the base table empties but
        the colocated $paxos entries persist -> on-disk ratio climbs > 1:1.

        The lever that makes a 1h run diverge: base default_time_to_live (300s) <
        test duration (~1h) << paxos_grace_seconds (10d). Base rows expire during the
        run, and a plain major compaction purges them from disk (tombstone_gc=immediate
        needs no repair), while $paxos entries carry paxos_grace_seconds (10d) and are
        nowhere near expiry.

        Phase 1 (populate): INSERT IF NOT EXISTS over a large UNIQUE key space -> CAS
          succeeds -> $paxos fills with one entry per key (stress_cmd).
        Phase 2 (delete): regular (non-LWT) DELETE over the same keys -> base rows
          tombstoned, $paxos persists (stress_cmd_w).
        Phase 3 (reclaim): wait past base TTL + gc_grace_seconds, major compaction ->
          base empties on disk, ratio maximized.

        Background: SERIAL SELECT to observe latency impact and paxos read amplification.

        Key metrics to watch:
          - $paxos vs base on-disk size ratio (success: > 1:1 after reclaim)
          - cas_write_timeout_due_to_uncertainty: same-pk requests hitting different
            coordinators (Petr 64802)
          - cas_write_contention / cas_write_timeouts: contention + semaphore exhaustion
        """
        self._log_test_parameters()
        self._start_paxos_monitor()

        stress_queue = []
        try:
            stress_cmd = self.params.get("stress_cmd")

            # Phase 1: LWT populate — INSERT IF NOT EXISTS over a large UNIQUE key
            # space. CAS SUCCEEDS on each fresh key -> one $paxos entry per key.
            self.log.info("=== Phase 1: LWT POPULATE — INSERT IF NOT EXISTS (CAS succeeds) ===")
            self.log.info("Each unique key creates a paxos_state entry retained for paxos_grace_seconds")
            if stress_cmd:
                cmds = stress_cmd if isinstance(stress_cmd, list) else [stress_cmd]
                write_threads = []
                for cmd in cmds:
                    self.log.info("Starting populate stress: %s", cmd)
                    threads = self.run_stress_thread(
                        stress_cmd=cmd,
                        round_robin=self.params.get("round_robin"),
                    )
                    write_threads.append(threads)
                for threads in write_threads:
                    try:
                        self.verify_stress_thread(threads)
                    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                        self.log.warning("Populate phase stress finished with error: %s", exc)

            self.log.info("=== Phase 1 complete — reporting sizes after populate ===")
            paxos_after_populate, user_after_populate = self._report_table_sizes_all_nodes()
            self._report_cas_metrics(label="post-populate")

            # GUARD 1: populate must have written $paxos entries, else the LWT path is
            # not creating paxos state (wrong CL / op) and the run is worthless.
            if paxos_after_populate <= 0:
                self.fail(
                    "Populate wrote 0 paxos bytes — INSERT IF NOT EXISTS did not create "
                    "paxos_state entries (check serial-cl / write_insert-if-not-exists op)."
                )

            # Start background SERIAL reads now that the table exists — observe latency
            # impact and paxos read amplification during the delete + reclaim phases.
            stress_read_cmd = self.params.get("stress_read_cmd")
            if stress_read_cmd:
                cmds = stress_read_cmd if isinstance(stress_read_cmd, list) else [stress_read_cmd]
                for cmd in cmds:
                    self.log.info("Starting background SERIAL read: %s", cmd)
                    read_threads = self.run_stress_thread(
                        stress_cmd=cmd,
                        round_robin=self.params.get("round_robin"),
                    )
                    stress_queue.append(read_threads)

            # Phase 2: plain (non-LWT) DELETE — empties base table, $paxos persists.
            self.log.info("=== Phase 2: PLAIN DELETE (non-LWT) — base empties, paxos persists ===")
            self.log.info("Regular DELETE removes base rows but NOT the colocated $paxos entries")
            stress_cmd_w = self.params.get("stress_cmd_w")
            if stress_cmd_w:
                cmds = stress_cmd_w if isinstance(stress_cmd_w, list) else [stress_cmd_w]
                delete_threads = []
                for cmd in cmds:
                    self.log.info("Starting DELETE stress: %s", cmd)
                    threads = self.run_stress_thread(
                        stress_cmd=cmd,
                        round_robin=self.params.get("round_robin"),
                    )
                    delete_threads.append(threads)
                for threads in delete_threads:
                    try:
                        self.verify_stress_thread(threads)
                    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                        self.log.warning("Delete phase stress finished with error: %s", exc)

            self.log.info("=== Phase 2 complete — reporting sizes after delete ===")
            self._report_table_sizes_all_nodes()
            self._report_cas_metrics(label="post-delete")

            # Phase 3: reclaim base table on disk. Base rows are TTL-expired (300s)
            # and DELETE-tombstoned. With tombstone_gc=immediate, compaction purges
            # dead/tombstoned data as soon as gc_grace_seconds (300s) has elapsed
            # since the local_deletion_time — no repair needed.
            # By this point 20+ min have passed since Phase 1 writes, so all expired
            # rows and tombstones are well past gc_grace_seconds eligibility.
            self.log.info("=== Phase 3: Reclaim base — compact to purge (tombstone_gc=immediate) ===")
            self.log.info("Forcing major compaction on %s.%s to purge base tombstones", self.KEYSPACE, self.TABLE)
            for node in self.db_cluster.nodes:
                node.run_nodetool(
                    "compact",
                    args=f"{self.KEYSPACE} {self.TABLE}",
                    ignore_status=True,
                    timeout=1800,
                )
            self.log.info("Waiting 2 min for compaction to settle...")
            time.sleep(120)

            self.log.info("=== Reclaim complete — reporting sizes ===")
            paxos_final, user_final = self._report_table_sizes_all_nodes()

            # GUARD 2: the whole point — after delete + reclaim, the base table must
            # have shrunk AND $paxos must now exceed it. If not, the mechanism did not
            # reproduce (e.g. tombstones not purged) — fail fast with diagnostics.
            ratio_final = paxos_final / user_final if user_final > 0 else float("inf")
            self.log.info(
                "Final reclaim check: paxos %.1f MB, user %.1f MB (was %.1f MB after populate), ratio %.2f:1",
                paxos_final / 1024 / 1024,
                user_final / 1024 / 1024,
                user_after_populate / 1024 / 1024,
                ratio_final,
            )
            if not user_final < user_after_populate:
                self.fail(
                    "Base table did not shrink after plain DELETE + repair + compaction "
                    f"(user {user_after_populate} -> {user_final} bytes). Tombstones not "
                    "purged — verify plain DELETE op and repair completion."
                )
            if not ratio_final > 1.0:
                self.fail(
                    f"paxos:user ratio is {ratio_final:.2f}:1 (<= 1:1) after reclaim — "
                    "$paxos did not outgrow the base table. Verify unique-key population "
                    "(CAS must succeed) and that DELETE is plain (non-LWT)."
                )

        finally:
            self._stop_paxos_monitor()
            # Wait for background reads to finish
            for stress in stress_queue:
                try:
                    self.verify_stress_thread(stress)
                except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                    self.log.warning("Background read stress finished with error: %s", exc)
            self.log.info("=== FINAL PAXOS REPORT (after reclaim) ===")
            self._report_table_sizes_all_nodes()
            self._report_cas_metrics(label="FINAL")
            self._log_table_properties()

    def _run_first_cycle(self):
        """Run the first insert+delete cycle to create the keyspace/table schema."""
        self.log.info(
            "Running first cycle to create schema: batch_size=%d, delete_count=%d, write_threads=%d, delete_threads=%d",
            self.BATCH_SIZE,
            self.DELETE_COUNT,
            self.WRITE_THREADS,
            self.DELETE_THREADS,
        )
        base_key = 1
        end_key = self.BATCH_SIZE
        del_end_key = self.DELETE_COUNT

        # INSERT 1000 unique keys — this creates the keyspace and table
        insert_cmd = (
            f"cassandra-stress user profile={self.PROFILE} "
            f"'ops(write_insert-if-not-exists=1)' no-warmup "
            f"cl=QUORUM serial-cl=LOCAL_SERIAL n={self.BATCH_SIZE} "
            f"-mode native cql3 -rate threads={self.WRITE_THREADS} "
            f"-pop seq={base_key}..{end_key}"
        )
        self.log.info("Cycle 1 INSERT: seq %d..%d (%d keys)", base_key, end_key, self.BATCH_SIZE)
        self.run_stress(stress_cmd=insert_cmd)

        # DELETE 900 of them (plain non-LWT delete)
        delete_cmd = (
            f"cassandra-stress user profile={self.PROFILE} "
            f"'ops(delete_plain=1)' no-warmup "
            f"cl=QUORUM n={self.DELETE_COUNT} "
            f"-mode native cql3 -rate threads={self.DELETE_THREADS} "
            f"-pop seq={base_key}..{del_end_key}"
        )
        self.log.info("Cycle 1 DELETE: seq %d..%d (%d keys)", base_key, del_end_key, self.DELETE_COUNT)
        self.run_stress(stress_cmd=delete_cmd)
        self.log.info("First cycle complete — schema created, background reads can start")

    def _run_write_delete_cycles(self, start_cycle=1):
        """Run insert/delete cycles for STRESS_DURATION_MIN minutes."""
        cycle = start_cycle - 1
        start_time = time.time()
        duration_sec = self.STRESS_DURATION_MIN * 60

        self.log.info(
            "Starting write/delete cycles: batch_size=%d, delete_count=%d, "
            "write_threads=%d, delete_threads=%d, duration=%d min",
            self.BATCH_SIZE,
            self.DELETE_COUNT,
            self.WRITE_THREADS,
            self.DELETE_THREADS,
            self.STRESS_DURATION_MIN,
        )

        while (time.time() - start_time) < duration_sec:
            cycle += 1
            base_key = (cycle - 1) * self.BATCH_SIZE + 1
            end_key = base_key + self.BATCH_SIZE - 1
            del_end_key = base_key + self.DELETE_COUNT - 1

            cycle_start = time.time()

            # Phase 1: INSERT 1000 unique keys (700-800 KB each)
            insert_cmd = (
                f"cassandra-stress user profile={self.PROFILE} "
                f"'ops(write_insert-if-not-exists=1)' no-warmup "
                f"cl=QUORUM serial-cl=LOCAL_SERIAL n={self.BATCH_SIZE} "
                f"-mode native cql3 -rate threads={self.WRITE_THREADS} "
                f"-pop seq={base_key}..{end_key}"
            )
            self.log.info("Cycle %d INSERT: seq %d..%d (%d keys)", cycle, base_key, end_key, self.BATCH_SIZE)
            try:
                self.run_stress(stress_cmd=insert_cmd)
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                self.log.error("Cycle %d INSERT failed: %s", cycle, exc)

            # Phase 2: DELETE 900 of them (plain non-LWT delete)
            delete_cmd = (
                f"cassandra-stress user profile={self.PROFILE} "
                f"'ops(delete_plain=1)' no-warmup "
                f"cl=QUORUM n={self.DELETE_COUNT} "
                f"-mode native cql3 -rate threads={self.DELETE_THREADS} "
                f"-pop seq={base_key}..{del_end_key}"
            )
            self.log.info("Cycle %d DELETE: seq %d..%d (%d keys)", cycle, base_key, del_end_key, self.DELETE_COUNT)
            try:
                self.run_stress(stress_cmd=delete_cmd)
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                self.log.error("Cycle %d DELETE failed: %s", cycle, exc)

            cycle_elapsed = time.time() - cycle_start
            total_elapsed = time.time() - start_time
            self.log.info(
                "Cycle %d complete in %.1fs (total elapsed: %.1f min / %d min). "
                "Surviving rows this cycle: %d. Total surviving rows: %d",
                cycle,
                cycle_elapsed,
                total_elapsed / 60,
                self.STRESS_DURATION_MIN,
                self.BATCH_SIZE - self.DELETE_COUNT,
                cycle * (self.BATCH_SIZE - self.DELETE_COUNT),
            )

        self.log.info("Write/delete cycles finished: %d cycles in %.1f min", cycle, (time.time() - start_time) / 60)

    # =========================================================================
    # Parameter logging
    # =========================================================================

    def _log_test_parameters(self):
        """Log all relevant test parameters at startup for verification."""
        self.log.info("=" * 80)
        self.log.info("=== SCYLLADB-2122 PAXOS GROWTH TEST PARAMETERS ===")
        self.log.info("=" * 80)

        # Scylla version
        node = self.db_cluster.nodes[0]
        try:
            scylla_version = node.scylla_version
            self.log.info("Scylla version: %s", scylla_version)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.warning("Could not get Scylla version: %s", exc)

        # Cycle parameters
        self.log.info("Cycle parameters:")
        self.log.info("  BATCH_SIZE (inserts per cycle): %d", self.BATCH_SIZE)
        self.log.info("  DELETE_COUNT (deletes per cycle): %d", self.DELETE_COUNT)
        self.log.info("  WRITE_THREADS: %d", self.WRITE_THREADS)
        self.log.info("  DELETE_THREADS: %d", self.DELETE_THREADS)
        self.log.info("  STRESS_DURATION_MIN: %d", self.STRESS_DURATION_MIN)
        self.log.info("  PROFILE: %s", self.PROFILE)
        self.log.info("  Surviving rows per cycle: %d", self.BATCH_SIZE - self.DELETE_COUNT)

        # Stress read cmd from YAML
        stress_read_cmd = self.params.get("stress_read_cmd")
        self.log.info("Background stress_read_cmd: %s", stress_read_cmd)

        # DESCRIBE TABLE
        try:
            with self.db_cluster.cql_connection_patient(node) as session:
                # User table schema
                result = session.execute(f"DESCRIBE TABLE {self.KEYSPACE}.{self.TABLE}")
                schema_str = "\n".join(row[0] if hasattr(row, "__getitem__") else str(row) for row in result)
                self.log.info("User table schema:\n%s", schema_str)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.warning("Could not DESCRIBE TABLE: %s", exc)

        # Table properties via system_schema
        self._log_table_properties()

        # System.paxos properties (for vnodes)
        try:
            with self.db_cluster.cql_connection_patient(node) as session:
                result = session.execute(
                    "SELECT default_time_to_live, gc_grace_seconds, compaction "
                    "FROM system_schema.tables "
                    "WHERE keyspace_name = 'system' AND table_name = 'paxos'"
                )
                for row in result:
                    self.log.info(
                        "system.paxos properties: default_time_to_live=%s, gc_grace_seconds=%s, compaction=%s",
                        row.default_time_to_live,
                        row.gc_grace_seconds,
                        row.compaction,
                    )
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            self.log.warning("Could not query system.paxos properties: %s", exc)

        # Cluster topology
        self.log.info("Cluster: %d db nodes, %d loaders", len(self.db_cluster.nodes), len(self.loaders.nodes))
        for i, db_node in enumerate(self.db_cluster.nodes):
            self.log.info("  DB node %d: %s", i, db_node.name)

        self.log.info("=" * 80)

    # =========================================================================
    # Paxos monitoring (background thread)
    # =========================================================================

    def _start_paxos_monitor(self):
        """Start background thread that reports paxos:user ratio periodically."""
        self._monitor_stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True, name="paxos-monitor")
        self._monitor_thread.start()
        self.log.info("Started paxos monitor thread (reporting every %d min)", self.REPORT_INTERVAL_MIN)

    def _stop_paxos_monitor(self):
        """Stop the background monitor thread."""
        self._monitor_stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=60)
            self.log.info("Paxos monitor thread stopped")

    def _monitor_loop(self):
        """Background loop: report table sizes and semaphore health every REPORT_INTERVAL_MIN minutes."""
        report_count = 0
        while not self._monitor_stop_event.is_set():
            self._monitor_stop_event.wait(timeout=self.REPORT_INTERVAL_MIN * 60)
            if self._monitor_stop_event.is_set():
                break
            report_count += 1
            self.log.info("=== Paxos Snapshot %d ===", report_count)
            try:
                self._report_table_sizes_all_nodes()
                self._report_semaphore_health()
                self._report_cas_metrics(label=f"snapshot-{report_count}")
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                self.log.warning("Paxos monitor error: %s", exc)

    # =========================================================================
    # Table size reporting
    # =========================================================================

    def _get_paxos_cfstats(self, node, user_table_sizes):
        """Return (paxos_space_bytes, paxos_sstables) for a node.

        On tablets: paxos is stored in ks_paxos_growth.event_history$paxos.
        On vnodes:  paxos is stored in system.paxos (single system-wide table).
        """
        paxos_shadow = f"{self.TABLE}$paxos"
        if paxos_shadow in user_table_sizes:
            space = user_table_sizes[paxos_shadow].get("Space used (total)", 0)
            sstables = user_table_sizes[paxos_shadow].get("SSTable count", 0)
            return space, sstables

        # vnodes path — flush and query system keyspace
        node.run_nodetool("flush", args="system", ignore_status=True, timeout=300)
        result = node.run_nodetool(sub_cmd="cfstats", args="system", ignore_status=True, timeout=300)
        system_sizes = self._parse_per_table_cfstats(result.stdout)
        space = system_sizes.get("paxos", {}).get("Space used (total)", 0)
        sstables = system_sizes.get("paxos", {}).get("SSTable count", 0)
        return space, sstables

    def _report_table_sizes_all_nodes(self):
        """Report user and paxos table sizes aggregated across ALL nodes."""
        total_paxos_bytes = 0
        total_user_bytes = 0
        total_paxos_sstables = 0
        total_user_sstables = 0

        for node in self.db_cluster.nodes:
            try:
                node.run_nodetool("flush", args=self.KEYSPACE, ignore_status=True, timeout=300)
                result = node.run_nodetool(
                    sub_cmd="cfstats",
                    args=self.KEYSPACE,
                    ignore_status=True,
                    timeout=300,
                )
                table_sizes = self._parse_per_table_cfstats(result.stdout)

                user_space = table_sizes.get(self.TABLE, {}).get("Space used (total)", 0)
                user_sstables = table_sizes.get(self.TABLE, {}).get("SSTable count", 0)
                paxos_space, paxos_sstables = self._get_paxos_cfstats(node, table_sizes)

                total_paxos_bytes += paxos_space
                total_user_bytes += user_space
                total_paxos_sstables += paxos_sstables
                total_user_sstables += user_sstables

                self.log.info(
                    "  Node %s: paxos %.1f MB (%d sst), user %.1f MB (%d sst)",
                    node.name,
                    paxos_space / 1024 / 1024,
                    paxos_sstables,
                    user_space / 1024 / 1024,
                    user_sstables,
                )
            except (ValueError, IndexError, AttributeError, OSError) as exc:
                self.log.warning("Failed to get table sizes from node %s: %s", node.name, exc)

        ratio = total_paxos_bytes / total_user_bytes if total_user_bytes > 0 else float("inf")
        self.log.info(
            "TABLE SIZES (cluster total) - paxos: %.1f MB (%d sst), user: %.1f MB (%d sst), paxos:user ratio: %.2f:1",
            total_paxos_bytes / 1024 / 1024,
            total_paxos_sstables,
            total_user_bytes / 1024 / 1024,
            total_user_sstables,
            ratio,
        )
        return total_paxos_bytes, total_user_bytes

    # =========================================================================
    # Semaphore health reporting
    # =========================================================================

    def _report_semaphore_health(self):
        """Per node: count semaphore timeout events and delta of dropped paxos messages."""
        interval = self.REPORT_INTERVAL_MIN + 1  # +1 min buffer
        for node in self.db_cluster.nodes:
            try:
                semaphore_result = node.remoter.run(
                    f"journalctl -u scylla-server --since '{interval} minutes ago' "
                    "--no-pager 2>/dev/null | grep -c 'reader_concurrency_semaphore' || true",
                    ignore_status=True,
                )
                semaphore_count = int(semaphore_result.stdout.strip() or 0)

                tp_result = node.run_nodetool("tpstats", ignore_status=True, timeout=60)
                current_drops = self._parse_tpstats_paxos_drops(tp_result.stdout)
                prev_drops = self._prev_paxos_drops.get(node.name, {})
                delta_drops = {k: current_drops[k] - prev_drops.get(k, 0) for k in current_drops}
                self._prev_paxos_drops[node.name] = current_drops

                drops_str = "  ".join(f"{k}={v}" for k, v in delta_drops.items() if v > 0) or "none"
                self.log.info(
                    "  Node %s semaphore: %d timeout events in last %d min | paxos dropped (delta): %s",
                    node.name,
                    semaphore_count,
                    interval,
                    drops_str,
                )
            except (ValueError, AttributeError, OSError) as exc:
                self.log.warning("Failed semaphore check on node %s: %s", node.name, exc)

    # =========================================================================
    # CAS metrics reporting (Prometheus scrape)
    # =========================================================================

    # CAS metrics that characterise the LWT load and timeout causes (storage_proxy.cc)
    CAS_METRICS = [
        "scylla_storage_proxy_coordinator_cas_write_condition_not_met",  # CAS condition failed
        "scylla_storage_proxy_coordinator_cas_dropped_prune",  # prune starvation
        "scylla_storage_proxy_coordinator_cas_write_contention",  # contention amplification
        "scylla_storage_proxy_coordinator_cas_write_timeout",  # timeout (semaphore)
        "scylla_storage_proxy_coordinator_cas_write_timeout_due_to_uncertainty",  # same-pk -> diff coordinator (Petr 64802)
        "scylla_storage_proxy_coordinator_cas_read_contention",  # read-path contention
        "scylla_storage_proxy_coordinator_cas_now_pruning",  # currently pruning
        "scylla_storage_proxy_replica_received_cas_dropped_prune",  # replica-side dropped prune
    ]

    def _report_cas_metrics(self, label=""):
        """Scrape CAS-specific Prometheus metrics from each DB node and log them."""
        self.log.info("=== CAS Metrics [%s] ===", label)
        # Build grep pattern for all metrics we care about
        pattern = "|".join(self.CAS_METRICS)
        for node in self.db_cluster.nodes:
            try:
                result = node.remoter.run(
                    f"curl -s http://localhost:9180/metrics 2>/dev/null | grep -E '^({pattern})' | grep -v '^#'",
                    ignore_status=True,
                    timeout=30,
                )
                if not result.stdout.strip():
                    self.log.info("  Node %s: no CAS metrics found (may not be exposed)", node.name)
                    continue

                # Parse and aggregate per-metric (sum across shards)
                metric_totals: dict[str, float] = {}
                for line in result.stdout.strip().splitlines():
                    parts = line.split()
                    if len(parts) >= 2:
                        # Metric name is everything before the first '{' or space
                        metric_name = parts[0].split("{")[0]
                        try:
                            metric_totals[metric_name] = metric_totals.get(metric_name, 0) + float(parts[-1])
                        except (ValueError, IndexError):
                            pass

                # Log non-zero metrics
                nonzero = {
                    k.replace("scylla_storage_proxy_coordinator_", "").replace(
                        "scylla_storage_proxy_replica_received_", "replica_"
                    ): int(v)
                    for k, v in metric_totals.items()
                    if v > 0
                }
                if nonzero:
                    metrics_str = "  ".join(f"{k}={v}" for k, v in sorted(nonzero.items()))
                    self.log.info("  Node %s CAS: %s", node.name, metrics_str)
                else:
                    self.log.info("  Node %s CAS: all zero", node.name)
            except (OSError, AttributeError) as exc:
                self.log.warning("  Node %s CAS metrics error: %s", node.name, exc)

    def _get_cas_metric_total(self, metric_name):
        """Sum a single CAS counter across all shards/nodes (Prometheus scrape)."""
        total = 0.0
        for node in self.db_cluster.nodes:
            try:
                result = node.remoter.run(
                    f"curl -s http://localhost:9180/metrics 2>/dev/null | grep -E '^{metric_name}' | grep -v '^#'",
                    ignore_status=True,
                    timeout=30,
                )
                for line in result.stdout.strip().splitlines():
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            total += float(parts[-1])
                        except (ValueError, IndexError):
                            pass
            except (OSError, AttributeError) as exc:
                self.log.warning("CAS metric scrape error on %s: %s", node.name, exc)
        return total

    # =========================================================================
    # Table properties logging
    # =========================================================================

    def _log_table_properties(self):
        """Log the table properties of both user and paxos tables for verification."""
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node) as session:
            result = session.execute(
                "SELECT default_time_to_live, gc_grace_seconds, compaction, compression "
                "FROM system_schema.tables "
                f"WHERE keyspace_name = '{self.KEYSPACE}' AND table_name = '{self.TABLE}'"
            )
            for row in result:
                self.log.info(
                    "User table properties: default_time_to_live=%s, gc_grace_seconds=%s, "
                    "compaction=%s, compression=%s",
                    row.default_time_to_live,
                    row.gc_grace_seconds,
                    row.compaction,
                    row.compression,
                )

            paxos_table_name = f"{self.TABLE}$paxos"
            result = session.execute(
                "SELECT default_time_to_live, gc_grace_seconds, compaction, compression "
                "FROM system_schema.tables "
                f"WHERE keyspace_name = '{self.KEYSPACE}' AND table_name = '{paxos_table_name}'"
            )
            for row in result:
                self.log.info(
                    "Paxos shadow table properties: default_time_to_live=%s, gc_grace_seconds=%s, "
                    "compaction=%s, compression=%s",
                    row.default_time_to_live,
                    row.gc_grace_seconds,
                    row.compaction,
                    row.compression,
                )

    # =========================================================================
    # Parsing utilities
    # =========================================================================

    @staticmethod
    def _parse_tpstats_paxos_drops(tpstats_output):
        """Extract dropped message counts for paxos message types from nodetool tpstats."""
        drops = {}
        in_dropped_section = False
        for line in tpstats_output.splitlines():
            stripped = line.strip()
            if "Message type" in stripped and "Dropped" in stripped:
                in_dropped_section = True
                continue
            if in_dropped_section:
                if not stripped:
                    break
                parts = stripped.split()
                if len(parts) >= 2:
                    msg_type = parts[0]
                    if "paxos" in msg_type.lower():
                        try:
                            drops[msg_type] = int(parts[1])
                        except ValueError:
                            pass
        return drops

    @staticmethod
    def _parse_per_table_cfstats(cfstats_output):
        """Parse cfstats output into per-table stat dictionaries."""
        tables = {}
        current_table = None
        for line in cfstats_output.splitlines():
            stripped = line.strip()
            if stripped.startswith("Table:"):
                current_table = stripped.split(":", 1)[1].strip()
                tables[current_table] = {}
            elif current_table and ":" in stripped:
                parts = stripped.split(":", 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    val_str = parts[1].strip().split()[0] if parts[1].strip() else ""
                    try:
                        if "." in val_str:
                            tables[current_table][key] = float(val_str)
                        else:
                            tables[current_table][key] = int(val_str)
                    except (ValueError, IndexError):
                        tables[current_table][key] = val_str
        return tables
