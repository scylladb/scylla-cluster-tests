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
# Copyright (c) 2026 ScyllaDB

"""Background cluster health monitor.

Runs health checks in a separate daemon thread with bounded timeouts,
ensuring the nemesis thread is never blocked or killed by health check failures.

Design:
- Daemon thread that runs health checks on a configurable interval
- Cycle deadline enforced via wait(timeout) + executor.shutdown(wait=False)
  Note: shutdown(wait=False, cancel_futures=True) cancels only pending (not yet started)
  futures. Already-running threads will continue until they finish naturally, but the
  monitor does NOT wait for them — the next cycle proceeds independently.
- Events published on state transitions only (no flooding)
- Nemesis-aware: skips nodes under active nemesis + grace period after completion
- Thread-safe read interface for nemesis thread to query cached health status
"""

from __future__ import annotations

import logging
import time
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sdcm.sct_events import Severity
from sdcm.sct_events.health import ClusterHealthValidatorEvent

if TYPE_CHECKING:
    from sdcm.cluster import BaseScyllaCluster, BaseNode


@dataclass(frozen=True)
class ClusterHealthMonitorConfig:
    """All tunable parameters for the health monitor, separated from runtime state."""

    interval: int = 60
    cycle_deadline: int = 600
    parallel_workers: int = 5
    nemesis_grace_period: int = 60
    node_retries: int = 3
    connect_timeout: int = 30
    internal_retry_n: int = 2
    internal_retry_sleep: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown: int = 300

    @classmethod
    def from_params(cls, params) -> ClusterHealthMonitorConfig:
        return cls(
            interval=params.get("cluster_health_check_interval") or 60,
            cycle_deadline=params.get("cluster_health_check_deadline") or 600,
            parallel_workers=min(max(1, params.get("cluster_health_check_parallel_workers") or 5), 10),
            nemesis_grace_period=params.get("cluster_health_check_nemesis_grace_period") or 60,
            node_retries=params.get("cluster_health_check_node_retries") or 3,
            connect_timeout=params.get("cluster_health_check_connect_timeout") or 30,
        )


@dataclass
class NodeHealthStatus:
    """Health status of a single node from the last check cycle."""

    node_name: str
    healthy: bool
    last_check_time: float = 0.0
    error: str = ""
    consecutive_failures: int = 0


@dataclass
class HealthCycleResult:
    """Result of a full health check cycle."""

    healthy_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0
    timed_out_nodes: int = 0
    deadline_reached: bool = False
    duration_seconds: float = 0.0
    errors: dict[str, str] = field(default_factory=dict)
    skipped_reasons: dict[str, str] = field(default_factory=dict)  # node_name -> reason

    @property
    def checked_nodes(self) -> int:
        return self.healthy_nodes + self.failed_nodes + self.timed_out_nodes


class ClusterHealthMonitor(threading.Thread):
    """Background daemon that periodically checks cluster health.

    Design principles:
    - Runs as a daemon thread (automatically dies with main process)
    - NEVER raises exceptions to external callers
    - Publishes ClusterHealthValidatorEvent on state transitions (visible in Argus)
    - Provides thread-safe read-only status query interface
    - Respects nemesis activity: skips nodes under active nemesis + grace period
    - Non-blocking executor: cycle deadline truly enforced via shutdown(wait=False)

    Usage:
        monitor = ClusterHealthMonitor(cluster=db_cluster, params=test_params)
        monitor.start()
        ...
        # From nemesis thread (non-blocking, never raises):
        is_healthy = monitor.is_cluster_healthy
        unhealthy = monitor.get_unhealthy_nodes()
        ...
        monitor.stop()
    """

    def __init__(self, cluster: BaseScyllaCluster, params=None, *,
                 config: ClusterHealthMonitorConfig | None = None) -> None:
        super().__init__(daemon=True, name=f"ClusterHealthMonitor-{cluster.name}")

        self.cluster = cluster
        self.log = logging.getLogger(f"{__name__}.{cluster.__class__.__name__}")
        self.config = config or ClusterHealthMonitorConfig.from_params(params)

        # Internal state (thread-safe via _lock)
        self._stop_event = threading.Event()
        self._first_cycle_done = threading.Event()
        self._health_status: dict[str, NodeHealthStatus] = {}
        self._skip_checks_until: dict[str, float] = {}  # node_name -> timestamp when cooldown expires
        self._nodes_in_flight: dict[str, float] = {}  # node_name -> submit timestamp
        self._lock = threading.Lock()
        self._extra_checks: list[Callable[[], None]] = []


    @property
    def is_cluster_healthy(self) -> bool:
        """True if all monitored nodes are healthy or no data yet."""
        with self._lock:
            if not self._health_status:
                return True
            return all(s.healthy for s in self._health_status.values())

    def get_unhealthy_nodes(self) -> list[str]:
        """Node names that failed their last health check."""
        with self._lock:
            return [name for name, s in self._health_status.items() if not s.healthy]

    def wait_for_first_cycle(self, timeout: float = 120) -> bool:
        """Block until first health check cycle completes. Returns True if completed."""
        return self._first_cycle_done.wait(timeout=timeout)

    def add_health_check(self, check_fn: Callable[[], None]) -> None:
        """Register an additional health check to run each cycle (e.g., K8s monitoring)"""
        self._extra_checks.append(check_fn)

    def stop(self, timeout: float = 30) -> None:
        """Signal the monitor to stop and optionally wait for termination."""
        self._stop_event.set()
        if self.is_alive():
            self.join(timeout=timeout)
            if self.is_alive():
                self.log.warning("Health monitor did not stop within %ds", timeout)

    def run(self) -> None:
        """Main loop. Never crashes."""
        self.log.info("Health monitor started: %s", self.config)
        while not self._stop_event.is_set():
            try:
                self._run_health_cycle()
            except Exception:  # noqa: BLE001
                self.log.exception("Health monitor cycle failed unexpectedly (non-fatal)")
            self._stop_event.wait(timeout=self.config.interval)
        self.log.info("Health monitor stopped")

    def _run_health_cycle(self) -> None:
        """Execute one full health check cycle."""
        cycle_start = time.time()
        deadline = cycle_start + self.config.cycle_deadline

        # Single snapshot of all nodes — avoids multiple list(self.cluster.nodes) calls
        all_nodes = list(self.cluster.nodes)

        # Collect IPs of nodes currently under nemesis or in grace period.
        # Passed to validators so they can downgrade severity for expected DN states.
        now = time.time()
        nemesis_node_ips = frozenset(
            n.ip_address for n in all_nodes
            if n.running_nemesis or (
                getattr(n, "last_nemesis_finish_time", 0)
                and now - n.last_nemesis_finish_time < self.config.nemesis_grace_period
            )
        )

        nodes, skipped_reasons = self._filter_eligible_nodes(all_nodes, now=now)
        result = HealthCycleResult(skipped_nodes=len(all_nodes) - len(nodes), skipped_reasons=skipped_reasons)

        with ClusterHealthValidatorEvent() as chc_event:
            if not nodes:
                self.log.debug("No nodes eligible for health check this cycle")
                self._finalize_cycle(result, cycle_start, chc_event)
                return

            self.log.debug("Starting health cycle: %d nodes to check, %d skipped",
                          len(nodes), result.skipped_nodes)

            self._check_nodes_parallel(nodes, result, deadline, nemesis_node_ips)
            self._run_extra_checks(deadline)
            self._finalize_cycle(result, cycle_start, chc_event)

    def _check_nodes_parallel(
        self,
        nodes: list[BaseNode],
        result: HealthCycleResult,
        deadline: float,
        nemesis_node_ips: frozenset[str],
    ) -> None:
        """Run health checks in parallel with non-blocking deadline enforcement.

        Uses wait() with timeout instead of as_completed().
        executor.shutdown(wait=False, cancel_futures=True) cancels only pending
        futures. Already-running threads continue in background, and nodes remain
        in _nodes_in_flight until their worker actually exits.
        """
        # Warn if too many abandoned workers are still running from previous cycles
        with self._lock:
            total_in_flight = len(self._nodes_in_flight)
        max_global_in_flight = self.config.parallel_workers * 3
        if total_in_flight > max_global_in_flight:
            self.log.warning(
                "High number of in-flight workers (%d > %d limit). "
                "Possible thread pile-up from slow/stuck nodes.",
                total_in_flight, max_global_in_flight,
            )

        executor = ThreadPoolExecutor(max_workers=min(self.config.parallel_workers, len(nodes)))
        try:
            futures: dict = {}
            for node in nodes:
                if self._stop_event.is_set():
                    self.log.info("Stop requested, aborting node submission")
                    break
                with self._lock:
                    self._nodes_in_flight[node.name] = time.time()
                try:
                    future = executor.submit(self._check_single_node, node, nemesis_node_ips)
                    futures[future] = node
                except Exception:
                    with self._lock:
                        self._nodes_in_flight.pop(node.name, None)
                    raise

            if not futures:
                return

            remaining_timeout = max(0, deadline - time.time())
            done, not_done = wait(futures.keys(), timeout=remaining_timeout)

            # Process completed futures
            for future in done:
                node = futures[future]
                try:
                    future.result()
                    self._update_node_status(node, healthy=True)
                    result.healthy_nodes += 1
                except Exception as exc:  # noqa: BLE001
                    error_msg = f"{type(exc).__name__}: {exc}"
                    self._update_node_status(node, healthy=False, error=error_msg)
                    result.failed_nodes += 1
                    result.errors[node.name] = error_msg
                    self.log.error("Node %s health check failed: %s", node.name, error_msg)

            # Handle timed-out futures — node stays in _nodes_in_flight until thread finishes
            if not_done:
                result.deadline_reached = True
                for future in not_done:
                    node = futures[future]
                    future.cancel()
                    error_msg = f"Health check abandoned (cycle deadline {self.config.cycle_deadline}s reached)"
                    self._update_node_status(node, healthy=False, error=error_msg)
                    result.timed_out_nodes += 1
                    result.errors[node.name] = error_msg
                    self.log.warning("Node %s: %s", node.name, error_msg)

                self.log.warning("Cycle deadline reached, abandoned %d node checks", len(not_done))
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _check_single_node(self, node: BaseNode, nemesis_node_ips: frozenset[str]) -> None:
        """Run health check on a single node. Called inside thread pool.

        Removes node from _nodes_in_flight when done (including abandoned threads
        that continue past the cycle deadline).
        """
        try:
            node.check_node_health(
                retries=self.config.node_retries,
                nemesis_node_ips=nemesis_node_ips,
                connect_timeout=self.config.connect_timeout,
                retry_n_override=self.config.internal_retry_n,
                retry_sleep_override=self.config.internal_retry_sleep,
            )
        finally:
            with self._lock:
                self._nodes_in_flight.pop(node.name, None)

    def _run_extra_checks(self, deadline: float) -> None:
        """Run any registered extra health checks (e.g., K8s monitoring)."""
        for check_fn in self._extra_checks:
            if time.time() > deadline:
                self.log.warning("Cycle deadline reached, skipping extra check %s", check_fn.__name__)
                break
            try:
                check_fn()
            except Exception:  # noqa: BLE001
                self.log.exception("Extra health check %s failed (non-fatal)", check_fn.__name__)

    def _filter_eligible_nodes(self, all_nodes: list[BaseNode], now: float
                               ) -> tuple[list[BaseNode], dict[str, str]]:
        """Filter nodes that shouldn't be health-checked right now."""
        dead_ips = set(self.cluster.dead_nodes_ip_address_list)
        nodes = []
        skipped_reasons: dict[str, str] = {}

        with self._lock:
            skip_until_snapshot = dict(self._skip_checks_until)
            in_flight_snapshot = dict(self._nodes_in_flight)

        for node in all_nodes:
            reason = self._get_skip_reason(node, dead_ips, in_flight_snapshot, skip_until_snapshot, now)
            if reason:
                skipped_reasons[node.name] = reason
                continue
            nodes.append(node)

        self._clear_expired_suspensions(nodes, skip_until_snapshot)
        if skipped_reasons:
            self.log.debug("Skipped nodes: %s", skipped_reasons)
        return nodes, skipped_reasons

    def _get_skip_reason(self, node: BaseNode, dead_ips: set[str],
                         in_flight: dict[str, float], skip_until: dict[str, float],
                         now: float) -> str | None:
        """Return skip reason string if node should be skipped, None if eligible."""
        submit_time = in_flight.get(node.name)
        if submit_time is not None:
            stuck_seconds = int(now - submit_time)
            if stuck_seconds > self.config.cycle_deadline:
                self.log.warning("Node %s stuck in-flight for %ds (> deadline %ds)",
                                 node.name, stuck_seconds, self.config.cycle_deadline)
            return f"in_flight ({stuck_seconds}s)"
        if node.ip_address in dead_ips:
            return "dead"
        if node.running_nemesis:
            return f"nemesis: {node.running_nemesis}"
        last_finish = getattr(node, "last_nemesis_finish_time", 0)
        if last_finish and (now - last_finish) < self.config.nemesis_grace_period:
            return "grace_period"
        if self._is_check_suspended(node.name, now, skip_until):
            return "suspended (too many failures)"
        return None

    def _is_check_suspended(self, node_name: str, now: float, skip_until: dict[str, float]) -> bool:
        """Check if node health checks are temporarily suspended (circuit breaker active)."""
        until = skip_until.get(node_name, 0)
        if until > now:
            self.log.debug("Skipping node %s (suspended, cooldown %ds remaining)",
                          node_name, int(until - now))
            return True
        return False

    def _clear_expired_suspensions(self, eligible_nodes: list[BaseNode],
                                    skip_until: dict[str, float]) -> None:
        """Remove expired entries from _skip_checks_until for nodes that passed filtering."""
        expired = [n.name for n in eligible_nodes if skip_until.get(n.name, 0)]
        if expired:
            with self._lock:
                for name in expired:
                    self._skip_checks_until.pop(name, None)
            for name in expired:
                self.log.info("Circuit breaker reset for node %s, will re-check", name)


    def _update_node_status(self, node: BaseNode, healthy: bool, error: str = "") -> None:
        """Thread-safe update of node health status with state-transition event publishing."""
        with self._lock:
            existing = self._health_status.get(node.name)
            was_healthy = existing.healthy if existing else True

            consecutive_failures = 0
            if not healthy:
                consecutive_failures = (existing.consecutive_failures + 1) if existing else 1

            self._health_status[node.name] = NodeHealthStatus(
                node_name=node.name,
                healthy=healthy,
                last_check_time=time.time(),
                error=error,
                consecutive_failures=consecutive_failures if not healthy else 0,
            )

            # Circuit breaker: trip after threshold consecutive failures (inside lock to avoid race)
            if not healthy and consecutive_failures >= self.config.circuit_breaker_threshold:
                if node.name not in self._skip_checks_until:
                    self._skip_checks_until[node.name] = time.time() + self.config.circuit_breaker_cooldown
                    circuit_just_tripped = True
                else:
                    circuit_just_tripped = False
            else:
                circuit_just_tripped = False

            if healthy and node.name in self._skip_checks_until:
                # Node recovered — clear circuit breaker
                del self._skip_checks_until[node.name]

        # Publish events on state transitions only (not every cycle)
        if circuit_just_tripped:
            # Circuit breaker tripped — CRITICAL: node is likely truly broken
            # Distinguish first trip (threshold reached) from re-trip (after cooldown expiry)
            is_reopen = consecutive_failures > self.config.circuit_breaker_threshold
            action = "RE-OPENED after cooldown" if is_reopen else "OPEN"
            self.log.warning(
                "Circuit breaker %s for node %s (%d consecutive failures). "
                "Skipping checks for %ds to prevent thread pile-up.",
                action, node.name, consecutive_failures, self.config.circuit_breaker_cooldown,
            )
            self._publish_failure_event(
                node,
                f"Circuit breaker {action}: {consecutive_failures} consecutive failures. "
                f"Node excluded from monitoring for {self.config.circuit_breaker_cooldown}s. Last error: {error}",
                severity=Severity.CRITICAL,
            )
        elif not healthy and was_healthy:
            # Transition: healthy → unhealthy
            self._publish_failure_event(node, error)
        elif healthy and not was_healthy:
            # Transition: unhealthy → healthy
            self._publish_failure_event(node, "Node recovered", severity=Severity.NORMAL)
        elif not healthy and consecutive_failures > 0 and consecutive_failures % 10 == 0:
            # Still unhealthy — reminder every 10 cycles (~10 min)
            self._publish_failure_event(node, f"[still failing, cycle #{consecutive_failures}] {error}")

    def _publish_failure_event(self, node: BaseNode, error_msg: str, severity: Severity = Severity.ERROR) -> None:
        """Publish a health check failure event for visibility in Argus."""
        try:
            ClusterHealthValidatorEvent.ParallelHealthCheckFailure(
                node=node,
                error=f"[HealthMonitor] {error_msg}",
                severity=severity,
            ).publish()
        except Exception:  # noqa: BLE001
            self.log.exception("Failed to publish health event for node %s", node.name)

    def _finalize_cycle(self, result: HealthCycleResult, cycle_start: float,
                        chc_event: ClusterHealthValidatorEvent) -> None:
        """Record cycle result and set ContinuousEvent message for Argus."""
        result.duration_seconds = time.time() - cycle_start
        self._first_cycle_done.set()

        chc_event.message = (
            f"Health cycle: checked={result.checked_nodes}, healthy={result.healthy_nodes}, "
            f"failed={result.failed_nodes}, timed_out={result.timed_out_nodes}, "
            f"skipped={result.skipped_nodes}, duration={result.duration_seconds:.1f}s"
        )

        if result.failed_nodes or result.timed_out_nodes:
            self.log.info(
                "Health cycle completed in %.1fs: checked=%d, healthy=%d, failed=%d, "
                "timed_out=%d, skipped=%d, deadline_reached=%s",
                result.duration_seconds, result.checked_nodes, result.healthy_nodes,
                result.failed_nodes, result.timed_out_nodes, result.skipped_nodes,
                result.deadline_reached,
            )
        else:
            self.log.debug(
                "Health cycle completed in %.1fs: checked=%d, all healthy, skipped=%d",
                result.duration_seconds, result.checked_nodes, result.skipped_nodes,
            )

