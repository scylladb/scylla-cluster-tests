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

"""Adaptive search for the thread count that reaches the best throughput under a latency limit.

Every step is unthrottled and differs only by thread count. The search doubles the thread count until a step
fails the latency limit, throughput stops growing, or the step budget runs out, then bisects between the last
passing and the first failing thread count. It needs no thread grid calibrated per instance type.
"""

import logging
import math
from dataclasses import dataclass
from typing import Self

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThreadSearchConfig:
    start_threads_per_shard: int = 16
    max_steps: int = 8
    precision_pct: float = 5
    min_throughput_gain_pct: float = 5

    @classmethod
    def from_params(cls, search_params: dict | None, workload_type: str) -> Self | None:
        """Build the workload's search config from `perf_best_throughput_search`.

        The workload's entry overrides the 'default' entry key by key. Returns None, meaning the workload keeps
        its fixed steps, when the parameter is not set or has neither entry.
        """
        if not search_params or (workload_type not in search_params and "default" not in search_params):
            return None
        return cls(**(search_params.get("default") or {}) | (search_params.get(workload_type) or {}))

    def start_threads(self, total_shards: int, num_stress_commands: int) -> int:
        """Threads per stress command, so all commands together run `start_threads_per_shard` per DB shard."""
        return max(1, math.ceil(self.start_threads_per_shard * total_shards / max(1, num_stress_commands)))


class ThreadSearch:
    """Decide the thread count of each step from the results of the steps before it.

    Usage: run a step at `next_threads()`, report it with `record()`, repeat until `next_threads()` is None;
    `stop_reason` then says why the search ended.
    """

    def __init__(self, start_threads: int, config: ThreadSearchConfig):
        self.config = config
        self.steps: list[tuple[int, bool, int | None]] = []
        self.lowest_failing: int | None = None
        self.highest_passing: int | None = None
        self._highest_passing_throughput: int | None = None
        self._next: int | None = max(1, start_threads)
        self.stop_reason: str | None = None

    def next_threads(self) -> int | None:
        return self._next

    def record(self, threads: int, passed: bool, throughput: int | None) -> None:
        """Record a step's result. A step without results counts as failed."""
        self.steps.append((threads, passed, throughput))
        self._next = self._decide(threads, passed, throughput)
        if self._next is not None and len(self.steps) >= self.config.max_steps:
            self._next = self._stop(f"used the step budget of {self.config.max_steps}")

    def _decide(self, threads: int, passed: bool, throughput: int | None) -> int | None:
        if passed:
            previous_throughput = self._highest_passing_throughput
            if self.highest_passing is None or threads > self.highest_passing:
                self.highest_passing, self._highest_passing_throughput = threads, throughput
            if self.lowest_failing is None:
                # Still expanding: past saturation more threads only queue, so stop before adding latency.
                if (
                    previous_throughput
                    and throughput is not None
                    and throughput < previous_throughput * (1 + self.config.min_throughput_gain_pct / 100)
                ):
                    return self._stop(
                        f"throughput grew less than {self.config.min_throughput_gain_pct}% at {threads} threads"
                    )
                return threads * 2
        else:
            if self.lowest_failing is None or threads < self.lowest_failing:
                self.lowest_failing = threads
            if self.highest_passing is None:
                if threads <= 1:
                    return self._stop("no thread count kept P99 within the limit")
                return max(1, threads // 2)

        gap = self.lowest_failing - self.highest_passing
        if gap <= max(1, self.highest_passing * self.config.precision_pct / 100):
            return self._stop(
                f"narrowed to {self.highest_passing}-{self.lowest_failing} threads "
                f"(within {self.config.precision_pct}%)"
            )
        return (self.highest_passing + self.lowest_failing) // 2

    def _stop(self, reason: str) -> None:
        self.stop_reason = reason
        LOGGER.info("Thread search stopped: %s", reason)
