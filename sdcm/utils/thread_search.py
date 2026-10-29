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
fails the latency limit or throughput stops growing, then bisects between the last passing and the first failing
thread count until they are within the precision or the step budget runs out. The budget never cuts the search
before it has found the limit, and doubling stops at a thread cap. A step that collected no results is run once
more before it counts as failed, so one collection error doesn't end doubling early. It needs no thread grid
calibrated per instance type.
"""

import logging
import math
from dataclasses import dataclass
from typing import Self

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThreadSearchConfig:
    start_threads_per_shard: int = 16
    max_steps: int = 10
    precision_pct: float = 5
    min_throughput_gain_pct: float = 5
    # About 2x the highest thread count of today's fixed steps (1900 per loader, mixed).
    max_threads_per_command: int = 4096

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


# Why a search ended, as the best-throughput row in Argus reports it. Only "narrowed" has a failing thread count
# within the precision of the best one; "thread cap" means the limit is above the cap, a lower bound.
STOP_KINDS = ("narrowed", "saturated", "thread cap", "budget", "none passed")


class ThreadSearch:
    """Decide the thread count of each step from the results of the steps before it.

    Usage: run a step at `next_threads()`, report it with `record()`, repeat until `next_threads()` is None;
    `stop_reason` then says why the search ended, and `stop_kind` names it in one of `STOP_KINDS`.
    """

    def __init__(self, start_threads: int, config: ThreadSearchConfig):
        self.config = config
        self.steps: list[tuple[int, bool, int | None]] = []
        self.lowest_failing: int | None = None
        self.highest_passing: int | None = None
        self._highest_passing_throughput: int | None = None
        self._next: int | None = min(max(1, start_threads), config.max_threads_per_command)
        self.stop_reason: str | None = None
        self.stop_kind: str | None = None
        self.is_retry = False
        self._retries = 0

    def next_threads(self) -> int | None:
        return self._next

    def record(self, threads: int, passed: bool, throughput: int | None) -> None:
        """Record a step's result; `throughput` None means the step collected no results.

        A step without results is run once more at the same thread count (`is_retry` is then True): a transient
        collection error must not count as the latency limit and end doubling early. Without results the second
        time too, it counts as failed. The retry doesn't count against the step budget.
        """
        self.steps.append((threads, passed, throughput))
        if throughput is None and not self.is_retry:
            LOGGER.warning("Thread search: the step at %s threads collected no results, running it again", threads)
            self.is_retry = True
            self._retries += 1
            return
        if throughput is None:
            LOGGER.warning(
                "Thread search: the step at %s threads collected no results twice, counting it as failed", threads
            )
        self.is_retry = False
        self._next = self._decide(threads, passed and throughput is not None, throughput)
        # The budget only cuts the bisection: until a passing and a failing thread count are both known, the
        # limit hasn't been found, and stopping would report a thread count that isn't the best.
        limit_found = self.highest_passing is not None and self.lowest_failing is not None
        if self._next is not None and limit_found and len(self.steps) - self._retries >= self.config.max_steps:
            self._next = self._stop(f"used the step budget of {self.config.max_steps}", kind="budget")

    def _decide(self, threads: int, passed: bool, throughput: int | None) -> int | None:
        if passed:
            previous_throughput = self._highest_passing_throughput
            if self.highest_passing is None or threads > self.highest_passing:
                self.highest_passing, self._highest_passing_throughput = threads, throughput
            if self.lowest_failing is None:
                return self._expand(threads, throughput, previous_throughput)
        else:
            if self.lowest_failing is None or threads < self.lowest_failing:
                self.lowest_failing = threads
            if self.highest_passing is None:
                if threads <= 1:
                    return self._stop("no thread count kept P99 within the limit", kind="none passed")
                return max(1, threads // 2)

        gap = self.lowest_failing - self.highest_passing
        if gap <= max(1, self.highest_passing * self.config.precision_pct / 100):
            return self._stop(
                f"narrowed to {self.highest_passing}-{self.lowest_failing} threads "
                f"(within {self.config.precision_pct}%)",
                kind="narrowed",
            )
        return (self.highest_passing + self.lowest_failing) // 2

    def _expand(self, threads: int, throughput: int | None, previous_throughput: int | None) -> int | None:
        """The next step while no thread count has failed yet: double, unless saturated or at the cap."""
        # Past saturation more threads only queue, so stop before adding latency. No bisection follows: no thread
        # count failed, and the passing steps already hold the best throughput the cluster gives, which
        # find_best_throughput picks among them.
        if (
            previous_throughput
            and throughput is not None
            and throughput < previous_throughput * (1 + self.config.min_throughput_gain_pct / 100)
        ):
            return self._stop(
                f"throughput grew less than {self.config.min_throughput_gain_pct}% at {threads} threads",
                kind="saturated",
            )
        if threads >= self.config.max_threads_per_command:
            # The limit lies above the cap, so the best throughput found is only a lower bound.
            return self._stop(
                f"reached the cap of {self.config.max_threads_per_command} threads per command while still within "
                "the latency limit: the best throughput is a lower bound",
                kind="thread cap",
            )
        return min(threads * 2, self.config.max_threads_per_command)

    def _stop(self, reason: str, kind: str) -> None:
        self.stop_reason, self.stop_kind = reason, kind
        LOGGER.info("Thread search stopped: %s", reason)
