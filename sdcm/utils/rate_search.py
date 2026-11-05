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

"""Search for the highest fixed rate whose P99 stays within the latency limit.

Every step runs the load at a fixed rate, between a minimum and a maximum rate per DB shard. The minimum runs
first and must pass, else the bounds are wrong for the cluster. The search then bisects the rate between the
highest passing and the lowest failing rate on a log scale, until they are within the precision or the step
budget runs out. Until a step fails, the maximum stands in as the lowest failing rate without being run: a step
at the maximum overloads the cluster, and for a workload whose dataset grows every step it leaves a compaction
backlog for all the steps after it. The maximum runs only when the search narrows up to it, and must fail there.
A step that collected no results is run once more before it counts as failed. It needs no rate list calibrated
per instance type.
"""

import logging
import math
from dataclasses import dataclass
from typing import Self

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateSearchConfig:
    min_rate_per_shard: int = 1000
    max_rate_per_shard: int = 60000
    precision_pct: float = 5
    max_steps: int = 10
    # A step that sent less of its rate than this was held back by the loaders, not by the cluster.
    min_sent_ratio: float = 0.95

    @classmethod
    def from_params(cls, search_params: dict | None, workload_type: str) -> Self | None:
        """Build the workload's search config from `perf_best_throughput_search`.

        The workload's entry overrides the 'default' entry key by key. Returns None, meaning the workload keeps
        its fixed steps, when the parameter is not set or has neither entry.
        """
        if not search_params or (workload_type not in search_params and "default" not in search_params):
            return None
        return cls(**(search_params.get("default") or {}) | (search_params.get(workload_type) or {}))


def in_flight_for(rate: int, p99_limit_ms: float) -> int:
    """Requests in flight that a fixed rate needs when each request takes up to the P99 limit (Little's law).

    With this many, the load tool never holds the rate back on a cluster that keeps P99 within the limit.
    """
    return max(1, math.ceil(rate * p99_limit_ms / 1000))


# Why a search ended, as the best-throughput row in Argus reports it. "min rate fails" and "max rate passes" mean
# the bounds don't fit the cluster: no step failed above the best one, so the result is not the limit.
STOP_KINDS = ("narrowed", "budget", "min rate fails", "max rate passes")
BOUND_ERRORS = ("min rate fails", "max rate passes")


class RateSearch:
    """Decide the rate of each step from the results of the steps before it.

    Usage: run a step at `next_rate()`, report it with `record()`, repeat until `next_rate()` is None;
    `stop_reason` then says why the search ended, and `stop_kind` names it in one of `STOP_KINDS`.
    """

    def __init__(self, config: RateSearchConfig, shards: int):
        self.config = config
        self.min_rate = config.min_rate_per_shard * max(1, shards)
        self.max_rate = config.max_rate_per_shard * max(1, shards)
        self.steps: list[tuple[int, bool, int | None]] = []
        self.highest_passing: int | None = None
        self.lowest_failing: int | None = None
        self._next: int | None = self.min_rate
        self.stop_reason: str | None = None
        self.stop_kind: str | None = None
        self.is_retry = False
        self._retries = 0

    def next_rate(self) -> int | None:
        return self._next

    def record(self, rate: int, passed: bool, throughput: int | None) -> None:
        """Record a step's result; `throughput` None means the step collected no results.

        A step without results is run once more at the same rate (`is_retry` is then True): a transient collection
        error must not count as a latency failure. Without results the second time too, it counts as failed. The
        retry doesn't count against the step budget.
        """
        self.steps.append((rate, passed, throughput))
        if throughput is None and not self.is_retry:
            LOGGER.warning("Rate search: the step at %s op/s collected no results, running it again", rate)
            self.is_retry = True
            self._retries += 1
            return
        if throughput is None:
            LOGGER.warning("Rate search: the step at %s op/s collected no results twice, counting it as failed", rate)
        self.is_retry = False
        self._next = self._decide(rate, passed and throughput is not None)
        if self._next is not None and len(self.steps) - self._retries >= self.config.max_steps:
            self._next = self._stop(f"used the step budget of {self.config.max_steps}", kind="budget")

    def _decide(self, rate: int, passed: bool) -> int | None:
        if passed:
            self.highest_passing = max(rate, self.highest_passing or 0)
        else:
            self.lowest_failing = min(rate, self.lowest_failing or rate)

        if rate == self.min_rate and not passed:
            return self._stop(
                f"the minimum rate of {self.min_rate} op/s ({self.config.min_rate_per_shard} per shard) went over "
                "the latency limit: lower min_rate_per_shard",
                kind="min rate fails",
            )
        if rate == self.max_rate and passed:
            return self._stop(
                f"the maximum rate of {self.max_rate} op/s ({self.config.max_rate_per_shard} per shard) stayed "
                "within the latency limit, so the best throughput is a lower bound: raise max_rate_per_shard",
                kind="max rate passes",
            )

        # Until a step fails, the maximum rate is taken as failing without running it.
        upper = self.lowest_failing or self.max_rate
        if upper <= self.highest_passing * (1 + self.config.precision_pct / 100):
            if self.lowest_failing is None:
                # Narrowed up to the maximum: only running it tells whether the bounds are wrong.
                return self.max_rate
            return self._stop(
                f"narrowed to {self.highest_passing}-{self.lowest_failing} op/s (within {self.config.precision_pct}%)",
                kind="narrowed",
            )
        # The middle on a log scale: the bounds are far apart, and the plain middle puts the first steps near
        # the top. Each step halves the ratio between the two ends.
        return round(math.sqrt(self.highest_passing * upper))

    def _stop(self, reason: str, kind: str) -> None:
        self.stop_reason, self.stop_kind = reason, kind
        LOGGER.info("Rate search stopped: %s", reason)
