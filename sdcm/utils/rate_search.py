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

Every step runs the load at a fixed rate. The search bisects the rate between the highest passing and the lowest
failing rate, until they are within the precision or the step budget runs out. It needs no rate
list calibrated per instance type, and starts in one of two ways:

- From the bounds: between a minimum and a maximum rate per DB shard. The first step runs at their middle on the
  log scale.
- From a baseline: a known rate per DB shard for the instance family and workload. The first step runs at the
  baseline, between bounds a range (25%) below and above it, and the search takes the plain middle of its ends.
  From the bounds, which are about 60 times apart, it takes the middle on the log scale.

Until a step fails, the upper bound stands in as the lowest failing rate without being run, and until a step
passes, the lower bound stands in as the highest passing rate: a step at the maximum overloads the cluster, and
leaves a compaction backlog for the steps after it, and a step at the minimum almost always passes. A bound runs
only when the search narrows to it, or, when no step passed yet, the lower bound runs as the last step of the
budget. A baseline bound that is crossed there moves out by another range, so a big regression or improvement
still gets a result. The minimum and maximum rates per shard always cap the bounds, and the minimum must pass and
the maximum must fail there.
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
    # A step passes only when it sent at least this share of its rate.
    min_sent_ratio: float = 0.98
    # A known rate per DB shard to start from, instead of the middle of the bounds.
    baseline_per_shard: int | None = None
    baseline_range_pct: float = 25
    # A loader over this CPU use during a step stops the search: the step measured the loaders, not the cluster.
    # The test checks it, not the search.
    max_loader_cpu_pct: float = 90

    @classmethod
    def from_params(
        cls, search_params: dict | None, workload_type: str, baselines: dict | None = None, instance_type: str = ""
    ) -> Self | None:
        """Build the workload's search config from `perf_best_throughput_search`.

        The workload's entry overrides the 'default' entry key by key. Returns None, meaning the workload keeps
        its fixed steps, when the parameter is not set or has neither entry. The baseline comes from
        `perf_best_throughput_baselines`, for the instance family of `instance_type`.
        """
        if not search_params or (workload_type not in search_params and "default" not in search_params):
            return None
        settings = (search_params.get("default") or {}) | (search_params.get(workload_type) or {})
        if baseline := baseline_for(baselines, instance_type, workload_type):
            settings["baseline_per_shard"] = baseline
        return cls(**settings)


def instance_family(instance_type: str) -> str:
    """The instance family that a baseline is kept for: 'i8g' for i8g.4xlarge; a type without a size, as is."""
    return instance_type.split(".", 1)[0]


def baseline_for(baselines: dict | None, instance_type: str, workload_type: str) -> int | None:
    """The workload's baseline rate per shard on the instance type, from the exact type or else its family."""
    for key in (instance_type, instance_family(instance_type)):
        if key and (baseline := ((baselines or {}).get(key) or {}).get(workload_type)):
            return baseline
    return None


def in_flight_for(rate: int, p99_limit_ms: float) -> int:
    """Requests in flight that a fixed rate needs when each request takes up to the P99 limit (Little's law).

    With this many, the load tool never holds the rate back on a cluster that keeps P99 within the limit.
    """
    return max(1, math.ceil(rate * p99_limit_ms / 1000))


# Why a search ended, as the best-throughput row in Argus reports it.
STOP_KINDS = ("narrowed", "budget", "min rate fails", "max rate passes", "no results", "loader cpu")
# The search ended without finding the cluster's limit: the run reports an error. "min rate fails" and "max rate
# passes" mean the bounds don't fit the cluster.
ERROR_STOP_KINDS = ("min rate fails", "max rate passes", "no results", "loader cpu")
# The search ended on a step that didn't measure the cluster: the run sends no best-throughput row, so its result
# doesn't land in the Argus history. "no results": a step collected none twice; "loader cpu": a loader ran out of CPU.
NO_RESULT_STOP_KINDS = ("no results", "loader cpu")


class RateSearch:
    """Decide the rate of each step from the results of the steps before it.

    Usage: run a step at `next_rate()`, report it with `record()`, repeat until `next_rate()` is None;
    `stop_reason` then says why the search ended, and `stop_kind` names it in one of `STOP_KINDS`.
    """

    def __init__(self, config: RateSearchConfig, shards: int):
        self.config = config
        shards = max(1, shards)
        self.min_rate = config.min_rate_per_shard * shards
        self.max_rate = config.max_rate_per_shard * shards
        self.steps: list[tuple[int, bool, int | None]] = []
        self.highest_passing: int | None = None
        self.lowest_failing: int | None = None
        self._range = config.baseline_range_pct / 100
        if config.baseline_per_shard:
            baseline = min(max(config.baseline_per_shard * shards, self.min_rate), self.max_rate)
            self.low_bound = self._widen_down(baseline)
            self.high_bound = self._widen_up(baseline)
            self._next: int | None = baseline
        else:
            self.low_bound, self.high_bound = self.min_rate, self.max_rate
            self._next = round(math.sqrt(self.min_rate * self.max_rate))
        self.stop_reason: str | None = None
        self.stop_kind: str | None = None
        self.is_retry = False
        self._retries = 0

    def _widen_down(self, rate: int) -> int:
        return max(round(rate * (1 - self._range)), self.min_rate)

    def _widen_up(self, rate: int) -> int:
        return min(round(rate * (1 + self._range)), self.max_rate)

    def next_rate(self) -> int | None:
        return self._next

    def record(self, rate: int, passed: bool, throughput: int | None) -> None:
        """Record a step's result; `throughput` None means the step collected no results.

        A step without results is run once more at the same rate (`is_retry` is then True): a transient collection
        error must not count as a latency failure. Without results the second time too, the search stops with no
        result. The retry doesn't count against the step budget.
        """
        self.steps.append((rate, passed, throughput))
        if throughput is None and not self.is_retry:
            LOGGER.warning("Rate search: the step at %s op/s collected no results, running it again", rate)
            self.is_retry = True
            self._retries += 1
            return
        self.is_retry = False
        if throughput is None:
            self._next = self._stop(f"the step at {rate} op/s collected no results twice", kind="no results")
            return
        self._next = self._decide(rate, passed)
        steps_run = len(self.steps) - self._retries
        if self._next is not None and self.highest_passing is None and steps_run == self.config.max_steps - 1:
            # Nothing passed, and one step is left: run the lower bound, so that a minimum rate that fails is reported
            # as one, at any budget.
            self._next = self.low_bound
        if self._next is not None and steps_run >= self.config.max_steps:
            self._next = self._stop(f"used the step budget of {self.config.max_steps}", kind="budget")

    def stop_on_loader_cpu(self, rate: int, loaders_cpu_pct: dict[str, float]) -> None:
        """Stop the search: a loader went over `max_loader_cpu_pct` in the step at `rate`, so the step measured the
        loaders, not the cluster. The step isn't recorded."""
        busy = ", ".join(f"{loader} {cpu:.0f}%" for loader, cpu in sorted(loaders_cpu_pct.items()))
        self._next = self._stop(
            f"the loaders went over {self.config.max_loader_cpu_pct:g}% CPU in the step at {rate} op/s ({busy}), so "
            "it measured them, not the cluster: raise n_loaders",
            kind="loader cpu",
        )

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
        # A bound the search narrowed to and ran, and crossed: the result is past the baseline's range.
        if passed and rate == self.high_bound:
            self.high_bound = self._widen_up(rate)
            LOGGER.info("Rate search: %s op/s passed, so the upper bound moves to %s op/s", rate, self.high_bound)
        elif not passed and rate == self.low_bound:
            self.low_bound = self._widen_down(rate)
            LOGGER.info("Rate search: %s op/s failed, so the lower bound moves to %s op/s", rate, self.low_bound)

        # Until a step fails (passes), the upper (lower) bound is taken as failing (passing) without running it.
        lower = self.highest_passing or self.low_bound
        upper = self.lowest_failing or self.high_bound
        if upper <= lower * (1 + self.config.precision_pct / 100):
            # Narrowed to a bound that hasn't run: only running it tells whether the result is past it.
            if self.lowest_failing is None:
                return self.high_bound
            if self.highest_passing is None:
                return self.low_bound
            return self._stop(
                f"narrowed to {self.highest_passing}-{self.lowest_failing} op/s (within {self.config.precision_pct}%)",
                kind="narrowed",
            )
        # Around a baseline, the ends are close, and the plain middle is within ~3% of the log-scale one. Between the
        # minimum and maximum rates, which are far apart, the plain middle puts the first steps near the top, so the
        # middle is on a log scale there: each step halves the ratio between the two ends.
        return round((lower + upper) / 2 if self.config.baseline_per_shard else math.sqrt(lower * upper))

    def _stop(self, reason: str, kind: str) -> None:
        self.stop_reason, self.stop_kind = reason, kind
        LOGGER.info("Rate search stopped: %s", reason)
