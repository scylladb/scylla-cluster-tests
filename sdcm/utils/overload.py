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

"""Goodput under overload (SCYLLADB-3267): offer more than the best throughput and measure what still succeeds.

After the best throughput under the latency limit is found, each overload step offers `factor x best throughput`,
throttled, with `factor x` the best step's requests in flight: its concurrency for latte, else its threads.
Goodput is the measured throughput of successful operations; it is expected to stay at `min_goodput_ratio` of the
best throughput or above.

A closed-loop stress tool such as cassandra-stress waits for each reply, so its threads cap what it sends, whatever
rate it is asked for: near saturation a step can attempt far less than its target. Each step records the rate it
attempted (successful plus failed operations per second), and one under `MIN_ATTEMPTED_RATIO` of its target did not
load the cluster at its factor, so its goodput ratio says little about overload.
"""

import math
from dataclasses import dataclass
from typing import Any, Self

from sdcm.utils.latency import step_latency

# A step attempting less of its target rate than this did not offer the overload it was meant to.
MIN_ATTEMPTED_RATIO = 0.95


@dataclass(frozen=True)
class OverloadConfig:
    factors: tuple[float, ...] = (1.3, 2)
    min_goodput_ratio: float = 0.9

    @classmethod
    def from_params(cls, overload_params: dict | None, workload_type: str) -> Self | None:
        """Build the workload's overload config from `perf_overload_steps`; None means no overload steps.

        The workload's entry overrides the 'default' entry key by key.
        """
        if not overload_params or (workload_type not in overload_params and "default" not in overload_params):
            return None
        settings = (overload_params.get("default") or {}) | (overload_params.get(workload_type) or {})
        if "factors" in settings:
            settings["factors"] = tuple(settings["factors"])
        return cls(**settings)

    def steps(
        self, best_threads: int, best_throughput: int, best_concurrency: int | None = None
    ) -> list["OverloadStep"]:
        """One step per factor: its rate and its requests in flight follow the run's own best step.

        With a concurrency (latte) the concurrency follows the factor and the threads stay; else the threads do.
        """
        return [
            OverloadStep(
                factor=factor,
                rate=round(best_throughput * factor),
                threads=best_threads if best_concurrency else math.ceil(best_threads * factor),
                concurrency=math.ceil(best_concurrency * factor) if best_concurrency else None,
            )
            for factor in self.factors
        ]


@dataclass(frozen=True)
class OverloadStep:
    factor: float
    rate: int
    threads: int
    concurrency: int | None = None

    @property
    def name(self) -> str:
        return f"overload_{self.factor:g}x"


@dataclass(frozen=True)
class GoodputResult:
    step: str
    factor: float
    target_rate: int
    threads: int
    goodput: int
    goodput_ratio: float
    p99: dict[str, float]
    failed_ops: int | None
    attempted_rate: int | None = None
    attempted_ratio: float | None = None

    @property
    def reached_target(self) -> bool:
        """False when the step attempted clearly less than its target rate; unknown counts as reached."""
        return self.attempted_ratio is None or self.attempted_ratio >= MIN_ATTEMPTED_RATIO


def goodput_result(
    step: OverloadStep,
    step_summary: dict[str, Any],
    best_throughput: int,
    failed_ops: int | None = None,
    operation_time_s: float | None = None,
) -> GoodputResult:
    """Goodput of an overload step: its HDR-measured throughput, which counts successful operations only.

    A step without HDR results gets a goodput of 0, so a run that lost all its results fails the ratio rule
    instead of passing it silently. The attempted rate adds the failed operations per second to the goodput; it is
    unknown when the tool reports no error count, or reports errors without the step's operation time.
    """
    p99, goodput = step_latency(step.name, step_summary) or ({}, 0)
    if failed_ops == 0:
        attempted_rate = goodput
    elif failed_ops and operation_time_s:
        attempted_rate = goodput + round(failed_ops / operation_time_s)
    else:
        attempted_rate = None
    return GoodputResult(
        step=step.name,
        factor=step.factor,
        target_rate=step.rate,
        threads=step.threads,
        goodput=goodput,
        goodput_ratio=round(goodput / best_throughput, 3) if best_throughput else 0.0,
        p99=p99,
        failed_ops=failed_ops,
        attempted_rate=attempted_rate,
        attempted_ratio=round(attempted_rate / step.rate, 3) if attempted_rate is not None and step.rate else None,
    )
