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
throttled, with a thread count bounded so concurrency doesn't grow without limit while the cluster sheds load.
Goodput is the measured throughput of successful operations; it is expected to stay at `min_goodput_ratio` of the
best throughput or above.
"""

import math
from dataclasses import dataclass
from typing import Any, Self

from sdcm.utils.latency import step_latency


@dataclass(frozen=True)
class OverloadConfig:
    factors: tuple[float, ...] = (1.3, 2)
    max_threads_multiplier: float = 4
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

    def steps(self, best_threads: int, best_throughput: int) -> list["OverloadStep"]:
        """One step per factor: its rate follows the run's own best throughput, its threads are bounded."""
        max_threads = max(best_threads, math.floor(best_threads * self.max_threads_multiplier))
        return [
            OverloadStep(
                factor=factor,
                rate=round(best_throughput * factor),
                threads=min(math.ceil(best_threads * factor), max_threads),
            )
            for factor in self.factors
        ]


@dataclass(frozen=True)
class OverloadStep:
    factor: float
    rate: int
    threads: int

    @property
    def name(self) -> str:
        return f"overload_{self.factor:g}x"


@dataclass(frozen=True)
class GoodputResult:
    step: str
    factor: float
    offered_rate: int
    threads: int
    goodput: int
    goodput_ratio: float
    p99: dict[str, float]
    failed_ops: int | None


def goodput_result(
    step: OverloadStep, step_summary: dict[str, Any], best_throughput: int, failed_ops: int | None = None
) -> GoodputResult:
    """Goodput of an overload step: its HDR-measured throughput, which counts successful operations only.

    A step without HDR results gets a goodput of 0, so a run that lost all its results fails the ratio rule
    instead of passing it silently.
    """
    p99, goodput = step_latency(step.name, step_summary) or ({}, 0)
    return GoodputResult(
        step=step.name,
        factor=step.factor,
        offered_rate=step.rate,
        threads=step.threads,
        goodput=goodput,
        goodput_ratio=round(goodput / best_throughput, 3) if best_throughput else 0.0,
        p99=p99,
        failed_ops=failed_ops,
    )
