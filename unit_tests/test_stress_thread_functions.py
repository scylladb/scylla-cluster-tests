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
# Copyright (c) 2022 ScyllaDB

import pytest

from sdcm.stress_thread import get_timeout_from_stress_cmd
from sdcm.utils.common import time_period_str_to_seconds


@pytest.mark.parametrize("duration,seconds", (
    ("1h1m20s", 3680),
    ("1m20s", 80),
    ("1h20s", 3620),
    ("25m", 1500),
    ("10h", 36000),
    ("25s", 25),
))
def test_duration_str_to_seconds_function(duration, seconds):
    assert time_period_str_to_seconds(duration) == seconds


@pytest.mark.parametrize("stress_cmd, timeout", (
    ("cassandra-stress counter_write cl=QUORUM duration=20m"
     " -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' no-warmup", 1200 + 900),
    ("scylla-bench -workload=uniform -concurrency 64 -duration 1h -validate-data", 3600 + 900),
    ("scylla-bench -partition-count=20000 -duration=250s", 250 + 900),
    ("gemini -d --duration 10m --warmup 10s -c 5 -m write", 610 + 900),
    ("latte run --duration 10m --sampling 5s", 600 + 900),
    # Gemini commands from the issue - test case with 24h duration
    ("--duration 24h --warmup 10m --concurrency 200 --mode mixed --max-mutation-retries-backoff 10s", 86400 + 600 + 900),
    # Gemini with equals sign format
    ("--duration=3h --warmup=30m --concurrency=50 --mode=mixed", 10800 + 1800 + 900),
    # Gemini command without warmup
    ("--duration 1h --concurrency 100 --mode write", 3600 + 900),
    # Critical case: YAML multiline format with newlines (the actual issue scenario)
    ("--duration 24h\n--warmup 10m\n--concurrency 200", 86400 + 600 + 900),
))
def test_get_timeout_from_stress_cmd(stress_cmd, timeout):
    assert get_timeout_from_stress_cmd(stress_cmd) == timeout
