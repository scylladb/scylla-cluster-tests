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

from sdcm.scylla_bench_thread import ScyllaBenchThread
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures(
        "events",
    ),
]


@pytest.mark.parametrize(
    "extra_cmd,error_at_row_limit_exists",
    (
        pytest.param("", True, id="no-limit-flags"),
        pytest.param("-error-limit=0", False, id="error-limit-eq-0"),
        pytest.param("-error-limit=1", False, id="error-limit-eq-1"),
        pytest.param("-error-limit=2", False, id="error-limit-eq-2"),
        pytest.param("-error-limit=10", False, id="error-limit-eq-10"),
        pytest.param("-error-limit 0", False, id="error-limit-space-0"),
        pytest.param("-error-limit 10", False, id="error-limit-space-10"),
        pytest.param("-retry-number=0", True, id="retry-number-eq-0"),
        pytest.param("-retry-number=1", True, id="retry-number-eq-1"),
        pytest.param("-retry-number=2", False, id="retry-number-eq-2"),
        pytest.param("-retry-number=10", False, id="retry-number-eq-10"),
        pytest.param("-retry-number= 10", False, id="retry-number-eq-space-10"),
        pytest.param("-retry-number 10", False, id="retry-number-space-10"),
        pytest.param("-retry-number  10", False, id="retry-number-double-space-10"),
        pytest.param("-retry-number 1", True, id="retry-number-space-1"),
        pytest.param("-retry-number 2", False, id="retry-number-space-2"),
        pytest.param("-error-at-row-limit=13", True, id="error-at-row-limit-eq-13"),
        pytest.param("-error-at-row-limit 15", True, id="error-at-row-limit-space-15"),
    ),
)
def test_02_scylla_bench_check_error_at_row_limit_addition(params, extra_cmd, error_at_row_limit_exists):
    """ScyllaBenchThread appends -error-at-row-limit unless an error limit or a retry count above 1 is set, keeping a user-given value."""
    loader_set = LocalLoaderSetDummy(nodes=[])
    cmd = (
        f"scylla-bench -workload=sequential {extra_cmd} -mode=write"
        " -replication-factor=1 -partition-count=10 -clustering-row-count=5555"
        " -clustering-row-size=uniform:10..20 -concurrency=10 -connection-count=10"
        " -consistency-level=one -rows-per-request=10 -timeout=60s -duration=1m"
    )
    bench_thread = ScyllaBenchThread(stress_cmd=cmd, loader_set=loader_set, timeout=120, node_list=[], params=params)

    assert bench_thread.stress_cmd.count("-error-at-row-limit") == int(error_at_row_limit_exists)
    assert error_at_row_limit_exists == ("-error-at-row-limit" in bench_thread.stress_cmd)
    if "-error-at-row-limit" in extra_cmd:
        actual_value = bench_thread.stress_cmd.split("-error-at-row-limit")[-1].replace("=", "")
        actual_value = int(actual_value.strip().split(" ")[0])
        assert int(extra_cmd.split("=" if "=" in extra_cmd else " ")[-1]) == actual_value
