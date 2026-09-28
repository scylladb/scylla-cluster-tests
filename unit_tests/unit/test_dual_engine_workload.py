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

"""Unit tests for the dual-engine (logstor + LSM) 60/40 workload config.

The test method itself reuses the existing gradual-load machinery, so what is worth
pinning down here are the invariants of logstor_lsm_dual_60_40.yaml: the ops split,
the aggregate rate, and the HDR tags the four commands produce (one Argus row each).
"""

import pathlib

import pytest
import yaml

from sdcm import sct_abs_path
from sdcm.stress.latte_thread import find_latte_fn_names


@pytest.fixture(scope="module")
def dual_engine_config():
    config_path = pathlib.Path(sct_abs_path("configurations/performance/logstor_lsm_dual_60_40.yaml"))
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_dual_engine_config_ops_split_is_60_40(dual_engine_config):
    """Total logstor op/s should be 60% and lsm 40% for each throttle step."""
    steps = dual_engine_config["perf_gradual_throttle_steps"]["dual_engine_mixed"]
    for step in steps:
        logstor_total = int(step["logstor_write_rate"]) + int(step["logstor_read_rate"])
        lsm_total = int(step["lsm_write_rate"]) + int(step["lsm_read_rate"])
        combined = logstor_total + lsm_total
        assert combined > 0
        logstor_pct = logstor_total / combined
        # Allow +/-2% tolerance around 60%
        assert 0.58 <= logstor_pct <= 0.62, f"Logstor share {logstor_pct:.2%} is not within 58-62% (step={step})"


def test_dual_engine_config_step_rate_matches_sum(dual_engine_config):
    """The aggregate 'rate' field must equal the sum of all four per-engine rates."""
    steps = dual_engine_config["perf_gradual_throttle_steps"]["dual_engine_mixed"]
    for step in steps:
        expected_total = (
            int(step["logstor_write_rate"])
            + int(step["logstor_read_rate"])
            + int(step["lsm_write_rate"])
            + int(step["lsm_read_rate"])
        )
        assert int(step["rate"]) == expected_total, (
            f"Step 'rate' {step['rate']} != sum of engine rates {expected_total}"
        )


def test_dual_engine_config_yields_four_hdr_tags(dual_engine_config):
    """The four commands must map to four distinct HDR tags -> four Argus latency rows.

    run_gradual_increase_load builds the decorator's hdr_tags from exactly this, and
    send_result_to_argus keeps a per-tag row only while there are more than two tags.
    """
    fn_names = [fn for cmd in dual_engine_config["stress_cmd_m"] for fn in find_latte_fn_names(cmd)]
    assert sorted(fn_names) == ["logstor_read", "logstor_write", "lsm_read", "lsm_write"]
