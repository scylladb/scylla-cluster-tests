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

from pathlib import Path

import pytest

from sdcm.utils.trigger_matrix import load_matrix_config

TRIGGERS_DIR = Path(__file__).parent.parent.parent / "configurations" / "triggers"


def test_scylla_doctor_gating_has_wait_true():
    path = Path(__file__).parent.parent.parent / "configurations/triggers/scylla-doctor-gating.yaml"
    if not path.exists():
        pytest.skip("scylla-doctor-gating.yaml not found")
    config = load_matrix_config(path)
    assert all(job.wait is True for job in config.jobs)
    assert all(job.fail_on_error is True for job in config.jobs)
    assert all(len(job.collect_results) > 0 for job in config.jobs)


def test_pgo_has_wait_false():
    path = Path(__file__).parent.parent.parent / "configurations/triggers/pgo-offline-installer.yaml"
    if not path.exists():
        pytest.skip("pgo-offline-installer.yaml not found")
    config = load_matrix_config(path)
    assert all(job.wait is False for job in config.jobs)
