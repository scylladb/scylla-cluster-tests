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

from contextlib import nullcontext
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
import yaml
from argus.client.generic_result import Status

from sdcm.argus_results import send_iotune_results_to_argus
from sdcm.utils.validators.iotune import IOTuneValidator

INSTALLED = {"read_iops": 1000, "read_bandwidth": 1000, "write_iops": 1000, "write_bandwidth": 1000}
MEASURED = {"read_iops": 700, "read_bandwidth": 1300, "write_iops": 1000, "write_bandwidth": 900}
EXPECTED_DEVIATION_PCT = {
    "read_iops_pct_deviation": 30,
    "read_bandwidth_pct_deviation": 30,
    "write_iops_pct_deviation": 0,
    "write_bandwidth_pct_deviation": 10,
}


def io_properties_file(disk: dict) -> nullcontext:
    """Return the content of an io_properties.yaml file with one disk, as remote_file gives it."""
    return nullcontext(StringIO(yaml.safe_dump({"disks": [{"mountpoint": "/var/lib/scylla", **disk}]})))


def submit_deviation_table(backend: str, instance_type: str):
    """Validate the I/O properties of one node and return the deviation table sent to Argus."""
    validator = IOTuneValidator(node=MagicMock(), backend=backend)
    with patch(
        "sdcm.utils.validators.iotune.remote_file",
        side_effect=[io_properties_file(INSTALLED), io_properties_file(MEASURED)],
    ):
        validator.validate()
    argus_client = MagicMock()
    argus_client.get_run.return_value = {"test_id": "test-id"}

    send_iotune_results_to_argus(
        argus_client=argus_client,
        results=validator.results,
        node=MagicMock(db_node_instance_type=instance_type),
        params={"cluster_backend": backend},
    )

    return next(
        call.args[0]
        for call in argus_client.submit_results.call_args_list
        if call.args[0].name.endswith("Percent deviation")
    )


@pytest.mark.parametrize(
    "backend,limit_pct",
    [
        pytest.param("aws", 25, id="aws"),
        pytest.param("gce", 20, id="gce"),
        pytest.param("oci", 30, id="oci"),
        pytest.param("azure", 35, id="azure"),
    ],
)
def test_iotune_validator_sends_absolute_deviation_with_backend_limit(backend, limit_pct):
    """Argus fails the run when a deviation is at or above the limit, so a preset too high or too low must give a
    positive deviation, and each deviation cell must be UNSET under a column with the backend limit.
    """
    table = submit_deviation_table(backend, instance_type="test-instance-type")

    assert {cell.column: cell.value for cell in table.results} == pytest.approx(EXPECTED_DEVIATION_PCT)
    assert {cell.status for cell in table.results} == {Status.UNSET}
    assert {column: rule.fixed_limit for column, rule in table.validation_rules.items()} == {
        column: limit_pct for column in EXPECTED_DEVIATION_PCT
    }


def test_iotune_validator_uses_one_table_per_backend_and_instance_type():
    """Argus keeps a history of the rules of a table, so each backend limit and each instance type must go to a table
    of its own.
    """
    runs = [("aws", "i4i.large"), ("aws", "i4i.xlarge"), ("gce", "n2-highmem-8"), ("azure", "Standard_L8s_v4")]

    tables = [submit_deviation_table(backend, instance_type) for backend, instance_type in runs]

    assert len({table.name for table in tables}) == len(runs)
    for (backend, instance_type), table in zip(runs, tables):
        assert backend in table.name
        assert instance_type in table.name
