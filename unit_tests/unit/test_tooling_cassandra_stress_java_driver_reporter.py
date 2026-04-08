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

"""Pytest tests for CassandraStressJavaDriverVersionReporter."""

from sdcm.reporting.tooling_reporter import CassandraStressJavaDriverVersionReporter


def test_driver_version_is_set_in_constructor():
    reporter = CassandraStressJavaDriverVersionReporter(
        driver_version="4.19.0.1", runner=None, command_prefix=None, argus_client=None
    )

    assert reporter.version == "4.19.0.1"
    assert reporter.TOOL_NAME == "java-driver"


def test_collect_version_info_is_noop():
    reporter = CassandraStressJavaDriverVersionReporter(
        driver_version="3.11.5.11", runner=None, command_prefix=None, argus_client=None
    )

    reporter._collect_version_info()

    # Version should remain unchanged
    assert reporter.version == "3.11.5.11"
