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
# Copyright (c) 2025 ScyllaDB

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

from sdcm.teardown_validators.base import TeardownValidator
from sdcm.utils.nvme_diagnostics import (
    EXTENDED_SELF_TEST_TIMEOUT,
    SHORT_SELF_TEST_TIMEOUT,
    SelfTestType,
    check_nvme_health,
    run_self_test_on_all_devices,
)

if TYPE_CHECKING:
    from sdcm.sct_config import SCTConfiguration
    from sdcm.tester import ClusterTester

LOGGER = logging.getLogger(__name__)


class NvmeValidator(TeardownValidator):
    """Run NVMe self-tests and health checks on all DB nodes during teardown.

    Unlike other validators that use teardown_validators.<name> config,
    this validator is controlled by the ``collect_nvme_diagnostics`` parameter.
    """

    validator_name = "nvme"

    def __init__(self, params: "SCTConfiguration", tester: "ClusterTester"):
        self.params = params
        self.tester = tester
        self.is_enabled = params.get("collect_nvme_diagnostics")
        if not self.is_enabled:
            self.validate = lambda: LOGGER.info("NVMe validator is disabled")

    def validate(self):
        if not self.tester.db_cluster:
            return

        self._run_self_tests()
        self._collect_health()

    def _run_self_tests(self):
        """Run NVMe self-tests in parallel with per-node timeout."""
        test_type_int = self.params.get("nvme_self_test_type") or 1
        try:
            test_type = SelfTestType(test_type_int)
        except ValueError:
            LOGGER.warning("Invalid nvme_self_test_type value: %s, skipping self-tests", test_type_int)
            return

        nodes = self.tester.db_cluster.nodes
        LOGGER.info("Running NVMe self-tests (type=%s) on %d DB nodes", test_type.name, len(nodes))

        per_device_timeout = SHORT_SELF_TEST_TIMEOUT if test_type == SelfTestType.SHORT else EXTENDED_SELF_TEST_TIMEOUT
        # Allow extra margin for device discovery, multiple disks, and polling overhead
        overall_timeout = per_device_timeout * 2
        with ThreadPoolExecutor(max_workers=min(len(nodes), 10)) as executor:
            futures = {
                executor.submit(run_self_test_on_all_devices, node, test_type, per_device_timeout): node
                for node in nodes
            }
            try:
                for future in as_completed(futures, timeout=overall_timeout):
                    node = futures[future]
                    try:
                        future.result()
                    except Exception:  # noqa: BLE001
                        LOGGER.warning("NVMe self-test failed on %s", node.name, exc_info=True)
            except TimeoutError:
                timed_out = [n.name for f, n in futures.items() if not f.done()]
                LOGGER.warning("NVMe self-tests timed out after %ds on nodes: %s", overall_timeout, timed_out)

    def _collect_health(self):
        """Collect NVMe health data and publish events for anomalies."""
        LOGGER.info("Collecting NVMe health data from DB nodes")
        for node in self.tester.db_cluster.nodes:
            try:
                for event in check_nvme_health(current_node=node):
                    event.publish()
            except Exception:  # noqa: BLE001
                LOGGER.warning("Failed to collect NVMe health on %s", node.name, exc_info=True)
