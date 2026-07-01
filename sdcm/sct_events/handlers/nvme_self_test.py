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
import time

from sdcm.sct_events.handlers import EventHandler
from sdcm.sct_events.system import FailedResultEvent
from sdcm.utils.nvme import SelfTestType, check_nvme_health, run_self_test_on_all_devices

LOGGER = logging.getLogger(__name__)

# Minimum interval between self-test runs (seconds). Prevents triggering
# self-tests too frequently when multiple FailedResultEvents arrive in
# quick succession.
MIN_SELF_TEST_INTERVAL = 600  # 10 minutes


class NvmeSelfTestHandler(EventHandler):
    """Triggers NVMe device self-tests when a FailedResultEvent is received.

    Runs a short self-test on all NVMe data disks across all DB nodes.
    Rate-limited to at most once per MIN_SELF_TEST_INTERVAL to avoid
    excessive testing when multiple failures arrive together.
    """

    def __init__(self):
        super().__init__()
        self._last_run_time = 0

    def handle(self, event: FailedResultEvent, tester_obj: "sdcm.tester.ClusterTester"):  # noqa: F821
        if not tester_obj.params.get("collect_nvme_diagnostics"):
            LOGGER.debug("NVMe diagnostics disabled, skipping self-test on FailedResultEvent")
            return

        if not tester_obj.db_cluster:
            LOGGER.debug("No DB cluster available, skipping NVMe self-test")
            return

        if time.time() - self._last_run_time < MIN_SELF_TEST_INTERVAL:
            LOGGER.debug(
                "Skipping NVMe self-test, last run was %ds ago (min interval: %ds)",
                time.time() - self._last_run_time,
                MIN_SELF_TEST_INTERVAL,
            )
            return

        self._last_run_time = time.time()
        test_type_int = tester_obj.params.get("nvme_self_test_type") or 1
        test_type = SelfTestType(test_type_int)
        LOGGER.info(
            "FailedResultEvent received — collecting NVMe SMART logs and triggering self-test (type=%s)", test_type.name
        )

        for node in tester_obj.db_cluster.nodes:
            try:
                # Collect SMART logs and publish health events for any issues
                for health_event in check_nvme_health(current_node=node):
                    health_event.publish()
            except Exception:  # noqa: BLE001
                LOGGER.warning("NVMe SMART log collection failed on %s", node.name, exc_info=True)

            try:
                run_self_test_on_all_devices(node, test_type=test_type)
            except Exception:  # noqa: BLE001
                LOGGER.warning("NVMe self-test failed on %s", node.name, exc_info=True)
