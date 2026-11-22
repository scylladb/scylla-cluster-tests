#!/usr/bin/env python

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

import re
from collections import Counter
from pathlib import Path

from longevity_test import LongevityTest
from sdcm.utils.cluster_config import ConfigScope, get_cluster_config
from sdcm.utils.file import File
from sdcm.utils.tablets.common import AUTO_REPAIR_OPTION

# The scopes that test-cases/features/auto-repair/longevity-auto-repair-scopes-4h.yaml sets:
# these two tables follow the cluster value, true
AUTO_REPAIR_TABLES = [("ks_auto", "t"), ("ks_mixed", "auto_t")]
# and these two have auto repair turned off at KEYSPACE scope and at TABLE scope
NO_AUTO_REPAIR_TABLES = [("ks_manual", "t"), ("ks_mixed", "manual_t")]

# The topology coordinator logs one such line for each tablet that auto repair finished
AUTO_REPAIR_LOG_LINE = re.compile(
    r"Finished tablet repair .* tablet=(?P<table_id>[0-9a-f-]+):\d+ .* request_type=auto_repair"
)


class AutoRepairScopesLongevityTest(LongevityTest):
    def test_auto_repair_scopes(self):
        self.test_custom_time()

        # A repair nemesis stores auto_repair_enabled = false at CLUSTER scope for the time of its repair
        if self.db_cluster.nemesis_threads:
            self.db_cluster.stop_nemesis(timeout=300)
        self.validate_auto_repair_scopes()

    def validate_auto_repair_scopes(self):
        """Check that the scopes are still stored, and that auto repair ran only on the tables they enable."""
        node = self.db_cluster.data_nodes[0]
        stored_scopes = {
            "cluster": get_cluster_config(node, AUTO_REPAIR_OPTION),
            "ks_manual": get_cluster_config(node, AUTO_REPAIR_OPTION, ConfigScope.KEYSPACE, "ks_manual"),
            "ks_mixed.manual_t": get_cluster_config(node, AUTO_REPAIR_OPTION, ConfigScope.TABLE, "ks_mixed.manual_t"),
        }
        assert stored_scopes == {"cluster": "true", "ks_manual": "false", "ks_mixed.manual_t": "false"}, (
            f"The stored {AUTO_REPAIR_OPTION} scopes changed during the test: {stored_scopes}"
        )

        with self.db_cluster.cql_connection_patient(node) as session:
            rows = session.execute("SELECT keyspace_name, table_name, id FROM system_schema.tables")
            table_ids = {(row.keyspace_name, row.table_name): str(row.id) for row in rows}

        # The number of tablets that auto repair finished, per table id. The coordinator moves between the
        # nodes, so read the log that the runner keeps of every node, also of nodes that a nemesis removed
        auto_repairs = Counter()
        for system_log in Path(self.logdir).glob("hosts/*/messages.log"):
            for line in File(str(system_log)).read_lines_filtered(AUTO_REPAIR_LOG_LINE):
                auto_repairs[AUTO_REPAIR_LOG_LINE.search(line)["table_id"]] += 1

        auto_repairs_per_table = {
            f"{keyspace}.{table}": auto_repairs[table_ids[keyspace, table]]
            for keyspace, table in AUTO_REPAIR_TABLES + NO_AUTO_REPAIR_TABLES
        }
        self.log.info("Tablets that auto repair finished, per table: %s", auto_repairs_per_table)
        for keyspace, table in AUTO_REPAIR_TABLES:
            assert auto_repairs_per_table[f"{keyspace}.{table}"], f"No auto repair ran on {keyspace}.{table}"
        for keyspace, table in NO_AUTO_REPAIR_TABLES:
            assert not auto_repairs_per_table[f"{keyspace}.{table}"], (
                f"Auto repair ran on {keyspace}.{table}, where a narrower scope turns it off"
            )
