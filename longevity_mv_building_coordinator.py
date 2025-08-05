import random
from uuid import uuid4
from longevity_test import LongevityTest
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.nemesis_utils.indexes import *
from sdcm.utils.raft.common import get_topology_coordinator_node


class LongevityMVBuildingCoordinator(LongevityTest):
    def test_consistency_base_table_and_mv(self):
        InfoEvent("Prepare Base table").publish()
        self.run_prepare_write_cmd()
        coordinator_node = get_topology_coordinator_node(node=self.db_cluster.nodes[0])
        ks_cf_list = self.db_cluster.get_non_system_ks_cf_with_tablets_list(
            coordinator_node, filter_empty_tables=True, filter_out_mv=True, filter_out_table_with_counter=True)
        ks_name, base_table_name = random.choice(ks_cf_list).split('.')
        view_name = f'{base_table_name}_view_{str(uuid4())[:8]}'

        with self.db_cluster.cql_connection_patient(node=coordinator_node) as session:

            create_mv_for_table(session,  keyspace_name=ks_name, base_table_name=base_table_name, view_name=view_name)
            wait_mv_building_tasks_started(session, ks_name, view_name, timeout=600)
            wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=3600)

            result_for_base_table = session.execute(f"select count(*) from {ks_name}.{base_table_name}")
            self.log.debug("Result for base table %s", result_for_base_table)
            result_for_mv_table = session.execute(f"select count(*) from {ks_name}.{view_name}")
            self.log.debug("Result for base table %s", result_for_mv_table)
            assert result_for_base_table == result_for_mv_table, f"Results are different {result_for_base_table} != {result_for_mv_table}"
