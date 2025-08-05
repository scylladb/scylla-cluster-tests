import random
from uuid import uuid4

from longevity_test import LongevityTest
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.nemesis_utils.indexes import *
from sdcm.utils.raft.common import get_topology_coordinator_node
from sdcm.cluster import BaseNode


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

            result_for_base_table = list(session.execute(f"select count(*) from {ks_name}.{base_table_name}"))
            self.log.debug("Result for base table %s", list(result_for_base_table))
            result_for_mv_table = list(session.execute(f"select count(*) from {ks_name}.{view_name}"))
            self.log.debug("Result for mv table %s", list(result_for_mv_table))
            assert result_for_base_table == result_for_mv_table, f"Results are different {result_for_base_table} != {result_for_mv_table}"

            mv_primary_key_columns = get_column_names(session, ks=ks_name, cf=view_name, is_primary_key=True)
            self.log.debug("primary keys: %s", mv_primary_key_columns)
            mv_primary_key_columns.sort()
            result_for_mv_table = list(session.execute(
                f"select {', '.join([f"'{c}'" for c in mv_primary_key_columns])} from {ks_name}.{view_name}"))
            self.log.debug("Result for mv table %s", result_for_mv_table)
            for row in result_for_mv_table:
                key_values = list(row)
                where_clause = [f"{key} = {value}" for key, value in zip(mv_primary_key_columns, key_values)]

                result1 = session.execute(
                    f"select * from {ks_name}.{base_table_name} where {' and '.join(where_clause)} ALLOW_FILTERING")
                result2 = session.execute(f"select * from {ks_name}.{view_name} where {' and '.join(where_clause)}")
                normalized_results1 = sorted([sorted(list(row)) for row in result1], key=lambda x: x[0])
                normalized_results2 = sorted([sorted(list(row)) for row in result2], key=lambda x: x[0])

                assert normalized_results1 == normalized_results2, f"ERROR! ERROR! list are wrong {normalized_results1} != {normalized_results2}"

    def test_stop_node_during_building_mv(self):
        InfoEvent("Prepare Base table").publish()
        self.run_prepare_write_cmd()
        coordinator_node = get_topology_coordinator_node(node=self.db_cluster.nodes[0])
        ks_cf_list = self.db_cluster.get_non_system_ks_cf_with_tablets_list(
            coordinator_node, filter_empty_tables=True, filter_out_mv=True, filter_out_table_with_counter=True)
        ks_name, base_table_name = random.choice(ks_cf_list).split('.')
        view_name = f'{base_table_name}_view_{str(uuid4())[:8]}'

        with self.db_cluster.cql_connection_patient(node=coordinator_node) as session:
            create_mv_for_table(session, keyspace_name=ks_name, base_table_name=base_table_name, view_name=view_name)
            wait_mv_building_tasks_started(session, ks_name, view_name, timeout=600)
        stopping_node: BaseNode = random.choice([node for node in self.db_cluster.nodes if node != coordinator_node])
        stopping_node.stop_scylla()
        try:
            wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=1800)
            raise Exception("MV was built")
        except TimeoutError:
            InfoEvent(f"MV {ks_name}.{view_name} was not built during 1800 minutes")

        stopping_node.start_scylla()
        wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=1800)

    def test_topology_operation_during_mv_building(self):
        InfoEvent("Prepare Base table").publish()
        self.run_prepare_write_cmd()
        coordinator_node = get_topology_coordinator_node(node=self.db_cluster.nodes[0])
        ks_cf_list = self.db_cluster.get_non_system_ks_cf_with_tablets_list(
            coordinator_node, filter_empty_tables=True, filter_out_mv=True, filter_out_table_with_counter=True)
        ks_name, base_table_name = random.choice(ks_cf_list).split('.')
        view_name = f'{base_table_name}_view_{str(uuid4())[:8]}'

        with self.db_cluster.cql_connection_patient(node=coordinator_node) as session:
            create_mv_for_table(session, keyspace_name=ks_name, base_table_name=base_table_name, view_name=view_name)
            wait_mv_building_tasks_started(session, ks_name, view_name, timeout=600)

        replacing_node: BaseNode = random.choice([node for node in self.db_cluster.nodes if node != coordinator_node])
        replacing_node_hostid = replacing_node.host_id
        replacing_node.stop_scylla()
        replace_cluster_node(self.db_cluster, coordinator_node, replacing_node_hostid,
                             replacing_node.dc_idx, replacing_node.rack)
        wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=1800)


def replace_cluster_node(cluster, verification_node: "BaseNode",
                         replacing_host_id: str | None = None,
                         dc_idx: int = 0,
                         rack: int = 0,
                         ignore_dead_node_host_ids: str = "",
                         timeout: int | float = 3600 * 8) -> "BaseNode":
    """When old_node_ip or host_id are not None then replacement node procedure is initiated"""
    cluster.log.info("Adding new node to cluster...")
    new_node: "BaseNode" = cluster.add_nodes(count=1, dc_idx=dc_idx, rack=rack, enable_auto_bootstrap=True)[0]
    cluster.monitor.reconfigure_scylla_monitoring()
    new_node.remoter.sudo(
        f"""echo 'ignore_dead_nodes_for_replace: {ignore_dead_node_host_ids}' | sudo tee -a  /etc/scylla/scylla.yaml""")
    new_node.replacement_host_id = replacing_host_id

    try:
        cluster.wait_for_init(node_list=[new_node], timeout=timeout, check_node_health=False)
        cluster.clean_replacement_node_options(new_node)
        cluster.set_seeds()
        cluster.update_seed_provider()
    except Exception:
        cluster.log.warning("TestConfig of the '%s' failed, removing it from list of nodes" % new_node)
        cluster.nodes.remove(new_node)
        cluster.log.warning("Node will not be terminated. Please terminate manually!!!")
        raise

    cluster.wait_for_nodes_up_and_normal(nodes=[new_node], verification_node=verification_node)
    new_node.wait_node_fully_start()
    new_node.remoter.sudo(
        f"""sed -i 's/ignore_dead_nodes_for_replace: {ignore_dead_node_host_ids}/# ignore_dead_nodes_for_replace:/' /etc/scylla/scylla.yaml""")

    return new_node
