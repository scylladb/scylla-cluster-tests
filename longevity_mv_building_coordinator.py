import contextlib
import random
import re
from uuid import uuid4

from longevity_test import LongevityTest
from sdcm.cluster import BaseScyllaCluster, BaseMonitorSet, BaseNode, BaseCluster
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.sct_events.group_common_events import ignore_stream_mutation_fragments_errors, ignore_ycsb_connection_refused
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.decorators import retrying
from sdcm.utils.issues import SkipPerIssues
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

            result_for_base_table = list(session.execute(f"select count(*) from {ks_name}.{base_table_name}"))
            self.log.debug("Result for base table %s", list(result_for_base_table))
            result_for_mv_table = list(session.execute(f"select count(*) from {ks_name}.{view_name}"))
            self.log.debug("Result for mv table %s", list(result_for_mv_table))
            assert result_for_base_table == result_for_mv_table, f"Results are different {result_for_base_table} != {result_for_mv_table}"

            mv_primary_key_columns = get_column_names(session, ks=ks_name, cf=view_name, is_primary_key=True)
            self.log.debug("primary keys: %s", mv_primary_key_columns)
            mv_primary_key_columns.sort()
            result_for_mv_table = session.execute(
                f"select {', '.join([f'"{c}"' for c in mv_primary_key_columns])} from {ks_name}.{view_name}")
            for row in result_for_mv_table:
                key_values = list(row)
                where_clause = [f'"{key}" = "{value}"' for key, value in zip(mv_primary_key_columns, key_values)]

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

    def test_topology_operation_replace_during_mv_building(self):
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
                             replacing_node.dc_idx, replacing_node.rack, monitoring=self.monitors)
        wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=1800)

    def test_topology_operation_remove_during_mv_building(self):
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

        try:
            wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=60)
        except TimeoutError:
            self.log.info("MV is building")

        removing_node: BaseNode = random.choice([node for node in self.db_cluster.nodes if node != coordinator_node])
        removing_node_hostid = removing_node.host_id
        removing_node.stop_scylla()
        remove_cluster_node(self.db_cluster, coordinator_node, node_to_remove=removing_node, removing_node_host_id=removing_node_hostid,
                            monitoring=self.monitors)
        wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=3600)

    def test_topology_operation_decommission_during_mv_building(self):
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

        try:
            wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=60)
        except TimeoutError:
            self.log.info("MV is building")

        decommission_node: BaseNode = random.choice(
            [node for node in self.db_cluster.nodes if node != coordinator_node])
        self.db_cluster.decommission(decommission_node)
        self.db_cluster.verify_decommission(decommission_node)

        wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=3600)

    def test_topology_operation_bootstrap_during_mv_building(self):
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

        try:
            wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=60)
        except TimeoutError:
            self.log.info("MV is building")

        new_node: BaseNode = add_cluster_node(
            self.db_cluster, dc_idx=coordinator_node.dc_idx, rack=coordinator_node.rack, monitoring=self.monitors)

        wait_for_view_to_be_built(coordinator_node, ks_name, view_name, timeout=3600)
        with self.db_cluster.cql_connection_exclusive(node=new_node) as session:
            session.default_timeout = 600
            result_for_base_table = list(session.execute(f"select count(*) from {ks_name}.{base_table_name}"))
            self.log.debug("Result for base table %s", list(result_for_base_table))
            result_for_mv_table = list(session.execute(f"select count(*) from {ks_name}.{view_name}"))
            self.log.debug("Result for mv table %s", list(result_for_mv_table))
            assert result_for_base_table == result_for_mv_table, f"Results are different {result_for_base_table} != {result_for_mv_table}"


def replace_cluster_node(cluster: "BaseScyllaCluster", verification_node: "BaseNode",
                         replacing_host_id: str | None = None,
                         dc_idx: int = 0,
                         rack: int = 0,
                         ignore_dead_node_host_ids: str = "",
                         monitoring: BaseMonitorSet | None = None,
                         timeout: int | float = 3600 * 8) -> "BaseNode":
    """When old_node_ip or host_id are not None then replacement node procedure is initiated"""
    cluster.log.info("Adding new node to cluster...")
    new_node: "BaseNode" = cluster.add_nodes(count=1, dc_idx=dc_idx, rack=rack, enable_auto_bootstrap=True)[0]
    if monitoring is not None:
        monitoring.reconfigure_scylla_monitoring()
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


def remove_cluster_node(cluster: BaseScyllaCluster | BaseCluster, verification_node: "BaseNode", node_to_remove: "BaseNode",
                        removing_node_host_id: str = "", ignore_dead_nodes_host_ids: list[str] | None = None,
                        monitoring: BaseMonitorSet | None = None):
    # node_to_remove must be different than node
    # node_to_remove is single/last seed in cluster, before
    # it will be terminated, choose new seed node
    num_of_seed_nodes = len(cluster.seed_nodes)
    if node_to_remove.is_seed and num_of_seed_nodes < 2:
        new_seed_node = random.choice([n for n in cluster.nodes if n is not node_to_remove])
        new_seed_node.set_seed_flag(True)
        cluster.update_seed_provider()

    # get node's host_id
    if not removing_node_host_id:
        removed_node_status = cluster.get_node_status_dictionary(
            ip_address=node_to_remove.ip_address, verification_node=verification_node)
        assert removed_node_status is not None, "failed to get host_id using nodetool status"
        removing_node_host_id = removed_node_status["host_id"]

    if SkipPerIssues('https://github.com/scylladb/scylladb/issues/21815', params=cluster.params):
        # TBD: To be removed after https://github.com/scylladb/scylladb/issues/21815 is resolved
        ignore_stream_mutation_errors_due_to_issue = ignore_stream_mutation_fragments_errors
    else:
        ignore_stream_mutation_errors_due_to_issue = contextlib.nullcontext

    with ignore_ycsb_connection_refused(), ignore_stream_mutation_errors_due_to_issue():
        # node stop and make sure its "DN"
        node_to_remove.stop_scylla_server(verify_up=False, verify_down=True)

        # terminate node
        cluster.terminate_node(node_to_remove)
        if monitoring is not None:
            monitoring.reconfigure_scylla_monitoring()

    @retrying(n=3, sleep_time=5, message="Removing node from cluster...")
    def remove_node():
        removenode_reject_msg = r"Rejected removenode operation.*the node being removed is alive"
        res = verification_node.run_nodetool(f"removenode {removing_node_host_id}",
                                             ignore_status=True, verbose=True, long_running=True, retry=0)
        if res.failed and re.match(removenode_reject_msg, res.stdout + res.stderr):
            raise Exception(f"Removenode was rejected {res.stdout}\n{res.stderr}")

        return res.exit_status

    # full cluster repair
    up_normal_nodes = cluster.get_nodes_up_and_normal(verification_node)
    # Repairing will result in a best effort repair due to the terminated node,
    # and as a result requires ignoring repair errors
    with DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR,
                        line="failed to repair"):
        for node in up_normal_nodes:
            try:
                node.run_nodetool(sub_cmd="repair", publish_event=True, ignore_status=True)
            except Exception as err:  # pylint: disable=broad-except  # noqa: BLE001
                cluster.log.warning(f"Repair failed to complete on node: {node}, with error: {str(err)}")

    exit_status = remove_node()
    # if remove node command failed by any reason,
    # we will remove the terminated node from
    # dead_nodes_list, so the health validator terminate the job
    if exit_status != 0:
        cluster.log.error(f"nodetool removenode command exited with status {exit_status}")
        # check difference between group0 and token ring,
        garbage_host_ids = verification_node.raft.get_diff_group0_token_ring_members()
        cluster.log.debug("Difference between token ring and group0 is %s", garbage_host_ids)
        if garbage_host_ids:
            # if difference found, clean garbage and continue
            verification_node.raft.clean_group0_garbage()
        else:
            # group0 and token ring are consistent. Removenode failed by meanigfull reason.
            # remove node from dead_nodes list to raise critical issue by HealthValidator
            self.log.debug(
                f"Remove failed node {node_to_remove} from dead node list {self.cluster.dead_nodes_list}")
            node = next((n for n in cluster.dead_nodes_list if n.ip_address ==
                         node_to_remove.ip_address), None)
            if node:
                cluster.dead_nodes_list.remove(node)
            else:
                cluster.log.debug(f"Node {node.name} with ip {node.ip_address} was not found in dead_nodes_list")

    # verify node is removed by nodetool status
    removed_node_status = cluster.get_node_status_dictionary(
        ip_address=node_to_remove.ip_address, verification_node=verification_node)
    assert removed_node_status is None, \
        "Node was not removed properly (Node status:{})".format(removed_node_status)


def add_cluster_node(cluster: BaseCluster | BaseScyllaCluster, dc_idx: int = 0, rack: int = 0, monitoring: BaseMonitorSet | None = None) -> "BaseNode":
    cluster.log.info("Adding new node")
    new_node = cluster.add_nodes(1, dc_idx=dc_idx, rack=rack, enable_auto_bootstrap=True)[0]
    cluster.wait_for_init(node_list=[new_node], timeout=900,
                          check_node_health=True)
    cluster.wait_for_nodes_up_and_normal(nodes=[new_node])
    if monitoring is not None:
        monitoring.reconfigure_scylla_monitoring()

    return new_node
