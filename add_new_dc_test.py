import warnings
from typing import Tuple, List

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from longevity_test import LongevityTest
from sdcm.cluster import BaseNode
from sdcm.stress_thread import CassandraStressThread
from sdcm.utils.common import skip_optional_stage
from sdcm.utils.decorators import optional_stage
from sdcm.utils.replication_strategy_utils import NetworkTopologyReplicationStrategy

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class TestAddNewDc(LongevityTest):
    """Test for procedure:
     https://docs.scylladb.com/operating-scylla/procedures/cluster-management/add-dc-to-existing-dc/
     """

    def test_add_new_dc(self) -> None:

        self.log.info("Starting add new DC test...")
        assert self.params.get('n_db_nodes').endswith(" 0"), "n_db_nodes must be a list and last dc must equal 0"
        system_keyspaces = ["system_distributed", "system_traces"]
        # auth-v2 is used when consistent topology is enabled
        if not self.db_cluster.nodes[0].raft.is_consistent_topology_changes_enabled:
            system_keyspaces.insert(0, "system_auth")

        # reconfigure system keyspaces to use NetworkTopologyStrategy
        status = self.db_cluster.get_nodetool_status()
        self.reconfigure_keyspaces_to_use_network_topology_strategy(
            keyspaces=system_keyspaces,
            replication_factors={dc: len(status[dc].keys()) for dc in status}
        )
        self.prewrite_db_with_data()
        if not skip_optional_stage('main_load'):
            read_thread, write_thread = self.start_stress_during_adding_new_dc()
        # no need to change network topology
        new_node = self.add_node_in_new_dc()

        self.querying_new_node_should_return_no_data(new_node)  # verify issue #8354

        status = self.db_cluster.get_nodetool_status()
        self.reconfigure_keyspaces_to_use_network_topology_strategy(
            keyspaces=system_keyspaces + ["keyspace1"],
            replication_factors={dc: len(status[dc].keys()) for dc in status}
        )

        self.log.info("Running rebuild on each node in new DC")
        new_node.run_nodetool(sub_cmd=f"rebuild -- {list(status.keys())[0]}", publish_event=True)

        self.log.info("Running repair on all nodes")
        for node in self.db_cluster.nodes:
            node.run_nodetool(sub_cmd="repair -pr", publish_event=True)

        if not skip_optional_stage('main_load'):
            # wait for stress to complete
            self.verify_stress_thread(read_thread)
            self.verify_stress_thread(write_thread)

        self.verify_data_can_be_read_from_new_dc(new_node)
        self.log.info("Test completed.")

    def reconfigure_keyspaces_to_use_network_topology_strategy(self, keyspaces: List[str], replication_factors: dict[str, int]) -> None:
        node = self.db_cluster.nodes[0]
        self.log.info("Reconfiguring keyspace Replication Strategy")
        network_topology_strategy = NetworkTopologyReplicationStrategy(
            **replication_factors)
        for keyspace in keyspaces:
            cql = f"ALTER KEYSPACE {keyspace} WITH replication = {network_topology_strategy}"
            node.run_cqlsh(cql)
        self.log.info("Replication Strategies for {} reconfigured".format(keyspaces))

    @optional_stage('prepare_write')
    def prewrite_db_with_data(self) -> None:
        self.log.info("Prewriting database...")
        stress_cmd = self.params.get('prepare_write_cmd')
        pre_thread = self.run_stress_thread(
            stress_cmd=stress_cmd,
            duration=self.params.get('prepare_stress_duration'),
            stats_aggregate_cmds=False,
            round_robin=False,
        )
        self.verify_stress_thread(pre_thread)
        self.log.info("Database pre write completed")

    def start_stress_during_adding_new_dc(self) -> Tuple[CassandraStressThread, CassandraStressThread]:
        self.log.info("Running stress during adding new DC")
        stress_cmds = self.params.get('stress_cmd')
        read_thread = self.run_stress_thread(stress_cmd=stress_cmds[0], stats_aggregate_cmds=False, round_robin=False)
        write_thread = self.run_stress_thread(stress_cmd=stress_cmds[1], stats_aggregate_cmds=False, round_robin=False)
        self.log.info("Stress during adding DC started")
        return read_thread, write_thread

    def add_node_in_new_dc(self) -> BaseNode:
        self.log.info("Adding new node")
        new_node = self.db_cluster.add_nodes(1, dc_idx=1, enable_auto_bootstrap=True)[0]  # add node
        self.db_cluster.wait_for_init(node_list=[new_node], timeout=900,
                                      check_node_health=False)
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=[new_node])
        self.monitors.reconfigure_scylla_monitoring()

        status = self.db_cluster.get_nodetool_status()
        assert len(status.keys()) == 2, f"new datacenter was not registered. Cluster status: {status}"
        self.log.info("New DC to cluster has been added")
        return new_node

    @optional_stage('post_test_load')
    def verify_data_can_be_read_from_new_dc(self, new_node: BaseNode) -> None:
        self.log.info("Veryfing if data has been transferred successfully to the new DC")
        stress_cmd = self.params.get('verify_data_after_entire_test') + f" -node {new_node.ip_address}"
        end_stress = self.run_stress_thread(stress_cmd=stress_cmd, stats_aggregate_cmds=False, round_robin=False)
        self.verify_stress_thread(end_stress)

    def querying_new_node_should_return_no_data(self, new_node: BaseNode) -> None:
        self.log.info("Verifying if querying new node with RF=0 returns no data and does not crash the node. #8354")
        with self.db_cluster.cql_connection_exclusive(new_node) as session:
            statement = SimpleStatement(
                "select * from keyspace1.standard1 limit 1;", consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                fetch_size=10)
            data = session.execute(statement).one()
            assert not data, f"no data should be returned when querying with CL=LOCAL_QUORUM and RF=0. {data}"
