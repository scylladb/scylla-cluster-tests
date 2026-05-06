from contextlib import ExitStack
from typing import List

from sdcm.cluster import BaseNode
from sdcm.exceptions import KillNemesis, UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass
from sdcm.provision.scylla_yaml import SeedProvider
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.decorators import skip_on_capacity_issues
from sdcm.utils.replication_strategy_utils import (
    NetworkTopologyReplicationStrategy,
    ReplicationStrategy,
    SimpleReplicationStrategy,
    temporary_replication_strategy_setter,
)
from sdcm.wait import wait_for_log_lines


class AddRemoveDcNemesis(NemesisBaseClass):
    disruptive = True
    limited = True
    topology_changes = True

    def _configure_new_dc(self, node: BaseNode):
        """Configure node for new datacenter before Scylla starts."""
        with node.remote_scylla_yaml() as scylla_yml:
            scylla_yml.rpc_address = node.ip_address
            scylla_yml.seed_provider = [
                SeedProvider(
                    class_name="org.apache.cassandra.locator.SimpleSeedProvider",
                    parameters=[{"seeds": ",".join(self.runner.tester.db_cluster.seed_nodes_addresses)}],
                )
            ]

        endpoint_snitch = self.runner.cluster.params.get("endpoint_snitch") or ""
        if endpoint_snitch.endswith("GossipingPropertyFileSnitch"):
            rackdc_value = {"dc": "add_remove_nemesis_dc"}
        else:
            rackdc_value = {"dc_suffix": "_nemesis_dc"}

        with node.remote_cassandra_rackdc_properties() as properties_file:
            properties_file.update(**rackdc_value)

    def _add_new_node_in_new_dc(self, is_zero_node=False) -> BaseNode:
        add_node_func_args = {
            "count": 1,
            "dc_idx": 0,
            "enable_auto_bootstrap": True,
            "disruption_name": self.runner.current_disruption,
            "after_config": self._configure_new_dc,
            **({"is_zero_node": is_zero_node} if is_zero_node else {}),
        }
        new_node = skip_on_capacity_issues(db_cluster=self.runner.tester.db_cluster)(self.runner.cluster.add_nodes)(
            **add_node_func_args
        )[0]
        # wait_for_init() will call node_setup(), which executes the callback after config_setup()
        self.runner.cluster.wait_for_init(node_list=[new_node], timeout=900, check_node_health=False)
        new_node.wait_node_fully_start()
        self.runner.monitoring_set.reconfigure_scylla_monitoring()
        return new_node

    def _write_read_data_to_multi_dc_keyspace(self, dc_rf3: str, dc_rf1: str) -> None:
        InfoEvent(message="Writing and reading data with new dc").publish()
        write_cmd = (
            f"cassandra-stress write no-warmup cl=ALL n=100000 -schema 'keyspace=keyspace_new_dc "
            f"replication(strategy=NetworkTopologyStrategy,{dc_rf3}=3,{dc_rf1}=1) "
            f"compression=LZ4Compressor compaction(strategy=SizeTieredCompactionStrategy)' "
            f"-mode cql3 native compression=lz4 -rate threads=5 -pop seq=1..100000 -log interval=5"
        )
        write_thread = self.runner.tester.run_stress_thread(
            stress_cmd=write_cmd, round_robin=True, stop_test_on_failure=False
        )
        self.runner.tester.verify_stress_thread(write_thread, error_handler=self.runner._nemesis_stress_failure_handler)
        with self.runner.action_log_scope("Verify multi DC keyspace data"):
            self._verify_multi_dc_keyspace_data(consistency_level="ALL")
        # flush data to ensure it is seen in monitoring
        for node in self.runner.cluster.nodes:
            with self.runner.action_log_scope(f"Flush data in keyspace_new_dc on {node.name} node"):
                node.run_nodetool("flush keyspace_new_dc")

    def _verify_multi_dc_keyspace_data(self, consistency_level: str = "ALL"):
        read_cmd = (
            f"cassandra-stress read no-warmup cl={consistency_level} n=100000 -schema 'keyspace=keyspace_new_dc "
            f"compression=LZ4Compressor' -mode cql3 native compression=lz4 -rate threads=5 "
            f"-pop seq=1..100000 -log interval=5"
        )
        read_thread = self.runner.tester.run_stress_thread(
            stress_cmd=read_cmd, round_robin=True, stop_test_on_failure=False
        )
        self.runner.tester.verify_stress_thread(read_thread, error_handler=self.runner._nemesis_stress_failure_handler)

    def _switch_to_network_replication_strategy(self, keyspaces: List[str]) -> None:
        """Switches replication strategy to NetworkTopology for given keyspaces."""
        node = self.runner.cluster.data_nodes[0]
        nodes_by_region = self.runner.tester.db_cluster.nodes_by_region(nodes=self.runner.tester.db_cluster.data_nodes)
        region = list(nodes_by_region.keys())[0]
        dc_name = self.runner.tester.db_cluster.get_nodetool_info(nodes_by_region[region][0])["Data Center"]
        for keyspace in keyspaces:
            replication_strategy = ReplicationStrategy.get(node, keyspace)
            if not isinstance(replication_strategy, SimpleReplicationStrategy):
                # no need to switch as already is NetworkTopology
                continue
            self.runner.log.info(f"Switching replication strategy to Network for '{keyspace}' keyspace")
            if keyspace == "system_auth" and replication_strategy.replication_factors[0] != len(
                nodes_by_region[region]
            ):
                self.runner.log.warning(
                    f"system_auth keyspace is not replicated on all nodes "
                    f"({replication_strategy.replication_factors[0]}/{len(nodes_by_region[region])})."
                )
            with self.runner.action_log_scope(f"Switching {keyspace} replication strategy to Network"):
                network_replication = NetworkTopologyReplicationStrategy(
                    **{dc_name: replication_strategy.replication_factors[0]}
                )
                network_replication.apply(node, keyspace)

    def disrupt(self) -> None:
        if self.runner.cluster.test_config.MULTI_REGION:
            raise UnsupportedNemesis(
                "add_remove_dc skipped for multi-dc scenario (https://github.com/scylladb/scylla-cluster-tests/issues/5369)"
            )
        InfoEvent(message="Starting New DC Nemesis").publish()
        node = self.runner.cluster.data_nodes[0]
        system_keyspaces = ["system_distributed", "system_traces"]
        if not node.raft.is_consistent_topology_changes_enabled:  # auth-v2 is used when consistent topology is enabled
            system_keyspaces.insert(0, "system_auth")
        self._switch_to_network_replication_strategy(self.runner.cluster.get_test_keyspaces() + system_keyspaces)
        datacenters = list(self.runner.tester.db_cluster.get_nodetool_status().keys())
        initial_dc_name = datacenters[0]
        self.runner.tester.create_keyspace(
            "keyspace_new_dc", replication_factor={initial_dc_name: min(3, len(self.runner.cluster.data_nodes))}
        )
        node_added = False
        with ExitStack() as context_manager:

            def finalizer(exc_type, *_):
                # in case of test end/killed, leave the cleanup alone
                if exc_type is not KillNemesis:
                    with self.runner.cluster.cql_connection_patient(node) as session:
                        session.execute("DROP KEYSPACE IF EXISTS keyspace_new_dc")
                    if node_added:
                        self.runner.cluster.decommission(new_node)

            context_manager.push(finalizer)

            with temporary_replication_strategy_setter(node) as replication_strategy_setter:
                with self.runner.action_log_scope("Add new node in new DC"):
                    new_node = self._add_new_node_in_new_dc()
                node_added = True
                status = self.runner.tester.db_cluster.get_nodetool_status()
                new_dc_list = [dc for dc in list(status.keys()) if dc.endswith("_nemesis_dc")]
                assert new_dc_list, "new datacenter was not registered"
                new_dc_name = new_dc_list[0]
                for keyspace in system_keyspaces + ["keyspace_new_dc"]:
                    strategy = ReplicationStrategy.get(node, keyspace)
                    assert isinstance(strategy, NetworkTopologyReplicationStrategy), (
                        "Should have been already switched to NetworkStrategy"
                    )
                    strategy.replication_factors_per_dc.update({new_dc_name: 1})
                    replication_strategy_setter(**{keyspace: strategy})

                for key, preserved_strategy in replication_strategy_setter.preserved.items():
                    preserved_strategy.replication_factors_per_dc[new_dc_name] = 0

                InfoEvent(message="execute rebuild on new datacenter").publish()
                cmd = f"rebuild -- {initial_dc_name}"
                with (
                    wait_for_log_lines(
                        node=new_node,
                        start_line_patterns=["rebuild.*started with keyspaces=", "Rebuild starts"],
                        end_line_patterns=["rebuild.*finished with keyspaces=", "Rebuild succeeded"],
                        start_timeout=60,
                        end_timeout=600,
                    ),
                    self.runner.action_log_scope(f"Run rebuild on the new datacenter with cmd: {cmd}"),
                ):
                    new_node.run_nodetool(sub_cmd=cmd, long_running=True, retry=0)
                InfoEvent(message="Running full cluster repair on each data node").publish()
                cmd = "repair -pr"
                for cluster_node in self.runner.cluster.data_nodes:
                    with self.runner.action_log_scope(f"Run repair on {cluster_node.name} node with cmd: {cmd}"):
                        cluster_node.run_nodetool(sub_cmd=cmd, publish_event=True)
                with self.runner.action_log_scope("Run write and then read data to multiDC keyspace"):
                    self._write_read_data_to_multi_dc_keyspace(dc_rf3=initial_dc_name, dc_rf1=new_dc_name)

            with self.runner.action_log_scope(f"Decommission of the new node {new_node.name}"):
                self.runner.cluster.decommission(new_node)
            node_added = False
            self.runner.node_allocator.unset_running_nemesis(new_node, self.runner.current_disruption)

            datacenters = list(self.runner.tester.db_cluster.get_nodetool_status().keys())
            assert not [dc for dc in datacenters if dc.endswith("_nemesis_dc")], "new datacenter was not unregistered"
            with self.runner.action_log_scope("Verify keyspace data after decommissioning"):
                self._verify_multi_dc_keyspace_data(consistency_level="QUORUM")
