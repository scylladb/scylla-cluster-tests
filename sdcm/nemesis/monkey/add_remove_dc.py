from collections.abc import Iterator
from contextlib import ExitStack, contextmanager

from sdcm.cluster import BaseNode
from sdcm.exceptions import KillNemesis, UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass
from sdcm.provision.scylla_yaml import SeedProvider
from sdcm.utils.decorators import retrying, skip_on_capacity_issues
from sdcm.utils.features import is_tablets_feature_enabled
from sdcm.utils.replication_strategy_utils import (
    NetworkTopologyReplicationStrategy,
    ReplicationStrategy,
    temporary_replication_strategy_setter,
)
from sdcm.wait import wait_for_log_lines


class AddRemoveDcNemesis(NemesisBaseClass):
    """
    https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cluster-management/add-dc-to-existing-dc.html
    https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cluster-management/decommissioning-data-center.html

    1. Create and populate a new keyspace
    2. Add some new nodes in a new datacenter
    3. Ensure relevant keyspaces are using NetworkTopologyStrategy
    4. Alter the keyspaces to add the new datacenter with the appropriate replication factor
    5. Run rebuild on the new datacenter nodes, if vnodes
    6. Run a full cluster repair
    7. Run some writes and reads to verify the new datacenter is working
    8. Alter the keyspaces to set replication factor to 0 for the new datacenter
    9. Decommission the new datacenter nodes
    10. Verify the keyspace is healthy after decommissioning the new datacenter
    """

    disruptive = True
    limited = True
    topology_changes = True

    def __init__(self, *args, **kwargs):
        """Initialize new-datacenter state and keyspace defaults."""
        super().__init__(*args, **kwargs)
        self.new_nodes: list[BaseNode] = []
        self.initial_dc_name: str = self.datacenters[0]
        self.new_ks_name: str = "keyspace_new_dc"
        self.new_ks_rf: int = len(self.runner.cluster.racks)

    @property
    def num_nodes_in_new_dc(self) -> int:
        """Return new datacenter size based on supported RF change size."""
        if self.runner.cluster.is_features_enabled_on_node(
            node=self.runner.target_node, feature_list=["KEYSPACE_MULTI_RF_CHANGE"]
        ):
            return 2
        return 1

    @property
    def system_keyspaces(self) -> list[str]:
        """Return system keyspaces"""
        system_keyspaces = ["system_distributed", "system_traces"]
        if (
            not self.runner.target_node.raft.is_consistent_topology_changes_enabled
        ):  # auth-v2 is used when consistent topology is enabled
            system_keyspaces.append("system_auth")
        return system_keyspaces

    @contextmanager
    def temporary_system_keyspaces_network_topology_strategy(self) -> Iterator[None]:
        """Temporarily switch system keyspaces to NetworkTopologyStrategy."""
        with (
            temporary_replication_strategy_setter(self.runner.target_node) as ntrs_setter,
            self.runner.action_log_scope("Temporarily switch system keyspaces to NetworkTopologyStrategy"),
        ):
            for keyspace in self.system_keyspaces:
                old_rs = ReplicationStrategy.get(self.runner.target_node, keyspace)
                if not isinstance(old_rs, NetworkTopologyReplicationStrategy):
                    new_rs = NetworkTopologyReplicationStrategy(**{self.initial_dc_name: old_rs.replication_factors[0]})
                    ntrs_setter(**{keyspace: new_rs})
            yield

    @contextmanager
    def temporary_new_dc_replication_factors(self) -> Iterator[None]:
        """Temporarily update replication factors for the new datacenter."""
        with (
            temporary_replication_strategy_setter(self.runner.target_node) as new_dc_setter,
            self.runner.action_log_scope("Temporarily update replication factors for new datacenter"),
        ):
            for keyspace in self.system_keyspaces + [self.new_ks_name]:
                strategy = ReplicationStrategy.get(self.runner.target_node, keyspace)
                strategy.replication_factors_per_dc.update({self.new_dc_name: self.num_nodes_in_new_dc})
                try:
                    new_dc_setter(**{keyspace: strategy})
                finally:
                    # If the ALTER command times out, the RF is still updated, just didn't finish streaming
                    # When context exits, set RF=0 for the new DC.
                    # Create a separate strategy object so the applied (RF>0) record is not mutated.
                    rollback_strategy = NetworkTopologyReplicationStrategy(
                        **{**strategy.replication_factors_per_dc, self.new_dc_name: 0}
                    )
                    new_dc_setter.preserved[keyspace] = rollback_strategy

            yield

    @property
    def datacenters(self) -> list[str]:
        """Return datacenter names currently reported by nodetool status."""
        return list(self.runner.tester.db_cluster.get_nodetool_status().keys())

    @property
    def new_dc_name(self) -> str | None:
        """Return the temporary nemesis datacenter name if it is registered."""
        if has_suffix := [dc for dc in self.datacenters if dc.endswith("_nemesis_dc")]:
            return has_suffix[0]
        return None

    @retrying(n=3, sleep_time=30, allowed_exceptions=(AssertionError,))
    def assert_new_dc_registered(self) -> None:
        """Assert that the temporary datacenter is visible in cluster status."""
        assert self.new_dc_name, "new datacenter was not registered"

    @retrying(n=3, sleep_time=30, allowed_exceptions=(AssertionError,))
    def assert_new_dc_unregistered(self) -> None:
        """Assert that the temporary datacenter is no longer visible in cluster status."""
        assert not self.new_dc_name, "new datacenter was not unregistered"

    def configure_new_dc(self, node: BaseNode) -> None:
        """Configure node for new datacenter before Scylla starts.

        Args:
            node: New datacenter node to configure.
        """
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

    def add_nodes_in_new_dc(self) -> None:
        """Add and initialize temporary nodes configured to join a new datacenter."""
        add_node_func_args = {
            "count": self.num_nodes_in_new_dc,
            "dc_idx": 0,
            "rack": None,
            "enable_auto_bootstrap": True,
            "disruption_name": self.runner.current_disruption,
            "after_config": self.configure_new_dc,
        }
        with self.runner.action_log_scope("Add nodes in new DC"):
            self.new_nodes = skip_on_capacity_issues(db_cluster=self.runner.tester.db_cluster)(
                self.runner.cluster.add_nodes
            )(**add_node_func_args)
        # wait_for_init() will call node_setup(), which executes the callback after config_setup()
        self.runner.cluster.wait_for_init(node_list=self.new_nodes, check_node_health=False)
        for new_node in self.new_nodes:
            new_node.wait_node_fully_start()
        self.runner.monitoring_set.reconfigure_scylla_monitoring()

    def rebuild_node(self, new_node: BaseNode) -> None:
        """Run nodetool rebuild on a node in the new datacenter.

        Args:
            new_node: New datacenter node to rebuild from the initial datacenter.
        """
        cmd = f"rebuild -- {self.initial_dc_name}"
        with (
            wait_for_log_lines(
                node=new_node,
                start_line_patterns=["rebuild.*started with keyspaces=", "Rebuild starts"],
                end_line_patterns=["rebuild.*finished with keyspaces=", "Rebuild succeeded"],
                start_timeout=60,
                end_timeout=600,
            ),
            self.runner.action_log_scope(f"Run rebuild on {new_node.name} with cmd: {cmd}"),
        ):
            new_node.run_nodetool(sub_cmd=cmd, long_running=True, retry=0)

    def rebuild_nodes_in_new_dc(self) -> None:
        """Run nodetool rebuild on all new datacenter nodes."""
        for new_node in self.new_nodes:
            self.rebuild_node(new_node)

    def create_new_dc_keyspace(self) -> None:
        """Create and populate the keyspace used to validate add/remove DC flow."""
        write_cmd = (
            f"cassandra-stress write no-warmup cl=ALL n=100000 -schema 'keyspace={self.new_ks_name} "
            f"replication(strategy=NetworkTopologyStrategy,{self.initial_dc_name}={self.new_ks_rf}) "
            f"compression=LZ4Compressor compaction(strategy=SizeTieredCompactionStrategy)' "
            f"-mode cql3 native compression=lz4 -rate threads=5 -pop seq=1..100000 -log interval=5"
        )
        write_thread = self.runner.tester.run_stress_thread(
            stress_cmd=write_cmd, round_robin=True, stop_test_on_failure=False
        )
        self.runner.tester.verify_stress_thread(write_thread, error_handler=self.runner._nemesis_stress_failure_handler)
        # flush data to ensure it is seen in monitoring
        for node in self.runner.cluster.nodes:
            with self.runner.action_log_scope(f"Flush data in {self.new_ks_name} on {node.name} node"):
                node.run_nodetool(f"flush {self.new_ks_name}")

    def write_to_multi_dc_keyspace(self) -> None:
        """Write data to the validation keyspace after it is replicated to both datacenters."""
        write_cmd = (
            f"cassandra-stress write no-warmup cl=ALL n=100000 -schema 'keyspace={self.new_ks_name} "
            f"replication(strategy=NetworkTopologyStrategy,{self.initial_dc_name}={self.new_ks_rf},{self.new_dc_name}={self.num_nodes_in_new_dc}) "
            f"compression=LZ4Compressor compaction(strategy=SizeTieredCompactionStrategy)' "
            f"-mode cql3 native compression=lz4 -rate threads=5 -pop seq=100001..200000 -log interval=5"
        )
        write_thread = self.runner.tester.run_stress_thread(
            stress_cmd=write_cmd, round_robin=True, stop_test_on_failure=False
        )
        with self.runner.action_log_scope("Run write data to multiDC keyspace"):
            self.runner.tester.verify_stress_thread(
                write_thread, error_handler=self.runner._nemesis_stress_failure_handler
            )
        # flush data to ensure it is seen in monitoring
        for node in self.runner.cluster.nodes:
            node.run_nodetool(f"flush {self.new_ks_name}")

    def verify_multi_dc_keyspace(self, consistency_level: str = "ALL") -> None:
        """Read validation data from the multi-DC keyspace at the requested consistency level.

        Args:
            consistency_level: Consistency level to use for the cassandra-stress read.
        """
        read_cmd = (
            f"cassandra-stress read no-warmup cl={consistency_level} n=100000 -schema 'keyspace={self.new_ks_name} "
            f"compression=LZ4Compressor' -mode cql3 native compression=lz4 -rate threads=5 "
            f"-pop seq=1..200000 -log interval=5"
        )
        read_thread = self.runner.tester.run_stress_thread(
            stress_cmd=read_cmd, round_robin=True, stop_test_on_failure=False
        )
        with self.runner.action_log_scope("Verify keyspace data"):
            self.runner.tester.verify_stress_thread(
                read_thread, error_handler=self.runner._nemesis_stress_failure_handler
            )

    def decommission_new_nodes(self) -> None:
        """Decommission temporary datacenter nodes and clear local node tracking."""
        with self.runner.action_log_scope(f"Decommission nodes in the new datacenter {self.new_dc_name}"):
            self.runner.decommission_nodes(self.new_nodes)
        self.new_nodes = []

    def finalizer(self, exc_type, *_):
        """Clean up the validation keyspace and temporary nodes on regular exits.

        Args:
            exc_type: Exception type received from the ExitStack callback, if any.
            *_: Additional exception callback arguments ignored by this finalizer.
        """
        # in case of test end/killed, leave the cleanup alone
        if exc_type is not KillNemesis:
            with self.runner.cluster.cql_connection_patient(self.runner.target_node) as session:
                session.execute(f"DROP KEYSPACE IF EXISTS {self.new_ks_name}")
            if self.new_nodes:
                self.decommission_new_nodes()

    def disrupt(self) -> None:
        """Execute the add/remove datacenter nemesis workflow."""
        if self.runner.cluster.test_config.MULTI_REGION:
            raise UnsupportedNemesis(
                "Skipped for multi-dc scenario (https://github.com/scylladb/scylla-cluster-tests/issues/5369)"
            )

        with ExitStack() as context_manager:
            context_manager.push(self.finalizer)
            self.create_new_dc_keyspace()
            self.add_nodes_in_new_dc()
            self.assert_new_dc_registered()

            with self.temporary_system_keyspaces_network_topology_strategy():
                with self.temporary_new_dc_replication_factors():
                    if not is_tablets_feature_enabled(self.runner.target_node):
                        self.rebuild_nodes_in_new_dc()
                    self.runner.run_repair()

                    self.write_to_multi_dc_keyspace()
                    self.verify_multi_dc_keyspace(consistency_level="ALL")

                self.decommission_new_nodes()
                self.assert_new_dc_unregistered()
                self.verify_multi_dc_keyspace(consistency_level="QUORUM")
