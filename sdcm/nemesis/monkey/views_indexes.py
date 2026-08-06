"""
Module containing materialized view (MV) and secondary index (SI) nemesis classes.

Includes:
- ``CreateIndexNemesis`` — create a secondary index on a random column, wait for
  it to be built, verify it can be queried, then drop it.
- ``AddRemoveMvNemesis`` — create a materialized view while a node is down, bring
  the node back and repair, verify the MV, then drop it.
- ``KillMVBuildingCoordinator`` — repeatedly kill the MV building coordinator
  (raft topology coordinator) while a view is being built and verify the view is
  still successfully built after re-election.
"""

import logging
from uuid import uuid4

from cassandra import InvalidRequest
from cassandra.query import SimpleStatement

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass, target_data_nodes, target_all_nodes
from sdcm.nemesis.utils.indexes import (
    ViewFinishedBuildingException,
    get_random_column_name,
    create_index,
    wait_for_index_to_be_built,
    verify_query_by_index_works,
    drop_index,
    wait_for_view_to_be_built,
    drop_materialized_view,
    create_materialized_view_for_random_column,
    wait_materialized_view_building_tasks_started,
)
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocationError
from sdcm.sct_events.group_common_events import (
    decorate_with_context,
    suppress_expected_unavailability_errors,
)
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.common import sleep_for_percent_of_duration
from sdcm.utils.context_managers import DbNodeLogger
from sdcm.utils.diagnostic_collector.handlers import EventExceptionHandler
from sdcm.utils.diagnostic_collector.manager import collect_diagnostics
from sdcm.utils.diagnostic_collector.views import MVDiagnosticCollector
from sdcm.utils.features import is_tablets_feature_enabled, is_views_with_tablets_enabled
from sdcm.utils.issues import SkipPerIssues
from sdcm.utils.raft.common import get_topology_coordinator_node
from sdcm.utils.version_utils import ComparableScyllaVersion

LOGGER = logging.getLogger(__name__)


@target_data_nodes
class CreateIndexNemesis(NemesisBaseClass):
    disruptive = False
    schema_changes = True
    free_tier_set = True
    supports_high_disk_utilization = False  # Creating an Index consumes disk space

    def disrupt(self):
        """
        Create index on a random column (regular or static) of a table with the most number of partitions and wait until it gets build.
        Then verify it can be used in a query. Finally, drop the index.
        """
        if self.runner.cluster.nemesis_count > 1 and SkipPerIssues(
            issues="https://github.com/scylladb/scylladb/issues/21695", params=self.runner.tester.params
        ):
            raise UnsupportedNemesis("Skip create index nemesis with parallel nemesis run")

        # Disable MV tests with tablets.
        if is_tablets_feature_enabled(self.runner.target_node):
            if ComparableScyllaVersion(self.runner.target_node.scylla_version) <= ComparableScyllaVersion("2025.3.99"):
                raise UnsupportedNemesis("MV/SI for tablets are not supported for Scylla 2025.3 and older versions")

        with (
            self.runner.cluster.cql_connection_patient(self.runner.target_node, connect_timeout=300) as session,
            collect_diagnostics(MVDiagnosticCollector(session, exception_handler=EventExceptionHandler())),
        ):
            ks_cf_list = self.runner.cluster.get_non_system_ks_cf_list(self.runner.target_node, filter_out_mv=True)
            if not ks_cf_list:
                raise UnsupportedNemesis("No table found to create index on")
            ks, cf = self.runner.random.choice(ks_cf_list).split(".")
            column = get_random_column_name(
                session, ks, cf, filter_out_static_columns=True, filter_out_column_types=["counter"]
            )
            if not column:
                raise UnsupportedNemesis("No column found to create index on")
            try:
                with (
                    DbNodeLogger(
                        self.runner.cluster.nodes,
                        "create index",
                        target_node=self.runner.target_node,
                        additional_info=f"on {ks}.{cf}.{column}",
                    ),
                    self.runner.action_log_scope(f"Create {ks}.{cf} {column} index"),
                ):
                    index_name = create_index(session, ks, cf, column)
            except InvalidRequest as exc:
                LOGGER.warning(exc)
                raise UnsupportedNemesis("Tried to create already existing index. See log for details")
            try:
                with adaptive_timeout(
                    operation=Operations.CREATE_INDEX, node=self.runner.target_node, timeout=14400
                ) as timeout:
                    with self.runner.action_log_scope("Wait for index to be built"):
                        wait_for_index_to_be_built(self.runner.target_node, ks, index_name, timeout=timeout * 2)
                verify_query_by_index_works(session, ks, cf, column)
                sleep_for_percent_of_duration(
                    self.runner.tester.test_duration * 60, percent=1, min_duration=300, max_duration=2400
                )
            finally:
                with DbNodeLogger(
                    self.runner.cluster.nodes,
                    "drop_index",
                    target_node=self.runner.target_node,
                    additional_info=f"index: {index_name}",
                ):
                    self.runner.actions_log.info(f"Drop {index_name} index")
                    drop_index(session, ks, index_name)


@target_data_nodes
class AddRemoveMvNemesis(NemesisBaseClass):
    disruptive = True
    schema_changes = True
    free_tier_set = True
    supports_high_disk_utilization = False  # Creating an MV consumes disk space

    def disrupt(self):
        """
        Create a Materialized view on an existing table while a node is down.
        Take node up and run a repair.
        Verify the MV can be used in a query.
        Finally, drop the MV.
        """

        # Disable MV tests with tablets.
        if is_tablets_feature_enabled(self.runner.target_node):
            if ComparableScyllaVersion(self.runner.target_node.scylla_version) <= ComparableScyllaVersion("2025.3.99"):
                raise UnsupportedNemesis("MV for tablets are not supported for Scylla 2025.3 and older versions")

        free_nodes = [node for node in self.runner.cluster.data_nodes if not node.running_nemesis]
        if not free_nodes:
            raise UnsupportedNemesis("Not enough free nodes for nemesis. Skipping.")
        cql_query_executor_node = self.runner.random.choice(free_nodes)
        with self.runner.node_allocator.nodes_running_nemesis(cql_query_executor_node, self.runner.current_disruption):
            ks_cfs = self.runner.cluster.get_non_system_ks_cf_list(
                db_node=cql_query_executor_node,
                filter_empty_tables=True,
                filter_out_mv=True,
                filter_out_table_with_counter=True,
            )
            if not ks_cfs:
                raise UnsupportedNemesis("Non-system keyspace and table are not found. nemesis can't be run")
            ks_name, base_table_name = self.runner.random.choice(ks_cfs).split(".")
            view_name = f"{base_table_name}_view"
            with suppress_expected_unavailability_errors():
                self.runner.target_node.stop_scylla()
            try:
                with (
                    self.runner.cluster.cql_connection_patient(
                        node=cql_query_executor_node, connect_timeout=600
                    ) as session,
                    collect_diagnostics(MVDiagnosticCollector(session, exception_handler=EventExceptionHandler())),
                ):
                    try:
                        create_materialized_view_for_random_column(session, ks_name, base_table_name, view_name)
                    except Exception as error:
                        self.runner.log.warning("Failed creating a materialized view: %s", error)
                        raise
                    try:
                        self.runner.log.info("Starting Scylla on node %s", self.runner.target_node.name)
                        self.runner.actions_log.info(f"Start Scylla on {self.runner.target_node.name} node")
                        self.runner.target_node.start_scylla()
                        with self.runner.action_log_scope(f"Run repair on {self.runner.target_node.name} node"):
                            self.runner.target_node.run_nodetool(sub_cmd="repair -pr")
                        with (
                            adaptive_timeout(
                                operation=Operations.CREATE_MV, node=self.runner.target_node, timeout=14400
                            ) as timeout,
                            self.runner.action_log_scope(
                                f"Wait for {ks_name}.{view_name} materialized view to be built on "
                                f"{self.runner.target_node.name} node"
                            ),
                        ):
                            wait_for_view_to_be_built(self.runner.target_node, ks_name, view_name, timeout=timeout * 2)
                        session.execute(SimpleStatement(f"SELECT * FROM {ks_name}.{view_name} limit 1", fetch_size=10))
                        sleep_for_percent_of_duration(
                            self.runner.tester.test_duration * 60, percent=1, min_duration=300, max_duration=2400
                        )
                    finally:
                        with self.runner.action_log_scope("Drop materialized view"):
                            drop_materialized_view(session, ks_name, view_name)
            except Exception:
                # Covers both a failed cql_connection_patient (node never restarted otherwise) and
                # any failure surfaced from inside the session block (start_scylla is a no-op if
                # the node is already up).
                self.runner.target_node.start_scylla()
                raise


@target_all_nodes
class KillMVBuildingCoordinator(NemesisBaseClass):
    disruptive = True
    schema_changes = True
    topology_changes = True
    supports_high_disk_utilization = False  # Creating an MV consumes disk space

    @decorate_with_context(suppress_expected_unavailability_errors)
    def disrupt(self):
        """
        MV building coordinator is responsible for building MV from base table in
        keyspaces with tablets enabled and located on the same node as raft topology coordinator.
        If mv building coordinator is died during the mv building process, new mv building coordinator
        as (group0 leader and raft topology coordinator) should be elected and mv building process continue.

        Nemesis kill mv building coordinator several times while materialized view is being built,
        and validate that after the node is restarted, the view is successfully built.
        """
        if not self.runner.target_node.raft.is_consistent_topology_changes_enabled:
            raise UnsupportedNemesis("Consistent topology changes feature is disabled")

        if not is_tablets_feature_enabled(self.runner.target_node):
            raise UnsupportedNemesis("MV building coordinator works only with tablets")

        with self.runner.cluster.cql_connection_patient(node=self.runner.target_node, connect_timeout=600) as session:
            if not is_views_with_tablets_enabled(session):
                raise UnsupportedNemesis("MV building coordinator works only with tablets")
            ks_cfs = self.runner.cluster.get_non_system_ks_cf_with_tablets_list(
                db_node=self.runner.target_node,
                filter_empty_tables=True,
                filter_out_mv=True,
                filter_out_table_with_counter=True,
            )
            if not ks_cfs:
                raise UnsupportedNemesis(
                    "Non-system keyspaces with enabled tablets are not found. nemesis can't be run"
                )

        coordinator_node = get_topology_coordinator_node(self.runner.target_node)
        try:
            self.runner.switch_target_node(coordinator_node)
        except NemesisNodeAllocationError:
            raise UnsupportedNemesis(f"Coordinator node is busy with {coordinator_node.running_nemesis}")

        with (
            self.runner.node_allocator.run_nemesis(
                node_list=self.runner.cluster.nodes, nemesis_label="Verification node for MV"
            ) as working_node,
            self.runner.cluster.cql_connection_patient(node=working_node, connect_timeout=600) as session,
            collect_diagnostics(MVDiagnosticCollector(session, exception_handler=EventExceptionHandler())),
        ):
            ks_name, base_table_name = self.runner.random.choice(ks_cfs).split(".")
            view_name = f"{base_table_name}_view_{str(uuid4())[:8]}"
            try:
                create_materialized_view_for_random_column(session, ks_name, base_table_name, view_name)
                wait_materialized_view_building_tasks_started(session, ks_name, view_name)
            except ViewFinishedBuildingException:
                drop_materialized_view(session, ks_name, view_name)
                raise UnsupportedNemesis(
                    f"Skip nemesis because view {ks_name}.{view_name} has already finished building"
                )
            except Exception as error:  # pylint: disable=broad-except
                self.runner.log.error("Failed creating a materialized view: %s", error)
                raise
            try:
                num_of_restarts = len(self.runner.cluster.nodes) // 2
                self.runner.log.debug("Number of serial restart of topology coordinator: %s", num_of_restarts)
                for i in range(num_of_restarts):
                    self.runner.log.debug("Kill coordinator node: %s round: %s", self.runner.target_node.name, i + 1)
                    self.runner._kill_scylla_daemon()
                    coordinator_node = get_topology_coordinator_node(working_node)
                    self.runner.log.debug("New coordinator node %s", coordinator_node.name)
                    try:
                        self.runner.switch_target_node(coordinator_node)
                    except NemesisNodeAllocationError:
                        self.runner.log.debug(
                            "Coordinator node is busy with %s, number of coordinator successful restarts: %s",
                            coordinator_node.running_nemesis,
                            i,
                        )
                        break

                with adaptive_timeout(operation=Operations.CREATE_MV, node=working_node, timeout=14400) as timeout:
                    wait_for_view_to_be_built(working_node, ks_name, view_name, timeout=timeout * 2)

                result = list(
                    session.execute(SimpleStatement(f"SELECT * FROM {ks_name}.{view_name} limit 1", fetch_size=10))
                )
                assert len(result) >= 1, f"MV {ks_name}.{view_name} was not built"
            finally:
                drop_materialized_view(session, ks_name, view_name)
