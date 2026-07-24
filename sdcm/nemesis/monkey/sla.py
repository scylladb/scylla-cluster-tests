import random
import time

from sdcm.db_stats import PrometheusDBStats
from sdcm.exceptions import NemesisSubTestFailure, UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass
from sdcm.sla.sla_tests import SlaTests
from sdcm.utils.loader_utils import DEFAULT_USER, DEFAULT_USER_PASSWORD, SERVICE_LEVEL_NAME_TEMPLATE
from test_lib.sla import MAX_ALLOWED_SERVICE_LEVELS, ServiceLevel


class SlaMonkeyBase(NemesisBaseClass):
    """Base class for SLA nemesis with shared validation logic."""

    sla = True

    additional_configs = [
        "configurations/nemesis/additional_configs/sla_config.yaml",
        "configurations/auth_cassandra.yaml",
    ]

    def validate_sla_preconditions(self):
        """Common validation for all SLA nemeses."""
        if not self.runner.cluster.params.get("sla"):
            raise UnsupportedNemesis("SLA nemesis can be run during SLA test only")

        if not self.runner.cluster.nodes[0].is_enterprise:
            raise UnsupportedNemesis("SLA feature is only supported by Scylla Enterprise")

        if not self.runner.cluster.params.get("authenticator"):
            raise UnsupportedNemesis("SLA feature can't work without authenticator")

    def get_cassandra_stress_write_cmds(self):
        write_cmds = self.runner.tester.params.get("prepare_write_cmd")
        if not write_cmds:
            return None

        if not isinstance(write_cmds, list):
            write_cmds = [write_cmds]

        stress_cmds = [cmd for cmd in write_cmds if " profile=" not in cmd and " n=" in cmd]
        if not stress_cmds:
            return None

        return stress_cmds

    def get_cassandra_stress_definition(self, stress_cmds, default_data_set_size=5000000):
        stress_cmd = stress_cmds[0]
        self.runner.log.debug("Stress commands to get cassandra_stress_definition: %s", stress_cmd)
        column_definition = self.runner.tester.get_c_s_column_definition(cs_cmd=stress_cmd)
        # Set small amount rows 5000000 when can not recognize a real dataset size, suppose that this amount should be
        # inserted in any case
        data_set_size = self.runner.tester.get_data_set_size(stress_cmd) or default_data_set_size

        self.runner.log.debug("Dataset size for nemesis: %s", data_set_size)
        self.runner.log.debug("Cassandra-stress column definition for nemesis: %s", column_definition)
        return column_definition, data_set_size

    def get_stress_params(self):
        """Return (column_definition, dataset_size) for SLA stress tests."""
        stress_cmds = self.get_cassandra_stress_write_cmds()
        if not stress_cmds:
            raise UnsupportedNemesis(
                "SLA nemesis needs cassandra-stress default table 'keyspace1.standard1' is created and prefilled"
            )

        return self.get_cassandra_stress_definition(stress_cmds)

    @staticmethod
    def format_error_for_sla_test_and_raise(error_events):
        if any(error_events):
            raise NemesisSubTestFailure(
                "\n".join(f"Step: {error.step}. Error:\n - {error}" for error in error_events if error)
            )


class RemoveServiceLevelMonkey(SlaMonkeyBase):
    """Remove service level while load is running, then re-create it."""

    disruptive = True

    def disrupt(self):
        self.validate_sla_preconditions()

        if not getattr(self.runner.tester, "roles", None):
            raise UnsupportedNemesis("This nemesis is supported with Service Level and role are pre-defined")

        role = self.runner.tester.roles[0]

        with self.runner.cluster.cql_connection_patient(
            node=self.runner.target_node, user=DEFAULT_USER, password=DEFAULT_USER_PASSWORD
        ) as session:
            self.runner.log.info("Drop service level %s", role.attached_service_level_name)
            removed_shares = role.attached_service_level.shares
            role.attached_service_level.session = session
            role.session = session
            dropped = False
            try:
                self.runner.actions_log.info(f"Dropping service level {role.attached_service_level_name}")
                role.attached_service_level.drop(if_exists=False)
                dropped = True
                time.sleep(300)  # let load to run without service level for 5 minutes
            finally:
                if dropped:
                    self.runner.actions_log.info(f"Re-creating service level {role.attached_service_level_name}")
                    role.attach_service_level(
                        ServiceLevel(
                            session=session,
                            name=SERVICE_LEVEL_NAME_TEMPLATE % (removed_shares, random.randint(0, 10)),
                            shares=removed_shares,
                        ).create()
                    )


class SlaIncreaseSharesDuringLoad(SlaMonkeyBase):
    """Increase SLA shares during load and verify throughput changes accordingly."""

    disruptive = False

    def disrupt(self):
        self.validate_sla_preconditions()
        column_definition, dataset_size = self.get_stress_params()

        prometheus_stats = PrometheusDBStats(host=self.runner.monitoring_set.nodes[0].external_address)
        error_events = SlaTests().test_increase_shares_during_load(
            tester=self.runner.tester,
            prometheus_stats=prometheus_stats,
            num_of_partitions=dataset_size,
            cassandra_stress_column_definition=column_definition,
        )
        self.format_error_for_sla_test_and_raise(error_events=error_events)


class SlaDecreaseSharesDuringLoad(SlaMonkeyBase):
    """Decrease SLA shares during load and verify throughput changes accordingly."""

    disruptive = False

    def disrupt(self):
        self.validate_sla_preconditions()
        column_definition, dataset_size = self.get_stress_params()

        prometheus_stats = PrometheusDBStats(host=self.runner.monitoring_set.nodes[0].external_address)
        error_events = SlaTests().test_decrease_shares_during_load(
            tester=self.runner.tester,
            prometheus_stats=prometheus_stats,
            num_of_partitions=dataset_size,
            cassandra_stress_column_definition=column_definition,
        )
        self.format_error_for_sla_test_and_raise(error_events=error_events)


class SlaReplaceUsingDetachDuringLoad(SlaMonkeyBase):
    """Replace service level using detach during load."""

    # TODO: Should be changed to False when https://github.com/scylladb/scylla-enterprise/issues/2572 is fixed.
    disruptive = True

    def disrupt(self):
        self.validate_sla_preconditions()
        column_definition, dataset_size = self.get_stress_params()

        prometheus_stats = PrometheusDBStats(host=self.runner.monitoring_set.nodes[0].external_address)
        error_events = SlaTests().test_replace_service_level_using_detach_during_load(
            tester=self.runner.tester,
            prometheus_stats=prometheus_stats,
            num_of_partitions=dataset_size,
            cassandra_stress_column_definition=column_definition,
        )
        self.format_error_for_sla_test_and_raise(error_events=error_events)


class SlaReplaceUsingDropDuringLoad(SlaMonkeyBase):
    """Replace service level using drop during load."""

    # TODO: Should be changed to False when https://github.com/scylladb/scylla-enterprise/issues/2572 is fixed.
    disruptive = True

    def disrupt(self):
        self.validate_sla_preconditions()
        column_definition, dataset_size = self.get_stress_params()

        prometheus_stats = PrometheusDBStats(host=self.runner.monitoring_set.nodes[0].external_address)
        error_events = SlaTests().test_replace_service_level_using_drop_during_load(
            tester=self.runner.tester,
            prometheus_stats=prometheus_stats,
            num_of_partitions=dataset_size,
            cassandra_stress_column_definition=column_definition,
        )
        self.format_error_for_sla_test_and_raise(error_events=error_events)


class SlaIncreaseSharesByAttachAnotherSlDuringLoad(SlaMonkeyBase):
    """Increase shares by attaching another service level during load."""

    # TODO: Should be changed to False when https://github.com/scylladb/scylla-enterprise/issues/2572 is fixed.
    disruptive = True

    def disrupt(self):
        self.validate_sla_preconditions()
        column_definition, dataset_size = self.get_stress_params()

        prometheus_stats = PrometheusDBStats(host=self.runner.monitoring_set.nodes[0].external_address)
        error_events = SlaTests().test_increase_shares_by_attach_another_sl_during_load(
            tester=self.runner.tester,
            prometheus_stats=prometheus_stats,
            num_of_partitions=dataset_size,
            cassandra_stress_column_definition=column_definition,
        )
        self.format_error_for_sla_test_and_raise(error_events=error_events)


class SlaMaximumAllowedSlsWithMaxSharesDuringLoad(SlaMonkeyBase):
    """Test maximum allowed service levels with max shares during load."""

    disruptive = False

    def disrupt(self):
        self.validate_sla_preconditions()
        column_definition, dataset_size = self.get_stress_params()

        prometheus_stats = PrometheusDBStats(host=self.runner.monitoring_set.nodes[0].external_address)
        with self.runner.cluster.cql_connection_patient(
            node=self.runner.target_node, user=DEFAULT_USER, password=DEFAULT_USER_PASSWORD
        ) as session:
            created_service_levels = len(ServiceLevel(session=session, name="dummy").list_all_service_levels()) + 1

        remaining_service_levels = MAX_ALLOWED_SERVICE_LEVELS - created_service_levels
        if remaining_service_levels <= 0:
            raise UnsupportedNemesis(
                f"No remaining service-level slots: {created_service_levels} already exist "
                f"(max allowed: {MAX_ALLOWED_SERVICE_LEVELS})"
            )

        error_events = SlaTests().test_maximum_allowed_sls_with_max_shares_during_load(
            tester=self.runner.tester,
            prometheus_stats=prometheus_stats,
            num_of_partitions=dataset_size,
            cassandra_stress_column_definition=column_definition,
            service_levels_amount=remaining_service_levels,
        )
        self.format_error_for_sla_test_and_raise(error_events=error_events)
