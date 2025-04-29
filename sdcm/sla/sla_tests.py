#!/usr/bin/env python
import logging
import time
import traceback
import uuid

from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestStepEvent
from sdcm.sla.libs.sla_utils import SlaUtils
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.decorators import retrying
from sdcm.utils.loader_utils import DEFAULT_USER, DEFAULT_USER_PASSWORD, SERVICE_LEVEL_NAME_TEMPLATE
from test_lib.sla import create_sla_auth, ServiceLevel, Role

LOGGER = logging.getLogger(__name__)


class Steps(SlaUtils):

    def run_stress_and_validate_scheduler_io_queue_operations_during_load(self, tester, read_cmds, prometheus_stats, read_roles,
                                                                          stress_queue, sleep=600):

        with TestStepEvent(step="Run stress command and validate io_queue_operations during load") as wp_event:
            try:
                start_time = time.time() + 60

                tester._run_all_stress_cmds(stress_queue, params={'stress_cmd': read_cmds, 'round_robin': True})
                time.sleep(sleep)
                end_time = time.time()

                self.validate_io_queue_operations(start_time=start_time,
                                                  end_time=end_time,
                                                  read_users=read_roles,
                                                  prometheus_stats=prometheus_stats,
                                                  db_cluster=tester.db_cluster,
                                                  possible_issue={'less resources': 'scylla-enterprise#2717'}
                                                  )
                return None
            except Exception as details:  # noqa: BLE001
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    def alter_sl_and_validate_io_queue_operations(self, tester, service_level, new_shares, read_roles, prometheus_stats,
                                                  sleep=600):

        with TestStepEvent(step=f"Alter shares from {service_level.shares} to {new_shares} Service "
                                f"Level {service_level.name} and validate io_queue_operations "
                                f"during load") as wp_event:
            try:
                service_level.alter(new_shares=new_shares)
                # Wait for SL update is propagated to all nodes
                with adaptive_timeout(Operations.SERVICE_LEVEL_PROPAGATION, node=tester.db_cluster.data_nodes[0], timeout=15,
                                      service_level_for_test_step="ALTER_SERVICE_LEVEL"):
                    self.wait_for_service_level_propagated(cluster=tester.db_cluster, service_level=service_level)
                start_time = time.time() + 60
                # Let load to run before validation
                time.sleep(sleep)
                end_time = time.time()
                self.validate_io_queue_operations(start_time=start_time,
                                                  end_time=end_time,
                                                  read_users=read_roles,
                                                  prometheus_stats=prometheus_stats,
                                                  db_cluster=tester.db_cluster,
                                                  possible_issue={'less resources': "scylla-enterprise#949"})
                return None
            except Exception as details:  # noqa: BLE001
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    @staticmethod
    def detach_service_level_and_run_load(sl_for_detach, role_with_sl_to_detach, sleep=600):

        with TestStepEvent(step=f"Detach service level {sl_for_detach.name} with {sl_for_detach.shares} shares from "
                                f"{role_with_sl_to_detach.name}.") as wp_event:
            try:
                role_with_sl_to_detach.detach_service_level()
                time.sleep(sleep)
                return None
            except Exception as details:  # noqa: BLE001
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    @staticmethod
    def drop_service_level_and_run_load(sl_for_drop, role_with_sl_to_drop, sleep=600):

        with TestStepEvent(step=f"Drop service level {sl_for_drop.name} with {role_with_sl_to_drop.name}.") as wp_event:
            try:
                sl_for_drop.drop()
                role_with_sl_to_drop.reset_service_level()
                time.sleep(sleep)
                return None
            except Exception as details:  # noqa: BLE001
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event

    def attach_sl_and_validate_io_queue_operations(self, tester, new_service_level, role_for_attach,
                                                   read_roles, prometheus_stats, sleep=600):
        @retrying(n=15, sleep_time=1, message="Wait for service level has been attached to the role",
                  allowed_exceptions=(Exception, ValueError,))
        def validate_role_service_level_attributes_against_db():
            role_for_attach.validate_role_service_level_attributes_against_db()

        with TestStepEvent(step=f"Attach service level {new_service_level.name} with "
                                f"{new_service_level.shares} shares to {role_for_attach.name}. "
                                f"Validate io_queue_operations during load") as wp_event:
            try:
                role_for_attach.attach_service_level(new_service_level)
                role_for_attach.validate_role_service_level_attributes_against_db()

                # Fix for issue https://github.com/scylladb/scylla-enterprise/issues/2572
                for node in tester.db_cluster.nodes:
                    if node.db_up():
                        node.remoter.run(
                            "curl -X POST --header 'Content-Type: application/json' --header 'Accept: application/json'"
                            " http://127.0.0.1:10000/service_levels/switch_tenants"
                        )

                # Wait for tenant switch
                # The above call to '/service_levels/switch_tenants' only marks
                # all of the CQL connections to switch tenant (it's scheduling group).
                # The actual switch on a connection will happen when the connection finshes processing current request.
                time.sleep(5*60)

                # Print connections map on each node to validate connections' scheduling groups
                for node in tester.db_cluster.nodes:
                    if node.db_up():
                        result = node.remoter.run(
                            "curl -X GET --header 'Content-Type: application/json' --header 'Accept: application/json'"
                            " http://127.0.0.1:10000/service_levels/count_connections"
                        )

                        connections_map = result.stdout.strip()
                        LOGGER.debug("[NODE {}] {}".format(node.private_ip_address, connections_map))
                # End fix

                start_time = time.time() + 60
                time.sleep(sleep)
                end_time = time.time()
                self.validate_io_queue_operations(start_time=start_time,
                                                  end_time=end_time,
                                                  read_users=read_roles,
                                                  prometheus_stats=prometheus_stats,
                                                  db_cluster=tester.db_cluster,
                                                  possible_issue={'less resources':
                                                                  'scylla-enterprise#2572 or scylla-enterprise#2717'}
                                                  )
                return None
            except Exception as details:  # noqa: BLE001
                wp_event.add_error([str(details)])
                wp_event.full_traceback = traceback.format_exc()
                wp_event.severity = Severity.ERROR
                return wp_event


class SlaTests(Steps):
    STRESS_READ_CMD = 'cassandra-stress read cl=ALL duration={duration} -mode cql3 native user={user} ' \
                      'password={password} -rate threads={threads} -pop {pop}'

    @staticmethod
    def unique_subsrtr_for_name():
        return str(uuid.uuid1()).split("-", maxsplit=1)[0]

    def _create_sla_auth(self, session, db_cluster, shares: int, index: str, superuser: bool = True) -> Role:
        role = None
        try:
            role = create_sla_auth(session=session, shares=shares, index=index, superuser=superuser)
            with adaptive_timeout(Operations.SERVICE_LEVEL_PROPAGATION, node=db_cluster.data_nodes[0], timeout=15,
                                  service_level_for_test_step="INITIAL_FOR_TEST"):
                self.wait_for_service_level_propagated(cluster=db_cluster, service_level=role.attached_service_level)
            return role
        except Exception:
            if role and role.attached_service_level:
                role.attached_service_level.drop()
            if role:
                role.drop()
            raise

    def _create_new_service_level(self, session, auth_entity_name_index, shares, db_cluster, service_level_for_test_step: str = None):
        new_sl = ServiceLevel(session=session,
                              name=SERVICE_LEVEL_NAME_TEMPLATE % (shares, auth_entity_name_index),
                              shares=shares).create()
        with adaptive_timeout(Operations.SERVICE_LEVEL_PROPAGATION, node=db_cluster.data_nodes[0], timeout=15,
                              service_level_for_test_step=service_level_for_test_step):
            self.wait_for_service_level_propagated(cluster=db_cluster, service_level=new_sl)
        return new_sl

    @staticmethod
    def verify_stress_threads(tester, stress_queue):
        for stress in stress_queue:
            try:
                tester.verify_stress_thread(cs_thread_pool=stress)
            except Exception as error:  # noqa: BLE001
                LOGGER.error("Stress verifying failed. Error: %s", error)

    @staticmethod
    def refresh_role_in_list(role_to_refresh, read_roles):
        for i, role in enumerate(read_roles):
            if role["role"] == role_to_refresh:
                read_roles[i]["service_level"] = role_to_refresh.attached_service_level

    def test_increase_shares_by_attach_another_sl_during_load(self, tester, prometheus_stats, num_of_partitions,
                                                              cassandra_stress_column_definition=None):
        low_share = 20
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = self.unique_subsrtr_for_name()

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = self._create_sla_auth(session=session, shares=low_share,
                                             index=auth_entity_name_index, db_cluster=tester.db_cluster)
            role_high = self._create_sla_auth(session=session, shares=high_share,
                                              index=auth_entity_name_index, db_cluster=tester.db_cluster)
            kwargs = {"-col": cassandra_stress_column_definition} if cassandra_stress_column_definition else {}

            # TODO: change stress_duration to 25. Now it is increased because of cluster rolling restart (workaround)
            stress_duration = 35
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs)
                         ]
            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            new_sl = None
            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_io_queue_operations_during_load(tester=tester, read_cmds=read_cmds,
                                                                                           prometheus_stats=prometheus_stats,
                                                                                           read_roles=read_roles,
                                                                                           stress_queue=stress_queue))
                # Create new role and attach it instead of detached
                new_sl = self._create_new_service_level(session=session,
                                                        auth_entity_name_index=auth_entity_name_index,
                                                        shares=800,
                                                        db_cluster=tester.db_cluster,
                                                        service_level_for_test_step="NEW_FOR_REPLACE_EXISTING")

                error_events.append(
                    self.attach_sl_and_validate_io_queue_operations(tester=tester,
                                                                    new_service_level=new_sl,
                                                                    role_for_attach=role_low,
                                                                    read_roles=read_roles,
                                                                    prometheus_stats=prometheus_stats,
                                                                    sleep=600))

            finally:
                self.verify_stress_threads(tester=tester, stress_queue=stress_queue)
                self.clean_auth(entities_list_of_dict=read_roles)
                if new_sl:
                    new_sl.drop()
                return error_events

    def test_increase_shares_during_load(self, tester, prometheus_stats, num_of_partitions,
                                         cassandra_stress_column_definition=None):
        low_share = 20
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = self.unique_subsrtr_for_name()

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = self._create_sla_auth(session=session, shares=low_share,
                                             index=auth_entity_name_index, db_cluster=tester.db_cluster)
            role_high = self._create_sla_auth(session=session, shares=high_share,
                                              index=auth_entity_name_index, db_cluster=tester.db_cluster)
            kwargs = {"-col": cassandra_stress_column_definition} if cassandra_stress_column_definition else {}

            stress_duration = 25
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs)
                         ]
            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_io_queue_operations_during_load(tester=tester, read_cmds=read_cmds,
                                                                                           prometheus_stats=prometheus_stats,
                                                                                           read_roles=read_roles,
                                                                                           stress_queue=stress_queue))
                error_events.append(
                    self.alter_sl_and_validate_io_queue_operations(tester=tester,
                                                                   service_level=role_low.attached_service_level,
                                                                   new_shares=900, read_roles=read_roles,
                                                                   prometheus_stats=prometheus_stats))
            finally:
                self.verify_stress_threads(tester=tester, stress_queue=stress_queue)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events

    def test_decrease_shares_during_load(self, tester, prometheus_stats, num_of_partitions,
                                         cassandra_stress_column_definition=None):
        low_share = 800
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = self.unique_subsrtr_for_name()

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = self._create_sla_auth(session=session, shares=low_share,
                                             index=auth_entity_name_index, db_cluster=tester.db_cluster)
            role_high = self._create_sla_auth(session=session, shares=high_share,
                                              index=auth_entity_name_index, db_cluster=tester.db_cluster)
            kwargs = {"-col": cassandra_stress_column_definition} if cassandra_stress_column_definition else {}

            stress_duration = 25
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs)
                         ]
            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_io_queue_operations_during_load(tester=tester, read_cmds=read_cmds,
                                                                                           prometheus_stats=prometheus_stats,
                                                                                           read_roles=read_roles,
                                                                                           stress_queue=stress_queue))
                error_events.append(
                    self.alter_sl_and_validate_io_queue_operations(tester=tester,
                                                                   service_level=role_low.attached_service_level,
                                                                   new_shares=100, read_roles=read_roles,
                                                                   prometheus_stats=prometheus_stats))

            finally:
                self.verify_stress_threads(tester=tester, stress_queue=stress_queue)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events

    def test_replace_service_level_using_detach_during_load(self, tester, prometheus_stats, num_of_partitions,
                                                            cassandra_stress_column_definition=None):
        low_share = 250
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = self.unique_subsrtr_for_name()

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = self._create_sla_auth(session=session, shares=low_share,
                                             index=auth_entity_name_index, db_cluster=tester.db_cluster)
            role_high = self._create_sla_auth(session=session, shares=high_share,
                                              index=auth_entity_name_index, db_cluster=tester.db_cluster)
            kwargs = {"-col": cassandra_stress_column_definition} if cassandra_stress_column_definition else {}

            stress_duration = 35
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs)
                         ]

            read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                          {"role": role_high, 'service_level': role_high.attached_service_level}]

            new_sl = None

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_io_queue_operations_during_load(tester=tester, read_cmds=read_cmds,
                                                                                           prometheus_stats=prometheus_stats,
                                                                                           read_roles=read_roles,
                                                                                           stress_queue=stress_queue))

                error_events.append(
                    self.detach_service_level_and_run_load(sl_for_detach=role_high.attached_service_level,
                                                           role_with_sl_to_detach=role_high,
                                                           sleep=600))
                self.refresh_role_in_list(role_to_refresh=role_high, read_roles=read_roles)

                # Create new role and attach it instead of detached
                new_sl = self._create_new_service_level(session=session,
                                                        auth_entity_name_index=auth_entity_name_index,
                                                        shares=50,
                                                        db_cluster=tester.db_cluster,
                                                        service_level_for_test_step="NEW_AFTER_DETACH")

                error_events.append(
                    self.attach_sl_and_validate_io_queue_operations(tester=tester,
                                                                    new_service_level=new_sl,
                                                                    role_for_attach=role_high,
                                                                    read_roles=read_roles,
                                                                    prometheus_stats=prometheus_stats,
                                                                    sleep=600))

            finally:
                self.verify_stress_threads(tester=tester, stress_queue=stress_queue)
                self.clean_auth(entities_list_of_dict=read_roles)
                if new_sl:
                    new_sl.drop()
                return error_events

    def test_replace_service_level_using_drop_during_load(self, tester, prometheus_stats, num_of_partitions,
                                                          cassandra_stress_column_definition=None):
        low_share = 250
        high_share = 500
        error_events = []
        stress_queue = []
        auth_entity_name_index = self.unique_subsrtr_for_name()

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            role_low = self._create_sla_auth(session=session, shares=low_share,
                                             index=auth_entity_name_index, db_cluster=tester.db_cluster)
            role_high = self._create_sla_auth(session=session, shares=high_share,
                                              index=auth_entity_name_index, db_cluster=tester.db_cluster)
            kwargs = {"-col": cassandra_stress_column_definition} if cassandra_stress_column_definition else {}

            # TODO: change stress_duration to 35. Now it is increased because of cluster rolling restart (workaround)
            stress_duration = 45
            read_cmds = [self.define_read_cassandra_stress_command(role=role_low,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs),
                         self.define_read_cassandra_stress_command(role=role_high,
                                                                   load_type=self.CACHE_ONLY_LOAD,
                                                                   c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                   threads=200,
                                                                   stress_duration_min=stress_duration,
                                                                   num_of_partitions=num_of_partitions,
                                                                   kwargs=kwargs)
                         ]

            read_roles = [{"role": role_low, "service_level": role_low.attached_service_level},
                          {"role": role_high, "service_level": role_high.attached_service_level}]

            new_sl = None

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_io_queue_operations_during_load(tester=tester, read_cmds=read_cmds,
                                                                                           prometheus_stats=prometheus_stats,
                                                                                           read_roles=read_roles,
                                                                                           stress_queue=stress_queue))

                error_events.append(
                    self.drop_service_level_and_run_load(sl_for_drop=role_low.attached_service_level,
                                                         role_with_sl_to_drop=role_low,
                                                         sleep=600))
                self.refresh_role_in_list(role_to_refresh=role_low, read_roles=read_roles)

                # Create new role and attach it instead of dropped
                new_sl = self._create_new_service_level(session=session,
                                                        auth_entity_name_index=auth_entity_name_index,
                                                        shares=800,
                                                        db_cluster=tester.db_cluster,
                                                        service_level_for_test_step="NEW_AFTER_DROP")

                error_events.append(
                    self.attach_sl_and_validate_io_queue_operations(tester=tester,
                                                                    new_service_level=new_sl,
                                                                    role_for_attach=role_low,
                                                                    read_roles=read_roles,
                                                                    prometheus_stats=prometheus_stats,
                                                                    sleep=600))

            finally:
                self.verify_stress_threads(tester=tester, stress_queue=stress_queue)
                self.clean_auth(entities_list_of_dict=read_roles)
                if new_sl:
                    new_sl.drop()
                return error_events

    def test_maximum_allowed_sls_with_max_shares_during_load(self, tester, prometheus_stats, num_of_partitions,
                                                             cassandra_stress_column_definition=None,
                                                             service_levels_amount=7):
        error_events = []
        stress_queue = []
        every_role_shares = 1000
        stress_duration = 20
        read_cmds = []
        read_roles = []

        with tester.db_cluster.cql_connection_patient(node=tester.db_cluster.nodes[0],
                                                      user=DEFAULT_USER,
                                                      password=DEFAULT_USER_PASSWORD) as session:
            roles = []
            kwargs = {"-col": cassandra_stress_column_definition} if cassandra_stress_column_definition else {}
            for _ in range(service_levels_amount):
                roles.append(self._create_sla_auth(session=session,
                                                   shares=every_role_shares,
                                                   index=self.unique_subsrtr_for_name(),
                                                   db_cluster=tester.db_cluster))
                read_cmds.append(self.define_read_cassandra_stress_command(role=roles[-1],
                                                                           load_type=self.MIXED_LOAD,
                                                                           c_s_workload_type=self.WORKLOAD_THROUGHPUT,
                                                                           threads=50,
                                                                           stress_duration_min=stress_duration,
                                                                           num_of_partitions=num_of_partitions,
                                                                           max_rows_for_read=num_of_partitions,
                                                                           kwargs=kwargs))

                read_roles.append({"role": roles[-1], 'service_level': roles[-1].attached_service_level})

            try:
                error_events.append(
                    self.run_stress_and_validate_scheduler_io_queue_operations_during_load(tester=tester,
                                                                                           read_cmds=read_cmds,
                                                                                           prometheus_stats=prometheus_stats,
                                                                                           read_roles=read_roles,
                                                                                           stress_queue=stress_queue))

            finally:
                self.verify_stress_threads(tester=tester, stress_queue=stress_queue)
                self.clean_auth(entities_list_of_dict=read_roles)
                return error_events
