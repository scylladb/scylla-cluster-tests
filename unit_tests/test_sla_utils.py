import logging
import time
import unittest
from typing import NamedTuple

from sdcm.cluster import BaseNode
from sdcm.utils.distro import Distro
from sdcm.utils.loader_utils import (STRESS_ROLE_NAME_TEMPLATE,
                                     STRESS_ROLE_PASSWORD_TEMPLATE,
                                     SERVICE_LEVEL_NAME_TEMPLATE)
from sdcm.sla.libs.sla_utils import SlaUtils, SchedulerRuntimeUnexpectedValue
from test_lib.sla import Role, UserRoleBase, ServiceLevel
from unit_tests.test_cluster import DummyDbCluster

logging.basicConfig(level=logging.DEBUG)


class DummyNode(BaseNode):
    _system_log = None
    is_enterprise = False
    distro = Distro.CENTOS7

    def init(self):
        super().init()
        self.remoter.stop()

    def wait_for_cloud_init(self):
        pass

    def jmx_up(self):
        return True

    def db_up(self):
        return True

    @property
    def private_ip_address(self):
        return '127.0.0.1'


class Row(NamedTuple):
    name: str
    service_level: str


class FakeUserRoleBase(UserRoleBase):
    def list_user_role_attached_service_levels(self):
        if self.attached_service_level:
            return [Row(name=self.name, service_level=self.attached_service_level.name)]
        else:
            return []


class FakeRole(FakeUserRoleBase, Role):
    def create(self, if_not_exists=True) -> Role:
        return self

    def attach_service_level(self, service_level: ServiceLevel):
        """
        :param service_level: service level object
        """
        self._attached_service_level = service_level
        self._attached_service_level_name = service_level.name
        self._attached_service_level_shares = service_level.shares
        self._attached_scheduler_group_name = service_level.scheduler_group_name


class FakeServiceLevel(ServiceLevel):
    def create(self, if_not_exists=True) -> ServiceLevel:
        self.created = True
        return self

    def list_service_level(self) -> ServiceLevel | None:
        return self


class FakePrometheus:
    @staticmethod
    def get_scylla_io_queue_total_operations(start_time, end_time, node_ip, irate_sample_sec='60s'):
        return {'127.0.0.1': {'sl:default': [410.5785714285715, 400.36428571428576],
                              'sl:sl200_abc': [177.11428571428573, 182.02857142857144],
                              'sl:sl50_abc': [477.11428571428573, 482.02857142857144]}
                }

    def get_scylla_reactor_utilization(self, start_time, end_time, instance):
        return 100


class FakeSession:
    default_timeout = 0


class TestSlaUtilsTest(unittest.TestCase, SlaUtils):
    def test_less_operations_than_expected_error(self):
        node = DummyNode(name='test_node',
                         parent_cluster=None,
                         ssh_login_info=dict(key_file='~/.ssh/scylla-test'))

        db_cluster = DummyDbCluster(nodes=[node])
        prometheus_stats = FakePrometheus()
        session = FakeSession()

        role_low = self.create_sla_auth(session=session, shares=50, index="abc")
        role_high = self.create_sla_auth(session=session, shares=200, index="abc")

        read_roles = [{"role": role_low, 'service_level': role_low.attached_service_level},
                      {"role": role_high, 'service_level': role_high.attached_service_level}]

        with self.assertRaises(SchedulerRuntimeUnexpectedValue) as error:
            self.validate_io_queue_operations(start_time=time.time(),
                                              end_time=time.time() + 60,
                                              read_users=read_roles,
                                              prometheus_stats=prometheus_stats,
                                              db_cluster=db_cluster,
                                              publish_wp_error_event=False)
        assert str(error.exception) == str('\n(Node 127.0.0.1) - Service level with higher shares got less resources '
                                           'unexpectedly. CPU%: 100. Runtime per service level group:\n  sl:sl50_abc '
                                           '(shares 50): 479.57\n  sl:sl200_abc (shares 200): 179.57')

    @staticmethod
    def create_sla_auth(session, shares: int, index: str) -> Role:
        role = FakeRole(session=session, name=STRESS_ROLE_NAME_TEMPLATE % (shares or '', index),
                        password=STRESS_ROLE_PASSWORD_TEMPLATE % shares or '', login=True).create()
        role.attach_service_level(
            FakeServiceLevel(session=session, name=SERVICE_LEVEL_NAME_TEMPLATE % (shares or '', index),
                             shares=shares).create())

        return role
