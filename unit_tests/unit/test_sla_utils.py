"""Tests for sdcm.sla.libs.sla_utils.SlaUtils."""

import logging
import time
from typing import NamedTuple

import pytest

from sdcm.sla.libs.sla_utils import SchedulerRuntimeUnexpectedValue, SlaUtils
from sdcm.utils.loader_utils import (
    SERVICE_LEVEL_NAME_TEMPLATE,
    STRESS_ROLE_NAME_TEMPLATE,
    STRESS_ROLE_PASSWORD_TEMPLATE,
)
from test_lib.sla import Role, ServiceLevel, UserRoleBase
from unit_tests.lib.fake_cluster import DummyDbCluster, DummyNode

logging.basicConfig(level=logging.DEBUG)


class Row(NamedTuple):
    """Row of list_user_role_attached_service_levels: role name and attached service level."""

    name: str
    service_level: str


class FakeUserRoleBase(UserRoleBase):
    """UserRoleBase that reports its attached service level without querying the database."""

    def list_user_role_attached_service_levels(self):
        """Return one row for the attached service level, or no rows when none is attached."""
        if self.attached_service_level:
            return [Row(name=self.name, service_level=self.attached_service_level.name)]
        else:
            return []


class FakeRole(FakeUserRoleBase, Role):
    """Role whose create and attach calls are local, without a database."""

    def create(self, if_not_exists=True) -> Role:
        """Return the role itself, without creating anything."""
        return self

    def attach_service_level(self, service_level: ServiceLevel):
        """Record the service level's name, shares and scheduler group on the role."""
        self._attached_service_level = service_level
        self._attached_service_level_name = service_level.name
        self._attached_service_level_shares = service_level.shares
        self._attached_scheduler_group_name = service_level.scheduler_group_name


class FakeServiceLevel(ServiceLevel):
    """ServiceLevel whose create and lookup calls are local, without a database."""

    def create(self, if_not_exists=True) -> ServiceLevel:
        """Mark the service level as created and return it."""
        self.created = True
        return self

    def list_service_level(self) -> ServiceLevel | None:
        """Return the service level itself, as if it exists."""
        return self


class FakePrometheus:
    """Prometheus stub with fixed io queue operations and reactor utilization."""

    @staticmethod
    def get_scylla_io_queue_total_operations(start_time, end_time, node_ip, irate_sample_sec="60s"):
        """Return per-service-level operation rates for the node, keyed by IP and then by service level group."""
        return {
            "127.0.0.1": {
                "sl:default": [410.5785714285715, 400.36428571428576],
                "sl:sl200_abc": [177.11428571428573, 182.02857142857144],
                "sl:sl50_abc": [477.11428571428573, 482.02857142857144],
            }
        }

    def get_scylla_reactor_utilization(self, start_time, end_time, instance):
        """Return a fixed 100% reactor utilization."""
        return 100


class FakeSession:
    """Driver session stub that only carries default_timeout."""

    default_timeout = 0


def create_sla_auth(session, shares: int, index: str) -> Role:
    """Create a role with a service level of the given shares, attached to it."""
    role = FakeRole(
        session=session,
        name=STRESS_ROLE_NAME_TEMPLATE % (shares or "", index),
        password=STRESS_ROLE_PASSWORD_TEMPLATE % shares or "",
        login=True,
    ).create()
    role.attach_service_level(
        FakeServiceLevel(
            session=session, name=SERVICE_LEVEL_NAME_TEMPLATE % (shares or "", index), shares=shares
        ).create()
    )

    return role


@pytest.fixture()
def session():
    """Fake driver session shared by the roles of a test."""
    return FakeSession()


@pytest.fixture()
def read_roles(session):
    """Two roles on the same index: shares 50 (low) and shares 200 (high), each with its service level."""
    role_low = create_sla_auth(session=session, shares=50, index="abc")
    role_high = create_sla_auth(session=session, shares=200, index="abc")

    return [
        {"role": role_low, "service_level": role_low.attached_service_level},
        {"role": role_high, "service_level": role_high.attached_service_level},
    ]


def test_less_operations_than_expected_error(read_roles):
    """validate_io_queue_operations raises when the higher-shares service level gets fewer operations."""
    node = DummyNode(
        name="test_node", parent_cluster=None, ssh_login_info=dict(key_file="~/.ssh/scylla_test_id_ed25519")
    )
    db_cluster = DummyDbCluster(nodes=[node])

    with pytest.raises(SchedulerRuntimeUnexpectedValue) as error:
        SlaUtils().validate_io_queue_operations(
            start_time=time.time(),
            end_time=time.time() + 60,
            read_users=read_roles,
            prometheus_stats=FakePrometheus(),
            db_cluster=db_cluster,
            publish_wp_error_event=False,
        )
    assert str(error.value) == str(
        "\n(Node 127.0.0.1) - Service level with higher shares got less resources "
        "unexpectedly. CPU%: 100. Runtime per service level group:\n  sl:sl50_abc "
        "(shares 50): 479.57\n  sl:sl200_abc (shares 200): 179.57"
    )
