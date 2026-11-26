"""Tests for keeping a during-upgrade stress command alive until the last node is upgraded."""

import threading
from unittest.mock import MagicMock

import pytest

from sdcm.utils.stress_keep_alive import StressKeepAlive, duration_bound_cmd, finish_all
from sdcm.wait import wait_for
from upgrade_test import UpgradeTest

UpgradeTest.__test__ = False

CS_WRITE = (
    "cassandra-stress write no-warmup cl=QUORUM n=15000000 -schema 'keyspace=keyspace_during_upgrade "
    "replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native -rate threads=200 "
    "throttle=6000/s -pop seq=1..15000000 -col 'n=FIXED(1) size=FIXED(1024)' -log interval=5"
)
YCSB = "bin/ycsb run dynamodb -P workloads/workloada -threads 100 -target 1200 -p operationcount=1005010 -p table=usertable"


class FakePool:
    """Stress pool stub whose done event the test sets when the stress finishes."""

    def __init__(self):
        self.done = threading.Event()


class FakeTester:
    """Tester stub that records verified pools and returns the configured pass result."""

    def __init__(self):
        self.verified, self.actions_log, self.log = [], MagicMock(), MagicMock()
        self.pass_ok, self.teardown_started = True, False

    def verify_stress_thread(self, pool):
        """Wait for the pool to finish, record it and return pass_ok."""
        assert pool.done.wait(timeout=5)
        self.verified.append(pool)
        return self.pass_ok


def test_duration_bound_cmd_limits_cassandra_stress_and_ycsb_by_time():
    """duration_bound_cmd caps cassandra-stress and YCSB runs by time, and skips unbounded stress."""
    assert duration_bound_cmd(CS_WRITE) == CS_WRITE.replace(" n=15000000 ", " duration=5m ")
    assert duration_bound_cmd(CS_WRITE.replace(" -pop seq=1..15000000", "")) is None
    assert duration_bound_cmd(YCSB) == YCSB + " -p maxexecutiontime=300"


def test_keep_alive_relaunches_until_finish():
    """A finished pass relaunches the time-bounded stress until finish() stops the keeper."""
    tester, first, second, launched = FakeTester(), FakePool(), FakePool(), []

    def relaunch(cmd):
        launched.append(cmd)
        return [second]

    keeper = StressKeepAlive(tester, CS_WRITE, [first], relaunch)
    first.done.set()
    wait_for(lambda: launched, timeout=5, step=0.01, throw_exc=True)
    keeper.stop()
    second.done.set()
    keeper.finish()
    assert launched == [duration_bound_cmd(CS_WRITE)]
    assert tester.verified == [first, second]
    assert keeper.passes == 1


def test_keep_alive_does_not_relaunch_after_a_failed_pass():
    """A failed verification pass does not relaunch the stress command."""
    tester, first, relaunch = FakeTester(), FakePool(), MagicMock()
    tester.pass_ok = False
    keeper = StressKeepAlive(tester, CS_WRITE, [first], relaunch)
    first.done.set()
    wait_for(lambda: tester.verified, timeout=5, step=0.01, throw_exc=True)
    keeper.finish()
    relaunch.assert_not_called()


def test_keep_alive_stops_relaunching_once_teardown_started():
    """No stress is relaunched once teardown has started, even after a passing pass."""
    tester, first, relaunch = FakeTester(), FakePool(), MagicMock()
    tester.teardown_started = True
    keeper = StressKeepAlive(tester, CS_WRITE, [first], relaunch)
    first.done.set()
    wait_for(lambda: tester.verified, timeout=5, step=0.01, throw_exc=True)
    keeper.finish()
    relaunch.assert_not_called()


def test_finish_all_raises_the_first_failure_and_logs_the_others():
    """finish_all raises the first keeper failure and logs the remaining ones."""
    tester = FakeTester()
    keepers = [StressKeepAlive(tester, CS_WRITE, [FakePool()], None) for _ in range(2)]
    tester.verify_stress_thread = MagicMock(side_effect=[ValueError("read failed"), ValueError("write failed")])
    with pytest.raises(ValueError, match="read failed"):
        finish_all(keepers)
    assert "write failed" in str(tester.log.error.call_args)


@pytest.mark.parametrize(
    "params, commands",
    [
        pytest.param({"keep_stress_alive_during_upgrade": False}, [CS_WRITE], id="keep-alive-off"),
        pytest.param(
            {"keep_stress_alive_during_upgrade": True, "stress_multiplier": 2}, [CS_WRITE, CS_WRITE], id="multiplied"
        ),
    ],
)
def test_upgrade_keep_alive_only_verifies(params, commands):
    """Upgrade keep-alive only verifies each finished stress pool and never relaunches it."""
    tester, pools, relaunch = FakeTester(), [FakePool() for _ in commands], MagicMock()
    tester.params = params
    for pool in pools:
        pool.done.set()
    for keeper in UpgradeTest._keep_alive(tester, commands, pools, relaunch):
        keeper.finish()
    relaunch.assert_not_called()
    assert tester.verified == pools
