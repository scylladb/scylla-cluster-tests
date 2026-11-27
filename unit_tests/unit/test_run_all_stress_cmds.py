"""Tests for LoaderUtilsMixin._run_all_stress_cmds."""

from unittest.mock import MagicMock

import pytest

from sdcm.utils.loader_utils import LoaderUtilsMixin


class FakeLoaderMixin(LoaderUtilsMixin):
    """Minimal LoaderUtilsMixin stand-in that records the stress commands it is asked to run."""

    def __init__(self, stress_multiplier: int):
        self.params = {"stress_multiplier": stress_multiplier, "round_robin": False}
        self.log = MagicMock()
        self.run_stress_thread = MagicMock()

    @property
    def effective_disk_size_bytes(self) -> int:
        return 1_000_000_000


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("sdcm.utils.loader_utils.time.sleep", lambda _: None)


@pytest.mark.parametrize("stress_multiplier, expected_runs", [(1, 2), (3, 6)])
def test_each_round_runs_the_same_number_of_stress_commands(stress_multiplier, expected_runs):
    # the list that the configuration holds: every round of stress_cmd reads it again
    stress_cmd = ["cassandra-stress write duration=10m", "cassandra-stress read duration=10m"]
    loader = FakeLoaderMixin(stress_multiplier)

    for _ in range(3):
        stress_queue = []
        loader._run_all_stress_cmds(stress_queue, {"stress_cmd": stress_cmd})
        assert len(stress_queue) == expected_runs

    assert stress_cmd == ["cassandra-stress write duration=10m", "cassandra-stress read duration=10m"]
