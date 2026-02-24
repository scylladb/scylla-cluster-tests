"""Tests for execute_nemesis method of NemesisRunner, which is responsible for executing a single nemesis disruption."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from unittest.mock import MagicMock

from sdcm.nemesis import NemesisRunner, UnsupportedNemesis, KillNemesis
from sdcm.nemesis.registry import NemesisRegistry


class TestExecuteBaseClass(ABC):
    """
    For tests purposes TestExecuteBaseClass is also a flag class
    """

    def __init__(self, runner):
        self.runner = runner

    @abstractmethod
    def disrupt(self):
        """Disrupt method"""


class CustomTestNemesis(TestExecuteBaseClass):
    """A simple nemesis for testing."""

    def disrupt(self):
        print("Disrupting cluster")


class FailingTestNemesis(TestExecuteBaseClass):
    """A nemesis that fails during disruption."""

    def disrupt(self):
        raise RuntimeError("Intentional failure")


class SkippingTestNemesis(TestExecuteBaseClass):
    """A nemesis that skips."""

    def disrupt(self):
        raise UnsupportedNemesis("Intentional skip")


class KillTestNemesis(TestExecuteBaseClass):
    """A nemesis that raises KillNemesis."""

    def disrupt(self):
        raise KillNemesis("Intentional kill")


class TestNemesisRunner(NemesisRunner):
    """Test subclass of NemesisRunner with simplified registry."""

    __test__ = False

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(
            tester_obj, termination_event, *args, nemesis_selector=nemesis_selector, nemesis_seed=nemesis_seed, **kwargs
        )
        # self.current_disruption = "TestDisruption-1"
        self.node_allocator = MagicMock()
        # self.log_on_all_nodes = MagicMock()
        self.metrics_srv = MagicMock()
        self.nemesis_registry = NemesisRegistry(base_class=TestExecuteBaseClass, flag_class=TestExecuteBaseClass)

    def set_target_node(self):
        self.target_node = self.tester.db_cluster.nodes[0]

    @contextmanager
    def verify_nodes(*args, **kwargs):
        """Mock verify_nodes context manager."""
        yield
