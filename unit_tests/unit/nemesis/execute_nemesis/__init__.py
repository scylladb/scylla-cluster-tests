"""Tests for execute_nemesis method of NemesisRunner, which is responsible for executing a single nemesis disruption."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from unittest.mock import MagicMock

from sdcm.nemesis import KillNemesis, NemesisBaseClass, NemesisRunner, UnsupportedNemesis
from sdcm.nemesis.registry import NemesisRegistry
from sdcm.utils.version_utils import MethodVersionNotFound


class TestExecuteBaseClass(ABC):
    """For tests purposes TestExecuteBaseClass is also a flag class"""

    def __init__(self, runner):
        self.runner = runner

    def precheck(self, node) -> str | None:
        """Stub matching NemesisBaseClass.precheck(node) contract; always runnable."""
        return None

    @abstractmethod
    def disrupt(self):
        """Disrupt method"""

    description = classmethod(NemesisBaseClass.description.__func__)


class CustomTestNemesis(TestExecuteBaseClass):
    """A simple nemesis for testing."""

    def disrupt(self):
        """Print a line and return normally."""
        print("Disrupting cluster")


class FailingTestNemesis(TestExecuteBaseClass):
    """A nemesis that fails during disruption."""

    def disrupt(self):
        """Raise RuntimeError from disrupt."""
        raise RuntimeError("Intentional failure")


class SkippingTestNemesis(TestExecuteBaseClass):
    """A nemesis that skips."""

    def disrupt(self):
        """Raise UnsupportedNemesis from disrupt."""
        raise UnsupportedNemesis("Intentional skip")


class KillTestNemesis(TestExecuteBaseClass):
    """A nemesis that raises KillNemesis."""

    def disrupt(self):
        """Raise KillNemesis from disrupt."""
        raise KillNemesis("Intentional kill")


class VersionNotFoundTestNemesis(TestExecuteBaseClass):
    """A nemesis that raises MethodVersionNotFound."""

    def disrupt(self):
        """Raise MethodVersionNotFound from disrupt."""
        raise MethodVersionNotFound("Intentional version skip")


class PrecheckSkipNemesis(TestExecuteBaseClass):
    """A nemesis whose precheck() returns a skip reason."""

    def precheck(self, node) -> str | None:
        """Return a skip reason for any node."""
        return "static condition not met"

    def disrupt(self):
        """Print a line and return normally."""
        print("Disrupting cluster")


class PrecheckErrorNemesis(TestExecuteBaseClass):
    """A nemesis whose precheck() raises an exception."""

    def precheck(self, node) -> str | None:
        """Raise RuntimeError from precheck."""
        raise RuntimeError("precheck blew up")

    def disrupt(self):
        """Print a line and return normally."""
        print("Disrupting cluster")


class TestNemesisRunner(NemesisRunner):
    """Test subclass of NemesisRunner with simplified registry."""

    __test__ = False

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(
            tester_obj, termination_event, *args, nemesis_selector=nemesis_selector, nemesis_seed=nemesis_seed, **kwargs
        )
        self.node_allocator = MagicMock()
        self.metrics_srv = MagicMock()
        self.nemesis_registry = NemesisRegistry(base_class=TestExecuteBaseClass, flag_class=TestExecuteBaseClass)

    def set_target_node(self):
        """Use the first DB node as target."""
        self.target_node = self.tester.db_cluster.nodes[0]

    @contextmanager
    def verify_nodes(self):
        """Mock verify_nodes context manager."""
        yield
