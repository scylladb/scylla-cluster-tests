"""Common test hierarchy for nemesis tests."""

from abc import ABC, abstractmethod
from unittest.mock import MagicMock


class TestRunner:
    __test__ = False  # prevent pytest from collecting this class
    """Lightweight mock runner for nemesis unit tests.

    Provides attributes expected by both the test-only ``TestBaseClass``
    hierarchy (``COMMON_STRING``) and the real ``NemesisBaseClass`` /
    ``ModifyTableBaseMonkey`` chain (``random``, ``cluster``, ``target_node``,
    ``tester``, etc.).

    ``executed`` collects every CQL statement passed to the mock session so
    tests can assert on them.
    """

    COMMON_STRING = "called test function "

    def __init__(self, ks_cfs=None, params=None):
        self.random = MagicMock()
        self.target_node = MagicMock()
        self.target_node.name = "node1"
        self.tester = MagicMock()
        self.tester.params = params
        self.log = MagicMock()
        self.actions_log = MagicMock()
        self.action_log_scope = MagicMock()

        self.cluster = MagicMock()
        self.cluster.get_non_system_ks_cf_list.return_value = ks_cfs
        self.cluster.data_nodes = [self.target_node]

        self.executed = []
        session = MagicMock()
        session.execute.side_effect = lambda cmd: self.executed.append(cmd)
        self.cluster.cql_connection_patient.return_value.__enter__.return_value = session


class TestBaseClass(ABC):
    """
    For tests purposes TestBaseClass is also a flag class
    """

    flag_a = False
    flag_b = False
    flag_c = False
    flag_d = False
    flag_common = False
    flag_true = True
    disabled = False

    def __init__(self, runner):
        self.runner = runner

    @abstractmethod
    def disrupt(self):
        """Disrupt method"""


class CustomNemesisA(TestBaseClass):
    flag_a = True
    flag_common = True
    flag_true = False

    def disrupt(self):
        print(self.runner.COMMON_STRING + "a")


class CustomNemesisB(TestBaseClass):
    flag_b = True
    flag_common = True
    disabled = True

    def disrupt(self):
        print(self.runner.COMMON_STRING + "b")


class IntermediateNemesis(TestBaseClass, ABC):
    flag_a = True


class CustomNemesisC(CustomNemesisA):
    flag_c = True

    def disrupt(self):
        print(self.runner.COMMON_STRING + "c")


class CustomNemesisAD(IntermediateNemesis):
    flag_d = True
    flag_a = True
    flag_common = True

    def disrupt(self):
        print(self.runner.COMMON_STRING + "d")
