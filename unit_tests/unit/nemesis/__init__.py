"""Common test hierarchy for nemesis tests."""

from abc import ABC, abstractmethod
from unittest.mock import MagicMock

from sdcm.nemesis import NemesisRunner
from sdcm.nemesis.registry import NemesisRegistry


class TestRunner:
    """Lightweight mock runner for nemesis unit tests.

    Provides attributes expected by both the test-only ``TestBaseClass``
    hierarchy (``COMMON_STRING``) and the real ``NemesisBaseClass`` /
    ``ModifyTableBaseMonkey`` chain (``random``, ``cluster``, ``target_node``,
    ``tester``, etc.).

    ``executed`` collects every CQL statement passed to the mock session so
    tests can assert on them.
    """

    __test__ = False  # prevent pytest from collecting this class

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


class TestNemesisClass(NemesisRunner):
    """Nemesis runner stub for unit tests.

    Overrides nemesis_registry to use the test-only TestBaseClass hierarchy so
    that registry/selector/filter logic can be exercised without touching the
    real nemesis tree.  The flag attributes mirror TestBaseClass to allow the
    same selector predicates to be tested on both sides.

    __test__ = False prevents pytest from collecting this as a test class.
    """

    COMMON_STRING = "called test function "
    kubernetes = False
    flag_a = False
    flag_b = False
    flag_c = False
    flag_common = False
    __test__ = False

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(
            tester_obj, termination_event, *args, nemesis_selector=nemesis_selector, nemesis_seed=nemesis_seed, **kwargs
        )
        self.nemesis_registry = NemesisRegistry(base_class=TestBaseClass, flag_class=TestBaseClass)
