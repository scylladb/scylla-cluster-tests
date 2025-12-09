"""Common test hierarchy for nemesis tests."""
from abc import ABC, abstractmethod


class TestRunner:
    COMMON_STRING = "called test function "


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
