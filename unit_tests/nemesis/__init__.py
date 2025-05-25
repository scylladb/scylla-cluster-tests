"""Common test hierarchy for nemesis tests."""


class TestRunner:
    COMMON_STRING = "called test function "


class TestBaseClass:
    flag_a = False
    flag_c = False
    flag_d = False
    flag_common = False
    disabled = False


class CustomNemesisA(TestBaseClass):
    flag_a = True
    flag_common = True

    def __init__(self, runner):
        print(runner.COMMON_STRING + "a")


class DisabledNemesis(TestBaseClass):
    """This nemesis is disabled when used by SisyphusRunner"""
    flag_common = True
    disabled = True

    def __init__(self, runner):
        print(runner.COMMON_STRING + "b")


class CustomNemesisC(TestBaseClass):
    flag_c = True
    flag_common = True

    def __init__(self, runner):
        print(runner.COMMON_STRING + "c")


class CustomNemesisD(TestBaseClass):
    flag_d = True
    flag_a = True
    flag_common = True

    def __init__(self, runner):
        print(runner.COMMON_STRING + "d")
