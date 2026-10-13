"""`nemesis.runners` entries: validated by the config model of the runner class they name.

Uses the test-only ``TestRunnerBase`` hierarchy, not the real runners.
"""

import pytest
from pydantic import ValidationError

from sdcm.nemesis.registry import NemesisRegistry
from sdcm.sct_config.nemesis_runners import nemesis_config_model
from unit_tests.unit.nemesis import (
    PlainTestRunner,
    TestBaseClass,
    TestRunnerBase,
    WeightedTestRunner,
    WeightedTestRunnerConfig,
)

TestNemesisConfig = nemesis_config_model(
    NemesisRegistry(base_class=TestRunnerBase, flag_class=TestBaseClass).get_subclasses()
)


def test_runner_entry_is_validated_by_its_class_config_model():
    config = TestNemesisConfig.model_validate(
        {
            "runners": [
                {"PlainTestRunner": {"selector": "flag_a"}},
                {"WeightedTestRunner": {"weights": {"CustomNemesisA": 2}}},
                {"PlainTestRunner": None},
            ]
        }
    )

    assert [runner.runner_class for runner in config.runners] == [PlainTestRunner, WeightedTestRunner, PlainTestRunner]
    assert isinstance(config.runners[1], WeightedTestRunnerConfig)
    assert config.runners[1].weights == {"CustomNemesisA": 2.0}
    assert config.runners[1].default_weight == 1


@pytest.mark.parametrize(
    "entry,error_location,error_message",
    [
        pytest.param(
            {"WeightedTestRunner": {}}, "runners.0.WeightedTestRunner.weights", "Field required", id="missing"
        ),
        pytest.param(
            {"PlainTestRunner": {"weights": {"CustomNemesisA": 2}}},
            "runners.0.PlainTestRunner.weights",
            "Extra inputs are not permitted",
            id="option_of_another_runner",
        ),
        pytest.param(
            {"WeightedTestRunner": {"weights": {"CustomNemesisA": -1}}},
            "runners.0.WeightedTestRunner.weights.CustomNemesisA",
            "greater than or equal to 0",
            id="out_of_range",
        ),
        pytest.param({"NoSuchRunner": {}}, "runners.0", "does not match any of the expected tags", id="unknown_class"),
        pytest.param(
            {"PlainTestRunner": {}, "WeightedTestRunner": {}},
            "runners.0",
            "exactly one key",
            id="several_classes_in_entry",
        ),
    ],
)
def test_runner_entry_is_rejected_by_its_class_config_model(entry, error_location, error_message):
    with pytest.raises(ValidationError) as error:
        TestNemesisConfig.model_validate({"runners": [entry]})

    (details,) = error.value.errors()
    assert ".".join(str(part) for part in details["loc"]) == error_location
    assert error_message in details["msg"]


def test_nemesis_threads_resolve_unset_options_from_defaults():
    config = TestNemesisConfig.model_validate(
        {
            "interval": 7,
            "runners": [
                {"PlainTestRunner": {"interval": 15, "seed": 3}},
                {"WeightedTestRunner": {"weights": {"A": 1}}},
            ],
        }
    )

    own_options, defaults = config.nemesis_threads()

    assert (own_options.runner_class, own_options.config.interval, own_options.config.seed) == (PlainTestRunner, 15, 3)
    assert (defaults.runner_class, defaults.config.interval, defaults.config.multiply_factor) == (
        WeightedTestRunner,
        7,
        2,
    )
    assert defaults.config.seed is not None
    assert config.runners[1].interval is None, "resolving must not change the stored runners"
