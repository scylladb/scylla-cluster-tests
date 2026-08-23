"""Tests for the duration-based `instance_provision` policy (SCT-850).

The policy must never override an explicit choice: performance jobs pin `on_demand` because spot noise and
reclamation invalidate latency numbers, and silently flipping them to spot would corrupt results rather than
just cost money. Both provenance channels (user config file and SCT_* env var, which the CLI also writes to)
are therefore tested against a real config load.
"""

import pytest

from sdcm.sct_config import SCTConfiguration


def _make_config(explicit: set[str] | None = None, **values):
    """Build a bare SCTConfiguration without running the full __init__ merge.

    `__init__` loads YAML defaults, resolves AMIs and talks to AWS; the policy under test only reads a handful
    of already-merged values, so it is exercised directly.
    """
    config = SCTConfiguration.model_construct()
    defaults = {
        "cluster_backend": "aws",
        "instance_provision": "spot",
        "test_duration": 240,
        "spot_max_test_duration": 720,
    }
    for key, value in {**defaults, **values}.items():
        setattr(config, key, value)
    config._explicitly_set_params = explicit or set()
    return config


@pytest.mark.parametrize(
    "duration, expected",
    [
        (60, "spot"),
        (719, "spot"),
        (720, "spot"),  # threshold is inclusive
        (721, "on_demand"),
        (7200, "on_demand"),
    ],
)
def test_duration_selects_provision_type(duration, expected):
    config = _make_config(test_duration=duration)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == expected


def test_explicit_on_demand_is_never_overridden():
    """A short perf job pinning on_demand must stay on_demand, or its latency numbers become meaningless."""
    config = _make_config(explicit={"instance_provision"}, instance_provision="on_demand", test_duration=90)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "on_demand"


def test_explicit_spot_is_never_overridden_for_long_tests():
    config = _make_config(explicit={"instance_provision"}, instance_provision="spot", test_duration=7200)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "spot"


def test_spot_fleet_is_not_flattened_to_spot():
    """spot_fleet is a spot variant; a short test must not lose the fleet request shape."""
    config = _make_config(instance_provision="spot_fleet", test_duration=120)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "spot_fleet"


def test_spot_fleet_still_moves_to_on_demand_for_long_tests():
    config = _make_config(instance_provision="spot_fleet", test_duration=7200)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "on_demand"


@pytest.mark.parametrize("backend", ["docker", "k8s-eks", "oci", "xcloud"])
def test_policy_skipped_for_unsupported_backends(backend):
    """OCI DenseIO shapes cannot be spot at all, and docker/k8s have no spot concept here."""
    config = _make_config(cluster_backend=backend, instance_provision="on_demand", test_duration=60)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "on_demand"


def test_policy_disabled_when_threshold_is_zero():
    config = _make_config(spot_max_test_duration=0, instance_provision="spot", test_duration=99999)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "spot"


def test_policy_noop_without_test_duration():
    """`test_duration` is typed `int`, so 0 is the only falsy value it can hold."""
    config = _make_config(test_duration=0, instance_provision="spot")
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "spot"


def test_custom_threshold_is_respected():
    config = _make_config(spot_max_test_duration=120, test_duration=180)
    config._apply_duration_based_provision_policy()
    assert config.get("instance_provision") == "on_demand"


def test_is_explicitly_set_reads_captured_provenance():
    config = _make_config(explicit={"instance_provision", "region_name"})
    assert config.is_explicitly_set("instance_provision") is True
    assert config.is_explicitly_set("region_name") is True
    assert config.is_explicitly_set("availability_zone") is False


def test_is_explicitly_set_defaults_to_false_when_unset():
    """Configs built without going through __init__ must not crash the policy."""
    config = SCTConfiguration.model_construct()
    assert config.is_explicitly_set("instance_provision") is False


class TestProvenanceCapture:
    """Provenance must be captured from real config loading, not inferred from merged values.

    This is the risky half of the policy: if a value inherited from `defaults/` were mistaken for an explicit
    one (or the reverse), jobs would silently get the wrong provision type.
    """

    @staticmethod
    def _load(monkeypatch, **env):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
        # a placement file left by an earlier run must not rewrite region/AZ under the test
        monkeypatch.setenv("SCT_IGNORE_RESOLVED_PLACEMENT", "1")
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        return SCTConfiguration()

    def test_defaults_are_not_explicit(self, monkeypatch):
        """`instance_provision: spot` comes from defaults/aws_config.yaml, so the policy may override it."""
        config = self._load(monkeypatch)
        assert config.is_explicitly_set("instance_provision") is False

    def test_env_var_marks_param_explicit(self, monkeypatch):
        config = self._load(monkeypatch, SCT_INSTANCE_PROVISION="on_demand")
        assert config.is_explicitly_set("instance_provision") is True
        assert config.get("instance_provision") == "on_demand"

    def test_user_config_file_marks_param_explicit(self, monkeypatch):
        """A test case listing instance_provision is an explicit choice; minimal_test_case.yaml does not."""
        config = self._load(monkeypatch, SCT_CONFIG_FILES="test-cases/artifacts/ami.yaml")
        assert config.is_explicitly_set("instance_provision") is True
        # sanity: a param the file really does set, and one it does not
        assert config.is_explicitly_set("n_db_nodes") is True
        assert config.is_explicitly_set("fallback_to_next_region") is False

    def test_short_test_gets_spot_from_policy(self, monkeypatch):
        config = self._load(monkeypatch)
        assert config.get("test_duration") <= config.get("spot_max_test_duration")
        assert config.get("instance_provision") == "spot"

    def test_long_test_gets_on_demand_from_policy(self, monkeypatch):
        config = self._load(monkeypatch, SCT_TEST_DURATION="7200")
        assert config.get("instance_provision") == "on_demand"

    def test_explicit_pin_survives_full_config_load(self, monkeypatch):
        """End-to-end guard for the perf-job case: long test, explicitly pinned spot, must stay spot."""
        config = self._load(monkeypatch, SCT_TEST_DURATION="7200", SCT_INSTANCE_PROVISION="spot")
        assert config.get("instance_provision") == "spot"
