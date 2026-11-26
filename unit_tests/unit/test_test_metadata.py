import pytest

from sdcm.test_metadata import TestMetadata, _get_valid_backends


def test_valid_minimal():
    """TestMetadata() defaults: tier is ondemand, list fields are empty, optional fields are None."""
    m = TestMetadata()
    assert m.description is None
    assert m.test_type is None
    assert m.tier == "ondemand"
    assert m.stress_tools == []
    assert m.features == []


def test_valid_full():
    """All fields set in one TestMetadata are kept as given."""
    m = TestMetadata(
        description="A 4-hour longevity test with SisyphusMonkey on a 6-node cluster.",
        test_type="longevity",
        tier="tier1",
        duration_class="short",
        supported_backends=["aws", "gce"],
        stress_tools=["cassandra-stress"],
        workload="write",
        features=["multi-dc"],
    )
    assert m.test_type == "longevity"
    assert m.tier == "tier1"
    assert m.supported_backends == ["aws", "gce"]


def test_invalid_test_type():
    """An unknown test_type is rejected with a validation error naming the field."""
    with pytest.raises(ValueError, match="Invalid test_type"):
        TestMetadata(test_type="unknown-type")


@pytest.mark.parametrize(
    "field,value,match",
    [
        pytest.param("tier", "gold", "Invalid tier", id="tier"),
        pytest.param("duration_class", "instant", "Invalid duration_class", id="duration-class"),
        pytest.param("workload", "delete", "Invalid workload", id="workload"),
    ],
)
def test_invalid_enum_value(field, value, match):
    """An enum field set to a value outside its taxonomy is rejected with a validation error."""
    with pytest.raises(ValueError, match=match):
        TestMetadata(**{field: value})


def test_invalid_backend():
    """An unknown backend in supported_backends is rejected."""
    with pytest.raises(ValueError, match="Invalid backend"):
        TestMetadata(supported_backends=["aws", "not-a-backend"])


def test_supported_backends_none_means_all():
    """supported_backends=None is kept as None, which means all backends."""
    m = TestMetadata(supported_backends=None)
    assert m.supported_backends is None


def test_all_valid_backends_accepted():
    """Every backend known to the taxonomy is accepted in supported_backends."""
    m = TestMetadata(supported_backends=list(_get_valid_backends()))
    assert set(m.supported_backends) == _get_valid_backends()


def test_empty_supported_backends_list():
    """An empty supported_backends list is kept as empty, not replaced by None."""
    m = TestMetadata(supported_backends=[])
    assert m.supported_backends == []


def test_model_dump_roundtrip():
    """model_dump() fed back into TestMetadata rebuilds an equal model."""
    original = TestMetadata(
        description="Test roundtrip.",
        test_type="artifacts",
        tier="sanity",
        duration_class="short",
        supported_backends=["aws"],
        stress_tools=[],
    )
    dumped = original.model_dump()
    restored = TestMetadata(**dumped)
    assert restored.description == original.description
    assert restored.test_type == original.test_type
    assert restored.tier == original.tier
    assert restored.duration_class == original.duration_class
    assert restored.supported_backends == original.supported_backends
    assert restored.stress_tools == original.stress_tools
    assert restored.workload == original.workload
    assert restored.features == original.features
    assert restored == original


@pytest.mark.parametrize(
    "tier",
    [
        pytest.param("sanity", id="sanity"),
        pytest.param("tier1", id="tier1"),
        pytest.param("tier2", id="tier2"),
        pytest.param("artifacts", id="artifacts"),
        pytest.param("release", id="release"),
        pytest.param("ondemand", id="ondemand"),
    ],
)
def test_all_valid_tiers(tier):
    """Every tier in the taxonomy is accepted and kept as given."""
    m = TestMetadata(tier=tier)
    assert m.tier == tier


@pytest.mark.parametrize(
    "test_type",
    [
        pytest.param("longevity", id="longevity"),
        pytest.param("performance", id="performance"),
        pytest.param("upgrade", id="upgrade"),
        pytest.param("artifacts", id="artifacts"),
        pytest.param("manager", id="manager"),
        pytest.param("functional", id="functional"),
        pytest.param("scale", id="scale"),
        pytest.param("jepsen", id="jepsen"),
        pytest.param("gemini", id="gemini"),
        pytest.param("features", id="features"),
        pytest.param("platform-migration", id="platform-migration"),
        pytest.param("vector-search", id="vector-search"),
        pytest.param("cdc", id="cdc"),
    ],
)
def test_all_valid_test_types(test_type):
    """Every test_type in the taxonomy is accepted and kept as given."""
    m = TestMetadata(test_type=test_type)
    assert m.test_type == test_type


@pytest.mark.parametrize(
    "workload",
    [
        pytest.param("write", id="write"),
        pytest.param("read", id="read"),
        pytest.param("mixed", id="mixed"),
        pytest.param("counter", id="counter"),
        pytest.param("lwt", id="lwt"),
        pytest.param("cdc", id="cdc"),
        pytest.param("mv", id="mv"),
        pytest.param("si", id="si"),
        pytest.param("alternator", id="alternator"),
        pytest.param("user-profile", id="user-profile"),
    ],
)
def test_all_valid_workloads(workload):
    """Every workload in the taxonomy is accepted and kept as given."""
    m = TestMetadata(workload=workload)
    assert m.workload == workload


def test_invalid_team_ownership():
    """An unknown team_ownership is rejected with a validation error."""
    with pytest.raises(ValueError, match="Invalid team_ownership"):
        TestMetadata(team_ownership="not-a-team")


@pytest.mark.parametrize(
    "team",
    [
        pytest.param("scylladb/testing-infrastructure", id="testing-infrastructure"),
        pytest.param("scylladb/core-qa", id="core-qa"),
        pytest.param("scylladb/storage-qa", id="storage-qa"),
        pytest.param("scylladb/rnd-cloud-qa", id="rnd-cloud-qa"),
        pytest.param("scylladb/alternator", id="alternator"),
        pytest.param("scylladb/mgmt", id="mgmt"),
    ],
)
def test_all_valid_team_ownership(team):
    """Every team in the taxonomy is accepted and kept as given."""
    m = TestMetadata(team_ownership=team)
    assert m.team_ownership == team


def test_team_ownership_none_by_default():
    """team_ownership is None when not set."""
    m = TestMetadata()
    assert m.team_ownership is None


def test_from_dict():
    """TestMetadata built from a dict keeps every field value."""
    data = {
        "description": "Upgrade test with TLS.",
        "test_type": "upgrade",
        "tier": "tier1",
        "duration_class": "medium",
        "supported_backends": ["gce", "aws"],
        "stress_tools": ["cassandra-stress"],
        "workload": "mixed",
        "features": ["tls-ssl"],
    }
    m = TestMetadata(**data)
    assert m.description == "Upgrade test with TLS."
    assert m.test_type == "upgrade"
    assert m.tier == "tier1"
    assert m.duration_class == "medium"
    assert m.supported_backends == ["gce", "aws"]
    assert m.stress_tools == ["cassandra-stress"]
    assert m.workload == "mixed"
    assert m.features == ["tls-ssl"]
