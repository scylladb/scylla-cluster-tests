import pytest

from sdcm.test_metadata import TestMetadata, _get_valid_backends


def test_valid_minimal():
    m = TestMetadata()
    assert m.description is None
    assert m.test_type is None
    assert m.tier is None
    assert m.stress_tools == []
    assert m.nemesis_labels == []
    assert m.features == []


def test_valid_full():
    m = TestMetadata(
        description="A 4-hour longevity test with SisyphusMonkey on a 6-node cluster.",
        test_type="longevity",
        tier="tier1",
        duration_class="short",
        supported_backends=["aws", "gce"],
        stress_tools=["cassandra-stress"],
        workload="write",
        nemesis_labels=["SisyphusMonkey"],
        features=["multi-dc"],
    )
    assert m.test_type == "longevity"
    assert m.tier == "tier1"
    assert m.supported_backends == ["aws", "gce"]


def test_invalid_test_type():
    with pytest.raises(Exception):
        TestMetadata(test_type="unknown-type")


def test_invalid_tier():
    with pytest.raises(Exception):
        TestMetadata(tier="gold")


def test_invalid_duration_class():
    with pytest.raises(Exception):
        TestMetadata(duration_class="instant")


def test_invalid_workload():
    with pytest.raises(Exception):
        TestMetadata(workload="delete")


def test_invalid_backend():
    with pytest.raises(ValueError, match="Invalid backend"):
        TestMetadata(supported_backends=["aws", "not-a-backend"])


def test_supported_backends_none_means_all():
    m = TestMetadata(supported_backends=None)
    assert m.supported_backends is None


def test_all_valid_backends_accepted():
    m = TestMetadata(supported_backends=list(_get_valid_backends()))
    assert set(m.supported_backends) == _get_valid_backends()


def test_empty_supported_backends_list():
    m = TestMetadata(supported_backends=[])
    assert m.supported_backends == []


def test_model_dump_roundtrip():
    original = TestMetadata(
        description="Test roundtrip.",
        test_type="artifacts",
        tier="sanity",
        duration_class="short",
        supported_backends=["aws"],
        stress_tools=[],
        nemesis_labels=["NoOpMonkey"],
    )
    dumped = original.model_dump()
    restored = TestMetadata(**dumped)
    assert restored == original


@pytest.mark.parametrize("tier", ["sanity", "tier1", "release", "ondemand"])
def test_all_valid_tiers(tier):
    m = TestMetadata(tier=tier)
    assert m.tier == tier


@pytest.mark.parametrize(
    "test_type",
    [
        "longevity",
        "performance",
        "upgrade",
        "artifacts",
        "manager",
        "functional",
        "scale",
        "jepsen",
        "gemini",
        "features",
        "platform-migration",
        "vector-search",
        "cdc",
    ],
)
def test_all_valid_test_types(test_type):
    m = TestMetadata(test_type=test_type)
    assert m.test_type == test_type


@pytest.mark.parametrize(
    "workload",
    [
        "write",
        "read",
        "mixed",
        "counter",
        "lwt",
        "cdc",
        "mv",
        "si",
        "alternator",
        "user-profile",
    ],
)
def test_all_valid_workloads(workload):
    m = TestMetadata(workload=workload)
    assert m.workload == workload


def test_from_dict():
    data = {
        "description": "Upgrade test with TLS.",
        "test_type": "upgrade",
        "tier": "tier1",
        "duration_class": "medium",
        "supported_backends": ["gce", "aws"],
        "stress_tools": ["cassandra-stress"],
        "workload": "mixed",
        "nemesis_labels": [],
        "features": ["tls-ssl"],
    }
    m = TestMetadata(**data)
    assert m.features == ["tls-ssl"]
    assert m.duration_class == "medium"
