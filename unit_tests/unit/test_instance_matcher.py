import logging

import pytest

from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo
from sdcm.utils.cloud_catalog.instance_matcher import (
    Constraint,
    NoMatchingInstanceError,
    is_literal_instance_type,
    parse_constraints,
    select_instance,
)


# ---------------------------------------------------------------------------
# parse_constraints — vcpu-only cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param(
            {"vcpu": 8},
            [Constraint("vcpus", "eq", 8)],
            id="vcpu_plain_int",
        ),
        pytest.param(
            {"vcpus": 4},
            [Constraint("vcpus", "eq", 4)],
            id="vcpu_canonical_name",
        ),
        pytest.param(
            {"vcpu": "16"},
            [Constraint("vcpus", "eq", 16)],
            id="vcpu_string_int",
        ),
        pytest.param(
            {"vcpu": "8-16"},
            [Constraint("vcpus", "range", (8, 16))],
            id="vcpu_range_string",
        ),
        pytest.param(
            {"vcpu": " 8 "},
            [Constraint("vcpus", "eq", 8)],
            id="vcpu_string_with_whitespace",
        ),
        pytest.param(
            {"vcpu": "2-4"},
            [Constraint("vcpus", "range", (2, 4))],
            id="vcpu_small_range",
        ),
    ],
)
def test_parse_constraints_vcpu_only(raw, expected):
    assert parse_constraints(raw) == expected


# ---------------------------------------------------------------------------
# parse_constraints — memory field cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "memory_raw,expected_constraint",
    [
        pytest.param(32, Constraint("memory_gb", "gte", 32.0), id="memory_plain_int"),
        pytest.param("32", Constraint("memory_gb", "gte", 32.0), id="memory_string_int"),
        pytest.param("32gb", Constraint("memory_gb", "gte", 32.0), id="memory_gb_suffix"),
        pytest.param("32GB", Constraint("memory_gb", "gte", 32.0), id="memory_GB_upper"),
        pytest.param(">32", Constraint("memory_gb", "gt", 32.0), id="memory_gt"),
        pytest.param(">32gb", Constraint("memory_gb", "gt", 32.0), id="memory_gt_gb"),
        pytest.param(">=32", Constraint("memory_gb", "gte", 32.0), id="memory_gte"),
        pytest.param(">=32gb", Constraint("memory_gb", "gte", 32.0), id="memory_gte_gb"),
        pytest.param("<64", Constraint("memory_gb", "lt", 64.0), id="memory_lt"),
        pytest.param("<64gb", Constraint("memory_gb", "lt", 64.0), id="memory_lt_gb"),
        pytest.param("<=64", Constraint("memory_gb", "lte", 64.0), id="memory_lte"),
        pytest.param("16-64", Constraint("memory_gb", "range", (16.0, 64.0)), id="memory_range"),
        pytest.param("16gb-64gb", Constraint("memory_gb", "range", (16.0, 64.0)), id="memory_range_gb"),
        pytest.param(" > 32gb ", Constraint("memory_gb", "gt", 32.0), id="memory_whitespace_around"),
    ],
)
def test_parse_constraints_memory(memory_raw, expected_constraint):
    result = parse_constraints({"vcpu": 4, "memory": memory_raw})
    memory_constraints = [c for c in result if c.field == "memory_gb"]
    assert len(memory_constraints) == 1
    assert memory_constraints[0] == expected_constraint


# ---------------------------------------------------------------------------
# parse_constraints — disk field cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "disk_raw,expected_constraint",
    [
        pytest.param(500, Constraint("local_disk_gb", "gte", 500.0), id="disk_plain_int"),
        pytest.param("500gb", Constraint("local_disk_gb", "gte", 500.0), id="disk_gb_suffix"),
        pytest.param("1tb", Constraint("local_disk_gb", "gte", 1024.0), id="disk_tb_suffix"),
        pytest.param("1TB", Constraint("local_disk_gb", "gte", 1024.0), id="disk_TB_upper"),
        pytest.param("2tb", Constraint("local_disk_gb", "gte", 2048.0), id="disk_2tb"),
        pytest.param(">=500", Constraint("local_disk_gb", "gte", 500.0), id="disk_gte"),
        pytest.param(">200gb", Constraint("local_disk_gb", "gt", 200.0), id="disk_gt_gb"),
        pytest.param("100-500", Constraint("local_disk_gb", "range", (100.0, 500.0)), id="disk_range"),
        pytest.param("local_disk_gb", None, id="disk_canonical_name_key"),
    ],
)
def test_parse_constraints_disk(disk_raw, expected_constraint):
    if disk_raw == "local_disk_gb":
        result = parse_constraints({"vcpu": 4, "local_disk_gb": 500})
        disk_constraints = [c for c in result if c.field == "local_disk_gb"]
        assert len(disk_constraints) == 1
        assert disk_constraints[0] == Constraint("local_disk_gb", "gte", 500.0)
    else:
        result = parse_constraints({"vcpu": 4, "disk": disk_raw})
        disk_constraints = [c for c in result if c.field == "local_disk_gb"]
        assert len(disk_constraints) == 1
        assert disk_constraints[0] == expected_constraint


# ---------------------------------------------------------------------------
# parse_constraints — arch field cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "arch_raw,expected_value",
    [
        pytest.param("arm64", "arm64", id="arch_arm64"),
        pytest.param("arm", "arm64", id="arch_arm_shorthand"),
        pytest.param("ARM", "arm64", id="arch_ARM_upper"),
        pytest.param("x86_64", "x86_64", id="arch_x86_64"),
        pytest.param("x86", "x86_64", id="arch_x86_shorthand"),
        pytest.param("amd64", "x86_64", id="arch_amd64_alias"),
        pytest.param("X86", "x86_64", id="arch_X86_upper"),
    ],
)
def test_parse_constraints_arch(arch_raw, expected_value):
    result = parse_constraints({"vcpu": 4, "arch": arch_raw})
    arch_constraints = [c for c in result if c.field == "arch"]
    assert len(arch_constraints) == 1
    assert arch_constraints[0] == Constraint("arch", "eq", expected_value)


# ---------------------------------------------------------------------------
# parse_constraints — combined / multi-field cases
# ---------------------------------------------------------------------------


def test_parse_constraints_all_fields():
    raw = {"vcpu": 8, "memory": 32, "disk": ">=500gb", "arch": "arm64"}
    result = parse_constraints(raw)
    fields = {c.field: c for c in result}
    assert fields["vcpus"] == Constraint("vcpus", "eq", 8)
    assert fields["memory_gb"] == Constraint("memory_gb", "gte", 32.0)
    assert fields["local_disk_gb"] == Constraint("local_disk_gb", "gte", 500.0)
    assert fields["arch"] == Constraint("arch", "eq", "arm64")


def test_parse_constraints_all_canonical_names():
    raw = {"vcpus": 8, "memory_gb": 32, "local_disk_gb": 100}
    result = parse_constraints(raw)
    fields = {c.field: c for c in result}
    assert fields["vcpus"] == Constraint("vcpus", "eq", 8)
    assert fields["memory_gb"] == Constraint("memory_gb", "gte", 32.0)
    assert fields["local_disk_gb"] == Constraint("local_disk_gb", "gte", 100.0)


def test_parse_constraints_string_values_from_env():
    raw = {"vcpu": "8", "memory": "32", "disk": "500"}
    result = parse_constraints(raw)
    fields = {c.field: c for c in result}
    assert fields["vcpus"] == Constraint("vcpus", "eq", 8)
    assert fields["memory_gb"] == Constraint("memory_gb", "gte", 32.0)
    assert fields["local_disk_gb"] == Constraint("local_disk_gb", "gte", 500.0)


def test_parse_constraints_vcpu_range_with_memory():
    raw = {"vcpu": "8-16", "memory": ">= 32gb"}
    result = parse_constraints(raw)
    fields = {c.field: c for c in result}
    assert fields["vcpus"] == Constraint("vcpus", "range", (8, 16))
    assert fields["memory_gb"] == Constraint("memory_gb", "gte", 32.0)


# ---------------------------------------------------------------------------
# parse_constraints — error cases
# ---------------------------------------------------------------------------


def test_parse_constraints_missing_vcpu_raises():
    with pytest.raises(ValueError, match="vcpu is mandatory"):
        parse_constraints({"memory": 32})


def test_parse_constraints_missing_vcpu_only_disk_raises():
    with pytest.raises(ValueError, match="vcpu is mandatory"):
        parse_constraints({"disk": 500, "arch": "arm64"})


def test_parse_constraints_empty_dict_raises():
    with pytest.raises(ValueError, match="vcpu is mandatory"):
        parse_constraints({})


def test_parse_constraints_unknown_arch_raises():
    with pytest.raises(ValueError, match="Unknown architecture"):
        parse_constraints({"vcpu": 4, "arch": "powerpc"})


# ---------------------------------------------------------------------------
# parse_constraints — unknown key warning
# ---------------------------------------------------------------------------


def test_parse_constraints_unknown_key_logs_warning(caplog):
    with caplog.at_level(logging.WARNING, logger="sdcm.utils.cloud_catalog.instance_matcher"):
        result = parse_constraints({"vcpu": 8, "gpu": 2, "network": "10gbe"})
    assert any("gpu" in msg for msg in caplog.messages)
    assert any("network" in msg for msg in caplog.messages)
    fields = {c.field for c in result}
    assert "gpu" not in fields
    assert "network" not in fields
    assert "vcpus" in fields


# ---------------------------------------------------------------------------
# parse_constraints — whitespace stripping
# ---------------------------------------------------------------------------


def test_parse_constraints_whitespace_in_string_values():
    raw = {"vcpu": " 8 ", "memory": " > 32gb ", "arch": " arm64 "}
    result = parse_constraints(raw)
    fields = {c.field: c for c in result}
    assert fields["vcpus"] == Constraint("vcpus", "eq", 8)
    assert fields["memory_gb"] == Constraint("memory_gb", "gt", 32.0)
    assert fields["arch"] == Constraint("arch", "eq", "arm64")


# ---------------------------------------------------------------------------
# is_literal_instance_type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param("i8g.2xlarge", True, id="aws_dot_type"),
        pytest.param("m5.xlarge", True, id="aws_m5_xlarge"),
        pytest.param("r6g.4xlarge", True, id="aws_r6g"),
        pytest.param("Standard_L8s_v4", True, id="azure_standard"),
        pytest.param("Standard_D16s_v3", True, id="azure_d16"),
        pytest.param("n2-highmem-8", True, id="gce_n2"),
        pytest.param("e2-standard-4", True, id="gce_e2"),
        pytest.param("z3-highmem-4", True, id="gce_z3"),
        pytest.param("c2-standard-8", True, id="gce_c2"),
        pytest.param("t2a-standard-1", True, id="gce_t2a_arm"),
        pytest.param("VM.Standard3.Flex", True, id="oci_vm_dot"),
        pytest.param("BM.Standard3.64", True, id="oci_bm_dot"),
        pytest.param("DenseIO.E5.Flex", True, id="oci_denseio_dot"),
        pytest.param({"vcpu": 8}, False, id="dict_is_not_literal"),
        pytest.param("small", False, id="plain_word_no_dot"),
        pytest.param("large", False, id="plain_word_large"),
        pytest.param("", False, id="empty_string"),
        pytest.param(None, False, id="none_is_not_literal"),
        pytest.param(8, False, id="int_is_not_literal"),
        pytest.param(["i8g.2xlarge"], False, id="list_is_not_literal"),
    ],
)
def test_is_literal_instance_type(value, expected):
    assert is_literal_instance_type(value) == expected


# ---------------------------------------------------------------------------
# NoMatchingInstanceError
# ---------------------------------------------------------------------------


def test_no_matching_instance_error_message():
    constraints = [Constraint("vcpus", "eq", 8), Constraint("memory_gb", "gte", 32.0)]
    err = NoMatchingInstanceError(constraints, candidates_checked=42)
    assert "vcpus eq 8" in str(err)
    assert "memory_gb gte 32.0" in str(err)
    assert "42" in str(err)
    assert err.candidates_checked == 42
    assert err.constraints is constraints


def test_no_matching_instance_error_zero_candidates():
    constraints = [Constraint("vcpus", "range", (8, 16))]
    err = NoMatchingInstanceError(constraints)
    assert "vcpus range (8, 16)" in str(err)
    assert "0" in str(err)


# ---------------------------------------------------------------------------
# select_instance — fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def test_catalog():
    cat = InstanceCatalog()
    cat.cloud_defaults = {"aws": {"arch": "arm64"}, "gce": {"arch": "x86_64"}, "oci": {"arch": "x86_64"}}
    cat.preferred_families = {
        "db": {"aws": ["i8g", "i7i"], "gce": ["z3-highmem"]},
        "loader": {"aws": ["c6i"], "oci": ["VM.Standard"]},
        "monitor": {"aws": ["t3"], "oci": ["VM.Standard"]},
    }
    cat.instances = [
        InstanceTypeInfo("i8g.large", "aws", "i8g", 2, 16.0, 468.0, 1, "arm64", 0.50),
        InstanceTypeInfo("i8g.xlarge", "aws", "i8g", 4, 32.0, 937.0, 1, "arm64", 1.00),
        InstanceTypeInfo("i8g.2xlarge", "aws", "i8g", 8, 64.0, 1875.0, 1, "arm64", 2.00),
        InstanceTypeInfo("i8g.4xlarge", "aws", "i8g", 16, 128.0, 3750.0, 2, "arm64", 4.00),
        InstanceTypeInfo("i7i.large", "aws", "i7i", 2, 16.0, 468.0, 1, "x86_64", 0.60),
        InstanceTypeInfo("i7i.xlarge", "aws", "i7i", 4, 32.0, 937.0, 1, "x86_64", 1.20),
        InstanceTypeInfo("i7i.2xlarge", "aws", "i7i", 8, 64.0, 1875.0, 1, "x86_64", 2.40),
        InstanceTypeInfo("c6i.xlarge", "aws", "c6i", 4, 8.0, 0, 0, "x86_64", 0.17),
        InstanceTypeInfo("c6i.2xlarge", "aws", "c6i", 8, 16.0, 0, 0, "x86_64", 0.34),
        InstanceTypeInfo("t3.large", "aws", "t3", 2, 8.0, 0, 0, "x86_64", 0.08),
        InstanceTypeInfo("t3.xlarge", "aws", "t3", 4, 16.0, 0, 0, "x86_64", 0.17),
        InstanceTypeInfo("z3-highmem-8", "gce", "z3-highmem", 8, 64.0, 1536.0, 4, "x86_64", 3.00),
        InstanceTypeInfo("z3-highmem-16", "gce", "z3-highmem", 16, 128.0, 3072.0, 8, "x86_64", 6.00),
        InstanceTypeInfo(
            "VM.Standard.E4.Flex:2",
            "oci",
            "VM.Standard",
            4,
            32.0,
            0,
            0,
            "x86_64",
            0.066,
            min_memory_gb=2.0,
            max_memory_gb=128.0,
        ),
        InstanceTypeInfo(
            "VM.Standard.E4.Flex:4",
            "oci",
            "VM.Standard",
            8,
            64.0,
            0,
            0,
            "x86_64",
            0.132,
            min_memory_gb=4.0,
            max_memory_gb=256.0,
        ),
    ]
    return cat


# ---------------------------------------------------------------------------
# select_instance — tests
# ---------------------------------------------------------------------------


def test_select_exact_vcpu(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 8})
    assert result.instance_type == "i8g.2xlarge"


def test_select_vcpu_range(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": "4-8"})
    assert result.instance_type == "i8g.xlarge"


def test_select_with_arch_override(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 8, "arch": "x86"})
    assert result.instance_type == "i7i.2xlarge"


def test_select_with_arch_param(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 8}, arch="x86_64")
    assert result.instance_type == "i7i.2xlarge"


def test_select_with_memory_constraint(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 8, "memory": ">60"})
    assert result.instance_type == "i8g.2xlarge"


def test_select_memory_range(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": "4-16", "memory": "30-70"})
    assert result.instance_type == "i8g.xlarge"


def test_select_db_implies_local_disk(test_catalog):
    result = select_instance(test_catalog, "loader", "aws", {"vcpu": 4}, arch="x86_64")
    assert result.instance_type == "c6i.xlarge"


def test_select_db_requires_local_disk(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 4})
    assert result.instance_type == "i8g.xlarge"
    assert result.local_disk_count > 0


def test_select_monitor(test_catalog):
    result = select_instance(test_catalog, "monitor", "aws", {"vcpu": 2}, arch="x86_64")
    assert result.instance_type == "t3.large"


def test_select_gce(test_catalog):
    result = select_instance(test_catalog, "db", "gce", {"vcpu": 8})
    assert result.instance_type == "z3-highmem-8"


def test_select_no_match_raises(test_catalog):
    with pytest.raises(NoMatchingInstanceError):
        select_instance(test_catalog, "db", "aws", {"vcpu": 1024})


def test_select_prefers_cheaper(test_catalog):
    test_catalog.instances.append(
        InstanceTypeInfo("i8g.2xlarge-cheap", "aws", "i8g", 8, 64.0, 1875.0, 1, "arm64", 1.50),
    )
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 8})
    assert result.instance_type == "i8g.2xlarge-cheap"
    assert result.price_per_hour == 1.50


def test_select_prefers_family_order(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 8, "arch": "x86_64"})
    assert result.family == "i7i"

    result_arm = select_instance(test_catalog, "db", "aws", {"vcpu": 8})
    assert result_arm.family == "i8g"


def test_select_deterministic(test_catalog):
    r1 = select_instance(test_catalog, "db", "aws", {"vcpu": 8})
    r2 = select_instance(test_catalog, "db", "aws", {"vcpu": 8})
    assert r1.instance_type == r2.instance_type


def test_select_smallest_vcpu(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": "4-16"})
    assert result.vcpus == 4
    assert result.family == "i8g"


# ---------------------------------------------------------------------------
# select_instance — OCI Flex memory resolution
# ---------------------------------------------------------------------------


def test_flex_no_memory_constraint_uses_default(test_catalog):
    result = select_instance(test_catalog, "monitor", "oci", {"vcpu": 4})
    assert result.instance_type == "VM.Standard.E4.Flex:2:16"
    assert result.memory_gb == 16.0


def test_flex_with_exact_memory(test_catalog):
    result = select_instance(test_catalog, "monitor", "oci", {"vcpu": 4, "memory": "16"})
    assert result.instance_type == "VM.Standard.E4.Flex:2:16"
    assert result.memory_gb == 16.0


def test_flex_with_gte_memory(test_catalog):
    result = select_instance(test_catalog, "monitor", "oci", {"vcpu": 4, "memory": ">=64"})
    assert result.instance_type == "VM.Standard.E4.Flex:2:64"
    assert result.memory_gb == 64.0


def test_flex_memory_gte_near_max(test_catalog):
    result = select_instance(test_catalog, "monitor", "oci", {"vcpu": 8, "memory": ">=250"})
    assert result.instance_type == "VM.Standard.E4.Flex:4:250"
    assert result.memory_gb == 250.0


def test_flex_lte_memory_uses_default(test_catalog):
    result = select_instance(test_catalog, "monitor", "oci", {"vcpu": 4, "memory": "<=128"})
    assert result.instance_type == "VM.Standard.E4.Flex:2:16"
    assert result.memory_gb == 16.0


def test_flex_memory_range(test_catalog):
    result = select_instance(test_catalog, "monitor", "oci", {"vcpu": 4, "memory": "16-64"})
    assert result.instance_type == "VM.Standard.E4.Flex:2:16"
    assert result.memory_gb == 16.0


def test_non_flex_instance_unchanged(test_catalog):
    result = select_instance(test_catalog, "db", "aws", {"vcpu": 8})
    assert result.instance_type == "i8g.2xlarge"
    assert result.memory_gb == 64.0
