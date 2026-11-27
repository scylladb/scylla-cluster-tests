import pytest

from sdcm.utils.perftune_override import cpu_mask_to_cpuset, parse_cpu_mask


@pytest.mark.parametrize(
    ("mask", "cpuset"),
    [
        pytest.param("0x0000fffe", "1-15", id="z4d-16-one-irq-thread"),
        pytest.param("0x0000fefe", "1-7,9-15", id="z4d-16-irq-core"),
        pytest.param("0x0000ffff", "0-15", id="all-cpus"),
        pytest.param("0x00000003,0x00000003", "0-1,32-33", id="two-words"),
        pytest.param("0x00000001,,0x00000001", "0,64", id="empty-word-is-zero"),
        pytest.param("0x00000008", "3", id="single-cpu"),
    ],
)
def test_cpu_mask_to_cpuset(mask, cpuset):
    assert cpu_mask_to_cpuset(mask) == cpuset


def test_cpu_mask_to_cpuset_rejects_empty_mask():
    with pytest.raises(ValueError, match="has no CPUs"):
        cpu_mask_to_cpuset("0x00000000")


@pytest.mark.parametrize(
    ("output", "mask"),
    [
        pytest.param("0x0000fffe\n", "0x0000fffe", id="mask-only"),
        pytest.param("WARNING: NIC eth0 is not a bond\n0x0000fffe\n", "0x0000fffe", id="warning-first"),
        pytest.param("0x00000003,,0x00000003\n", "0x00000003,,0x00000003", id="multi-word"),
    ],
)
def test_parse_cpu_mask(output, mask):
    assert parse_cpu_mask(output) == mask


def test_parse_cpu_mask_rejects_output_without_mask():
    with pytest.raises(ValueError, match="No CPU mask"):
        parse_cpu_mask("ERROR: something failed\n")
