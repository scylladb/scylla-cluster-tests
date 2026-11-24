import pytest

from sdcm.utils.perftune_override import cpu_mask_to_cpuset, irq_on_all_cpus_options


@pytest.mark.parametrize(
    "mask,expected",
    [
        pytest.param("0x0000ffff", "0-15", id="16-cpus"),
        pytest.param("0x00000101", "0,8", id="irq-core-0-and-8"),
        pytest.param("0x0000fefe", "1-7,9-15", id="all-but-irq-core"),
        pytest.param("0x00000003,0x00000003", "0-1,32-33", id="two-words"),
        pytest.param("0x0000003f,,,0x0000003f", "0-5,96-101", id="empty-words-are-zero"),
        pytest.param("ffffffff,ffffffff", "0-63", id="no-0x-prefix"),
    ],
)
def test_cpu_mask_to_cpuset_converts_hwloc_masks(mask, expected):
    """A perftune/hwloc mask must become the CPU list Scylla's --cpuset expects, word order included."""
    assert cpu_mask_to_cpuset(mask) == expected


def test_cpu_mask_to_cpuset_empty_mask_raises():
    """An all-zero mask would start Scylla with no CPUs, so it must be rejected."""
    with pytest.raises(ValueError, match="no CPUs"):
        cpu_mask_to_cpuset("0x00000000")


def test_irq_on_all_cpus_options_sets_irq_mask_to_cpu_mask_and_drops_mode():
    """Perftune spreads IRQs over all CPUs only when irq_cpu_mask equals cpu_mask, and a mode key would override it."""
    original = {
        "cpu_mask": "0x0000ffff",
        "irq_cpu_mask": "0x00000101",
        "mode": "sq_split",
        "nic": ["eth0"],
        "tune": ["net", "system"],
    }

    options = irq_on_all_cpus_options(original)

    assert options == {
        "cpu_mask": "0x0000ffff",
        "irq_cpu_mask": "0x0000ffff",
        "nic": ["eth0"],
        "tune": ["net", "system"],
    }
    assert original["irq_cpu_mask"] == "0x00000101", "the input dict must not change"


def test_irq_on_all_cpus_options_without_cpu_mask_raises():
    """Without cpu_mask there is no 'all CPUs' to copy, so the yaml from scylla_setup is unusable."""
    with pytest.raises(ValueError, match="no cpu_mask"):
        irq_on_all_cpus_options({"nic": ["eth0"]})
