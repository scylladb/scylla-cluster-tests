from sdcm.utils.perftune_validator import count_bits


def test_single_mask():
    """count_bits counts the set bits of one hex mask."""
    assert count_bits("0x00000001") == 1
    assert count_bits("0x0000000F") == 4
    assert count_bits("0x00000000") == 0


def test_multiple_masks():
    """count_bits sums the set bits across comma-separated masks."""
    assert count_bits("0x00000001,0x00000001") == 2
    assert count_bits("0x0000000F,0x0000000F") == 8
    assert count_bits("0x00000001,0x00000000") == 1


def test_empty_mask():
    """count_bits returns 0 for an empty string or only commas."""
    assert count_bits("") == 0
    assert count_bits(",") == 0
    assert count_bits(",,,") == 0


def test_mixed_empty_and_valid():
    """count_bits skips empty fields between valid masks."""
    assert count_bits("0x00000001,,0x00000001") == 2
    assert count_bits("0x00070000,0x00000007,,0x00070000,0x00000007") == 12


def test_leading_trailing_commas():
    """count_bits ignores empty fields at either end of the list."""
    assert count_bits(",0x00000001,") == 1
    assert count_bits(",0x00000001,,") == 1
