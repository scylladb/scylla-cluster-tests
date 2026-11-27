from sdcm.utils.perftune_validator import count_bits


def test_single_mask():
    """count_bits counts the set bits of a single hex mask."""
    assert count_bits("0x00000001") == 1
    assert count_bits("0x0000000F") == 4
    assert count_bits("0x00000000") == 0


def test_multiple_masks():
    """count_bits sums the set bits across comma-separated masks."""
    assert count_bits("0x00000001,0x00000001") == 2
    assert count_bits("0x0000000F,0x0000000F") == 8
    assert count_bits("0x00000001,0x00000000") == 1


def test_empty_mask():
    """Empty masks and comma-only strings count as zero bits."""
    assert count_bits("") == 0
    assert count_bits(",") == 0
    assert count_bits(",,,") == 0


def test_mixed_empty_and_valid():
    """Empty entries between commas are skipped while the valid masks are summed."""
    assert count_bits("0x00000001,,0x00000001") == 2
    assert count_bits("0x00070000,0x00000007,,0x00070000,0x00000007") == 12


def test_leading_trailing_commas():
    """Leading and trailing commas are ignored."""
    assert count_bits(",0x00000001,") == 1
    assert count_bits(",0x00000001,,") == 1
