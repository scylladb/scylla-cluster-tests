from sdcm.utils.perftune_validator import count_bits


def test_single_mask():
    assert count_bits('0x00000001') == 1
    assert count_bits('0x0000000F') == 4
    assert count_bits('0x00000000') == 0


def test_multiple_masks():
    assert count_bits('0x00000001,0x00000001') == 2
    assert count_bits('0x0000000F,0x0000000F') == 8
    assert count_bits('0x00000001,0x00000000') == 1


def test_empty_mask():
    assert count_bits('') == 0
    assert count_bits(',') == 0
    assert count_bits(',,,') == 0


def test_mixed_empty_and_valid():
    assert count_bits('0x00000001,,0x00000001') == 2
    assert count_bits('0x00070000,0x00000007,,0x00070000,0x00000007') == 12


def test_leading_trailing_commas():
    assert count_bits(',0x00000001,') == 1
    assert count_bits(',0x00000001,,') == 1
