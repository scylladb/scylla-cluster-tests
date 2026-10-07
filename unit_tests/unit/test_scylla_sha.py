import pytest

from sdcm.utils.scylla_sha import scylla_sha_from_version, scylla_version_matches_sha, sha_selector


def test_scylla_sha_lookup():
    assert scylla_sha_from_version("2026.4.0~dev-0.20261006.bedcc695789c") == "bedcc695789c"
    assert scylla_sha_from_version("2026-4-0-dev-0-20261006-bedcc695789c") == "bedcc695789c"  # GCE label
    assert scylla_sha_from_version("2026.4.0") == ""
    assert sha_selector("latest") is None
    assert sha_selector("BEDCC69") == "bedcc69"
    assert scylla_version_matches_sha("2026.4.0~dev-0.20261006.bedcc695789c", "bedcc69")
    assert not scylla_version_matches_sha("2026.4.0~dev-0.20261006.bedcc695789c", "df04dc4")
    with pytest.raises(ValueError, match="not a Scylla SHA"):
        sha_selector("2131")
