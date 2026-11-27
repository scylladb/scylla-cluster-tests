import pytest

from sdcm.utils import common
from sdcm.utils.common import RELOCATABLE_RELEASES_PREFIX, get_latest_scylla_release

PREFIX = RELOCATABLE_RELEASES_PREFIX


def series_listing(*series: str) -> tuple[list[str], list[str]]:
    return [f"{PREFIX}/scylladb-{s}/" for s in series], []


def build_listing(series: str, *versions: str) -> tuple[list[str], list[str]]:
    return [], [f"{PREFIX}/scylladb-{series}/00-Build-{v}-0.20261123.094008fc13e0.txt" for v in versions]


@pytest.fixture
def bucket(monkeypatch):
    """Serve S3 listings from a {prefix: (prefixes, keys)} dict instead of the downloads bucket."""
    listings = {}

    def fake_list(prefix, verify):
        return listings.get(prefix, ([], []))

    monkeypatch.setattr(common, "_list_relocatable_releases", fake_list)
    yield listings
    get_latest_scylla_release.cache_clear()


@pytest.mark.parametrize(
    "series, builds, expected",
    [
        pytest.param(
            ["2026.3", "2026.4"],
            {"2026.4": ["2026.4.0~rc3", "2026.4.0"], "2026.3": ["2026.3.6"]},
            "2026.4.0",
            id="newest_series_ga",
        ),
        pytest.param(
            ["2026.4", "2027.1"],
            {"2027.1": ["2027.1.0~rc0", "2027.1.0~rc1"], "2026.4": ["2026.4.0"]},
            "2026.4.0",
            id="newest_series_only_rcs",
        ),
        pytest.param(
            ["2026.4"],
            {"2026.4": ["2026.4.2", "2026.4.10", "2026.4.9"]},
            "2026.4.10",
            id="patch_compared_as_number",
        ),
        pytest.param(
            ["2026.10", "2026.9"],
            {"2026.10": ["2026.10.0"], "2026.9": ["2026.9.1"]},
            "2026.10.0",
            id="series_compared_as_version",
        ),
    ],
)
def test_get_latest_scylla_release(bucket, series, builds, expected):
    bucket[f"{PREFIX}/scylladb-"] = series_listing(*series)
    for name, versions in builds.items():
        bucket[f"{PREFIX}/scylladb-{name}/00-Build-{name}."] = build_listing(name, *versions)

    assert get_latest_scylla_release() == expected


def test_get_latest_scylla_release_without_ga(bucket):
    bucket[f"{PREFIX}/scylladb-"] = series_listing("2027.1")
    bucket[f"{PREFIX}/scylladb-2027.1/00-Build-2027.1."] = build_listing("2027.1", "2027.1.0~rc0")

    with pytest.raises(ValueError, match="No GA scylla release"):
        get_latest_scylla_release()


def test_list_relocatable_releases_reads_all_pages(monkeypatch):
    pages = [
        {"CommonPrefixes": [{"Prefix": "a/"}], "Contents": [{"Key": "k1"}]},
        {"Contents": [{"Key": "k2"}]},
        {"CommonPrefixes": [{"Prefix": "b/"}]},
    ]
    calls = []

    class FakePaginator:
        def paginate(self, **kwargs):
            calls.append(kwargs)
            return iter(pages)

    class FakeClient:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return FakePaginator()

    monkeypatch.setattr(common.boto3, "client", lambda *args, **kwargs: FakeClient())

    assert common._list_relocatable_releases("p/", verify=True) == (["a/", "b/"], ["k1", "k2"])
    assert calls == [{"Bucket": common.SCYLLA_DOWNLOADS_BUCKET, "Prefix": "p/", "Delimiter": "/"}]
