"""Region and runner enumeration under minicloud: no call the emulator cannot serve."""

import os
from unittest.mock import patch

from sdcm.sct_runner import AwsSctRunner, list_sct_runners
from sdcm.utils.common import all_aws_regions


def test_all_aws_regions_uses_static_list_when_minicloud_active(monkeypatch):
    """minicloud has no DescribeRegions, so the API must not be called at all."""
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    with patch("sdcm.utils.common.boto3") as boto3_mock:
        regions = all_aws_regions()
    boto3_mock.client.assert_not_called()
    assert regions == all_aws_regions(cached=True)


def test_all_aws_regions_queries_the_api_without_minicloud(monkeypatch):
    """The real-cloud path is untouched: uncached still means DescribeRegions."""
    for var in ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL"):
        monkeypatch.delenv(var, raising=False)
    with patch("sdcm.utils.common.boto3") as boto3_mock:
        boto3_mock.client.return_value.describe_regions.return_value = {"Regions": [{"RegionName": "eu-west-1"}]}
        regions = all_aws_regions()
    assert regions == ["eu-west-1"]
    boto3_mock.client.assert_called_once()


def test_runner_lookup_bypasses_the_minicloud_endpoint(monkeypatch):
    """Runner discovery must reach the real cloud: the emulator answers with the guests."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:5000")
    monkeypatch.setenv("SCT_MINICLOUD_ENDPOINT_URL", "http://localhost:5000")
    seen = {}

    def fake_list(cls_self=None, verbose=True):
        seen["aws_endpoint_url"] = os.environ.get("AWS_ENDPOINT_URL")
        return []

    with patch.object(AwsSctRunner, "list_sct_runners", classmethod(lambda cls, verbose=True: fake_list())):
        list_sct_runners(backend="aws", test_id="some-test-id", verbose=False)

    assert seen["aws_endpoint_url"] is None, "runner lookup ran against the emulated endpoint"
    assert os.environ["AWS_ENDPOINT_URL"] == "http://localhost:5000", "endpoint not restored"
