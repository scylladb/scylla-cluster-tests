"""Region discovery of all_aws_regions."""

from unittest.mock import patch

from sdcm.utils.common import all_aws_regions


def test_all_aws_regions_skips_unavailable_regions(monkeypatch):
    """A region that the account enables but AWS cannot serve must not reach the region scans."""
    for var in ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL"):
        monkeypatch.delenv(var, raising=False)
    with patch("sdcm.utils.common.boto3") as boto3_mock:
        boto3_mock.client.return_value.describe_regions.return_value = {
            "Regions": [{"RegionName": "eu-west-1"}, {"RegionName": "me-south-1"}, {"RegionName": "us-east-1"}]
        }
        regions = all_aws_regions()

    assert regions == ["eu-west-1", "us-east-1"]
