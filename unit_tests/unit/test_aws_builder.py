from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sdcm.utils.aws_builder import AwsBuilder


def test_auto_scaling_group_skips_zones_without_an_sct_subnet():
    """AWS adds zones (eu-west-2d) before SCT has a subnet there; the group must use only zones with one."""
    subnets = {"eu-west-2a": "subnet-a", "eu-west-2b": "subnet-b", "eu-west-2d": None}
    region = MagicMock(region_name="eu-west-2", availability_zones=list(subnets))
    region.sct_subnet.side_effect = lambda region_az: (
        subnets[region_az] and SimpleNamespace(subnet_id=subnets[region_az])
    )
    asg_client = MagicMock()

    with (
        patch("sdcm.utils.aws_builder.KeyStore"),
        patch("sdcm.utils.aws_builder.jenkins"),
        patch("sdcm.utils.aws_builder.boto3.client", return_value=asg_client),
    ):
        AwsBuilder(region).create_auto_scaling_group()

    kwargs = asg_client.create_auto_scaling_group.call_args.kwargs
    assert kwargs["AvailabilityZones"] == ["eu-west-2a", "eu-west-2b"]
    assert kwargs["VPCZoneIdentifier"] == "subnet-a,subnet-b"
