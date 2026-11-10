from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.oci_builder import OciBuilder

INSTANCE_CONFIG = {
    "image_id": "img",
    "shape": "VM.Standard.E4.Flex",
    "vcn_id": "vcn",
    "subnet_id": "subnet",
    "compartment_id": "comp",
}


@pytest.fixture(name="builder")
def fixture_builder():
    region = MagicMock(region_name="us-phoenix-1", availability_domains=["a"])
    with patch("sdcm.utils.oci_builder.KeyStore"), patch("sdcm.utils.oci_builder.OciSctRunner"):
        builder = OciBuilder(region)
    builder.jenkins_info = {"url": "https://jenkins", "username": "u", "password": "p"}
    builder.runner._full_availability_domain = "ewbj:PHX-AD-1"
    builder._upload_regional_oci_credential = MagicMock(return_value="cred")
    builder._create_instance_configuration = MagicMock(return_value=INSTANCE_CONFIG)
    return builder


def test_script_console_error_with_http_200_fails(builder):
    """The script console returns 200 when the groovy throws; that must not pass as success."""
    response = MagicMock(status_code=200, text="java.lang.NoSuchMethodException: BaremetalCloudAgentTemplate.<init>")
    with (
        patch("sdcm.utils.oci_builder.requests.post", return_value=response),
        pytest.raises(RuntimeError, match="NoSuchMethodException"),
    ):
        builder._add_cloud_configuration_to_jenkins()


def test_script_console_success(builder):
    response = MagicMock(
        status_code=200, text="OCI cloud configured programmatically: oci-us-phoenix-1-qa-builder-v3-1"
    )
    with patch("sdcm.utils.oci_builder.requests.post", return_value=response) as post:
        builder._add_cloud_configuration_to_jenkins()
    script = post.call_args.kwargs["data"]["script"]
    assert "oci-sct-builders-us-phoenix-1-v3" in script
    assert "maxTotalUses" in script
