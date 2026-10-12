import base64
import gzip
import json
from email import message_from_string

import pytest
import yaml
from unittest.mock import patch

from sdcm.sct_provision.aws.user_data import ScyllaUserDataBuilder
from sdcm.utils.sct_cmd_helpers import get_test_config

pytestmark = pytest.mark.parametrize(
    "logs_transport",
    [
        "libssh2",
        "syslog-ng",
        "vector",
    ],
)


@pytest.fixture
def test_config():
    config = get_test_config()
    config.set_test_id("12345678-87654321")
    config.SYSLOGNG_ADDRESS = ("localhost", 12345)
    return config


def test_user_data_format_version_v2_building(logs_transport, test_config):
    builder = ScyllaUserDataBuilder(
        cluster_name="test",
        syslog_host_port=("0.0.0.0", 1234),
        user_data_format_version="2",
        test_config=test_config,
        params={"data_volume_disk_num": 0, "logs_transport": logs_transport},
    )
    output = builder.to_string()
    json_output = json.loads(output)
    assert "post_configuration_script" in json_output
    assert json_output["start_scylla_on_first_boot"] is False
    assert json_output["raid_level"] == 0


def test_user_data_format_version_v1_building(logs_transport, test_config):
    builder = ScyllaUserDataBuilder(
        cluster_name="test",
        syslog_host_port=("0.0.0.0", 1234),
        test_config=test_config,
        user_data_format_version="1",
        params={"data_volume_disk_num": 0, "logs_transport": logs_transport},
    )
    output = builder.to_string()
    assert "--stop-services --base64postscript=" in output


def test_user_data_format_version_v3_building(logs_transport, test_config):
    builder = ScyllaUserDataBuilder(
        cluster_name="test",
        user_data_format_version="3",
        syslog_host_port=("0.0.0.0", 1234),
        test_config=test_config,
        params={"data_volume_disk_num": 0, "logs_transport": logs_transport},
    )
    output = builder.return_in_format_v3()

    assert "Content-Type: multipart/mixed" in output
    assert "Content-Type: x-scylla/json" in output
    assert "Content-Type: text/cloud-config" in output

    # compressed to fit the EC2 user data limit; cloud-init unpacks it into a shell script part
    parts = {part.get_content_type(): part for part in message_from_string(output).walk()}
    script = gzip.decompress(parts["application/x-gzip"].get_payload(decode=True)).decode()
    assert script == base64.b64decode(builder.post_configuration_script).decode()
    assert script.startswith("#!/bin/bash")


@pytest.mark.parametrize("minicloud", [True, False], ids=["minicloud", "real-aws"])
def test_v3_raises_image_setup_timeout_only_on_minicloud(logs_transport, test_config, minicloud):
    """Under TCG emulation the image's own 15 min scylla-image-setup limit kills the setup."""
    builder = ScyllaUserDataBuilder(
        cluster_name="test",
        user_data_format_version="3",
        syslog_host_port=("0.0.0.0", 1234),
        test_config=test_config,
        params={"data_volume_disk_num": 0, "logs_transport": logs_transport},
    )
    with patch("sdcm.sct_provision.aws.user_data.is_minicloud_active", return_value=minicloud):
        output = builder.return_in_format_v3()
    parts = {part.get_content_type(): part for part in message_from_string(output).walk()}
    cloud_config = yaml.safe_load(parts["text/cloud-config"].get_payload())
    if minicloud:
        assert "TimeoutStartSec=3600" in " ".join(cloud_config["bootcmd"])
        assert cloud_config["bootcmd"][-1] == "systemctl daemon-reload"
    else:
        assert "bootcmd" not in cloud_config
