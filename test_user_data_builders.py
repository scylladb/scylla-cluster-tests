import json

import pytest

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

<<<<<<< HEAD:test_user_data_builders.py
    assert 'Content-Type: multipart/mixed' in output
    assert 'Content-Type: x-scylla/json' in output
    assert 'Content-Type: text/cloud-config' in output
    assert 'Content-Type: text/x-shellscript' in output
    print(output)
||||||| parent of e29892926 (improvement(treewide): Reformat using ruff):unit_tests/test_user_data_builders.py
    assert 'Content-Type: multipart/mixed' in output
    assert 'Content-Type: x-scylla/json' in output
    assert 'Content-Type: text/cloud-config' in output
    assert 'Content-Type: text/x-shellscript' in output
=======
    assert "Content-Type: multipart/mixed" in output
    assert "Content-Type: x-scylla/json" in output
    assert "Content-Type: text/cloud-config" in output
    assert "Content-Type: text/x-shellscript" in output
>>>>>>> e29892926 (improvement(treewide): Reformat using ruff):unit_tests/test_user_data_builders.py
