# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

import gzip
import subprocess
from email import message_from_string
from textwrap import dedent
from unittest.mock import Mock

import pytest

from sdcm.provision.aws.configuration_script import AWSConfigurationScriptBuilder
from sdcm.provision.common.utils import minify_shell_script
from sdcm.sct_provision.aws.user_data import AWSInstanceUserDataBuilder, ScyllaUserDataBuilder

# EC2 rejects user data above this many bytes, counted before base64 (SCT-1147)
EC2_USER_DATA_LIMIT = 16384


def _run(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", "-e", "-c", script], capture_output=True, text=True, check=False)


def test_heredoc_bodies_reach_the_file_byte_for_byte(tmp_path):
    body = "sources:\n    journald:\n        type: journald\n\n# a YAML comment, not a shell one\n"
    script = dedent(f"""
        # write the config
        if true; then
            cat > {tmp_path}/plain.yaml <<'EOF'
    {{body}}EOF
            cat > {tmp_path}/tabs.conf <<-EOF
    \t[Service]
    \tEOF
        fi  # trailing comment
        echo done
    """).replace("{body}", body)

    result = _run(minify_shell_script(script))

    assert result.returncode == 0, result.stderr
    assert result.stdout == "done\n"
    assert (tmp_path / "plain.yaml").read_text() == body
    assert (tmp_path / "tabs.conf").read_text() == "[Service]\n"


def test_keeps_hash_signs_which_are_not_comments():
    script = dedent("""
        set -- a b
        echo "$# # not a comment"
        echo $#  # a comment
    """)

    result = _run(minify_shell_script(script))

    assert result.stdout == "2 # not a comment\n2\n"


def test_keeps_the_shebang():
    assert minify_shell_script("#!/bin/bash\n# comment\necho x\n") == "#!/bin/bash\necho x\n"


@pytest.mark.parametrize("logs_transport", ["vector", "syslog-ng", "libssh2"])
def test_aws_boot_script_is_valid_bash(logs_transport):
    script = AWSConfigurationScriptBuilder(
        syslog_host_port=("10.0.0.1", 49153),
        logs_transport=logs_transport,
        install_docker=True,
        aws_ipv6_workaround=True,
    ).to_string()

    result = subprocess.run(["bash", "-n"], input=script, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


class _Params(dict):
    def get(self, key, default=None):
        return super().get(key, default)


def _scylla_user_data(params, test_config, install_docker):
    return ScyllaUserDataBuilder.model_construct(
        params=params,
        cluster_name="longevity-test-cust-time-loader-set-03290f28",
        user_data_format_version="3",
        syslog_host_port=("10.4.12.200", 49153),
        test_config=test_config,
        install_docker=install_docker,
        install_agent=True,
    ).to_string()


def _instance_user_data(params, test_config, install_docker):
    return AWSInstanceUserDataBuilder.model_construct(
        params=params,
        syslog_host_port=("10.4.12.200", 49153),
        test_config=test_config,
        aws_additional_interface=False,
        install_docker=install_docker,
        install_agent=True,
    ).to_string()


@pytest.mark.parametrize(
    "build_user_data,install_docker",
    [
        pytest.param(_scylla_user_data, True, id="sct-runner-provisioned-loader"),
        pytest.param(_instance_user_data, True, id="provision-resources-loader"),
        pytest.param(_instance_user_data, False, id="provision-resources-monitor"),
    ],
)
def test_aws_user_data_fits_the_ec2_limit_with_everything_enabled(build_user_data, install_docker):
    """The largest boot scripts SCT sends to EC2: docker, the agent and the ipv6 workaround all on.

    Both AWS paths: the cluster classes of the test itself, and `provision-resources`, which builds
    loaders and monitors with `AWSInstanceUserDataBuilder`.
    """
    params = _Params(
        data_volume_disk_num=0,
        raid_level=0,
        logs_transport="vector",
        cluster_backend="aws",
        ip_ssh_connections="ipv6",
        agent={
            "enabled": True,
            "binary_url": "https://downloads.scylladb.com/sct-agent/sct-agent-linux-amd64-" + "0" * 40,
            "port": 16000,
            "max_concurrent_jobs": 10,
            "log_level": "info",
        },
    )
    test_config = Mock()
    test_config.agent_api_key.return_value = "k" * 64
    # model_construct: validation would turn the dict into an SCTConfiguration, with the agent disabled
    user_data = build_user_data(params, test_config, install_docker)

    parts = {part.get_content_type(): part for part in message_from_string(user_data).walk()}
    script = gzip.decompress(parts["application/x-gzip"].get_payload(decode=True)).decode()
    assert script.startswith("#!/bin/bash")
    assert "sct-agent" in script, "the agent install must be part of what is measured"
    # half the limit: room for what a test config can still add, like a longer scylla_yaml
    assert len(user_data.encode()) < EC2_USER_DATA_LIMIT // 2
