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

from unittest.mock import patch

import paramiko
import pytest
from click.testing import CliRunner

import sct
from sdcm.sct_config import available_backends
from sdcm.utils import credentials
from sdcm.utils.credentials import (
    aws_credentials_available,
    backend_needs_aws,
    ensure_local_ssh_key,
    prepare_cli_credentials,
)


@pytest.mark.parametrize("backend", ["docker", "k8s-local-kind"])
def test_local_backends_do_not_need_aws(backend):
    assert not backend_needs_aws(backend)


@pytest.mark.parametrize("backend", sorted(set(available_backends) - {"docker", "k8s-local-kind"}) + [None])
def test_cloud_and_unknown_backends_need_aws(backend):
    assert backend_needs_aws(backend)


@pytest.fixture
def no_aws_credentials(monkeypatch, tmp_path):
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "missing-credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")


@pytest.mark.usefixtures("no_aws_credentials")
def test_aws_credentials_not_available():
    assert not aws_credentials_available()


@pytest.mark.usefixtures("no_aws_credentials")
def test_aws_credentials_available_from_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    assert aws_credentials_available()


def test_ensure_local_ssh_key_generates_loadable_private_key(tmp_path):
    key_path = tmp_path / "ssh" / "scylla_test_id_ed25519"

    assert ensure_local_ssh_key(key_path)

    assert key_path.stat().st_mode & 0o777 == 0o600
    assert key_path.parent.stat().st_mode & 0o777 == 0o700
    assert paramiko.Ed25519Key.from_private_key_file(str(key_path)).get_base64()


def test_ensure_local_ssh_key_never_touches_existing_file(tmp_path):
    key_path = tmp_path / "scylla_test_id_ed25519"
    key_path.write_text("shared key")

    assert not ensure_local_ssh_key(key_path)
    assert key_path.read_text() == "shared key"


@pytest.fixture
def start_up_steps():
    with (
        patch.object(credentials, "try_auth_with_okta") as okta,
        patch.object(credentials, "sync_shared_ssh_key") as sync,
        patch.object(credentials, "ensure_local_ssh_key") as local_key,
        patch.object(credentials, "aws_credentials_available") as available,
    ):
        yield okta, sync, local_key, available


def test_cloud_backend_keeps_eager_flow(start_up_steps):
    okta, sync, local_key, _ = start_up_steps

    prepare_cli_credentials("aws")

    okta.assert_called_once()
    sync.assert_called_once()
    local_key.assert_not_called()


def test_cloud_backend_fails_without_okta(start_up_steps):
    okta, sync, _, _ = start_up_steps
    okta.side_effect = ValueError("OKTA isn't configured")

    with pytest.raises(ValueError, match="OKTA"):
        prepare_cli_credentials("gce")
    sync.assert_not_called()


def test_local_backend_without_credentials_skips_okta(start_up_steps):
    okta, sync, local_key, available = start_up_steps
    available.return_value = False

    prepare_cli_credentials("docker")

    okta.assert_not_called()
    sync.assert_not_called()
    local_key.assert_called_once()


def test_local_backend_with_credentials_uses_shared_key(start_up_steps):
    okta, sync, local_key, available = start_up_steps
    available.return_value = True

    prepare_cli_credentials("k8s-local-kind")

    okta.assert_called_once()
    sync.assert_called_once()
    local_key.assert_called_once()


def test_local_backend_with_rejected_credentials_carries_on(start_up_steps):
    okta, sync, local_key, available = start_up_steps
    available.return_value = True
    okta.side_effect = ValueError("not the SCT account")

    prepare_cli_credentials("docker")

    sync.assert_not_called()
    local_key.assert_called_once()


@pytest.mark.parametrize(
    "args,expected_backend",
    [
        (["conf", "-b", "docker"], "docker"),
        (["conf", "-b", "k8s-local-kind"], "k8s-local-kind"),
        (["conf"], "aws"),
    ],
)
@patch("sct.docker_hub_login")
@patch("sct.add_file_logger")
@patch("sct.SCTConfiguration")
@patch("sct.prepare_cli_credentials")
def test_conf_prepares_credentials_once_for_its_backend(prepare, _config, _logger, _hub, args, expected_backend):
    result = CliRunner().invoke(sct.cli, args)

    assert result.exit_code == 0, result.output
    prepare.assert_called_once_with(backend=expected_backend)


@patch("sct.docker_hub_login")
@patch("sct.prepare_cli_credentials")
def test_commands_without_credentials_skip_set_up(prepare, hub):
    result = CliRunner().invoke(sct.cli, ["conf-docs", "--output-format", "yaml"])

    assert result.exit_code == 0, result.output
    prepare.assert_not_called()
    hub.assert_not_called()
