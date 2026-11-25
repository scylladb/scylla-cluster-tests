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

import pytest
from click.testing import CliRunner

import sct


@pytest.fixture
def okta():
    """Patch Okta auth, the keystore and the docker login; yield the Okta auth mock."""
    with (
        patch("sct.try_auth_with_okta") as auth,
        patch("sct.KeyStore"),
        patch("sct.docker_hub_login"),
    ):
        yield auth


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["list-resources", "--help"], id="command"),
        pytest.param(["sizing", "preview", "--help"], id="nested-group"),
    ],
)
def test_help_skips_okta(okta, args):
    """--help on a command or nested group prints usage without authenticating with Okta."""
    result = CliRunner().invoke(sct.cli, args)

    assert result.exit_code == 0, result.output
    assert "Usage:" in result.output
    okta.assert_not_called()


def test_command_still_authenticates(okta):
    """A real command invocation authenticates with Okta before its own option parsing fails."""
    result = CliRunner().invoke(sct.cli, ["list-resources", "--no-such-option"])

    assert result.exit_code == 2  # the subcommand rejects the option only after the group callback ran
    okta.assert_called_once_with()


def test_help_after_double_dash_is_a_value_not_a_help_request(okta):
    """--help after -- is passed to the remote command as a value, and Okta still authenticates."""
    with patch("sct_ssh.ssh_run_cmd") as run_command:
        CliRunner().invoke(sct.cli, ["ssh-cmd", "-t", "abc", "--", "nodetool", "--help"])

    assert run_command.call_args.args[1] == "nodetool --help"
    okta.assert_called_once_with()
