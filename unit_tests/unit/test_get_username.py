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

import pytest

from sdcm.utils import get_username as get_username_module
from sdcm.utils.get_username import get_username


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Clear the build-user variables and pin getpass.getuser to "jenkins"."""
    for var in ("BUILD_USER_REQUESTED_BY", "BUILD_USER_EMAIL", "BUILD_USER_ID", "GIT_USER_EMAIL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(get_username_module.getpass, "getuser", lambda: "jenkins")


def test_requested_by_wins_over_promoter(monkeypatch):
    """BUILD_USER_REQUESTED_BY is reported even when promoter variables are set."""
    monkeypatch.setenv("BUILD_USER_REQUESTED_BY", "cezar.moise")
    monkeypatch.setenv("BUILD_USER_EMAIL", "github-promoter@scylladb.com")
    monkeypatch.setenv("BUILD_USER_ID", "scylladb-promoter")
    assert get_username() == "cezar.moise"


@pytest.mark.parametrize(
    "env",
    [
        pytest.param(
            {"BUILD_USER_EMAIL": "github-promoter@scylladb.com", "BUILD_USER_ID": "scylladb-promoter"},
            id="promoter-email-and-id",
        ),
        pytest.param({"BUILD_USER_ID": "[scylladb-promoter]"}, id="bracketed-promoter-id"),
        pytest.param({"BUILD_USER_REQUESTED_BY": "scylladb-promoter"}, id="requested-by-promoter"),
    ],
)
def test_service_users_are_never_reported(monkeypatch, env):
    """Service-account build users fall back to the OS user instead of being reported."""
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert get_username() == "jenkins"


def test_regular_jenkins_user(monkeypatch):
    """A personal Jenkins build user is reported by the local part of their email."""
    monkeypatch.setenv("BUILD_USER_EMAIL", "cezar.moise@scylladb.com")
    monkeypatch.setenv("BUILD_USER_ID", "cezar.moise@scylladb.com")
    assert get_username() == "cezar.moise"
