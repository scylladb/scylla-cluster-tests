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

from sdcm.sct_config import available_backends
from sdcm.utils.credentials import aws_credentials_available, backend_needs_aws


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
