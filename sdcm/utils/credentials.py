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

"""Decide which credentials a run needs, and set them up at sct.py start-up.

Local backends (Docker, kind) run without any AWS credentials; everything else talks to a cloud
and keeps the eager Okta + shared SSH key flow.
"""

import logging
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError

from sdcm.keystore import KeyStore
from sdcm.utils.aws_okta import try_auth_with_okta

LOGGER = logging.getLogger(__name__)

# Backends that run entirely on the local host. kind on a cloud VM (k8s-local-kind-aws/gce) is not one.
BACKENDS_WITHOUT_AWS = frozenset({"docker", "k8s-local-kind"})

SHARED_SSH_KEY_NAME = "scylla_test_id_ed25519"
SSH_KEY_DIR = Path("~/.ssh")


def backend_needs_aws(backend: str | None) -> bool:
    """Whether a run on ``backend`` needs AWS. An unknown backend (``None``) is treated as needing it."""
    return backend not in BACKENDS_WITHOUT_AWS


def backend_needs_ssh_key(backend: str | None) -> bool:
    """Whether a run on ``backend`` SSHes with ``user_credentials_path``.

    Docker nodes are reached with ``docker exec`` and kind pods with ``kubectl``, so local backends don't.
    """
    return backend_needs_aws(backend)


def aws_credentials_available() -> bool:
    """Whether boto3's credential chain resolves to something.

    Makes no AWS API call, so it says nothing about the credentials being valid or for which account.
    """
    try:
        return boto3.Session().get_credentials() is not None
    except BotoCoreError:
        return False


def sync_shared_ssh_key() -> None:
    KeyStore().sync(keys=[SHARED_SSH_KEY_NAME], local_path=SSH_KEY_DIR.expanduser(), permissions=0o600)


def prepare_cli_credentials(backend: str | None) -> None:
    """Set up credentials for an sct.py command that runs against ``backend``.

    A backend that needs AWS gets Okta auth and the shared SSH key, and fails if either fails.
    A local backend uses the same flow when AWS credentials are around (so nothing changes for
    Jenkins and staff), but carries on when they are missing or rejected: it never SSHes with the
    shared key, so it doesn't need it (see ``backend_needs_ssh_key``).
    """
    if backend_needs_aws(backend):
        try_auth_with_okta()
        sync_shared_ssh_key()
        return

    if aws_credentials_available():
        try:
            try_auth_with_okta()
            sync_shared_ssh_key()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Backend %r doesn't need AWS, continuing without the shared SSH key: %s", backend, exc)
    else:
        LOGGER.info("No AWS credentials found; backend %r doesn't need them", backend)
