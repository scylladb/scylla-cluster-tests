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

"""Decide which credentials a run needs.

Local backends (Docker, kind) run without any AWS credentials; everything else talks to a cloud.
"""

import boto3
from botocore.exceptions import BotoCoreError

# Backends that run entirely on the local host. kind on a cloud VM (k8s-local-kind-aws/gce) is not one.
BACKENDS_WITHOUT_AWS = frozenset({"docker", "k8s-local-kind"})


def backend_needs_aws(backend: str | None) -> bool:
    """Whether a run on ``backend`` needs AWS. An unknown backend (``None``) is treated as needing it."""
    return backend not in BACKENDS_WITHOUT_AWS


def aws_credentials_available() -> bool:
    """Whether boto3's credential chain resolves to something.

    Makes no AWS API call, so it says nothing about the credentials being valid or for which account.
    """
    try:
        return boto3.Session().get_credentials() is not None
    except BotoCoreError:
        return False
