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

"""Scylla commit SHA helpers for `<branch>:<sha>' image selectors.

Leaf module (stdlib only), so backend image lookups can import it without import cycles.
"""

import re


def scylla_sha_from_version(scylla_version: str | None) -> str:
    """Scylla commit SHA from an image's `scylla_version' tag, or "" if it has none.

    Handles the dotted form used by AWS/Azure/OCI tags ("2026.4.0~dev-0.20261006.bedcc695789c")
    and the dashed form of GCE labels ("2026-4-0-dev-0-20261006-bedcc695789c").
    """
    match = re.search(r"(?<=\d{8}[.-])[0-9a-f]+", scylla_version or "")
    return match.group() if match else ""


def sha_selector(selector: str) -> str | None:
    """The part after ':' in a branch version: None for 'latest'/'all', else the validated Scylla SHA."""
    if selector in ("latest", "all"):
        return None
    sha = selector.lower()
    if not re.fullmatch(r"[0-9a-f]{7,40}", sha):
        raise ValueError(
            f"'{selector}' is not a Scylla SHA: use 'latest', 'all' or 7-40 hex chars of the Scylla commit"
        )
    return sha


def scylla_version_matches_sha(scylla_version: str | None, sha: str) -> bool:
    """Match a short or full SHA (from `sha_selector') against an image's `scylla_version' tag."""
    image_sha = scylla_sha_from_version(scylla_version)
    return bool(image_sha) and (image_sha.startswith(sha) or sha.startswith(image_sha))
