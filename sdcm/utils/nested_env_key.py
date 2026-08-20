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

"""
Standalone helpers for splitting an SCT_* env var name on its nested-key separator.

Deliberately dependency-free (stdlib only): kept out of sdcm.sct_config so that
callers who can't afford sct_config's heavyweight transitive imports (boto3,
google-cloud, azure-mgmt, ...) -- e.g. sct_sizing.py's CLI, which needs to stay
fast to start -- can still reuse this logic instead of duplicating it.
"""

# Separators accepted between an SCT_<FIELD> env var and a nested sub-key, e.g.
# SCT_STRESS_IMAGE.ycsb=... or SCT_STRESS_IMAGE__ycsb=... . "__" exists because
# dots are invalid in bash variable names, so it is the only form usable with
# plain `export`.
NESTED_ENV_SEPARATORS = (".", "__")


def nested_env_subkey(env_key: str, prefix: str, sep: str) -> str | None:
    """Return the nested sub-key of *env_key* under `prefix + sep`, or None if it doesn't nest under it.

    Anchored on `prefix + sep` (never a bare global split), so that, e.g., a
    prefix "SCT_SIZING_DB" never wrongly claims "SCT_SIZING_DB_ORACLE__vcpu",
    and a single underscore inside the prefix (like SCT_INSTANCE_TYPE_DB) is
    never mistaken for a separator.

    Only the first sub-key level is captured: e.g. under prefix
    "SCT_STRESS_IMAGE" and sep "__", env_key "SCT_STRESS_IMAGE__foo__bar"
    resolves to sub-key "foo", silently dropping the trailing "__bar".
    """
    full_prefix = prefix + sep
    if env_key.startswith(full_prefix):
        return env_key[len(full_prefix) :].split(sep, maxsplit=1)[0]
    return None


def split_earliest_nested_key(key: str, separators: tuple[str, ...] = NESTED_ENV_SEPARATORS) -> tuple[str, str] | None:
    """Split *key* on whichever of *separators* occurs earliest in *key*.

    Unlike `nested_env_subkey`, this has no prefix to anchor on: it is meant
    for callers scanning a raw key that has already been stripped of any
    fixed prefix (e.g. the "SCT_" prefix), where the caller doesn't know the
    field name up front. Returns (before, sub_key), or None if none of
    *separators* appear in *key*.
    """
    sep_pos, sep_len = -1, 0
    for sep in separators:
        pos = key.find(sep)
        if pos != -1 and (sep_pos == -1 or pos < sep_pos):
            sep_pos, sep_len = pos, len(sep)
    if sep_pos == -1:
        return None
    return key[:sep_pos], key[sep_pos + sep_len :]
