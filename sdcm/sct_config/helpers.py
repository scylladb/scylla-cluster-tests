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
# Copyright (c) 2020 ScyllaDB

"""
Helper functions for the SCT configuration: appendable-option merging, environment sub-key
parsing, region counting and docker-image defaults loading.
"""

import copy
import json
import os
import pathlib
from functools import lru_cache

import anyconfig
from pydantic import BaseModel

from sdcm import sct_abs_path
from sdcm.sct_config.types import is_ignored_field
from sdcm.utils.nested_env_key import nested_env_subkey


def _nested_env_subkey(env_key: str, field_env: str, sep: str) -> str | None:
    """Return the nested sub-key of *env_key* for *field_env* under separator *sep*, or None if it doesn't nest under it.

    Delegates the anchored splitting itself to `nested_env_subkey` (see
    sdcm.utils.nested_env_key) so that, e.g., SCT_SIZING_DB_ORACLE__vcpu is
    never wrongly claimed by field `sizing_db`, and a single underscore inside
    a field name (like SCT_INSTANCE_TYPE_DB) is never mistaken for a
    separator.

    *sep* is required: the caller is always iterating NESTED_ENV_SEPARATORS itself
    (see `_load_environment_variables`, which needs "." matches applied before
    "__" ones for deterministic last-write-wins precedence), so there is no
    "check every separator" convenience mode here to keep in sync with that order.

    The "__" form is lower-cased (bash-exportable env vars are conventionally
    upper-case, but sub-keys like operation names are lower-case); the "."
    form keeps its existing case-preserving behaviour for backwards
    compatibility. This means the two forms are NOT drop-in equivalents for
    non-lowercase sub-keys consumed by case-sensitive lookups: prefer
    uppercase sub-keys with "__" (they'll be lowered, matching the common
    convention) rather than relying on ".", whose case is passed through
    verbatim.

    Only the first sub-key level is supported: a multi-level key like
    SCT_STRESS_IMAGE__foo__bar resolves to sub-key "foo", silently dropping the
    trailing "__bar" (same pre-existing limitation as the dot notation, e.g.
    SCT_STRESS_IMAGE.foo.bar also resolves to "foo" -- not a regression from
    adding "__" support, just previously undocumented).
    """
    sub_key = nested_env_subkey(env_key, field_env, sep)
    if sub_key is None:
        return None
    return sub_key.lower() if sep == "__" else sub_key


def is_config_option_appendable(option_name: str, model: type[BaseModel]) -> bool:
    for field_name, field in model.model_fields.items():
        if is_ignored_field(field):
            continue
        if field_name == option_name:
            break
    else:
        raise ValueError(f"Option {option_name} not found in {model.__name__} fields")

    # type: ignore[union-attr]
    return field.json_schema_extra and field.json_schema_extra.get("appendable", False)


def merge_dicts_append_strings(d1, d2, model: type[BaseModel]):
    """
    merge two dictionaries, while having option
    to append string if the value starts with '++'
    and append list if first item is '++'
    """

    for key, value in copy.deepcopy(d2).items():
        if isinstance(value, str) and value.startswith("++"):
            assert is_config_option_appendable(key, model), f"Option {key} is not appendable"
            if key not in d1 or d1[key] is None:
                d1[key] = ""
            d1[key] += value[2:]
            del d2[key]
        if isinstance(value, list) and value and isinstance(value[0], str) and value[0].startswith("++"):
            assert is_config_option_appendable(key, model), f"Option {key} is not appendable"
            if key not in d1 or d1[key] is None:
                d1[key] = []
            d1[key].extend(value[1:])
            del d2[key]

    anyconfig.merge(d1, d2, ac_merge=anyconfig.MS_DICTS)


def count_regions(region_string: str) -> int:
    """Count the number of regions in a region string.

    Handles JSON arrays ('["us-east-1","eu-west-1"]'), space-separated
    strings ('us-east-1 eu-west-1'), and single region strings.
    """
    if not region_string:
        return 1
    try:
        regions = json.loads(region_string.replace("'", '"'))
        if isinstance(regions, list):
            return len(regions)
    except json.JSONDecodeError, ValueError:
        # Not a JSON array — fall through to treat as a plain string
        pass
    if " " in region_string:
        return len(region_string.split())
    return 1


@lru_cache(maxsize=1)
def _load_docker_images_defaults_cached():
    """Load and cache docker image defaults from YAML files.

    Cached at module level so repeated SCTConfiguration() instantiations
    (e.g. in lint-pipelines workers) don't re-read and re-parse the same
    YAML files from disk each time.
    """
    docker_images_dir = pathlib.Path(sct_abs_path("defaults/docker_images"))
    if docker_images_dir.is_dir():
        yaml_files = []
        for root, _, files in os.walk(docker_images_dir):
            yaml_files.extend([os.path.join(root, f) for f in files if f.endswith(".yaml")])
        if yaml_files:
            docker_images_defaults = anyconfig.load(yaml_files)
            return {key: value.get("image") for key, value in docker_images_defaults.items()}
    return None


#: First Scylla release whose Docker image entrypoint accepts the `--dc`/`--rack` arguments.
DOCKER_RACK_ARG_MIN_VERSION = "2026.1.0-dev"


def simulated_racks_enabled(params) -> bool:
    """True when simulated racks actually take effect.

    Racks need both more than one rack and more than one DB node to spread over.  A single-node
    cluster stays in one rack whatever `simulated_racks` says: the snitch auto-resolution leaves
    `endpoint_snitch` alone, so nothing ever reads the rack.  It must therefore not pay any of the
    cost of racks either -- in particular the Scylla >= 2026.1 requirement of the Docker
    `--dc`/`--rack` entrypoint arguments, which older images reject outright.

    Kept in one place because three call sites have to agree on it: the Docker version check and
    the snitch auto-resolution in `SCTConfiguration.__init__`, and the `--dc`/`--rack` injection in
    `NodeContainerMixin.node_container_run_args`.  `n_db_nodes` is the configured topology and is
    never mutated at runtime, so growing a cluster does not change the answer mid-test.
    """
    return (params.get("simulated_racks") or 0) > 1 and sum(params.get("n_db_nodes") or []) > 1
