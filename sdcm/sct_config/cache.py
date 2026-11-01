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
Cache of a loaded ``SCTConfiguration``, shared between the hydra invocations of one pipeline build.

``hydra load-config --export-cache <file>`` does a full load (yaml + env merge, image and repository
resolution, instance sizing) and writes the result here.  Every later ``SCTConfiguration()`` in the
same build finds the file through ``SCT_CONFIG_CACHE_FILE`` and rebuilds the model from it, which
costs milliseconds instead of the seconds of cloud lookups a full load spends.

The file is self-contained: every field whose value differs from its declared default (the fields
excluded from ``model_dump`` included) and the private attributes the loader leaves behind.  A model
restored from it is indistinguishable from the one that was written.  Restoring runs the per-field
Pydantic validation, so nested models come back as models, and skips everything the full load does
beyond that: merging, resolution and the cross-field checks the values already passed.
"""

import json
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from sdcm import sct_abs_path

if TYPE_CHECKING:
    from sdcm.sct_config.config import SCTConfiguration

LOGGER = logging.getLogger(__name__)

#: Names the cache file, relative to the SCT root or absolute.  Set build-wide by the pipelines.
CACHE_FILE_ENV = "SCT_CONFIG_CACHE_FILE"

#: Loader state kept outside the Pydantic fields; restored verbatim so region fallback and the
#: explicit-parameter bookkeeping see what the full load produced.
PRIVATE_ATTRS = ("_explicitly_set_params", "_ami_params_snapshot")


def config_cache_path() -> Path | None:
    """Return the cache file the environment names, resolved against the SCT root, or None when unset."""
    value = os.environ.get(CACHE_FILE_ENV)
    if not value:
        return None
    return Path(sct_abs_path(value))


@contextmanager
def cold_load() -> Iterator[None]:
    """Hide the cache from ``SCTConfiguration()`` for the duration of the block.

    Used by the producer: it runs with ``SCT_CONFIG_CACHE_FILE`` already exported build-wide and must
    do the full load that fills the file.
    """
    saved = os.environ.pop(CACHE_FILE_ENV, None)
    try:
        yield
    finally:
        if saved is not None:
            os.environ[CACHE_FILE_ENV] = saved


def cache_payload(config: "SCTConfiguration") -> dict:
    """Return the JSON-ready state of *config*: non-default fields, private loader attributes, fields set.

    Field values go through the model's own serializer, so nested models and enums become JSON.
    The serializer honours ``exclude=True``, so the excluded fields are added from the instance; they
    hold plain lists, dicts and scalars.
    """
    model = type(config)
    # __pydantic_serializer__ and __pydantic_validator__ (below) are pydantic-core internals, checked
    # against pydantic 2.13 (pydantic-core 2.46); the round-trip unit tests catch a release that changes them.
    serialized = model.__pydantic_serializer__.to_python(config, mode="json")
    fields = {}
    for name, field in model.model_fields.items():
        value = getattr(config, name)
        if value == field.get_default(call_default_factory=True):
            continue
        fields[name] = serialized[name] if name in serialized else value
    private = {name: getattr(config, name, None) for name in PRIVATE_ATTRS}
    private["_explicitly_set_params"] = sorted(private["_explicitly_set_params"] or ())
    return {"fields": fields, "private": private, "fields_set": sorted(config.model_fields_set)}


def write_config_cache(config: "SCTConfiguration", path: str | os.PathLike) -> Path:
    """Write the cache file for *config* at *path* (relative to the SCT root or absolute) and return it."""
    target = Path(sct_abs_path(str(path)))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(cache_payload(config), indent=1), encoding="utf-8")
    LOGGER.info("Wrote the configuration cache to %s", target)
    return target


def read_config_cache(path: Path) -> dict | None:
    """Return the payload stored at *path*, or None with a warning when the file is absent."""
    if not path.is_file():
        LOGGER.warning("%s names %s but the file is absent; loading the configuration in full", CACHE_FILE_ENV, path)
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def restore_config_from_cache(config: "SCTConfiguration", payload: dict) -> None:
    """Fill the freshly allocated *config* from *payload*, the way ``BaseModel.__init__`` fills a model.

    Runs the per-field validation on the stored values, which turns nested dicts back into their
    models, then puts back the private loader attributes and the set of explicitly set fields.
    """
    model = type(config)
    model.__pydantic_validator__.validate_python(payload["fields"], self_instance=config)
    for name, stored in payload["private"].items():
        object.__setattr__(config, name, set(stored) if name == "_explicitly_set_params" else stored)
    object.__setattr__(config, "__pydantic_fields_set__", set(payload["fields_set"]))
