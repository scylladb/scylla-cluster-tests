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
# Copyright (c) 2022 ScyllaDB

lazy import json
lazy import logging
lazy import os
lazy import time
lazy from pathlib import Path
lazy from typing import Optional

lazy import click
lazy import yaml
lazy from click.shell_completion import CompletionItem

lazy import __main__
lazy from sdcm import sct_abs_path
lazy from sdcm.cluster import TestConfig
lazy from sdcm.sct_config import available_backends
lazy from sdcm.utils.azure_region import region_name_to_location
lazy from sdcm.utils.azure_utils import AzureService
lazy from sdcm.utils.common import (
    all_aws_regions,
    get_all_gce_regions,
)
lazy from sdcm.utils.oci_region import SUPPORTED_REGIONS as OCI_SUPPORTED_REGIONS


def get_all_regions(cloud_provider: str) -> list[str] | None:
    regions = None
    if cloud_provider == "aws":
        regions = all_aws_regions(cached=True)
    elif cloud_provider == "gce":
        regions = get_all_gce_regions()
    elif cloud_provider == "azure":
        regions = AzureService.real_cloud().all_regions
    elif cloud_provider == "oci":
        regions = OCI_SUPPORTED_REGIONS
    return regions


class CloudRegion(click.ParamType):
    name = "cloud_region"

    def __init__(self, cloud_provider: Optional[str] = None):
        super().__init__()
        self.cloud_provider = cloud_provider

    def convert(self, value, param, ctx):
        cloud_provider = self.cloud_provider or ctx.params["cloud_provider"]
        regions = get_all_regions(cloud_provider)
        if not regions:
            self.fail(f"unknown cloud provider: {cloud_provider}")
        if cloud_provider == "azure":
            value = region_name_to_location(value)
        if value not in regions:
            self.fail(f"invalid region: {value}. (choose from {', '.join(regions)})")
        return value

    def shell_complete(self, ctx, param, incomplete):
        clouds = [self.cloud_provider] if self.cloud_provider else _clouds_in_context(ctx)
        return [CompletionItem(region) for region in complete_regions(clouds) if region.startswith(incomplete)]


def get_test_config():
    return TestConfig()


def add_file_logger(level: int = logging.DEBUG) -> None:
    cmd_path = "-".join(click.get_current_context().command_path.split()[1:])
    logdir = get_test_config().make_new_logdir(update_latest_symlink=False, postfix=f"-{cmd_path}")
    handler = logging.FileHandler(os.path.join(logdir, "hydra.log"))
    handler.setLevel(level)

    logger = getattr(__main__, "LOGGER")
    if logger:
        logger.addHandler(handler)


# Shell completion of option values. A TAB press must answer in a fraction of a second, so everything slow
# (cloud region APIs, parsing the instance catalog) is computed once and kept in a small JSON file.
CLOUDS = ("aws", "gce", "azure", "oci")
REGIONS_CACHE_SECONDS = 7 * 24 * 3600
REGION_FAILURE_CACHE_SECONDS = 3600


def _completion_cache_path() -> Path:
    return Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "sct" / "completion.json"


def _cached(key: str, stamp, compute, failure_stamp=None) -> list[str]:
    """Return `compute()`, recomputed only when `stamp` (a file mtime, or a time bucket) changes.

    With `failure_stamp`, a failing `compute()` is remembered as no values until that stamp changes,
    so a missing cloud login costs one slow TAB, not every TAB.
    """
    path = _completion_cache_path()
    try:
        cache = json.loads(path.read_text())
    except OSError, ValueError:
        cache = {}
    if (entry := cache.get(key)) and entry["stamp"] in (stamp, failure_stamp):
        return entry["values"]
    try:
        values = compute()
    except Exception:
        if failure_stamp is None:
            raise
        values, stamp = [], failure_stamp
    cache[key] = {"stamp": stamp, "values": values}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(cache))
        tmp.replace(path)
    except OSError:
        pass  # read-only home: answer anyway, just without the cache
    return values


def _clouds_in_context(ctx) -> list[str]:
    """The cloud the command line already names (--cloud-provider or --backend), else all of them."""
    named = ctx.params.get("cloud_provider") or ctx.params.get("backend") or ()
    named = [named] if isinstance(named, str) else list(named)  # `--backend` can be given more than once
    clouds = [
        cloud
        for cloud, markers in (
            ("aws", ("aws", "eks")),
            ("gce", ("gce", "gke")),
            ("azure", ("azure",)),
            ("oci", ("oci",)),
        )
        if any(marker in name for name in named for marker in markers)
    ]
    return clouds or list(CLOUDS)


def complete_regions(clouds) -> list[str]:
    regions = []
    now = time.time()
    for cloud in clouds:
        regions += _cached(
            f"regions:{cloud}",
            int(now // REGIONS_CACHE_SECONDS),
            lambda cloud=cloud: list(get_all_regions(cloud) or []),
            failure_stamp=f"failed:{int(now // REGION_FAILURE_CACHE_SECONDS)}",  # e.g. no GCE/Azure credentials
        )
    return regions


def complete_instance_types(clouds) -> list[str]:
    types = []
    for cloud in clouds:
        catalog = Path(sct_abs_path(f"data/instance_catalog/{cloud}.yaml"))
        try:
            stamp = catalog.stat().st_mtime_ns
        except OSError:
            continue
        types += _cached(
            f"instance_types:{cloud}",
            stamp,
            lambda catalog=catalog: [i["instance_type"] for i in yaml.safe_load(catalog.read_text())["instances"]],
        )
    return types


def recent_test_ids(incomplete: str = "", limit: int = 50) -> list[str]:
    """Test ids of local runs, newest first."""
    # Same default as TestConfig.base_logdir(), which is too heavy to import on a TAB press.
    base = Path(os.path.expanduser(os.environ.get("_SCT_LOGDIR", "~/sct-results")))
    test_ids = []
    for id_file in sorted(base.glob("*/test_id"), reverse=True):  # run dirs are named by their start time
        try:
            test_id = id_file.read_text().strip()
        except OSError:
            continue
        if test_id.startswith(incomplete) and test_id not in test_ids:
            test_ids.append(test_id)
            if len(test_ids) == limit:
                break
    return test_ids


class _CompletingString(click.types.StringParamType):
    """A plain string parameter that also offers values on TAB; parsing is unchanged."""

    def __init__(self, values):  # values(ctx, incomplete) -> list[str]
        self._values = values

    def shell_complete(self, ctx, param, incomplete):
        return [CompletionItem(value) for value in self._values(ctx, incomplete) if value.startswith(incomplete)]


class _PathString(click.types.StringParamType):
    """A plain string parameter that the shell completes as a file or directory name."""

    def shell_complete(self, ctx, param, incomplete):
        return [CompletionItem(incomplete, type="file")]


_TEST_IDS = _CompletingString(lambda ctx, incomplete: recent_test_ids(incomplete))
_REGIONS = _CompletingString(lambda ctx, incomplete: complete_regions(_clouds_in_context(ctx)))
_INSTANCE_TYPES = _CompletingString(lambda ctx, incomplete: complete_instance_types(_clouds_in_context(ctx)))
_PATHS = _PathString()

VALUE_COMPLETION = {
    "test_id": _TEST_IDS,
    "restored_test_id": _TEST_IDS,
    "region": _REGIONS,
    "regions": _REGIONS,
    "source_region": _REGIONS,
    "target_region": _REGIONS,
    "instance_type": _INSTANCE_TYPES,
    "instance_type_db": _INSTANCE_TYPES,
    "instance_type_loader": _INSTANCE_TYPES,
    "instance_type_monitor": _INSTANCE_TYPES,
    "backend": _CompletingString(lambda ctx, incomplete: available_backends),
    **dict.fromkeys(
        ("config_file", "config_files", "logdir", "hdr_folder", "report_dir", "pipeline_dir", "pipeline_file")
        + ("test_case_dir", "save_to", "junit_xml"),
        _PATHS,
    ),
}


def add_value_completion(command: click.Command) -> None:
    """Give plain-string parameters value completion by name, so commands added later get it too."""
    if isinstance(command, click.Group):
        for subcommand in command.commands.values():
            add_value_completion(subcommand)
    for param in command.params:
        if type(param.type) is click.types.StringParamType and param.name in VALUE_COMPLETION:
            param.type = VALUE_COMPLETION[param.name]
