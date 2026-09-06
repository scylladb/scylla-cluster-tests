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
Handling Scylla-cluster-test configuration loading
"""

import os
import random
import re
import json
import logging
import getpass
import pathlib
import tempfile
from textwrap import dedent

import yaml
from typing import List, Union, Set, Literal, get_origin, get_args, ClassVar
from functools import cached_property

import anyconfig
from argus.client.sct.types import Package
from packaging import version
from pydantic import Field, ConfigDict, fields as pydantic_fields
from typing_extensions import Annotated
from pydantic.functional_validators import BeforeValidator
from sdcm import sct_abs_path
import sdcm.provision.azure.utils as azure_utils
from sdcm.cloud_api_client import ScyllaCloudAPIClient, CloudProviderType
from sdcm.keystore import KeyStore
from sdcm.utils.cloud_api_utils import (
    get_cloud_rest_credentials_from_file,
    expand_availability_zones,
    parse_availability_zones,
)
from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.capacity_errors import RegionAMINotFoundError
from sdcm.provision.aws.dedicated_host import SCTDedicatedHosts
from sdcm.provision.common.oracle import ORACLE_IMAGE_PARAMS, ORACLE_USER_PREFIX_SUFFIX
from sdcm.utils.aws_utils import get_arch_from_instance_type, aws_check_instance_type_supported
from sdcm.utils.common import (
    ami_built_by_scylla,
    get_ami_tags,
    get_branched_ami,
    get_branched_gce_images,
    get_scylla_ami_versions,
    get_scylla_gce_images_versions,
    convert_name_to_ami_if_needed,
    find_equivalent_ami,
    get_sct_root_path,
    get_vector_store_ami_versions,
)
from sdcm.utils import oci_utils
from sdcm.utils.operations_thread import ConfigParams
from sdcm.utils.version_utils import (
    ARGUS_VERSION_RE,
    get_branch_version,
    get_branch_version_for_multiple_repositories,
    get_scylla_docker_repo_from_version,
    resolve_latest_repo_symlink,
    get_specific_tag_of_docker_image,
    find_scylla_repo,
    is_enterprise,
    ComparableScyllaVersion,
    latest_unified_package,
)
from sdcm.sct_events.base import add_severity_limit_rules, print_critical_events
from sdcm.utils.gce_utils import (
    get_gce_image_tags,
    get_gce_compute_machine_types_client,
    get_gce_compute_regions_client,
    gce_check_if_machine_type_supported,
)
from sdcm.utils.azure_utils import (
    azure_check_instance_type_available,
)
from sdcm.utils.cloud_api_utils import MIN_SCYLLA_VERSION_FOR_VS
from sdcm.remote import LOCALRUNNER, shell_script_cmd
from sdcm.utils.curl import curl_with_retry
from sdcm.test_config import TestConfig
from sdcm.utils.version_utils import parse_scylla_version_tag
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog
from sdcm.utils.cloud_catalog.instance_matcher import ARCH_ALIASES, NoMatchingInstanceError, select_instance
from sdcm.utils.nested_env_key import NESTED_ENV_SEPARATORS
from sdcm.sct_config.defaults import (
    AMI_ID_PARAMS,
    AWS_SUPPORTED_REGIONS,
    BACKEND_REQUIRED_PARAMS,
    DEFAULTS_CONFIG_FILES,
    PER_PROVIDER_MULTI_REGION_PARAMS,
    REQUIRED_PARAMS,
    STRESS_CMD_PARAMS,
    XCLOUD_PER_PROVIDER_REQUIRED_PARAMS,
    available_backends,
)
from sdcm.sct_config.mixins import CONFIG_GROUPS
from sdcm.sct_config.helpers import (
    DOCKER_RACK_ARG_MIN_VERSION,
    _load_docker_images_defaults_cached,
    _nested_env_subkey,
    is_config_option_appendable,
    merge_dicts_append_strings,
    simulated_racks_enabled,
)
from sdcm.sct_config.types import (
    IgnoredType,
    InputType,
    _check_file_exists,
    is_ignored_field,
)

_SIZING_RESOLUTION_CACHE: dict[tuple, tuple[str, str]] = {}

LOGGER = logging.getLogger("sdcm.sct_config")

_ARCH_IMAGE_MARKERS: dict[str, dict[str, str]] = {
    "{arch}": {"x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"},
    "{arch_sku}": {"x86_64": "server", "arm64": "server-arm64", "aarch64": "server-arm64"},
}


_LOADER_IMAGE_PARAMS: dict[str, tuple[str, str, str]] = {
    "aws": ("ami_id_loader", "instance_type_loader", "region_name"),
    "azure": ("azure_image_loader", "azure_instance_type_loader", "azure_region_name"),
    "gce": ("gce_image_loader", "gce_instance_type_loader", "gce_datacenter"),
    "oci": ("oci_image_loader", "oci_instance_type_loader", "oci_region_name"),
}

# every backend that inherits one of the cloud defaults files above, so a backend
# such as k8s-eks resolves the marker its inherited ami_id_loader carries
_BACKEND_TO_IMAGE_CLOUD: dict[str, str] = {
    "aws": "aws",
    "aws-siren": "aws",
    "k8s-eks": "aws",
    "k8s-local-kind-aws": "aws",
    "gce": "gce",
    "gce-siren": "gce",
    "k8s-gke": "gce",
    "azure": "azure",
    "oci": "oci",
}

_ARM_INSTANCE_TYPE_PATTERNS: dict[str, tuple[str, ...]] = {
    "aws": (r"^\w+\d+g[a-z]*\.", r"^a1\."),
    "gce": (r"^(t2a|c4a|n4a|x4a)-",),
    "azure": (r"^Standard_\w*?\d+p[a-z]*_v\d+$",),
    "oci": (r"^(BM|VM)\.Standard\.A\d+\.", r"^A\d+\.Flex"),
}


def is_arm_instance_type(cloud: str, instance_type: str) -> bool:
    return any(re.match(pattern, instance_type) for pattern in _ARM_INSTANCE_TYPE_PATTERNS.get(cloud, ()))


_SIZING_SKIP_BACKENDS = frozenset({"docker", "baremetal", "k8s-local-kind", "k8s-local-kind-aws", "k8s-local-kind-gce"})

_BACKEND_TO_CLOUD: dict[str, str] = {
    "aws": "aws",
    "aws-siren": "aws",
    "k8s-eks": "aws",
    "gce": "gce",
    "gce-siren": "gce",
    "k8s-gke": "gce",
    "azure": "azure",
    "oci": "oci",
}

_AMD64_ONLY_STRESS_TOOLS: dict[str, tuple[str, ...]] = {
    "harry": ("cassandra-harry",),
    "kcl": ("hydra-kcl",),
    "ndbench": ("ndbench",),
    "nosqlbench": ("nosqlbench",),
}

_YCSB_COMMAND_MARKER = "bin/ycsb"

_SIZING_ROLE_PARAMS: dict[str, dict[str, str]] = {
    "aws": {
        "db": "instance_type_db",
        "db_oracle": "instance_type_db_oracle",
        "zero_token": "zero_token_instance_type_db",
        "loader": "instance_type_loader",
        "monitor": "instance_type_monitor",
    },
    "gce": {
        "db": "gce_instance_type_db",
        "db_oracle": "gce_instance_type_db_oracle",
        "loader": "gce_instance_type_loader",
        "monitor": "gce_instance_type_monitor",
    },
    "azure": {
        "db": "azure_instance_type_db",
        "db_oracle": "azure_instance_type_db_oracle",
        "loader": "azure_instance_type_loader",
        "monitor": "azure_instance_type_monitor",
    },
    "oci": {
        "db": "oci_instance_type_db",
        "db_oracle": "oci_instance_type_db_oracle",
        "loader": "oci_instance_type_loader",
        "monitor": "oci_instance_type_monitor",
    },
}


def backend_to_cloud(backend: str | None, xcloud_provider: str | None = None) -> str | None:
    if backend == "xcloud":
        return xcloud_provider or None
    return _BACKEND_TO_CLOUD.get(str(backend))


def substitute_arch_markers(template: str, arch: str) -> str:
    resolved = template
    for marker, values in _ARCH_IMAGE_MARKERS.items():
        if marker in resolved:
            if arch not in values:
                raise ValueError(
                    f"Cannot resolve {marker} in {template!r} for arch {arch!r}. Known values: {sorted(values)}"
                )
            resolved = resolved.replace(marker, values[arch])
    return resolved




# SCT_KEYSTORE_* env vars this process exported itself (see the keystore
# propagation at the end of SCTConfiguration.__init__), mapped to the value we
# wrote.  The value matters, not just the name: environment variables outrank
# config files, so a value this process leaked into os.environ would otherwise
# beat a later config file asking for a different backend.  Comparing against
# the tracked value lets `_load_environment_variables` ignore our own export
# while still honouring a value the user changed behind our back.
_KEYSTORE_ENV_EXPORTED: dict[str, str] = {}


def _is_self_exported_keystore_env(env_name: str) -> bool:
    """True if os.environ[env_name] is still the value this process exported."""
    return env_name in _KEYSTORE_ENV_EXPORTED and os.environ.get(env_name) == _KEYSTORE_ENV_EXPORTED[env_name]


def _resolve_oracle_images_aws(conf, oracle_scylla_version: str) -> str:
    """Resolve oracle_scylla_version to per-region AMI IDs (space-joined) on AWS."""
    ami_list = []
    for region in conf.region_names:
        aws_arch = get_arch_from_instance_type(conf.get("instance_type_db_oracle"), region_name=region)
        try:
            if ":" in oracle_scylla_version:
                ami = get_branched_ami(scylla_version=oracle_scylla_version, region_name=region, arch=aws_arch)[0]
            else:
                ami = get_scylla_ami_versions(version=oracle_scylla_version, region_name=region, arch=aws_arch)[0]
        except Exception as ex:  # noqa: BLE001
            raise ValueError(
                f"AMIs for oracle_scylla_version='{oracle_scylla_version}' not found in {region} arch={aws_arch}"
            ) from ex
        conf.log.debug("Found AMI %s for oracle_scylla_version='%s' in %s", ami.image_id, oracle_scylla_version, region)
        ami_list.append(ami)

    return " ".join(ami.image_id for ami in ami_list)


def _resolve_oracle_images_azure(conf, oracle_scylla_version: str) -> str:
    """Resolve oracle_scylla_version to per-region Azure image IDs (space-joined)"""
    oracle_azure_images = []
    azure_arch = azure_utils.get_arch_from_azure_instance_type(conf.get("azure_instance_type_db_oracle"))
    for region in conf.get("azure_region_name"):
        try:
            if ":" in oracle_scylla_version:
                azure_image = azure_utils.get_scylla_images(
                    scylla_version=oracle_scylla_version, region_name=region, arch=azure_arch
                )[0]
            else:
                if azure_arch != azure_utils.VmArch.X86:
                    raise ValueError(
                        f"Azure released images (community gallery) do not support "
                        f"arch={azure_arch.value}. Use a branch version "
                        f"(e.g. 'master:latest') for ARM instances."
                    )
                azure_image = azure_utils.get_released_scylla_images(
                    scylla_version=oracle_scylla_version, region_name=region, arch=azure_arch
                )[0]
        except Exception as ex:  # noqa: BLE001
            raise ValueError(
                f"Azure Image for oracle_scylla_version='{oracle_scylla_version}' not found in {region}"
                f" (arch={azure_arch.value})"
            ) from ex
        conf.log.debug(
            "Found Azure Image %s for oracle_scylla_version='%s' in %s (arch=%s)",
            azure_image.name,
            oracle_scylla_version,
            region,
            azure_arch.value,
        )
        oracle_azure_images.append(azure_image)

    return " ".join(getattr(image, "id", None) or getattr(image, "unique_id", None) for image in oracle_azure_images)


def _resolve_oracle_images_gce(conf, oracle_scylla_version: str) -> str:
    """Resolve oracle_scylla_version to a global GCE image self_link"""
    try:
        if ":" in oracle_scylla_version:
            gce_image = get_branched_gce_images(scylla_version=oracle_scylla_version)[0]
        else:
            gce_image = get_scylla_gce_images_versions(version=oracle_scylla_version)[0]
    except Exception as ex:  # noqa: BLE001
        raise ValueError(f"GCE image for oracle_scylla_version='{oracle_scylla_version}' was not found") from ex

    conf.log.debug("Found GCE image %s for oracle_scylla_version='%s'", gce_image.name, oracle_scylla_version)
    return gce_image.self_link


def _resolve_oracle_images_oci(conf, oracle_scylla_version: str) -> str:
    """Resolve oracle_scylla_version to per-region image OCIDs (space-joined) on OCI"""
    oci_oracle_images = []
    oci_region_names = conf.get("oci_region_name") or []
    if not isinstance(oci_region_names, list):
        oci_region_names = [oci_region_names]

    for region in oci_region_names:
        try:
            if ":" in oracle_scylla_version:
                oci_image = oci_utils.get_scylla_images_by_branch(oracle_scylla_version, region)[0]
            else:
                oci_image = oci_utils.get_scylla_images_by_version(oracle_scylla_version, region)[0]
        except Exception as ex:  # noqa: BLE001
            raise ValueError(
                f"OCI Image for oracle_scylla_version='{oracle_scylla_version}' not found in {region}"
            ) from ex
        conf.log.debug(
            "Found OCI Image %s for oracle_scylla_version='%s' in %s", oci_image[1], oracle_scylla_version, region
        )
        oci_oracle_images.append(oci_image)

    return " ".join(image[2] for image in oci_oracle_images)


# backend -> resolver turning oracle_scylla_version into the value for
# ORACLE_IMAGE_PARAMS[backend]; backends missing here keep ignoring oracle_scylla_version
_ORACLE_IMAGE_RESOLVERS = {
    "aws": _resolve_oracle_images_aws,
    "azure": _resolve_oracle_images_azure,
    "gce": _resolve_oracle_images_gce,
    "oci": _resolve_oracle_images_oci,
}


class SCTConfiguration(*CONFIG_GROUPS):
    """The SCT configuration, assembled from the domain mixins in `sdcm.sct_config.mixins`.

    Every configuration option lives in exactly one mixin, grouped by domain (cross-cutting
    concerns, then per-backend, then per-test-type). Fields stay flat -- `config.nemesis_class_name`
    and every YAML key are unchanged; only the source is split. `CONFIG_GROUPS` fixes both the
    field order on this model and the section order in the generated documentation.

    What stays here: the assembled model's own runtime state, the per-backend lookup tables, and
    the loading, validation and doc-generation logic that spans domains.
    """

    log: ClassVar = logging.getLogger("sdcm.sct_config")

    multi_region_params: Annotated[list[str], IgnoredType] = Field(default=[], exclude=True)
    regions_data: Annotated[dict[str, dict[str, str]], IgnoredType] = Field(default={}, exclude=True)

    # computed values, user can't fill those from configuration,
    # see `update_config_based_on_version` for more information
    artifact_scylla_version: str | None = Field(default=None, exclude=True)
    is_enterprise: bool = Field(default=False, exclude=True)
    scylla_version_upgrade_target: str | None = Field(default=None, exclude=True)

    target_db_image_ids: Annotated[list[str], IgnoredType] = Field(default=[], exclude=True)

    required_params: Annotated[list, IgnoredType] = REQUIRED_PARAMS
    backend_required_params: Annotated[dict, IgnoredType] = BACKEND_REQUIRED_PARAMS
    defaults_config_files: Annotated[dict, IgnoredType] = DEFAULTS_CONFIG_FILES
    per_provider_multi_region_params: Annotated[dict, IgnoredType] = PER_PROVIDER_MULTI_REGION_PARAMS
    xcloud_per_provider_required_params: Annotated[dict, IgnoredType] = XCLOUD_PER_PROVIDER_REQUIRED_PARAMS
    stress_cmd_params: Annotated[list, IgnoredType] = STRESS_CMD_PARAMS
    ami_id_params: Annotated[list, IgnoredType] = AMI_ID_PARAMS
    aws_supported_regions: Annotated[list, IgnoredType] = AWS_SUPPORTED_REGIONS

    model_config = ConfigDict(
        ignored_types=(IgnoredType,),
        validate_assignment=True,
    )

    # Dict-like access methods, since we need to have both attribute-style and dict-style access for dict merging

    def __getitem__(self, item):
        """Enable dict-like access (config['key']) while enforcing that only defined fields can be accessed."""
        if not hasattr(self, item):
            raise ValueError(f"Unknown configuration {item=}")
        return getattr(self, item)

    def __setitem__(self, key, value):
        """Enable dict-like assignment (config['key'] = value) with validation via Pydantic's setattr."""
        if not hasattr(self, key):
            raise ValueError(f"Unknown configuration {key=}")
        setattr(self, key, value)

    def __contains__(self, key):
        """Enable membership testing ('key' in config) to check if a configuration parameter exists."""
        return hasattr(self, key)

    def update(self, other=None, **new_data):
        """
        Provide dict-like update() method that triggers Pydantic validation on each field assignment.

        Enables updating multiple config fields at once while ensuring validation, unlike direct
        attribute assignment which could bypass checks when done in bulk operations.
        """
        if other is not None:
            if hasattr(other, "keys"):
                for key in other.keys():
                    setattr(self, key, other[key])
            else:
                for key, value in other:
                    setattr(self, key, value)

        for key, value in new_data.items():
            setattr(self, key, value)

    def __init__(self, /, **data):  # noqa: PLR0912, PLR0914, PLR0915
        """
        Initialize configuration by loading and merging settings from multiple sources.

        Loads configuration in priority order: defaults → backend configs → user config files →
        environment variables → region-specific data. Validates and resolves cloud images (AMI/GCE)
        based on scylla_version when not explicitly provided.
        """
        super().__init__(**data)

        env = self._load_environment_variables()
        config_files = env.get("config_files", [])
        config_files = [sct_abs_path(f) for f in config_files]

        # prepend to the config list the defaults the config files
        backend = env.get("cluster_backend")
        backend_config_files = [sct_abs_path("defaults/test_default.yaml")]
        if backend:
            if backend == "xcloud":
                assert "xcloud_provider" in env, "xcloud_provider must be set for xcloud backend"
                backend_config_files += self.defaults_config_files[env.get("xcloud_provider")]
            backend_config_files += self.defaults_config_files[str(backend)]
        self.multi_region_params = list(self.per_provider_multi_region_params.get(str(backend), []))

        # load docker images defaults
        self.load_docker_images_defaults()

        # 1) load the default backend config files
        files = anyconfig.load(list(backend_config_files))
        merge_dicts_append_strings(self, files, SCTConfiguration)

        # 2) load the config files
        if config_files:
            for conf_file in list(config_files):
                if not os.path.exists(conf_file):
                    raise FileNotFoundError(f"Couldn't find config file: {conf_file}")
            files = anyconfig.load(list(config_files))
            merge_dicts_append_strings(self, files, SCTConfiguration)

        regions_data = self.get("regions_data") or {}
        if regions_data:
            del self["regions_data"]

        # 2.2) load the region data

        cluster_backend = self.get("cluster_backend")
        cluster_backend = env.get("cluster_backend", cluster_backend)

        region_names = self.region_names

        if cluster_backend in ["aws", "aws-siren", "k8s-eks"]:
            if regions_data:
                for region in region_names:
                    if region not in regions_data:
                        raise ValueError(f"{region} isn't supported, use: {list(regions_data.keys())}")

                    for key, value in regions_data.get(region, {}).items():
                        if key not in self.keys():
                            self[key] = value
                        elif len(self[key].split()) < len(region_names):
                            self[key] += f" {value}"
            else:
                for region in region_names:
                    if region not in self.aws_supported_regions:
                        raise ValueError(f"{region} isn't supported, use: {self.aws_supported_regions}")

        # 3) overwrite with environment variables
        self._constrain_loader_arch_to_stress_tools(env)
        self._resolve_instance_sizes(env)
        merge_dicts_append_strings(self, env, SCTConfiguration)

        # All keystore sources are now merged, so export them before any of the
        # resolution below can construct a KeyStore (xcloud's release tag lookup does).
        self._propagate_keystore_env()

        if not self.get("billing_project"):
            if job_name := os.environ.get("JOB_NAME"):
                release_folder = job_name.split("/")[0]
                if release_folder.startswith(("scylla-", "scylladb-")):
                    billing_project_value = release_folder.removeprefix("scylla-").removeprefix("scylladb-")
                    # Don't set billing_project to "staging"
                    if billing_project_value != "staging":
                        self["billing_project"] = billing_project_value
                        self.log.info(f"Setting billing_project to '{release_folder}' from JOB_NAME: {job_name}")

        if not self.get("billing_project"):
            try:
                result = LOCALRUNNER.run(
                    "git rev-parse --abbrev-ref HEAD", ignore_status=True, verbose=False, timeout=5
                )
                branch_name = result.stdout.strip()
                if branch_name.startswith("branch-"):
                    self["billing_project"] = branch_name.removeprefix("branch-")
                    self.log.info(f"Setting billing_project to '{branch_name}' from git branch name.")
            except (OSError, RuntimeError, TimeoutError) as e:
                self.log.warning(f"Could not get git branch name to set billing_project: {e}")

        if not self.get("billing_project"):
            self["billing_project"] = "no_billing_project"

        # 4) update events max severities
        add_severity_limit_rules(self.get("max_events_severities"))
        print_critical_events()

        self._apply_resolved_placement()

        # snapshot the original AMI params before resolution, so AWS region fallback can
        # re-resolve them for a relocated region
        self._validate_loader_arch_supports_stress_tools()
        self._resolve_loader_image_arch()

        self._ami_params_snapshot = {key: self.get(key) for key in self.ami_id_params}

        # 5) overwrite AMIs
        for key in self.ami_id_params:
            if param := self.get(key):
                self[key] = convert_name_to_ami_if_needed(param, tuple(self.region_names))

        # 6) handle scylla_version if exists
        scylla_linux_distro = self.get("scylla_linux_distro")
        dist_type = scylla_linux_distro.split("-")[0]
        dist_version = scylla_linux_distro.split("-")[-1]

        # 6.0) handle relocatable:<version> scylla_version format
        relocatable_arch = None  # Track arch resolved from relocatable: format for use in section 6.0.1
        if scylla_version := self.get("scylla_version"):
            if scylla_version.startswith("relocatable:"):
                self.log.info("Resolving scylla_version='%s' to unified package URL", scylla_version)
                # Parse format: relocatable:<branch>:<arch>
                # Examples: relocatable:latest, relocatable:master:x86_64, relocatable:branch-2025.1:aarch64
                parts = scylla_version.split(":")
                branch = parts[1] if len(parts) > 1 and parts[1] not in ("latest", "") else "master"
                arch = parts[2] if len(parts) > 2 and parts[2] else "x86_64"
                backend = self.get("cluster_backend")
                if backend == "aws" and len(parts) <= 2:
                    # Auto-detect arch from AWS instance type when not explicitly specified
                    arch = self._get_normalized_arch(self.get("instance_type_db"), region_name=region_names[0])
                elif backend != "aws" and len(parts) <= 2:
                    # TODO: get_arch_from_instance_type should be implemented for all backends, not just AWS
                    self.log.warning(
                        "Architecture auto-detection is only supported on AWS backend. "
                        "Defaulting to '%s'. Use 'relocatable:<branch>:<arch>' format to specify "
                        "architecture explicitly, e.g. 'relocatable:master:aarch64'",
                        arch,
                    )
                relocatable_arch = arch
                unified_url = latest_unified_package(arch=arch, branch=branch)
                self.log.info("Resolved unified package URL: %s", unified_url)
                self["unified_package"] = unified_url
                # ami_id_db_scylla auto-setting is handled below in section 6.0.1
                self["scylla_version"] = ""
                scylla_version = ""

            if (
                scylla_version
                and self.get("cluster_backend") in ["docker", "k8s-eks", "k8s-gke"]
                and not self.get("docker_image")
            ):
                self["docker_image"] = get_scylla_docker_repo_from_version(scylla_version)
            if self.get("cluster_backend") in (
                "docker",
                "k8s-eks",
                "k8s-gke",
                "k8s-local-kind",
                "k8s-local-kind-aws",
                "k8s-local-kind-gce",
            ):
                self.log.info("Assume that Scylla Docker image has repo file pre-installed.")
                self._replace_docker_image_latest_tag()
            elif self.get("unified_package"):
                # unified_package is already set (either directly or resolved from relocatable:);
                # skip image/repo lookup — the base OS image will be auto-set in section 6.0.1
                pass
            elif not self.get("ami_id_db_scylla") and self.get("cluster_backend") == "aws":
                ami_list = []
                for region in region_names:
                    aws_arch = get_arch_from_instance_type(self.get("instance_type_db"), region_name=region)
                    try:
                        # Check if this is a full version tag
                        if parse_scylla_version_tag(scylla_version):
                            # For full version tags, use regular AMI lookup (will match exact tag)
                            ami = get_scylla_ami_versions(version=scylla_version, region_name=region, arch=aws_arch)[0]
                        elif ":" in scylla_version:
                            # For branch versions like "master:latest"
                            ami = get_branched_ami(scylla_version=scylla_version, region_name=region, arch=aws_arch)[0]
                        else:
                            # For simple versions like "5.2.1"
                            ami = get_scylla_ami_versions(version=scylla_version, region_name=region, arch=aws_arch)[0]
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError(
                            f"AMIs for scylla_version='{scylla_version}' not found in {region} arch={aws_arch}"
                        ) from ex
                    self.log.debug(
                        "Found AMI %s(%s) for scylla_version='%s' in %s", ami.name, ami.image_id, scylla_version, region
                    )
                    ami_list.append(ami)
                self["ami_id_db_scylla"] = " ".join(ami.image_id for ami in ami_list)
            elif not self.get("gce_image_db") and self.get("cluster_backend") == "gce":
                try:
                    if parse_scylla_version_tag(scylla_version):
                        # For full version tags, use regular GCE image lookup (will match exact tag)
                        gce_image = get_scylla_gce_images_versions(version=scylla_version)[0]
                    elif ":" in scylla_version:
                        # For branch versions like "master:latest"
                        gce_image = get_branched_gce_images(scylla_version=scylla_version)[0]
                    else:
                        # For simple versions like "5.2.1"
                        # gce_image.name format examples: scylla-4-3-6 or scylla-enterprise-2021-1-2
                        gce_image = get_scylla_gce_images_versions(version=scylla_version)[0]
                except Exception as ex:  # noqa: BLE001
                    raise ValueError(f"GCE image for scylla_version='{scylla_version}' was not found") from ex

                self.log.debug("Found GCE image %s for scylla_version='%s'", gce_image.name, scylla_version)
                self["gce_image_db"] = gce_image.self_link
            elif not self.get("azure_image_db") and self.get("cluster_backend") == "azure":
                scylla_azure_images = []
                azure_region_names = self.get("azure_region_name")
                azure_arch = azure_utils.get_arch_from_azure_instance_type(self.get("azure_instance_type_db"))

                for region in azure_region_names:
                    try:
                        if parse_scylla_version_tag(scylla_version):
                            azure_image = azure_utils.get_scylla_images(
                                scylla_version=scylla_version, region_name=region, arch=azure_arch
                            )[0]
                        elif ":" in scylla_version:
                            azure_image = azure_utils.get_scylla_images(
                                scylla_version=scylla_version, region_name=region, arch=azure_arch
                            )[0]
                        else:
                            if azure_arch != azure_utils.VmArch.X86:
                                raise ValueError(
                                    f"Azure released images (community gallery) do not support "
                                    f"arch={azure_arch.value}. Use a branch version "
                                    f"(e.g. 'master:latest') for ARM instances."
                                )
                            azure_image = azure_utils.get_released_scylla_images(
                                scylla_version=scylla_version, region_name=region, arch=azure_arch
                            )[0]
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError(
                            f"Azure Image for scylla_version='{scylla_version}' not found in {region}"
                            f" (arch={azure_arch.value})"
                        ) from ex
                    self.log.debug(
                        "Found Azure Image %s for scylla_version='%s' in %s (arch=%s)",
                        azure_image.name,
                        scylla_version,
                        region,
                        azure_arch.value,
                    )
                    scylla_azure_images.append(azure_image)
                self["azure_image_db"] = " ".join(
                    getattr(image, "id", None) or getattr(image, "unique_id", None) for image in scylla_azure_images
                )
            elif not self.get("oci_image_db") and self.get("cluster_backend") == "oci":
                scylla_oci_images = []
                if isinstance(self.get("oci_region_name"), list):
                    oci_region_names = self.get("oci_region_name")
                else:
                    oci_region_names = [self.get("oci_region_name")]

                for region in oci_region_names:
                    try:
                        if ":" in scylla_version:
                            oci_image = oci_utils.get_scylla_images_by_branch(scylla_version, region)[0]
                        else:
                            oci_image = oci_utils.get_scylla_images_by_version(scylla_version, region)[0]
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError(
                            f"Oracle Image for scylla_version='{scylla_version}' not found in {region}"
                        ) from ex
                    # NOTE: oci_image: ["OCI", <name>, <id>, ...]
                    self.log.info(
                        "Found Oracle Image %s for scylla_version='%s' in %s",
                        oci_image[1],
                        scylla_version,
                        region,
                    )
                    scylla_oci_images.append(oci_image)
                self["oci_image_db"] = " ".join(image[2] for image in scylla_oci_images)
            elif self.get("cluster_backend") == "xcloud" and ":" in scylla_version:
                self._resolve_xcloud_version_tag(self.get("scylla_version"))
            elif not self.get("scylla_repo"):
                self["scylla_repo"] = find_scylla_repo(scylla_version, dist_type, dist_version)
            else:
                raise ValueError(
                    "'scylla_version' can't used together with 'ami_id_db_scylla', 'gce_image_db' or with 'scylla_repo'"
                )

            # auto-discover target image for platform migration
            self.target_db_image_ids = []
            if self.get("instance_type_db_target"):
                if self.get("cluster_backend") == "aws":
                    target_ami_list = []
                    for region in region_names:
                        target_arch = get_arch_from_instance_type(
                            self.get("instance_type_db_target"), region_name=region
                        )
                        try:
                            ami = (
                                get_branched_ami(scylla_version=scylla_version, region_name=region, arch=target_arch)[0]
                                if ":" in scylla_version
                                else get_scylla_ami_versions(
                                    version=scylla_version, region_name=region, arch=target_arch
                                )[0]
                            )
                        except Exception as ex:  # noqa: BLE001
                            raise ValueError(
                                f"Target AMI for scylla_version='{scylla_version}' not found in {region} "
                                f"arch={target_arch} (for instance_type_db_target)"
                            ) from ex
                        self.log.debug(
                            "Found target AMI %s(%s) for scylla_version='%s' arch=%s in %s",
                            ami.name,
                            ami.image_id,
                            scylla_version,
                            target_arch,
                            region,
                        )
                        target_ami_list.append(ami)
                    self.target_db_image_ids = [ami.image_id for ami in target_ami_list]

        # 6.0.1) when unified_package is used (either directly or resolved from relocatable:),
        #        override use_preinstalled_scylla and ami_db_scylla_user for AWS
        if self.get("unified_package"):
            self.log.info("unified_package is set, forcing use_preinstalled_scylla=False")
            self["use_preinstalled_scylla"] = False
            if self.get("cluster_backend") == "aws":
                self.log.info("unified_package is set on AWS backend, forcing ami_db_scylla_user='ubuntu'")
                self["ami_db_scylla_user"] = "ubuntu"
                if not self.get("ami_id_db_scylla"):
                    # Use arch from relocatable: format if available, otherwise detect from instance type
                    arch = relocatable_arch or self._get_normalized_arch(
                        self.get("instance_type_db"), region_name=region_names[0]
                    )
                    ubuntu_ssm_map = {
                        "x86_64": "resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
                        "aarch64": "resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id",
                    }
                    ami_ssm = ubuntu_ssm_map.get(arch, ubuntu_ssm_map["x86_64"])
                    self.log.info(
                        "unified_package: auto-setting ami_id_db_scylla to Ubuntu 24.04 base AMI for arch=%s: %s",
                        arch,
                        ami_ssm,
                    )
                    self["ami_id_db_scylla"] = convert_name_to_ami_if_needed(ami_ssm, tuple(region_names))

        # 6.1) handle oracle_scylla_version if exists
        if (oracle_scylla_version := self.get("oracle_scylla_version")) and self.get("db_type") == "mixed_scylla":
            if resolver := _ORACLE_IMAGE_RESOLVERS.get(self.get("cluster_backend")):
                oracle_image_param = ORACLE_IMAGE_PARAMS[self.get("cluster_backend")]
                if self.get(oracle_image_param):
                    raise ValueError(f"'oracle_scylla_version' and '{oracle_image_param}' can't used together")
                self[oracle_image_param] = resolver(self, oracle_scylla_version)

        # 6.2) handle vector_store_version if exists
        if vs_version := self.get("vector_store_version"):
            if self.get("ami_id_vector_store"):
                raise ValueError("'vector_store_version' can't be used together with 'ami_id_vector_store'")
            if self.get("cluster_backend") == "aws":
                ami_list = []
                for region in region_names:
                    aws_arch = get_arch_from_instance_type(self.get("instance_type_vector_store"), region_name=region)
                    try:
                        ami = get_vector_store_ami_versions(version=vs_version, region_name=region, arch=aws_arch)[0]
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError(
                            f"AMIs for vs_version='{vs_version}' not found in {region} arch={aws_arch}"
                        ) from ex
                    self.log.debug(
                        "Found AMI %s(%s) for vs_version='%s' in %s", ami.name, ami.image_id, vs_version, region
                    )
                    ami_list.append(ami)
                self["ami_id_vector_store"] = " ".join(ami.image_id for ami in ami_list)

        # 6.3) if a region-fallback relocated the cluster, re-resolve region-bound AWS AMIs for the new region
        if self._resolved_placement_source_region:
            self.resolve_amis(self.region_names, source_region=self._resolved_placement_source_region)

        # 7) support lookup of repos for upgrade test
        new_scylla_version = self.get("new_version")
        if new_scylla_version and not "k8s" in cluster_backend:
            if not self.get("ami_id_db_scylla") and cluster_backend == "aws":
                raise ValueError("'new_version' isn't supported for AWS AMIs")

            elif not self.get("new_scylla_repo"):
                self["new_scylla_repo"] = find_scylla_repo(new_scylla_version, dist_type, dist_version)

        # 8) resolve repo symlinks
        for repo_key in (
            "scylla_repo",
            "new_scylla_repo",
        ):
            if repo_link := self.get(repo_key):
                setattr(self, repo_key, resolve_latest_repo_symlink(repo_link))

        # 9) append username or ami_id_db_scylla_desc to the user_prefix
        version_tag = self.get("ami_id_db_scylla_desc") or getpass.getuser()
        user_prefix = self.get("user_prefix") or getpass.getuser()
        prefix_max_len = 35
        if version_tag != user_prefix:
            user_prefix = f"{user_prefix}-{version_tag}"
        if self.get("cluster_backend") == "azure":
            # for Azure need to shorten it more due longer region names
            prefix_max_len -= 2
        if self.get("cluster_backend") == "oci":
            # OCI node names include region (up to 14 chars) and must fit X.509 CN 64-char limit
            prefix_max_len -= 10
        if self.get("db_type") == "mixed_scylla" and self.get("cluster_backend") in ("gce", "azure", "oci"):
            # reserve space for the '-oracle' suffix so generated names stay within backend limits
            prefix_max_len -= len(ORACLE_USER_PREFIX_SUFFIX)
        if (self.get("simulated_regions") or 0) > 1:
            # another shortening for simulated regions due added simulated dc suffix
            prefix_max_len -= 3
        self["user_prefix"] = user_prefix[:prefix_max_len]

        # remove any special characters from user_prefix, since later it will be used as a part of the instance names
        # and some platfrom don't support special characters in the instance names (docker, AWS and such)
        self["user_prefix"] = re.sub(r"[^a-zA-Z0-9-]", "-", self.get("user_prefix"))

        # 11) validate that supported instance_provision selected
        if self.get("instance_provision") not in ["spot", "on_demand", "spot_fleet"]:
            raise ValueError(f"Selected instance_provision type '{self.get('instance_provision')}' is not supported!")

        # 12) validate authenticator parameters
        if self.get("authenticator") and self.get("authenticator") == "PasswordAuthenticator":
            authenticator_user = self.get("authenticator_user")
            authenticator_password = self.get("authenticator_password")
            if not (authenticator_password and authenticator_user):
                raise ValueError(
                    "For PasswordAuthenticator authenticator authenticator_user and authenticator_password"
                    " have to be provided"
                )

        if self.get("alternator_enforce_authorization"):
            if not self.get("authenticator") or not self.get("authorizer"):
                raise ValueError(
                    "When enabling `alternator_enforce_authorization` both `authenticator` and `authorizer` should be defined"
                )

        # 13) validate stress and prepare duration:
        if stress_duration := self.get("stress_duration"):
            try:
                self["stress_duration"] = abs(int(stress_duration))
            except ValueError:
                raise ValueError(
                    f"Configured stress duration for generic test duratinon have to be \
                                 positive integer number in minutes. Current value: {stress_duration}"
                ) from ValueError
        if prepare_stress_duration := self.get("prepare_stress_duration"):
            try:
                self["prepare_stress_duration"] = abs(int(prepare_stress_duration))
            except ValueError:
                raise ValueError(
                    f"Configured stress duration for generic test duratinon have to be \
                                 positive integer number in minutes. Current value: {prepare_stress_duration}"
                ) from ValueError

        # 14 Validate run_fullscan parameters
        if run_fullscan_params := self.get("run_fullscan"):
            if not isinstance(run_fullscan_params, list) or not len(run_fullscan_params) > 0:
                raise ValueError(f"run_fullscan parameter must be non empty list, but got: {run_fullscan_params}")
            for param in run_fullscan_params:
                try:
                    ConfigParams(**json.loads(param))
                except json.decoder.JSONDecodeError as exp:
                    raise ValueError(
                        f"each item of run_fullscan list: {run_fullscan_params}, "
                        f"item {param}, must be JSON but got error: {exp!r}"
                    ) from exp
                except TypeError as exp:
                    raise ValueError(f" Got error: {exp!r}, on item '{param}'") from exp

        # 14.1 On Docker, simulated racks are injected as --dc/--rack entrypoint arguments at container
        #      creation, which the Scylla image entrypoint only understands since 2026.1. Check it here,
        #      before section 15 forces the snitch and before any container is created.
        if cluster_backend == "docker" and simulated_racks_enabled(self):
            self._validate_docker_simulated_racks()

        # 15 Force endpoint_snitch to GossipingPropertyFileSnitch if using simulated_regions or simulated_racks
        if (self.get("simulated_regions") or 0) > 1 or simulated_racks_enabled(self):
            if snitch := self.get("endpoint_snitch"):
                assert snitch.endswith("GossipingPropertyFileSnitch"), (
                    f"Simulating racks requires endpoint_snitch to be GossipingPropertyFileSnitch while it set to {self['endpoint_snitch']}"
                )
            self["endpoint_snitch"] = "org.apache.cassandra.locator.GossipingPropertyFileSnitch"

        # 16 Validate use_dns_names
        if self.get("use_dns_names"):
            if cluster_backend and cluster_backend not in ("aws", "gce", "oci"):
                raise ValueError(f"use_dns_names is not supported for {cluster_backend} backend")

        # 17 Validate scylla network configuration mandatory values
        if scylla_network_config := self.get("scylla_network_config"):
            check_list = {
                "listen_address": None,
                "rpc_address": None,
                "broadcast_rpc_address": None,
                "broadcast_address": None,
                "test_communication": None,
            }
            number2word = {1: "first", 2: "second", 3: "third"}
            nics = set()
            for i, address_config in enumerate(scylla_network_config):
                for param in ["address", "ip_type", "public", "nic"]:
                    if address_config.get(param) is None:
                        raise ValueError(
                            f"'{param}' parameter value for {number2word[i + 1]} address is not defined. It is must parameter"
                        )

                if (
                    address_config["ip_type"] == "ipv4"
                    and address_config["nic"] == 1
                    and address_config["public"] is True
                ):
                    raise ValueError(
                        "If ipv4 and public is True it has to be primary network interface, it means device index (nic) is 0"
                    )

                if (
                    self.get("cluster_backend") == "gce"
                    and address_config["nic"] != 0
                    and address_config.get("use_dns")
                ):
                    raise ValueError(
                        "GCE creates a private DNS record for the primary network interface only, so a DNS name "
                        "on a secondary interface resolves back to the primary one. Set 'use_dns: false' for "
                        f"'{address_config['address']}' or move it to nic 0"
                    )

                nics.add(address_config["nic"])
                if address_config["address"] not in check_list:
                    continue

                check_list[address_config["address"]] = True

            if not_defined_address := ",".join([key for key, value in check_list.items() if value is None]):
                raise ValueError(f"Interface address(es) were not defined: {not_defined_address}")

            regions = self.gce_datacenters if self.get("cluster_backend") == "gce" else self.region_names
            if len(nics) > 1 and len(regions) >= 2:
                raise ValueError("Multiple network interfaces aren't supported for multi region use cases")

        # 18 Validate K8S TLS+SNI values
        if self.get("k8s_enable_sni") and not self.get("k8s_enable_tls"):
            raise ValueError("'k8s_enable_sni=true' requires 'k8s_enable_tls' also to be 'true'.")

        SCTCapacityReservation.get_cr_from_aws(self)
        SCTDedicatedHosts.reserve(self)

        # Validate zero token nodes
        if self.get("use_zero_nodes"):
            self._validate_zero_token_backend_support(backend=cluster_backend)
            zero_nodes_num = self.get("n_db_zero_token_nodes")
            data_nodes_num = self.get("n_db_nodes")
            # if number of zero nodes is set for cluster setup, check correctness of settings
            if zero_nodes_num:
                assert len(zero_nodes_num) == len(data_nodes_num), (
                    "Config of zero token nodes is not equal config of data nodes for multi dc"
                )

        self._validate_perf_gradual_throttle_steps()

        if self.get("c_s_driver_version") == "random":
            self["c_s_driver_version"] = random.choice(["4", "3"])
            self.log.debug("Using random cassandra-stress driver version: %s", self["c_s_driver_version"])

    def _propagate_keystore_env(self):
        """Export the resolved keystore settings so bare ``KeyStore()`` callers agree.

        KeyStore reads ``SCT_KEYSTORE_*`` from the environment, so utility code that
        doesn't hold an SCTConfiguration reference needs the config-file / CLI values
        mirrored there.  A value we exported ourselves is refreshed, but an env var
        the user actually set is never overwritten -- otherwise the first instance's
        value would leak and outrank the config file of every later instance in the
        same process.

        Must run as soon as the config sources are merged and *before* anything in
        ``__init__`` can construct a KeyStore: xcloud's ``release:latest`` path
        reaches ``cloud_env_credentials`` -> ``KeyStore()`` from inside __init__, so
        exporting at the end of __init__ left that fetch on the wrong backend and
        made the documented ``keystore_backend: 's3'`` opt-out a no-op for it.
        """
        for param in ("keystore_backend", "keystore_sm_prefix", "keystore_sm_region"):
            env_name = f"SCT_{param.upper()}"
            value = self.get(param)
            if not value:
                continue
            if env_name not in os.environ or _is_self_exported_keystore_env(env_name):
                os.environ[env_name] = str(value)
                _KEYSTORE_ENV_EXPORTED[env_name] = str(value)
            else:
                # A user override we must not touch.  Drop our marker so that
                # setting the env var back to the value we once exported still
                # reads as a real override rather than as our own leak.
                _KEYSTORE_ENV_EXPORTED.pop(env_name, None)

    def load_docker_images_defaults(self):
        stress_image = _load_docker_images_defaults_cached()
        if stress_image:
            anyconfig.merge(self, dict(stress_image=stress_image))

    def log_config(self):
        self.log.info(self.dump_config())

    @property
    def total_db_nodes(self) -> List[int]:
        """Used to get total number of db nodes data nodes and zero nodes"""
        use_zero_nodes = self.get("use_zero_nodes")
        zero_nodes_num = self.get("n_db_zero_token_nodes")
        data_nodes_num = self.get("n_db_nodes")
        total_nodes = data_nodes_num[:]
        if use_zero_nodes and zero_nodes_num:
            total_nodes = [n1 + n2 for n1, n2 in zip(data_nodes_num, zero_nodes_num)]

        self.log.debug("Total nodes: %s", total_nodes)
        return total_nodes

    def _apply_resolved_placement(self) -> None:
        """Apply region/AZ selected in a previous provisioning step.

        Provisioning and test execution can run in separate `hydra` invocations, so a region selected
        by the 'region fallback' procedure at provisioning can be lost when config is reloaded during
        test execution.
        The provisioner writes the selected region to a test_id-keyed file, and this method applies
        it in the loaded config.

        The behavior can be disabled with `SCT_IGNORE_RESOLVED_PLACEMENT`.
        """
        self._resolved_placement_source_region = None
        if os.environ.get("SCT_IGNORE_RESOLVED_PLACEMENT"):
            return

        test_id = self.get("reuse_cluster") or self.get("test_id")
        if not test_id or test_id == "None":
            return

        placement = TestConfig.read_resolved_placement(test_id)
        if not placement:
            return

        original_region_list = " ".join(self.region_names) if self.region_names else None
        original_first_region = self.region_names[0] if self.region_names else None
        region_name = placement.get("region_name")
        availability_zone = placement.get("availability_zone")
        amis = placement.get("amis")
        if region_name:
            if self.get("cluster_backend") == "gce":
                # GCE has no AWS-style region_name or per-region AMIs: the relocated region is
                # carried in gce_datacenter (env-first), so a split provision->run pipeline picks
                # up the region chosen by region fallback instead of the original one.
                os.environ["SCT_GCE_DATACENTER"] = region_name
                self["gce_datacenter"] = region_name
            else:
                os.environ["SCT_REGION_NAME"] = region_name
                self["region_name"] = region_name
                if amis:
                    for key, value in amis.items():
                        self[key] = value
                elif original_region_list and region_name != original_region_list:
                    self._resolved_placement_source_region = original_first_region
        if availability_zone:
            os.environ["SCT_AVAILABILITY_ZONE"] = availability_zone
            self["availability_zone"] = availability_zone

        self.log.info(
            "Applied resolved placement for test_id=%s: region_name=%s availability_zone=%s amis=%s",
            test_id,
            region_name,
            availability_zone,
            bool(amis),
        )

    def resolve_amis(self, region_names: List[str], source_region: str | None = None) -> None:
        """Re-resolve region-bound AWS AMI IDs for the given regions `region_names`."""
        if self.get("cluster_backend") != "aws":
            return

        target_regions = list(region_names)
        intent = getattr(self, "_ami_params_snapshot", None) or {}
        for key in self.ami_id_params:
            original = intent.get(key)
            current = self.get(key)
            if original and not original.split()[0].startswith("ami-"):
                self[key] = convert_name_to_ami_if_needed(original, tuple(target_regions))
            elif current and source_region:
                self[key] = self._remap_amis_to_regions(current, source_region, target_regions, key)

        if getattr(self, "target_db_image_ids", None) and source_region:
            self.target_db_image_ids = [
                self._remap_amis_to_regions(ami, source_region, target_regions, "instance_type_db_target")
                for ami in self.target_db_image_ids
            ]

    @staticmethod
    def _remap_amis_to_regions(value: str, source_region: str, region_names: List[str], key: str) -> str:
        """Remap AMI IDs from `source_region` into each target region."""
        remapped = []
        for region in region_names:
            for ami_id in value.split():
                matches = find_equivalent_ami(ami_id, source_region, target_regions=[region])
                if not matches:
                    raise RegionAMINotFoundError(
                        f"No equivalent AMI for {key}={ami_id} (from {source_region}) in {region}; region ineligible for fallback"
                    )
                remapped.append(matches[0]["ami_id"])
        return " ".join(remapped)

    @property
    def region_names(self) -> List[str]:
        region_names = self.environment.get("region_name")
        if region_names is None:
            region_names = self.get("region_name")
        if region_names is None:
            region_names = ""
        if isinstance(region_names, str):
            region_names = region_names.split()
        output = []
        for region_name in region_names:
            output.extend(region_name.split())
        return output

    @property
    def gce_datacenters(self) -> List[str]:
        gce_datacenters = self.environment.get("gce_datacenter")
        if gce_datacenters is None:
            gce_datacenters = self.get("gce_datacenter")
        if gce_datacenters is None:
            gce_datacenters = ""
        if isinstance(gce_datacenters, str):
            gce_datacenters = gce_datacenters.split()
        output = []
        for gce_datacenter in gce_datacenters:
            output.extend(gce_datacenter.split())
        return output

    @cached_property
    def cloud_provider_params(self) -> dict:
        cloud_provider = self.get("xcloud_provider").lower()
        if cloud_provider == "aws":
            return {
                "region": self.region_names[0],
                "instance_type_db": self.get("instance_type_db"),
                "instance_type_loader": self.get("instance_type_loader"),
                "root_disk_size_loader": self.get("root_disk_size_loader"),
                "root_disk_type_loader": self.get("root_disk_type_loader"),
            }
        elif cloud_provider == "gce":
            return {
                "region": self.gce_datacenters[0],
                "instance_type_db": self.get("gce_instance_type_db"),
                "instance_type_loader": self.get("gce_instance_type_loader"),
                "root_disk_size_loader": self.get("gce_root_disk_size_loader"),
                "root_disk_type_loader": self.get("gce_root_disk_type_loader"),
            }
        return {}

    @cached_property
    def cloud_env_credentials(self) -> dict:
        if creds_file := self.get("xcloud_credentials_path"):
            creds = get_cloud_rest_credentials_from_file(creds_file)
        else:
            creds = KeyStore().get_cloud_rest_credentials(self.get("xcloud_env"))
        return creds

    @property
    def environment(self) -> dict:
        return self._load_environment_variables()

    def get_default_value(self, key, include_backend=False):
        default_config_files = [sct_abs_path("defaults/test_default.yaml")]
        if self.cluster_backend and include_backend:
            default_config_files += self.defaults_config_files[str(self.cluster_backend)]

        return anyconfig.load(list(default_config_files)).get(key, None)

    @staticmethod
    def _format_default_value_for_docs(value):
        if value in ("", None):
            return "N/A"
        return value

    @classmethod
    def _fields_by_group(cls):
        """Yield ``(group_title, [(field_name, field), ...])`` in `CONFIG_GROUPS` order.

        A field belongs to the mixin that declares it, which is what makes the generated docs
        browsable by domain. Each mixin's own fields come from its `model_fields` -- mixins
        inherit only from `BaseModel`, so that is exactly its own set. (Reading
        `__dict__["__annotations__"]` does not work here: Python 3.14 defers annotations, so the
        key is absent until something forces evaluation.) Anything declared on `SCTConfiguration`
        itself falls into a trailing "Other" section, so a field can never silently vanish from
        the documentation.
        """
        documented = {
            name: field for name, field in cls.model_fields.items() if not (field.exclude or is_ignored_field(field))
        }

        grouped = []
        claimed = set()
        for mixin in CONFIG_GROUPS:
            names = [n for n in mixin.model_fields if n in documented and n not in claimed]
            if not names:
                continue
            claimed.update(names)
            grouped.append((mixin.config_group, [(n, documented[n]) for n in names]))

        if leftover := [(n, f) for n, f in documented.items() if n not in claimed]:
            grouped.append(("Other", leftover))
        return grouped

    @classmethod
    def _get_defaults_for_docs(cls):
        test_default_path = sct_abs_path("defaults/test_default.yaml")
        base_defaults = anyconfig.load(test_default_path)
        defaults_config_files = cls.model_fields["defaults_config_files"].default
        backend_defaults = {
            backend: anyconfig.load([test_default_path, *config_files])
            for backend, config_files in defaults_config_files.items()
        }
        return base_defaults, backend_defaults

    @classmethod
    def _get_backend_overrides_for_docs(cls, field_name, base_defaults, backend_defaults):
        if field_name in base_defaults:
            base_value = str(cls._format_default_value_for_docs(base_defaults[field_name]))
        else:
            base_value = "N/A"
        grouped_values = {}

        for backend, defaults in backend_defaults.items():
            if field_name not in defaults:
                continue

            backend_value = str(cls._format_default_value_for_docs(defaults[field_name]))
            if backend_value == base_value:
                continue

            grouped_values.setdefault(backend_value, []).append(backend)

        if not grouped_values:
            return ""

        return "\n".join(f"- `{value}`: {', '.join(backends)}" for value, backends in grouped_values.items())

    def _load_environment_variables(self):
        """Load configuration from environment variables.

        Custom implementation instead of Pydantic's BaseSettings because we need:
        1. Control over the order in which env vars are applied (after defaults, before config files)
        2. Support for appendable fields with '++' syntax via SCT_<FIELD>++<INDEX> pattern
        3. Custom validators (BeforeValidator) to be applied during env var parsing
        4. Backwards compatibility with existing SCT_* environment variable naming
        """
        environment_vars = {}
        for field_name, field in self.__class__.model_fields.items():
            if field.exclude or is_ignored_field(field):
                continue

            field_env = f"SCT_{field_name.upper()}"

            # Env vars this process exported itself are not user input, so they must
            # not be given environment-variable precedence over a config file.  Only
            # skip them while they still hold the value we wrote -- a value the user
            # changed since is a genuine override.
            if _is_self_exported_keystore_env(field_env):
                continue

            def no_op(x):
                return x

            for annotation in field.metadata:
                if isinstance(annotation, BeforeValidator):
                    from_env_func = annotation.func
                    break
            else:
                from_env_func = no_op

            if field_env and any(key.startswith(field_env) for key in os.environ.keys()):
                if field_env in os.environ.keys():
                    try:
                        raw_value = os.environ[field_env]
                        if isinstance(raw_value, str):
                            raw_value = raw_value.strip()
                        environment_vars[field_name] = from_env_func(raw_value)
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError(f"failed to parse {field_env} from environment variable") from ex
                # Iterate "." matches before "__" matches so that, if both forms set the
                # same sub-key, "__" deterministically wins (last-write-wins below) --
                # independent of os.environ's iteration order.
                nested_keys = [
                    (key, sub_key)
                    for sep in NESTED_ENV_SEPARATORS
                    for key in os.environ
                    if (sub_key := _nested_env_subkey(key, field_env, sep)) is not None
                ]
                if nested_keys:
                    list_value = []
                    dict_value = {}
                    for key, nest_key in nested_keys:
                        nested_value = os.environ.get(key)
                        if isinstance(nested_value, str):
                            nested_value = nested_value.strip()
                        if nest_key.isdigit():
                            list_value.insert(int(nest_key), nested_value)
                        else:
                            dict_value[nest_key] = nested_value
                    current_value = environment_vars.get(field_name)
                    if current_value and isinstance(current_value, dict):
                        current_value.update(dict_value)
                    else:
                        environment_vars[field_name] = list_value or dict_value

        return environment_vars

    def _resolve_instance_sizes(self, env: dict | None = None) -> None:  # noqa: PLR0914
        """Resolve constraint dicts for instance-type params to literal type strings.

        If *env* is provided (pre-merge env vars), resolution happens in *env*
        in-place so the dict is replaced by a string before it hits the Pydantic
        model (which only accepts strings for instance-type fields).  When *env*
        is None, values are read from and written to ``self``.

        Raises:
            ValueError: If a constraint dict cannot be resolved to any instance.
        """
        backend = (env.get("cluster_backend") if env else None) or self.get("cluster_backend")
        if backend in _SIZING_SKIP_BACKENDS:
            self.log.info("Skipping instance size resolution for backend %s", backend)
            return

        cloud = backend_to_cloud(backend, (env.get("xcloud_provider") if env else None) or self.get("xcloud_provider"))
        if not cloud:
            self.log.warning("Unknown backend %s — skipping instance size resolution", backend)
            return

        AGNOSTIC_FALLBACK = {
            "db": "sizing_db",
            "db_oracle": "sizing_db_oracle",
            "zero_token": "sizing_db",
            "loader": "sizing_loader",
            "monitor": "sizing_monitor",
        }

        db_type = (env.get("db_type") if env else None) or self.get("db_type") or ""
        role_params = _SIZING_ROLE_PARAMS.get(cloud, {})
        for role, param_name in role_params.items():
            if role == "db_oracle" and db_type not in ("mixed_scylla", "mixed_cassandra"):
                continue
            is_fallback = False
            if env is not None and param_name in env:
                value = env[param_name]
            else:
                value = self.get(param_name)
            if isinstance(value, str) and value:
                continue
            if not value:
                fallback_param = AGNOSTIC_FALLBACK.get(role)
                if not fallback_param or fallback_param == param_name:
                    continue
                fallback_value = (env.get(fallback_param) if env else None) or self.get(fallback_param)
                if not isinstance(fallback_value, dict):
                    continue
                value = fallback_value
                is_fallback = True
                self.log.info("Using agnostic fallback %s for %s", fallback_param, param_name)
            if isinstance(value, dict):
                cache_key = (role, cloud, tuple(sorted(value.items())))
                cached = _SIZING_RESOLUTION_CACHE.get(cache_key)
                resolved, resolved_arch = cached if cached else (None, None)
                if resolved is None:
                    try:
                        catalog_dir = pathlib.Path(sct_abs_path("data/instance_catalog"))
                        catalog = InstanceCatalog.from_directory(catalog_dir)
                        result = select_instance(catalog, role, cloud, value)
                        resolved = result.instance_type
                        resolved_arch = result.arch
                        _SIZING_RESOLUTION_CACHE[cache_key] = (resolved, resolved_arch)
                    except NoMatchingInstanceError as exc:
                        if is_fallback:
                            self.log.warning("Agnostic fallback for %s found no match: %s", param_name, exc)
                            if env is not None:
                                env.pop(param_name, None)
                            continue
                        raise ValueError(f"Cannot resolve {param_name}: {exc}") from exc
                    except FileNotFoundError as exc:
                        self.log.warning("Instance catalog not found for %s: %s", param_name, exc)
                        if env is not None:
                            env.pop(param_name, None)
                        continue
                    except ValueError as exc:
                        if is_fallback:
                            self.log.warning("Agnostic fallback for %s has invalid constraints: %s", param_name, exc)
                            if env is not None:
                                env.pop(param_name, None)
                            continue
                        raise ValueError(f"Invalid constraint for {param_name}: {exc}") from exc
                self.log.info("Resolved %s: %s → %s", param_name, value, resolved)
                if resolved_arch:
                    if not hasattr(self, "_sizing_resolved_arch"):
                        self._sizing_resolved_arch = {}
                    self._sizing_resolved_arch[role] = resolved_arch
                if env is not None and param_name in env:
                    env[param_name] = resolved
                else:
                    setattr(self, param_name, resolved)

    def get(self, key: str | None):
        """
        get the value of test configuration parameter by the name
        """
        if key is None:
            return None

        if key and "." in key:
            if ret_val := self._dotted_get(key):
                return ret_val
        ret_val = getattr(self, key, None)

        if key in self.multi_region_params and isinstance(ret_val, list):
            ret_val = " ".join(str(v) for v in ret_val)

        return ret_val

    def _dotted_get(self, key: str):
        """
        if key for retrieval is dot notation, ex. 'stress_image.ycsb'
        we assume `stress_image` would be a dict
        """
        keys = key.split(".")
        current = self.get(keys[0])
        for k in keys[1:]:
            if not isinstance(current, dict):
                break
            current = current.get(k)
        return current

    def _validate_value(self, field_name: str, field: pydantic_fields.FieldInfo):
        def no_op(x):
            return x

        for annotation in field.metadata:
            if isinstance(annotation, BeforeValidator):
                from_env_func = annotation.func
                break
        else:
            from_env_func = no_op

        param_value = self.get(field_name)

        # Handle list-based validation (e.g. nemesis_selector when multiple nemesis classes)
        if field_name == "nemesis_selector" and len(self.get("nemesis_class_name")) > 1:
            for list_element in param_value:
                try:
                    from_env_func(list_element)
                except Exception as ex:  # pylint: disable=broad-except  # noqa: BLE001
                    raise ValueError(f"failed to validate {field_name}") from ex
            return

        # Regular single-value validation
        from_env_func(param_value)

    @staticmethod
    def _as_list(value) -> list:
        """Normalise a stress-cmd param value to a list.

        Most stress-cmd params are ``list[str]`` after Phase-2 normalisation,
        but ``gemini_cmd`` is a plain ``String`` scalar.  Wrapping it here
        prevents the iteration loops below from consuming it character-by-character.
        """
        if value is None:
            return []
        return value if isinstance(value, list) else [value]

    @property
    def list_of_stress_tools(self) -> Set[str]:
        stress_tools = set()
        for param_name in self.stress_cmd_params:
            stress_cmds = self._as_list(self.get(param_name))
            if not stress_cmds:
                continue

            for stress_cmd in stress_cmds:
                if not stress_cmd:
                    continue
                if stress_tool := stress_cmd.split(maxsplit=2)[0]:
                    stress_tools.add(stress_tool)

        return stress_tools

    def check_required_files(self):
        if user_creds := self.get("user_credentials_path"):
            _check_file_exists(user_creds)

        for param_name in self.stress_cmd_params:
            stress_cmds = self._as_list(self.get(param_name))
            if not stress_cmds:
                continue
            for stress_cmd in stress_cmds:
                if not stress_cmd:
                    continue
                cmd = stress_cmd.strip(" ")
                if cmd.startswith("latte"):
                    script_name_regx = re.compile(r"([/\w-]*\.rn)")
                    script_name = script_name_regx.search(cmd).group(1)
                    if script_name.startswith("scylla-qa-internal"):
                        continue
                    full_path = pathlib.Path(get_sct_root_path()) / script_name
                    assert full_path.exists(), f"{full_path} doesn't exists, please check your configuration"

                if not cmd.startswith("cassandra-stress"):
                    continue
                for option in cmd.split():
                    if option.startswith("profile="):
                        option = option.split("=", 1)  # noqa: PLW2901
                        if len(option) < 2:
                            continue
                        profile_path = option[1]
                        if "scylla-qa-internal" in profile_path:
                            continue
                        if not profile_path.startswith("/tmp"):
                            raise ValueError(
                                f"Stress command parameter '{param_name}' contains wrong path "
                                f"'{profile_path}' to profile, it should be formed in following "
                                "manner '/tmp/{file_name_from_data_dir}'"
                            )
                        profile_name = profile_path[5:]
                        if pathlib.Path(sct_abs_path(os.path.join("data_dir", profile_name))).exists():
                            break  # We are ok here and skipping whole command if file is there
                        raise ValueError(
                            f"Stress command parameter '{param_name}' contains profile "
                            f"'{profile_path}' that does not exists under data_dir/"
                        )
        self._validate_scylla_d_overrides_files_exists()

    def verify_configuration(self):
        """
        Check that all required values are set, and validated each value to be of correct type or value
        also check required options per backend

        :return: None
        :raises ValueError: on failures in validations
        :raise Exception: on unsupported backends
        """
        self._check_unexpected_sct_variables()
        self._validate_sct_variable_values()
        backend = self.get("cluster_backend")
        db_type = self.get("db_type")
        self._check_version_supplied(backend)
        self._check_per_backend_required_values(backend)
        if backend in ("aws", "gce") and db_type != "cloud_scylla" and (self.get("simulated_regions") or 0) < 2:
            self._check_multi_region_params(backend)
        if backend == "docker":
            self._validate_docker_backend_parameters()
        if backend == "xcloud":
            self._validate_cloud_backend_parameters()
        self._verify_data_volume_configuration(backend)
        self._verify_cs_safepoint_logging(backend)

        if self.get("n_db_nodes"):
            self._validate_seeds_number()
            self._validate_nemesis_can_run_on_non_seed()
            self._validate_number_of_db_nodes_divides_by_az_number()

        self._validate_nemesis_parallel_config()

        if self.get("use_zero_nodes"):
            self._validate_zero_token_backend_support(backend)

        self._check_partition_range_with_data_validation_correctness()
        self._verify_scylla_bench_mode_and_workload_parameters()

        self._validate_placement_group_required_values()
        self._instance_type_validation()

        if (teardown_validators := self.get("teardown_validators.rackaware")) and teardown_validators.get(
            "enabled", False
        ):
            self._verify_rackaware_configuration()

        if backtrace_decoding_disable_regex := self.get("backtrace_decoding_disable_regex"):
            re.compile(backtrace_decoding_disable_regex)

        self._validate_perf_gradual_throttle_steps()

        self._verify_migrator_source_params()
        self._verify_emr_spark_mode()

        if (nvme_test_type := self.get("nvme_self_test_type")) not in (1, 2):
            raise ValueError(f"nvme_self_test_type must be 1 (short) or 2 (extended), got {nvme_test_type!r}")

    def _get_normalized_arch(self, instance_type: str, region_name: str, default: str = "x86_64") -> str:
        """Detect architecture from AWS instance type and normalize to Scylla package naming.

        Args:
            instance_type: AWS instance type (e.g. "i4i.large", "im4gn.xlarge").
            region_name: AWS region to query.
            default: Fallback architecture when detection fails.

        Returns:
            Normalized architecture string: "x86_64" or "aarch64".
        """
        try:
            arch = get_arch_from_instance_type(instance_type, region_name=region_name)
            # Normalize AWS arch naming ("arm64") to Scylla package naming ("aarch64")
            if arch == "arm64":
                arch = "aarch64"
            return arch
        except Exception:  # noqa: BLE001
            self.log.warning(
                "Could not detect architecture from instance type '%s' in %s, defaulting to '%s'",
                instance_type,
                region_name,
                default,
            )
            return default

    # perf_gradual_throttle_steps dict-entry fields: (key, is_valid, description-for-error-message)
    _THROTTLE_STEP_FIELD_CHECKS: ClassVar[tuple] = (
        ("threads", lambda v: isinstance(v, int) and v > 0, "a positive integer"),
        ("concurrency", lambda v: isinstance(v, int) and v > 0, "a positive integer"),
        ("rate", lambda v: isinstance(v, str), "a string"),
        ("duration", lambda v: isinstance(v, str) and v, "a non-empty string"),
        ("wait_no_compactions", lambda v: isinstance(v, bool), "a boolean"),
    )

    @staticmethod
    def _validate_throttle_step_dict(workload: str, step_idx: int, step: dict) -> None:
        """Validate a single dict-format perf_gradual_throttle_steps entry."""
        if not step:
            raise ValueError(
                f"perf_gradual_throttle_steps for {workload} step {step_idx}: "
                f"dict must have at least one key (threads, concurrency, or rate)"
            )
        for key, is_valid, description in SCTConfiguration._THROTTLE_STEP_FIELD_CHECKS:
            if key in step and not is_valid(step[key]):
                raise ValueError(
                    f"perf_gradual_throttle_steps for {workload} step {step_idx}: "
                    f"'{key}' must be {description}, got {step[key]!r}"
                )

    def _image_cloud(self) -> str | None:
        backend = self.get("cluster_backend")
        if backend == "xcloud":
            provider = self.get("xcloud_provider")
            return provider if provider in _LOADER_IMAGE_PARAMS else None
        return _BACKEND_TO_IMAGE_CLOUD.get(backend)

    def _resolve_loader_image_arch(self) -> None:
        cloud = self._image_cloud()
        if not cloud:
            return
        image_param, instance_param, region_param = _LOADER_IMAGE_PARAMS[cloud]
        template = self.get(image_param) or ""
        if not any(marker in template for marker in _ARCH_IMAGE_MARKERS):
            return
        regions = self.get(region_param) or []
        if isinstance(regions, str):
            regions = regions.split()
        instance_type = self.get(instance_param) or ""
        loader_arch = self._get_loader_arch(cloud, instance_type, (regions or [""])[0])
        if loader_arch is None:
            if is_arm_instance_type(cloud, instance_type):
                raise ValueError(
                    f"Cannot resolve {image_param}: architecture of Arm {cloud} loader instance type "
                    f"'{instance_type}' is unknown, so an amd64 image would be selected. "
                    f"Set sizing_loader.arch or add '{instance_type}' to the {cloud} instance catalog."
                )
            self.log.warning(
                "Could not detect architecture for %s instance type '%s', defaulting to x86_64", cloud, instance_type
            )
            loader_arch = "x86_64"
        elif loader_arch == "x86_64" and is_arm_instance_type(cloud, instance_type):
            raise ValueError(
                f"Cannot resolve {image_param}: {cloud} loader instance type '{instance_type}' is Arm "
                f"but its architecture resolved to x86_64."
            )
        resolved = substitute_arch_markers(template, loader_arch)
        if remaining := [marker for marker in _ARCH_IMAGE_MARKERS if marker in resolved]:
            raise ValueError(f"Cannot resolve {image_param}: markers {remaining} survived in {resolved!r}")
        self.log.info("Resolved %s for arch=%s: %s", image_param, loader_arch, resolved)
        self[image_param] = resolved

    def _stress_commands(self, env: dict | None = None) -> list[str]:
        commands: list[str] = []
        pending = [(env.get(param) if env and param in env else self.get(param)) for param in self.stress_cmd_params]
        while pending:
            value = pending.pop()
            if isinstance(value, str):
                commands.append(value)
            elif isinstance(value, (list, tuple)):
                pending.extend(value)
        return commands

    def _amd64_only_stress_tools(self, env: dict | None = None) -> list[str]:
        commands = self._stress_commands(env)
        tools = [
            tool
            for tool, markers in _AMD64_ONLY_STRESS_TOOLS.items()
            if any(marker in command for command in commands for marker in markers)
        ]
        dns_routing = (
            env.get("alternator_use_dns_routing")
            if env and "alternator_use_dns_routing" in env
            else self.get("alternator_use_dns_routing")
        )
        if dns_routing and any(_YCSB_COMMAND_MARKER in command for command in commands):
            tools.append("alternator-dns")
        return sorted(tools)

    def _constrain_loader_arch_to_stress_tools(self, env: dict | None = None) -> None:
        if self._sizing_role_arch("loader") or (
            env and isinstance(env.get("sizing_loader"), dict) and env["sizing_loader"].get("arch")
        ):
            return
        if not (tools := self._amd64_only_stress_tools(env)):
            return
        env_sizing_loader = env.get("sizing_loader") if env else None
        sizing_loader = dict(
            env_sizing_loader if isinstance(env_sizing_loader, dict) else (self.get("sizing_loader") or {})
        )
        if not sizing_loader:
            return
        sizing_loader["arch"] = "x86_64"
        if isinstance(env_sizing_loader, dict):
            env["sizing_loader"] = sizing_loader
        self["sizing_loader"] = sizing_loader
        self.log.info("Constraining loaders to x86_64, amd64-only stress tool images in use: %s", ", ".join(tools))

    def _validate_loader_arch_supports_stress_tools(self) -> None:
        if not (tools := self._amd64_only_stress_tools()):
            return
        cloud = backend_to_cloud(self.get("cluster_backend"), self.get("xcloud_provider"))
        if not cloud:
            return
        instance_param = _SIZING_ROLE_PARAMS.get(cloud, {}).get("loader")
        instance_type = (self.get(instance_param) or "") if instance_param else ""
        if instance_type and is_arm_instance_type(cloud, instance_type):
            raise ValueError(
                f"Loader instance type '{instance_type}' is Arm, but these stress tools only publish "
                f"linux/amd64 images: {', '.join(tools)}. Use an x86_64 loader instance type, "
                f"or set sizing_loader with vcpu/memory constraints so the arch is chosen for you."
            )

    def _sizing_role_arch(self, role: str) -> str | None:
        requested_arch = (self.get(f"sizing_{role}") or {}).get("arch")
        if requested_arch:
            return ARCH_ALIASES.get(str(requested_arch).strip().lower(), requested_arch)
        return None

    def _aws_instance_arch(self, instance_type: str, region_name: str) -> str | None:
        try:
            return get_arch_from_instance_type(instance_type, region_name=region_name)
        except Exception:  # noqa: BLE001
            self.log.warning(
                "Could not detect architecture for aws instance type '%s' in %s",
                instance_type,
                region_name,
            )
            return None

    def _get_loader_arch(self, cloud: str, instance_type: str, region_name: str) -> str | None:
        if instance_type:
            catalog_dir = pathlib.Path(__file__).parent.parent / "data" / "instance_catalog"
            try:
                if instance_info := InstanceCatalog.from_directory(catalog_dir).get_instance(cloud, instance_type):
                    return instance_info.arch
            except (FileNotFoundError, ValueError) as exc:
                self.log.warning("Could not load instance catalog for %s arch lookup: %s", cloud, exc)
            if cloud == "aws" and (aws_arch := self._aws_instance_arch(instance_type, region_name)):
                return aws_arch
        if sizing_resolved_arch := getattr(self, "_sizing_resolved_arch", {}).get("loader"):
            return sizing_resolved_arch
        return self._sizing_role_arch("loader")

    def _validate_perf_gradual_throttle_steps(self):
        """Validate perf_gradual_throttle_steps configuration parameter."""
        if not (performance_throughput_params := self.get("perf_gradual_throttle_steps")):
            return

        for workload, params in performance_throughput_params.items():
            if not isinstance(params, list):
                raise ValueError(f"perf_gradual_throttle_steps for {workload} should be a list")

            # Validate each step - can be string, int (backward compatible), or dict (new format)
            # Convert integers to strings for backward compatibility
            for step_idx, step in enumerate(params):
                if isinstance(step, int):
                    # Integer format - convert to string for backward compatibility
                    params[step_idx] = str(step)
                elif isinstance(step, str):
                    # String format for backward compatibility (cassandra-stress)
                    continue
                elif isinstance(step, dict):
                    self._validate_throttle_step_dict(workload, step_idx, step)
                else:
                    raise ValueError(
                        f"perf_gradual_throttle_steps for {workload} step {step_idx}: "
                        f"each step must be a string, int, or dict, got {type(step).__name__}"
                    )

            # Validate perf_gradual_threads if using string format or if dict steps don't have threads
            has_dict_steps = any(isinstance(step, dict) for step in params)
            all_dict_steps_have_threads = all(
                isinstance(step, dict) and "threads" in step for step in params if isinstance(step, dict)
            )

            # Only require perf_gradual_threads if using string format or dict without threads
            if not has_dict_steps or not all_dict_steps_have_threads:
                if not (gradual_threads := self.get("perf_gradual_threads")):
                    raise ValueError(
                        "perf_gradual_threads should be defined when using string format "
                        "or when dict steps don't specify threads"
                    )

                if workload not in gradual_threads:
                    raise ValueError(
                        f"Gradual threads for '{workload}' test is not defined in 'perf_gradual_threads' parameter"
                    )

                if not isinstance(gradual_threads[workload], list | int):
                    raise ValueError(f"perf_gradual_threads for {workload} should be a list or integer")

                if isinstance(gradual_threads[workload], int):
                    gradual_threads[workload] = [gradual_threads[workload]]

                for thread_count in gradual_threads[workload]:
                    if not isinstance(thread_count, int):
                        raise ValueError(
                            f"Invalid thread count type for '{workload}': {thread_count} "
                            f"(type: {type(thread_count).__name__})"
                        )

                # The value of perf_gradual_threads[load] must be either:
                #   - a single-element list (applied to all throttle steps) or integer
                #   - a list with the same length as perf_gradual_throttle_steps[workload] (one thread count per step).
                if len(gradual_threads[workload]) > 1 and len(gradual_threads[workload]) != len(params):
                    raise ValueError(
                        f"perf_gradual_threads for {workload} should be a single-element, integer or list, "
                        f"or a list with the same length as perf_gradual_throttle_steps for {workload}"
                    )

    def _validate_docker_simulated_racks(self) -> None:
        """Reject `simulated_racks` on Docker images that predate the --dc/--rack entrypoint arguments.

        On Docker a rack is injected as a `--dc`/`--rack` entrypoint argument at container creation.
        Scylla 2026.1 is the first release whose entrypoint accepts them and writes
        `cassandra-rackdc.properties` before the first boot; older images forward the arguments
        verbatim to the Scylla binary, which rejects them and exits, so the container never comes up.

        The configuration is unrunnable, so fail here rather than at container creation: an error
        naming the version requirement beats a container that dies on startup with no explanation.
        A Docker test-case that must run on older images sets `simulated_racks: 1` itself.
        """
        scylla_version = self.get("scylla_version") or ""
        try:
            image_version = ComparableScyllaVersion(scylla_version)
        except ValueError:
            # Branched versions ('master:latest' and friends) are not comparable and are always new enough.
            self.log.debug(
                "Cannot determine whether Scylla docker image '%s' supports the --rack entrypoint argument "
                "(scylla_version=%r); assuming it does.",
                self.get("docker_image"),
                scylla_version,
            )
            return
        if image_version < DOCKER_RACK_ARG_MIN_VERSION:
            raise ValueError(
                f"simulated_racks={self.get('simulated_racks')} is not supported on Scylla {scylla_version} "
                f"(docker backend): the --dc/--rack entrypoint arguments were added in "
                f"{DOCKER_RACK_ARG_MIN_VERSION}. Use a 2026.1+ image or set simulated_racks: 1."
            )

    def _replace_docker_image_latest_tag(self):
        docker_repo = self.get("docker_image")
        scylla_version = self.get("scylla_version")

        if scylla_version == "latest":
            result = get_specific_tag_of_docker_image(docker_repo=docker_repo)
            if result == "latest":
                raise ValueError(
                    "scylla-operator expects semver-like tags for Scylla docker images. 'latest' should not be used."
                )
            self["scylla_version"] = result

    def _resolve_xcloud_version_tag(self, version_tag: str) -> None:
        """
        Resolve version tags for xcloud backend.

        Resolves version tag given in the format <tag_type>:<tag_value> to actual Scylla Cloud release.
        For example: 'release:latest', is to be resolved into latest Scylla release supported by Scylla Cloud.
        """
        tag_type, tag_value = version_tag.split(":", 1)
        if tag_type == "release":
            if tag_value == "latest":
                cloud_api_client = ScyllaCloudAPIClient(
                    api_url=self.cloud_env_credentials["base_url"],
                    auth_token=self.cloud_env_credentials["api_token"],
                    raise_for_status=True,
                )

                self["scylla_version"] = cloud_api_client.current_scylla_version["version"]
                self.log.debug("Resolved xcloud version tag '%s' to '%s'", version_tag, self["scylla_version"])
        else:
            # TODO: support for non-release tag type will be added after Scylla Cloud supports deploying dev versions
            pass

    def _get_target_upgrade_version(self):
        # 10) update target_upgrade_version automatically
        if new_scylla_repo := self.get("new_scylla_repo"):
            if not self.get("target_upgrade_version"):
                self["target_upgrade_version"] = get_branch_version(new_scylla_repo)
            scylla_version = get_branch_version(new_scylla_repo, full_version=True)
            self.scylla_version_upgrade_target = scylla_version
            self.update_argus_with_version(scylla_version, "scylla-server-upgrade-target")

    def _check_unexpected_sct_variables(self):
        # check if there are SCT_* environment variable which aren't documented
        config_keys = {
            f"SCT_{field_name.upper()}"
            for field_name, field in self.__class__.model_fields.items()
            if not is_ignored_field(field)
        }
        # Truncate at the first "." or "__" (whichever appears first) so nested
        # SCT_<FIELD>.sub / SCT_<FIELD>__sub forms resolve to their parent field.
        # No config field name contains "__", so a global split is safe here.
        env_keys = {o.split(".")[0].split("__")[0] for o in os.environ if o.startswith("SCT_")}
        unknown_env_keys = env_keys.difference(config_keys)
        if unknown_env_keys:
            output = [f"{key}={os.environ.get(key)}" for key in unknown_env_keys]
            raise ValueError("Unsupported environment variables were used:\n\t - {}".format("\n\t - ".join(output)))

    def _validate_sct_variable_values(self):
        for field_name, field in self.__class__.model_fields.items():
            if is_ignored_field(field):
                continue
            if field_name in self and field.json_schema_extra:
                self._validate_value(field_name, field)

    def _check_multi_region_params(self, backend):
        region_param_names = {"aws": "region_name", "gce": "gce_datacenter"}
        current_region_param_name = region_param_names[backend]
        multi_region_params = list(self.multi_region_params)
        # For Cassandra db_type, check ami_id_db_cassandra instead of ami_id_db_scylla
        if self.get("db_type") in ("cassandra", "mixed_cassandra") and "ami_id_db_scylla" in multi_region_params:
            multi_region_params[multi_region_params.index("ami_id_db_scylla")] = "ami_id_db_cassandra"
        region_count = {}
        for opt in multi_region_params:
            val = self.get(opt)
            if isinstance(val, str):
                region_count[opt] = len(self.get(opt).split())
            elif isinstance(val, list):
                region_count[opt] = len(val)
            else:
                region_count[opt] = 1
        if not all(region_count[current_region_param_name] == x for x in region_count.values()):
            raise ValueError(f"not all multi region values are equal: \n\t{region_count}")

    def _validate_seeds_number(self):
        seeds_num = self.get("seeds_num")
        assert seeds_num > 0, "Seed number should be at least one"

        num_of_db_nodes = sum(
            self.get("n_db_nodes") if isinstance(self.get("n_db_nodes"), list) else [self.get("n_db_nodes")]
        )
        assert not num_of_db_nodes or seeds_num <= num_of_db_nodes, (
            f"Seeds number ({seeds_num}) should be not more then nodes number ({num_of_db_nodes})"
        )

    def _validate_nemesis_can_run_on_non_seed(self) -> None:
        if self.get("nemesis_filter_seeds") is False or self.get("nemesis_class_name") == ["NoOpMonkey"]:
            return
        seeds_num = self.get("seeds_num")
        num_of_db_nodes = sum(self.get("n_db_nodes")) + self.get("add_node_cnt")
        assert num_of_db_nodes > seeds_num, (
            "Nemesis cannot run when 'nemesis_filter_seeds' is true and seeds number is equal to nodes number"
        )

    def _validate_nemesis_parallel_config(self) -> None:
        """Validate that nemesis_selector and nemesis_seed list lengths match nemesis_class_name."""
        class_names = self.get("nemesis_class_name")
        if not class_names:
            return
        num_threads = len(class_names)

        selectors = self.get("nemesis_selector")
        if selectors and len(selectors) > 1 and len(selectors) != num_threads:
            raise ValueError(
                f"'nemesis_selector' has {len(selectors)} entries but 'nemesis_class_name' has "
                f"{num_threads}. Either use a single selector (broadcast to all threads) or provide "
                f"exactly one selector per class name.\n"
                f"  nemesis_class_name: {class_names}\n"
                f"  nemesis_selector:   {selectors}"
            )

        seeds = self.get("nemesis_seed")
        if seeds is not None:
            if len(seeds) > 1 and len(seeds) != num_threads:
                raise ValueError(
                    f"'nemesis_seed' has {len(seeds)} entries but 'nemesis_class_name' has "
                    f"{num_threads}. Either use a single seed (broadcast to all threads) or provide "
                    f"exactly one seed per class name.\n"
                    f"  nemesis_class_name: {class_names}\n"
                    f"  nemesis_seed:       {seeds}"
                )

    def _validate_number_of_db_nodes_divides_by_az_number(self):
        if self.get("cluster_backend").startswith("k8s"):
            return
        az_count = len(self.get("availability_zone").split(",")) if self.get("availability_zone") else 1
        for nodes_num in (
            self.get("n_db_nodes") if isinstance(self.get("n_db_nodes"), list) else [self.get("n_db_nodes")]
        ):
            assert nodes_num % az_count == 0, (
                f"Number of db nodes ({nodes_num}) should be divisible by number of availability zones ({az_count})"
            )

    def _validate_placement_group_required_values(self):
        if self.get("use_placement_group"):
            az_count = len(self.get("availability_zone").split(",")) if self.get("availability_zone") else 1
            backend = self.get("cluster_backend")
            if backend == "azure":
                azure_regions = self.get("azure_region_name")
                regions_count = len(azure_regions) if isinstance(azure_regions, list) else 1
            elif backend == "gce":
                regions_count = len(self.gce_datacenters)
            else:
                regions_count = len(self.region_names)
            assert az_count == 1 and regions_count == 1, (
                f"Number of Regions({regions_count}) and AZ({az_count}) should be 1 "
                f"when param use_placement_group is used"
            )

    def _validate_scylla_d_overrides_files_exists(self):
        if scylla_d_overrides_files := self.get("scylla_d_overrides_files"):
            for config_file_path in scylla_d_overrides_files:
                if config_file_path.startswith("scylla-qa-internal"):
                    continue
                config_file = pathlib.Path(get_sct_root_path()) / config_file_path
                assert config_file.exists(), f"{config_file} doesn't exists, please check your configuration"

    def _check_per_backend_required_values(self, backend: str):
        if backend in available_backends:
            if backend in ("aws", "gce") and self.get("db_type") == "cloud_scylla":
                backend += "-siren"
            if backend == "xcloud":
                cloud_cluster_type = "xcloud" if self.get("xcloud_scaling_config") else "standard"
                self.backend_required_params[backend] += self.xcloud_per_provider_required_params[cloud_cluster_type][
                    self.get("xcloud_provider")
                ]
            if backend == "aws" and self.get("n_vector_store_nodes") > 0:
                self.backend_required_params["aws"].extend(
                    ["ami_id_vector_store", "instance_type_vector_store", "ami_vector_store_user"]
                )
            self._check_backend_defaults(backend, self.backend_required_params[backend])
        else:
            raise ValueError(f"Unsupported backend [{backend}]")

    def _check_backend_defaults(self, backend, required_params):
        fields = [
            field_name
            for field_name, field in self.__class__.model_fields.items()
            if field_name in required_params and not is_ignored_field(field)
        ]
        for field_name in fields:
            value = self.get(field_name)
            assert value is not None and not (isinstance(value, str) and not value.strip()), (
                f"{field_name} missing from config for {backend}"
            )

    def _instance_type_validation(self):
        backend = self.get("cluster_backend")

        # Validate main instance types (db, loader, monitor) are available in the target region
        if backend == "aws":
            instance_type_params = [
                "instance_type_db",
                "instance_type_loader",
                "instance_type_monitor",
                "instance_type_db_target",
            ]
            if self.get("db_type") in ("mixed_scylla", "mixed_cassandra"):
                instance_type_params.append("instance_type_db_oracle")
            for param_name in instance_type_params:
                if instance_type := self.get(param_name):
                    for region in self.region_names:
                        assert aws_check_instance_type_supported(instance_type, region), (
                            f"Instance type '{instance_type}' (param: {param_name}) "
                            f"is not supported in region '{region}'"
                        )

        # Validate nemesis_grow_shrink_instance_type
        if instance_type := self.get("nemesis_grow_shrink_instance_type"):
            match backend:
                case "aws":
                    for region in self.region_names:
                        assert aws_check_instance_type_supported(instance_type, region), (
                            f"Instance type[{instance_type}] not supported in region [{region}]"
                        )
                case "gce":
                    machine_types_client, info = get_gce_compute_machine_types_client()
                    regions_client, _ = get_gce_compute_regions_client()
                    for datacenter in self.gce_datacenters:
                        region_info = regions_client.get(project=info["project_id"], region=datacenter)
                        zones = [z.rsplit("/", 1)[-1] for z in region_info.zones]
                        for _zone in zones:
                            assert gce_check_if_machine_type_supported(
                                machine_types_client, instance_type, project=info["project_id"], zone=_zone
                            ), f"Instance type[{instance_type}] not supported in zone [{_zone}]"
                case "azure":
                    if azure_region_names := self.get("azure_region_name"):
                        for region in azure_region_names:
                            assert azure_check_instance_type_available(instance_type, region), (
                                f"Instance type [{instance_type}] not supported in region [{region}]"
                            )
                case "oci":
                    if oci_region_names := self.get("oci_region_name"):
                        if not isinstance(oci_region_names, list):
                            oci_region_names = [oci_region_names]
                        for region in oci_region_names:
                            assert oci_utils.is_shape_available(instance_type, region), (
                                f"Instance type [{instance_type}] not supported in region [{region}]"
                            )
                case _:
                    raise ValueError(f"Unsupported backend [{backend}] for using nemesis_grow_shrink_instance_type")

    def _check_version_supplied(self, backend: str):
        options_must_exist = []

        if (
            self.get("db_type") not in ("cassandra",)
            and not self.get("use_preinstalled_scylla")
            and not backend == "baremetal"
            and not self.get("unified_package")
        ):
            options_must_exist += ["scylla_repo"]

        # When unified_package is set, backend-specific image is not required since
        # Scylla will be installed from the unified package on top of a base OS image.
        if self.get("unified_package"):
            pass
        elif self.get("db_type") == "cloud_scylla":
            options_must_exist += ["cloud_cluster_id"]
        elif backend == "aws":
            if self.get("db_type") in ("cassandra", "mixed_cassandra"):
                options_must_exist += ["ami_id_db_cassandra"]
            else:
                options_must_exist += ["ami_id_db_scylla"]
        elif backend == "gce":
            options_must_exist += ["gce_image_db"]
        elif backend == "azure":
            options_must_exist += ["azure_image_db"]
        elif backend == "oci":
            options_must_exist += ["oci_image_db"]
        elif backend == "docker":
            if self.get("db_type") == "cassandra":
                options_must_exist += ["docker_image_cassandra"]
            else:
                options_must_exist += ["docker_image"]
        elif "k8s" in backend or backend == "xcloud":
            options_must_exist += ["scylla_version"]

        if not options_must_exist:
            return
        missing_options = [o for o in options_must_exist if not self.get(o)]
        assert not missing_options, (
            "scylla version/repos wasn't configured correctly\n"
            f"missing options: {missing_options}\n"
            f"set those environment variables: {['SCT_' + o.upper() for o in missing_options]}"
        )

    def _check_partition_range_with_data_validation_correctness(self):
        if data_validation := self.get("data_validation"):
            data_validation_params = yaml.safe_load(data_validation)

            partition_range_with_data_validation = data_validation_params.get("partition_range_with_data_validation")
            if partition_range_with_data_validation:
                error_message_template = (
                    "Expected format of 'partition_range_with_data_validation' parameter is: "
                    "<min PK value>-<max PK value>. {}Example: 0-250. "
                    "Got value: %s" % partition_range_with_data_validation
                )

                if "-" not in partition_range_with_data_validation:
                    raise ValueError(error_message_template.format(""))

                partition_range_splitted = partition_range_with_data_validation.split("-")

                if not (partition_range_splitted[0].isdigit() and partition_range_splitted[1].isdigit()):
                    raise ValueError(error_message_template.format("PK values should be integer. "))

                if int(partition_range_splitted[1]) < int(partition_range_splitted[0]):
                    raise ValueError(
                        error_message_template.format("<max PK value> should be bigger then <min PK value>. ")
                    )

    @staticmethod
    def _validate_zero_token_backend_support(backend: str):
        assert backend == "aws", "Only AWS supports zero nodes configuration"

    def verify_configuration_urls_validity(self):  # noqa: PLR0914
        """
        Check if ami_id and repo urls are valid
        """
        backend = self.get("cluster_backend")
        if backend in ("k8s-eks", "k8s-gke"):
            return

        self._get_target_upgrade_version()

        # verify that the AMIs used all have 'user_data_format_version' tag
        if backend == "aws":
            ami_id_db_scylla = self.get("ami_id_db_scylla").split()
            region_names = self.region_names
            ami_id_db_oracle = self.get("ami_id_db_oracle").split()
            for key_to_update, ami_list in [
                ("user_data_format_version", ami_id_db_scylla),
                ("oracle_user_data_format_version", ami_id_db_oracle),
            ]:
                if ami_list:
                    user_data_format_versions = set()
                    self[key_to_update] = "3"
                    for ami_id, region_name in zip(ami_list, region_names):
                        if not ami_built_by_scylla(ami_id, region_name):
                            continue
                        tags = get_ami_tags(ami_id, region_name)
                        assert "user_data_format_version" in tags.keys(), (
                            f"\n\t'user_data_format_version' tag missing from [{ami_id}] on {region_name}\n\texisting "
                            f"tags: {tags}"
                        )
                        user_data_format_versions.add(tags["user_data_format_version"])
                    assert len(user_data_format_versions) <= 1, (
                        f"shouldn't have mixed versions {user_data_format_versions}"
                    )
                    if user_data_format_versions:
                        self[key_to_update] = list(user_data_format_versions)[0]

        if backend == "gce":
            gce_image_db = self.get("gce_image_db").split()
            for image in gce_image_db:
                tags = get_gce_image_tags(image)
                if "user_data_format_version" not in tags.keys():
                    # since older release aren't tagged, we default to 2 which was the version on the first gce images
                    LOGGER.warning("'user_data_format_version' tag missing from [%s]: existing tags: %s", image, tags)
                self["user_data_format_version"] = tags.get("user_data_format_version", "2")

        if backend == "azure":
            azure_image_db = self.get("azure_image_db").split()
            for image in azure_image_db:
                tags = azure_utils.get_image_tags(image)
                if "user_data_format_version" not in tags.keys():
                    # since older release aren't tagged, we default to 2 which was the version on the first gce images
                    LOGGER.warning("'user_data_format_version' tag missing from [%s]: existing tags: %s", image, tags)
                self["user_data_format_version"] = tags.get("user_data_format_version", "2")

        if backend == "oci":
            oci_image_db = self.get("oci_image_db")
            if oci_image_db.startswith("resolve:platform:"):
                # NOTE: Resolve platform image to actual OCID
                parts = oci_image_db.replace("resolve:platform:", "").split(":")
                os_name = parts[0]
                os_version = parts[1] if len(parts) > 1 else "n/a"
                oci_region_names = self.get("oci_region_name") or []
                if not isinstance(oci_region_names, list):
                    oci_region_names = [oci_region_names]
                shape_name = (self.get("oci_instance_type_db") or "").split(":")[0] or None
                resolved_images = []
                for region in oci_region_names:
                    resolved_images.append(
                        oci_utils.get_platform_image_ocid(
                            compartment_id=oci_utils.get_oci_compartment_id(),
                            region=region,
                            operating_system=os_name,
                            version=os_version,
                            shape=shape_name,
                        )
                    )
                self["oci_image_db"] = " ".join(resolved_images)
                # NOTE: use default format version because platform images don't have scylla tags
                self["user_data_format_version"] = "3"
            else:
                oci_region_names = self.get("oci_region_name") or []
                if not isinstance(oci_region_names, list):
                    oci_region_names = [oci_region_names]
                for image, region in zip(oci_image_db.split(), oci_region_names):
                    tags = oci_utils.get_image_tags(region, image, "scylla")
                    if "user_data_format_version" not in tags.keys():
                        LOGGER.warning(
                            "'user_data_format_version' tag missing from [%s]: existing tags: %s", image, tags
                        )
                    self["user_data_format_version"] = tags.get("user_data_format_version", "3")

        # For each Scylla repo file we will check that there is at least one valid URL through which to download a
        # version of SCYLLA, otherwise we will get an error.
        repos_to_validate = []
        if backend in ("aws", "gce", "baremetal"):
            repos_to_validate.extend(
                [
                    "scylla_repo_m",
                    "scylla_mgmt_address",
                    "scylla_mgmt_agent_address",
                ]
            )
        get_branch_version_for_multiple_repositories(urls=(self.get(url) for url in repos_to_validate if self.get(url)))

    def get_version_based_on_conf(self):
        """
        figure out which version and if it's enterprise version
        base on configuration only, before nodes are up and running
        so test configuration can set up things which need to happen
        before nodes are up

        this is information is cached on the SCTConfiguration object
        :return: tuple - (scylla_version, is_enterprise)
        """
        backend = self.get("cluster_backend")
        scylla_version = None
        _is_enterprise = False

        if self.get("db_type") in ("cassandra",):
            return scylla_version, _is_enterprise

        if unified_package := self.get("unified_package"):
            with tempfile.TemporaryDirectory() as tmpdirname:
                try:
                    LOCALRUNNER.run(
                        shell_script_cmd(f"""
                        cd {tmpdirname}
                        {curl_with_retry(unified_package, output="./unified_package.tar.gz", follow_redirects=True, fail_early=True)}
                        tar xvfz ./unified_package.tar.gz
                        """),
                        verbose=False,
                    )
                except Exception as exc:
                    raise FileNotFoundError(
                        f"Unified package not found or failed to download: {unified_package}. "
                        f"The URL may not exist or is not accessible."
                    ) from exc

                scylla_version = next(pathlib.Path(tmpdirname).glob("**/SCYLLA-VERSION-FILE")).read_text()
                scylla_product = next(pathlib.Path(tmpdirname).glob("**/SCYLLA-PRODUCT-FILE")).read_text()
                _is_enterprise = scylla_product == "scylla-enterprise"
        elif not self.get("use_preinstalled_scylla"):
            scylla_repo = self.get("scylla_repo")
            scylla_version = get_branch_version(scylla_repo, full_version=True)
            _is_enterprise = is_enterprise(scylla_version)
        elif self.get("db_type") == "cloud_scylla":
            _is_enterprise = True
        elif backend == "aws":
            amis = self.get("ami_id_db_scylla").split()
            region_name = self.region_names[0]
            tags = get_ami_tags(ami_id=amis[0], region_name=region_name)
            scylla_version = self._require_scylla_version_tag(
                tags=tags,
                resource_label="AMI",
                resource_id=amis[0],
                tag_keys=("scylla_version", "ScyllaVersion"),
                region_name=region_name,
                resource_type="AMI",
                resource_id_label="AMI ID",
            )
            _is_enterprise = is_enterprise(scylla_version)
        elif backend == "gce":
            images = self.get("gce_image_db").split()
            tags = get_gce_image_tags(images[0])
            scylla_version = self._require_scylla_version_tag(
                tags=tags,
                resource_label="GCE image",
                resource_id=images[0],
                tag_keys=("scylla_version",),
                resource_type="image",
                resource_id_label="image name",
            ).replace("-", ".")
            _is_enterprise = is_enterprise(scylla_version)
        elif backend == "azure":
            images = self.get("azure_image_db").split()
            tags = azure_utils.get_image_tags(images[0])
            scylla_version = self._require_scylla_version_tag(
                tags=tags,
                resource_label="Azure image",
                resource_id=images[0],
                tag_keys=("scylla_version",),
                resource_type="image",
                resource_id_label="image name",
            )
            _is_enterprise = is_enterprise(scylla_version)
        elif backend == "oci":
            images = self.get("oci_image_db").split()
            oci_region_names = self.get("oci_region_name") or []
            if not isinstance(oci_region_names, list):
                oci_region_names = [oci_region_names]
            tags = oci_utils.get_image_tags(oci_region_names[0], images[0], "scylla")
            scylla_version = self._require_scylla_version_tag(
                tags=tags,
                resource_label="Oracle image",
                resource_id=images[0],
                tag_keys=("scylla_version",),
                resource_type="image",
                resource_id_label="image name",
            )
            _is_enterprise = True
        elif "k8s" in backend:
            scylla_version = self.get("scylla_version")
            _is_enterprise = is_enterprise(scylla_version)
        elif backend == "docker":
            docker_repo = self.get("docker_image")
            scylla_version = self.get("scylla_version")
            _is_enterprise = "enterprise" in docker_repo
        elif backend == "xcloud":
            scylla_version = self.get("scylla_version")
            _is_enterprise = is_enterprise(scylla_version)
        self.artifact_scylla_version = scylla_version
        self.is_enterprise = _is_enterprise
        self.update_argus_with_version(scylla_version, "scylla-server-target")

        return scylla_version, _is_enterprise

    @staticmethod
    def _require_scylla_version_tag(
        *,
        tags: dict,
        resource_label: str,
        resource_id: str,
        tag_keys: tuple[str, ...],
        resource_type: str,
        resource_id_label: str,
        region_name: str | None = None,
    ) -> str:
        for key in tag_keys:
            value = tags.get(key)
            if value:
                return value
        tag_list = " or ".join(f"'{key}'" for key in tag_keys)
        location = f" in region '{region_name}'" if region_name else ""
        raise ValueError(
            f"{resource_label} '{resource_id}'{location} does not have {tag_list} tag. "
            f"This {resource_type} may not be a valid Scylla {resource_type}. "
            f"Please check the {resource_id_label} and ensure it is tagged correctly."
        )

    def update_argus_with_version(self, scylla_version: str, package_name: str):
        try:
            version_regex = ARGUS_VERSION_RE
            if match := version_regex.match(scylla_version):
                version_info = match.groupdict()
                package = Package(
                    name=package_name,
                    date=version_info.get("date", "#NO_DATE"),
                    version=version_info["short"],
                    revision_id=version_info.get("commit", "#NO_COMMIT"),
                    build_id="#NO_BUILDID",
                )
                self.log.info("Saving upgraded Scylla version...")
                test_config = TestConfig()
                test_config.init_argus_client(params=self, test_id=self.get("reuse_cluster") or self.get("test_id"))
                test_config.argus_client().submit_packages([package])
                test_config.argus_client().update_scylla_version(version_info["short"])
        except Exception as exc:
            self.log.exception("Failed to save target Scylla version in Argus", exc_info=exc)

    def update_config_based_on_version(self):
        if self.is_enterprise and ComparableScyllaVersion(self.artifact_scylla_version) >= "2025.1.0~dev":
            if "views-with-tablets" not in self.get("experimental_features"):
                self.experimental_features.append("views-with-tablets")

    def dump_config(self):
        """
        Dump current configuration to string

        :return: str
        """
        return anyconfig.dumps(self.model_dump(exclude_none=True), ac_parser="yaml")

    @classmethod
    def get_annotations_as_strings(cls, field_type, field_metadata=None):  # noqa: PLR0911
        """Convert a type annotation to a human-readable string for configuration docs.

        Recursively resolves complex type annotations into clean, simplified
        strings.  The following annotation kinds are handled:

        * **Annotated** – The wrapper is stripped and only the underlying type
          is shown (e.g. ``Annotated[int, BeforeValidator(...)]`` → ``int``).
        * **Literal** – Rendered with the allowed values
          (e.g. ``Literal['a', 'b']``).
        * **Union** – ``NoneType`` members are removed for readability;
          remaining types are joined with ``|``.
        * **Generic** types (``list[str]``, ``dict[str, int]``, …) – The
          origin and type arguments are resolved recursively.
        * **Basic** types (``str``, ``int``, …) – Returned as-is after
          stripping internal prefixes (``typing.``, ``<class '…'>``, etc.).

        Args:
            field_type: The type annotation to convert.  Accepts simple types,
                generics, ``Union``, ``Literal``, and ``Annotated`` forms.
            field_metadata: Optional sequence of Pydantic field metadata
                objects.

        Returns:
            A cleaned-up string representation of the annotation suitable for
            display in generated Markdown documentation, e.g. ``"str"``,
            ``"list[int]"``, ``"int | str"``, ``"Literal['a', 'b']"``.
        """
        origin = get_origin(field_type)
        args = get_args(field_type)

        def clear_class(type_str):
            return (
                type_str.replace("typing.", "")
                .replace("<class '", "")
                .replace("'>", "")
                .replace("types.", "")
                .replace("UnionType", "")
            )

        # If it's an Annotated type, extract the first arg (the actual type)
        # This handles cases like Annotated[int | list[int], BeforeValidator(...)]
        if origin is Annotated and args:
            # First arg is the actual type
            actual_type = args[0]
            return cls.get_annotations_as_strings(actual_type, field_metadata=None)

        # Handle Literal types - display the allowed values
        if origin is Literal and args:
            literal_values = ", ".join(repr(arg) for arg in args)
            return f"Literal[{literal_values}]"

        # Handle Union types with None - filter out None for readability
        if origin is Union and args:
            # Filter out NoneType from the union args
            non_none_args = [arg for arg in args if arg is not type(None)]
            if len(non_none_args) == 1:
                # If only one type remains after removing None, just show that type
                return cls.get_annotations_as_strings(non_none_args[0], field_metadata=None)
            elif len(non_none_args) < len(args):
                # There was a None in the union, show remaining types without None
                arg_strings = [cls.get_annotations_as_strings(arg, field_metadata=None) for arg in non_none_args]
                return " | ".join(arg_strings)
            else:
                # No None in the union, show all types
                arg_strings = [cls.get_annotations_as_strings(arg, field_metadata=None) for arg in args]
                return " | ".join(arg_strings)

        if origin:
            if args:
                # Handle generic types like list[str] - recursively process args
                arg_strings = [cls.get_annotations_as_strings(arg, field_metadata=None) for arg in args]
                type_string = f"{clear_class(str(origin))}[{', '.join(arg_strings)}]"
            else:
                type_string = clear_class(str(origin))
        else:
            # Handle basic types like str, int
            type_string = clear_class(str(field_type))

        return type_string

    @classmethod
    def dump_help_config_markdown(cls):
        """
        Dump all configuration options with their defaults and help to string in markdown format

        :return: str
        """
        header = """
            # scylla-cluster-tests configuration options

            #### Appending with environment variables or with config files
            * **strings:** can be appended with adding `++` at the beginning of the string:
                   `export SCT_APPEND_SCYLLA_ARGS="++ --overprovisioned 1"`
            * **list:** can be appended by adding `++` as the first item of the list
                   `export SCT_SCYLLA_D_OVERRIDES_FILES='["++", "extra_file/scylla.d/io.conf"]'`

            #### Nested (dict/list) options
            * A single sub-key of a dict/list option can be set on its own, without
                   quoting the whole value, using either dot-notation or double-underscore
                   notation: `SCT_STRESS_IMAGE.ycsb=...` or `SCT_STRESS_IMAGE__ycsb=...`.
            * `__` is the bash-exportable form (dots are invalid in bash variable names),
                   so prefer it with plain `export`, e.g. `export SCT_STRESS_IMAGE__ycsb=...`.
            * Sub-keys containing `-` (e.g. `cassandra-stress`) still require the dot form,
                   set via `env 'SCT_STRESS_IMAGE.cassandra-stress=...' ...`, since `-` is not
                   a valid bash identifier character either.
            * **Case matters:** the `__` form lower-cases the sub-key (e.g.
                   `SCT_INSTANCE_TYPE_DB__ARCH` becomes sub-key `arch`), while the `.` form
                   preserves case verbatim (`SCT_INSTANCE_TYPE_DB.ARCH` stays `ARCH`). This
                   matters for sub-keys consumed by case-sensitive lookups -- prefer
                   uppercase sub-keys with `__` (they'll be lowered, matching the common
                   convention) rather than the case-preserving dot form.
        """
        defaults, backend_defaults = cls._get_defaults_for_docs()

        def strip_help_text(text, preserve_indent=False):
            """
            Strip all lines, and also remove empty lines from start or end.

            If *preserve_indent* is set, dedent to the common margin and keep each
            line's *relative* indentation (so multi-line list-item continuations,
            like the ones in *header* above, stay visually nested under their
            bullet in the rendered markdown, matching this docstring's own
            indentation) instead of flattening every line flush left.
            """
            if preserve_indent:
                output = [line.rstrip() for line in dedent(text).splitlines()]
            else:
                output = [l.strip() for l in text.splitlines()]
            return "\n".join(output[1 if not output[0] else 0 : -1 if not output[-1] else None])

        ret = strip_help_text(header, preserve_indent=True)

        ret += "\n\n" + strip_help_text(
            """
            #### Options by group
            The options below are grouped by domain -- cross-cutting concerns first, then one
            section per backend, then one per test type. Each group mirrors a mixin module under
            `sdcm/sct_config/mixins/`.
        """,
            preserve_indent=True,
        )

        for group_title, group_fields in cls._fields_by_group():
            ret += f"\n\n# {group_title}\n"
            for field_name, field in group_fields:
                ret += "\n\n"
                if description := field.description:
                    help_text = "<br>".join(strip_help_text(description).splitlines())
                else:
                    help_text = ""

                appendable = " (appendable)" if is_config_option_appendable(field_name, cls) else ""
                if field_name in defaults:
                    default_text = cls._format_default_value_for_docs(defaults[field_name])
                else:
                    default_text = "N/A"
                backend_overrides = cls._get_backend_overrides_for_docs(field_name, defaults, backend_defaults)

                field_metadata = getattr(field, "metadata", None)
                input_type_meta = next((m for m in (field_metadata or []) if isinstance(m, InputType)), None)
                output_type = cls.get_annotations_as_strings(field.annotation, field_metadata=field_metadata)
                type_str = f"{input_type_meta.description} → {output_type}" if input_type_meta else output_type
                ret += dedent(f"""
                    ## **{field_name}** / SCT_{field_name.upper()}

                    {help_text}

                    **default:** {default_text}

                    **type:** {type_str}{appendable}
                    """).strip()
                if backend_overrides:
                    ret += f"\n\n**backend overrides:**\n{backend_overrides}"
                ret += "\n"
        return ret

    @classmethod
    def dump_help_config_yaml(cls):
        """
        Dump all configuration options with their defaults and help to string in yaml format

        :return: str
        """
        defaults = anyconfig.load(sct_abs_path("defaults/test_default.yaml"))
        ret = ""

        for group_title, group_fields in cls._fields_by_group():
            ret += f"### {group_title}\n\n"
            for field_name, field in group_fields:
                if description := field.description:
                    help_text = "\n".join(f"# {l.strip()}" for l in description.splitlines() if l.strip()) + "\n"
                else:
                    help_text = ""
                if field_name in defaults:
                    default = cls._format_default_value_for_docs(defaults[field_name])
                else:
                    default = "N/A"
                ret += f"{help_text}{field_name}: {default}\n\n"

        return ret

    def _verify_migrator_source_params(self):
        if self.get("migrator_source_hosts") and self.get("migrator_source_test_id"):
            raise ValueError("migrator_source_hosts and migrator_source_test_id are mutually exclusive — set only one")

    def _verify_emr_spark_mode(self):
        """Validate that EMR release label matches selected Spark provisioning mode."""
        label = self.get("emr_release_label")
        if not label:
            return

        native = not self.get("emr_install_spark4_via_bootstrap")
        is_spark_release = label.startswith("emr-spark-")
        if native and not is_spark_release:
            raise ValueError(
                f"emr_release_label={label!r} is not a native Spark 4.X release; the native path "
                "(emr_install_spark4_via_bootstrap=false) requires an 'emr-spark-*' label such as "
                "'emr-spark-8.0.0'. Set emr_install_spark4_via_bootstrap=true to use an emr-7.x release."
            )
        if not native and is_spark_release:
            self.log.warning(
                "emr_install_spark4_via_bootstrap=true installs standalone Spark 4.x via bootstrap, but "
                "release %s already ships native Spark 4 - the bootstrap is redundant.",
                label,
            )

    def _verify_data_volume_configuration(self, backend):
        dev_num = self.get("data_volume_disk_num")
        if dev_num == 0:
            return

        if backend not in ("aws", "k8s-eks", "oci"):
            raise ValueError("Data volume configuration is supported only for 'aws', 'k8s-eks' and 'oci'")

        if not self.get("data_volume_disk_size") or not self.get("data_volume_disk_type"):
            raise ValueError("Data volume configuration requires: data_volume_disk_type, data_volume_disk_size")

    def _verify_cs_safepoint_logging(self, backend):
        if not self.get("cs_safepoint_logging"):
            return

        if "k8s" in backend or self.get("use_prepared_loaders"):
            raise ValueError(
                "'cs_safepoint_logging' requires cassandra-stress to run in a docker container on the loader, "
                "it is not supported for k8s backends or with 'use_prepared_loaders'"
            )

    def _verify_scylla_bench_mode_and_workload_parameters(self):
        for param_name in self.stress_cmd_params:
            stress_cmds = self._as_list(self.get(param_name))
            if not stress_cmds:
                continue
            for stress_cmd in stress_cmds:
                if not stress_cmd:
                    continue
                cmd = stress_cmd.strip(" ")
                if not cmd.startswith("scylla-bench"):
                    continue
                if "-mode=" not in cmd:
                    raise ValueError(f"Scylla-bench command {cmd} doesn't have parameter -mode")
                if "-workload=" not in cmd:
                    raise ValueError(f"Scylla-bench command {cmd} doesn't have parameter -workload")

    def _validate_docker_backend_parameters(self):
        if self.get("use_mgmt"):
            raise ValueError("Scylla Manager is not supported for docker backend")

    def _verify_rackaware_configuration(self):
        if not self.get("rack_aware_loader"):
            raise ValueError("'rack_aware_loader' must be set to True for rackaware validator.")

        if self.get("cluster_backend") == "xcloud":
            # xcloud+gce keeps the region in gce_datacenters, not region_names
            regions = self.get("simulated_regions") or len(self.region_names or self.gce_datacenters)
            # deterministic placement is required: this is the only source of AZ layout
            racks_count = len(set(parse_availability_zones(self.get("xcloud_availability_zones"))))
            if racks_count < 2:
                raise ValueError(
                    "Rack-aware validation on the xcloud backend requires 'xcloud_availability_zones' "
                    "with at least two distinct zones."
                )
        else:
            regions = self.get("simulated_regions") or len(self.region_names)
            availability_zone, simulated_racks = self.get("availability_zone"), self.get("simulated_racks")
            racks_count = simulated_racks or (len(availability_zone.split(",")) if availability_zone else 1)

        if racks_count == 1 and regions == 1:
            raise ValueError(
                "Rack-aware validation can only be performed in multi-availability zone or multi-region environments."
            )

        loaders = sum(n_loaders if isinstance((n_loaders := self.get("n_loaders")), list) else [n_loaders])

        zones = racks_count * regions
        if loaders >= zones:
            raise ValueError("Rack-aware validation requires zones without loaders.")

    def _validate_xcloud_availability_zones(self, cloud_api_client, provider_id: int, region_id: int):
        """Validate the AZ placement knob against node count, scaling config and the target region"""
        zones = parse_availability_zones(self.get("xcloud_availability_zones"))
        if not zones:
            return
        if self.get("xcloud_scaling_config"):
            raise ValueError(
                "'xcloud_availability_zones' cannot be combined with 'xcloud_scaling_config': "
                "node count is managed by Scylla Cloud scaling."
            )
        expand_availability_zones(zones, self.get("n_db_nodes")[0])

        # zones outside the region, and zones that cannot host the DB instance type, are only rejected by Siren
        # after provisioning starts, with a generic "general error creating the cluster" error.
        # So checking them against the account real zone list here instead
        is_aws = self.get("xcloud_provider") == "aws"
        region_name = self.region_names[0] if is_aws else self.gce_datacenters[0]
        instance_type = self.get("instance_type_db" if is_aws else "gce_instance_type_db")
        instance_type_id = cloud_api_client.get_instance_id_by_name(
            cloud_provider_id=provider_id, region_id=region_id, instance_type_name=instance_type
        )
        available_zones = cloud_api_client.get_availability_zones(
            cloud_provider_id=provider_id, region_id=region_id, instance_type_id=instance_type_id
        )

        available_zone_ids = {zone["id"] for zone in available_zones}
        mismatched = [zone for zone in zones if zone not in available_zone_ids]
        if mismatched:
            available = ", ".join(f"{zone['id']} ({zone['name']})" for zone in available_zones)
            raise ValueError(
                f"'xcloud_availability_zones' {mismatched} cannot be used in region '{region_name}' with "
                f"instance type '{instance_type}'. Available zones: {available}."
            )

    def _validate_cloud_backend_parameters(self):
        cloud_api_client = ScyllaCloudAPIClient(
            api_url=self.cloud_env_credentials["base_url"],
            auth_token=self.cloud_env_credentials["api_token"],
            raise_for_status=True,
        )

        # validate if selected cloud provider is supported
        cloud_provider = self.get("xcloud_provider")
        if cloud_provider not in ["aws", "gce"]:
            raise ValueError(f"Unsupported Scylla Cloud provider: {cloud_provider}. Must be 'aws' or 'gce'")

        # validate if selected Scylla version is supported
        supported_versions = [v["version"] for v in cloud_api_client.get_scylla_versions()["scyllaVersions"]]
        if (selected_version := self.get("scylla_version")) not in supported_versions:
            raise ValueError(
                f"Selected Scylla version '{selected_version}' is not supported by cloud backend.\n"
                f"Currently supported versions: {', '.join(supported_versions)}"
            )

        # validate if selected region is supported by the cloud provider
        provider_id = cloud_api_client.cloud_provider_ids[CloudProviderType.from_sct_backend(cloud_provider)]
        supported_regions = [
            r["externalId"] for r in cloud_api_client.get_regions(cloud_provider_id=provider_id)["regions"]
        ]
        region_name = (self.region_names if cloud_provider == "aws" else self.gce_datacenters)[0]
        if region_name not in supported_regions:
            raise ValueError(
                f"Selected region '{region_name}' is not supported by cloud provider '{cloud_provider}'.\n"
                f"Supported regions for '{cloud_provider}': {', '.join(supported_regions)}"
            )

        region_id = cloud_api_client.get_region_id_by_name(cloud_provider_id=provider_id, region_name=region_name)

        # DB instance type is not provided for XCloud cluster - it's defined by Scylla Cloud based on scaling config
        if not self.get("xcloud_scaling_config"):
            # validate if instance types are supported in the selected region
            supported_instances = [
                i["externalId"]
                for i in cloud_api_client.get_instance_types(cloud_provider_id=provider_id, region_id=region_id)[
                    "instances"
                ]
            ]
            db_instance_type = self.get("instance_type_db" if cloud_provider == "aws" else "gce_instance_type_db")
            if db_instance_type not in supported_instances:
                raise ValueError(
                    f"Database instance type '{db_instance_type}' is not supported in region '{region_name}' for "
                    f"cloud provider '{cloud_provider}'.\n"
                    f"Supported instance types: {', '.join(supported_instances)}"
                )

        rf = self.get("xcloud_replication_factor")
        n_nodes: list[int] = self.get("n_db_nodes")
        if rf is None:
            self["xcloud_replication_factor"] = min(*n_nodes, 3)
        elif rf > min(n_nodes):
            raise ValueError(f"xcloud_replication_factor ({rf}) cannot be greater than n_db_nodes ({n_nodes})")

        self._validate_xcloud_availability_zones(cloud_api_client, provider_id, region_id)

        # validate Vector Search parameters for cloud backend
        # TODO: update after Vector Search moves out of Beta for Scylla Cloud and limitations are changed/no longer apply
        if int(self.get("n_vector_store_nodes")) > 0:
            scylla_version = self.get("scylla_version").split("~")[0]
            if version.parse(scylla_version) < version.parse(MIN_SCYLLA_VERSION_FOR_VS):
                raise ValueError(
                    f"Vector Search requires ScyllaDB {MIN_SCYLLA_VERSION_FOR_VS}+, "
                    f"but selected version is {scylla_version}"
                )

            vs_instance_type = self.get("instance_type_vector_store")
            supported_vs_types = cloud_api_client.get_vector_search_instance_types(
                cloud_provider_id=provider_id, region_id=region_id
            )
            if vs_instance_type not in supported_vs_types:
                raise ValueError(
                    f"Instance type '{vs_instance_type}' is not supported for Vector Search on {cloud_provider.upper()}.\n"
                    f"Supported types: {', '.join(supported_vs_types)}"
                )


def init_and_verify_sct_config() -> SCTConfiguration:
    sct_config = SCTConfiguration()
    sct_config.log_config()
    sct_config.verify_configuration()
    sct_config.verify_configuration_urls_validity()
    sct_config.get_version_based_on_conf()
    sct_config.update_config_based_on_version()
    sct_config.check_required_files()
    return sct_config
