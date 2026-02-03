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
import ast
import json
import logging
import getpass
import pathlib
import tempfile
from textwrap import dedent

import yaml
import copy
from typing import List, Union, Set, Literal, get_origin, get_args, ClassVar
from functools import cached_property

from distutils.util import strtobool
import anyconfig
from argus.client.sct.types import Package
from packaging import version
from pydantic import BaseModel, Field, ConfigDict, fields as pydantic_fields
from typing_extensions import Annotated
from pydantic.functional_validators import BeforeValidator
from pydantic.fields import FieldInfo
from sdcm import sct_abs_path
import sdcm.provision.azure.utils as azure_utils
from sdcm.cloud_api_client import ScyllaCloudAPIClient, CloudProviderType
from sdcm.keystore import KeyStore
from sdcm.utils.cloud_api_utils import get_cloud_rest_credentials_from_file
from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.dedicated_host import SCTDedicatedHosts
from sdcm.utils import alternator
from sdcm.utils.aws_utils import get_arch_from_instance_type, aws_check_instance_type_supported
from sdcm.utils.common import (
    ami_built_by_scylla,
    get_ami_tags,
    get_branched_ami,
    get_branched_gce_images,
    get_scylla_ami_versions,
    get_scylla_gce_images_versions,
    convert_name_to_ami_if_needed,
    get_sct_root_path,
    get_vector_store_ami_versions,
)
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
)
from sdcm.sct_events.base import add_severity_limit_rules, print_critical_events
from sdcm.utils.gce_utils import (
    SUPPORTED_REGIONS as GCE_SUPPORTED_REGIONS,
    get_gce_image_tags,
    get_gce_compute_machine_types_client,
    gce_check_if_machine_type_supported,
)
from sdcm.utils.azure_utils import (
    azure_check_instance_type_available,
)
from sdcm.utils.cloud_api_utils import MIN_SCYLLA_VERSION_FOR_VS
from sdcm.remote import LOCALRUNNER, shell_script_cmd
from sdcm.test_config import TestConfig
from sdcm.kafka.kafka_config import SctKafkaConfiguration
from sdcm.mgmt.common import AgentBackupParameters
from sdcm.utils.version_utils import parse_scylla_version_tag


class IgnoredType:
    pass


def is_ignored_field(field) -> bool:
    """Check if a field is annotated with IgnoredType and should be skipped."""
    return any(isinstance(m, type) and issubclass(m, IgnoredType) for m in getattr(field, "metadata", []))


def _str(value: str | None) -> str | None:
    if value is None:
        return value
    if isinstance(value, str):
        return value
    raise ValueError(f"{value} isn't a string, it is '{type(value)}'")


String = Annotated[str | None, BeforeValidator(_str), Field(json_schema_extra={"appendable": True})]


def _file(value: str) -> str:
    file_path = pathlib.Path(value).expanduser()
    if file_path.is_file() and file_path.exists():
        return value
    raise ValueError(f"{value} isn't an existing file")


ExistingFile = Annotated[str, BeforeValidator(_file)]


def str_or_list_or_eval(value: Union[str, List[str], None]) -> List[str] | None:
    """Convert an environment variable into a Python's list."""

    if value is None:
        return None
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:  # noqa: BLE001
            pass
        return (
            [
                str(value),
            ]
            if str(value)
            else []
        )

    if isinstance(value, list):
        ret_values = []
        for val in value:
            try:
                ret_values += [ast.literal_eval(val)]
            except Exception:  # noqa: BLE001
                ret_values += [str(val)]
        return ret_values

    raise ValueError(f"{value} isn't a string or a list")


StringOrList = Annotated[
    str | list[str], BeforeValidator(str_or_list_or_eval), Field(json_schema_extra={"appendable": True})
]


def int_or_space_separated_ints(value: str | int | list[int]) -> int | list[int]:
    if value is None:
        return None
    try:
        value = int(value)
        return value
    except Exception:  # noqa: BLE001
        pass

    if isinstance(value, list):
        # Handle list of ints or list of strings that can be converted to ints
        try:
            return [int(v) for v in value]
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{value} isn't a list of integers") from exc

    if isinstance(value, str):
        try:
            values = value.split()
            return [int(v) for v in values]
        except Exception:  # noqa: BLE001
            pass

    raise ValueError("{} isn't int or list".format(value))


IntOrList = Annotated[int | list[int], BeforeValidator(int_or_space_separated_ints)]


def boolean_or_space_separated_booleans(value: bool | list[bool] | str | None) -> bool | list[bool] | None:  # noqa: PLR0911
    """Convert value to a single bool or list of bools.

    Accepts:
    - None -> None
    - bool -> bool
    - list of bools -> list of bools
    - list of strings (true/false/yes/no/1/0) -> list of bools
    - space-separated string of boolean values -> list of bools
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, list):
        if len(value) == 1:
            # Single item list, return just the bool
            if isinstance(value[0], bool):
                return value[0]
            if isinstance(value[0], str):
                return bool(strtobool(value[0]))

        # Handle list of bools or list of strings that can be converted to bools
        try:
            result = []
            for v in value:
                if isinstance(v, bool):
                    result.append(v)
                else:
                    result.append(bool(strtobool(str(v))))
            return result
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{value} isn't a list of booleans") from exc

    if isinstance(value, str):
        try:
            values = value.split()
            if len(values) == 1:
                return bool(strtobool(values[0]))
            return [bool(strtobool(v)) for v in values]
        except Exception:  # noqa: BLE001
            pass

    raise ValueError("{} isn't bool or list".format(value))


BooleanOrList = Annotated[bool | list[bool], BeforeValidator(boolean_or_space_separated_booleans)]


class MultitenantValueMarker:
    """Marker class to identify MultitenantValue types at runtime."""


def is_multitenant_field(field: pydantic_fields.FieldInfo) -> bool:
    """Check if a field uses MultitenantValue type by looking for the marker in its annotation."""
    if not hasattr(field, "annotation") or field.annotation is None:
        return False

    # Check in field.metadata first (where Pydantic stores annotation metadata)
    if hasattr(field, "metadata"):
        for meta in field.metadata:
            if isinstance(meta, MultitenantValueMarker):
                return True

    # Check in annotation args
    def check_annotation(annotation):
        origin = get_origin(annotation)
        if origin is Annotated:
            for arg in get_args(annotation):
                if isinstance(arg, MultitenantValueMarker):
                    return True
                # Recursively check nested annotations
                if check_annotation(arg):
                    return True
        return False

    return check_annotation(field.annotation)


def MultitenantValue(inner_type):  # noqa: N802
    """
    Type wrapper that adds dict[str, T] support to any type T for k8s multitenancy.

    Usage: MultitenantValue[IntOrList], MultitenantValue[StringOrList], etc.

    This allows configuration values to be specified as:
    - Single value: 5
    - List (index-based): [5, 7]
    - Dict (key-based): {tenant1: 5, tenant2: 7}

    The presence of this type automatically indicates multitenancy support,
    eliminating the need for k8s_multitenancy_supported=True flag.
    """
    return Annotated[
        inner_type | dict[str, inner_type],
        MultitenantValueMarker(),
    ]


def dict_or_str(value: dict | str | None) -> dict | None:
    if value is None:
        return None
    elif isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:  # noqa: BLE001
            pass

        # ast.literal_eval() can fail on some strings (e.g. which contain lowercased booleans), try parsing such strings
        # using yaml.safe_load()
        try:
            return yaml.safe_load(value)
        except Exception:  # noqa: BLE001
            pass

    if isinstance(value, dict):
        return value

    raise ValueError('"{}" isn\'t a dict'.format(value))


DictOrStr = Annotated[dict | str, BeforeValidator(dict_or_str)]


def dict_or_str_or_pydantic(value: dict | str | BaseModel | None) -> dict | BaseModel | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return ast.literal_eval(value)
        except Exception:  # noqa: BLE001
            pass

    if isinstance(value, (dict, BaseModel)):
        return value

    raise ValueError('"{}" isn\'t a dict, str or Pydantic model'.format(value))


DictOrStrOrPydantic = Annotated[dict | str | BaseModel, BeforeValidator(dict_or_str_or_pydantic)]


def _boolean(value):
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        return bool(strtobool(value))
    else:
        raise ValueError("{} isn't a boolean".format(type(value)))


def is_config_option_appendable(option_name: str) -> bool:
    for field_name, field in SCTConfiguration.model_fields.items():
        if is_ignored_field(field):
            continue
        if field_name == option_name:
            break
    else:
        raise ValueError(f"Option {option_name} not found in SCTConfiguration fields")

    # type: ignore[union-attr]
    return field.json_schema_extra and field.json_schema_extra.get("appendable", False)


def merge_dicts_append_strings(d1, d2):
    """
    merge two dictionaries, while having option
    to append string if the value starts with '++'
    and append list if first item is '++'
    """

    for key, value in copy.deepcopy(d2).items():
        if isinstance(value, str) and value.startswith("++"):
            assert is_config_option_appendable(key), f"Option {key} is not appendable"
            if key not in d1 or d1[key] is None:
                d1[key] = ""
            d1[key] += value[2:]
            del d2[key]
        if isinstance(value, list) and value and isinstance(value[0], str) and value[0].startswith("++"):
            assert is_config_option_appendable(key), f"Option {key} is not appendable"
            if key not in d1 or d1[key] is None:
                d1[key] = []
            d1[key].extend(value[1:])
            del d2[key]

    anyconfig.merge(d1, d2, ac_merge=anyconfig.MS_DICTS)


Boolean = Annotated[bool, BeforeValidator(_boolean)]


class SctField(FieldInfo):
    """Custom field class for SCT configuration fields.

    This class extends Pydantic's FieldInfo to support SCT-specific metadata.

    Args:
        *args: Positional arguments passed to Pydantic FieldInfo
        **kwargs: Keyword arguments including:
            - description (str): Field description for documentation
            - default: Default value for the field
            - appendable (bool): Whether this field supports the '++' append syntax
                                 in configuration files. When True, values can be
                                 appended using '++value' for strings or ['++', 'value']
                                 for lists. Some types (String, StringOrList) are
                                 appendable by default. Other types like version strings
                                 or region names should set appendable=False.
                                 See merge_dicts_append_strings() for implementation.
            - Other Pydantic Field parameters (validation_alias, etc.)

    Example:
        ```python
        my_field: str = SctField(
            description="Example field",
            appendable=True,  # Allow ++append syntax
        )
        ```
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("default", None)
        extra = {k: v for k, v in kwargs.items() if k in ("appendable",)}
        kwargs.setdefault("json_schema_extra", extra)
        # remove extra keys from kwargs since we moved them to json_schema_extra
        for key in extra:
            kwargs.pop(key, None)
        super().__init__(*args, **kwargs)


available_backends: list[str] = [
    "azure",
    "baremetal",
    "docker",
    # TODO: remove 'aws-siren' and 'gce-siren' backends completely when
    #       'siren-tests' project gets switched to the 'aws' and 'gce' ones.
    #       Such a switch must be fast change.
    "aws",
    "aws-siren",
    "k8s-local-kind-aws",
    "k8s-eks",
    "gce",
    "gce-siren",
    "k8s-local-kind-gce",
    "k8s-gke",
    "k8s-local-kind",
    "xcloud",
    "oci",
]


class SCTConfiguration(BaseModel):
    """
    Class the hold the SCT configuration
    """

    log: ClassVar = logging.getLogger(__name__)

    perf_extra_jobs_to_compare: StringOrList = SctField(
        description="""Jobs to compare performance results with, for example if running in staging,
         we still can compare with official jobs""",
    )
    perf_simple_query_extra_command: String = SctField(
        description="Extra command line options to pass to perf_simple_query",
    )
    force_run_iotune: Boolean = SctField(
        description="Force running iotune on the DB nodes, regardless if image has predefined values",
    )
    data_volume_disk_throughput: int = SctField(
        description="Throughput in MiB/sec for ebs type gp3. Min is 125. Max is 1000.",
    )

    multi_region_params: Annotated[list[str], IgnoredType] = Field(default=[], exclude=True)
    regions_data: Annotated[dict[str, dict[str, str]], IgnoredType] = Field(default={}, exclude=True)

    # computed values, user can't fill those from configuration,
    # see `update_config_based_on_version` for more information
    artifact_scylla_version: str | None = Field(default=None, exclude=True)
    is_enterprise: bool = Field(default=False, exclude=True)
    scylla_version_upgrade_target: str | None = Field(default=None, exclude=True)

    config_files: StringOrList = SctField(
        description="a list of config files that would be used",
        appendable=False,
    )
    cluster_backend: String = SctField(
        description="backend that will be used, aws/gce/azure/docker/xcloud",
        appendable=False,
    )
    test_method: String = SctField(
        description="class.method used to run the test. Filled automatically with run-test sct command.",
        appendable=False,
    )
    test_duration: int = SctField(
        description="""
              Test duration (min). Parameter used to keep instances produced by tests
              and for jenkins pipeline timeout and TimoutThread.
        """,
    )
    db_type: String = SctField(
        description="Db type to install into db nodes, scylla/cassandra",
    )
    prepare_stress_duration: int = SctField(
        description="""
              Time in minutes, which is required to run prepare stress commands
              defined in prepare_*_cmd for dataset generation, and is used in
              test duration calculation
         """,
    )
    stress_duration: int = SctField(
        description="""
              Time in minutes, Time of execution for stress commands from stress_cmd parameters
              and is used in test duration calculation
        """,
    )
    alternator_stress_rate: int = SctField(
        description="""
           Number of operations per second to achieve in stress commands for alternator testing.
      """,
    )
    alternator_write_always_lwt_stress_rate: int = SctField(
        description="""
              Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.
         """,
    )
    n_db_nodes: IntOrList = SctField(
        description="Number list of database nodes in multiple data centers.",
    )
    n_test_oracle_db_nodes: IntOrList = SctField(
        description="Number list of oracle test nodes in multiple data centers.",
    )
    n_loaders: IntOrList = SctField(
        description="Number list of loader nodes in multiple data centers",
    )
    n_monitor_nodes: IntOrList = SctField(
        description="Number list of monitor nodes in multiple data centers",
    )
    intra_node_comm_public: Boolean = SctField(
        description="If True, all communication between nodes are via public addresses",
    )
    endpoint_snitch: String = SctField(
        description="""
            The snitch class scylla would use

            'GossipingPropertyFileSnitch' - default
            'Ec2MultiRegionSnitch' - default on aws backend
            'GoogleCloudSnitch'
         """,
    )
    user_credentials_path: ExistingFile = SctField(
        description="""Path to your user credentials. qa key are downloaded automatically from S3 bucket""",
    )
    cloud_credentials_path: String = SctField(
        description="""Path to your user credentials. qa key are downloaded automatically from S3 bucket""",
    )
    cloud_cluster_id: int = SctField(
        description="""scylla cloud cluster id""",
    )
    cloud_prom_bearer_token: String = SctField(
        description="""scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance""",
    )
    cloud_prom_path: String = SctField(
        description="""scylla cloud promproxy path to federate monitoring data into our monitoring instance""",
    )
    cloud_prom_host: String = SctField(
        description="""scylla cloud promproxy hostname to federate monitoring data into our monitoring instance""",
    )
    ip_ssh_connections: Literal["public", "private", "ipv6"] = SctField(
        description="""
            Type of IP used to connect to machine instances.
            This depends on whether you are running your tests from a machine inside
            your cloud provider, where it makes sense to use 'private', or outside (use 'public')

            Default: Use public IPs to connect to instances (public)
            Use private IPs to connect to instances (private)
            Use IPv6 IPs to connect to instances (ipv6)
         """,
    )

    scylla_repo: String = SctField(
        description="Url to the repo of scylla version to install scylla. Can provide specific version after a colon "
        "e.g: `https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-2021.1.list:2021.1.18`",
    )
    scylla_apt_keys: StringOrList = SctField(
        description="APT keys for ScyllaDB repos",
    )
    unified_package: String = SctField(
        description="Url to the unified package of scylla version to install scylla",
    )
    nonroot_offline_install: Boolean = SctField(
        description="Install Scylla without required root privilege",
    )

    install_mode: String = SctField(
        description="Scylla install mode, repo/offline/web",
        appendable=False,
    )

    scylla_version: String = SctField(
        description="""Version of scylla to install, ex. '2.3.1'
                       Automatically lookup AMIs and repo links for formal versions.
                       WARNING: can't be used together with 'scylla_repo' or 'ami_id_db_scylla'""",
        appendable=False,
    )
    user_data_format_version: String = SctField(
        description="""Format version of the user-data to use for scylla images,
                       default to what tagged on the image used""",
        appendable=False,
    )
    oracle_user_data_format_version: String = SctField(
        description="""Format version of the user-data to use for scylla images,
                       default to what tagged on the image used""",
        appendable=False,
    )
    oracle_scylla_version: String = SctField(
        description="""Version of scylla to use as oracle cluster with gemini tests, ex. '3.0.11'
                 Automatically lookup AMIs for formal versions.
                 WARNING: can't be used together with 'ami_id_db_oracle'""",
        appendable=False,
    )
    scylla_linux_distro: String = SctField(
        description="""The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.""",
        appendable=False,
    )
    scylla_linux_distro_loader: String = SctField(
        description="""The distro name and family name to use. Example: 'ubuntu-jammy' or 'debian-bookworm'.""",
        appendable=False,
    )
    assert_linux_distro_features: StringOrList = SctField(
        description="""List of distro features relevant to SCT test. Example: 'fips'.
            This is used to assert that the distro features are supported by the scylla version being tested.
            If the feature is not supported, the test will fail.""",
        appendable=True,
    )
    scylla_repo_m: String = SctField(
        description="Url to the repo of scylla version to install scylla from for management tests",
    )
    scylla_mgmt_address: String = SctField(
        description="Url to the repo of scylla manager version to install for management tests",
    )
    scylla_mgmt_agent_address: String = SctField(
        description="Url to the repo of scylla manager agent version to install for management tests",
    )
    manager_version: String = SctField(
        description="Version of Scylla Manager server and agent to install",
        appendable=False,
    )
    target_manager_version: String = SctField(
        description="Version of Scylla Manager server and agent to upgrade to",
        appendable=False,
    )
    manager_scylla_backend_version: String = SctField(
        description="Version of ScyllaDB to install as Manager backend",
        appendable=False,
    )
    scylla_mgmt_agent_version: String = SctField(
        description="Version of Scylla Manager agent to install for management tests",
        appendable=False,
    )
    scylla_mgmt_pkg: String = SctField(
        description="Url to the scylla manager packages to install for management tests",
    )
    manager_backup_restore_method: String = SctField(
        description="The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.",
    )
    use_cloud_manager: Boolean = SctField(
        description="When define true, will install scylla cloud manager",
    )
    use_mgmt: Boolean = SctField(
        description="When define true, will install scylla management",
    )
    agent: DictOrStr = SctField(
        description="""
            Configuration for SCT agent - a lightweight service for remote command execution.                 When enabled, replaces SSH-based command execution with RESTful API calls for DB nodes.
            Configuration options:
            - enabled: bool - enable agent (required)
            - port: int - agent HTTP API port (default: 16000)
            - binary_url: str - URL to download agent binary
            - max_concurrent_jobs: int - max concurrent jobs per agent (default: 10)
            - log_level: str - logging level (default: info)""",
    )
    manager_prometheus_port: int = SctField(
        description="Port to be used by the manager to contact Prometheus",
    )
    target_scylla_mgmt_server_address: String = SctField(
        description="Url to the repo of scylla manager version used to upgrade the manager server",
    )
    target_scylla_mgmt_agent_address: String = SctField(
        description="Url to the repo of scylla manager version used to upgrade the manager agents",
    )
    use_ldap: Boolean = SctField(
        description="When defined true, LDAP is going to be used.",
    )
    use_ldap_authorization: Boolean = SctField(
        description="When defined true, will create a docker container with LDAP and configure scylla.yaml to use it",
    )
    use_ldap_authentication: Boolean = SctField(
        description="When defined true, will create a docker container with LDAP and configure scylla.yaml to use it",
    )
    prepare_saslauthd: Boolean = SctField(
        description="When defined true, will install and start saslauthd service",
    )
    ldap_server_type: String = SctField(
        description="This option indicates which server is going to be used for LDAP operations. [openldap, ms_ad]",
    )
    parallel_node_operations: Boolean = SctField(
        description="When defined true, will run node operations in parallel. Supported operations: startup",
    )
    update_db_packages: String = SctField(
        description="""A local directory of rpms to install a custom version on top of
                 the scylla installed (or from repo or from ami)""",
    )
    monitor_branch: String = SctField(
        description="The port of scylla management",
    )
    user_prefix: String = SctField(
        description="the prefix of the name of the cloud instances, defaults to username",
    )
    ami_id_db_scylla_desc: String = SctField(
        description="version name to report stats to Elasticsearch and tagged on cloud instances",
    )
    sct_public_ip: String = SctField(
        description="""
            Override the default hostname address of the sct test runner,
            for the monitoring of the Nemesis.
            can only work out of the box in AWS
        """,
    )
    peer_verification: Boolean = SctField(
        description="enable peer verification for encrypted communication",
    )
    client_encrypt_mtls: Boolean = SctField(
        description="when enabled scylla will enforce mutual authentication when client-to-node encryption is enabled",
    )
    server_encrypt_mtls: Boolean = SctField(
        description="when enabled scylla will enforce mutual authentication when node-to-node encryption is enabled",
    )
    sct_ngrok_name: String = SctField(
        description="Override the default hostname address of the sct test runner, using ngrok server, see readme for more instructions",
    )
    backtrace_decoding: Boolean = SctField(
        description="""If True, all backtraces found in db nodes would be decoded automatically""",
    )
    backtrace_stall_decoding: Boolean = SctField(
        description="""If True, reactor stall backtraces will be decoded. If False, reactor stalls are skipped during
         backtrace decoding to reduce overhead in performance tests. Only applies when backtrace_decoding is True.""",
    )
    backtrace_decoding_disable_regex: String = SctField(
        description="""Regex pattern to disable backtrace decoding for specific event types. If an event type matches
         this regex, its backtrace will not be decoded. This can be used to reduce overhead in performance tests
         by skipping backtrace decoding for certain types of events. Only applies when backtrace_decoding is True.""",
    )
    print_kernel_callstack: Boolean = SctField(
        description="""Scylla will print kernel callstack to logs if True, otherwise, it will try and may print a message
         that it failed to.""",
    )
    instance_provision: Literal["spot", "on_demand", "spot_fleet", "spot_low_price"] = SctField(
        description="instance_provision: spot|on_demand|spot_fleet",
    )
    instance_provision_fallback_on_demand: Boolean = SctField(
        description="instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected "
        "'instance_provision' type creation failed. "
        "Expected values: true|false (default - false",
    )
    reuse_cluster: String = SctField(
        description="""
        If reuse_cluster is set it should hold test_id of the cluster that will be reused.
        `reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`
        """,
    )
    test_id: String = SctField(
        description="""Set the test_id of the run manually. Use only from the env before running Hydra""",
    )
    billing_project: String = SctField(
        description="""Billing project for the test run. Used for cost tracking and reporting""",
    )
    db_nodes_shards_selection: Literal["default", "random"] = SctField(
        description="""How to select number of shards of Scylla. Expected values: default/random.
         Default value: 'default'.
         In case of random option - Scylla will start with different (random) shards on every node of the cluster
         """,
    )
    seeds_selector: Literal["random", "first", "all"] = SctField(
        description="""How to select the seeds. Expected values: random/first/all""",
    )
    seeds_num: int = SctField(
        description="""Number of seeds to select""",
    )
    email_recipients: StringOrList = SctField(
        description="""list of email of send the performance regression test to""",
    )
    email_subject_postfix: String = SctField(
        description="""Email subject postfix""",
    )
    enable_test_profiling: Boolean = SctField(
        description="""Turn on sct profiling""",
    )
    ssh_transport: Literal["libssh2", "fabric"] = SctField(
        description="""Set type of ssh library to use. Could be 'fabric' (default) or 'libssh2'""",
        default="libssh2",
    )

    # Scylla command line arguments options
    experimental_features: StringOrList = SctField(
        description="unlock specified experimental features",
    )
    server_encrypt: Boolean = SctField(
        description="when enable scylla will use encryption on the server side",
    )
    client_encrypt: Boolean = SctField(
        description="when enable scylla will use encryption on the client side",
    )
    hinted_handoff: String = SctField(
        description="when enable or disable scylla hinted handoff (enabled/disabled)",
    )
    nemesis_double_load_during_grow_shrink_duration: int = SctField(
        description="After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.",
    )
    authenticator: Literal[
        "PasswordAuthenticator", "AllowAllAuthenticator", "com.scylladb.auth.SaslauthdAuthenticator"
    ] = SctField(
        description="which authenticator scylla will use AllowAllAuthenticator/PasswordAuthenticator",
    )
    authenticator_user: String = SctField(
        description="the username if PasswordAuthenticator is used",
    )
    authenticator_password: String = SctField(
        description="the password if PasswordAuthenticator is used",
    )
    authorizer: Literal["AllowAllAuthorizer", "CassandraAuthorizer"] = SctField(
        description="which authorizer scylla will use AllowAllAuthorizer/CassandraAuthorizer",
    )
    # Temporary solution. We do not want to run SLA nemeses during not-SLA test until the feature is stable
    sla: Boolean = SctField(
        description="run SLA nemeses if the test is SLA only",
    )
    service_level_shares: list = SctField(
        description="List if service level shares - how many server levels to create and test. Uses in SLA test. list of int, like: [100, 200]",
    )
    alternator_port: int = SctField(
        description="Port to configure for alternator in scylla.yaml",
    )
    dynamodb_primarykey_type: Literal[tuple(x.value for x in alternator.enums.YCSBSchemaTypes.__members__.values())] = (
        SctField(
            description="Type of dynamodb table to create with range key or not",
        )
    )
    alternator_write_isolation: String = SctField(
        description="Set the write isolation for the alternator table, see https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more details",
    )
    alternator_use_dns_routing: Boolean = SctField(
        description="If true, spawn a docker with a dns server for the ycsb loader to point to",
    )
    alternator_loadbalancing: Boolean = SctField(
        description="If true, enable native load balancing for alternator",
    )
    alternator_test_table: DictOrStr = SctField(
        description="""Dictionary of a test alternator table features:
                name: str - the name of the table
                lsi_name: str - the name of the local secondary index to create with a table
                gsi_name: str - the name of the global secondary index to create with a table
                tags: dict - the tags to apply to the created table
                items: int - expected number of items in the table after prepare""",
    )
    alternator_enforce_authorization: Boolean = SctField(
        description="If true, enable the authorization check in dynamodb api (alternator)",
    )
    alternator_access_key_id: String = SctField(description="the aws_access_key_id that would be used for alternator")
    alternator_secret_access_key: String = SctField(
        description="the aws_secret_access_key that would be used for alternator",
    )
    alternator_trust_all_certificates: Boolean = SctField(
        description="If true, trust all TLS certificates for alternator connections (for testing with self-signed certs)",
    )
    region_aware_loader: Boolean = SctField(
        description="When in multi region mode, run stress on loader that is located in the same region as db node",
    )
    append_scylla_args: String = SctField(
        description="More arguments to append to scylla command line",
    )
    append_scylla_args_oracle: String = SctField(
        description="More arguments to append to oracle command line",
    )
    append_scylla_yaml: DictOrStrOrPydantic = SctField(
        description="More configuration to append to /etc/scylla/scylla.yaml",
    )
    append_scylla_node_exporter_args: String = SctField(
        description="More arguments to append to scylla-node-exporter command line",
    )

    # Nemesis config options
    nemesis_class_name: MultitenantValue(StringOrList) = SctField(
        description="""
                Nemesis class to use (possible types in sdcm.nemesis).
                Next syntax supporting:
                - nemesis_class_name: "NemesisName"  Run one nemesis in single thread
                - nemesis_class_name: "<NemesisName>:<num>" Run <NemesisName> in <num>
                  parallel threads on different nodes. Ex.: "ChaosMonkey:2"
                - nemesis_class_name: "<NemesisName1>:<num1> <NemesisName2>:<num2>" Run
                  <NemesisName1> in <num1> parallel threads and <NemesisName2> in <num2>
                  parallel threads. Ex.: "DisruptiveMonkey:1 NonDisruptiveMonkey:2"
        """,
    )
    nemesis_interval: MultitenantValue(IntOrList) = SctField(
        description="""Nemesis sleep interval to use if None provided specifically in the test""",
    )
    nemesis_sequence_sleep_between_ops: MultitenantValue(IntOrList) = SctField(
        description="""Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests""",
    )
    nemesis_during_prepare: MultitenantValue(BooleanOrList) = SctField(
        description="""Run nemesis during prepare stage of the test""",
    )
    nemesis_seed: MultitenantValue(IntOrList) = SctField(
        description="""A seed number in order to repeat nemesis sequence as part of SisyphusMonkey""",
    )
    nemesis_add_node_cnt: MultitenantValue(IntOrList) = SctField(
        description="""Add/remove nodes during GrowShrinkCluster nemesis""",
    )
    nemesis_grow_shrink_instance_type: String = SctField(
        description="""Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis""",
    )
    cluster_target_size: IntOrList = SctField(
        description="""Used for scale test: max size of the cluster""",
    )
    space_node_threshold: MultitenantValue(IntOrList) = SctField(
        description="""
             Space node threshold before starting nemesis (bytes)
             The default value is 6GB (6x1024^3 bytes)
             This value is supposed to reproduce
             https://github.com/scylladb/scylla/issues/1140
         """,
    )
    nemesis_filter_seeds: MultitenantValue(BooleanOrList) = SctField(
        description="""If true runs the nemesis only on non seed nodes""",
    )

    # Stress Commands
    stress_cmd: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. multiple commands can passed as a list",
    )
    gemini_schema_url: String = SctField(
        description="Url of the schema/configuration the gemini tool would use",
    )
    gemini_cmd: String = SctField(
        description="gemini command to run (for now used only in GeminiTest)",
    )
    gemini_seed: int = SctField(
        description="Seed number for gemini command",
    )
    gemini_log_cql_statements: Boolean = SctField(
        description="Log CQL statements to file",
    )
    gemini_table_options: list = SctField(
        description="table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']",
    )
    run_gemini_in_rolling_upgrade: Boolean = SctField(
        description="Enable running Gemini workload during rolling upgrade test. Default is false.",
    )
    # AWS config options
    instance_type_loader: String = SctField(
        description="AWS image type of the loader node",
    )
    instance_type_monitor: String = SctField(
        description="AWS image type of the monitor node",
    )
    instance_type_db: String = SctField(
        description="AWS image type of the db node",
    )
    instance_type_db_oracle: String = SctField(
        description="AWS image type of the oracle node",
    )
    instance_type_runner: String = SctField(
        description="instance type of the sct-runner node",
    )
    region_name: StringOrList = SctField(
        description="AWS regions to use",
        appendable=False,
    )
    use_placement_group: Boolean = SctField(
        description="if true, create 'cluster' placement group for test case "
        "for low-latency network performance achievement",
    )
    ami_id_db_scylla: String = SctField(
        description="AMS AMI id to use for scylla db node",
    )
    ami_id_loader: String = SctField(
        description="AMS AMI id to use for loader node",
    )
    ami_id_monitor: String = SctField(
        description="AMS AMI id to use for monitor node",
    )
    ami_id_db_cassandra: String = SctField(
        description="AMS AMI id to use for cassandra node",
    )
    ami_id_db_oracle: String = SctField(
        description="AMS AMI id to use for oracle node",
    )
    ami_id_vector_store: String = SctField(
        description="AMS AMI id to use for vector store node",
    )
    instance_type_vector_store: String = SctField(
        description="AWS/GCP cloud provider instance type for Vector Store nodes",
    )
    root_disk_size_db: int = SctField(
        description="",
    )
    root_disk_size_monitor: int = SctField(
        description="",
    )
    root_disk_size_loader: int = SctField(
        description="",
    )
    root_disk_size_runner: int = SctField(
        description="root disk size in Gb for sct-runner",
    )
    ami_db_scylla_user: String = SctField(
        description="",
    )
    ami_monitor_user: String = SctField(
        description="",
    )
    ami_loader_user: String = SctField(
        description="",
    )
    ami_db_cassandra_user: String = SctField(
        description="",
    )
    ami_vector_store_user: String = SctField(
        description="",
    )
    spot_max_price: float = SctField(
        description="The max percentage of the on demand price we set for spot/fleet instances",
    )
    extra_network_interface: Boolean = SctField(
        description="if true, create extra network interface on each node",
    )
    aws_instance_profile_name_db: String = SctField(
        description="This is the name of the instance profile to set on all db instances",
    )
    aws_instance_profile_name_loader: String = SctField(
        description="This is the name of the instance profile to set on all loader instances",
    )
    backup_bucket_backend: String = SctField(
        description="the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')",
    )
    backup_bucket_location: StringOrList = SctField(
        description="the bucket name to be used for backup (e.g., 'manager-backup-tests')",
    )
    backup_bucket_region: String = SctField(
        description="the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')",
    )
    use_prepared_loaders: Boolean = SctField(
        description="If True, we use prepared VMs for loader (instead of using docker images)",
    )
    scylla_d_overrides_files: StringOrList = SctField(
        description="list of files that should upload to /etc/scylla.d/ directory to override scylla config files",
        appendable=True,
    )
    gce_project: String = SctField(
        description="gcp project name to use",
    )
    gce_datacenter: String = SctField(
        description="Supported regions: us-east1, us-east4, us-west1, us-central1. Specifying just the region "
        "(e.g., us-east1) means the zone will be selected automatically, or you can mention the zone "
        "explicitly (e.g., us-east1-b)",
        appendable=False,
    )
    gce_network: String = SctField(
        description="gce network to use",
    )
    gce_image_db: String = SctField(
        description="gce image to use for db nodes",
    )
    gce_image_monitor: String = SctField(
        description="gce image to use for monitor nodes",
    )
    scylla_network_config: list = SctField(
        description="""Configure Scylla networking with single or multiple NIC/IP combinations.
              It must be defined for listen_address and rpc_address. For each address mandatory parameters are:
              - address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication
              - ip_type: ipv4 or ipv6
              - public: false or true
              - nic: number of NIC. 0, 1
              Supported for AWS only meanwhile""",
    )
    gce_image_loader: String = SctField(
        description="Google Compute Engine image to use for loader nodes",
    )
    gce_image_username: String = SctField(
        description="Username for the Google Compute Engine image",
    )
    gce_instance_type_loader: String = SctField(
        description="Instance type for loader nodes in Google Compute Engine",
    )
    gce_root_disk_type_loader: String = SctField(
        description="Root disk type for loader nodes in Google Compute Engine",
    )
    gce_n_local_ssd_disk_loader: int = SctField(
        description="Number of local SSD disks for loader nodes in Google Compute Engine",
    )
    gce_instance_type_monitor: String = SctField(
        description="Instance type for monitor nodes in Google Compute Engine",
    )
    gce_root_disk_type_monitor: String = SctField(
        description="Root disk type for monitor nodes in Google Compute Engine",
    )
    validate_large_collections: Boolean = SctField(
        description="Flag to validate large collections in the database",
    )
    run_commit_log_check_thread: Boolean = SctField(
        description="Flag to run a thread that checks commit logs",
    )
    teardown_validators: DictOrStr = SctField(
        description="Validators to use during teardown phase",
    )
    use_capacity_reservation: Boolean = SctField(
        description="Flag to use capacity reservation for instances",
    )
    use_dedicated_host: Boolean = SctField(
        description="Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)",
    )
    aws_dedicated_host_ids: StringOrList = SctField(
        description="List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)",
    )
    post_behavior_dedicated_host: Literal["keep", "destroy"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.

        'destroy' - Destroy hosts (default)
        'keep' - Keep hosts allocated
        """,
    )
    bisect_start_date: String = SctField(
        description="Start date for bisecting test runs to find regressions",
    )
    bisect_end_date: String = SctField(
        description="End date for bisecting test runs to find regressions",
    )
    kafka_backend: Literal["localstack", "vm", "msk"] | None = SctField(
        description="Type of Kafka backend to use",
    )
    kafka_connectors: list[SctKafkaConfiguration] = SctField(
        description="Kafka connectors to use",
    )
    run_scylla_doctor: Boolean = SctField(
        description="Flag to run Scylla Doctor tool",
    )
    scylla_doctor_version: String = SctField(
        description="""Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.9')
                to hardcode the version, or leave empty to use the latest available version. For stability,
                artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.""",
    )
    skip_test_stages: DictOrStr = SctField(
        description="Skip selected stages of a test scenario",
    )
    use_zero_nodes: Boolean = SctField(
        description="If True, enable support in SCT of zero nodes (configuration, nemesis)",
    )
    n_db_zero_token_nodes: IntOrList = SctField(
        description="Number of zero token nodes in cluster. Value should be set as '0 1 1' "
        "for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions",
    )
    zero_token_instance_type_db: String = SctField(
        description="Instance type for zero token node",
    )
    sct_aws_account_id: String = SctField(
        description="AWS account id on behalf of which the test is run",
    )
    latency_decorator_error_thresholds: DictOrStr = SctField(
        description="Error thresholds for latency decorator. "
        "Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}",
    )
    workload_name: String = SctField(
        description="Workload name, can be: write|read|mixed|unset. "
        "Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). "
        "If unset, workload is taken from test name.",
    )
    adaptive_timeout_store_metrics: Boolean = SctField(
        description="Store adaptive timeout metrics in Argus. Disabled for performance tests only.",
    )

    # Google Compute Engine options
    gce_n_local_ssd_disk_monitor: int = SctField(
        description="Number of local SSD disks for monitor nodes in Google Compute Engine",
    )
    gce_instance_type_db: String = SctField(
        description="Instance type for database nodes in Google Compute Engine",
    )
    gce_root_disk_type_db: String = SctField(
        description="Root disk type for database nodes in Google Compute Engine",
    )
    gce_n_local_ssd_disk_db: int = SctField(
        description="Number of local SSD disks for database nodes in Google Compute Engine",
    )
    gce_pd_standard_disk_size_db: int = SctField(
        description="The size of the standard persistent disk in GB used for GCE database nodes",
    )
    gce_pd_ssd_disk_size_db: int = SctField(
        description="",
    )
    gce_setup_hybrid_raid: Boolean = SctField(
        description="If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data",
    )
    gce_pd_ssd_disk_size_loader: int = SctField(
        description="",
    )
    gce_pd_ssd_disk_size_monitor: int = SctField(
        description="",
    )

    # azure options
    azure_region_name: StringOrList = SctField(
        description="Azure region(s) where the resources will be deployed. Supports single or multiple regions.",
        appendable=False,
    )
    azure_instance_type_loader: String = SctField(
        description="The Azure virtual machine size to be used for loader nodes.",
    )
    azure_instance_type_monitor: String = SctField(
        description="The Azure virtual machine size to be used for monitor nodes.",
    )
    azure_instance_type_db: String = SctField(
        description="The Azure virtual machine size to be used for database nodes.",
    )
    azure_instance_type_db_oracle: String = SctField(
        description="The Azure virtual machine size to be used for Oracle database nodes.",
    )
    azure_image_db: String = SctField(
        description="The Azure image to be used for database nodes.",
    )
    azure_image_monitor: String = SctField(
        description="The Azure image to be used for monitor nodes.",
    )
    azure_image_loader: String = SctField(
        description="The Azure image to be used for loader nodes.",
    )
    azure_image_username: String = SctField(
        description="The username for the Azure image.",
    )

    # k8s-eks options
    eks_service_ipv4_cidr: String = SctField(
        description="EKS service IPv4 CIDR block",
    )
    eks_vpc_cni_version: String = SctField(
        description="EKS VPC CNI plugin version",
    )
    eks_role_arn: String = SctField(
        description="ARN of the IAM role for EKS",
    )
    eks_admin_arn: StringOrList = SctField(
        description="ARN(s) of the IAM user or role to be granted cluster admin access",
    )
    eks_cluster_version: String = SctField(
        description="EKS cluster Kubernetes version",
    )
    eks_nodegroup_role_arn: String = SctField(
        description="ARN of the IAM role for EKS node groups",
    )

    # k8s-gke options
    gke_cluster_version: String = SctField(
        description="Specifies the version of the GKE cluster to be used.",
    )
    gke_k8s_release_channel: String = SctField(
        description="K8S release channel name to be used. Expected values are: 'rapid', 'regular', 'stable' and '' (static / No channel).",
    )
    k8s_scylla_utils_docker_image: String = SctField(
        description="Docker image to be used by Scylla operator to tune K8S nodes for performance. Used when 'k8s_enable_performance_tuning' is defined to 'True'. If not set then the default from operator will be used.",
    )
    k8s_enable_performance_tuning: Boolean = SctField(
        description="Define whether performance tuning must run or not.",
    )
    k8s_deploy_monitoring: Boolean = SctField(
        description="Determines if monitoring should be deployed alongside the Scylla cluster.",
    )
    k8s_local_volume_provisioner_type: String = SctField(
        description="Defines the type of the K8S local volume provisioner to be deployed. It may be either 'static' or 'dynamic'. Details about 'dynamic': 'dynamic': https://github.com/scylladb/k8s-local-volume-provisioner; 'static': sdcm/k8s_configs/static-local-volume-provisioner.yaml",
    )
    k8s_scylla_operator_docker_image: String = SctField(
        description="Docker image to be used for installation of Scylla operator.",
    )
    k8s_scylla_operator_upgrade_docker_image: String = SctField(
        description="Docker image to be used for upgrade of Scylla operator.",
    )
    k8s_scylla_operator_helm_repo: String = SctField(
        description="Link to the Helm repository where to get 'scylla-operator' charts from.",
    )
    k8s_scylla_operator_upgrade_helm_repo: String = SctField(
        description="Link to the Helm repository where to get 'scylla-operator' charts for upgrade.",
    )
    k8s_scylla_operator_chart_version: String = SctField(
        description="Version of 'scylla-operator' Helm chart to use. If not set then latest one will be used.",
    )
    k8s_scylla_operator_upgrade_chart_version: String = SctField(
        description="Version of 'scylla-operator' Helm chart to use for upgrade.",
    )
    k8s_functional_test_dataset: String = SctField(
        description="Defines whether dataset uses for pre-fill cluster in functional test. Defined in sdcm.utils.sstable.load_inventory. Expected values: BIG_SSTABLE_MULTI_COLUMNS_DATA, MULTI_COLUMNS_DATA",
    )
    k8s_scylla_cpu_limit: String = SctField(
        description="The CPU limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '500m' or '2'",
    )
    k8s_scylla_memory_limit: String = SctField(
        description="The memory limit that will be set for each Scylla cluster deployed in K8S. If not set, then will be autocalculated. Example: '16384Mi'",
    )
    k8s_scylla_cluster_name: String = SctField(
        description="Specifies the name of the Scylla cluster to be deployed in K8S.",
    )
    k8s_n_scylla_pods_per_cluster: int = SctField(
        description="Number of Scylla pods per cluster.",
    )
    k8s_scylla_disk_gi: int = SctField(
        description="Specifies the disk size in GiB for Scylla pods.",
    )
    k8s_scylla_disk_class: String = SctField(
        description="Specifies the disk class for Scylla pods.",
    )
    k8s_loader_cluster_name: String = SctField(
        description="Specifies the name of the loader cluster.",
    )
    k8s_n_loader_pods_per_cluster: int = SctField(
        description="Number of loader pods per loader cluster.",
    )
    k8s_loader_run_type: String = SctField(
        description="Defines how the loader pods must run. It may be either 'static' (default, run stress command on the constantly existing idle pod having reserved resources, perf-oriented) or 'dynamic' (run stress command in a separate pod as main thread and get logs in a separate retryable API call not having resource reservations).",
    )
    k8s_instance_type_auxiliary: String = SctField(
        description="Instance type for the nodes of the K8S auxiliary/default node pool.",
    )
    k8s_instance_type_monitor: String = SctField(
        description="Instance type for the nodes of the K8S monitoring node pool.",
    )
    mini_k8s_version: String = SctField(
        description="Specifies the version of the mini K8S cluster to be used.",
    )
    k8s_cert_manager_version: String = SctField(
        description="Specifies the version of the cert-manager to be used in K8S.",
    )
    k8s_minio_storage_size: String = SctField(
        description="Specifies the storage size for MinIO deployment in K8S.",
    )
    k8s_log_api_calls: Boolean = SctField(
        description="Defines whether the K8S API server logging must be enabled and its logs gathered. Be aware that it may be a really huge set of data.",
    )
    k8s_tenants_num: int = SctField(
        description="Number of Scylla clusters to create in the K8S cluster.",
    )
    k8s_enable_tls: Boolean = SctField(
        description="Defines whether to enable the operator serverless options.",
    )
    k8s_enable_sni: Boolean = SctField(
        description="Defines whether we install SNI and use it or not (serverless feature).",
    )
    k8s_enable_alternator: Boolean = SctField(
        description="Defines whether we enable the alternator feature using scylla-operator or not.",
    )
    k8s_connection_bundle_file: String = SctField(
        description="Serverless configuration bundle file.",
    )
    k8s_db_node_service_type: String = SctField(
        description="Defines the type of the K8S 'Service' objects type used for ScyllaDB pods. Empty value means 'do not set and allow scylla-operator to choose'.",
    )
    k8s_db_node_to_node_broadcast_ip_type: String = SctField(
        description="Defines the source of the IP address to be used for the 'broadcast_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.",
    )
    k8s_db_node_to_client_broadcast_ip_type: String = SctField(
        description="Defines the source of the IP address to be used for the 'broadcast_rpc_address' config option in the 'scylla.yaml' files. Empty value means 'do not set and allow scylla-operator to choose'.",
    )
    k8s_use_chaos_mesh: Boolean = SctField(
        description="Enables chaos-mesh for K8S testing.",
    )
    k8s_n_auxiliary_nodes: int = SctField(
        description="Number of nodes in the auxiliary pool.",
    )
    k8s_n_monitor_nodes: int = SctField(
        description="Number of nodes in the monitoring pool that will be used for scylla-operator's deployed monitoring pods.",
    )

    # docker config options
    mgmt_docker_image: String = SctField(
        description="Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'",
    )
    docker_image: String = SctField(
        description="Scylla docker image repo, i.e. 'scylladb/scylla', if omitted is calculated from scylla_version",
    )
    docker_network: String = SctField(
        description="Local docker network to use, if there's need to have db cluster connect to other services running in docker",
    )
    vector_store_docker_image: String = SctField(
        description="Vector Store docker image repo, i.e. 'scylladb/vector-store', if omitted is calculated from vector_store_version",
    )
    vector_store_version: String = SctField(
        description="Vector Store version / docker image tag",
    )
    # baremetal config options
    s3_baremetal_config: String = SctField(
        description="Configuration for S3 in baremetal setups. This includes details such as endpoint URL, access key, secret key, and bucket name.",
    )
    db_nodes_private_ip: StringOrList = SctField(
        description="Private IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    db_nodes_public_ip: StringOrList = SctField(
        description="Public IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    loaders_private_ip: StringOrList = SctField(
        description="Private IP addresses of loader nodes. Loaders are used for running stress tests or other workloads against the DB. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    loaders_public_ip: StringOrList = SctField(
        description="Public IP addresses of loader nodes. These IPs are used for accessing the loaders from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    monitor_nodes_private_ip: StringOrList = SctField(
        description="Private IP addresses of monitor nodes. Monitoring nodes host monitoring tools like Prometheus and Grafana for DB performance monitoring. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    monitor_nodes_public_ip: StringOrList = SctField(
        description="Public IP addresses of monitor nodes. These IPs are used for accessing the monitoring tools from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    # test specific config parameters

    # GrowClusterTest
    cassandra_stress_population_size: int = SctField(
        description="The total population size over which the Cassandra stress tests are run.",
    )
    cassandra_stress_threads: int = SctField(
        description="The number of threads used by Cassandra stress tests.",
    )
    add_node_cnt: int = SctField(
        description="The number of nodes to add during the test.",
    )

    # LongevityTest
    stress_multiplier: int = SctField(
        description="Multiplier for stress command intensity",
    )
    stress_multiplier_w: int = SctField(
        description="Write stress command intensity multiplier",
    )
    stress_multiplier_r: int = SctField(
        description="Read stress command intensity multiplier",
    )
    stress_multiplier_m: int = SctField(
        description="Mixed operations stress command intensity multiplier",
    )
    run_fullscan: list = SctField(
        description="Enable or disable running full scans during tests",
    )
    run_full_partition_scan: String = SctField(
        description="Enable or disable running full partition scans during tests",
    )
    run_tombstone_gc_verification: String = SctField(
        description="Enable or disable tombstone garbage collection verification during tests",
    )
    keyspace_num: int = SctField(
        description="Number of keyspaces to use in the test",
    )
    round_robin: MultitenantValue(BooleanOrList) = SctField(
        description="Enable or disable round robin selection of nodes for operations",
    )
    batch_size: int = SctField(
        description="Batch size for operations",
    )
    pre_create_schema: Boolean = SctField(
        description="Enable or disable pre-creation of schema before running workload",
    )
    pre_create_keyspace: StringOrList = SctField(
        description="Command to create keyspace to be pre-created before running workload",
    )
    post_prepare_cql_cmds: StringOrList = SctField(
        description="CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)",
    )
    prepare_wait_no_compactions_timeout: int = SctField(
        description="Time to wait for compaction to finish at the end of prepare stage. Use only when compaction affects the test or load",
    )
    compaction_strategy: String = SctField(
        description="Compaction strategy to use for pre-created schema",
    )
    sstable_size: int = SctField(
        description="Configure sstable size for pre-create-schema mode",
    )
    cluster_health_check: Boolean = SctField(
        description="Enable or disable starting cluster health checker for all nodes",
    )
    data_validation: String = SctField(
        description="Specify the type of data validation to perform",
    )
    stress_read_cmd: StringOrList = SctField(
        description="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    prepare_verify_cmd: StringOrList = SctField(
        description="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    user_profile_table_count: int = SctField(
        description="Number of user profile tables to create for the test",
    )
    add_cs_user_profiles_extra_tables: Boolean = SctField(
        description="extra tables to create for template user c-s, in addition to pre-created tables",
    )

    # MgmtCliTest
    scylla_mgmt_upgrade_to_repo: String = SctField(
        description="Url to the repo of scylla manager version to upgrade to for management tests",
    )
    mgmt_agent_backup_config: AgentBackupParameters | None = SctField(
        description="Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}",
    )
    mgmt_restore_extra_params: String = SctField(
        description="Manager restore operation extra parameters: batch-size, parallel, etc. "
        "For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd",
    )
    mgmt_reuse_backup_snapshot_name: String = SctField(
        description="Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. "
        "The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)",
    )
    mgmt_skip_post_restore_stress_read: Boolean = SctField(
        description="Skip post-restore c-s verification read in the Manager restore benchmark tests",
    )
    mgmt_nodetool_refresh_flags: String = SctField(
        description="Nodetool refresh extra options like --load-and-stream or --primary-replica-only",
    )
    mgmt_prepare_snapshot_size: int = SctField(
        description="Size of backup snapshot in Gb to be prepared for backup",
    )
    mgmt_snapshots_preparer_params: DictOrStr = SctField(
        description="Custom parameters of c-s write operation used in snapshots preparer",
    )

    # PerformanceRegressionTest
    stress_cmd_w: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_r: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_m: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_read_disk: MultitenantValue(StringOrList) = SctField(
        description="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list""",
    )
    stress_cmd_cache_warmup: MultitenantValue(StringOrList) = SctField(
        description="""cassandra-stress commands for warm-up before read workload.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    prepare_write_cmd: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_no_mv: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_no_mv_profile: StringOrList = SctField(
        description="",
    )
    cs_user_profiles: StringOrList = SctField(
        description="cassandra-stress user-profiles list. Executed in test step",
    )
    prepare_cs_user_profiles: StringOrList = SctField(
        description="cassandra-stress user-profiles list. Executed in prepare step",
    )
    cs_duration: String = SctField(
        description="",
    )
    cs_debug: Boolean = SctField(
        description="enable debug for cassandra-stress",
    )
    stress_cmd_mv: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    prepare_stress_cmd: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    perf_gradual_threads: DictOrStr = SctField(
        description="Threads amount of stress load for gradual performance test per sub-test. "
        "Example: {'read': 100, 'write': [200, 300], 'mixed': 300}",
    )
    perf_gradual_throttle_steps: DictOrStr = SctField(
        description="Used for gradual performance test. Define throttle for load step in ops. "
        "Example: {'read': ['100000', '150000'], 'mixed': ['300']}",
    )
    perf_gradual_step_duration: DictOrStr = SctField(
        description="Step duration of c-s load for gradual performance test per sub-test. "
        "Example: {'read': '30m', 'write': None, 'mixed': '30m'}",
    )

    # PerformanceRegressionLWTTest
    stress_cmd_lwt_i: StringOrList = SctField(
        description="Stress command for LWT performance test for INSERT baseline",
    )
    stress_cmd_lwt_d: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE baseline",
    )
    stress_cmd_lwt_u: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE baseline",
    )
    stress_cmd_lwt_ine: StringOrList = SctField(
        description="Stress command for LWT performance test for INSERT with IF NOT EXISTS",
    )
    stress_cmd_lwt_uc: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE with IF <condition>",
    )
    stress_cmd_lwt_ue: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE with IF EXISTS",
    )

    stress_cmd_lwt_de: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE with IF EXISTS",
    )
    stress_cmd_lwt_dc: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE with IF <condition>",
    )
    stress_cmd_lwt_mixed: StringOrList = SctField(
        description="Stress command for LWT performance test for mixed lwt load",
    )
    stress_cmd_lwt_mixed_baseline: StringOrList = SctField(
        description="Stress command for LWT performance test for mixed lwt load baseline",
    )

    # RefreshTest
    skip_download: Boolean = SctField(description="")
    sstable_file: String = SctField(description="")
    sstable_url: String = SctField(description="")
    sstable_md5: String = SctField(description="")
    flush_times: int = SctField(description="")
    flush_period: int = SctField(description="")

    # UpgradeTest
    new_scylla_repo: String = SctField(
        description="URL to the Scylla repository for new versions.",
    )
    new_version: String = SctField(
        description="Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1",
    )
    target_upgrade_version: String = SctField(description="The target version to upgrade Scylla to.")
    disable_raft: Boolean = SctField(
        description="Flag to disable Raft consensus for LWT operations.",
    )
    enable_tablets_on_upgrade: Boolean = SctField(
        description="By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade",
    )
    enable_views_with_tablets_on_upgrade: Boolean = SctField(
        description="Enables creating materialized views in keyspaces using tablets by adding an experimental feature."
        "It should not be used when upgrading to versions before 2025.1 and it should be used for upgrades"
        "where we create such views.",
    )
    upgrade_node_packages: String = SctField(description="Specifies the packages to be upgraded on the node.")
    upgrade_node_system: Boolean = SctField(
        description="Upgrade system packages on nodes before upgrading Scylla. Enabled by default.",
    )
    stress_cmd_1: StringOrList = SctField(
        description="Primary stress command to be executed.",
    )
    stress_cmd_complex_prepare: StringOrList = SctField(
        description="Stress command for complex preparation steps.",
    )
    prepare_write_stress: StringOrList = SctField(
        description="Stress command to prepare write operations.",
    )
    stress_cmd_read_10m: StringOrList = SctField(
        description="Stress command to perform read operations for 10 minutes.",
    )
    stress_cmd_read_cl_one: StringOrList = SctField(
        description="Stress command to perform read operations with consistency level ONE.",
    )
    stress_cmd_read_60m: StringOrList = SctField(
        description="Stress command to perform read operations for 60 minutes.",
    )
    stress_cmd_complex_verify_read: StringOrList = SctField(
        description="Stress command to verify complex read operations.",
    )
    stress_cmd_complex_verify_more: StringOrList = SctField(
        description="Additional stress command to verify complex operations.",
    )
    write_stress_during_entire_test: StringOrList = SctField(
        description="Stress command to perform write operations throughout the entire test.",
    )
    verify_data_after_entire_test: StringOrList = SctField(
        description="Stress command to verify data integrity after the entire test.",
    )
    stress_cmd_read_cl_quorum: StringOrList = SctField(
        description="Stress command to perform read operations with consistency level QUORUM.",
    )
    verify_stress_after_cluster_upgrade: StringOrList = SctField(
        description="""cassandra-stress commands.
        You can specify everything but the -node parameter, which is going to
        be provided by the test suite infrastructure.
        multiple commands can passed as a list""",
    )
    stress_cmd_complex_verify_delete: StringOrList = SctField(
        description="""cassandra-stress commands.
        You can specify everything but the -node parameter, which is going to
        be provided by the test suite infrastructure.
        multiple commands can passed as a list""",
    )
    scylla_encryption_options: String = SctField(
        description="options will be used for enable encryption at-rest for tables",
    )
    kms_key_rotation_interval: int = SctField(
        description="The time interval in minutes which gets waited before the KMS key rotation happens."
        " Applied when the AWS KMS service is configured to be used.",
    )
    enable_kms_key_rotation: Boolean = SctField(
        description="Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.",
    )
    enterprise_disable_kms: Boolean = SctField(
        description="An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up",
    )
    logs_transport: Literal["ssh", "docker", "syslog-ng", "vector"] = SctField(
        description="How to transport logs: syslog-ng, ssh or docker",
    )
    collect_logs: Boolean = SctField(
        description="Collect logs from instances and sct runner",
    )
    use_scylla_doctor_on_failure: Boolean = SctField(
        description="Run scylla-doctor on test failure to collect additional diagnostics",
    )
    execute_post_behavior: Boolean = SctField(
        description="Run post behavior actions in sct teardown step",
    )
    post_behavior_db_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.

            'destroy' - Destroy instances and credentials (default)
            'keep' - Keep instances running and leave credentials alone
            'keep-on-failure' - Keep instances if testrun failed
         """,
    )
    post_behavior_loader_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.

            'destroy' - Destroy instances and credentials (default)
            'keep' - Keep instances running and leave credentials alone
            'keep-on-failure' - Keep instances if testrun failed
         """,
    )
    post_behavior_monitor_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.
         """,
    )
    post_behavior_k8s_cluster: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.

        'destroy' - Destroy k8s cluster and credentials (default)
        'keep' - Keep k8s cluster running and leave credentials alone
        'keep-on-failure' - Keep k8s cluster if testrun failed
        """,
    )
    post_behavior_vector_store_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.

        'destroy' - Destroy instances and credentials (default)
        'keep' - Keep instances running and leave credentials alone
        'keep-on-failure' - Keep instances if testrun failed
        """,
    )
    internode_compression: String = SctField(description="Scylla option: internode_compression.")
    internode_encryption: String = SctField(
        description="Scylla sub option of server_encryption_options: internode_encryption.",
    )
    jmx_heap_memory: int = SctField(
        description="The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).",
    )

    loader_swap_size: int = SctField(
        description="The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB",
    )
    monitor_swap_size: int = SctField(
        description="The size of the swap file for the monitors. Its size in bytes calculated by x * 1MB",
    )
    append_scylla_setup_args: String = SctField(
        description="More arguments to append to scylla_setup command line",
    )
    use_preinstalled_scylla: Boolean = SctField(
        description="Don't install/update ScyllaDB on DB nodes",
    )
    stress_cdclog_reader_cmd: String = SctField(
        description="""cdc-stressor command to read cdc_log table.
                       You can specify everything but the -node, -keyspace, -table parameter, which is going to
                       be provided by the test suite infrastructure.
                       Multiple commands can be passed as a list.""",
    )
    store_cdclog_reader_stats_in_es: Boolean = SctField(
        description="Add cdclog reader stats to ES for future performance result calculating",
    )
    stop_test_on_stress_failure: Boolean = SctField(
        description="""If set to True the test will be stopped immediately when stress command failed.
                       When set to False the test will continue to run even when there are errors in the
                       stress process""",
    )
    stress_cdc_log_reader_batching_enable: Boolean = SctField(
        description="""retrieving data from multiple streams in one poll""",
    )
    use_legacy_cluster_init: Boolean = SctField(
        description="""Use legacy cluster initialization with autobootsrap disabled and parallel node setup""",
    )
    availability_zone: String = SctField(
        description="""Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).
              "Same for multi-region scenario.""",
    )
    aws_fallback_to_next_availability_zone: Boolean = SctField(
        description="Try all availability zones one by one in order to maximize the chances of getting the requested instance capacity.",
    )
    num_nodes_to_rollback: int = SctField(
        description="Number of nodes to upgrade and rollback in test_generic_cluster_upgrade",
    )
    upgrade_sstables: Boolean = SctField(
        description="Whether to upgrade sstables as part of upgrade_node or not",
    )
    enable_truncate_checks_on_node_upgrade: Boolean = SctField(
        description="Enables or disables truncate checks on each node upgrade and rollback",
    )
    stress_before_upgrade: StringOrList = SctField(
        description="Stress command to be run before upgrade (prepare stage)",
    )
    stress_during_entire_upgrade: StringOrList = SctField(
        description="Stress command to be run during the upgrade - user should take care for suitable duration",
    )
    stress_after_cluster_upgrade: StringOrList = SctField(
        description="Stress command to be run after full upgrade - usually used to read the dataset for verification",
    )

    # Jepsen test.
    jepsen_scylla_repo: String = SctField(
        description="Link to the git repository with Jepsen Scylla tests",
    )
    jepsen_test_cmd: StringOrList = SctField(
        description="Jepsen test command (e.g., 'test-all')",
    )
    jepsen_test_count: int = SctField(description="Possible number of reruns of single Jepsen test command")
    jepsen_test_run_policy: Literal["most", "any", "all"] = SctField(
        description="""
        Jepsen test run policy (i.e., what we want to consider as passed for a single test)

        'most' - most test runs are passed
        'any'  - one pass is enough
        'all'  - all test runs should pass
        """,
    )
    max_events_severities: StringOrList = SctField(
        default=[],
        description="Limit severity level for event types",
    )
    scylla_rsyslog_setup: Boolean = SctField(
        description="Configure rsyslog on Scylla nodes to send logs to monitoring nodes",
    )
    events_limit_in_email: int = SctField(
        description="Limit number events in email reports",
    )
    data_volume_disk_num: int = SctField(
        description="""Number of additional data volumes attached to instances
         if data_volume_disk_num > 0, then data volumes (ebs on aws) will be
         used for scylla data directory""",
    )
    data_volume_disk_type: Literal["gp2", "gp3", "io2", "io3", ""] = SctField(
        description="Type of additional volumes: gp2|gp3|io2|io3",
    )
    data_volume_disk_size: int = SctField(
        description="Size of additional volume in GB",
    )
    data_volume_disk_iops: int = SctField(
        description="Number of iops for ebs type io2|io3|gp3",
    )
    run_db_node_benchmarks: Boolean = SctField(
        description="Flag for running db node benchmarks before the tests",
    )
    nemesis_selector: MultitenantValue(StringOrList) = SctField(
        description="""nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has
        ALL the properties in that list which are set to true (the intersection of all properties).
        (In other words filters out all nemesis that doesn't ONE of these properties set to true)
        IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.""",
    )
    nemesis_exclude_disabled: MultitenantValue(BooleanOrList) = SctField(
        description="""nemesis_exclude_disabled determines whether 'disabled' nemeses are filtered out from list
        or are allowed to be used. This allows to easily disable too 'risky' or 'extreme' nemeses by default,
        for all longevities. For example: it is unwanted to run the ToggleGcModeMonkey in standard longevities
        that runs a stress with data validation.""",
    )
    nemesis_multiply_factor: MultitenantValue(IntOrList) = SctField(
        description="Multiply the list of nemesis to execute by the specified factor",
    )
    raid_level: int = SctField(
        description="Number of of raid level: 0 - RAID0, 5 - RAID5",
    )
    bare_loaders: Boolean = SctField(
        description="Don't install anything but node_exporter to the loaders during cluster setup",
    )
    stress_image: DictOrStr = SctField(
        description="Dict of the images to use for the stress tools",
    )
    enable_argus: Boolean = SctField(description="Control reporting to argus")
    cs_populating_distribution: String = SctField(
        description="set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests",
    )
    latte_schema_parameters: DictOrStr = SctField(
        description="""Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.
        For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}""",
    )
    num_loaders_step: int = SctField(
        description="Number of loaders which should be added per step",
    )
    stress_threads_start_num: int = SctField(
        description="Number of threads for c-s command",
    )
    num_threads_step: int = SctField(
        description="Number of threads which should be added on per step",
    )
    stress_step_duration: String = SctField(
        description="Duration of time for stress round",
    )
    max_deviation: float = SctField(
        description="Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one",
    )
    n_stress_process: int = SctField(
        description="Number of stress processes per loader",
    )
    stress_process_step: int = SctField(
        description="add/remove num of process on each round",
    )
    use_hdrhistogram: Boolean = SctField(
        description="Enable hdr histogram logging for cs",
    )
    stop_on_hw_perf_failure: Boolean = SctField(
        description="""Stop sct performance test if hardware performance test failed

    Hardware performance tests runs on each node with sysbench and cassandra-fio tools.
    Results stored in ES. HW perf tests run during cluster setups and not affect
    SCT Performance tests. Results calculated as average among all results for certain
    instance type or among all nodes during single run.
    if results for a single node is not in margin 0.01 of
    average result for all nodes, hw test considered as Failed.
    If stop_on_hw_perf_failure is True, then sct performance test will be terminated
       after hw perf tests detect node with hw results not in margin with average
    If stop_on_hw_perf_failure is False, then sct performance test will be run
       even after hw perf tests detect node with hw results not in margin with average""",
    )

    simulated_regions: Literal[0, 2, 3, 4, 5] = SctField(
        description="Number of simulated regions for the test",
    )
    simulated_racks: int = SctField(
        description="""Forces GossipingPropertyFileSnitch (regardless `endpoint_snitch`) to simulate racks.
         Provide number of racks to simulate.""",
    )
    rack_aware_loader: Boolean = SctField(
        description="When enabled, loaders will look for nodes on the same rack.",
    )
    use_dns_names: Boolean = SctField(
        description="""Use dns names instead of ip addresses for nodes in cluster""",
    )
    xcloud_credentials_path: String = SctField(
        description="Path to Scylla Cloud credentials file, if stored locally",
    )
    xcloud_env: String = SctField(
        description="Scylla Cloud environment (e.g., lab).",
    )
    xcloud_provider: String = SctField(
        description="Cloud provider for Scylla Cloud deployment (aws, gce)",
    )
    xcloud_replication_factor: int = SctField(
        description="Replication factor for Scylla Cloud cluster",
    )
    xcloud_vpc_peering: Annotated[dict, BeforeValidator(dict_or_str)] = SctField(
        description="""Dictionary of VPC peering parameters for private connectivity between
         SCT infrastructure and Scylla Cloud. The following parameters are used:
         enabled: bool - indicates whether VPC peering is to be used
         cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)
         cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)""",
    )
    xcloud_scaling_config: Annotated[dict | None, BeforeValidator(dict_or_str)] = SctField(
        description="""Scaling policy configuration. The payload should follow the following structure:

        {
            "InstanceFamilies": ["i8g"],
            "Mode": "xcloud",
            "Policies": {
                "Storage": {"Min": 0, "TargetUtilization": 0.8},
                "VCPU": {"Min": 0}
            }
        }

        - InstanceFamilies(list): instance families to use for scaling (e.g., ["i4i", "i8g"])
        - Mode(str): scaling mode, always "xcloud"
        - Policies(dict): scaling policies with the following keys:
            - Storage(dict):
                - Min(int): minimum storage in TB to maintain
                - TargetUtilization(float): target storage utilization from 0.7 to 0.9 with 0.05 step
            - VCPU(dict):
                - Min(int): minimum number of virtual CPUs to maintain

        For more details, see `scaling` parameter description in Cloud REST API documentation:
        https://cloud.docs.scylladb.com/stable/api.html#tag/Cluster/operation/createCluster""",
    )
    n_vector_store_nodes: int = SctField(
        description="Number of vector store nodes (0 = VS is disabled)",
    )
    vector_store_port: int = SctField(
        description="Vector Store API port",
    )
    vector_store_scylla_port: int = SctField(
        description="ScyllaDB connection port for Vector Store",
    )
    vector_store_threads: int = SctField(
        description="Vector Store indexing threads (if not set, defaults to number of CPU cores on VS node)",
    )
    download_from_s3: list = SctField(
        description="Destination-source map of dirs/buckets to download from S3 before starting the test",
    )
    argus_email_report_template: String = SctField(
        description="Path to the email report template used for sending argus email reports",
    )
    enable_argus_email_report: Boolean = SctField(
        description="Whether or not to send email using argus instead of SCT.",
    )
    c_s_driver_version: Literal["3", "4", "random"] = SctField(
        description="cassandra-stress driver version to use: 3|4|random",
    )

    required_params: Annotated[list, IgnoredType] = [
        "cluster_backend",
        "test_duration",
        "n_db_nodes",
        "n_loaders",
        "use_preinstalled_scylla",
        "user_credentials_path",
        "root_disk_size_db",
        "root_disk_size_monitor",
        "root_disk_size_loader",
    ]

    # those can be added to a json scheme to validate / or write the validation code for it to be a bit clearer output
    backend_required_params: Annotated[dict, IgnoredType] = {
        "aws": [
            "user_prefix",
            "instance_type_loader",
            "instance_type_monitor",
            "instance_type_db",
            "region_name",
            "ami_id_db_scylla",
            "ami_id_loader",
            "ami_id_monitor",
            "aws_root_disk_name_monitor",
            "ami_db_scylla_user",
            "ami_monitor_user",
            "scylla_network_config",
        ],
        "gce": [
            "user_prefix",
            "gce_network",
            "gce_image_db",
            "gce_image_username",
            "gce_instance_type_db",
            "gce_root_disk_type_db",
            "gce_n_local_ssd_disk_db",
            "gce_instance_type_loader",
            "gce_root_disk_type_loader",
            "gce_instance_type_monitor",
            "gce_root_disk_type_monitor",
            "gce_datacenter",
        ],
        "azure": [
            "user_prefix",
            "azure_image_db",
            "azure_image_username",
            "azure_instance_type_db",
            "azure_instance_type_loader",
            "azure_instance_type_monitor",
            "azure_region_name",
        ],
        "docker": ["user_credentials_path", "scylla_version"],
        "baremetal": ["s3_baremetal_config", "db_nodes_private_ip", "db_nodes_public_ip", "user_credentials_path"],
        "aws-siren": [
            "user_prefix",
            "instance_type_loader",
            "region_name",
            "cloud_credentials_path",
        ],
        "gce-siren": [
            "user_prefix",
            "gce_network",
            "gce_image_username",
            "gce_instance_type_db",
            "gce_root_disk_type_db",
            "gce_n_local_ssd_disk_db",
            "gce_instance_type_loader",
            "gce_root_disk_type_loader",
            "gce_instance_type_monitor",
            "gce_root_disk_type_monitor",
            "gce_datacenter",
        ],
        "k8s-local-kind": [
            "user_credentials_path",
            "scylla_version",
            "scylla_mgmt_agent_version",
            "k8s_scylla_operator_helm_repo",
            "k8s_scylla_cluster_name",
            "k8s_scylla_disk_gi",
            "mini_k8s_version",
            "mgmt_docker_image",
        ],
        "k8s-local-kind-aws": [
            "user_credentials_path",
            "scylla_version",
            "scylla_mgmt_agent_version",
            "k8s_scylla_operator_helm_repo",
            "k8s_scylla_cluster_name",
            "k8s_scylla_disk_gi",
            "mini_k8s_version",
            "mgmt_docker_image",
        ],
        "k8s-local-kind-gce": [
            "user_credentials_path",
            "scylla_version",
            "scylla_mgmt_agent_version",
            "k8s_scylla_operator_helm_repo",
            "k8s_scylla_cluster_name",
            "k8s_scylla_disk_gi",
            "mini_k8s_version",
            "mgmt_docker_image",
        ],
        "k8s-gke": [
            "gke_cluster_version",
            "gce_instance_type_db",
            "gce_root_disk_type_db",
            "gce_n_local_ssd_disk_db",
            "user_credentials_path",
            "scylla_version",
            "scylla_mgmt_agent_version",
            "k8s_scylla_operator_helm_repo",
            "k8s_scylla_cluster_name",
            "k8s_loader_cluster_name",
            "gce_instance_type_loader",
            "gce_image_monitor",
            "gce_instance_type_monitor",
            "gce_root_disk_type_monitor",
            "gce_n_local_ssd_disk_monitor",
            "mgmt_docker_image",
        ],
        "k8s-eks": [
            "instance_type_loader",
            "instance_type_monitor",
            "instance_type_db",
            "region_name",
            "ami_id_db_scylla",
            "ami_id_monitor",
            "aws_root_disk_name_monitor",
            "ami_db_scylla_user",
            "ami_monitor_user",
            "user_credentials_path",
            "scylla_version",
            "scylla_mgmt_agent_version",
            "k8s_scylla_operator_docker_image",
            "k8s_scylla_cluster_name",
            "k8s_loader_cluster_name",
            "mgmt_docker_image",
            "eks_service_ipv4_cidr",
            "eks_vpc_cni_version",
            "eks_role_arn",
            "eks_admin_arn",
            "eks_cluster_version",
            "eks_nodegroup_role_arn",
        ],
        "xcloud": ["user_prefix", "xcloud_provider", "scylla_version"],
    }

    defaults_config_files: Annotated[dict, IgnoredType] = {
        "aws": [sct_abs_path("defaults/aws_config.yaml")],
        "gce": [sct_abs_path("defaults/gce_config.yaml")],
        "azure": [sct_abs_path("defaults/azure_config.yaml")],
        "oci": [sct_abs_path("defaults/oci_config.yaml")],
        "docker": [sct_abs_path("defaults/docker_config.yaml")],
        "baremetal": [sct_abs_path("defaults/baremetal_config.yaml")],
        "aws-siren": [sct_abs_path("defaults/aws_config.yaml")],
        "gce-siren": [sct_abs_path("defaults/gce_config.yaml")],
        "k8s-local-kind": [sct_abs_path("defaults/k8s_local_kind_config.yaml")],
        "k8s-local-kind-aws": [
            sct_abs_path("defaults/aws_config.yaml"),
            sct_abs_path("defaults/k8s_local_kind_aws_config.yaml"),
            sct_abs_path("defaults/k8s_local_kind_config.yaml"),
        ],
        "k8s-local-kind-gce": [
            sct_abs_path("defaults/k8s_local_kind_gce_config.yaml"),
            sct_abs_path("defaults/k8s_local_kind_config.yaml"),
        ],
        "k8s-gke": [sct_abs_path("defaults/gce_config.yaml"), sct_abs_path("defaults/k8s_gke_config.yaml")],
        "k8s-eks": [sct_abs_path("defaults/aws_config.yaml"), sct_abs_path("defaults/k8s_eks_config.yaml")],
        "xcloud": [sct_abs_path("defaults/cloud_config.yaml")],
    }

    per_provider_multi_region_params: Annotated[dict, IgnoredType] = {
        "aws": ["region_name", "ami_id_db_scylla", "ami_id_loader"],
        "gce": ["gce_datacenter"],
    }

    xcloud_per_provider_required_params: Annotated[dict, IgnoredType] = {
        # There are two types of Cloud clusters available - Standard and XCloud
        # For XCloud clusters, the scaling policy (xcloud_scaling_config) includes instance type,
        # so it won't be provided in the params
        "standard": {
            "aws": ["region_name", "instance_type_db"],
            "gce": ["gce_datacenter", "gce_instance_type_db"],
        },
        "xcloud": {
            "aws": ["region_name"],
            "gce": ["gce_datacenter"],
        },
    }

    stress_cmd_params: Annotated[list, IgnoredType] = [
        # this list is used for variouse checks against stress commands, such as:
        # 1. Check if all c-s profile files existing that are referred in the commands
        # 2. Check what stress tools test is needed when loader is prepared
        "gemini_cmd",
        "stress_cmd",
        "stress_read_cmd",
        "stress_cmd_w",
        "stress_cmd_r",
        "stress_cmd_m",
        "prepare_write_cmd",
        "stress_cmd_no_mv",
        "stress_cmd_no_mv_profile",
        "prepare_stress_cmd",
        "stress_cmd_1",
        "stress_cmd_complex_prepare",
        "prepare_write_stress",
        "stress_cmd_read_10m",
        "stress_cmd_read_cl_one",
        "stress_cmd_complex_verify_read",
        "stress_cmd_complex_verify_more",
        "write_stress_during_entire_test",
        "verify_data_after_entire_test",
        "stress_cmd_read_cl_quorum",
        "verify_stress_after_cluster_upgrade",
        "stress_cmd_complex_verify_delete",
        "stress_cmd_lwt_mixed",
        "stress_cmd_lwt_de",
        "stress_cmd_lwt_dc",
        "stress_cmd_lwt_ue",
        "stress_cmd_lwt_uc",
        "stress_cmd_lwt_ine",
        "stress_cmd_lwt_d",
        "stress_cmd_lwt_u",
        "stress_cmd_lwt_i",
    ]
    ami_id_params: Annotated[list, IgnoredType] = [
        "ami_id_db_scylla",
        "ami_id_loader",
        "ami_id_monitor",
        "ami_id_db_cassandra",
        "ami_id_db_oracle",
    ]
    aws_supported_regions: Annotated[list, IgnoredType] = [
        "eu-west-1",
        "eu-west-2",
        "us-west-2",
        "us-east-1",
        "eu-north-1",
        "eu-central-1",
    ]

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
        self.multi_region_params = self.per_provider_multi_region_params.get(str(backend), [])

        # load docker images defaults
        self.load_docker_images_defaults()

        # 1) load the default backend config files
        files = anyconfig.load(list(backend_config_files))
        merge_dicts_append_strings(self, files)

        # 2) load the config files
        if config_files:
            for conf_file in list(config_files):
                if not os.path.exists(conf_file):
                    raise FileNotFoundError(f"Couldn't find config file: {conf_file}")
            files = anyconfig.load(list(config_files))
            merge_dicts_append_strings(self, files)

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
                            self[key] += " {}".format(value)
            else:
                for region in region_names:
                    if region not in self.aws_supported_regions:
                        raise ValueError(f"{region} isn't supported, use: {self.aws_supported_regions}")

        # 3) overwrite with environment variables
        merge_dicts_append_strings(self, env)

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

        # 5) overwrite AMIs
        for key in self.ami_id_params:
            if param := self.get(key):
                self[key] = convert_name_to_ami_if_needed(param, tuple(self.region_names))

        # 6) handle scylla_version if exists
        scylla_linux_distro = self.get("scylla_linux_distro")
        dist_type = scylla_linux_distro.split("-")[0]
        dist_version = scylla_linux_distro.split("-")[-1]

        if scylla_version := self.get("scylla_version"):
            if self.get("cluster_backend") in ["docker", "k8s-eks", "k8s-gke"] and not self.get("docker_image"):
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
                if isinstance(self.get("azure_region_name"), list):
                    azure_region_names = self.get("azure_region_name")
                else:
                    azure_region_names = [self.get("azure_region_name")]

                for region in azure_region_names:
                    try:
                        # Check if this is a full version tag
                        if parse_scylla_version_tag(scylla_version):
                            # Full version tag: use get_scylla_images for exact matching
                            azure_image = azure_utils.get_scylla_images(
                                scylla_version=scylla_version, region_name=region
                            )[0]
                        elif ":" in scylla_version:
                            # Branch version: use get_scylla_images
                            azure_image = azure_utils.get_scylla_images(
                                scylla_version=scylla_version, region_name=region
                            )[0]
                        else:
                            # Simple version: use get_released_scylla_images
                            azure_image = azure_utils.get_released_scylla_images(
                                scylla_version=scylla_version, region_name=region
                            )[0]
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError(
                            f"Azure Image for scylla_version='{scylla_version}' not found in {region}"
                        ) from ex
                    self.log.debug(
                        "Found Azure Image %s for scylla_version='%s' in %s", azure_image.name, scylla_version, region
                    )
                    scylla_azure_images.append(azure_image)
                self["azure_image_db"] = " ".join(
                    getattr(image, "id", None) or getattr(image, "unique_id", None) for image in scylla_azure_images
                )
            elif self.get("cluster_backend") == "xcloud" and ":" in scylla_version:
                self._resolve_xcloud_version_tag(self.get("scylla_version"))
            elif not self.get("scylla_repo"):
                self["scylla_repo"] = find_scylla_repo(scylla_version, dist_type, dist_version)
            else:
                raise ValueError(
                    "'scylla_version' can't used together with 'ami_id_db_scylla', 'gce_image_db' or with 'scylla_repo'"
                )

        # 6.1) handle oracle_scylla_version if exists
        if (oracle_scylla_version := self.get("oracle_scylla_version")) and self.get("db_type") == "mixed_scylla":
            if not self.get("ami_id_db_oracle") and self.get("cluster_backend") == "aws":
                ami_list = []
                for region in region_names:
                    aws_arch = get_arch_from_instance_type(self.get("instance_type_db_oracle"), region_name=region)
                    try:
                        if ":" in oracle_scylla_version:
                            ami = get_branched_ami(
                                scylla_version=oracle_scylla_version, region_name=region, arch=aws_arch
                            )[0]
                        else:
                            ami = get_scylla_ami_versions(
                                version=oracle_scylla_version, region_name=region, arch=aws_arch
                            )[0]
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError(
                            f"AMIs for oracle_scylla_version='{oracle_scylla_version}' not found in {region} arch={aws_arch}"
                        ) from ex

                    self.log.debug(
                        "Found AMI %s for oracle_scylla_version='%s' in %s", ami.image_id, oracle_scylla_version, region
                    )
                    ami_list.append(ami)
                self["ami_id_db_oracle"] = " ".join(ami.image_id for ami in ami_list)
            else:
                raise ValueError("'oracle_scylla_version' and 'ami_id_db_oracle' can't used together")

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
            user_prefix = "{}-{}".format(user_prefix, version_tag)
        if self.get("cluster_backend") == "azure":
            # for Azure need to shorten it more due longer region names
            prefix_max_len -= 2
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
                        f"item {param}, must be JSON but got error: {repr(exp)}"
                    ) from exp
                except TypeError as exp:
                    raise ValueError(f" Got error: {repr(exp)}, on item '{param}'") from exp

        # 15 Force endpoint_snitch to GossipingPropertyFileSnitch if using simulated_regions or simulated_racks
        n_db_nodes = self.get("n_db_nodes") or 0
        num_of_db_nodes = sum(n_db_nodes if isinstance(n_db_nodes, list) else [n_db_nodes])
        if (
            (self.get("simulated_regions") or 0) > 1
            or (self.get("simulated_racks") or 0) > 1
            and num_of_db_nodes > 1
            and cluster_backend != "docker"
        ):
            if snitch := self.get("endpoint_snitch"):
                assert snitch.endswith("GossipingPropertyFileSnitch"), (
                    f"Simulating racks requires endpoint_snitch to be GossipingPropertyFileSnitch while it set to {self['endpoint_snitch']}"
                )
            self["endpoint_snitch"] = "org.apache.cassandra.locator.GossipingPropertyFileSnitch"

        # 16 Validate use_dns_names
        if self.get("use_dns_names"):
            if cluster_backend not in ("aws",):
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

                nics.add(address_config["nic"])
                if address_config["address"] not in check_list:
                    continue

                check_list[address_config["address"]] = True

            if not_defined_address := ",".join([key for key, value in check_list.items() if value is None]):
                raise ValueError(f"Interface address(es) were not defined: {not_defined_address}")

            if len(nics) > 1 and len(self.region_names) >= 2:
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
                zero_nodes_num = (
                    [zero_nodes_num]
                    if isinstance(zero_nodes_num, int)
                    else [int(i) for i in str(zero_nodes_num).split()]
                )
                data_nodes_num = (
                    [data_nodes_num]
                    if isinstance(data_nodes_num, int)
                    else [int(i) for i in str(data_nodes_num).split()]
                )
                assert len(zero_nodes_num) == len(data_nodes_num), (
                    "Config of zero token nodes is not equal config of data nodes for multi dc"
                )

        # 21 validate performance throughput parameters
        if performance_throughput_params := self.get("perf_gradual_throttle_steps"):
            for workload, params in performance_throughput_params.items():
                if not isinstance(params, list):
                    raise ValueError(f"perf_gradual_throttle_steps for {workload} should be a list")

                if not (gradual_threads := self.get("perf_gradual_threads")):
                    raise ValueError("perf_gradual_threads should be defined for performance throughput test")

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

        if self.get("c_s_driver_version") == "random":
            self["c_s_driver_version"] = random.choice(["4", "3"])
            self.log.debug("Using random cassandra-stress driver version: %s", self["c_s_driver_version"])

    def load_docker_images_defaults(self):
        docker_images_dir = pathlib.Path(sct_abs_path("defaults/docker_images"))
        if docker_images_dir.is_dir():
            yaml_files = []
            for root, _, files in os.walk(docker_images_dir):
                yaml_files.extend([os.path.join(root, f) for f in files if f.endswith(".yaml")])
            if yaml_files:
                docker_images_defaults = anyconfig.load(yaml_files)
                stress_image = {key: value.get("image") for key, value in docker_images_defaults.items()}
                anyconfig.merge(self, dict(stress_image=stress_image))

    def log_config(self):
        self.log.info(self.dump_config())

    @property
    def total_db_nodes(self) -> List[int]:
        """Used to get total number of db nodes data nodes and zero nodes"""
        use_zero_nodes = self.get("use_zero_nodes")
        zero_nodes_num = self.get("n_db_zero_token_nodes")
        data_nodes_num = self.get("n_db_nodes")
        zero_nodes_num = (
            [zero_nodes_num] if isinstance(zero_nodes_num, int) else [int(i) for i in str(zero_nodes_num).split()]
        )
        data_nodes_num = (
            [data_nodes_num] if isinstance(data_nodes_num, int) else [int(i) for i in str(data_nodes_num).split()]
        )
        total_nodes = data_nodes_num[:]
        if use_zero_nodes and zero_nodes_num:
            total_nodes = [n1 + n2 for n1, n2 in zip(data_nodes_num, zero_nodes_num)]

        self.log.debug("Total nodes: %s", total_nodes)
        return total_nodes

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

    def _load_environment_variables(self):
        """Load configuration from environment variables.

        Custom implementation instead of Pydantic's BaseSettings because we need:
        1. Control over the order in which env vars are applied (after defaults, before config files)
        2. Support for appendable fields with '++' syntax via SCT_<FIELD>++<INDEX> pattern
        3. Custom validators (BeforeValidator) to be applied during env var parsing
        4. Backwards compatibility with existing SCT_* environment variable naming
        """
        environment_vars = {}
        for field_name, field in self.model_fields.items():
            if field.exclude or is_ignored_field(field):
                continue

            field_env = f"SCT_{field_name.upper()}"

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
                        environment_vars[field_name] = from_env_func(os.environ[field_env])
                    except Exception as ex:  # noqa: BLE001
                        raise ValueError("failed to parse {} from environment variable".format(field_env)) from ex
                nested_keys = [key for key in os.environ if key.startswith(field_env + ".")]
                if nested_keys:
                    list_value = []
                    dict_value = {}
                    for key in nested_keys:
                        nest_key, *_ = key.split(".")[1:]
                        if nest_key.isdigit():
                            list_value.insert(int(nest_key), os.environ.get(key))
                        else:
                            dict_value[nest_key] = os.environ.get(key)
                    current_value = environment_vars.get(field_name)
                    if current_value and isinstance(current_value, dict):
                        current_value.update(dict_value)
                    else:
                        environment_vars[field_name] = list_value or dict_value

        return environment_vars

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

        # Handle dict-based multitenancy - validate each value
        if (
            self.get("cluster_backend").startswith("k8s")
            and self.get("k8s_tenants_num") > 1
            and is_multitenant_field(field)
            and isinstance(param_value, dict)
            and len(param_value) > 1
        ):
            for tenant_key, tenant_value in param_value.items():
                try:
                    from_env_func(tenant_value)
                except Exception as ex:  # pylint: disable=broad-except  # noqa: BLE001
                    raise ValueError(f"failed to validate {field_name}[{tenant_key}]") from ex
            return

        # Handle list-based multitenancy - validate each value
        if (  # noqa: PLR0916
            self.get("cluster_backend").startswith("k8s")
            and self.get("k8s_tenants_num") > 1
            and is_multitenant_field(field)
            and isinstance(param_value, list)
            and len(param_value) > 1
        ) or (
            field_name == "nemesis_selector"
            and isinstance(self.get("nemesis_class_name"), str)
            and len(self.get("nemesis_class_name").split(" ")) > 1
        ):
            for list_element in param_value:
                try:
                    from_env_func(list_element)
                except Exception as ex:  # pylint: disable=broad-except  # noqa: BLE001
                    raise ValueError("failed to validate {}".format(field_name)) from ex
            return

        # Regular single-value validation
        from_env_func(param_value)

    @property
    def list_of_stress_tools(self) -> Set[str]:
        stress_tools = set()
        for param_name in self.stress_cmd_params:
            stress_cmds = self.get(param_name)
            if not (isinstance(stress_cmds, (list, str)) and stress_cmds):
                continue
            if isinstance(stress_cmds, str):
                stress_cmds = [stress_cmds]

            for stress_cmd in stress_cmds:
                if not stress_cmd:
                    continue
                if not isinstance(stress_cmd, list):
                    stress_cmd = [stress_cmd]  # noqa: PLW2901
                for cmd in stress_cmd:
                    if stress_tool := cmd.split(maxsplit=2)[0]:
                        stress_tools.add(stress_tool)

        return stress_tools

    def check_required_files(self):
        for param_name in self.stress_cmd_params:
            stress_cmds = self.get(param_name)
            if stress_cmds is None:
                continue
            if isinstance(stress_cmds, str):
                stress_cmds = [stress_cmds]
            for stress_cmd in stress_cmds:
                if not stress_cmd:
                    continue
                if not isinstance(stress_cmd, list):
                    stress_cmd = [stress_cmd]  # noqa: PLW2901
                for cmd in stress_cmd:
                    cmd = cmd.strip(" ")  # noqa: PLW2901
                    if cmd.startswith("latte"):
                        script_name_regx = re.compile(r"([/\w-]*\.rn)")
                        script_name = script_name_regx.search(cmd).group(1)
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

        if self.get("n_db_nodes"):
            self._validate_seeds_number()
            self._validate_nemesis_can_run_on_non_seed()
            self._validate_number_of_db_nodes_divides_by_az_number()

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
            for field_name, field in self.model_fields.items()
            if not is_ignored_field(field)
        }
        env_keys = {o.split(".")[0] for o in os.environ if o.startswith("SCT_")}
        unknown_env_keys = env_keys.difference(config_keys)
        if unknown_env_keys:
            output = ["{}={}".format(key, os.environ.get(key)) for key in unknown_env_keys]
            raise ValueError("Unsupported environment variables were used:\n\t - {}".format("\n\t - ".join(output)))

    def _validate_sct_variable_values(self):
        for field_name, field in self.model_fields.items():
            if is_ignored_field(field):
                continue
            if field_name in self and field.json_schema_extra:
                self._validate_value(field_name, field)

    def _check_multi_region_params(self, backend):
        region_param_names = {"aws": "region_name", "gce": "gce_datacenter"}
        current_region_param_name = region_param_names[backend]
        region_count = {}
        for opt in self.multi_region_params:
            val = self.get(opt)
            if isinstance(val, str):
                region_count[opt] = len(self.get(opt).split())
            elif isinstance(val, list):
                region_count[opt] = len(val)
            else:
                region_count[opt] = 1
        if not all(region_count[current_region_param_name] == x for x in region_count.values()):
            raise ValueError("not all multi region values are equal: \n\t{}".format(region_count))

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
        if self.get("nemesis_filter_seeds") is False or self.get("nemesis_class_name") == "NoOpMonkey":
            return
        seeds_num = self.get("seeds_num")
        num_of_db_nodes = sum(
            self.get("n_db_nodes") if isinstance(self.get("n_db_nodes"), list) else [self.get("n_db_nodes")]
        ) + int(self.get("add_node_cnt"))
        assert num_of_db_nodes > seeds_num, (
            "Nemesis cannot run when 'nemesis_filter_seeds' is true and seeds number is equal to nodes number"
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
            regions_count = len(self.region_names)
            assert az_count == 1 and regions_count == 1, (
                f"Number of Regions({regions_count}) and AZ({az_count}) should be 1 "
                f"when param use_placement_group is used"
            )

    def _validate_scylla_d_overrides_files_exists(self):
        if scylla_d_overrides_files := self.get("scylla_d_overrides_files"):
            for config_file_path in scylla_d_overrides_files:
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
            raise ValueError("Unsupported backend [{}]".format(backend))

    def _check_backend_defaults(self, backend, required_params):
        fields = [
            field_name
            for field_name, field in self.model_fields.items()
            if field_name in required_params and not is_ignored_field(field)
        ]
        for field in fields:
            assert self.get(field) is not None, "{} missing from config for {}".format(field, backend)

    def _instance_type_validation(self):
        if instance_type := self.get("nemesis_grow_shrink_instance_type"):
            backend = self.get("cluster_backend")
            match backend:
                case "aws":
                    for region in self.region_names:
                        assert aws_check_instance_type_supported(instance_type, region), (
                            f"Instance type[{instance_type}] not supported in region [{region}]"
                        )
                case "gce":
                    machine_types_client, info = get_gce_compute_machine_types_client()
                    for datacenter in self.gce_datacenters:
                        for zone in GCE_SUPPORTED_REGIONS.get(datacenter):
                            _zone = f"{datacenter}-{zone}"
                            assert gce_check_if_machine_type_supported(
                                machine_types_client, instance_type, project=info["project_id"], zone=_zone
                            ), f"Instance type[{instance_type}] not supported in zone [{_zone}]"
                case "azure":
                    if azure_region_names := self.get("azure_region_name"):
                        if not isinstance(azure_region_names, list):
                            azure_region_names = [self.get("azure_region_name")]
                        for region in self.get("azure_region_name"):
                            assert azure_check_instance_type_available(instance_type, region), (
                                f"Instance type [{instance_type}] not supported in region [{region}]"
                            )
                case _:
                    raise ValueError(f"Unsupported backend [{backend}] for using nemesis_grow_shrink_instance_type")

    def _check_version_supplied(self, backend: str):
        options_must_exist = []

        if not self.get("use_preinstalled_scylla") and not backend == "baremetal" and not self.get("unified_package"):
            options_must_exist += ["scylla_repo"]

        if self.get("db_type") == "cloud_scylla":
            options_must_exist += ["cloud_cluster_id"]
        elif backend == "aws":
            options_must_exist += ["ami_id_db_scylla"]
        elif backend == "gce":
            options_must_exist += ["gce_image_db"]
        elif backend == "azure":
            options_must_exist += ["azure_image_db"]
        elif backend == "docker":
            options_must_exist += ["docker_image"]
        elif backend == "baremetal":
            options_must_exist += ["db_nodes_public_ip"]
        elif "k8s" in backend or backend == "xcloud":
            options_must_exist += ["scylla_version"]

        if not options_must_exist:
            return
        assert all(self.get(o) for o in options_must_exist), (
            "scylla version/repos wasn't configured correctly\n"
            f"configure those options: {options_must_exist}\n"
            f"and those environment variables: {['SCT_' + o.upper() for o in options_must_exist]}"
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

    def verify_configuration_urls_validity(self):
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
                    logging.warning("'user_data_format_version' tag missing from [%s]: existing tags: %s", image, tags)
                self["user_data_format_version"] = tags.get("user_data_format_version", "2")

        if backend == "azure":
            azure_image_db = self.get("azure_image_db").split()
            for image in azure_image_db:
                tags = azure_utils.get_image_tags(image)
                if "user_data_format_version" not in tags.keys():
                    # since older release aren't tagged, we default to 2 which was the version on the first gce images
                    logging.warning("'user_data_format_version' tag missing from [%s]: existing tags: %s", image, tags)
                self["user_data_format_version"] = tags.get("user_data_format_version", "2")

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

        if unified_package := self.get("unified_package"):
            with tempfile.TemporaryDirectory() as tmpdirname:
                LOCALRUNNER.run(
                    shell_script_cmd(f"""
                    cd {tmpdirname}
                    curl {unified_package} -o ./unified_package.tar.gz
                    tar xvfz ./unified_package.tar.gz
                    """),
                    verbose=False,
                )

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
        return anyconfig.dumps(self.dict(exclude_none=True), ac_parser="yaml")

    @classmethod
    def get_annotations_as_strings(cls, field_type, field_metadata=None):  # noqa: PLR0911
        """Convert a type annotation to a human-readable string for configuration docs.

        Recursively resolves complex type annotations into clean, simplified
        strings.  The following annotation kinds are handled:

        * **MultitenantValue** – When *field_metadata* contains a
          ``MultitenantValueMarker``, the outer ``Union[T, dict[str, T]]``
          wrapper is unwrapped so only ``T`` is displayed.
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
                objects.  When a ``MultitenantValueMarker`` is present the
                outer multitenant ``Union`` is unwrapped to the inner type.

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

        # Check if this is a MultitenantValue type by checking metadata
        # Pydantic unwraps the Annotated type and stores metadata separately
        is_multitenant = False
        if field_metadata:
            for meta in field_metadata:
                if isinstance(meta, MultitenantValueMarker):
                    is_multitenant = True
                    break

        # If it's a multitenant field, the type is Union[T, dict[str, T]]
        # We want to extract just T (the first arg of the Union)
        if is_multitenant and origin is Union and args:
            # First arg is the inner type T
            inner_type = args[0]
            return cls.get_annotations_as_strings(inner_type, field_metadata=None)

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
        """
        defaults = anyconfig.load(sct_abs_path("defaults/test_default.yaml"))

        def strip_help_text(text):
            """
            strip all lines, and also remove empty lines from start or end
            """
            output = [l.strip() for l in text.splitlines()]
            return "\n".join(output[1 if not output[0] else 0 : -1 if not output[-1] else None])

        ret = strip_help_text(header)

        for field_name, field in cls.model_fields.items():
            if field.exclude or is_ignored_field(field):
                continue
            ret += "\n\n"
            if description := field.description:
                help_text = "<br>".join(strip_help_text(description).splitlines())
            else:
                help_text = ""

            appendable = "\n* appendable" if is_config_option_appendable(field_name) else ""
            default = defaults.get(field_name, None)
            default_text = default if default else "N/A"

            # Check if field supports k8s multitenancy
            multitenant_note = ""
            if is_multitenant_field(field):
                multitenant_note = "\n* supports k8s multitenancy - see [multitenancy docs](k8s-multitenancy.md)"

            # Pass field metadata to correctly handle MultitenantValue types
            field_metadata = getattr(field, "metadata", None)
            ret += dedent(f"""
                ## **{field_name}** / SCT_{field_name.upper()}

                {help_text}

                **default:** {default_text}

                **type:** {cls.get_annotations_as_strings(field.annotation, field_metadata=field_metadata)}
                """).strip()
            if appendable:
                ret += appendable
            if multitenant_note:
                ret += multitenant_note
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

        for field_name, field in cls.model_fields.items():
            if field.exclude or is_ignored_field(field):
                continue

            if description := field.description:
                help_text = "\n".join("# {}".format(l.strip()) for l in description.splitlines() if l.strip()) + "\n"
            else:
                help_text = ""
            default = defaults.get(field_name, None)
            default = default if default else "N/A"
            ret += "{help_text}{name}: {default}\n\n".format(help_text=help_text, default=default, name=field_name)

        return ret

    def _verify_data_volume_configuration(self, backend):
        dev_num = self.get("data_volume_disk_num")
        if dev_num == 0:
            return

        if backend not in ["aws", "k8s-eks"]:
            raise ValueError("Data volume configuration is supported only for aws, k8s-eks")

        if not self.get("data_volume_disk_size") or not self.get("data_volume_disk_type"):
            raise ValueError("Data volume configuration requires: data_volume_disk_type, data_volume_disk_size")

    def _verify_scylla_bench_mode_and_workload_parameters(self):
        for param_name in self.stress_cmd_params:
            stress_cmds = self.get(param_name)
            if stress_cmds is None:
                continue
            if isinstance(stress_cmds, str):
                stress_cmds = [stress_cmds]
            for stress_cmd in stress_cmds:
                if not stress_cmd:
                    continue
                if not isinstance(stress_cmd, list):
                    stress_cmd = [stress_cmd]  # noqa: PLW2901
                for cmd in stress_cmd:
                    cmd = cmd.strip(" ")  # noqa: PLW2901
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

        regions = self.get("simulated_regions") or len(self.region_names)
        availability_zone = self.get("availability_zone")
        racks_count = (
            simulated_racks
            if (simulated_racks := self.get("simulated_racks"))
            else len(availability_zone.split(","))
            if availability_zone
            else 1
        )
        if racks_count == 1 and regions == 1:
            raise ValueError(
                "Rack-aware validation can only be performed in multi-availability zone or multi-region environments."
            )

        loaders = sum(n_loaders if isinstance((n_loaders := self.get("n_loaders")), list) else [n_loaders])

        zones = racks_count * regions
        if loaders >= zones:
            raise ValueError("Rack-aware validation requires zones without loaders.")

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
        n_nodes = int(self.get("n_db_nodes"))
        if rf is None:
            self["xcloud_replication_factor"] = min(n_nodes, 3)
        elif rf > n_nodes:
            raise ValueError(f"xcloud_replication_factor ({rf}) cannot be greater than n_db_nodes ({n_nodes})")

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
