"""Constants, data classes, and presets for the staging trigger module."""

from dataclasses import dataclass, field

from sdcm.keystore import KeyStore
from sdcm.utils.get_username import get_username

CUSTOM_VALUE_SENTINEL = "__custom_value__"

ARGUS_URL = "https://argus.scylladb.com/tests/scylla-cluster-tests"
SCT_REPO = "git@github.com:scylladb/scylla-cluster-tests.git"


def _get_jenkins_url() -> str:
    """Get Jenkins URL from credentials."""
    creds = KeyStore().get_json("jenkins.json")
    return creds["url"].rstrip("/")


def _default_folder() -> str:
    return f"scylla-staging/{get_username()}"


# Maps jenkinsfile pipeline function to preset name
PIPELINE_TO_PRESET = {
    "longevityPipeline": "longevity",
    "managerPipeline": "manager",
    "artifactsPipeline": "artifacts",
    "rollingUpgradePipeline": "longevity",
    "perfRegressionParallelPipeline": "perf",
    "perfSearchBestConfigParallelPipeline": "perf",
    "jepsenPipeline": "longevity",
    "rollingOperatorUpgradePipeline": "longevity",
    "byoLongevityPipeline": "longevity",
}


@dataclass
class Preset:
    """Parameter preset for a job category."""

    params: dict[str, str]
    folder_prefix: str = field(default_factory=_default_folder)


def _build_presets() -> dict[str, Preset]:
    """Build presets with current username for email and requested_by_user."""
    username = get_username()
    email = f"{username}@scylladb.com"
    common = {"requested_by_user": username}

    return {
        "longevity": Preset(
            params={
                **common,
                "provision_type": "on_demand",
                "scylla_version": "master:latest",
                "email_recipients": email,
                "post_behavior_db_nodes": "destroy",
                "post_behavior_loader_nodes": "destroy",
                "post_behavior_monitor_nodes": "destroy",
                "ip_ssh_connections": "private",
                "extra_environment_variables": "",
                "region": "eu-west-1",
                "availability_zone": "a",
            },
        ),
        "manager": Preset(
            params={
                **common,
                "provision_type": "on_demand",
                "scylla_version": "master:latest",
                "scylla_repo": "",
                "scylla_ami_id": "",
                "gce_image_db": "",
                "azure_image_db": "",
                "manager_version": "master_latest",
                "scylla_mgmt_address": "",
                "scylla_mgmt_agent_address": "",
                "scylla_mgmt_pkg": "",
                "backup_bucket_backend": "",
                "backup_bucket_location": "",
                "email_recipients": email,
                "post_behavior_db_nodes": "destroy",
                "post_behavior_loader_nodes": "destroy",
                "post_behavior_monitor_nodes": "destroy",
                "ip_ssh_connections": "private",
                "extra_environment_variables": "",
                "region": "eu-west-1",
                "availability_zone": "a",
            },
        ),
        "artifacts": Preset(
            params={
                **common,
                "scylla_version": "",
                "scylla_repo": "",
                "email_recipients": email,
                "nonroot_offline_install": "false",
                "unified_package": "",
                "availability_zone": "",
                "ip_ssh_connections": "private",
                "post_behavior_db_nodes": "destroy",
            },
        ),
        "perf": Preset(
            params={
                **common,
                "provision_type": "on_demand",
                "scylla_version": "master:latest",
                "email_recipients": email,
                "post_behavior_db_nodes": "destroy",
                "post_behavior_monitor_nodes": "destroy",
                "extra_environment_variables": "",
                "region": "eu-west-1",
                "availability_zone": "a",
            },
        ),
        "dtest": Preset(
            params={
                **common,
                "SCYLLA_CCM_BRANCH": "",
                "SCYLLA_CCM_REPO": "",
                "SPLIT_FLEET_LABEL": "",
                "INCLUDE_DTESTS": "",
                "PYTEST_EXTRA_COMMANDLINE_OPTIONS": "--no-tablets",
            },
            folder_prefix="scylla-staging",
        ),
    }


# Lazy-initialized preset cache
_PRESETS: dict[str, Preset] | None = None


def get_presets() -> dict[str, Preset]:
    """Get presets, building them on first access."""
    global _PRESETS  # noqa: PLW0603
    if _PRESETS is None:
        _PRESETS = _build_presets()
    return _PRESETS


PRESET_NAMES = ["longevity", "manager", "artifacts", "perf", "dtest"]


DTEST_TOPOLOGY_FLAGS = {
    "no-tablets": "--no-tablets",
    "tablets": "--tablets",
    "gossip": "--force-gossip-topology-changes",
}

# Known choices for parameters that Jenkins stores as StringParameterDefinition
# but actually have a fixed set of valid values (from SCT config Literal types
# or Groovy pipeline definitions).
KNOWN_PARAM_CHOICES: dict[str, list[str]] = {
    "provision_type": ["spot", "on_demand", "spot_fleet", "spot_low_price"],
    "instance_provision_fallback_on_demand": ["true", "false"],
    "post_behavior_db_nodes": ["destroy", "keep", "keep-on-failure"],
    "post_behavior_loader_nodes": ["destroy", "keep", "keep-on-failure"],
    "post_behavior_monitor_nodes": ["destroy", "keep", "keep-on-failure"],
    "post_behavior_k8s_cluster": ["destroy", "keep", "keep-on-failure"],
    "post_behavior_vector_store_nodes": ["destroy", "keep", "keep-on-failure"],
    "post_behavior_dedicated_host": ["keep", "destroy"],
    "ip_ssh_connections": ["private", "public", "IPv6"],
    "nonroot_offline_install": ["true", "false"],
}


@dataclass
class ParamDefinition:
    """Metadata for a Jenkins job parameter definition."""

    default: str
    choices: list[str] | None = None


@dataclass
class TriggeredJob:
    """Record of a triggered job for checklist generation."""

    job_url: str
    build_number: int
    job_name: str
    description: str = ""
