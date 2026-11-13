"""Build SCT environment variables from parsed Jenkins pipeline parameters.

Converts PipelineConfig into the environment variable dict that
SCTConfiguration expects, with the same generated mapping the Jenkins steps use (sdcm/utils/pipeline_env.py).
"""

import json
import logging

from sdcm.sct_config import BACKEND_IMAGE_FIELD, SCTConfiguration
from sdcm.sct_config import count_regions as count_regions_from_string
from sdcm.utils.lint.jenkins_parser import PipelineConfig
from sdcm.utils.pipeline_env import PipelineInput, option_params, pipeline_env

logger = logging.getLogger(__name__)

# Required params that Jenkins injects at runtime (their default in the pipeline
# definition is empty), so they are never pinned in the pipeline file itself.
# During linting we fill a working placeholder for any backend that lists them as
# required, so the check validates config structure instead of failing on a
# runtime-only param. Keyed by param name -> (SCT_* env var, placeholder value).
#   - scylla_version: chosen so resolution stays offline —
#     get_scylla_docker_repo_from_version() maps it to a nightly repo and
#     _replace_docker_image_latest_tag() only hits the network for "latest".
#     The numeric part must stay representative of what an unpinned pipeline actually
#     runs (master / latest release), because validations may compare it: a 0.0.0
#     placeholder read as "ancient Scylla" and failed every version-gated check.
_RUNTIME_REQUIRED_PARAM_PLACEHOLDERS: dict[str, tuple[str, str]] = {
    "scylla_version": ("SCT_SCYLLA_VERSION", "2026.1.0-lint-placeholder"),
}

# Precompute, per backend, the placeholder env vars to inject — derived from
# backend_required_params so it stays in sync as required params change.
_BACKEND_RUNTIME_PLACEHOLDERS: dict[str, dict[str, str]] = {}
for _param, (_env_var, _value) in _RUNTIME_REQUIRED_PARAM_PLACEHOLDERS.items():
    for _backend, _required in SCTConfiguration.model_fields["backend_required_params"].default.items():
        if _param in _required:
            _BACKEND_RUNTIME_PLACEHOLDERS.setdefault(_backend, {})[_env_var] = _value

# Parameters that are not mapped to environment variables — used for
# pipeline orchestration only
NON_ENV_PARAMS = frozenset(
    {
        "test_name",
        "timeout",
        "sub_tests",
        "base_versions",
        "builds_to_keep",
        "functional_test",
        "downstream_jobs_to_run",
        "mgmt_reuse_backup_snapshot_name",
        "backup_bucket_location",
        # base_version_all_sts_versions is a CLI flag for get_supported_scylla_base_versions.py,
        # not an SCT env var
        "base_version_all_sts_versions",
        # post_behaviour is a pipeline orchestration shorthand (British spelling alias);
        # individual post_behavior_* params are the canonical SCT env vars
        "post_behaviour",
        # minicloud selects an execution mode rather than an SCT setting: it adds the
        # 'Start Minicloud' stage, and the endpoint env var is exported by the pipeline at
        # runtime rather than coming from the jenkinsfile. See vars/startMinicloud.groovy.
        "minicloud",
        # minicloud_docker maps to SCT_MINICLOUD_DOCKER_IMAGE (the minicloud_docker_image option), but only when minicloud is on -
        # the pipeline decides, so it is not a static jenkinsfile-to-env mapping
        "minicloud_docker",
        # local_agent picks the topology (KVM Jenkins agent vs sct-runner), which decides which
        # stages run and which agent label is resolved - pipeline orchestration, not an SCT setting
        "local_agent",
        # jenkins_label pins the build to a Jenkins agent (getJenkinsLabels.groovy) - not an SCT setting
        "jenkins_label",
    }
)

# Build placeholder image env vars from the canonical mapping in sct_config.
# Each backend's image field gets a "SCT_" + UPPER(field) env var with a placeholder.
_IMAGE_PLACEHOLDER_BACKENDS: dict[str, dict[str, str]] = {
    backend: {f"SCT_{field.upper()}": f"{backend}-lint-placeholder"} for backend, field in BACKEND_IMAGE_FIELD.items()
}

# Placeholder for scylla repo when needed
_SCYLLA_REPO_PLACEHOLDER = "https://placeholder-repo.scylladb.com"


def count_regions(config: PipelineConfig) -> int:
    """Count the number of regions from pipeline region parameters.

    Only AWS uses 'region' and GCE uses 'gce_datacenter' for multi-region.
    Azure uses 'azure_region_name' (single-region only) and Docker has no region concept.
    """
    region_str = config.params.get("region", "") or config.params.get("gce_datacenter", "")
    return count_regions_from_string(region_str)


def build_env(config: PipelineConfig) -> dict[str, str]:
    """Convert a PipelineConfig into SCT environment variables.

    The jenkinsfile arguments go through the generated mapping of sdcm/utils/pipeline_env.py.

    Args:
        config: Parsed pipeline configuration.

    Returns:
        Dictionary of environment variable names to values.
    """
    env: dict[str, str] = {}

    # Pipeline functions default backend to 'aws' when not specified (see longevityPipeline.groovy)
    if not config.backend:
        env["SCT_CLUSTER_BACKEND"] = "aws"

    env |= pipeline_env(PipelineInput(pipeline_params=config.params))

    known_params = {param for params in option_params().values() for param in params}
    for param_name, param_value in config.params.items():
        if not param_value:
            continue
        # extra_environment_variables → parse and export directly
        if param_name == "extra_environment_variables":
            parse_extra_env_vars(param_value, env)
        elif param_name not in known_params and param_name not in NON_ENV_PARAMS:
            logger.warning("Unmapped pipeline parameter '%s' in %s", param_name, config.file_path)

    # the parser's list, since the raw value can be a Groovy list literal rather than JSON
    if config.test_config:
        env["SCT_CONFIG_FILES"] = json.dumps(config.test_config)

    # Generate placeholder images for backends that require them
    effective_backend = env.get("SCT_CLUSTER_BACKEND", "")
    _add_image_placeholders(env, effective_backend, count_regions(config))

    # Handle baremetal placeholder IPs
    if effective_backend == "baremetal" and "SCT_DB_NODES_PUBLIC_IP" not in env:
        env["SCT_DB_NODES_PUBLIC_IP"] = json.dumps(["127.0.0.1", "127.0.0.2"])

    return env


def parse_extra_env_vars(raw: str, env: dict[str, str]) -> None:
    """Parse extra_environment_variables string and add to env dict.

    Handles single 'KEY=val' strings and space-separated multiple vars.
    """
    for part in raw.split():
        if "=" in part:
            key, _, value = part.partition("=")
            env[key] = value
        else:
            logger.warning("Could not parse extra_environment_variable: %s", part)


def _add_image_placeholders(env: dict[str, str], backend: str, num_regions: int) -> None:
    """Add placeholder image/AMI values for backends that need them.

    Only adds placeholders if the corresponding env var is not already set
    (e.g., from the pipeline's own scylla_ami_id parameter).
    """
    # Determine the base backend for k8s variants (k8s-eks → aws, k8s-gke → gce)
    base_backend = backend
    if backend.startswith("k8s-") and "eks" in backend:
        base_backend = "aws"
    elif backend.startswith("k8s-") and "gke" in backend:
        base_backend = "gce"
    elif backend.startswith("k8s-local-kind"):
        base_backend = "aws"

    placeholders = _IMAGE_PLACEHOLDER_BACKENDS.get(base_backend, {})
    for env_var, placeholder in placeholders.items():
        if env_var not in env:
            if num_regions > 1 and base_backend == "aws":
                # Multi-region needs matching number of AMI placeholders
                env[env_var] = " ".join([placeholder] * num_regions)
            else:
                env[env_var] = placeholder

    # Multi-region AWS also needs loader AMI placeholders to match region count
    if base_backend == "aws" and num_regions > 1 and "SCT_AMI_ID_LOADER" not in env:
        env["SCT_AMI_ID_LOADER"] = " ".join(["aws-lint-placeholder"] * num_regions)

    # Add scylla_repo placeholder if not set and backend needs it
    if (
        base_backend in ("aws", "gce", "azure", "oci", "baremetal")
        and "SCT_SCYLLA_REPO" not in env
        and "SCT_SCYLLA_VERSION" not in env
    ):
        env["SCT_SCYLLA_REPO"] = _SCYLLA_REPO_PLACEHOLDER

    # Some required params (i.e. scylla_version)
    # are supplied by Jenkins at runtime and never pinned in the pipeline file.
    # Inject placeholders for those so linting validates config structure instead
    # of failing on a runtime-only param — they stay mandatory for the actual check.
    for env_var, value in _BACKEND_RUNTIME_PLACEHOLDERS.get(backend, {}).items():
        env.setdefault(env_var, value)
