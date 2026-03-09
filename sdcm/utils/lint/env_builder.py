"""Build SCT environment variables from parsed Jenkins pipeline parameters.

Converts PipelineConfig into the environment variable dict that
SCTConfiguration expects, mirroring the logic in vars/runSctTest.groovy.
"""

import json
import logging

from sdcm.utils.lint.jenkins_parser import PipelineConfig

logger = logging.getLogger(__name__)

# Mapping from pipeline parameter names to SCT_* environment variable names.
# "Direct" means the env var name follows the SCT_ + UPPER(param) convention.
# "Adapted" means the env var name differs from the parameter name.
PARAM_TO_ENV: dict[str, str] = {
    # Always set
    "backend": "SCT_CLUSTER_BACKEND",
    "test_config": "SCT_CONFIG_FILES",
    # Region/location
    "region": "SCT_REGION_NAME",
    "gce_datacenter": "SCT_GCE_DATACENTER",
    "azure_region_name": "SCT_AZURE_REGION_NAME",
    "availability_zone": "SCT_AVAILABILITY_ZONE",
    # Scylla version/image
    "scylla_version": "SCT_SCYLLA_VERSION",
    "scylla_ami_id": "SCT_AMI_ID_DB_SCYLLA",
    "gce_image_db": "SCT_GCE_IMAGE_DB",
    "azure_image_db": "SCT_AZURE_IMAGE_DB",
    "scylla_repo": "SCT_SCYLLA_REPO",
    "new_scylla_repo": "SCT_NEW_SCYLLA_REPO",
    "new_version": "SCT_NEW_VERSION",
    "oracle_scylla_version": "SCT_ORACLE_SCYLLA_VERSION",
    "docker_image": "SCT_DOCKER_IMAGE",
    # Provisioning
    "provision_type": "SCT_INSTANCE_PROVISION",
    "instance_provision_fallback_on_demand": "SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND",
    "linux_distro": "SCT_SCYLLA_LINUX_DISTRO",
    "use_preinstalled_scylla": "SCT_USE_PREINSTALLED_SCYLLA",
    "ip_ssh_connections": "SCT_IP_SSH_CONNECTIONS",
    # Manager
    "manager_version": "SCT_MANAGER_VERSION",
    "target_manager_version": "SCT_TARGET_MANAGER_VERSION",
    "scylla_mgmt_agent_version": "SCT_SCYLLA_MGMT_AGENT_VERSION",
    "scylla_mgmt_agent_address": "SCT_SCYLLA_MGMT_AGENT_ADDRESS",
    "scylla_mgmt_address": "SCT_SCYLLA_MGMT_ADDRESS",
    "backup_bucket_backend": "SCT_BACKUP_BUCKET_BACKEND",
    # K8s operator
    "k8s_scylla_operator_docker_image": "SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE",
    "k8s_scylla_operator_upgrade_docker_image": "SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE",
    "k8s_scylla_operator_helm_repo": "SCT_K8S_SCYLLA_OPERATOR_HELM_REPO",
    "k8s_scylla_operator_upgrade_helm_repo": "SCT_K8S_SCYLLA_OPERATOR_UPGRADE_HELM_REPO",
    "k8s_scylla_operator_chart_version": "SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION",
    "k8s_scylla_operator_upgrade_chart_version": "SCT_K8S_SCYLLA_OPERATOR_UPGRADE_CHART_VERSION",
    "k8s_enable_tls": "SCT_K8S_ENABLE_TLS",
    "k8s_enable_sni": "SCT_K8S_ENABLE_SNI",
    "k8s_enable_performance_tuning": "SCT_K8S_ENABLE_PERFORMANCE_TUNING",
    "k8s_log_api_calls": "SCT_K8S_LOG_API_CALLS",
    "k8s_deploy_monitoring": "SCT_K8S_DEPLOY_MONITORING",
    "k8s_scylla_utils_docker_image": "SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE",
    # xcloud
    "xcloud_provider": "SCT_XCLOUD_PROVIDER",
    "xcloud_env": "SCT_XCLOUD_ENV",
    # Stress
    "stress_duration": "SCT_STRESS_DURATION",
    "prepare_stress_duration": "SCT_PREPARE_STRESS_DURATION",
    "gemini_seed": "SCT_GEMINI_SEED",
    # Post behavior
    "post_behavior_db_nodes": "SCT_POST_BEHAVIOR_DB_NODES",
    "post_behavior_loader_nodes": "SCT_POST_BEHAVIOR_LOADER_NODES",
    "post_behavior_monitor_nodes": "SCT_POST_BEHAVIOR_MONITOR_NODES",
    "post_behavior_k8s_cluster": "SCT_POST_BEHAVIOR_K8S_CLUSTER",
    "post_behavior_vector_store_nodes": "SCT_POST_BEHAVIOR_VECTOR_STORE_NODES",
    # Misc
    "disable_raft": "SCT_DISABLE_RAFT",
    "internode_compression": "SCT_INTERNODE_COMPRESSION",
    "update_db_packages": "SCT_UPDATE_DB_PACKAGES",
    "pytest_addopts": "PYTEST_ADDOPTS",
    "test_email_title": "SCT_EMAIL_SUBJECT_POSTFIX",
    "stop_on_hw_perf_failure": "SCT_STOP_ON_HW_PERF_FAILURE",
}

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
    }
)

# Backends that require placeholder image values when lazy image resolution
# is not available
_IMAGE_PLACEHOLDER_BACKENDS = {
    "aws": {"SCT_AMI_ID_DB_SCYLLA": "ami-placeholder"},
    "gce": {"SCT_GCE_IMAGE_DB": "https://placeholder-image"},
    "azure": {"SCT_AZURE_IMAGE_DB": "azure-placeholder-image"},
}

# Placeholder for scylla repo when needed
_SCYLLA_REPO_PLACEHOLDER = "https://placeholder-repo.scylladb.com"


def _count_regions(config: PipelineConfig) -> int:
    """Count the number of regions from pipeline region parameters."""
    region_str = config.params.get("region", "") or config.params.get("gce_datacenter", "")
    if not region_str:
        return 1

    # Try parsing as JSON array
    try:
        regions = json.loads(region_str.replace("'", '"'))
        if isinstance(regions, list):
            return len(regions)
    except json.JSONDecodeError, ValueError:
        pass

    # Space-separated regions
    if " " in region_str:
        return len(region_str.split())

    return 1


def _needs_image_placeholders() -> bool:
    """Check if we need placeholder image values.

    Returns True if SCTConfiguration still requires images at init time
    (i.e., PR #13877 Phase 2 lazy image resolution is not yet available).
    """
    try:
        # Deferred import: SCTConfiguration triggers heavy Docker/cloud SDK imports
        from sdcm.sct_config import SCTConfiguration  # noqa: PLC0415

        return not hasattr(SCTConfiguration, "resolve_images")
    except ImportError:
        return True


def build_env(config: PipelineConfig) -> dict[str, str]:
    """Convert a PipelineConfig into SCT environment variables.

    Mirrors the parameter-to-env-var mapping in vars/runSctTest.groovy.

    Args:
        config: Parsed pipeline configuration.

    Returns:
        Dictionary of environment variable names to values.
    """
    env: dict[str, str] = {}
    need_placeholders = _needs_image_placeholders()

    for param_name, param_value in config.params.items():
        if not param_value:
            continue

        # Check direct mapping
        env_var = PARAM_TO_ENV.get(param_name)
        if env_var:
            env[env_var] = param_value
            continue

        # k8s_version maps to both EKS and GKE cluster versions
        if param_name == "k8s_version":
            env["SCT_EKS_CLUSTER_VERSION"] = param_value
            env["SCT_GKE_CLUSTER_VERSION"] = param_value
            continue

        # requested_by_user → BUILD_USER_REQUESTED_BY (conditional)
        if param_name == "requested_by_user":
            env["BUILD_USER_REQUESTED_BY"] = param_value
            continue

        # perf_extra_jobs_to_compare → JSON-encoded
        if param_name == "perf_extra_jobs_to_compare":
            env["SCT_PERF_EXTRA_JOBS_TO_COMPARE"] = json.dumps(param_value)
            continue

        # email_recipients → JSON-encoded
        if param_name == "email_recipients":
            env["SCT_EMAIL_RECIPIENTS"] = json.dumps(param_value)
            continue

        # extra_environment_variables → parse and export directly
        if param_name == "extra_environment_variables":
            _parse_extra_env_vars(param_value, env)
            continue

        # Skip known non-env params
        if param_name in NON_ENV_PARAMS:
            continue

        logger.warning("Unmapped pipeline parameter '%s' in %s", param_name, config.file_path)

    # Ensure test_config is JSON-encoded (as runSctTest.groovy does via JsonOutput.toJson)
    if config.test_config:
        env["SCT_CONFIG_FILES"] = json.dumps(config.test_config)

    # Generate placeholder images for backends that require them
    if need_placeholders:
        backend = config.backend or ""
        _add_image_placeholders(env, backend, _count_regions(config))

    # Handle baremetal placeholder IPs
    if config.backend == "baremetal" and "SCT_DB_NODES_PUBLIC_IP" not in env:
        env["SCT_DB_NODES_PUBLIC_IP"] = json.dumps(["127.0.0.1", "127.0.0.2"])

    return env


def _parse_extra_env_vars(raw: str, env: dict[str, str]) -> None:
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
            if env_var == "SCT_AMI_ID_DB_SCYLLA" and num_regions > 1:
                # Multi-region needs matching number of AMI placeholders
                env[env_var] = " ".join([placeholder] * num_regions)
            else:
                env[env_var] = placeholder

    # Add scylla_repo placeholder if not set and backend needs it
    if base_backend in ("aws", "gce", "azure") and "SCT_SCYLLA_REPO" not in env and "SCT_SCYLLA_VERSION" not in env:
        env["SCT_SCYLLA_REPO"] = _SCYLLA_REPO_PLACEHOLDER
