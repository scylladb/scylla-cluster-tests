"""SCT support for minicloud — a local QEMU/KVM cloud emulator.

Public API re-exported here; the implementation lives in the submodules:
endpoint (activation predicates), config, activation (probe/env/validation),
preflight, networking, gcp, log_collection, manager, bootstrap.
"""

from sdcm.utils.minicloud.activation import (
    check_minicloud_reachability,
    set_minicloud_endpoint_env,
    validate_minicloud_params,
)
from sdcm.utils.minicloud.bootstrap import ensure_minicloud_ready
from sdcm.utils.minicloud.config import (
    MINICLOUD_CONTAINER_NAME,
    MINICLOUD_DEFAULT_REGION,
    MINICLOUD_GCP_PROJECT_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT,
    MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT,
    MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT,
    MINICLOUD_STATE_DIR_DEFAULT,
    MinicloudConfig,
    MinicloudError,
    default_minicloud_image,
    resolve_minicloud_default_region,
    resolve_minicloud_regions,
)
from sdcm.utils.minicloud.endpoint import MINICLOUD_PORT, get_minicloud_endpoint, is_minicloud_active
from sdcm.utils.minicloud.log_collection import collect_minicloud_logs, redact_docker_inspect
from sdcm.utils.minicloud.manager import MinicloudManager

__all__ = [
    "MINICLOUD_CONTAINER_NAME",
    "MINICLOUD_DEFAULT_REGION",
    "MINICLOUD_GCP_PROJECT_DEFAULT",
    "MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT",
    "MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT",
    "MINICLOUD_PORT",
    "MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT",
    "MINICLOUD_STATE_DIR_DEFAULT",
    "MinicloudConfig",
    "MinicloudError",
    "MinicloudManager",
    "check_minicloud_reachability",
    "collect_minicloud_logs",
    "default_minicloud_image",
    "ensure_minicloud_ready",
    "get_minicloud_endpoint",
    "is_minicloud_active",
    "redact_docker_inspect",
    "resolve_minicloud_default_region",
    "resolve_minicloud_regions",
    "set_minicloud_endpoint_env",
    "validate_minicloud_params",
]
