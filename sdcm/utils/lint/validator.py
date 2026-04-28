"""Validation engine for Jenkins pipeline configurations.

Validates SCT configurations derived from Jenkins pipeline files by
instantiating SCTConfiguration with the pipeline-derived environment.
"""

import contextlib
import logging
import os
import pathlib
import traceback
from collections import namedtuple
from pathlib import Path
from unittest.mock import patch

logger = logging.getLogger(__name__)

# Lightweight stub for AMI/GCE image objects returned by cloud lookup functions
_FakeImage = namedtuple("_FakeImage", ["image_id", "name", "self_link"])
_FAKE_IMAGE = _FakeImage(image_id="ami-lint-placeholder", name="lint-placeholder", self_link="lint-placeholder")


def _check_file_exists_skip_credentials(value: str) -> None:
    """Skip credential keys (fetched from AWS KeyStore at runtime), validate everything else."""
    expanded = pathlib.Path(value).expanduser()
    if expanded.parent == pathlib.Path.home() / ".ssh":
        return
    if not expanded.is_file():
        raise ValueError(f"{value} isn't an existing file")


class _FakeKeyStore:
    """Stub KeyStore that avoids S3 calls during linting."""

    def get_json(self, *a, **kw):
        return {}

    def get_file_contents(self, *a, **kw):
        return ""

    def get_cloud_rest_credentials(self, *a, **kw):
        return {"base_url": "https://lint", "api_token": "lint"}


# Functions in SCTConfiguration.__init__ that make real cloud API calls.
# We patch them to avoid network I/O during linting.
_CLOUD_API_PATCHES = {
    # AMI name → AMI ID resolution (EC2 DescribeImages)
    "sdcm.sct_config.convert_name_to_ami_if_needed": lambda param, regions: param,
    # scylla_version → S3 repo URL lookup
    "sdcm.sct_config.find_scylla_repo": lambda *a, **kw: "https://lint-placeholder-repo",
    # EC2 AMI version lookup
    "sdcm.sct_config.get_scylla_ami_versions": lambda **kw: [_FAKE_IMAGE],
    # EC2 branched AMI lookup
    "sdcm.sct_config.get_branched_ami": lambda **kw: [_FAKE_IMAGE],
    # GCE image version lookup
    "sdcm.sct_config.get_scylla_gce_images_versions": lambda **kw: [_FAKE_IMAGE],
    # GCE branched image lookup
    "sdcm.sct_config.get_branched_gce_images": lambda **kw: [_FAKE_IMAGE],
    # EC2 instance type → architecture resolution (DescribeInstanceTypes)
    "sdcm.sct_config.get_arch_from_instance_type": lambda *a, **kw: "x86_64",
    # EC2 instance type validation
    "sdcm.sct_config.aws_check_instance_type_supported": lambda *a, **kw: True,
    # Azure image lookup
    "sdcm.provision.azure.utils.get_scylla_images": lambda **kw: [_FAKE_IMAGE],
    # Azure instance type validation
    "sdcm.sct_config.azure_check_instance_type_available": lambda *a, **kw: True,
    # KeyStore reads credentials from S3 — not needed for config structure validation
    "sdcm.sct_config.KeyStore": _FakeKeyStore,
    # OCI image tag lookup (verify_configuration_urls_validity)
    "sdcm.utils.oci_utils.get_image_tags": lambda *a, **kw: {
        "user_data_format_version": "3",
        "scylla_version": "2024.2.0",
    },
    # Azure image tag lookup (verify_configuration_urls_validity)
    "sdcm.provision.azure.utils.get_image_tags": lambda *a, **kw: {
        "user_data_format_version": "2",
        "scylla_version": "2024.2.0",
    },
    # Credential keys (e.g. ~/.ssh/) are fetched from AWS KeyStore at runtime — skip them
    "sdcm.sct_config._check_file_exists": _check_file_exists_skip_credentials,
}


def validate_pipeline(pipeline_path: Path, env: dict[str, str]) -> tuple[bool, str]:
    """Validate a single pipeline's SCT configuration.

    Sets the given environment variables, instantiates SCTConfiguration,
    and runs verify_configuration() and check_required_files().

    Cloud API calls (AMI lookups, repo resolution, etc.) are mocked out
    since linting only needs to verify configuration structure, not
    resolve real cloud resources.

    Args:
        pipeline_path: Path to the .jenkinsfile (for error messages).
        env: Environment variables to set (from build_env()).

    Returns:
        Tuple of (is_error, error_message). Empty message on success.
    """
    # Suppress logging noise from SCTConfiguration
    logging.getLogger().handlers = []
    logging.getLogger().disabled = True

    try:
        # Deferred import: runs in worker process to avoid importing heavy deps in main process
        from sdcm.sct_config import SCTConfiguration  # noqa: PLC0415

        with patch.dict(os.environ, env, clear=True), contextlib.ExitStack() as stack:
            for target, mock_fn in _CLOUD_API_PATCHES.items():
                stack.enter_context(patch(target, side_effect=mock_fn))
            stack.enter_context(patch.object(SCTConfiguration, "_validate_cloud_backend_parameters"))
            stack.enter_context(patch.object(SCTConfiguration, "_validate_docker_backend_parameters"))
            stack.enter_context(patch.object(SCTConfiguration, "_resolve_xcloud_version_tag"))
            config = SCTConfiguration()
            config.verify_configuration()
            config.check_required_files()
            return False, ""
    except Exception as exc:  # noqa: BLE001
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return True, tb
