"""Unit tests for the pipeline linter's cloud-API isolation.

Linting only validates configuration structure, so it must never reach a cloud provider. Each
test pipeline under `unit_tests/test_data/lint/` drives `SCTConfiguration` down one cloud-lookup
path; linting it with the network refused proves that path is stubbed in `_CLOUD_API_PATCHES`.
"""

import pytest

from sdcm.utils.lint.env_builder import build_env
from sdcm.utils.lint.jenkins_parser import parse_jenkinsfile
from sdcm.utils.lint.validator import validate_pipeline


@pytest.mark.parametrize(
    "pipeline",
    [
        "azure-released-image",
        "azure-branched-image",
        "oci-released-image",
        "oci-branched-image",
    ],
)
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(
    pipeline, test_data_dir, remote_connections, restore_root_logger
):
    pipeline_path = test_data_dir / "lint" / f"{pipeline}.jenkinsfile"
    env = build_env(parse_jenkinsfile(pipeline_path))

    is_error, message = validate_pipeline(pipeline_path, env)

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
