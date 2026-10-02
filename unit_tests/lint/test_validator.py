"""Unit tests for the pipeline linter's cloud-API isolation.

Linting only validates configuration structure, so it must never reach a cloud provider. Each
test pipeline under `unit_tests/test_data/lint/` drives `SCTConfiguration` down one cloud-lookup
path; linting it with the network refused proves that path is stubbed in `_CLOUD_API_PATCHES`.
"""

from pathlib import Path

import pytest

from sdcm.utils.lint.env_builder import build_env
from sdcm.utils.lint.jenkins_parser import parse_jenkinsfile
from sdcm.utils.lint.validator import validate_pipeline

TEST_PIPELINES_DIR = Path(__file__).parent.parent / "test_data" / "lint"

# The capacity-reservation and dedicated-host lookups only run when `test_id` is set. A bare local
# `sct.py lint-pipelines` leaves it unset; CI runs through `docker/env/hydra.sh`, which always
# exports `SCT_TEST_ID`, so set it here to take the same path as CI.
CI_ENV = {"SCT_TEST_ID": "11111111-2222-3333-4444-555555555555"}


@pytest.mark.parametrize(
    "pipeline",
    [
        pytest.param("azure-released-image", id="azure-released-image"),
        pytest.param("azure-branched-image", id="azure-branched-image"),
        pytest.param("oci-released-image", id="oci-released-image"),
        pytest.param("oci-branched-image", id="oci-branched-image"),
        pytest.param("aws-capacity-reservation", id="aws-capacity-reservation"),
        pytest.param("aws-dedicated-host", id="aws-dedicated-host"),
    ],
)
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(pipeline, remote_connections, restore_root_logger):
    pipeline_path = TEST_PIPELINES_DIR / f"{pipeline}.jenkinsfile"
    env = build_env(parse_jenkinsfile(pipeline_path)) | CI_ENV

    is_error, message = validate_pipeline(pipeline_path, env)

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
