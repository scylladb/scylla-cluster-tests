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
        "aws-capacity-reservation",
        "aws-dedicated-host",
    ],
)
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(
    pipeline, test_data_dir, remote_connections, restore_root_logger
):
    pipeline_path = test_data_dir / "lint" / f"{pipeline}.jenkinsfile"
    # The capacity-reservation and dedicated-host lookups only run when `test_id` is set. A bare
    # local `sct.py lint-pipelines` leaves it unset; CI runs through `docker/env/hydra.sh`, which
    # always exports `SCT_TEST_ID`, so set it here to take the same path as CI.
    env = build_env(parse_jenkinsfile(pipeline_path)) | {"SCT_TEST_ID": "11111111-2222-3333-4444-555555555555"}

    is_error, message = validate_pipeline(pipeline_path, env)

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
