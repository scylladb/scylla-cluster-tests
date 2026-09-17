"""Unit tests for the pipeline linter's cloud-API isolation.

Linting only validates configuration structure, so it must never reach a cloud provider.
`_CLOUD_API_PATCHES` stubs the known lookups; `_no_remote_network` is the backstop that turns
a lookup nobody stubbed into a failure naming the linter, instead of an expired-credential or
timeout error raised from deep inside a vendor SDK.
"""

import logging
import socket
from pathlib import Path

import pytest

from sdcm.utils.lint.env_builder import build_env
from sdcm.utils.lint.jenkins_parser import parse_jenkinsfile
from sdcm.utils.lint.validator import (
    _CLOUD_API_PATCHES,
    _FAKE_IMAGE,
    _FAKE_OCI_IMAGE,
    LintNetworkAccessError,
    _no_remote_network,
    validate_pipeline,
)

# An aws pipeline whose test-case enables capacity reservation, so SCTConfiguration.__init__
# takes the reservation path that used to call EC2 for real.
_CAPACITY_RESERVATION_PIPELINE = Path(
    "jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/"
    "latte-perf-regression-predefined-throughput-steps-tablets.jenkinsfile"
)


def test_remote_connect_is_refused():
    with _no_remote_network(), socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        with pytest.raises(LintNetworkAccessError, match="_CLOUD_API_PATCHES"):
            sock.connect(("169.254.169.254", 80))


def test_remote_connect_ex_is_refused():
    with _no_remote_network(), socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        with pytest.raises(LintNetworkAccessError, match="_CLOUD_API_PATCHES"):
            sock.connect_ex(("169.254.169.254", 80))


def test_loopback_stays_reachable():
    """Only calls leaving the machine indicate an unstubbed cloud lookup."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        with _no_remote_network(), socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect(("127.0.0.1", port))


def test_the_guard_is_lifted_on_exit():
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    with _no_remote_network():
        assert socket.socket.connect is not original_connect
    assert socket.socket.connect is original_connect
    assert socket.socket.connect_ex is original_connect_ex


@pytest.mark.parametrize(
    "target",
    [
        "sdcm.provision.azure.utils.get_scylla_images",
        "sdcm.provision.azure.utils.get_released_scylla_images",
        "sdcm.utils.oci_utils.get_scylla_images_by_branch",
        "sdcm.utils.oci_utils.get_scylla_images_by_version",
        "sdcm.provision.aws.capacity_reservation.SCTCapacityReservation.get_cr_from_aws",
        "sdcm.provision.aws.dedicated_host.SCTDedicatedHosts.reserve",
    ],
)
def test_every_cloud_lookup_reached_from_sct_configuration_is_stubbed(target):
    """Each of these is a live API call made straight from `SCTConfiguration.__init__`.

    They are reached by disjoint sets of pipelines -- branch vs released image versions, capacity
    reservation vs dedicated hosts -- so a missing one stays invisible until a pipeline using that
    exact form gets linted with credentials that happen to work.
    """
    assert target in _CLOUD_API_PATCHES


@pytest.mark.parametrize("attribute", ["image_id", "self_link", "id", "unique_id", "name"])
def test_fake_image_carries_every_attribute_the_resolvers_read(attribute):
    """AWS reads image_id, GCE self_link, Azure id/unique_id -- a missing one joins into a TypeError."""
    assert getattr(_FAKE_IMAGE, attribute)


def test_fake_oci_image_is_indexable_as_the_resolvers_expect():
    """OCI lookups return positional lists; the resolvers read [1] for the name and [2] for the OCID."""
    assert _FAKE_OCI_IMAGE[1]
    assert _FAKE_OCI_IMAGE[2]


@pytest.fixture
def _restore_root_logger():
    """`validate_pipeline` silences the root logger for good; keep that out of the other tests."""
    root = logging.getLogger()
    handlers, disabled = root.handlers, root.disabled
    yield
    root.handlers, root.disabled = handlers, disabled


def test_capacity_reservation_pipeline_lints_without_reaching_ec2(_restore_root_logger):
    """Regression guard for a failure that only ever appeared in CI.

    `SCTConfiguration.__init__` calls `SCTCapacityReservation.get_cr_from_aws`, which describes
    capacity reservations on EC2 -- but only when `test_id` is set. A bare local `sct.py
    lint-pipelines` leaves it unset and skips the call entirely; CI runs through
    `docker/env/hydra.sh`, which always exports `SCT_TEST_ID`, so only CI took the path. Passing
    the test id explicitly here reproduces the CI environment.
    """
    config = parse_jenkinsfile(_CAPACITY_RESERVATION_PIPELINE)
    assert config is not None, f"{_CAPACITY_RESERVATION_PIPELINE} no longer parses"
    env = build_env(config) | {"SCT_TEST_ID": "11111111-2222-3333-4444-555555555555"}

    is_error, message = validate_pipeline(_CAPACITY_RESERVATION_PIPELINE, env)

    assert not is_error, message
