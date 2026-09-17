"""Unit tests for the pipeline linter's cloud-API isolation.

Linting only validates configuration structure, so it must never reach a cloud provider.
`_CLOUD_API_PATCHES` stubs the known lookups; `_no_remote_network` is the backstop that turns
a lookup nobody stubbed into a failure naming the linter, instead of an expired-credential or
timeout error raised from deep inside a vendor SDK.
"""

import socket

import pytest

from sdcm.utils.lint.validator import (
    _CLOUD_API_PATCHES,
    _FAKE_IMAGE,
    _FAKE_OCI_IMAGE,
    LintNetworkAccessError,
    _no_remote_network,
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
    ],
)
def test_every_image_lookup_reached_from_sct_configuration_is_stubbed(target):
    """SCTConfiguration.__init__ resolves scylla_version and oracle_scylla_version through these.

    Each one is a live API call, and each pair (branch vs released version) is reached by a
    different pipeline, so missing one stays invisible until a pipeline using that form is linted.
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
