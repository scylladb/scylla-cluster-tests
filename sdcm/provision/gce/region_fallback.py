# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Shared helpers for GCE region fallback.

Both the modern provisioning path (`sct.py` -> `instances_provider.provision_sct_resources`) and the
legacy tester path (`tester.ClusterTester.get_cluster_gce`) relocate the full cluster to another
region when region capacity is exhausted. This module shares their placement and cleanup logic.

Unlike AWS region fallback, GCE needs neither VPC-peering checks (single global VPC) nor per-region
image re-resolution (images are global resources), so switching is a plain config/env update.
"""

import logging
import os
from collections.abc import Callable, Iterable

from sdcm.provision.common.region_fallback import (
    provision_with_dc_fallback as _provision_with_dc_fallback,
    provision_with_region_fallback as _provision_with_region_fallback,
)
from sdcm.provision.gce.capacity_errors import is_zone_capacity_error
from sdcm.provision.gce.provisioner import GceProvisioner
from sdcm.provision.gce.zone_resolver import GceAZResolver
from sdcm.provision.provisioner import ProvisionError, ZoneResourcesExhaustedError

LOGGER = logging.getLogger(__name__)


def _is_capacity_error(exc: BaseException) -> bool:
    """True for GCE capacity/exhaustion errors that should trigger relocation to another region."""
    if isinstance(exc, ZoneResourcesExhaustedError):
        return True
    return isinstance(exc, ProvisionError) and is_zone_capacity_error(exc)


def _current_datacenters(params) -> list[str]:
    """Live list of configured GCE datacenters, reading the value ``switch_dc_region`` mutates.

    Uses ``gce_datacenter`` (updated in place on relocation) rather than the ``gce_datacenters``
    property so multi-DC relocations are visible across fallback retries.
    """
    raw = params.get("gce_datacenter")
    if isinstance(raw, list):
        return list(raw)
    return raw.split() if raw else []


def enforce_single_region_gate(params) -> None:
    """Require exactly one configured GCE datacenter for region fallback."""
    datacenters = params.gce_datacenters
    if len(datacenters) != 1:
        raise ValueError(
            f"fallback_to_next_region requires a single GCE datacenter (got {datacenters}); "
            "region fallback does not support multi-DC."
        )


def switch_region(params, region: str, az_letters: list[str]) -> None:
    """Switch to a target GCE region and its availability zones.

    `gce_datacenter` is env-first (like `region_name` on AWS), so the env var is updated as well.
    Unlike AWS, GCE needs no cache invalidation here: images are global and the resolver holds no
    region-bound instance state.
    """
    os.environ["SCT_GCE_DATACENTER"] = region
    params["gce_datacenter"] = region
    params["availability_zone"] = ",".join(az_letters)


def restore_region(params, region: str | None, availability_zone: str, env_region: str | None) -> None:
    """Restore original region and AZ values after a failed relocation."""
    if env_region is None:
        os.environ.pop("SCT_GCE_DATACENTER", None)
    else:
        os.environ["SCT_GCE_DATACENTER"] = env_region

    if region is not None:
        params["gce_datacenter"] = region
    params["availability_zone"] = availability_zone


def switch_dc_region(params, dc_index: int, region: str, az_letters: list[str]) -> None:  # noqa: ARG001
    """Relocate the DC at ``dc_index`` to ``region`` in the space-joined ``gce_datacenter`` list.

    GCE ``availability_zone`` is a single global setting (zones are chosen per region at provision
    time), so - unlike the single-region switch - it is left untouched here; ``az_letters`` is
    accepted only for signature parity with the shared loop.
    """
    datacenters = _current_datacenters(params)
    datacenters[dc_index] = region
    joined = " ".join(datacenters)
    os.environ["SCT_GCE_DATACENTER"] = joined
    params["gce_datacenter"] = joined


def _failed_dc_index(params, exc: BaseException) -> int | None:
    """Best-effort: which configured DC's region the capacity error refers to.

    GCE capacity errors name the exhausted zone (e.g. ``us-east1-c``), so the DC is the one whose
    configured region is a substring of the error message. Returns None when it cannot be attributed.
    """
    message = str(exc)
    for index, region in enumerate(_current_datacenters(params)):
        if region and region in message:
            return index
    return None


def cleanup_region(
    test_id: str,
    region: str,
    network_name: str,
    partial_cleanup: Callable[[], None] | None = None,
) -> None:
    """Clean up abandoned resources in the old region.

    First runs the caller-supplied partial cleanup (legacy path destroys the in-memory cluster
    objects), then sweeps any instances left in ``region`` for this test across all its zones.
    The sweep is essential for the modern path, which has no cluster objects to destroy.
    """
    if partial_cleanup is not None:
        partial_cleanup()

    try:
        provisioners = GceProvisioner.discover_regions(test_id, network_name=network_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Region fallback: failed to discover GCE resources for cleanup in %s: %s", region, exc)
        return

    for provisioner in provisioners:
        if provisioner.region != region:
            continue
        LOGGER.info("Region fallback: cleaning up abandoned GCE resources in %s (zone %s)", region, provisioner.zone)
        try:
            # wait=True is essential: instance names are not region-scoped, so a relocation recreates
            # the same names in the new region. Deletion must fully complete before we retry there,
            # otherwise GCE rejects the create with 409 "resource already exists".
            provisioner.cleanup(wait=True)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Region fallback: cleanup failed for %s: %s", provisioner, exc)


def provision_with_region_fallback(
    *,
    params,
    test_id: str,
    network_name: str,
    region_candidates: Callable[[], Iterable[tuple[str, list[str]]]],
    provision_once: Callable[[], None],
    error_factory: Callable[[str], BaseException],
    partial_cleanup: Callable[[], None] | None = None,
) -> None:
    """Provision in the configured GCE region, relocating to fallback regions on capacity exhaustion.

    Thin GCE binding over the backend-agnostic loop in
    :func:`sdcm.provision.common.region_fallback.provision_with_region_fallback`: it enforces the
    single-region gate, captures the original placement, and supplies GCE-specific switch / restore /
    cleanup and capacity-error detection. Single-node / single-region only.

    Args:
        params: SCT configuration; ``gce_datacenter`` / ``availability_zone`` are mutated on relocation.
        test_id: Test id used to discover and clean up abandoned resources in an exhausted region.
        network_name: GCE network (global VPC) used both to provision and to scope cleanup.
        region_candidates: Callable returning the ordered ``(region, az_letters)`` fallback targets;
            invoked lazily only after the configured region is exhausted.
        provision_once: Provisions the whole cluster in the currently configured region/AZ.
        error_factory: Builds the exception raised when every region is exhausted (each path keeps
            its own failure type, e.g. ``CriticalTestFailure`` vs ``ProvisionUnrecoverableError``).
        partial_cleanup: Optional per-region cleanup (the legacy path destroys in-memory clusters).

    Raises:
        ValueError: If more than one GCE datacenter is configured (multi-DC is unsupported here).
    """
    enforce_single_region_gate(params)

    original_region = params.gce_datacenters[0] if params.gce_datacenters else None
    original_az = params.get("availability_zone")
    original_env_region = os.environ.get("SCT_GCE_DATACENTER")

    _provision_with_region_fallback(
        original_region=original_region,
        region_candidates=region_candidates,
        provision_once=provision_once,
        switch_region=lambda region, az_letters: switch_region(params, region, az_letters),
        restore_region=lambda: restore_region(params, original_region, original_az, original_env_region),
        cleanup_region=lambda region: cleanup_region(
            test_id, region, network_name=network_name, partial_cleanup=partial_cleanup
        ),
        is_capacity_error=_is_capacity_error,
        error_factory=error_factory,
    )


def provision_with_dc_fallback(
    *,
    params,
    test_id: str,
    network_name: str,
    provision_once: Callable[[], None],
    error_factory: Callable[[str], BaseException],
    partial_cleanup: Callable[[], None] | None = None,
) -> None:
    """Provision a multi-DC GCE cluster, relocating an exhausted DC's region on capacity errors.

    GCE binding over :func:`sdcm.provision.common.region_fallback.provision_with_dc_fallback`. Captures
    the original multi-DC placement, computes per-DC candidates (no peering / image checks), and
    supplies GCE-specific relocation, cleanup and capacity-error attribution.
    """
    original_datacenters = " ".join(params.gce_datacenters) if params.gce_datacenters else None
    original_az = params.get("availability_zone")
    original_env_region = os.environ.get("SCT_GCE_DATACENTER")
    resolver = GceAZResolver(params)

    def restore() -> None:
        restore_region(params, original_datacenters, original_az, original_env_region)

    def cleanup() -> None:
        # Destroy in-memory clusters (legacy path) once, then sweep every currently-configured region.
        if partial_cleanup is not None:
            partial_cleanup()
        for region in dict.fromkeys(_current_datacenters(params)):
            cleanup_region(test_id, region, network_name=network_name, partial_cleanup=None)

    _provision_with_dc_fallback(
        dc_candidates=resolver.get_dc_fallback_candidates,
        provision_once=provision_once,
        failed_dc_index=lambda exc: _failed_dc_index(params, exc),
        switch_dc_region=lambda index, region, az_letters: switch_dc_region(params, index, region, az_letters),
        restore=restore,
        cleanup=cleanup,
        is_capacity_error=_is_capacity_error,
        error_factory=error_factory,
    )


def provision_with_fallback(
    *,
    params,
    test_id: str,
    network_name: str,
    provision_once: Callable[[], None],
    error_factory: Callable[[str], BaseException],
    partial_cleanup: Callable[[], None] | None = None,
) -> None:
    """Route GCE capacity-error fallback: whole-cluster (single region) or per-DC (multi region).

    This is the single entry both the legacy tester path and the modern provisioning path call, so all
    fallback control flow lives here rather than in the callers. Fallback candidates are resolved
    lazily (only if a capacity error actually triggers relocation), so a run that provisions in the
    configured placement never probes zone availability across the other regions.
    """
    if len(params.gce_datacenters) > 1:
        provision_with_dc_fallback(
            params=params,
            test_id=test_id,
            network_name=network_name,
            provision_once=provision_once,
            error_factory=error_factory,
            partial_cleanup=partial_cleanup,
        )
        return

    provision_with_region_fallback(
        params=params,
        test_id=test_id,
        network_name=network_name,
        region_candidates=lambda: GceAZResolver(params).get_region_fallback_candidates(),
        provision_once=provision_once,
        error_factory=error_factory,
        partial_cleanup=partial_cleanup,
    )
