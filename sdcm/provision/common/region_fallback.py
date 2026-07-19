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

"""Backend-agnostic whole-cluster region-fallback loop.

Relocates a single-region cluster to another region when the configured region's capacity is
exhausted. The loop itself is backend-neutral; each backend injects how to switch/restore/clean up a
region and how to recognise a capacity error, so the same logic can serve GCE today and OCI/Azure
(and eventually AWS) without duplicating the control flow.
"""

import logging
from collections.abc import Callable, Iterable, Iterator

LOGGER = logging.getLogger(__name__)


def provision_with_region_fallback(
    *,
    original_region: str | None,
    region_candidates: Callable[[], Iterable[tuple[str, list[str]]]],
    provision_once: Callable[[], None],
    switch_region: Callable[[str, list[str]], None],
    restore_region: Callable[[], None],
    cleanup_region: Callable[[str | None], None],
    is_capacity_error: Callable[[BaseException], bool],
    error_factory: Callable[[str], BaseException],
) -> None:
    """Provision in the configured region, relocating to fallback regions on capacity exhaustion.

    The configured region is tried first; on a capacity error the region is cleaned up and the next
    candidate is tried. A non-capacity error restores the original placement and propagates. When the
    configured region and every candidate are exhausted, the original placement is restored and
    ``error_factory`` builds the raised exception (so each caller keeps its own failure type).

    Args:
        original_region: Currently configured region (starting point, for logging/messages).
        region_candidates: Callable returning an iterable of the ordered ``(region, az_letters)``
            fallback targets (current region excluded). Resolved lazily - only after the configured
            region is exhausted, and each candidate is probed only when the loop reaches it - so a
            successful configured-region run never probes the fallback regions, and a fallback that
            succeeds early never scans the remaining ones.
        provision_once: Provisions the whole cluster in the currently configured region/AZ.
        switch_region: Relocate config/env to ``(region, az_letters)`` before the next attempt.
        restore_region: Undo any relocation, restoring the original region/AZ (and env).
        cleanup_region: Tear down abandoned resources left in the given exhausted region.
        is_capacity_error: True when an exception is a capacity/exhaustion error (retry elsewhere)
            rather than a hard failure (propagate).
        error_factory: Builds the exception raised when every region is exhausted.

    Raises:
        BaseException: Any non-capacity error from ``provision_once`` propagates unchanged after the
            original region is restored; otherwise the result of ``error_factory`` on full exhaustion.
    """
    last_error: BaseException | None = None

    def attempt(region: str | None) -> bool:
        """Run ``provision_once`` in ``region``; return True on success, False on capacity exhaustion."""
        nonlocal last_error
        try:
            provision_once()
            return True
        except Exception as exc:  # noqa: BLE001
            if not is_capacity_error(exc):
                restore_region()
                raise
            last_error = exc
        cleanup_region(region)
        return False

    if attempt(original_region):
        return

    # Resolve fallback candidates only now (configured region exhausted), and consume them lazily so
    # each region's zone availability is probed only when we actually reach it - the common path where
    # the configured region succeeds never probes any fallback region, and a fallback that succeeds
    # early never scans the rest.
    tried_regions: list[str] = []
    for target_region, az_letters in region_candidates():
        tried_regions.append(target_region)
        LOGGER.warning(
            "Region '%s' exhausted; relocating whole cluster to '%s' (fallback region #%d)",
            original_region,
            target_region,
            len(tried_regions),
        )
        switch_region(target_region, az_letters)
        if attempt(target_region):
            return

    restore_region()
    tried = ", ".join(tried_regions) or "(no eligible candidates)"
    raise error_factory(
        f"Failed creating clusters in region '{original_region}' and all fallback candidates [{tried}]: {last_error}"
    )


def provision_with_dc_fallback(
    *,
    dc_candidates: Callable[[int], Iterable[tuple[str, list[str]]]],
    provision_once: Callable[[], None],
    failed_dc_index: Callable[[BaseException], int | None],
    switch_dc_region: Callable[[int, str, list[str]], None],
    restore: Callable[[], None],
    cleanup: Callable[[], None],
    is_capacity_error: Callable[[BaseException], bool],
    error_factory: Callable[[str], BaseException],
) -> None:
    """Provision a multi-DC cluster, relocating the exhausted DC to another region on capacity errors.

    Unlike the single-region loop, the whole cluster is (re)provisioned each attempt. On a capacity
    error the failing DC - identified by ``failed_dc_index`` from the error - is relocated to its next
    candidate region and the whole cluster is retried (so already-placed DCs are cleaned up and rebuilt
    against the updated placement). This keeps the control flow identical for the legacy and modern
    provisioning paths, which each expose only a whole-cluster ``provision_once``.

    A DC that runs out of candidates, an unattributable capacity error, or any non-capacity error
    restores the original placement and stops (via ``error_factory`` for the capacity cases).

    Args:
        dc_candidates: ``dc_index -> iterable of ordered (region, az_letters)`` relocation targets,
            resolved lazily the first time a given DC is exhausted (a DC that never fails is never
            probed), and each candidate region probed only when that DC needs it.
        provision_once: Provisions the whole (multi-DC) cluster in the current placement.
        failed_dc_index: Maps a capacity error to the index of the DC whose region was exhausted, or
            None when it cannot be attributed.
        switch_dc_region: Relocate the DC at ``index`` to ``(region, az_letters)``.
        restore: Undo all relocations, restoring the original placement (and env).
        cleanup: Tear down the partial cluster left by a failed attempt (all current DC regions).
        is_capacity_error: True when an exception is a capacity/exhaustion error.
        error_factory: Builds the exception raised when a DC exhausts its candidates.
    """
    dc_iterators: dict[int, Iterator[tuple[str, list[str]]]] = {}

    def next_candidate(dc_index: int) -> tuple[str, list[str]] | None:
        """Pull a DC's next fallback candidate, resolving its iterator on first use so idle DCs cost
        nothing and each candidate region is probed only when that DC actually reaches it."""
        if dc_index not in dc_iterators:
            dc_iterators[dc_index] = iter(dc_candidates(dc_index))
        return next(dc_iterators[dc_index], None)

    # Terminates naturally: every capacity failure either consumes one (finite) candidate for the
    # failing DC or stops once that DC runs out - no eager precompute of any DC's candidates is needed.
    while True:
        try:
            provision_once()
            return
        except Exception as exc:  # noqa: BLE001
            if not is_capacity_error(exc):
                restore()
                raise
            dc_index = failed_dc_index(exc)
            cleanup()
            candidate = next_candidate(dc_index) if dc_index is not None else None
            if candidate is None:
                restore()
                raise error_factory(
                    f"Failed creating multi-DC clusters; DC {dc_index} exhausted its fallback candidates: {exc}"
                )
            target_region, az_letters = candidate
            LOGGER.warning(
                "DC %d region exhausted; relocating it to '%s' and retrying the whole cluster",
                dc_index,
                target_region,
            )
            switch_dc_region(dc_index, target_region, az_letters)
