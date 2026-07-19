# Mini-Plan: GCE Region Fallback (SCT-296)

**Date:** 2026-07-14
**Estimated LOC:** ~450 (impl ~250, tests ~200) — **actual: ~1675 across 25 files** (scope grew, see below)
**Related PR:** [#15427](https://github.com/scylladb/scylla-cluster-tests/pull/15427)
**Jira:** SCT-296 — Provision/GCE: upfront capacity validation and automatic zone/region fallback

## Status: ✅ DONE

All planned behavior is implemented, unit-tested, and manually verified on real GCE (see
[test plan](2026-07-16-gce-region-fallback-testplan.md)). The final implementation went beyond the
original single-region plan; the notable deviations from **Approach** below are:

- **Multi-DC region fallback added** (not just single-region). The original plan rejected multi-DC via
  `enforce_single_region_gate`; the final design keeps that gate for the *single-region* loop but adds
  a `provision_with_dc_fallback` loop that relocates only the exhausted DC in a multi-region cluster.
  A single **router** (`gce/region_fallback.provision_with_fallback`) dispatches single- vs multi-DC.
- **Centralized, backend-agnostic loop.** The fallback control flow lives in a new
  `sdcm/provision/common/region_fallback.py` (shared shape ready for OCI/Azure/AWS), with a thin GCE
  binding in `sdcm/provision/gce/region_fallback.py`. Both the legacy tester path and the modern
  `provision_sct_resources` path delegate to the same router.
- **Enable-gate moved to a leaf module.** `is_az_fallback_enabled` / `is_region_fallback_enabled` were
  moved out of `aws/az_resolver.py` into `sdcm/provision/common/fallback.py` so neither backend imports
  the other's package; `is_region_fallback_enabled` now accepts `("aws", "gce")`.
- **Lazy candidate resolution.** `get_region_fallback_candidates` / `get_dc_fallback_candidates` are
  generators — candidates are resolved only after the configured placement is exhausted, and each
  region's zone/machine-type availability is probed only when the loop reaches it (a run that
  provisions in its configured placement never probes other regions).
- **Concurrent cleanup.** Exhausted-region instances are deleted concurrently (was serial), so a
  `wait=True` cleanup before relocating no longer serializes slow per-instance deletes.
- **Placement handoff for the split pipeline.** The modern path persists the relocated placement
  (`persist_resolved_placement_if_changed`) and the run step re-applies it for GCE
  (`_apply_resolved_placement`), so a `provision-resources` relocation is picked up by a later
  `run-test`, mirroring AWS.
- **Enabled by default for GCE** (`defaults/gce_config.yaml`) and the **Jenkins provision step now runs
  for the gce backend**, so the modern path is exercised in CI.
- Modern path is wired as `provision_sct_resources` → `_provision_gce_resources` (folds resolve +
  fallback + handoff), rather than a separate top-level `provision_gce_resources()`.

## Problem

GCE already has upfront zone filtering, capacity-error detection, and runtime **zone (AZ) fallback**
(landed in `5a72b47d44` + follow-ups). The remaining SCT-296 scope is **region fallback**: when the
configured GCE datacenter has no capacity in any of its zones, relocate the whole (single-node,
single-region) cluster to another region. Today `is_region_fallback_enabled()` is hardcoded to
`cluster_backend == "aws"` and the entire region-fallback orchestration is AWS-only, so
`fallback_to_next_region` is a no-op on GCE.

The implementation mirrors the AWS design but drops machinery GCE does not need:
- **No VPC-peering gate** — the SCT GCE network is a single *global* VPC
  (`projects/{project}/global/networks/{name}`), so every region's subnet is reachable.
- **No per-region image re-resolution** — GCE images are *global* resources (`gce_image_db` is a
  global image path), so AWS's `resolve_amis` / `RegionAMINotFoundError` / `remap_dc_ami` are moot.

Region fallback must work and be tested in **both** provisioning paths, exactly like AWS:
- **Legacy / test runtime:** `sdcm/tester.py::get_cluster_gce`
- **Modern / `provision-resources` step:** `sct.py` GCE branch → `provision_sct_resources()`

## Approach

1. **Enable-gate** — generalize `is_region_fallback_enabled()` (`sdcm/provision/aws/az_resolver.py:107`)
   to accept `cluster_backend in ("aws", "gce")`. Update `fallback_to_next_region` docstring in
   `sdcm/sct_config.py:2154` (drop "AWS-only", note single-node/single-region).
2. **Candidate generation** — add `GceAZResolver.get_region_fallback_candidates()` in
   `sdcm/provision/gce/zone_resolver.py`, mirroring `AZResolver.get_region_fallback_candidates`
   **minus the peering check**. Reuse existing `_common_supported_letters` / `required_machine_types`.
   Iterate a new `SUPPORTED_GCE_REGIONS` constant; skip current region; keep regions that supply the
   configured zone-cardinality supporting all required machine types.
3. **Region constant** — add `SUPPORTED_GCE_REGIONS` to `sdcm/provision/gce/constants.py`
   (mirrors `AWS_SUPPORTED_REGIONS`; the four documented regions + any others we support).
4. **Shared switch/cleanup helpers** — new `sdcm/provision/gce/region_fallback.py` with
   `enforce_single_region_gate`, `switch_region` (env-first `SCT_GCE_DATACENTER` + `gce_datacenter` +
   `availability_zone`), `restore_region`, `cleanup_region`. `cleanup_region` runs a **per-region
   cleanup inside the loop** (see step 5/6), sweeping the exhausted region via
   `GceProvisioner(region=...).cleanup(test_id)` plus a caller-supplied partial-cleanup callback.
   No `resolve_amis`, no reservation reset (GCE has neither).
5. **Legacy path** — in `sdcm/tester.py`, refactor `get_cluster_gce` into `_get_cluster_gce_once`
   (existing body) + `_get_cluster_gce_with_region_fallback` mirroring
   `_get_cluster_aws_with_region_fallback` (`tester.py:1912`). Loop `[None, *candidates]`; on
   capacity exhaustion `cleanup_region(source_region)` **immediately in the loop iteration** then
   continue; non-capacity errors → `restore_region` + re-raise; success → return; total failure →
   `restore_region` + `CriticalTestFailure`. No `RegionAMINotFoundError` skip branch (images global).
6. **Modern path** — in `sct.py` GCE branch (`sct.py:373-378`), wrap `provision_sct_resources()` in
   the same loop using the shared helpers (extract a small `_provision_gce_with_region_fallback`
   helper, callable from both `sct.py` and reusable logic). Per-region cleanup happens in the loop.
   Gate on `is_region_fallback_enabled(params) and len(params.gce_datacenters) == 1`.
7. **Cleanup widening** — NOT NEEDED for GCE. `clean_instances_gce` (`resources_cleanup.py:193`)
   takes no `regions` arg; GCE cleanup is project-wide (aggregated list across all zones by tag), so
   it already reaps fallback regions. The AWS region-widening exists only because AWS cleanup is
   per-region. Left unchanged.
8. **Tests** — unit tests for candidate generation, gate, switch/restore/cleanup helpers, and the
   fallback loops in both paths (mirror `unit_tests/unit/provisioner/test_az_fallback.py` structure).

## Files to Modify

- `sdcm/provision/aws/az_resolver.py` — generalize `is_region_fallback_enabled` to aws+gce.
- `sdcm/provision/gce/zone_resolver.py` — add `get_region_fallback_candidates()` to `GceAZResolver`.
- `sdcm/provision/gce/constants.py` — add `SUPPORTED_GCE_REGIONS`.
- `sdcm/provision/gce/region_fallback.py` — **new**: `enforce_single_region_gate`, `switch_region`,
  `restore_region`, `cleanup_region` (per-region cleanup helper).
- `sdcm/tester.py` — split `get_cluster_gce` into `_get_cluster_gce_once` +
  `_get_cluster_gce_with_region_fallback`; wire gate into `get_cluster_gce`.
- `sct.py` — GCE branch (lines 373-378) now calls new `provision_gce_resources()` (resolve + region-fallback loop).
- `sdcm/sct_provision/instances_provider.py` — new `provision_gce_resources()` + `_provision_gce_with_region_fallback()`.
- `sdcm/sct_config.py` — update `fallback_to_next_region` docstring (line ~2154).
- `unit_tests/unit/provisioner/test_gce_region_fallback.py` — **new**: candidate gen, gate, helpers,
  both-path loop behavior (capacity → relocate + per-region cleanup; non-capacity → propagate).

## Verification

- [x] `is_region_fallback_enabled` returns True for gce backend with `fallback_to_next_region=true`
      (now in `common/fallback.py`, backends `("aws", "gce")`).
- [x] `get_region_fallback_candidates()` excludes current region, filters by machine-type zone
      availability, and returns `(region, az_letters)` with correct cardinality — **no** peering call
      (now a lazy generator; also added `get_dc_fallback_candidates()` for multi-DC).
- [x] Legacy path: simulated `ZoneResourcesExhaustedError` in region A relocates to region B, cleans
      up region A **within the loop iteration**, and provisions successfully; non-capacity error in A
      propagates without relocation; all-exhausted raises `CriticalTestFailure`.
- [x] Modern path (`provision_sct_resources`): same relocate + per-region cleanup + propagate behavior.
- [x] Region switch updates `SCT_GCE_DATACENTER` env, `gce_datacenter`, and `availability_zone`;
      `restore_region` reverts all three on total failure (env-first confirmed: `environment` is a
      plain property, `gce_datacenters` reads env first).
- [x] Multi-datacenter config: the **single-region** loop rejects it via `enforce_single_region_gate`;
      the router instead dispatches multi-DC configs to `provision_with_dc_fallback` (per-DC relocation).
- [x] Unit tests pass: `uv run python -m pytest unit_tests/unit/provisioner/test_gce_region_fallback.py -v`
- [x] AWS region-fallback tests still pass (shared gate change): `uv run python -m pytest unit_tests/unit/provisioner/ -v` (243 passed, 1 skipped)
- [x] `uv run sct.py pre-commit` passes (enforced on every commit via hooks).
- [x] Manual verification on real GCE — single-region relocation, multi-DC relocation, and split
      provision→run handoff — via the `BUILD_SIMULATE_GCE_EXHAUSTION` hook (see
      [test plan §8](2026-07-16-gce-region-fallback-testplan.md)).
