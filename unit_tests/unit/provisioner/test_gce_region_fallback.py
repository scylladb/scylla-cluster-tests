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

import os
from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.common.fallback import is_region_fallback_enabled
from sdcm.provision.gce import region_fallback as rf
from sdcm.provision.gce.constants import SUPPORTED_GCE_REGIONS
from sdcm.provision.gce.zone_resolver import GceAZResolver
from sdcm.provision.provisioner import ProvisionError, ProvisionUnrecoverableError, ZoneResourcesExhaustedError
from sdcm.sct_provision.instances_provider import provision_sct_resources
from sdcm.tester import ClusterTester, CriticalTestFailure

from unit_tests.lib.dot_dict import DotDict

CAPACITY_MSG = "Operation failed: ZONE_RESOURCE_POOL_EXHAUSTED"


class _GceParams(DotDict):
    """Params stand-in whose ``gce_datacenters`` is re-derived on every access.

    ``SCTConfiguration.gce_datacenters`` is a plain (uncached) property over ``gce_datacenter``, which
    is exactly what lets the fallback loops observe a relocation. Snapshotting it in the test double
    would hide staleness bugs that bite in production, so mirror the real liveness here.
    """

    @property
    def gce_datacenters(self) -> list[str]:
        raw = self.get("gce_datacenter") or ""
        return raw.split() if isinstance(raw, str) else list(raw)


def _make_params(**overrides):
    """Build a minimal single-DC GCE params object with region fallback enabled by default."""
    return _GceParams(
        {
            "cluster_backend": "gce",
            "gce_datacenter": "us-east1",
            "availability_zone": "b",
            "gce_instance_type_db": "n2-highmem-8",
            "gce_instance_type_loader": "e2-standard-2",
            "gce_instance_type_monitor": "n2-highmem-8",
            "gce_network": "qa-vpc",
            "n_db_nodes": 1,
            "n_loaders": 1,
            "n_monitor_nodes": 1,
            "fallback_to_next_region": True,
            "pre_filter_unavailable_availability_zones": True,
            **overrides,
        }
    )


# --------------------------------------------------------------------------- gate


def test_is_region_fallback_enabled_gce():
    """Region fallback is enabled for GCE when fallback_to_next_region is set."""
    assert is_region_fallback_enabled(_make_params(fallback_to_next_region=True)) is True


def test_is_region_fallback_enabled_disabled():
    """Region fallback is disabled when fallback_to_next_region is false."""
    assert is_region_fallback_enabled(_make_params(fallback_to_next_region=False)) is False


def test_is_region_fallback_enabled_other_backend():
    """Region fallback is rejected for unsupported backends even if the flag is set."""
    params = _make_params(cluster_backend="docker", fallback_to_next_region=True)
    assert is_region_fallback_enabled(params) is False


def test_enforce_single_region_gate_ok():
    """A single configured GCE datacenter passes the region-fallback gate."""
    rf.enforce_single_region_gate(_make_params())  # does not raise


def test_enforce_single_region_gate_rejects_multi_dc():
    """More than one configured GCE datacenter is rejected by the gate."""
    params = _make_params(gce_datacenter="us-east1 us-west1")
    with pytest.raises(ValueError, match="single GCE datacenter"):
        rf.enforce_single_region_gate(params)


# ------------------------------------------------------------------ switch/restore


def test_switch_region_updates_params_and_env(monkeypatch):
    """switch_region updates gce_datacenter, availability_zone, and the env override."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params()
    rf.switch_region(params, "us-west1", ["a", "b"])
    assert params["gce_datacenter"] == "us-west1"
    assert params["availability_zone"] == "a,b"
    assert os.environ["SCT_GCE_DATACENTER"] == "us-west1"


def test_restore_region_pops_env_when_originally_unset(monkeypatch):
    """restore_region drops the env override when the region was not originally set in the env."""
    monkeypatch.setenv("SCT_GCE_DATACENTER", "us-west1")
    params = _make_params()
    rf.restore_region(params, "us-east1", "b", None)
    assert params["gce_datacenter"] == "us-east1"
    assert params["availability_zone"] == "b"
    assert "SCT_GCE_DATACENTER" not in os.environ


def test_restore_region_restores_prior_env(monkeypatch):
    """restore_region puts back the original env value when there was one."""
    monkeypatch.setenv("SCT_GCE_DATACENTER", "us-west1")
    params = _make_params()
    rf.restore_region(params, "us-east1", "b", "us-east1")
    assert os.environ["SCT_GCE_DATACENTER"] == "us-east1"


# ------------------------------------------------------------------- cleanup_region


def test_cleanup_region_sweeps_only_matching_region():
    """cleanup_region runs the partial cleanup and sweeps only provisioners in the target region."""
    prov_east = MagicMock(region="us-east1", zone="us-east1-b")
    prov_west = MagicMock(region="us-west1", zone="us-west1-a")
    partial = MagicMock()
    with patch.object(rf.GceProvisioner, "discover_regions", return_value=[prov_east, prov_west]) as discover:
        rf.cleanup_region("test-id", "us-east1", network_name="qa-vpc", partial_cleanup=partial)
    partial.assert_called_once()
    discover.assert_called_once_with("test-id", network_name="qa-vpc")
    # wait=True so the old region is fully torn down before a relocation reuses the same names
    prov_east.cleanup.assert_called_once_with(wait=True)
    prov_west.cleanup.assert_not_called()


def test_cleanup_region_swallows_discovery_failure():
    """A discovery failure during cleanup is logged and swallowed; partial cleanup still runs."""
    partial = MagicMock()
    with patch.object(rf.GceProvisioner, "discover_regions", side_effect=RuntimeError("boom")):
        rf.cleanup_region("test-id", "us-east1", network_name="qa-vpc", partial_cleanup=partial)
    partial.assert_called_once()  # partial cleanup still ran; discovery error did not propagate


# ------------------------------------------------------ get_region_fallback_candidates


def test_get_region_fallback_candidates_excludes_current_and_filters_by_zone_support():
    """Candidates exclude the current region and any region without enough supported zones."""
    params = _make_params()  # current region us-east1, configured letter "b"
    zone_table = {
        "us-east4": ["us-east4-a", "us-east4-b"],
        "us-west1": ["us-west1-b"],
        "us-central1": [],  # no zone supports the machine types -> excluded
    }
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        mock_cls.return_value.get_common_zones.side_effect = lambda region, machine_types, preferred_zones: (
            zone_table.get(region, [])
        )
        candidates = list(GceAZResolver(params).get_region_fallback_candidates())

    regions = [region for region, _ in candidates]
    assert "us-east1" not in regions  # current region excluded
    assert "us-central1" not in regions  # unsupported region excluded
    assert candidates == [("us-east4", ["b"]), ("us-west1", ["b"])]


def test_get_region_fallback_candidates_never_checks_peering():
    """GCE uses a global VPC; the candidate path must never perform an AWS-style peering lookup."""
    params = _make_params()
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        mock_cls.return_value.get_common_zones.return_value = ["us-west1-b"]
        candidates = list(GceAZResolver(params).get_region_fallback_candidates())
    assert all(isinstance(letters, list) for _, letters in candidates)
    assert not hasattr(rf, "_is_region_peered")


# ------------------------------------------------- shared loop (exercises both paths)


def test_loop_success_first_attempt_no_switch_no_cleanup():
    """When the configured region succeeds, no relocation or cleanup happens."""
    params = _make_params()
    provision_once = MagicMock()
    with patch.object(rf, "cleanup_region") as cleanup:
        rf.provision_with_region_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            region_candidates=lambda: [("us-west1", ["a"])],
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )
    provision_once.assert_called_once()
    cleanup.assert_not_called()
    assert params["gce_datacenter"] == "us-east1"  # unchanged


def test_loop_success_does_not_resolve_candidates():
    """A run that provisions in the configured region must never invoke the (costly) candidate resolver."""
    params = _make_params()
    provision_once = MagicMock()  # succeeds on the first attempt
    candidates = MagicMock(return_value=[("us-west1", ["a"])])
    with patch.object(rf, "cleanup_region"):
        rf.provision_with_region_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            region_candidates=candidates,
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )
    provision_once.assert_called_once()
    candidates.assert_not_called()  # lazy: fallback regions are not probed on the success path


def test_loop_relocates_on_zone_exhausted_then_succeeds(monkeypatch):
    """A ZoneResourcesExhaustedError relocates to the next region, which then succeeds."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params()
    provision_once = MagicMock(side_effect=[ZoneResourcesExhaustedError(CAPACITY_MSG), None])
    with patch.object(rf, "cleanup_region") as cleanup:
        rf.provision_with_region_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            region_candidates=lambda: [("us-west1", ["a"])],
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )
    assert provision_once.call_count == 2
    cleanup.assert_called_once_with("t", "us-east1", network_name="qa-vpc", partial_cleanup=None)
    assert params["gce_datacenter"] == "us-west1"  # relocated
    assert params["availability_zone"] == "a"


def test_loop_relocates_on_capacity_provision_error(monkeypatch):
    """A capacity-class ProvisionError is treated like exhaustion and triggers relocation."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params()
    provision_once = MagicMock(side_effect=[ProvisionError(CAPACITY_MSG), None])
    with patch.object(rf, "cleanup_region"):
        rf.provision_with_region_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            region_candidates=lambda: [("us-west1", ["a"])],
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )
    assert provision_once.call_count == 2


def test_loop_non_capacity_error_propagates_and_restores(monkeypatch):
    """A non-capacity error propagates unchanged and restores the original region/AZ."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params()
    provision_once = MagicMock(side_effect=ProvisionError("permission denied creating instance"))
    with patch.object(rf, "cleanup_region") as cleanup:
        with pytest.raises(ProvisionError, match="permission denied"):
            rf.provision_with_region_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                region_candidates=lambda: [("us-west1", ["a"])],
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    cleanup.assert_not_called()
    assert params["gce_datacenter"] == "us-east1"  # restored
    assert params["availability_zone"] == "b"


def test_loop_all_regions_exhausted_raises_via_error_factory(monkeypatch):
    """When the configured region and every candidate are exhausted, error_factory builds the error."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params()
    provision_once = MagicMock(side_effect=ZoneResourcesExhaustedError(CAPACITY_MSG))
    with patch.object(rf, "cleanup_region") as cleanup:
        with pytest.raises(ProvisionUnrecoverableError, match="all fallback candidates"):
            rf.provision_with_region_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                region_candidates=lambda: [("us-west1", ["a"]), ("us-east4", ["b"])],
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    assert provision_once.call_count == 3  # original + 2 candidates
    assert cleanup.call_count == 3  # each exhausted region cleaned up inside the loop
    assert params["gce_datacenter"] == "us-east1"  # restored
    assert "SCT_GCE_DATACENTER" not in os.environ


def test_loop_restores_placement_when_candidate_resolution_fails(monkeypatch):
    """A candidate generator that blows up mid-iteration must not leave us on the fallback region.

    Resolution probes the cloud API, so it can fail *after* an earlier candidate already relocated us;
    the original placement has to be restored before the error propagates.
    """
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params()
    provision_once = MagicMock(side_effect=ZoneResourcesExhaustedError(CAPACITY_MSG))

    def candidates():
        yield "us-west1", ["a"]
        raise RuntimeError("zone list API blew up")

    with patch.object(rf, "cleanup_region"):
        with pytest.raises(RuntimeError, match="zone list API blew up"):
            rf.provision_with_region_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                region_candidates=candidates,
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    assert params["gce_datacenter"] == "us-east1"  # relocation to us-west1 undone
    assert params["availability_zone"] == "b"
    assert "SCT_GCE_DATACENTER" not in os.environ


def test_loop_rejects_multi_dc():
    """The shared loop refuses to run for a multi-DC config."""
    params = _make_params(gce_datacenter="us-east1 us-west1")
    with pytest.raises(ValueError, match="single GCE datacenter"):
        rf.provision_with_region_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            region_candidates=list,
            provision_once=MagicMock(),
            error_factory=ProvisionUnrecoverableError,
        )


# ----------------------------------------------------- multi-DC: candidates / switch / attribution


def test_get_dc_fallback_candidates_excludes_in_use_and_filters_by_zone_support():
    """DC candidates exclude every in-use region and regions without enough supported zones."""
    params = _make_params(gce_datacenter="us-east1 us-west1")  # in use: us-east1, us-west1
    zone_table = {
        "us-east4": ["us-east4-b"],
        "us-central1": [],  # no supported zone -> excluded
    }
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        mock_cls.return_value.get_common_zones.side_effect = lambda region, machine_types, preferred_zones: (
            zone_table.get(region, [])
        )
        candidates = list(GceAZResolver(params).get_dc_fallback_candidates(0))

    regions = [region for region, _ in candidates]
    assert "us-east1" not in regions and "us-west1" not in regions  # in-use DCs excluded
    assert "us-central1" not in regions  # unsupported region excluded
    assert candidates == [("us-east4", ["b"])]


def test_get_dc_fallback_candidates_skips_regions_missing_the_configured_zone():
    """A region that qualifies only through some *other* zone letter is skipped, not silently retargeted.

    switch_dc_region deliberately leaves the global availability_zone alone, so a candidate qualified
    by an alternative letter would aim the retry at a zone the target region does not have.
    """
    params = _make_params(gce_datacenter="us-east1 us-west1", availability_zone="b")
    zone_table = {
        "us-east4": ["us-east4-a", "us-east4-c"],  # supports 'a'/'c' but NOT the configured 'b'
        "us-central1": ["us-central1-b"],  # supports the configured 'b'
    }
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        mock_cls.return_value.get_common_zones.side_effect = lambda region, machine_types, preferred_zones: (
            zone_table.get(region, [])
        )
        candidates = list(GceAZResolver(params).get_dc_fallback_candidates(0))

    assert candidates == [("us-central1", ["b"])]  # us-east4 rejected: no 'b' zone


def test_get_dc_fallback_candidates_rechecks_live_regions_between_yields():
    """The in-use region set is re-read per candidate, so a region a sibling DC just took is skipped.

    The multi-DC loop holds one generator per DC for its whole lifetime while *other* DCs keep
    relocating. A set snapshotted on first use would hand back an already-occupied region and land two
    DCs in the same one.
    """
    params = _make_params(gce_datacenter="us-east1 us-west1", availability_zone="b")
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        # Every region supports the configured zone, so only occupancy can filter candidates.
        mock_cls.return_value.get_common_zones.side_effect = lambda region, machine_types, preferred_zones: (
            [f"{region}-b"]
        )
        candidates = GceAZResolver(params).get_dc_fallback_candidates(0)
        first_region, _ = next(candidates)
        # Simulate DC 1 relocating into a region this generator has not offered yet.
        stolen = next(r for r in SUPPORTED_GCE_REGIONS if r not in (first_region, "us-east1", "us-west1"))
        params["gce_datacenter"] = f"us-east1 {stolen}"
        remaining = [region for region, _ in candidates]

    assert stolen not in remaining  # would be a duplicate-region placement


def test_switch_dc_region_updates_only_target_dc(monkeypatch):
    """switch_dc_region relocates just the chosen DC and leaves the global availability_zone alone.

    Safe only because get_dc_fallback_candidates yields back the configured letters (see
    test_get_dc_fallback_candidates_skips_regions_missing_the_configured_zone).
    """
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")
    rf.switch_dc_region(params, 1, "us-east4", ["b"])
    assert params["gce_datacenter"] == "us-east1 us-east4"
    assert os.environ["SCT_GCE_DATACENTER"] == "us-east1 us-east4"
    assert params["availability_zone"] == "b"  # global AZ untouched (candidate already supports it)


def test_failed_dc_index_attributes_region_from_error():
    """A capacity error naming a zone is attributed to the DC whose region it belongs to."""
    params = _make_params(gce_datacenter="us-east1 us-west1")
    assert rf._failed_dc_index(params, ZoneResourcesExhaustedError("Zone us-west1-b exhausted")) == 1
    assert rf._failed_dc_index(params, ZoneResourcesExhaustedError("Zone us-east1-c exhausted")) == 0
    assert rf._failed_dc_index(params, ZoneResourcesExhaustedError("vague error, no region")) is None


def test_required_machine_types_excludes_gated_off_types():
    """A machine type whose node count is 0 (e.g. vector store) is not required, so it never constrains candidates."""
    params = _make_params(instance_type_vector_store="e2-medium", n_vector_store_nodes=0)
    types = GceAZResolver(params).required_machine_types()
    assert "e2-medium" not in types  # gated off (no vector-store nodes)
    assert "n2-highmem-8" in types  # db type is always required


# ------------------------------------------------------------- multi-DC fallback loop


def test_dc_loop_relocates_exhausted_dc_then_succeeds(monkeypatch):
    """The exhausted DC is relocated to a candidate region and the whole cluster retried to success."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")
    provision_once = MagicMock(
        side_effect=[ZoneResourcesExhaustedError("Zone us-west1-b resource pool exhausted"), None]
    )
    with (
        patch.object(rf, "cleanup_region") as cleanup,
        patch.object(rf, "GceAZResolver") as resolver,
    ):
        resolver.return_value.get_dc_fallback_candidates.side_effect = lambda idx: (
            [("us-east4", ["b"])] if idx == 1 else []
        )
        rf.provision_with_dc_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )
    assert provision_once.call_count == 2
    assert cleanup.called  # exhausted attempt cleaned up before retry
    assert params["gce_datacenter"] == "us-east1 us-east4"  # only DC 1 relocated


def test_dc_loop_success_does_not_resolve_candidates(monkeypatch):
    """A successful multi-DC run must never resolve any DC's fallback candidates."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")
    provision_once = MagicMock()  # succeeds on the first attempt
    with patch.object(rf, "cleanup_region"), patch.object(rf, "GceAZResolver") as resolver:
        rf.provision_with_dc_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )
    provision_once.assert_called_once()
    resolver.return_value.get_dc_fallback_candidates.assert_not_called()  # lazy per-DC resolution


def test_dc_loop_all_candidates_exhausted_raises(monkeypatch):
    """When the failing DC runs out of candidates, error_factory builds the raised error."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")

    def always_exhaust_dc1(*_args, **_kwargs):
        raise ZoneResourcesExhaustedError(f"Zone {params['gce_datacenter'].split()[1]}-b resource pool exhausted")

    provision_once = MagicMock(side_effect=always_exhaust_dc1)
    with (
        patch.object(rf, "cleanup_region"),
        patch.object(rf, "GceAZResolver") as resolver,
    ):
        resolver.return_value.get_dc_fallback_candidates.side_effect = lambda idx: (
            [("us-east4", ["b"])] if idx == 1 else []
        )
        with pytest.raises(ProvisionUnrecoverableError, match="exhausted its fallback candidates") as exc_info:
            rf.provision_with_dc_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    assert provision_once.call_count == 2  # original + one candidate
    assert params["gce_datacenter"] == "us-east1 us-west1"  # restored
    # Chained, so the traceback still shows which capacity error actually ended the run.
    assert isinstance(exc_info.value.__cause__, ZoneResourcesExhaustedError)


def test_dc_loop_restores_placement_when_candidate_resolution_fails(monkeypatch):
    """A per-DC candidate generator failing after a relocation must restore the original placement."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")

    def always_exhaust_dc1(*_args, **_kwargs):
        raise ZoneResourcesExhaustedError(f"Zone {params['gce_datacenter'].split()[1]}-b resource pool exhausted")

    def candidates_then_boom(_dc_index):
        yield "us-east4", ["b"]
        raise RuntimeError("zone list API blew up")

    provision_once = MagicMock(side_effect=always_exhaust_dc1)
    with patch.object(rf, "cleanup_region"), patch.object(rf, "GceAZResolver") as resolver:
        resolver.return_value.get_dc_fallback_candidates.side_effect = candidates_then_boom
        with pytest.raises(RuntimeError, match="zone list API blew up"):
            rf.provision_with_dc_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    assert provision_once.call_count == 2  # original + the one candidate before resolution broke
    assert params["gce_datacenter"] == "us-east1 us-west1"  # us-east4 relocation undone
    assert "SCT_GCE_DATACENTER" not in os.environ


def test_dc_loop_non_capacity_error_propagates_and_restores(monkeypatch):
    """A non-capacity error propagates unchanged and restores the original multi-DC placement."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")
    provision_once = MagicMock(side_effect=ProvisionError("permission denied creating instance"))
    with (
        patch.object(rf, "cleanup_region") as cleanup,
        patch.object(rf, "GceAZResolver") as resolver,
    ):
        resolver.return_value.get_dc_fallback_candidates.return_value = [("us-east4", ["b"])]
        with pytest.raises(ProvisionError, match="permission denied"):
            rf.provision_with_dc_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    cleanup.assert_not_called()
    assert params["gce_datacenter"] == "us-east1 us-west1"  # restored


def test_dc_loop_unattributable_capacity_error_raises(monkeypatch):
    """A capacity error that names no configured region cannot be relocated and stops the loop."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")
    provision_once = MagicMock(side_effect=ZoneResourcesExhaustedError("capacity exhausted, region unknown"))
    with (
        patch.object(rf, "cleanup_region"),
        patch.object(rf, "GceAZResolver") as resolver,
    ):
        resolver.return_value.get_dc_fallback_candidates.return_value = [("us-east4", ["b"])]
        with pytest.raises(ProvisionUnrecoverableError):
            rf.provision_with_dc_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    assert provision_once.call_count == 1  # could not attribute -> no relocation/retry


def test_dc_loop_relocates_two_dcs_in_sequence(monkeypatch):
    """Independent per-DC candidate lists let two different DCs each relocate across retries."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")
    calls = {"n": 0}

    def side_effect(*_args, **_kwargs):
        calls["n"] += 1
        dcs = params["gce_datacenter"].split()
        if calls["n"] == 1:
            raise ZoneResourcesExhaustedError(f"Zone {dcs[0]}-b resource pool exhausted")
        if calls["n"] == 2:
            raise ZoneResourcesExhaustedError(f"Zone {dcs[1]}-b resource pool exhausted")

    provision_once = MagicMock(side_effect=side_effect)
    with patch.object(rf, "cleanup_region"), patch.object(rf, "GceAZResolver") as resolver:
        resolver.return_value.get_dc_fallback_candidates.side_effect = lambda idx: (
            [("us-east4", ["b"])] if idx == 0 else [("us-central1", ["b"])]
        )
        rf.provision_with_dc_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            provision_once=provision_once,
            error_factory=ProvisionUnrecoverableError,
        )
    assert provision_once.call_count == 3  # DC0 relocate, DC1 relocate, then success
    assert params["gce_datacenter"] == "us-east4 us-central1"  # both DCs relocated independently


def test_dc_loop_is_bounded_when_relocation_keeps_failing(monkeypatch):
    """The attempt count is bounded by the per-DC candidate lists, so a persistently-failing DC stops."""
    monkeypatch.delenv("SCT_GCE_DATACENTER", raising=False)
    params = _make_params(gce_datacenter="us-east1 us-west1")

    def always_exhaust_dc0(*_args, **_kwargs):
        raise ZoneResourcesExhaustedError(f"Zone {params['gce_datacenter'].split()[0]}-b resource pool exhausted")

    provision_once = MagicMock(side_effect=always_exhaust_dc0)
    with patch.object(rf, "cleanup_region"), patch.object(rf, "GceAZResolver") as resolver:
        resolver.return_value.get_dc_fallback_candidates.side_effect = lambda idx: (
            [("us-east4", ["b"]), ("us-central1", ["b"])] if idx == 0 else []
        )
        with pytest.raises(ProvisionUnrecoverableError):
            rf.provision_with_dc_fallback(
                params=params,
                test_id="t",
                network_name="qa-vpc",
                provision_once=provision_once,
                error_factory=ProvisionUnrecoverableError,
            )
    assert provision_once.call_count == 3  # original + 2 DC0 candidates, then stop (no infinite loop)


# ------------------------------------------------------------------ zone (AZ) fallback


def test_get_az_fallback_candidates_yields_the_regions_other_supported_zones():
    """Zone candidates are the region's other supported letters, with the configured one excluded."""
    params = _make_params(availability_zone="b")
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        mock_cls.return_value.get_common_zones.return_value = ["us-east1-b", "us-east1-c", "us-east1-d"]
        assert list(GceAZResolver(params).get_az_fallback_candidates()) == [["c"], ["d"]]


def test_get_az_fallback_candidates_skips_multi_az_configs():
    """Multi-AZ configs yield nothing: which letter to swap is ambiguous, so region fallback takes over."""
    params = _make_params(availability_zone="b,c")
    with patch("sdcm.provision.gce.zone_resolver.GceZoneResolver") as mock_cls:
        mock_cls.return_value.get_common_zones.return_value = ["us-east1-b", "us-east1-c", "us-east1-d"]
        assert list(GceAZResolver(params).get_az_fallback_candidates()) == []


def test_az_fallback_disabled_returns_the_original_callable():
    """With fallback_to_next_availability_zone off, provision_once is handed back untouched."""
    params = _make_params(fallback_to_next_availability_zone=False)
    provision_once = MagicMock()
    wrapped = rf.provision_with_az_fallback(
        params=params, test_id="t", network_name="qa-vpc", provision_once=provision_once
    )
    assert wrapped is provision_once


def test_az_fallback_retries_next_zone_in_the_same_region():
    """An exhausted zone retries the region's next supported zone, without relocating the cluster."""
    params = _make_params(fallback_to_next_availability_zone=True)
    provision_once = MagicMock(side_effect=[ZoneResourcesExhaustedError(CAPACITY_MSG), None])
    with patch.object(rf, "cleanup_region") as cleanup, patch.object(rf, "GceAZResolver") as resolver:
        resolver.return_value.get_az_fallback_candidates.return_value = iter([["c"], ["d"]])
        rf.provision_with_az_fallback(
            params=params, test_id="t", network_name="qa-vpc", provision_once=provision_once
        )()
    assert provision_once.call_count == 2
    assert params["availability_zone"] == "c"  # moved to the next zone
    assert params["gce_datacenter"] == "us-east1"  # region untouched
    assert cleanup.called  # exhausted zone swept before the retry


def test_az_fallback_success_never_resolves_candidates():
    """A first-attempt success must not probe zone candidates at all."""
    params = _make_params(fallback_to_next_availability_zone=True)
    provision_once = MagicMock()
    with patch.object(rf, "GceAZResolver") as resolver:
        rf.provision_with_az_fallback(
            params=params, test_id="t", network_name="qa-vpc", provision_once=provision_once
        )()
    provision_once.assert_called_once()
    resolver.return_value.get_az_fallback_candidates.assert_not_called()


def test_az_fallback_restores_zone_and_escalates_when_all_zones_exhausted():
    """Once every zone is exhausted the AZ is restored and the capacity error escalates to the caller.

    This is what hands control to region / multi-DC fallback - and it must start from the configured
    placement, not from the last zone we tried.
    """
    params = _make_params(fallback_to_next_availability_zone=True)
    provision_once = MagicMock(side_effect=ZoneResourcesExhaustedError(CAPACITY_MSG))
    with patch.object(rf, "cleanup_region"), patch.object(rf, "GceAZResolver") as resolver:
        resolver.return_value.get_az_fallback_candidates.return_value = iter([["c"]])
        with pytest.raises(ZoneResourcesExhaustedError):
            rf.provision_with_az_fallback(
                params=params, test_id="t", network_name="qa-vpc", provision_once=provision_once
            )()
    assert provision_once.call_count == 2  # configured zone + one candidate
    assert params["availability_zone"] == "b"  # restored


def test_az_fallback_propagates_non_capacity_error_and_restores_zone():
    """A non-capacity failure is not a zone problem: restore the AZ and propagate straight away."""
    params = _make_params(fallback_to_next_availability_zone=True)
    provision_once = MagicMock(side_effect=ProvisionError("permission denied creating instance"))
    with patch.object(rf, "GceAZResolver") as resolver:
        with pytest.raises(ProvisionError, match="permission denied"):
            rf.provision_with_az_fallback(
                params=params, test_id="t", network_name="qa-vpc", provision_once=provision_once
            )()
    provision_once.assert_called_once()
    resolver.return_value.get_az_fallback_candidates.assert_not_called()
    assert params["availability_zone"] == "b"


# ----------------------------------------------------------------- fallback router


def test_router_single_region_routes_to_region_fallback():
    """provision_with_fallback sends a single-DC config to the whole-cluster region loop."""
    params = _make_params()
    with (
        patch.object(rf, "GceAZResolver"),
        patch.object(rf, "provision_with_region_fallback") as region,
        patch.object(rf, "provision_with_dc_fallback") as multi_dc,
    ):
        rf.provision_with_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            provision_once=MagicMock(),
            error_factory=ProvisionUnrecoverableError,
        )
    region.assert_called_once()
    multi_dc.assert_not_called()


def test_router_multi_dc_routes_to_dc_fallback():
    """provision_with_fallback sends a multi-DC config to the per-DC loop."""
    params = _make_params(gce_datacenter="us-east1 us-west1")
    with (
        patch.object(rf, "provision_with_region_fallback") as region,
        patch.object(rf, "provision_with_dc_fallback") as multi_dc,
    ):
        rf.provision_with_fallback(
            params=params,
            test_id="t",
            network_name="qa-vpc",
            provision_once=MagicMock(),
            error_factory=ProvisionUnrecoverableError,
        )
    multi_dc.assert_called_once()
    region.assert_not_called()


# ------------------------------------------------ modern path routing (provision_sct_resources)


def test_provision_gce_resources_routes_to_fallback_when_enabled():
    """provision_sct_resources drives the fallback router when region fallback is enabled."""
    params = _make_params(fallback_to_next_region=True)
    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver") as resolver,
        patch("sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_fallback") as fallback,
        patch("sdcm.sct_provision.instances_provider._provision_sct_resources_once") as once,
    ):
        provision_sct_resources(params=params, test_config=MagicMock())
    resolver.return_value.resolve.assert_called_once()
    fallback.assert_called_once()
    once.assert_not_called()


def test_provision_gce_resources_routes_to_plain_when_disabled():
    """Without region fallback, GCE provisions once (no fallback router)."""
    params = _make_params(fallback_to_next_region=False)
    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver") as resolver,
        patch("sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_fallback") as fallback,
        patch("sdcm.sct_provision.instances_provider._provision_sct_resources_once") as once,
    ):
        provision_sct_resources(params=params, test_config=MagicMock())
    resolver.return_value.resolve.assert_called_once()
    fallback.assert_not_called()
    once.assert_called_once()


def test_provision_gce_resources_wraps_provisioning_in_zone_fallback():
    """The modern path hands the region loop a zone-fallback-wrapped attempt, not the bare one.

    Regression: without this the modern path ignored fallback_to_next_availability_zone entirely, so a
    single exhausted zone relocated the whole cluster instead of trying the region's other zones.
    """
    params = _make_params(fallback_to_next_region=True, fallback_to_next_availability_zone=True)
    sentinel = MagicMock(name="zone_fallback_wrapper")
    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver"),
        patch(
            "sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_az_fallback",
            return_value=sentinel,
        ) as az_fallback,
        patch("sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_fallback") as fallback,
    ):
        provision_sct_resources(params=params, test_config=MagicMock())
    az_fallback.assert_called_once()
    assert fallback.call_args.kwargs["provision_once"] is sentinel


def test_provision_gce_resources_uses_zone_fallback_even_without_region_fallback():
    """Zone fallback is gated by its own flag, so it still applies when region fallback is off."""
    params = _make_params(fallback_to_next_region=False, fallback_to_next_availability_zone=True)
    wrapper = MagicMock(name="zone_fallback_wrapper")
    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver"),
        patch(
            "sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_az_fallback",
            return_value=wrapper,
        ),
        patch("sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_fallback") as fallback,
    ):
        provision_sct_resources(params=params, test_config=MagicMock())
    fallback.assert_not_called()
    wrapper.assert_called_once()  # provisioning still goes through the zone-retry wrapper


def test_provision_gce_resources_persists_zone_only_change():
    """A zone changed by AZ fallback (region untouched) is persisted for the separate Run Test step.

    Regression: the handoff used to be written only on region change, but GCE instance discovery is
    zone-scoped (no AWS-style region-wide test_id search), so a Run Test step aimed at the configured
    zone would miss the instances and provision a duplicate cluster.
    """
    params = _make_params(fallback_to_next_region=False, fallback_to_next_availability_zone=True)
    test_config = MagicMock()
    test_config.test_id.return_value = "tid-1"

    def zone_fallback_wrapper():
        params["availability_zone"] = "c"

    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver"),
        patch(
            "sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_az_fallback",
            return_value=zone_fallback_wrapper,
        ),
    ):
        provision_sct_resources(params=params, test_config=test_config)
    test_config.write_resolved_placement.assert_called_once_with("tid-1", region_name="us-east1", availability_zone="c")


def test_provision_gce_resources_persists_resolver_selected_zone():
    """A zone the resolver picks when none is configured is persisted too.

    With availability_zone unset the resolver chooses a random valid zone, so a separate Run Test
    step would roll its own pick unless the provisioning step's choice is handed off.
    """
    params = _make_params(fallback_to_next_region=False, availability_zone=None)
    test_config = MagicMock()
    test_config.test_id.return_value = "tid-1"

    def fake_resolve():
        params["availability_zone"] = "d"

    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver") as resolver,
        patch("sdcm.sct_provision.instances_provider._provision_sct_resources_once"),
    ):
        resolver.return_value.resolve.side_effect = fake_resolve
        provision_sct_resources(params=params, test_config=test_config)
    test_config.write_resolved_placement.assert_called_once_with("tid-1", region_name="us-east1", availability_zone="d")


def test_provision_gce_resources_skips_handoff_when_placement_unchanged():
    """No handoff file is written when provisioning ends up in the configured region and zone."""
    params = _make_params(fallback_to_next_region=False)
    test_config = MagicMock()
    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver"),
        patch("sdcm.sct_provision.instances_provider._provision_sct_resources_once"),
    ):
        provision_sct_resources(params=params, test_config=test_config)
    test_config.write_resolved_placement.assert_not_called()


def test_provision_gce_resources_routes_to_fallback_when_multi_dc_enabled():
    """Multi-DC GCE configs also go through the fallback router (it dispatches to per-DC fallback)."""
    params = _make_params(fallback_to_next_region=True, gce_datacenter="us-east1 us-west1")
    with (
        patch("sdcm.sct_provision.instances_provider.GceAZResolver"),
        patch("sdcm.sct_provision.instances_provider.gce_region_fallback.provision_with_fallback") as fallback,
        patch("sdcm.sct_provision.instances_provider._provision_sct_resources_once") as once,
    ):
        provision_sct_resources(params=params, test_config=MagicMock())
    fallback.assert_called_once()
    once.assert_not_called()


# --------------------------------------------------------- legacy path (tester) routing


def _fake_tester(params):
    """Build a MagicMock tester bound to the given params for exercising ClusterTester methods."""
    fake = MagicMock()
    fake.params = params
    return fake


def test_legacy_get_cluster_gce_dispatches_to_region_fallback_when_enabled():
    """get_cluster_gce routes to the fallback path for a single-DC config with fallback on."""
    fake = _fake_tester(_make_params(fallback_to_next_region=True))
    ClusterTester.get_cluster_gce(fake, loader_info={}, db_info={}, monitor_info={})
    fake._get_cluster_gce_with_region_fallback.assert_called_once()
    fake._get_cluster_gce_once.assert_not_called()


def test_legacy_get_cluster_gce_uses_once_path_when_disabled():
    """get_cluster_gce uses the single-attempt path when region fallback is disabled."""
    fake = _fake_tester(_make_params(fallback_to_next_region=False))
    ClusterTester.get_cluster_gce(fake, loader_info={}, db_info={}, monitor_info={})
    fake._get_cluster_gce_with_region_fallback.assert_not_called()
    fake._get_cluster_gce_once.assert_called_once()


def test_legacy_get_cluster_gce_dispatches_to_fallback_when_multi_dc():
    """get_cluster_gce routes multi-DC configs to the fallback path too (per-DC fallback)."""
    fake = _fake_tester(_make_params(fallback_to_next_region=True, gce_datacenter="us-east1 us-west1"))
    ClusterTester.get_cluster_gce(fake, loader_info={}, db_info={}, monitor_info={})
    fake._get_cluster_gce_with_region_fallback.assert_called_once()
    fake._get_cluster_gce_once.assert_not_called()


def test_legacy_region_fallback_delegates_to_router():
    """The legacy wrapper delegates to the centralized router (which computes candidates itself)."""
    fake = _fake_tester(_make_params(fallback_to_next_region=True))
    with (
        patch("sdcm.tester.TestConfig") as test_config,
        patch("sdcm.tester.gce_region_fallback.provision_with_fallback") as router,
    ):
        test_config.return_value.test_id.return_value = "test-id"
        ClusterTester._get_cluster_gce_with_region_fallback(fake, loader_info={}, db_info={}, monitor_info={})
    router.assert_called_once()
    kwargs = router.call_args.kwargs
    assert kwargs["error_factory"] is CriticalTestFailure
    assert kwargs["network_name"] == "qa-vpc"
    assert "region_candidates" not in kwargs  # router resolves candidates internally
