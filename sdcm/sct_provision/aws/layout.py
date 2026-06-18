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
# Copyright (c) 2021 ScyllaDB

import logging
import os
from functools import cached_property

from botocore.exceptions import ClientError

from sdcm.exceptions import CapacityReservationError
from sdcm.provision.aws.az_resolver import (
    AZResolver,
    is_az_fallback_enabled,
    is_region_fallback_enabled,
    run_pre_flight_capacity_probe,
)
from sdcm.provision.aws.capacity_errors import ProvisioningCapacityExhausted, RegionAMINotFoundError, is_capacity_error
from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.dedicated_host import SCTDedicatedHosts
from sdcm.provision.aws.region_fallback import (
    cleanup_region,
    enforce_single_region_gate,
    restore_region,
    switch_region,
)
from sdcm.provision.aws.utils import ec2_clients
from sdcm.sct_provision.aws.cluster import OracleDBCluster, DBCluster, LoaderCluster, MonitoringCluster, PlacementGroup
from sdcm.sct_provision.common.layout import SCTProvisionLayout
from sdcm.test_config import TestConfig


LOGGER = logging.getLogger(__name__)

_CLUSTER_CACHED_PROPS: tuple[str, ...] = (
    "db_cluster",
    "loader_cluster",
    "monitoring_cluster",
    "cs_db_cluster",
    "placement_group",
)


class SCTProvisionAWSLayout(SCTProvisionLayout, cluster_backend="aws"):
    @cached_property
    def _test_config(self):
        return TestConfig()

    def provision(self):
        if is_region_fallback_enabled(self._params):
            self._provision_with_region_fallback()
            return
        self._provision_once()

    def _provision_once(self) -> None:
        """Provision the whole cluster in the configured region, with AZ fallback. (No region fallback.)"""
        AZResolver(self._params).resolve()

        # capacity reservation handles its own AZ fallback internally
        if SCTCapacityReservation.is_capacity_reservation_enabled(self._params):
            self._do_provision()
            return

        if not is_az_fallback_enabled(self._params):
            self._do_provision()
            return

        candidates = AZResolver(self._params).get_fallback_candidates()
        if not candidates:
            self._do_provision()
            return

        region_exhausted, last_error = self._provision_az_loop(candidates)
        if region_exhausted:
            tried = ", ".join("+".join(c) for c in candidates)
            raise RuntimeError(f"Provisioning failed in all {len(candidates)} AZ candidate(s): {tried}") from last_error

    def _provision_az_loop(self, candidates: list[list[str]]) -> tuple[bool, Exception | None]:
        """Try each AZ candidate in the current region."""
        original_az = self._params.get("availability_zone")
        last_error: Exception | None = None
        for attempt_idx, candidate in enumerate(candidates):
            self._params["availability_zone"] = ",".join(candidate)

            if attempt_idx > 0:
                LOGGER.warning(
                    "Capacity error in previous AZ; retrying in '%s' (attempt %d/%d)",
                    self._params["availability_zone"],
                    attempt_idx + 1,
                    len(candidates),
                )
                self._clear_cluster_caches()

            try:
                self._do_provision()
                return False, last_error
            except (ClientError, ProvisioningCapacityExhausted) as exc:
                # ClientError is the on-demand path (raised by ec2.create_instances);
                # ProvisioningCapacityExhausted is the spot path.
                if isinstance(exc, ClientError) and not is_capacity_error(exc):
                    raise
                last_error = exc
                LOGGER.warning(
                    "Provision failed with capacity error in AZ '%s': %s", self._params["availability_zone"], exc
                )
                self._cleanup_partial_provision()

        self._params["availability_zone"] = original_az
        return True, last_error

    def _attempt_region(self) -> tuple[bool, Exception | None]:
        """Provision the whole cluster in the currently-configured region."""
        AZResolver(self._params).resolve()

        if SCTCapacityReservation.is_capacity_reservation_enabled(self._params):
            try:
                self._do_provision()
                return False, None
            except (CapacityReservationError, ProvisioningCapacityExhausted) as exc:
                return True, exc
            except ClientError as exc:
                if is_capacity_error(exc):
                    return True, exc
                raise

        if not is_az_fallback_enabled(self._params):
            return self._provision_capacity_aware()

        candidates = AZResolver(self._params).get_fallback_candidates()
        if not candidates:
            return self._provision_capacity_aware()

        return self._provision_az_loop(candidates)

    def _provision_capacity_aware(self) -> tuple[bool, Exception | None]:
        """Provision once; report capacity errors as region exhaustion and re-raise any other error."""
        try:
            self._do_provision()
        except (ClientError, ProvisioningCapacityExhausted) as exc:
            if isinstance(exc, ClientError) and not is_capacity_error(exc):
                raise
            return True, exc
        return False, None

    def _provision_with_region_fallback(self) -> None:
        """Provision in the current region, then relocate to fallback regions on capacity exhaustion.

        On success, keeps `os.environ["SCT_REGION_NAME"]` set to the resolved region.
        On failure, restores the original region/AZ in both env and params before raising.
        """
        self._enforce_single_region_gate()

        original_region = self._params.region_names[0] if self._params.region_names else None
        original_az = self._params.get("availability_zone")
        original_env_region = os.environ.get("SCT_REGION_NAME")

        source_region = original_region
        region_candidates = AZResolver(self._params).get_region_fallback_candidates()
        last_error: Exception | None = None

        for attempt_idx, candidate in enumerate([None, *region_candidates]):
            if attempt_idx > 0:
                target_region, az_letters = candidate
                LOGGER.warning(
                    "Region '%s' exhausted; relocating whole cluster to '%s' (region attempt %d/%d)",
                    source_region,
                    target_region,
                    attempt_idx + 1,
                    len(region_candidates) + 1,
                )
                try:
                    self._switch_region(target_region, az_letters, source_region)
                except RegionAMINotFoundError as exc:
                    last_error = exc
                    LOGGER.warning("Region '%s' ineligible (no equivalent AMI): %s", target_region, exc)
                    continue
                source_region = target_region

            try:
                region_exhausted, attempt_error = self._attempt_region()
            except Exception:
                self._restore_region(original_region, original_az, original_env_region)
                raise

            if not region_exhausted:
                return

            last_error = attempt_error or last_error
            self._cleanup_region(source_region)

        self._restore_region(original_region, original_az, original_env_region)
        tried = ", ".join(region for region, _ in region_candidates) or "(no eligible candidates)"
        raise RuntimeError(
            f"Provisioning failed in region '{original_region}' and all fallback candidates [{tried}]"
        ) from last_error

    def _enforce_single_region_gate(self) -> None:
        enforce_single_region_gate(self._params)

    def _switch_region(self, region: str, az_letters: list[str], source_region: str | None) -> None:
        switch_region(self._params, region, az_letters, source_region, invalidate_caches=self._clear_cluster_caches)

    def _restore_region(self, region: str | None, availability_zone: str, env_region: str | None) -> None:
        restore_region(self._params, region, availability_zone, env_region)

    def _cleanup_region(self, region: str | None) -> None:
        cleanup_region(self._test_config.test_id(), region, partial_cleanup=self._cleanup_partial_provision)

    def _do_provision(self):
        use_scylla_cloud = self._params.get("cluster_backend") == "xcloud" or self._params.get("xcloud_provider")

        # raises ProvisioningCapacityExhausted on probe failure; the surrounding
        # AZ fallback loop in `provision()` retries the next candidate.
        run_pre_flight_capacity_probe(self._params)

        if self.placement_group:
            self.placement_group.provision()
        SCTCapacityReservation.reserve(self._params)
        SCTDedicatedHosts.reserve(self._params)

        # skip DB cluster provisioning for Scylla Cloud
        if not use_scylla_cloud and self.db_cluster:
            self.db_cluster.provision()
        if self.monitoring_cluster:
            self.monitoring_cluster.provision()
        if self.loader_cluster:
            self.loader_cluster.provision()
        if self.cs_db_cluster:
            self.cs_db_cluster.provision()

    def _cleanup_partial_provision(self) -> None:
        """Terminate instances launched by any cluster that completed before the capacity error.

        Multi-region provisioning can leave partial instances spread across more than one
        region, so terminate per-region rather than against a single hardcoded region.
        """
        instance_ids_by_region: dict[str, list[str]] = {}
        for prop in _CLUSTER_CACHED_PROPS:
            if (cluster_obj := self.__dict__.get(prop)) is None:
                continue
            for instance in getattr(cluster_obj, "_provisioned_instances", None) or []:
                instance_id = getattr(instance, "instance_id", None) or getattr(instance, "id", None)
                region = self._instance_region(instance)
                if not instance_id or not region:
                    if instance_id:
                        LOGGER.warning("Cannot determine region for instance %s; skipping cleanup", instance_id)
                    continue
                instance_ids_by_region.setdefault(region, []).append(instance_id)

        if not instance_ids_by_region:
            return

        for region, instance_ids in instance_ids_by_region.items():
            LOGGER.info(
                "Terminating %d partially-provisioned instance(s) in %s: %s",
                len(instance_ids),
                region,
                instance_ids,
            )
            try:
                ec2_clients[region].terminate_instances(InstanceIds=instance_ids)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to terminate instances in %s: %s", region, exc)

    @staticmethod
    def _instance_region(instance) -> str | None:
        """Resolve the AWS region of a boto3 EC2 Instance from its bound client metadata."""
        meta = getattr(instance, "meta", None)
        client = getattr(meta, "client", None)
        client_meta = getattr(client, "meta", None)
        return getattr(client_meta, "region_name", None)

    def _clear_cluster_caches(self) -> None:
        """Drop cached cluster objects so the next attempt re-reads `availability_zone`."""
        for prop in _CLUSTER_CACHED_PROPS:
            self.__dict__.pop(prop, None)

    @cached_property
    def db_cluster(self):
        return DBCluster(
            params=self._params,
            common_tags=self._test_config.common_tags(),
            test_id=self._test_config.test_id(),
        )

    @cached_property
    def loader_cluster(self):
        return LoaderCluster(
            params=self._params,
            common_tags=self._test_config.common_tags(),
            test_id=self._test_config.test_id(),
        )

    @cached_property
    def monitoring_cluster(self):
        return MonitoringCluster(
            params=self._params,
            common_tags=self._test_config.common_tags(),
            test_id=self._test_config.test_id(),
        )

    @cached_property
    def cs_db_cluster(self):
        if not self._provision_another_scylla_cluster:
            return None
        return OracleDBCluster(
            params=self._params,
            common_tags=self._test_config.common_tags(),
            test_id=self._test_config.test_id(),
        )

    @cached_property
    def placement_group(self):
        return PlacementGroup(
            params=self._params,
            common_tags=self._test_config.common_tags(),
            test_id=self._test_config.test_id(),
        )
