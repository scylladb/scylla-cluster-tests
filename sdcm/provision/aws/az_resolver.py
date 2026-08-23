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

import logging
from typing import Callable

import boto3
from botocore.exceptions import ClientError

from sdcm.provision.aws.capacity_errors import ProvisioningCapacityExhausted, is_capacity_error
from sdcm.provision.aws.spot_placement_score import get_scores, rank_az_letters, rank_regions
from sdcm.sct_config import AWS_SUPPORTED_REGIONS
from sdcm.sct_provision.common.utils import INSTANCE_PROVISION_ON_DEMAND
from sdcm.test_config import TestConfig
from sdcm.utils.aws_peering import AwsVpcPeering
from sdcm.utils.aws_region import AwsRegion

LOGGER = logging.getLogger(__name__)

# instance types added mid-test; AZ fallback at provisioning time is unsafe for them
_DYNAMIC_ADD_TYPE_PARAMS: tuple[str, ...] = (
    "instance_type_db_target",
    "nemesis_grow_shrink_instance_type",
)


def _node_count_positive(value) -> bool:
    """Indicates if an SCT node-count parameter resolves to >0 in any region."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, list):
        return any(int(n) > 0 for n in value if str(n).strip().lstrip("-").isdigit())
    if isinstance(value, str):
        return any(int(n) > 0 for n in value.split() if n.strip().lstrip("-").isdigit())
    return False


def _has_loaders(params) -> bool:
    return _node_count_positive(params.get("n_loaders"))


def _has_monitor(params) -> bool:
    return _node_count_positive(params.get("n_monitor_nodes"))


def _has_oracle(params) -> bool:
    return params.get("db_type") in ("mixed_scylla", "mixed_cassandra") and _node_count_positive(
        params.get("n_test_oracle_db_nodes")
    )


def _has_zero_token(params) -> bool:
    return bool(params.get("use_zero_nodes")) and _node_count_positive(params.get("n_db_zero_token_nodes"))


def _has_vector_store(params) -> bool:
    return _node_count_positive(params.get("n_vector_store_nodes"))


def _always(params) -> bool:  # noqa: ARG001
    return True


def _node_count_total(value) -> int:
    """Sum an SCT node-count parameter across regions/DCs, treating anything unparseable as 0."""
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return max(int(value), 0)
    if isinstance(value, list):
        return sum(int(n) for n in value if str(n).strip().lstrip("-").isdigit() and int(n) > 0)
    if isinstance(value, str):
        return sum(int(n) for n in value.split() if n.strip().lstrip("-").isdigit() and int(n) > 0)
    return 0


# Node-count params paired with the gate deciding whether the test launches them at all, mirroring
# `_INSTANCE_TYPE_PARAM_GATES`. Used to size the spot placement score request.
_NODE_COUNT_PARAM_GATES: tuple[tuple[str, Callable[[object], bool]], ...] = (
    ("n_db_nodes", _always),
    ("n_loaders", _has_loaders),
    ("n_monitor_nodes", _has_monitor),
    ("n_db_zero_token_nodes", _has_zero_token),
    ("n_test_oracle_db_nodes", _has_oracle),
)


def is_spot_placement_scoring_enabled(params) -> bool:
    """Return True when AZ/region ordering should consult `ec2:GetSpotPlacementScores`.

    Pointless for on-demand runs (the scores only describe spot capacity), so those skip the API entirely.
    """
    if not params.get("use_spot_placement_scores"):
        return False
    if params.get("cluster_backend") != "aws":
        return False
    return params.get("instance_provision") != INSTANCE_PROVISION_ON_DEMAND


# (instance-type param key, gate). The gate decides whether this type will actually
# be launched by the test; types whose gate is False are excluded from the AZ-filter
# intersection so SCT defaults like `instance_type_vector_store: 't4g.medium'` do not
# constrain AZ choice for tests that have n_vector_store_nodes=0.
_INSTANCE_TYPE_PARAM_GATES: tuple[tuple[str, Callable[[object], bool]], ...] = (
    ("instance_type_db", _always),
    ("instance_type_loader", _has_loaders),
    ("instance_type_monitor", _has_monitor),
    ("zero_token_instance_type_db", _has_zero_token),
    ("instance_type_db_oracle", _has_oracle),
    ("instance_type_db_target", _always),
    ("nemesis_grow_shrink_instance_type", _always),
    ("instance_type_vector_store", _has_vector_store),
)


class NoValidAvailabilityZoneError(Exception):
    """Raised when no AZ supports all required instance types in the configured region(s)."""


class AZResolver:
    """Resolve `availability_zone` config to AZs supporting all required instance types."""

    def __init__(self, params):
        self._params = params

    def required_instance_types(self) -> list[str]:
        """Get the deduplicated list of instance types a test will launch."""
        selected = []
        for key, gate in _INSTANCE_TYPE_PARAM_GATES:
            instance_type = self._params.get(key)
            if instance_type and gate(self._params) and instance_type not in selected:
                selected.append(instance_type)
        return selected

    def resolve(self) -> None:
        """Filter `availability_zone` to AZs supporting all required types in every region.

        If `pre_filter_unavailable_availability_zones` is False, returns without
        modifying params. Multi-AZ configs ("a,b,c") have invalid AZs replaced with
        valid alternatives that are valid across all configured regions.
        Raises `NoValidAvailabilityZoneError` when no AZ letter is valid in every region.
        """
        if not self._params.get("pre_filter_unavailable_availability_zones"):
            LOGGER.info("Upfront AZ filter disabled; skipping AZResolver.resolve()")
            return

        instance_types = self.required_instance_types()
        if not instance_types:
            LOGGER.info("No instance types declared; skipping AZ filter")
            return

        region_names = self._region_names()
        if not region_names:
            LOGGER.info("No region configured; skipping AZ filter")
            return

        configured_letters = self._configured_az_letters()
        supported_letters = self._common_supported_letters(region_names, configured_letters, instance_types)
        if not supported_letters:
            raise NoValidAvailabilityZoneError(
                f"No availability zone supports all required instance types {instance_types} "
                f"across regions {region_names}; cannot proceed with provisioning."
            )

        # Spot scores only reorder the pool the AZ slots are filled from. By default the configured letters keep
        # priority, so an explicit `availability_zone` stays honoured and only backfilled slots are score-driven;
        # `spot_score_overrides_configured_az` opts into letting the score win outright.
        ranked_letters = self._rank_letters_by_spot_score(region_names, supported_letters)
        if self._params.get("spot_score_overrides_configured_az") and is_spot_placement_scoring_enabled(self._params):
            resolved = ranked_letters[: len(configured_letters)]
        else:
            resolved = [letter for letter in configured_letters if letter in supported_letters]
            for letter in ranked_letters:
                if len(resolved) >= len(configured_letters):
                    break
                if letter not in resolved:
                    resolved.append(letter)

        new_value = ",".join(resolved)
        original_value = self._params.get("availability_zone")
        if new_value != original_value:
            # Two different reasons land here, and saying the wrong one sends whoever reads this log
            # chasing a capacity problem that does not exist: the configured AZ may be genuinely
            # unsupported for the required types, or it may be perfectly valid and merely outranked on
            # spot placement score because `spot_score_overrides_configured_az` is on.
            dropped = [letter for letter in configured_letters if letter not in supported_letters]
            if dropped:
                LOGGER.warning(
                    "AZResolver: availability_zone '%s' does not support all required "
                    "instance types %s in regions %s; replacing with '%s'",
                    original_value,
                    instance_types,
                    region_names,
                    new_value,
                )
            else:
                LOGGER.info(
                    "AZResolver: availability_zone '%s' is valid but outranked on spot placement score "
                    "in regions %s; using '%s' (spot_score_overrides_configured_az is enabled)",
                    original_value,
                    region_names,
                    new_value,
                )
            self._params["availability_zone"] = new_value
        else:
            LOGGER.info("AZResolver: availability_zone '%s' already valid for regions %s", original_value, region_names)

    def get_fallback_candidates(self) -> list[list[str]]:
        """Build ordered AZ candidate sets for capacity-error fallback.

        Each candidate preserves the configured `availability_zone` shape:
        single-AZ stays single-AZ, and multi-AZ stays multi-AZ. The first
        candidate is the configured AZ list with unsupported letters dropped and
        backfilled from supported alternatives. Later candidates replace one
        supported AZ per slot. For multi-region configs, AZ letters are
        intersected across regions so every candidate is valid everywhere.

        Single-AZ config "a", supported ["a","b","c"] -> [["a"], ["b"], ["c"]]
        Single-AZ config "c", supported ["a","b"]     -> [["a"], ["b"]]   # 'c' dropped, 'a' backfilled
        Multi-AZ config "a,b,c", supported ["a","b","c","d"]
            -> [["a","b","c"], ["d","b","c"], ["a","d","c"], ["a","b","d"]]
        """
        configured_letters = self._configured_az_letters()
        if not configured_letters:
            return []

        supported_letters = self._supported_az_letters()
        if not supported_letters:
            return [configured_letters]

        # Rank once, then use the ranked pool for both the primary candidate's backfill and the later
        # single-slot replacements, so fallback attempts walk the best-scoring AZs first.
        supported_letters = self._rank_letters_by_spot_score(self._region_names(), supported_letters)

        if self._params.get("spot_score_overrides_configured_az") and is_spot_placement_scoring_enabled(self._params):
            primary = supported_letters[: len(configured_letters)]
        else:
            primary = [letter for letter in configured_letters if letter in supported_letters]
            for letter in supported_letters:
                if len(primary) >= len(configured_letters):
                    break
                if letter not in primary:
                    primary.append(letter)

        if not primary:
            return [configured_letters]

        candidates = [primary]
        seen = {tuple(primary)}

        for slot_index in range(len(primary)):
            for letter in supported_letters:
                if letter in primary:
                    continue
                candidate = list(primary)
                candidate[slot_index] = letter
                if tuple(candidate) not in seen:
                    candidates.append(candidate)
                    seen.add(tuple(candidate))

        return candidates

    def get_region_fallback_candidates(self) -> list[tuple[str, list[str]]]:
        """Collects ordered ``(region, az_letters)`` candidates for cluster region fallback.

        A region is eligible only if it is:
            - VPC-peered with the runner region
            - able to supply the configured number of AZs that support the required instance types.
        The current region is excluded (as the starting point).

        Eligible regions are ordered by spot placement score when scoring is enabled, so the first relocation
        attempt goes to the region most likely to have spot capacity instead of the next one in
        `AWS_SUPPORTED_REGIONS` order. The peering and AZ-cardinality gates are unchanged.
        """
        region_names = self._region_names()
        if not region_names:
            return []
        current_region = region_names[0]
        configured_letters = self._configured_az_letters()
        cardinality = len(configured_letters) or 1
        instance_types = self.required_instance_types()

        candidates = []
        for region in self._ordered_fallback_regions(exclude={current_region}, instance_types=instance_types):
            if not self._is_region_peered(current_region, region):
                LOGGER.info("Region fallback: skipping %s (no active VPC peering with %s)", region, current_region)
                continue
            letters = self._common_supported_letters([region], configured_letters, instance_types)
            if len(letters) < cardinality:
                LOGGER.info(
                    "Region fallback: skipping %s (only %d AZ letter(s) support %s, need %d)",
                    region,
                    len(letters),
                    instance_types,
                    cardinality,
                )
                continue
            letters = self._rank_letters_by_spot_score([region], letters)
            candidates.append((region, letters[:cardinality]))
        return candidates

    def get_preferred_spot_region(self) -> tuple[str, list[str]] | None:
        """Return a ``(region, az_letters)`` worth relocating to BEFORE the first provisioning attempt.

        Region fallback is reactive: it only relocates after the configured region has actually failed. When the
        configured region is a poor spot bet to begin with (e.g. `region: random` landed on it by shuffle), that
        first attempt is predictably wasted. This returns a better-scoring, VPC-peered candidate when its score
        beats the configured region's by at least `spot_score_region_relocation_margin`, else None.

        Opt-in: the runner already lives in the configured region, so relocating the cluster splits them across
        a peering link. Callers must respect the same eligibility gates as `get_region_fallback_candidates`.
        """
        margin = int(self._params.get("spot_score_region_relocation_margin") or 0)
        region_names = self._region_names()
        instance_types = self.required_instance_types()
        if (
            margin <= 0
            or not is_spot_placement_scoring_enabled(self._params)
            or len(region_names) != 1
            or not instance_types
        ):
            return None
        current_region = region_names[0]

        scored = get_scores(
            instance_types=instance_types,
            target_capacity=self.spot_target_capacity(),
            regions=[current_region, *(r for r in AWS_SUPPORTED_REGIONS if r != current_region)],
            single_az=False,
        )
        by_region = {item.region: item.score for item in scored}
        if (current_score := by_region.get(current_region)) is None:
            return None

        for candidate in self.get_region_fallback_candidates():
            region, az_letters = candidate
            candidate_score = by_region.get(region)
            if candidate_score is None or candidate_score - current_score < margin:
                continue
            LOGGER.info(
                "Spot placement scores: relocating upfront from %s (score %d) to %s (score %d), margin >= %d",
                current_region,
                current_score,
                region,
                candidate_score,
                margin,
            )
            return region, az_letters
        LOGGER.info(
            "Spot placement scores: keeping configured region %s (score %s); no candidate beats it by %d",
            current_region,
            current_score,
            margin,
        )
        return None

    def _ordered_fallback_regions(self, exclude: set[str], instance_types: list[str]) -> list[str]:
        """Candidate regions for relocation, spot-score-ordered when enabled."""
        regions = [region for region in AWS_SUPPORTED_REGIONS if region not in exclude]
        if not is_spot_placement_scoring_enabled(self._params) or len(regions) < 2:
            return regions
        ranked = rank_regions(
            instance_types=instance_types, target_capacity=self.spot_target_capacity(), regions=regions
        )
        if ranked != regions:
            LOGGER.info("Spot placement scores reordered region fallback candidates: %s -> %s", regions, ranked)
        return ranked or regions

    def get_dc_fallback_candidates(self, dc_index: int) -> list[tuple[str, list[str]]]:
        """Collect ordered ``(region, az_letters)`` candidates to relocate the DC at ``dc_index``."""
        region_names = self._region_names()
        if dc_index >= len(region_names):
            return []

        in_use_regions = set(region_names)
        staying_regions = [region for index, region in enumerate(region_names) if index != dc_index]
        configured_letters = self._configured_az_letters()
        required_az_count = len(configured_letters) or 1
        instance_types = [instance_type for instance_type in [self._params.get("instance_type_db")] if instance_type]

        candidates = []
        for region in self._ordered_fallback_regions(exclude=in_use_regions, instance_types=instance_types):
            if not all(self._is_region_peered(staying, region) for staying in staying_regions):
                LOGGER.info(
                    "Region fallback (DC %d): skipping %s (no active VPC peering with all staying DCs %s)",
                    dc_index,
                    region,
                    staying_regions,
                )
                continue

            supported_letters = self._common_supported_letters([region], configured_letters, instance_types)
            if len(supported_letters) < required_az_count:
                LOGGER.info(
                    "Region fallback (DC %d): skipping %s (only %d AZ letter(s) support %s, need %d)",
                    dc_index,
                    region,
                    len(supported_letters),
                    instance_types,
                    required_az_count,
                )
                continue
            supported_letters = self._rank_letters_by_spot_score([region], supported_letters)
            candidates.append((region, supported_letters[:required_az_count]))
        return candidates

    @staticmethod
    def _is_region_peered(current_region: str, candidate_region: str) -> bool:
        """Return True only when an ACTIVE VPC peering exists between the two regions."""
        try:
            peering, _ = AwsVpcPeering._find_existing_peering(
                AwsRegion(region_name=current_region), AwsRegion(region_name=candidate_region)
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Region fallback: peering check %s<->%s failed: %s", current_region, candidate_region, exc)
            return False
        return peering is not None and peering.get("Status", {}).get("Code") == "active"

    def _supported_az_letters(self) -> list[str]:
        """Return AZ letters supported in EVERY configured region for the required types.

        When no instance types are declared, returns the configured AZ letters as-is.
        """
        instance_types = self.required_instance_types()
        region_names = self._region_names()

        if not instance_types or not region_names:
            return self._configured_az_letters()

        return self._common_supported_letters(region_names, self._configured_az_letters(), instance_types)

    @staticmethod
    def _common_supported_letters(
        region_names: list[str], configured_letters: list[str], instance_types: list[str]
    ) -> list[str]:
        """Intersect supported AZ letters across all configured regions.

        Returns letters in this order: configured letters first (preserving user
        intent), then any additional supported letters sorted alphabetically.
        """
        common: set[str] | None = None
        for region in region_names:
            preferred_full = [f"{region}{letter}" for letter in configured_letters]
            supported_full = AwsRegion(region_name=region).get_common_availability_zones(
                instance_types=instance_types,
                preferred_azs=preferred_full,
            )
            letters = {az[len(region) :] for az in supported_full}
            common = letters if common is None else common & letters

        if not common:
            return []

        configured_first = [letter for letter in configured_letters if letter in common]
        additional = sorted(common - set(configured_first))
        return configured_first + additional

    def spot_target_capacity(self) -> int:
        """Total instance count a spot placement score request should be sized for."""
        total = sum(
            _node_count_total(self._params.get(key)) for key, gate in _NODE_COUNT_PARAM_GATES if gate(self._params)
        )
        return max(total, 1)

    def _rank_letters_by_spot_score(self, region_names: list[str], letters: list[str]) -> list[str]:
        """Reorder `letters` by spot placement score, best-first.

        A no-op unless spot scoring is enabled, and a no-op for multi-region configs: AZ letters there must be
        valid in every region, and a letter that scores well in one region may score poorly in another, so
        there is no single meaningful ranking. Falls back to the given order whenever scores are unavailable.
        """
        if len(letters) < 2 or not is_spot_placement_scoring_enabled(self._params):
            return letters
        if len(region_names) != 1:
            LOGGER.debug("Spot placement scores: skipping AZ ranking for multi-region config %s", region_names)
            return letters

        ranked = rank_az_letters(
            instance_types=self.required_instance_types(),
            target_capacity=self.spot_target_capacity(),
            region=region_names[0],
            az_letters=letters,
            min_score=int(self._params.get("spot_placement_score_min") or 0),
        )
        if ranked != letters:
            LOGGER.info(
                "Spot placement scores reordered AZ candidates in %s: %s -> %s", region_names[0], letters, ranked
            )
        return ranked or letters

    def _region_names(self) -> list[str]:
        if regions := getattr(self._params, "region_names", None):
            return list(regions)
        raw = self._params.get("region_name") or ""
        return raw.split()

    def _configured_az_letters(self) -> list[str]:
        raw = self._params.get("availability_zone") or ""
        return [letter.strip() for letter in raw.split(",") if letter.strip()]

    def probe_capacity_for_types(self, types: list[str], az: str) -> dict[str, bool]:
        """Confirm AWS has on-demand capacity for each `type` in `az`.

        Launches and immediately terminates one on-demand instance per type. Returns
        `{type: True}` on success and `{type: False}` for capacity-class errors per
        `is_capacity_error`. Re-raises any non-capacity ClientError. Probe instances
        always terminate in `finally` so a probe failure cannot leak a running
        instance.
        """
        if not types:
            return {}

        region_names = self._region_names()
        if not region_names:
            raise RuntimeError("Pre-flight capacity probe: no region configured")
        region = region_names[0]
        region_az = az if az.startswith(region) else f"{region}{az[-1]}"

        aws_region = AwsRegion(region_name=region)
        subnet = aws_region.sct_subnet(region_az=region_az, subnet_index=0)
        if subnet is None:
            raise RuntimeError(
                f"Pre-flight capacity probe: no SCT subnet found in {region_az}; "
                f"run `hydra prepare-aws-region --region {region}` to enable"
            )
        security_group = aws_region.sct_security_group
        if security_group is None:
            raise RuntimeError(f"Pre-flight capacity probe: no SCT security group in {region}")

        ec2_client = boto3.client("ec2", region_name=region)
        ssm_client = boto3.client("ssm", region_name=region)

        results = {}
        for instance_type in types:
            ami_id = _get_amazon_linux_ami_for_type(ec2_client, ssm_client, instance_type)
            tags = TestConfig.common_tags(self._params) | {
                "Purpose": "capacity-probe",
                "Name": f"capacity-probe-{instance_type}-{region_az}",
                "keep_alive": "false",
            }
            instance_ids = []
            try:
                response = ec2_client.run_instances(
                    ImageId=ami_id,
                    InstanceType=instance_type,
                    MinCount=1,
                    MaxCount=1,
                    SubnetId=subnet.subnet_id,
                    SecurityGroupIds=[security_group.group_id],
                    TagSpecifications=[
                        {
                            "ResourceType": "instance",
                            "Tags": [{"Key": k, "Value": str(v)} for k, v in tags.items()],
                        }
                    ],
                )
                instance_ids = [i["InstanceId"] for i in response.get("Instances", [])]
                results[instance_type] = True
                LOGGER.info("Pre-flight probe: capacity available for %s in %s", instance_type, region_az)
            except ClientError as exc:
                if is_capacity_error(exc):
                    LOGGER.warning("Pre-flight probe: no capacity for %s in %s: %s", instance_type, region_az, exc)
                    results[instance_type] = False
                else:
                    LOGGER.warning(
                        "Pre-flight probe: skipping %s in %s due to non-capacity error (%s): %s",
                        instance_type,
                        region_az,
                        exc.response.get("Error", {}).get("Code"),
                        exc,
                    )
                    results[instance_type] = True
            finally:
                if instance_ids:
                    try:
                        ec2_client.terminate_instances(InstanceIds=instance_ids)
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning(
                            "Pre-flight probe: failed to terminate %s in %s: %s", instance_ids, region_az, exc
                        )
        return results


def _get_amazon_linux_ami_for_type(ec2_client, ssm_client, instance_type: str) -> str:
    """Return the latest Amazon Linux 2 AMI matching `instance_type`'s architecture."""
    info = ec2_client.describe_instance_types(InstanceTypes=[instance_type])
    arch = info["InstanceTypes"][0]["ProcessorInfo"]["SupportedArchitectures"][0]
    ssm_arch = "arm64" if arch == "arm64" else "x86_64"
    ssm_path = f"/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-{ssm_arch}-gp2"
    return ssm_client.get_parameter(Name=ssm_path)["Parameter"]["Value"]


def run_pre_flight_capacity_probe(params) -> None:
    """Probe each configured AZ for capacity of every dynamically-added instance type."""
    if not params.get("pre_flight_capacity_probe"):
        return

    types = [t for key in _DYNAMIC_ADD_TYPE_PARAMS if (t := params.get(key))]
    if not types:
        return

    az_letters = [az.strip() for az in (params.get("availability_zone") or "").split(",") if az.strip()]
    if not az_letters:
        return

    LOGGER.info(
        "pre_flight_capacity_probe enabled — probing types %s in AZs %s (~1 min per type per AZ)",
        types,
        az_letters,
    )

    resolver = AZResolver(params)
    failures = []
    for letter in az_letters:
        results = resolver.probe_capacity_for_types(types=types, az=letter)
        failures.extend(f"{instance_type}@{letter}" for instance_type, ok in results.items() if not ok)

    if failures:
        raise ProvisioningCapacityExhausted(f"Pre-flight capacity probe failed for: {failures}")
