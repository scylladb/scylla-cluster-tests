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
# Copyright (c) 2022 ScyllaDB

import logging
import itertools
import time

from botocore.exceptions import ClientError

from sdcm.utils.aws_region import AwsRegion
from sdcm.sct_config import AWS_SUPPORTED_REGIONS

LOG = logging.getLogger(__name__)

SCT_PEERING_TAG_KEY = "CreatedBy"
SCT_PEERING_TAG_VALUE = "SCT"


class AwsVpcPeering:
    def __init__(self, regions=None, dry_run=False):
        regions = regions or AWS_SUPPORTED_REGIONS
        self.regions = [AwsRegion(r) for r in regions]
        self.dry_run = dry_run

    @staticmethod
    def _find_existing_peering(origin_region, target_region):
        """Find an existing peering between two VPCs, regardless of which side created it."""
        peering_name = f"{origin_region.SCT_VPC_NAME}-{origin_region.region_name}-{target_region.region_name}"

        candidates = []
        for requester_vpc, accepter_vpc, client in [
            (origin_region.sct_vpc.vpc_id, target_region.sct_vpc.vpc_id, origin_region.client),
            (target_region.sct_vpc.vpc_id, origin_region.sct_vpc.vpc_id, origin_region.client),
        ]:
            response = client.describe_vpc_peering_connections(
                Filters=[
                    {"Name": "requester-vpc-info.vpc-id", "Values": [requester_vpc]},
                    {"Name": "accepter-vpc-info.vpc-id", "Values": [accepter_vpc]},
                ]
            )
            candidates.extend(response.get("VpcPeeringConnections", []))

        active = [p for p in candidates if p["Status"]["Code"] == "active"]
        if active:
            return active[0], peering_name

        pending = [p for p in candidates if p["Status"]["Code"] == "pending-acceptance"]
        if pending:
            return pending[0], peering_name

        return None, peering_name

    def _tag_peering(self, region, peering_id, peering_name):
        if self.dry_run:
            LOG.info(
                "[DRY-RUN] Would tag peering %s with Name=%s, %s=%s",
                peering_id,
                peering_name,
                SCT_PEERING_TAG_KEY,
                SCT_PEERING_TAG_VALUE,
            )
            return
        try:
            region.client.create_tags(
                Resources=[peering_id],
                Tags=[
                    {"Key": "Name", "Value": peering_name},
                    {"Key": SCT_PEERING_TAG_KEY, "Value": SCT_PEERING_TAG_VALUE},
                ],
            )
        except ClientError as ex:
            LOG.warning("Failed to tag peering %s: %s", peering_id, ex)

    def peering_connection(self, origin_region, target_region):
        chosen, peering_name = self._find_existing_peering(origin_region, target_region)

        if chosen:
            peering_id = chosen["VpcPeeringConnectionId"]
            LOG.info(
                "peering for %s-%s found: %s (state: %s)",
                origin_region.region_name,
                target_region.region_name,
                peering_id,
                chosen["Status"]["Code"],
            )
        else:
            if self.dry_run:
                LOG.info("[DRY-RUN] Would create peering %s-%s", origin_region.region_name, target_region.region_name)
                return None, None
            res = origin_region.client.create_vpc_peering_connection(
                PeerVpcId=target_region.sct_vpc.vpc_id,
                VpcId=origin_region.sct_vpc.vpc_id,
                PeerRegion=target_region.region_name,
                TagSpecifications=[
                    {
                        "ResourceType": "vpc-peering-connection",
                        "Tags": [
                            {"Key": "Name", "Value": peering_name},
                            {"Key": SCT_PEERING_TAG_KEY, "Value": SCT_PEERING_TAG_VALUE},
                        ],
                    },
                ],
            )
            peering_id = res.get("VpcPeeringConnection").get("VpcPeeringConnectionId")
            LOG.info(
                "Created new peering %s for %s-%s",
                peering_id,
                origin_region.region_name,
                target_region.region_name,
            )

        self._tag_peering(origin_region, peering_id, peering_name)

        # Determine which region is the accepter based on actual peering data
        if chosen:
            accepter_region_name = chosen["AccepterVpcInfo"]["Region"]
        else:
            accepter_region_name = target_region.region_name

        if accepter_region_name == target_region.region_name:
            accepter_region = target_region
        else:
            accepter_region = origin_region

        accepter_vpx = accepter_region.resource.VpcPeeringConnection(peering_id)
        accepter_vpx.wait_until_exists()
        accepter_vpx.reload()

        if accepter_vpx.status.get("Code") == "pending-acceptance":
            if self.dry_run:
                LOG.info(
                    "[DRY-RUN] Would accept peering %s from %s",
                    peering_id,
                    accepter_region.region_name,
                )
            else:
                LOG.info("Accepting peering %s from %s", peering_id, accepter_region.region_name)
                try:
                    accepter_vpx.accept()
                except ClientError as ex:
                    error_code = ex.response.get("Error", {}).get("Code", "")
                    if error_code == "VpcPeeringConnectionAlreadyExists":
                        LOG.info("Peering %s already accepted, continuing", peering_id)
                    else:
                        raise
            self._tag_peering(accepter_region, peering_id, peering_name)
        elif accepter_vpx.status.get("Code") == "active":
            LOG.info("Peering %s already active", peering_id)
            self._tag_peering(accepter_region, peering_id, peering_name)
        else:
            LOG.warning("Peering %s in unexpected state: %s", peering_id, accepter_vpx.status)
            return None, None

        if not self.dry_run:
            for attempt in range(30):
                accepter_vpx.reload()
                if accepter_vpx.status.get("Code") == "active":
                    break
                LOG.info(
                    "Waiting for peering %s to become active (current: %s, attempt %d/30)",
                    peering_id,
                    accepter_vpx.status.get("Code"),
                    attempt + 1,
                )
                time.sleep(2)
            else:
                LOG.error(
                    "Peering %s did not become active after 60s, skipping route configuration",
                    peering_id,
                )
                return None, None

        origin_vpx = origin_region.resource.VpcPeeringConnection(peering_id)
        target_vpx = target_region.resource.VpcPeeringConnection(peering_id)
        return origin_vpx, target_vpx

    def cleanup_duplicate_peerings(self, origin_region, target_region, active_peering_id):
        """Delete pending-acceptance peerings that duplicate an already-active connection."""
        response = origin_region.client.describe_vpc_peering_connections(
            Filters=[
                {"Name": "requester-vpc-info.vpc-id", "Values": [origin_region.sct_vpc.vpc_id]},
                {"Name": "accepter-vpc-info.vpc-id", "Values": [target_region.sct_vpc.vpc_id]},
                {"Name": "status-code", "Values": ["pending-acceptance", "failed", "expired", "rejected"]},
            ]
        )
        for pcx in response.get("VpcPeeringConnections", []):
            pcx_id = pcx["VpcPeeringConnectionId"]
            if pcx_id == active_peering_id:
                continue
            if self.dry_run:
                LOG.info(
                    "[DRY-RUN] Would delete duplicate peering %s (state: %s) for %s-%s",
                    pcx_id,
                    pcx["Status"]["Code"],
                    origin_region.region_name,
                    target_region.region_name,
                )
            else:
                LOG.info(
                    "Deleting duplicate peering %s (state: %s) for %s-%s",
                    pcx_id,
                    pcx["Status"]["Code"],
                    origin_region.region_name,
                    target_region.region_name,
                )
                try:
                    origin_region.client.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
                except ClientError as ex:
                    LOG.warning("Failed to delete peering %s: %s", pcx_id, ex)

    def _ensure_route(self, route_table, vpc_peering_id, origin_region, **route_kwargs):
        try:
            if self.dry_run:
                dest_cidr = route_kwargs.get("DestinationCidrBlock") or route_kwargs.get("DestinationIpv6CidrBlock")
                for route in route_table.routes:
                    existing_cidr = route.destination_cidr_block or route.destination_ipv6_cidr_block
                    if existing_cidr == dest_cidr:
                        if route.vpc_peering_connection_id == vpc_peering_id:
                            return
                        LOG.info(
                            "[DRY-RUN] Would replace route %s in %s rt %s: %s -> %s",
                            dest_cidr,
                            origin_region.region_name,
                            route_table.route_table_id,
                            route.vpc_peering_connection_id,
                            vpc_peering_id,
                        )
                        return
                LOG.info(
                    "[DRY-RUN] Would create route %s -> %s in %s rt %s",
                    dest_cidr,
                    vpc_peering_id,
                    origin_region.region_name,
                    route_table.route_table_id,
                )
                return
            route_table.create_route(VpcPeeringConnectionId=vpc_peering_id, **route_kwargs)
        except ClientError as ex:
            if "already exists" not in str(ex):
                raise
            dest_cidr = route_kwargs.get("DestinationCidrBlock") or route_kwargs.get("DestinationIpv6CidrBlock")
            for route in route_table.routes:
                existing_cidr = route.destination_cidr_block or route.destination_ipv6_cidr_block
                if existing_cidr == dest_cidr and route.vpc_peering_connection_id != vpc_peering_id:
                    LOG.warning(
                        "Replacing stale route %s in %s rt %s: %s -> %s",
                        dest_cidr,
                        origin_region.region_name,
                        route_table.route_table_id,
                        route.vpc_peering_connection_id,
                        vpc_peering_id,
                    )
                    origin_region.client.replace_route(
                        RouteTableId=route_table.route_table_id,
                        VpcPeeringConnectionId=vpc_peering_id,
                        **route_kwargs,
                    )
                    break

    def _tag_route_table(self, origin_region, route_table):
        if self.dry_run:
            return
        try:
            origin_region.client.create_tags(
                Resources=[route_table.route_table_id],
                Tags=[
                    {"Key": SCT_PEERING_TAG_KEY, "Value": SCT_PEERING_TAG_VALUE},
                ],
            )
        except ClientError as ex:
            LOG.warning("Failed to tag route table %s: %s", route_table.route_table_id, ex)

    def configure_route_tables(self, origin_region, target_region, vpc_peering_id):
        for index in range(AwsRegion.SCT_SUBNET_PER_AZ):
            route_table = origin_region.sct_route_table(index=index)
            self._tag_route_table(origin_region, route_table)
            self._ensure_route(
                route_table,
                vpc_peering_id,
                origin_region,
                DestinationCidrBlock=str(target_region.vpc_ipv4_cidr),
            )
            self._ensure_route(
                route_table,
                vpc_peering_id,
                origin_region,
                DestinationIpv6CidrBlock=str(target_region.vpc_ipv6_cidr),
            )

    def cleanup_stale_routes(self, region):
        """Remove blackhole routes from SCT route tables in a region."""
        for index in range(AwsRegion.SCT_SUBNET_PER_AZ):
            route_table = region.sct_route_table(index=index)
            if not route_table:
                continue
            for route in route_table.routes:
                if route.state == "blackhole" and route.vpc_peering_connection_id:
                    dest = route.destination_cidr_block or route.destination_ipv6_cidr_block
                    if self.dry_run:
                        LOG.info(
                            "[DRY-RUN] Would remove blackhole route %s via %s from %s rt %s",
                            dest,
                            route.vpc_peering_connection_id,
                            region.region_name,
                            route_table.route_table_id,
                        )
                        continue
                    LOG.info(
                        "Removing blackhole route %s via %s from %s rt %s",
                        dest,
                        route.vpc_peering_connection_id,
                        region.region_name,
                        route_table.route_table_id,
                    )
                    try:
                        if route.destination_cidr_block:
                            region.client.delete_route(
                                RouteTableId=route_table.route_table_id,
                                DestinationCidrBlock=route.destination_cidr_block,
                            )
                        else:
                            region.client.delete_route(
                                RouteTableId=route_table.route_table_id,
                                DestinationIpv6CidrBlock=route.destination_ipv6_cidr_block,
                            )
                    except ClientError as ex:
                        LOG.warning("Failed to delete route %s: %s", dest, ex)

    SCT_SG_PEERING_DESC = "SCT-peering:{region}"
    SCT_SG_LOCAL_DESC = "SCT-local:{region}"

    def open_security_group(self, origin_region, target_region):
        if self.dry_run:
            LOG.info(
                "[DRY-RUN] Would authorize SG ingress in %s for %s",
                origin_region.region_name,
                target_region.region_name,
            )
            return
        try:
            origin_region.sct_security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "IpProtocol": "-1",
                        "IpRanges": [
                            {
                                "CidrIp": str(target_region.vpc_ipv4_cidr),
                                "Description": self.SCT_SG_PEERING_DESC.format(region=target_region.region_name),
                            }
                        ],
                        "Ipv6Ranges": [
                            {
                                "CidrIpv6": str(target_region.vpc_ipv6_cidr),
                                "Description": self.SCT_SG_PEERING_DESC.format(region=target_region.region_name),
                            }
                        ],
                    },
                ]
            )
        except ClientError as ex:
            if "InvalidPermission.Duplicate" not in str(ex):
                raise

    def open_security_group_to_local(self, origin_region):
        if self.dry_run:
            LOG.info("[DRY-RUN] Would authorize local SG ingress in %s", origin_region.region_name)
            return
        try:
            origin_region.sct_security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "IpProtocol": "-1",
                        "IpRanges": [
                            {
                                "CidrIp": str(origin_region.vpc_ipv4_cidr),
                                "Description": self.SCT_SG_LOCAL_DESC.format(region=origin_region.region_name),
                            }
                        ],
                        "Ipv6Ranges": [
                            {
                                "CidrIpv6": str(origin_region.vpc_ipv6_cidr),
                                "Description": self.SCT_SG_LOCAL_DESC.format(region=origin_region.region_name),
                            }
                        ],
                    },
                ]
            )
        except ClientError as ex:
            if "InvalidPermission.Duplicate" not in str(ex):
                raise

    def cleanup_stale_sg_rules(self, region, valid_regions):
        """Remove SCT security group rules that reference regions no longer in the supported list."""
        sg = region.sct_security_group
        valid_region_names = {r.region_name if hasattr(r, "region_name") else r for r in valid_regions}
        rules_to_revoke = []

        for rule in sg.ip_permissions:
            for ip_range in rule.get("IpRanges", []) + rule.get("Ipv6Ranges", []):
                desc = ip_range.get("Description", "")
                if not desc.startswith("SCT-peering:"):
                    continue
                referenced_region = desc.split(":", 1)[1]
                if referenced_region not in valid_region_names:
                    if self.dry_run:
                        LOG.info("[DRY-RUN] Would revoke stale SG rule in %s: %s", region.region_name, desc)
                    else:
                        LOG.info("Revoking stale SG rule in %s: %s", region.region_name, desc)
                    rules_to_revoke.append(rule)
                    break

        if rules_to_revoke and not self.dry_run:
            try:
                sg.revoke_ingress(IpPermissions=rules_to_revoke)
            except ClientError as ex:
                LOG.warning("Failed to revoke stale SG rules in %s: %s", region.region_name, ex)

    def configure(self):
        regions_pair: list[tuple[AwsRegion, AwsRegion]] = list(itertools.combinations(self.regions, 2))
        for origin_region, target_region in regions_pair:
            LOG.info(
                "Configure peering for: %s %s, %s %s",
                origin_region.region_name,
                origin_region.vpc_ipv4_cidr,
                target_region.region_name,
                target_region.vpc_ipv4_cidr,
            )
            origin_vpx, target_vpx = self.peering_connection(origin_region, target_region)
            if origin_vpx is None:
                continue

            LOG.info("Configuring route tables")
            self.configure_route_tables(origin_region, target_region, origin_vpx.vpc_peering_connection_id)
            self.configure_route_tables(target_region, origin_region, target_vpx.vpc_peering_connection_id)

            self.cleanup_duplicate_peerings(origin_region, target_region, origin_vpx.vpc_peering_connection_id)

            LOG.info("Configuring security groups")
            self.open_security_group(origin_region, target_region)
            self.open_security_group(origin_region=target_region, target_region=origin_region)
            self.open_security_group_to_local(origin_region)

        LOG.info("Cleaning up stale routes and security group rules")
        for region in self.regions:
            self.cleanup_stale_routes(region)
            self.cleanup_stale_sg_rules(region, self.regions)
