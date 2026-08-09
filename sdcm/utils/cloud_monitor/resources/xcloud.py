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
# Copyright (c) 2025 ScyllaDB

import datetime
import logging
import re
from dataclasses import dataclass

import pytz

from sdcm.cloud_api_client import ScyllaCloudAPIClient
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)

KEEP_HOURS_PATTERN = re.compile(r"-keep-(\d+)h$")
SHORT_ID_PATTERN = re.compile(r"-([0-9a-f]{8})-keep-")


@dataclass
class XCloudCluster:
    """ScyllaDB Cloud cluster data for reporting"""

    environment: str
    cloud_provider: str
    cluster_name: str
    cluster_id: int
    status: str
    owner: str
    created_at: datetime.datetime
    db_node_count: int
    vs_node_count: int
    keep_hours: int | None
    hours_remaining: float | None


class XCloudResources:
    """Collects all xcloud clusters across environments"""

    ENVIRONMENTS = ["lab", "staging", "prod"]

    def __init__(self):
        self.clusters: list[XCloudCluster] = []
        self.collect_all_clusters()

    def collect_all_clusters(self):
        for environment in self.ENVIRONMENTS:
            try:
                self.clusters.extend(self.get_clusters_for_environment(environment))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(f"Failed to collect xcloud clusters for '{environment}': {exc}")

    def get_clusters_for_environment(self, environment: str) -> list[XCloudCluster]:
        try:
            credentials = KeyStore().get_cloud_rest_credentials(environment)
            api_client = ScyllaCloudAPIClient(api_url=credentials["base_url"], auth_token=credentials["api_token"])
            account_id = api_client.get_account_details().get("accountId")
            clusters = api_client.get_clusters(account_id=account_id, enriched=True)
            LOGGER.info(f"Found {len(clusters)} cluster(s) in environment '{environment}'")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(f"Failed to initialize API client for '{environment}' environment: {exc}")
            return []

        processed = [self.process_cluster(api_client, account_id, cluster, environment) for cluster in clusters]
        return [c for c in processed if c is not None]

    def process_cluster(self, api_client, account_id, cluster, environment):
        try:
            cluster_id = cluster.get("id")
            cluster_name = cluster.get("clusterName")
            cluster_details = api_client.get_cluster_details(
                account_id=account_id, cluster_id=cluster_id, enriched=True
            )

            created_at_str = cluster_details.get("createdAt")
            created_at = datetime.datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            keep_hours, hours_remaining = self.calculate_hours_remaining(cluster_name, created_at_str)

            db_node_count = len(
                api_client.get_cluster_nodes(account_id=account_id, cluster_id=cluster_id, enriched=True)
            )
            vs_node_count = self.get_vs_node_count(api_client, account_id, cluster_id, cluster_name)

            return XCloudCluster(
                environment=environment,
                cloud_provider=cluster_details.get("cloudProvider", {}).get("name"),
                cluster_name=cluster_name,
                cluster_id=cluster_id,
                status=cluster_details.get("status", "Unknown"),
                owner=self.extract_owner_from_name(cluster_name),
                created_at=created_at,
                db_node_count=db_node_count,
                vs_node_count=vs_node_count,
                keep_hours=keep_hours,
                hours_remaining=hours_remaining,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.error(f"Failed to process cluster {cluster.get('clusterName')} in '{environment}': {exc}")
            return None

    @staticmethod
    def get_vs_node_count(api_client, account_id, cluster_id, cluster_name):
        """Calculate the total number of VS nodes across all datacenters."""
        vs_node_count = 0
        try:
            dcs = api_client.get_cluster_dcs(account_id=account_id, cluster_id=cluster_id, enriched=True) or []
            for dc in dcs:
                dc_id = dc.get("id")
                availability_zones = (
                    api_client.get_vector_search_nodes(account_id=account_id, cluster_id=cluster_id, dc_id=dc_id).get(
                        "availabilityZones"
                    )
                    or []
                )
                vs_node_count += sum(len(az.get("nodes", [])) for az in availability_zones)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug(f"Cluster {cluster_name}: could not retrieve VS node information: {exc}")
        return vs_node_count

    @staticmethod
    def extract_owner_from_name(cluster_name: str) -> str:
        """Extract owner username from cluster name"""
        match = SHORT_ID_PATTERN.search(cluster_name)
        if match:
            parts = cluster_name[: match.start()].rsplit("-", 1)
            if len(parts) > 1:
                return parts[-1].replace("_", ".")
        return "N/A"

    @staticmethod
    def calculate_hours_remaining(cluster_name: str, created_at_str: str) -> tuple[int | None, float | None]:
        """Extract keep_hours from cluster name and calculate hours remaining to keep the cluster"""
        match = KEEP_HOURS_PATTERN.search(cluster_name)
        if not match:
            return None, None

        keep_hours = int(match.group(1))
        created_at = datetime.datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        age_hours = (datetime.datetime.now(tz=pytz.utc) - created_at).total_seconds() / 3600.0

        return keep_hours, keep_hours - age_hours
