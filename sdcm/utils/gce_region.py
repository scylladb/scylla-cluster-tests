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
from functools import cached_property

import googleapiclient.errors
from googleapiclient.discovery import build
from google.oauth2 import service_account
import google
import google.api_core.exceptions
from google.cloud import storage
from google.cloud import compute_v1
from google.cloud.compute_v1 import Firewall

from sdcm.keystore import KeyStore


LOGGER = logging.getLogger(__name__)


class GceRegion:
    SCT_NETWORK_NAME = "qa-vpc"
    SCT_BACKUP_SERVICE_ACCOUNT = "sct-manager-backup"

    def __init__(self, region_name):
        self.region_name = region_name
        info = KeyStore().get_gcp_credentials()
        self.project = info['project_id']

        credentials = service_account.Credentials.from_service_account_info(info)

        self.iam = build('iam', 'v1', credentials=credentials, cache_discovery=False)

        self.network_client = compute_v1.NetworksClient(credentials=credentials)
        self.firewall_client = compute_v1.FirewallsClient(credentials=credentials)
        self.storage_client = storage.Client(credentials=credentials)

    @property
    def backup_storage_bucket_name(self):
        return f"manager-backup-tests-{self.project}-{self.region_name}"

    @cached_property
    def network(self) -> compute_v1.Network:
        try:
            _network = self.network_client.get(project=self.project, network=self.SCT_NETWORK_NAME)
        except google.api_core.exceptions.NotFound:
            _network = self.create_network()
        return _network

    def create_network(self) -> compute_v1.Network:
        _network = compute_v1.Network()
        _network.name = self.SCT_NETWORK_NAME
        _network.auto_create_subnetworks = True
        self.network_client.insert(project=self.project, network_resource=_network)
        return self.network_client.get(project=self.project, network=self.SCT_NETWORK_NAME)

    def configure_firewall(self):
        firewall_configurations = [
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-ssh',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["22"])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-all-internal',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="all")],
                     source_ranges=["10.0.0.0/8"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-icmp',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="icmp")],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-grafana',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["3000"])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-node-gossip',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=['7000', '7001'])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-node-api',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=['10000'])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-node-cql',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=['9042', '9142', '19042', '19142'])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-prometheus',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=['9090', '9100', '9103', '9180'])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-http',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=['80'])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-alternator',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=['8080'])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-manager',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=['10001', '5080', '5443', '5090', '5112'])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-deny-all',
                     direction="INGRESS",
                     denied=[compute_v1.Denied(I_p_protocol="all")],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link,
                     target_tags=["sct-network-only"],
                     priority=200),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-only-sct-network',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="all")],
                     source_ranges=["10.0.0.0/8"],
                     network=self.network.self_link,
                     target_tags=["sct-network-only"],
                     priority=100),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-grafana-public',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["3000"])],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link,
                     target_tags=["sct-network-only"],
                     priority=100),
            Firewall(name=f'{self.SCT_NETWORK_NAME}-allow-public',
                     direction="INGRESS",
                     allowed=[compute_v1.Allowed(I_p_protocol="all")],
                     source_ranges=["0.0.0.0/0"],
                     network=self.network.self_link,
                     target_tags=["sct-allow-public"],
                     priority=100),
        ]
        for firewall_rule in firewall_configurations:
            try:
                self.firewall_client.insert(project=self.project, firewall_resource=firewall_rule)
            except google.api_core.exceptions.Conflict:
                firewall = self.firewall_client.get(project=self.project, firewall=firewall_rule.name)
                if firewall.allowed != firewall_rule.allowed or firewall.source_ranges != firewall_rule.source_ranges:
                    LOGGER.info("updating firewall")
                    LOGGER.info("before: %s %s", firewall.allowed, firewall.source_ranges)
                    LOGGER.info("after : %s %s", firewall_rule.allowed, firewall_rule.source_ranges)
                    self.firewall_client.patch(project=self.project, firewall=firewall_rule.name,
                                               firewall_resource=firewall_rule)

    def create_backup_service_account(self):
        """Creates a service account."""
        backup_service_account = None

        try:
            backup_service_account = self.iam.projects().serviceAccounts().create(
                name='projects/' + self.project,
                body={
                    'accountId': self.SCT_BACKUP_SERVICE_ACCOUNT,
                    'serviceAccount': {
                        'displayName': "Account for having access to the gcs bucket for SCT backup tests"
                    }
                }).execute()
            LOGGER.info('Created service account: %s', backup_service_account['email'])
        except googleapiclient.errors.HttpError as exc:
            if not exc.status_code == 409:
                raise
            service_accounts = self.iam.projects().serviceAccounts().list(
                name=f'projects/{self.project}', pageSize=100).execute()
            for service in service_accounts['accounts']:
                if self.SCT_BACKUP_SERVICE_ACCOUNT in service['name']:
                    backup_service_account = service
            assert backup_service_account, f"couldn't find {self.SCT_BACKUP_SERVICE_ACCOUNT} service"
        return backup_service_account

    def configure_backup_storage(self):
        try:
            bucket = self.storage_client.create_bucket(self.backup_storage_bucket_name, location=self.region_name)
        except google.api_core.exceptions.Conflict:
            bucket = self.storage_client.bucket(self.backup_storage_bucket_name)
        bucket.add_lifecycle_delete_rule(age=7)
        bucket.patch()

        service = self.create_backup_service_account()
        policy = bucket.get_iam_policy(requested_policy_version=3)
        role = 'roles/storage.objectAdmin'
        policy.bindings.append({"role": role, "members": {f"serviceAccount:{service['email']}"}})
        bucket.set_iam_policy(policy)

    def configure(self):
        LOGGER.info("Configuring '%s' region...", self.region_name)
        self.configure_firewall()
        self.create_backup_service_account()
        self.configure_backup_storage()
        LOGGER.info("Region configured successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    a = GceRegion('us-east1')
    print(a.configure())
