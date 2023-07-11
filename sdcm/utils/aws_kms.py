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
# Copyright (c) 2023 ScyllaDB

from itertools import cycle
import logging

import botocore
import boto3

LOGGER = logging.getLogger(__name__)


class AwsKms:
    NUM_OF_KMS_KEYS = 3
    KMS_KEYS_TAGS = {
        'Purpose': 'Rotation',
        'UsedBy': 'QA',
    }

    def __init__(self, region_names):
        if not region_names:
            raise ValueError("'region_names' parameter cannot be empty")
        self.region_names = region_names if isinstance(region_names, list) else [region_names]
        self.mapping = {
            region_name: {
                'client': boto3.client('kms', region_name=region_name),
                'kms_key_ids': [],
                'kms_keys_aliases': {},
            } for region_name in self.region_names
        }
        self.num_of_tags_to_match = len(self.KMS_KEYS_TAGS)

    def create_kms_key(self, region_name):
        LOGGER.info("Creating KMS key in the '%s' region", region_name)
        kms_key = self.mapping[region_name]['client'].create_key(
            Description='qa-kms-key-for-rotation',
            Tags=[{'TagKey': k, 'TagValue': v} for k, v in self.KMS_KEYS_TAGS.items()],
        )
        self.mapping[region_name]['kms_key_ids'].append(kms_key['KeyMetadata']['KeyId'])

    def get_kms_key_tags(self, kms_key_id, region_name):
        try:
            tags = self.mapping[region_name]['client'].list_resource_tags(KeyId=kms_key_id, Limit=999)['Tags']
            return {tag['TagKey']: tag['TagValue'] for tag in tags}
        except botocore.exceptions.ClientError as exc:
            LOGGER.debug(exc.response)
            if any(msg in exc.response['Error']['Code'] for msg in ('AccessDeniedException', 'NotFound')):
                return {}
            raise

    def get_kms_keys(self, region_name, next_marker=None):
        client, kwargs = self.mapping[region_name]['client'], {'Limit': 30}
        if next_marker:
            kwargs['Marker'] = next_marker
        kms_keys = client.list_keys(**kwargs)
        for kms_key in kms_keys['Keys']:
            current_kms_key_id = kms_key['KeyId']
            if not client.describe_key(KeyId=current_kms_key_id)['KeyMetadata']['Enabled']:
                continue
            yield current_kms_key_id
        if kms_keys.get("NextMarker"):
            yield from self.get_kms_keys(region_name=region_name, next_marker=kms_keys["NextMarker"])

    def find_or_create_suitable_kms_keys(self):
        for region_name in self.region_names:
            if self.NUM_OF_KMS_KEYS <= len(self.mapping[region_name]['kms_key_ids']):
                continue
            for current_kms_key_id in self.get_kms_keys(region_name):
                current_kms_key_tags = self.get_kms_key_tags(current_kms_key_id, region_name)
                if not current_kms_key_tags:
                    continue
                kms_key_tags_match_counter = 0
                for expected_k, expected_v in self.KMS_KEYS_TAGS.items():
                    if current_kms_key_tags.get(expected_k) != expected_v:
                        break
                    kms_key_tags_match_counter += 1
                if kms_key_tags_match_counter >= self.num_of_tags_to_match:
                    self.mapping[region_name]['kms_key_ids'].append(current_kms_key_id)
                if self.NUM_OF_KMS_KEYS == len(self.mapping[region_name]['kms_key_ids']):
                    break
            while self.NUM_OF_KMS_KEYS > len(self.mapping[region_name]['kms_key_ids']):
                self.create_kms_key(region_name)

    def get_next_kms_key(self, kms_key_alias_name, region_name):
        # Create endless KMS keys iterator
        if kms_key_alias_name not in self.mapping[region_name]['kms_keys_aliases']:
            self.mapping[region_name]['kms_keys_aliases'][kms_key_alias_name] = cycle(
                self.mapping[region_name]['kms_key_ids'])

        kms_key_id_candidate = next(self.mapping[region_name]['kms_keys_aliases'][kms_key_alias_name])
        # Walk through the aliases of the KMS key candidate and check that our alias is not there
        for alias in self.mapping[region_name]['client'].list_aliases(
                KeyId=kms_key_id_candidate, Limit=999)['Aliases']:
            if kms_key_alias_name == alias['AliasName']:
                # Current KMS Key candidate is already assigned to the alias, so, return another one
                return next(self.mapping[region_name]['kms_keys_aliases'][kms_key_alias_name])
        # Current KMS Key candidate is not assigned to the alias, use it
        return kms_key_id_candidate

    def create_alias(self, kms_key_alias_name, tolerate_already_exists=True):
        self.find_or_create_suitable_kms_keys()
        for region_name in self.region_names:
            kms_key_id = self.get_next_kms_key(kms_key_alias_name, region_name)
            LOGGER.info(
                "Creating '%s' alias for the '%s' KMS key in the '%s' region",
                kms_key_alias_name, kms_key_id, region_name)
            try:
                self.mapping[region_name]['client'].create_alias(
                    AliasName=kms_key_alias_name, TargetKeyId=kms_key_id)
            except botocore.exceptions.ClientError as exc:
                LOGGER.debug(exc.response)
                if not ('AlreadyExistsException' in exc.response['Error']['Code'] and tolerate_already_exists):
                    raise

    def rotate_kms_key(self, kms_key_alias_name):
        self.find_or_create_suitable_kms_keys()
        for region_name in self.region_names:
            new_kms_key_id = self.get_next_kms_key(kms_key_alias_name, region_name)
            LOGGER.info(
                "Assigning the '%s' alias to the '%s' KMS key in the '%s' region",
                kms_key_alias_name, new_kms_key_id, region_name)
            self.mapping[region_name]['client'].update_alias(
                AliasName=kms_key_alias_name, TargetKeyId=new_kms_key_id)

    def delete_alias(self, kms_key_alias_name, tolerate_errors=True):
        LOGGER.info("Deleting the '%s' alias in the KMS", kms_key_alias_name)
        for region_name in self.region_names:
            try:
                self.mapping[region_name]['client'].delete_alias(AliasName=kms_key_alias_name)
                if kms_key_alias_name in self.mapping[region_name]['kms_keys_aliases']:
                    self.mapping[region_name]['kms_keys_aliases'].remove(kms_key_alias_name)
            except botocore.exceptions.ClientError as exc:
                LOGGER.debug(exc.response)
                if not tolerate_errors:
                    raise
