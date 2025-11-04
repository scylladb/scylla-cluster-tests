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
# Copyright (c) 2020 ScyllaDB

import unittest
from unittest.mock import MagicMock, Mock, patch

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.utils import resources_cleanup
from sdcm.utils.resources_cleanup import (
    clean_cloud_resources,
    clean_clusters_gke,
    clean_elastic_ips_aws,
    clean_instances_aws,
    clean_instances_gce,
    clean_resources_docker,
)


SCT_RUNNER_AWS = {
    "Tags": [{"Key": "NodeType", "Value": "sct-runner"}],
    "InstanceId": "i-1111",
}


@patch("boto3.client")
class CleanInstanceAwsTest(unittest.TestCase):
    def test_empty_tags_dict(self, _):
        self.assertRaisesRegex(AssertionError, "not provided", clean_instances_aws, {})

    def test_empty_list(self, ec2_client):
        with patch.object(resources_cleanup, "list_instances_aws", Mock(return_value={})) as list_instances_aws:
            resources_cleanup.clean_instances_aws({"TestId": 1111, })
        list_instances_aws.assert_called_with(tags_dict={"TestId": 1111, }, group_as_region=True)
        ec2_client().terminate_instances.assert_not_called()

    def test_sct_runner(self, ec2_client):
        with patch.object(
                resources_cleanup, "list_instances_aws",
                Mock(return_value={"eu-north-1": [SCT_RUNNER_AWS]})) as list_instances_aws:
            clean_instances_aws({"TestId": 1111, })
        list_instances_aws.assert_called_with(tags_dict={"TestId": 1111, }, group_as_region=True)
        ec2_client().terminate_instances.assert_not_called()

    def test_terminate(self, ec2_client):
        with patch.object(
                resources_cleanup, "list_instances_aws",
                Mock(return_value={"eu-north-1": [{"InstanceId": "i-1111"}]})) as list_instances_aws:
            clean_instances_aws({"TestId": 1111, })
        list_instances_aws.assert_called_with(tags_dict={"TestId": 1111, }, group_as_region=True)
        ec2_client().terminate_instances.assert_called_once_with(InstanceIds=["i-1111"])


@patch("boto3.client")
class CleanElasticIpsAws(unittest.TestCase):
    def test_empty_tags_dict(self, _):
        self.assertRaisesRegex(AssertionError, "not provided", clean_elastic_ips_aws, {})

    def test_empty_list(self, ec2_client):
        with patch.object(
                resources_cleanup, "list_elastic_ips_aws",
                Mock(return_value={})) as list_elastic_ips_aws:
            clean_elastic_ips_aws({"TestId": 1111, })
        list_elastic_ips_aws.assert_called_with(tags_dict={"TestId": 1111, }, group_as_region=True)
        ec2_client().disassociate_address.assert_not_called()
        ec2_client().release_address.assert_not_called()

    def test_release(self, ec2_client):
        return_value = {"eu-north-1": [{
            "AssociationId": 2222,
            "AllocationId": 3333,
            "PublicIp": '127.0.0.1',
        }]}
        with patch.object(
                resources_cleanup, "list_elastic_ips_aws",
                Mock(return_value=return_value)) as list_elastic_ips_aws:
            clean_elastic_ips_aws({"TestId": 1111, })
        list_elastic_ips_aws.assert_called_with(tags_dict={"TestId": 1111, }, group_as_region=True)
        ec2_client().disassociate_address.assert_called_once_with(AssociationId=2222)
        ec2_client().release_address.assert_called_once_with(AllocationId=3333)


class CleanClustersGkeTest(unittest.TestCase):
    def test_empty_tags_dict(self):
        self.assertRaisesRegex(AssertionError, "not provided", clean_clusters_gke, {})

    def test_destroy(self):
        cluster = MagicMock()
        with patch.object(
                resources_cleanup, "list_clusters_gke",
                Mock(return_value=[cluster])) as list_clusters_gke:
            clean_clusters_gke({"TestId": 1111, })
        list_clusters_gke.assert_called_with(tags_dict={"TestId": 1111, })
        cluster.destroy.assert_called_once_with()


class CleanInstancesGceTest(unittest.TestCase):
    def test_empty_tags_dict(self):
        self.assertRaisesRegex(AssertionError, "not provided", clean_instances_gce, {})

    def test_destroy(self):
        instance = MagicMock()
        with patch.object(
                resources_cleanup, "list_instances_gce",
                Mock(return_value=[instance])) as list_instances_gce:
            with patch.object(
                    resources_cleanup, 'get_gce_compute_instances_client',
                    Mock(return_value=(instance, dict(project_id='test')))):
                clean_instances_gce({"TestId": 1111, })
        list_instances_gce.assert_called_with(tags_dict={"TestId": 1111, })
        instance.delete.assert_called_once_with(instance=instance.name, project='test', zone=unittest.mock.ANY)


class CleanResourcesDockerTest(unittest.TestCase):
    def test_empty_tags_dict(self):
        self.assertRaisesRegex(AssertionError, "not provided", clean_resources_docker, {})

    @staticmethod
    def test_destroy():
        image = MagicMock()
        container = MagicMock()
        with patch.object(
                resources_cleanup, "list_resources_docker",
                Mock(return_value={"images": [image], "containers": [container]})) as list_resources_docker:
            clean_resources_docker({"TestId": 1111, })
        list_resources_docker.assert_called_with(
            tags_dict={"TestId": 1111, },
            builder_name=None,
            group_as_builder=False,
        )
        container.remove.assert_called_once_with(v=True, force=True)
        image.client.images.remove.assert_called_once_with(image=image.id, force=True)


class CleanCloudResourcesTest(unittest.TestCase):
    integration = False  # set it to True if you want to run test with actual cloud operations.
    functions_to_patch = (
        "sdcm.utils.resources_cleanup.clean_instances_aws",
        "sdcm.utils.resources_cleanup.clean_elastic_ips_aws",
        "sdcm.utils.resources_cleanup.clean_clusters_gke",
        "sdcm.utils.resources_cleanup.clean_orphaned_gke_disks",
        "sdcm.utils.resources_cleanup.clean_clusters_eks",
        "sdcm.utils.resources_cleanup.clean_instances_gce",
        "sdcm.utils.resources_cleanup.clean_instances_azure",
        "sdcm.utils.resources_cleanup.clean_resources_docker",
        "sdcm.utils.resources_cleanup.clean_test_security_groups",
        "sdcm.utils.resources_cleanup.clean_load_balancers_aws",
    )

    @pytest.fixture(autouse=True)
    def fixture_config(self, monkeypatch):
        monkeypatch.setenv(name="SCT_CLUSTER_BACKEND", value="aws")
        monkeypatch.setenv(name="SCT_REGION_NAME", value="eu-north-1 eu-west-1")

        self.config = SCTConfiguration()

        if not self.integration:
            for func in self.functions_to_patch:
                patch(func).start()
        yield
        patch.stopall()

    def test_no_tag_testid_and_runbyuser(self):
        params = {}
        res = clean_cloud_resources(params, self.config)
        self.assertFalse(res)

    def test_other_tags_and_no_testid_and_runbyuser(self):
        params = {"NodeType": "scylla-db"}
        res = clean_cloud_resources(params, self.config)
        self.assertFalse(res)

    def test_tag_testid_only(self):
        params = {"RunByUser": "test"}
        res = clean_cloud_resources(params, self.config)
        self.assertTrue(res)

    def test_tag_runbyuser_only(self):
        params = {"TestId": "1111"}
        res = clean_cloud_resources(params, self.config)
        self.assertTrue(res)

    def test_tags_testid_and_runbyuser(self):
        params = {"RunByUser": "test", "TestId": "1111"}
        res = clean_cloud_resources(params, self.config)
        self.assertTrue(res)

    def test_tags_testid_and_runbyuser_with_other(self):
        params = {"RunByUser": "test",
                  "TestId": "1111",
                  "NodeType": "monitor"}
        res = clean_cloud_resources(params, self.config)
        self.assertTrue(res)
