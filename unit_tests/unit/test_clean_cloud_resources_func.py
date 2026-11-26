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

from unittest.mock import ANY, MagicMock, Mock, patch

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.utils import resources_cleanup
from sdcm.utils.resources_cleanup import (
    clean_cloud_resources,
    clean_clusters_gke,
    clean_elastic_ips_aws,
    clean_instances_aws,
    clean_instances_gce,
    clean_instances_oci,
    clean_resources_docker,
    clean_spot_fleet_requests_aws,
)
from utils.cloud_cleanup.aws.clean_aws import clean_unattached_security_groups

SCT_RUNNER_AWS = {
    "Tags": [{"Key": "NodeType", "Value": "sct-runner"}],
    "InstanceId": "i-1111",
}
FUNCTIONS_TO_PATCH = (
    "sdcm.utils.resources_cleanup.clean_emr_clusters",
    "sdcm.utils.resources_cleanup.clean_spot_fleet_requests_aws",
    "sdcm.utils.resources_cleanup.clean_instances_aws",
    "sdcm.utils.resources_cleanup.clean_elastic_ips_aws",
    "sdcm.utils.resources_cleanup.clean_clusters_gke",
    "sdcm.utils.resources_cleanup.clean_orphaned_gke_disks",
    "sdcm.utils.resources_cleanup.clean_clusters_eks",
    "sdcm.utils.resources_cleanup.clean_instances_gce",
    "sdcm.utils.resources_cleanup.clean_instances_azure",
    "sdcm.utils.resources_cleanup.clean_instances_oci",
    "sdcm.utils.resources_cleanup.clean_orphan_block_volumes_oci",
    "sdcm.utils.resources_cleanup.clean_resources_docker",
    "sdcm.utils.resources_cleanup.clean_test_security_groups",
    "sdcm.utils.resources_cleanup.clean_load_balancers_aws",
    "sdcm.utils.resources_cleanup.clean_launch_templates_aws",
    "sdcm.utils.resources_cleanup.clean_cloudformation_stacks_aws",
    "sdcm.utils.resources_cleanup.clean_placement_groups_aws",
    "sdcm.utils.resources_cleanup.clean_aws_kms_alias",
    "sdcm.utils.resources_cleanup.SCTCapacityReservation",
    "sdcm.utils.resources_cleanup.SCTDedicatedHosts",
)
INTEGRATION = False  # set it to True if you want to run test with actual cloud operations.


@pytest.fixture
def ec2_client():
    """Patch boto3.client for one test; the yielded mock's return value is the EC2 client."""
    with patch("boto3.client") as client:
        yield client


@pytest.fixture
def oci_instance():
    """Factory for a mocked OCI SCT instance with the given keep_action and lifecycle state."""

    def create(keep_action: str = "terminate", lifecycle_state: str = "RUNNING"):
        """Build one mocked OCI instance tagged with keep_action."""
        instance = MagicMock()
        instance.region = "us-ashburn-1"
        instance.id = "ocid1.instance.oc1..example"
        instance.display_name = "test-db-node"
        instance.lifecycle_state = lifecycle_state
        instance.defined_tags = {
            "sct": {
                "TestId": "1111",
                "NodeType": "scylla-db",
                "keep_action": keep_action,
            }
        }
        return instance

    return create


@pytest.fixture
def oci_block_volume():
    """Factory for a mocked OCI block volume with the given keep_action."""

    def create(keep_action: str = "terminate"):
        """Build one mocked OCI block volume tagged with keep_action."""
        volume = MagicMock()
        volume.id = "ocid1.volume.oc1..example"
        volume.display_name = "test-db-node-vol-0"
        volume.defined_tags = {
            "sct": {
                "TestId": "1111",
                "NodeType": "scylla-db",
                "keep_action": keep_action,
            }
        }
        return volume

    return create


@pytest.fixture
def cleanup_config(monkeypatch):
    """SCT config for an AWS run in two regions, with the cloud cleanup functions mocked."""
    monkeypatch.setenv(name="SCT_CLUSTER_BACKEND", value="aws")
    monkeypatch.setenv(name="SCT_REGION_NAME", value="eu-north-1 eu-west-1")

    config = SCTConfiguration()

    if not INTEGRATION:
        for func in FUNCTIONS_TO_PATCH:
            patch(func).start()
    yield config
    patch.stopall()


def fleet_config(request_id, state, tags):
    """Build a DescribeSpotFleetRequests entry with the given state and tags."""
    return {
        "SpotFleetRequestId": request_id,
        "SpotFleetRequestState": state,
        "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
    }


def paginator_returning(configs):
    """Return a paginator whose paginate() yields one page holding configs."""
    paginator = MagicMock()
    paginator.paginate.return_value = [{"SpotFleetRequestConfigs": configs}]
    return paginator


def test_clean_instances_aws_empty_tags_dict_raises(ec2_client):
    """clean_instances_aws refuses an empty tags dict."""
    with pytest.raises(AssertionError, match="not provided"):
        clean_instances_aws({})


def test_clean_instances_aws_empty_list_terminates_nothing(ec2_client):
    """clean_instances_aws terminates nothing when no instance matches the tags."""
    with patch.object(resources_cleanup, "list_instances_aws", Mock(return_value={})) as list_instances_aws:
        resources_cleanup.clean_instances_aws(
            {
                "TestId": 1111,
            }
        )
    list_instances_aws.assert_called_with(
        tags_dict={
            "TestId": 1111,
        },
        group_as_region=True,
    )
    ec2_client().terminate_instances.assert_not_called()


def test_clean_instances_aws_skips_sct_runner(ec2_client):
    """clean_instances_aws leaves the sct-runner instance running."""
    with patch.object(
        resources_cleanup, "list_instances_aws", Mock(return_value={"eu-north-1": [SCT_RUNNER_AWS]})
    ) as list_instances_aws:
        clean_instances_aws(
            {
                "TestId": 1111,
            }
        )
    list_instances_aws.assert_called_with(
        tags_dict={
            "TestId": 1111,
        },
        group_as_region=True,
    )
    ec2_client().terminate_instances.assert_not_called()


def test_clean_instances_aws_terminates_matching_instance(ec2_client):
    """clean_instances_aws terminates each matching instance by its InstanceId."""
    with patch.object(
        resources_cleanup, "list_instances_aws", Mock(return_value={"eu-north-1": [{"InstanceId": "i-1111"}]})
    ) as list_instances_aws:
        clean_instances_aws(
            {
                "TestId": 1111,
            }
        )
    list_instances_aws.assert_called_with(
        tags_dict={
            "TestId": 1111,
        },
        group_as_region=True,
    )
    ec2_client().terminate_instances.assert_called_once_with(InstanceIds=["i-1111"])


def test_clean_elastic_ips_aws_empty_tags_dict_raises(ec2_client):
    """clean_elastic_ips_aws refuses an empty tags dict."""
    with pytest.raises(AssertionError, match="not provided"):
        clean_elastic_ips_aws({})


def test_clean_elastic_ips_aws_empty_list_releases_nothing(ec2_client):
    """clean_elastic_ips_aws neither disassociates nor releases when no address matches."""
    with patch.object(resources_cleanup, "list_elastic_ips_aws", Mock(return_value={})) as list_elastic_ips_aws:
        clean_elastic_ips_aws(
            {
                "TestId": 1111,
            }
        )
    list_elastic_ips_aws.assert_called_with(
        tags_dict={
            "TestId": 1111,
        },
        group_as_region=True,
    )
    ec2_client().disassociate_address.assert_not_called()
    ec2_client().release_address.assert_not_called()


def test_clean_elastic_ips_aws_disassociates_and_releases(ec2_client):
    """clean_elastic_ips_aws disassociates then releases each matching address."""
    return_value = {
        "eu-north-1": [
            {
                "AssociationId": 2222,
                "AllocationId": 3333,
                "PublicIp": "127.0.0.1",
            }
        ]
    }
    with patch.object(
        resources_cleanup, "list_elastic_ips_aws", Mock(return_value=return_value)
    ) as list_elastic_ips_aws:
        clean_elastic_ips_aws(
            {
                "TestId": 1111,
            }
        )
    list_elastic_ips_aws.assert_called_with(
        tags_dict={
            "TestId": 1111,
        },
        group_as_region=True,
    )
    ec2_client().disassociate_address.assert_called_once_with(AssociationId=2222)
    ec2_client().release_address.assert_called_once_with(AllocationId=3333)


def test_clean_clusters_gke_empty_tags_dict_raises():
    """clean_clusters_gke refuses an empty tags dict."""
    with pytest.raises(AssertionError, match="not provided"):
        clean_clusters_gke({})


def test_clean_clusters_gke_destroys_cluster():
    """clean_clusters_gke destroys each matching GKE cluster."""
    cluster = MagicMock()
    with patch.object(resources_cleanup, "list_clusters_gke", Mock(return_value=[cluster])) as list_clusters_gke:
        clean_clusters_gke(
            {
                "TestId": 1111,
            }
        )
    list_clusters_gke.assert_called_with(
        tags_dict={
            "TestId": 1111,
        }
    )
    cluster.destroy.assert_called_once_with()


def test_clean_instances_gce_empty_tags_dict_raises():
    """clean_instances_gce refuses an empty tags dict."""
    with pytest.raises(AssertionError, match="not provided"):
        clean_instances_gce({})


def test_clean_instances_gce_deletes_instance():
    """clean_instances_gce deletes each matching GCE instance in its project."""
    instance = MagicMock()
    with (
        patch.object(resources_cleanup, "list_instances_gce", Mock(return_value=[instance])) as list_instances_gce,
        patch.object(
            resources_cleanup,
            "get_gce_compute_instances_client",
            Mock(return_value=(instance, dict(project_id="test"))),
        ),
    ):
        clean_instances_gce(
            {
                "TestId": 1111,
            }
        )
    list_instances_gce.assert_called_with(
        tags_dict={
            "TestId": 1111,
        }
    )
    instance.delete.assert_called_once_with(instance=instance.name, project="test", zone=ANY)


def test_clean_instances_oci_terminate_calls_argus(oci_instance):
    """clean_instances_oci terminates a keep_action=terminate instance and reports it to Argus."""
    instance = oci_instance(keep_action="terminate")
    mock_compute_client = MagicMock()
    mock_argus_client = MagicMock()

    with (
        patch.object(resources_cleanup, "list_instances_oci", return_value=[instance]) as list_instances_oci,
        patch.object(resources_cleanup, "OciService") as service_class,
        patch.object(
            resources_cleanup,
            "init_argus_client",
            return_value=mock_argus_client,
        ) as init_argus_client,
        patch.object(resources_cleanup, "terminate_resource_in_argus") as terminate_in_argus,
    ):
        service_class.return_value.get_compute_client.return_value = mock_compute_client
        clean_instances_oci({"TestId": "1111"})

    list_instances_oci.assert_called_once_with(tags_dict={"TestId": "1111"}, verbose=False)
    init_argus_client.assert_called_once_with("1111")
    mock_compute_client.terminate_instance.assert_called_once_with("ocid1.instance.oc1..example")
    mock_compute_client.instance_action.assert_not_called()
    terminate_in_argus.assert_called_once_with(client=mock_argus_client, resource_name="test-db-node")


def test_clean_instances_oci_stop_when_keep_action_is_not_terminate(oci_instance):
    """clean_instances_oci stops, instead of terminating, an instance whose keep_action is not terminate."""
    instance = oci_instance(keep_action="stop")
    mock_compute_client = MagicMock()

    with (
        patch.object(resources_cleanup, "list_instances_oci", return_value=[instance]) as list_instances_oci,
        patch.object(resources_cleanup, "OciService") as service_class,
        patch.object(resources_cleanup, "terminate_resource_in_argus") as terminate_in_argus,
    ):
        service_class.return_value.get_compute_client.return_value = mock_compute_client
        clean_instances_oci({"TestId": "1111"})

    list_instances_oci.assert_called_once_with(tags_dict={"TestId": "1111"}, verbose=False)
    mock_compute_client.instance_action.assert_called_once_with("ocid1.instance.oc1..example", "STOP")
    mock_compute_client.terminate_instance.assert_not_called()
    terminate_in_argus.assert_not_called()


@pytest.mark.parametrize(
    ("keep_action", "should_delete"),
    [
        pytest.param("terminate", True, id="delete-volume"),
        pytest.param("stop", False, id="keep-volume"),
    ],
)
def test_clean_orphan_block_volumes_oci_keep_action_behavior(oci_block_volume, keep_action, should_delete):
    """clean_orphan_block_volumes_oci deletes an orphan volume only when its keep_action is terminate."""
    volume = oci_block_volume(keep_action=keep_action)
    mock_block_storage_client = MagicMock()

    with (
        patch.object(resources_cleanup, "OciService") as service_class,
        patch.object(resources_cleanup, "get_oci_compartment_id", return_value="ocid1.compartment.test"),
        patch(
            "sdcm.utils.resources_cleanup.oci.pagination.list_call_get_all_results_generator",
            return_value=[volume],
        ) as pager,
        patch.object(resources_cleanup, "delete_oci_volume_with_retry") as delete_volume,
    ):
        service_class.return_value.get_block_storage_client.return_value = mock_block_storage_client
        resources_cleanup.clean_orphan_block_volumes_oci(
            tags_dict={"TestId": "1111", "NodeType": ["scylla-db"]},
            regions=["us-ashburn-1"],
        )

    pager.assert_called_once_with(
        mock_block_storage_client.list_volumes,
        yield_mode="record",
        compartment_id="ocid1.compartment.test",
        lifecycle_state="AVAILABLE",
    )

    if should_delete:
        delete_volume.assert_called_once_with(
            block_storage_client=mock_block_storage_client,
            volume_id="ocid1.volume.oc1..example",
            volume_name="test-db-node-vol-0",
            region="us-ashburn-1",
            logger=resources_cleanup.LOGGER,
        )
    else:
        delete_volume.assert_not_called()


def test_clean_resources_docker_empty_tags_dict_raises():
    """clean_resources_docker refuses an empty tags dict."""
    with pytest.raises(AssertionError, match="not provided"):
        clean_resources_docker({})


def test_clean_resources_docker_removes_containers_and_images():
    """clean_resources_docker force-removes the matching container and image."""
    image = MagicMock()
    container = MagicMock()
    with patch.object(
        resources_cleanup,
        "list_resources_docker",
        Mock(return_value={"images": [image], "containers": [container]}),
    ) as list_resources_docker:
        clean_resources_docker(
            {
                "TestId": 1111,
            }
        )
    list_resources_docker.assert_called_with(
        tags_dict={
            "TestId": 1111,
        },
    )
    container.remove.assert_called_once_with(v=True, force=True)
    image.client.images.remove.assert_called_once_with(image=image.id, force=True)


def test_no_tag_testid_and_runbyuser(cleanup_config):
    """clean_cloud_resources does nothing for empty tags."""
    params = {}
    res = clean_cloud_resources(params, cleanup_config)
    assert not res


def test_other_tags_and_no_testid_and_runbyuser(cleanup_config):
    """clean_cloud_resources does nothing when only unrelated tags are given."""
    params = {"NodeType": "scylla-db"}
    res = clean_cloud_resources(params, cleanup_config)
    assert not res


def test_tag_testid_only(cleanup_config):
    """clean_cloud_resources runs when only RunByUser is given."""
    params = {"RunByUser": "test"}
    res = clean_cloud_resources(params, cleanup_config)
    assert res


def test_tag_runbyuser_only(cleanup_config):
    """clean_cloud_resources runs when only TestId is given."""
    params = {"TestId": "1111"}
    res = clean_cloud_resources(params, cleanup_config)
    assert res


def test_tags_testid_and_runbyuser(cleanup_config):
    """clean_cloud_resources runs when TestId and RunByUser are both given."""
    params = {"RunByUser": "test", "TestId": "1111"}
    res = clean_cloud_resources(params, cleanup_config)
    assert res


def test_tags_testid_and_runbyuser_with_other(cleanup_config):
    """clean_cloud_resources runs when TestId and RunByUser are given alongside other tags."""
    params = {"RunByUser": "test", "TestId": "1111", "NodeType": "monitor"}
    res = clean_cloud_resources(params, cleanup_config)
    assert res


def test_step_failure_does_not_abort_cleanup(cleanup_config):
    """A cleanup step that raises does not stop later cleanup steps."""
    # SCT-507: a cleanup step that raises (e.g. an unreachable region) must not
    # abort the whole run; later steps still execute.
    resources_cleanup.SCTDedicatedHosts.release_by_tags.side_effect = RuntimeError("me-south-1 unreachable")
    res = clean_cloud_resources({"RunByUser": "test"}, cleanup_config)
    assert res  # the run completed and did not propagate the exception
    # clean_elastic_ips_aws runs right after the failing dedicated-hosts step
    # regions depend on fallback_to_next_region, so only the tags and dry_run are pinned
    resources_cleanup.clean_elastic_ips_aws.assert_called_once_with({"RunByUser": "test"}, regions=ANY, dry_run=False)


# SCT-779: Spot Fleet Requests are tagged with the standard SCT tags at creation, so clean-resources
# discovers and cancels them by tag - like every other resource - even when the process that created
# them was killed before it could cancel them itself.


def test_clean_spot_fleet_requests_aws_cancels_matching_active_request():
    """clean_spot_fleet_requests_aws cancels an active request whose tags match, terminating its instances."""
    tags = {"TestId": "1111", "RunByUser": "test"}
    configs = [fleet_config("sfr-match", "active", tags)]
    with patch("boto3.client") as ec2_client_factory:
        ec2_client = ec2_client_factory.return_value
        ec2_client.get_paginator.return_value = paginator_returning(configs)
        with patch("time.sleep") as sleep_mock:
            clean_spot_fleet_requests_aws(tags, regions=["eu-west-3"])

    ec2_client_factory.assert_called_once_with("ec2", region_name="eu-west-3")
    ec2_client.cancel_spot_fleet_requests.assert_called_once_with(
        SpotFleetRequestIds=["sfr-match"], TerminateInstances=True
    )
    sleep_mock.assert_called_once_with(30)


def test_clean_spot_fleet_requests_aws_skips_non_matching_tags():
    """clean_spot_fleet_requests_aws leaves a request whose tags do not match alone."""
    tags = {"TestId": "1111"}
    configs = [fleet_config("sfr-other", "active", {"TestId": "2222"})]
    with patch("boto3.client") as ec2_client_factory:
        ec2_client = ec2_client_factory.return_value
        ec2_client.get_paginator.return_value = paginator_returning(configs)
        clean_spot_fleet_requests_aws(tags, regions=["eu-west-3"])

    ec2_client.cancel_spot_fleet_requests.assert_not_called()


def test_clean_spot_fleet_requests_aws_skips_terminal_state_requests():
    """clean_spot_fleet_requests_aws leaves a request already in a terminal state alone."""
    tags = {"TestId": "1111"}
    configs = [fleet_config("sfr-old", "cancelled", tags)]
    with patch("boto3.client") as ec2_client_factory:
        ec2_client = ec2_client_factory.return_value
        ec2_client.get_paginator.return_value = paginator_returning(configs)
        clean_spot_fleet_requests_aws(tags, regions=["eu-west-3"])

    ec2_client.cancel_spot_fleet_requests.assert_not_called()


def test_clean_spot_fleet_requests_aws_dry_run_does_not_cancel():
    """clean_spot_fleet_requests_aws with dry_run neither cancels a request nor sleeps."""
    tags = {"TestId": "1111"}
    configs = [fleet_config("sfr-match", "active", tags)]
    with patch("boto3.client") as ec2_client_factory:
        ec2_client = ec2_client_factory.return_value
        ec2_client.get_paginator.return_value = paginator_returning(configs)
        with patch("time.sleep") as sleep_mock:
            clean_spot_fleet_requests_aws(tags, regions=["eu-west-3"], dry_run=True)

    ec2_client.cancel_spot_fleet_requests.assert_not_called()
    sleep_mock.assert_not_called()


def test_clean_spot_fleet_requests_aws_matches_list_valued_node_type():
    """clean_spot_fleet_requests_aws matches a scalar fleet tag against a list of allowed NodeType values."""
    # clean_resources_according_post_behavior() passes NodeType as a list of allowed values (like
    # every other clean_*_aws tag filter), while a fleet request's own tag is a scalar string.
    tags = {"TestId": "1111", "NodeType": ["scylla-db", "loader"]}
    configs = [fleet_config("sfr-match", "active", {"TestId": "1111", "NodeType": "scylla-db"})]
    with patch("boto3.client") as ec2_client_factory:
        ec2_client = ec2_client_factory.return_value
        ec2_client.get_paginator.return_value = paginator_returning(configs)
        with patch("time.sleep"):
            clean_spot_fleet_requests_aws(tags, regions=["eu-west-3"])

    ec2_client.cancel_spot_fleet_requests.assert_called_once_with(
        SpotFleetRequestIds=["sfr-match"], TerminateInstances=True
    )


def test_clean_spot_fleet_requests_aws_error_in_one_region_does_not_abort_others():
    """clean_spot_fleet_requests_aws keeps cleaning the other regions when one region raises ClientError."""
    tags = {"TestId": "1111"}
    match = [fleet_config("sfr-ok", "active", tags)]
    with patch("boto3.client") as ec2_client_factory:
        broken = MagicMock()
        broken.get_paginator.side_effect = resources_cleanup.ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}}, "DescribeSpotFleetRequests"
        )
        ok = MagicMock()
        ok.get_paginator.return_value = paginator_returning(match)
        ec2_client_factory.side_effect = [broken, ok]
        with patch("time.sleep"):
            clean_spot_fleet_requests_aws(tags, regions=["me-south-1", "eu-west-1"])

    ok.cancel_spot_fleet_requests.assert_called_once_with(SpotFleetRequestIds=["sfr-ok"], TerminateInstances=True)


@pytest.mark.parametrize(
    "state, reported",
    [
        pytest.param("running", True, id="running"),
        pytest.param("terminated", False, id="terminated"),
        pytest.param("shutting-down", False, id="shutting-down"),
    ],
)
def test_clean_instances_aws_prices_only_instances_it_terminates(state, reported):
    """AWS keeps listing terminated instances; their exact cost was sent when SCT ended them."""
    instance = {"InstanceId": "i-1111", "State": {"Name": state}, "Tags": [{"Key": "Name", "Value": "db-1"}]}
    with (
        patch("boto3.client"),
        patch.object(resources_cleanup, "list_instances_aws", Mock(return_value={"eu-north-1": [instance]})),
        patch.object(resources_cleanup, "init_argus_client"),
        patch.object(resources_cleanup, "terminate_resource_in_argus"),
        patch.object(resources_cleanup, "report_costs_from_tags") as report_costs,
    ):
        clean_instances_aws({"TestId": 1111})
    assert report_costs.called is reported


@pytest.mark.parametrize(
    "state, reported",
    [
        pytest.param("RUNNING", True, id="running"),
        pytest.param("TERMINATED", False, id="terminated"),
        pytest.param("TERMINATING", False, id="terminating"),
    ],
)
def test_clean_instances_oci_prices_only_instances_it_terminates(oci_instance, state, reported):
    """OCI keeps listing terminated instances; their exact cost was sent when SCT ended them."""
    with (
        patch.object(resources_cleanup, "list_instances_oci", return_value=[oci_instance(lifecycle_state=state)]),
        patch.object(resources_cleanup, "OciService"),
        patch.object(resources_cleanup, "init_argus_client"),
        patch.object(resources_cleanup, "terminate_resource_in_argus"),
        patch.object(resources_cleanup, "report_costs_from_tags") as report_costs,
    ):
        clean_instances_oci({"TestId": "1111"})
    assert report_costs.called is reported


def test_unattached_cleanup_keeps_the_builder_security_group():
    """clean_unattached_security_groups deletes an unattached stale group but keeps the builder group."""
    # builders stop after 10 idle minutes, so the group is often attached to nothing
    groups = [
        {"GroupId": "sg-builder", "GroupName": "SCT-builder-sg", "Tags": [{"Key": "Name", "Value": "SCT-builder-sg"}]},
        {"GroupId": "sg-stale", "GroupName": "SCT-2-sg-leftover", "Tags": []},
    ]
    ec2 = MagicMock()
    ec2.get_paginator.side_effect = lambda name: MagicMock(
        paginate=MagicMock(
            return_value=[{"SecurityGroups": groups}]
            if name == "describe_security_groups"
            else [{"NetworkInterfaces": []}]
        )
    )
    with patch("utils.cloud_cleanup.aws.clean_aws.boto3.Session") as session:
        session.return_value.client.return_value = ec2
        clean_unattached_security_groups("eu-west-1")
    ec2.delete_security_group.assert_called_once_with(GroupId="sg-stale")
