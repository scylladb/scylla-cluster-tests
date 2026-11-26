from unittest.mock import MagicMock, PropertyMock, patch

from sdcm.sct_runner import BUILDER_KEEP_HOURS
from sdcm.utils.aws_builder import AwsBuilder
from sdcm.utils.gce_builder import GceBuilder


def make_aws_builder():
    """Return an AwsBuilder with a mocked region and no constructor call."""
    builder = AwsBuilder.__new__(AwsBuilder)
    builder.region = MagicMock(region_name="eu-west-1", availability_zones=["a"])
    builder.region.sct_subnet.return_value.subnet_id = "subnet"
    builder.number = 1
    return builder


def tags_by_key(tags):
    """Map a list of {Key, Value} tag dicts to a plain dict."""
    return {tag["Key"]: tag["Value"] for tag in tags}


@patch.object(AwsBuilder, "mixed_instances_policy", new_callable=PropertyMock, return_value={})
@patch("sdcm.utils.aws_builder.boto3.client")
def test_aws_new_asg_launches_with_keep(client, _):
    """Auto scaling group tags propagate at launch and carry keep and keep_action=terminate."""
    make_aws_builder().create_auto_scaling_group()

    tags = client.return_value.create_auto_scaling_group.call_args.kwargs["Tags"]
    assert all(tag["PropagateAtLaunch"] for tag in tags)
    assert (tags_by_key(tags)["keep"], tags_by_key(tags)["keep_action"]) == (BUILDER_KEEP_HOURS, "terminate")


@patch.object(GceBuilder, "_wait_for_extended_operation")
@patch("sdcm.utils.gce_builder.compute_v1.InstanceTemplatesClient")
def test_gce_template_launches_with_keep(client, _):
    """GCE instance template carries keep and keep_action=terminate in both labels and metadata."""
    builder = GceBuilder.__new__(GceBuilder)
    builder.region = MagicMock(project="sct-project-1", region_name="us-east1")
    builder.region.network.self_link = "network"
    builder.runner = MagicMock()
    builder.runner.image.self_link = "image"
    builder.credentials = None
    builder._create_launch_template()

    template = client.return_value.insert.call_args.kwargs["instance_template_resource"]
    labels = dict(template.properties.labels)
    assert (labels["keep"], labels["keep_action"]) == (BUILDER_KEEP_HOURS, "terminate")
    metadata = {item.key: item.value for item in template.properties.metadata.items}
    assert (metadata["keep"], metadata["keep_action"]) == (BUILDER_KEEP_HOURS, "terminate")
