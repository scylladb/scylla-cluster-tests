import re
from collections import defaultdict
from unittest.mock import MagicMock, PropertyMock, patch

from sdcm.sct_runner import BUILDER_KEEP_HOURS
from sdcm.utils.aws_builder import ASG_CONFIG_FORMAT, AwsBuilder
from sdcm.utils.gce_builder import GceBuilder
from sdcm.utils.oci_builder import OCI_JENKINS_CONFIG_TEMPLATE


def _aws_builder():
    builder = AwsBuilder.__new__(AwsBuilder)
    builder.region = MagicMock(region_name="eu-west-1", availability_zones=["a"])
    builder.region.sct_subnet.return_value.subnet_id = "subnet"
    builder.number = 1
    return builder


def _tags(tags):
    return {tag["Key"]: tag["Value"] for tag in tags}


def test_aws_builder_serves_one_build():
    assert '"1", // maxTotalUses' in ASG_CONFIG_FORMAT


@patch.object(AwsBuilder, "mixed_instances_policy", new_callable=PropertyMock, return_value={})
@patch("sdcm.utils.aws_builder.boto3.client")
def test_aws_new_asg_launches_with_keep(client, _):
    _aws_builder().create_auto_scaling_group()

    tags = client.return_value.create_auto_scaling_group.call_args.kwargs["Tags"]
    assert all(tag["PropagateAtLaunch"] for tag in tags)
    assert (_tags(tags)["keep"], _tags(tags)["keep_action"]) == (BUILDER_KEEP_HOURS, "terminate")


@patch.object(GceBuilder, "_wait_for_extended_operation")
@patch("sdcm.utils.gce_builder.compute_v1.InstanceTemplatesClient")
def test_gce_template_launches_with_keep(client, _):
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


def test_oci_template_tags_and_single_use():
    script = OCI_JENKINS_CONFIG_TEMPLATE.substitute(defaultdict(lambda: "x", keep_hours=BUILDER_KEEP_HOURS))

    assert 'keep: "168",' in script
    assert 'CreatedBy: "SCT",' in script
    assert 'tagClass.newInstance("sct", k, v)' in script
    assert re.search(r"^\s+1\s+// maxTotalUses", script, re.MULTILINE)

    # Plugin 1.0.21 has a single 40-argument constructor; both lists must match it
    signature = script.split("getDeclaredConstructor(")[1].split("\n)")[0]
    arguments = script.split("ctor.newInstance(")[1].split("\n)")[0]
    assert len(signature.strip().splitlines()) == len(arguments.strip().splitlines()) == 40
