"""Region calls of list_instances_aws must end inside the scan timeout."""

from unittest.mock import patch

from sdcm.utils.common import AWS_LIST_INSTANCES_TIMEOUT, list_instances_aws


def test_list_instances_aws_region_call_ends_before_the_scan_timeout():
    """A region endpoint that does not answer must fail before the scan timeout.

    A region call that runs longer than the scan keeps its worker thread alive,
    and the process waits for that thread at exit.
    """
    with (
        patch("sdcm.utils.common.boto3.client") as boto3_client,
        patch("sdcm.utils.common.random.random", return_value=0),
    ):
        boto3_client.return_value.describe_instances.return_value = {"Reservations": []}
        list_instances_aws(region_name="eu-west-1")

    config = boto3_client.call_args.kwargs["config"]
    worst_case = (config.connect_timeout + config.read_timeout) * config.retries["total_max_attempts"]
    assert worst_case < AWS_LIST_INSTANCES_TIMEOUT
