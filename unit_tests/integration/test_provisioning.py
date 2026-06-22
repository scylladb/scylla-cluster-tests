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

"""Regression tests for init_resources() provisioning paths across all backends.

These tests call ClusterTester.init_resources() with all cloud SDK calls mocked
at the provisioner/cluster-constructor boundary. They exist to catch breakage
where SCTConfiguration parameter types change (e.g. IntOrList now always returns
list[int]) and downstream code still treats the value as a scalar.

Every test exercises the full control-flow path inside get_cluster_<backend>()
so that bugs like

    monitor_info["n_nodes"] = self.params.get("n_monitor_nodes")  # list[int]
    if monitor_info["n_nodes"] > 0:                               # TypeError!

are caught immediately.

External services required: none — all cloud SDK calls are mocked.
"""

import logging
from contextlib import ExitStack
from unittest.mock import patch

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.test_config import TestConfig
from sdcm.tester import ClusterTester


# ---------------------------------------------------------------------------
# MinimalProvisioningTester
# ---------------------------------------------------------------------------


class MinimalProvisioningTester(ClusterTester):
    """Direct ClusterTester subclass with no event system, argus, or fixture machinery.

    Only initialises the instance state that init_resources() reads, nothing more.
    """

    __test__ = False

    def runTest(self):
        pass

    def prepare(self, params: SCTConfiguration) -> None:
        """Replicate only the setUp() lines that init_resources() depends on."""
        self.params = params
        self.log = logging.getLogger(self.__class__.__name__)
        self.credentials = []
        self.db_cluster = None
        self.cs_db_cluster = None
        self.loaders = None
        self.monitors = None
        self.emr_cluster = None
        self.kafka_cluster = None
        self.k8s_clusters = []
        self.test_config = TestConfig()


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


def blank_info() -> dict:
    """Return a fresh empty info dict that ClusterTester.init_resources() builds.

    Must be a plain function (not a fixture) because init_resources() mutates all
    three info dicts independently and each call must return a distinct instance.
    """
    return {
        "n_nodes": None,
        "type": None,
        "disk_size": None,
        "disk_type": None,
        "n_local_ssd": None,
        "device_mappings": None,
    }


@pytest.fixture
def mock_gce_backend():
    with ExitStack() as stack:
        stack.enter_context(patch("sdcm.tester.ScyllaGCECluster"))
        stack.enter_context(patch("sdcm.tester.LoaderSetGCE"))
        stack.enter_context(patch("sdcm.tester.MonitorSetGCE"))
        stack.enter_context(patch("sdcm.tester.get_gce_compute_instances_client"))
        stack.enter_context(patch("sdcm.tester.provisioner_factory"))
        stack.enter_context(patch("sdcm.tester.GceAZResolver"))
        stack.enter_context(patch("sdcm.tester.UserRemoteCredentials"))
        stack.enter_context(patch("sdcm.tester.TestConfig"))
        stack.enter_context(patch("sdcm.tester.start_posting_grafana_annotations"))
        yield


@pytest.fixture
def mock_azure_backend():
    with ExitStack() as stack:
        stack.enter_context(patch("sdcm.tester.ScyllaAzureCluster"))
        stack.enter_context(patch("sdcm.tester.LoaderSetAzure"))
        stack.enter_context(patch("sdcm.tester.MonitorSetAzure"))
        stack.enter_context(patch("sdcm.tester.provisioner_factory"))
        stack.enter_context(patch("sdcm.tester.UserRemoteCredentials"))
        stack.enter_context(patch("sdcm.tester.TestConfig"))
        stack.enter_context(patch("sdcm.tester.start_posting_grafana_annotations"))
        yield


@pytest.fixture
def mock_oci_backend():
    with ExitStack() as stack:
        stack.enter_context(patch("sdcm.tester.ScyllaOciCluster"))
        stack.enter_context(patch("sdcm.tester.LoaderSetOci"))
        stack.enter_context(patch("sdcm.tester.MonitorSetOci"))
        stack.enter_context(patch("sdcm.tester.provisioner_factory"))
        stack.enter_context(patch("sdcm.tester.UserRemoteCredentials"))
        stack.enter_context(patch("sdcm.tester.TestConfig"))
        stack.enter_context(patch("sdcm.tester.start_posting_grafana_annotations"))
        yield


@pytest.fixture
def mock_aws_backend():
    with ExitStack() as stack:
        stack.enter_context(patch("sdcm.tester.ScyllaAWSCluster"))
        stack.enter_context(patch("sdcm.tester.LoaderSetAWS"))
        stack.enter_context(patch("sdcm.tester.MonitorSetAWS"))
        stack.enter_context(patch("sdcm.tester.get_ec2_services"))
        stack.enter_context(patch("sdcm.tester.get_common_params"))
        stack.enter_context(patch("sdcm.tester.AZResolver"))
        stack.enter_context(patch("sdcm.tester.is_az_fallback_enabled"))
        stack.enter_context(patch("sdcm.tester.SCTCapacityReservation"))
        stack.enter_context(patch("sdcm.tester.run_pre_flight_capacity_probe"))
        stack.enter_context(patch("sdcm.tester.wait_ami_available"))
        stack.enter_context(patch("sdcm.tester.UserRemoteCredentials"))
        stack.enter_context(patch("sdcm.tester.TestConfig"))
        stack.enter_context(patch("sdcm.tester.start_posting_grafana_annotations"))
        # ec2_ami_get_root_device_name is imported into tester.py namespace AND called
        # inside aws_utils itself — patch both sites.
        stack.enter_context(patch("sdcm.tester.ec2_ami_get_root_device_name"))
        stack.enter_context(patch("sdcm.utils.aws_utils.ec2_ami_get_root_device_name"))
        # KeyStore is imported into aws_utils; patch there to stop S3 calls.
        stack.enter_context(patch("sdcm.utils.aws_utils.KeyStore"))
        yield


@pytest.fixture
def mock_xcloud_backend(mock_aws_backend, mock_gce_backend):
    """Composes AWS + GCE fixtures (covers both xcloud providers) and adds
    xcloud-specific mocks. KeyStore for aws_utils is already covered by mock_aws_backend."""
    with ExitStack() as stack:
        stack.enter_context(patch("sdcm.tester.ScyllaCloudCluster"))
        stack.enter_context(patch("sdcm.tester.ScyllaCloudAPIClient"))
        # cloud_env_credentials is a cached_property on SCTConfiguration that calls
        # KeyStore() directly from sct_config — patch at the usage site.
        stack.enter_context(patch("sdcm.sct_config.KeyStore"))
        yield


# ---------------------------------------------------------------------------
# Parametrized test
# ---------------------------------------------------------------------------

COMMON = {
    "SCT_N_DB_NODES": "3",
    "SCT_N_LOADERS": "1",
    "SCT_N_MONITOR_NODES": "1",
    "SCT_USE_MGMT": "false",
    "SCT_CONFIG_FILES": "unit_tests/test_configs/minimal_test_case.yaml",
}

GCE_BASE = COMMON | {
    "SCT_CLUSTER_BACKEND": "gce",
    "SCT_GCE_DATACENTER": "us-east1",
    "SCT_GCE_INSTANCE_TYPE_DB": "n2-highmem-2",
}

AZURE_BASE = COMMON | {
    "SCT_CLUSTER_BACKEND": "azure",
    "SCT_AZURE_REGION_NAME": "eastus",
    "SCT_AZURE_INSTANCE_TYPE_DB": "Standard_L8s_v3",
    "SCT_AZURE_IMAGE_DB": "fake-azure-image",
}

OCI_BASE = COMMON | {
    "SCT_CLUSTER_BACKEND": "oci",
    "SCT_OCI_REGION_NAME": "us-ashburn-1",
    "SCT_OCI_INSTANCE_TYPE_DB": "VM.Standard3.Flex",
    "SCT_OCI_IMAGE_DB": "ocid1.image.oc1.fake",
}

AWS_BASE = COMMON | {
    "SCT_CLUSTER_BACKEND": "aws",
    "SCT_REGION_NAME": "eu-west-1",
    "SCT_INSTANCE_TYPE_DB": "i4i.large",
    "SCT_AMI_ID_DB_SCYLLA": "ami-fake00001",
    "SCT_AMI_ID_LOADER": "ami-fake00002",
    "SCT_AMI_ID_MONITOR": "ami-fake00003",
}

XCLOUD_AWS_BASE = COMMON | {
    "SCT_CLUSTER_BACKEND": "xcloud",
    "SCT_XCLOUD_PROVIDER": "aws",
    "SCT_REGION_NAME": "eu-west-1",
    "SCT_INSTANCE_TYPE_DB": "i4i.large",
    "SCT_AMI_ID_LOADER": "ami-fake00002",
    "SCT_AMI_ID_MONITOR": "ami-fake00003",
    "SCT_XCLOUD_ENV": "fake-env",
}

XCLOUD_GCE_BASE = COMMON | {
    "SCT_CLUSTER_BACKEND": "xcloud",
    "SCT_XCLOUD_PROVIDER": "gce",
    "SCT_GCE_DATACENTER": "us-east1",
    "SCT_GCE_INSTANCE_TYPE_DB": "n2-highmem-2",
    "SCT_XCLOUD_ENV": "fake-env",
}


@pytest.mark.integration
@pytest.mark.parametrize(
    "env,backend_mock",
    [
        pytest.param(GCE_BASE, "mock_gce_backend", id="gce-single-dc"),
        pytest.param(
            GCE_BASE
            | {
                "SCT_GCE_DATACENTER": "us-east1 us-west1",
                "SCT_N_DB_NODES": "3 3",
                "SCT_N_LOADERS": "1 1",
                "SCT_N_MONITOR_NODES": "1 0",
            },
            "mock_gce_backend",
            id="gce-multi-dc",
        ),
        pytest.param(AZURE_BASE, "mock_azure_backend", id="azure-single-dc"),
        pytest.param(
            AZURE_BASE
            | {
                "SCT_AZURE_REGION_NAME": "eastus westus",
                "SCT_N_DB_NODES": "3 3",
                "SCT_N_LOADERS": "1 1",
                "SCT_N_MONITOR_NODES": "1 0",
            },
            "mock_azure_backend",
            id="azure-multi-dc",
        ),
        pytest.param(OCI_BASE, "mock_oci_backend", id="oci"),
        pytest.param(AWS_BASE, "mock_aws_backend", id="aws-single-region"),
        pytest.param(
            AWS_BASE
            | {
                "SCT_REGION_NAME": '["eu-west-1", "us-east-1"]',
                "SCT_N_DB_NODES": "3 3",
                "SCT_N_LOADERS": "1 1",
                "SCT_N_MONITOR_NODES": "1 0",
                "SCT_AMI_ID_DB_SCYLLA": "ami-fake00001 ami-fake00011",
                "SCT_AMI_ID_LOADER": "ami-fake00002 ami-fake00012",
            },
            "mock_aws_backend",
            id="aws-multi-region",
        ),
        pytest.param(XCLOUD_AWS_BASE, "mock_xcloud_backend", id="xcloud-aws"),
        pytest.param(XCLOUD_GCE_BASE, "mock_xcloud_backend", id="xcloud-gce"),
        # Zero-monitor variants: exercise the NoMonitorSet else-branch and confirm
        # sum(monitor_info["n_nodes"]) > 0 correctly returns False for [0].
        pytest.param(GCE_BASE | {"SCT_N_MONITOR_NODES": "0"}, "mock_gce_backend", id="gce-no-monitors"),
        pytest.param(
            XCLOUD_AWS_BASE | {"SCT_N_MONITOR_NODES": "0"}, "mock_xcloud_backend", id="xcloud-aws-no-monitors"
        ),
        pytest.param(
            XCLOUD_GCE_BASE | {"SCT_N_MONITOR_NODES": "0"}, "mock_xcloud_backend", id="xcloud-gce-no-monitors"
        ),
    ],
)
def test_init_resources(request, monkeypatch, env, backend_mock):
    """init_resources() must complete without TypeError or AttributeError for every backend.

    Regression for: IntOrList/StringOrList normalization returning list[int]/list[str]
    where tester.py provisioning paths expected scalars (tester.py:1699, 1769, 1837,
    2066, 2597, 2666, 2702).
    """
    request.getfixturevalue(backend_mock)

    for key, val in env.items():
        monkeypatch.setenv(key, val)

    tester = MinimalProvisioningTester()
    tester.prepare(SCTConfiguration())
    ClusterTester.init_resources(tester, loader_info=blank_info(), db_info=blank_info(), monitor_info=blank_info())
