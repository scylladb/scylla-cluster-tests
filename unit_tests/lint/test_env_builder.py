"""Unit tests for the Jenkins pipeline environment builder."""

import json

import pytest

from sdcm.utils.lint.jenkins_parser import PipelineConfig
from sdcm.utils.lint.env_builder import (
    count_regions,
    parse_extra_env_vars,
    build_env,
)


def _make_config(params=None, test_config=None, pipeline_function="longevityPipeline"):
    """Helper to create a PipelineConfig with defaults."""
    params = params or {}
    test_config = test_config or ["test-cases/test.yaml"]
    return PipelineConfig(
        file_path="test.jenkinsfile",
        pipeline_function=pipeline_function,
        test_config=test_config,
        params=params,
    )


# --- Tests for _count_regions ---


@pytest.mark.parametrize(
    "params,expected",
    [
        pytest.param({}, 1, id="no-region"),
        pytest.param({"region": "eu-west-1"}, 1, id="single-region"),
        pytest.param(
            {"region": '["eu-west-1", "eu-west-2"]'},
            2,
            id="two-regions-json",
        ),
        pytest.param(
            {"region": '["eu-west-1", "eu-west-2", "eu-north-1"]'},
            3,
            id="three-regions-json",
        ),
        pytest.param(
            {"gce_datacenter": '["us-east1", "us-west1"]'},
            2,
            id="gce-two-datacenters",
        ),
    ],
)
def test_count_regions(params, expected):
    config = _make_config(params=params)
    assert count_regions(config) == expected


# --- Tests for _parse_extra_env_vars ---


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param("SCT_USE_MGMT=false", {"SCT_USE_MGMT": "false"}, id="single"),
        pytest.param(
            "SCT_N_DB_NODES=9 SCT_USE_MGMT=false", {"SCT_N_DB_NODES": "9", "SCT_USE_MGMT": "false"}, id="multiple"
        ),
        pytest.param("", {}, id="empty"),
    ],
)
def test_parse_extra_env_vars(raw, expected):
    env = {}
    parse_extra_env_vars(raw, env)
    assert env == expected


# --- Tests for build_env ---


def test_build_env_basic_aws():
    config = _make_config(
        params={
            "backend": "aws",
            "region": "eu-west-1",
            "test_config": "test-cases/test.yaml",
        }
    )
    env = build_env(config)
    assert env["SCT_CLUSTER_BACKEND"] == "aws"
    assert env["SCT_REGION_NAME"] == "eu-west-1"
    assert json.loads(env["SCT_CONFIG_FILES"]) == ["test-cases/test.yaml"]


def test_build_env_gce_backend():
    config = _make_config(
        params={
            "backend": "gce",
            "gce_datacenter": "us-east1",
        }
    )
    env = build_env(config)
    assert env["SCT_CLUSTER_BACKEND"] == "gce"
    assert env["SCT_GCE_DATACENTER"] == "us-east1"


def test_build_env_azure_backend():
    config = _make_config(
        params={
            "backend": "azure",
            "azure_region_name": "eastus",
        }
    )
    env = build_env(config)
    assert env["SCT_CLUSTER_BACKEND"] == "azure"
    assert env["SCT_AZURE_REGION_NAME"] == "eastus"


def test_build_env_docker_backend():
    config = _make_config(
        params={
            "backend": "docker",
        }
    )
    env = build_env(config)
    assert env["SCT_CLUSTER_BACKEND"] == "docker"
    # Docker backend should not have image placeholders
    assert "SCT_AMI_ID_DB_SCYLLA" not in env
    assert "SCT_GCE_IMAGE_DB" not in env


def test_build_env_k8s_version_maps_to_both():
    config = _make_config(
        params={
            "backend": "k8s-eks",
            "k8s_version": "1.32",
        }
    )
    env = build_env(config)
    assert env["SCT_EKS_CLUSTER_VERSION"] == "1.32"
    assert env["SCT_GKE_CLUSTER_VERSION"] == "1.32"


def test_build_env_provision_type_mapping():
    config = _make_config(
        params={
            "backend": "aws",
            "provision_type": "spot",
        }
    )
    env = build_env(config)
    assert env["SCT_INSTANCE_PROVISION"] == "spot"


def test_build_env_linux_distro_mapping():
    config = _make_config(
        params={
            "backend": "gce",
            "linux_distro": "ubuntu-jammy",
        }
    )
    env = build_env(config)
    assert env["SCT_SCYLLA_LINUX_DISTRO"] == "ubuntu-jammy"


def test_build_env_test_email_title_mapping():
    config = _make_config(
        params={
            "backend": "aws",
            "test_email_title": "My Test",
        }
    )
    env = build_env(config)
    assert env["SCT_EMAIL_SUBJECT_POSTFIX"] == "My Test"


def test_build_env_scylla_ami_id_mapping():
    config = _make_config(
        params={
            "backend": "aws",
            "scylla_ami_id": "ami-abc123",
        }
    )
    env = build_env(config)
    assert env["SCT_AMI_ID_DB_SCYLLA"] == "ami-abc123"


def test_build_env_extra_environment_variables():
    config = _make_config(
        params={
            "backend": "docker",
            "extra_environment_variables": "SCT_USE_MGMT=false",
        }
    )
    env = build_env(config)
    assert env["SCT_USE_MGMT"] == "false"


def test_build_env_baremetal_placeholder_ips():
    config = _make_config(
        params={
            "backend": "baremetal",
        }
    )
    env = build_env(config)
    assert "SCT_DB_NODES_PUBLIC_IP" in env
    ips = json.loads(env["SCT_DB_NODES_PUBLIC_IP"])
    assert len(ips) >= 2


def test_build_env_test_config_json_encoded():
    config = _make_config(
        params={"backend": "aws"},
        test_config=["test-cases/a.yaml", "configurations/b.yaml"],
    )
    env = build_env(config)
    assert json.loads(env["SCT_CONFIG_FILES"]) == [
        "test-cases/a.yaml",
        "configurations/b.yaml",
    ]


def test_build_env_non_env_params_skipped():
    """Parameters like test_name, timeout, sub_tests should not become env vars."""
    config = _make_config(
        params={
            "backend": "aws",
            "test_name": "longevity_test.LongevityTest.test_custom_time",
            "sub_tests": '["test_read"]',
            "base_versions": "",
        }
    )
    env = build_env(config)
    assert "SCT_TEST_NAME" not in env
    assert "SCT_SUB_TESTS" not in env
    assert "SCT_BASE_VERSIONS" not in env


def test_build_env_post_behavior_params():
    config = _make_config(
        params={
            "backend": "aws",
            "post_behavior_db_nodes": "destroy",
            "post_behavior_loader_nodes": "destroy",
            "post_behavior_monitor_nodes": "keep",
        }
    )
    env = build_env(config)
    assert env["SCT_POST_BEHAVIOR_DB_NODES"] == "destroy"
    assert env["SCT_POST_BEHAVIOR_LOADER_NODES"] == "destroy"
    assert env["SCT_POST_BEHAVIOR_MONITOR_NODES"] == "keep"


def test_build_env_k8s_operator_params():
    config = _make_config(
        params={
            "backend": "k8s-eks",
            "k8s_scylla_operator_helm_repo": "https://charts.example.com/stable",
            "k8s_scylla_operator_chart_version": "latest",
            "k8s_enable_tls": "true",
        }
    )
    env = build_env(config)
    assert env["SCT_K8S_SCYLLA_OPERATOR_HELM_REPO"] == "https://charts.example.com/stable"
    assert env["SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION"] == "latest"
    assert env["SCT_K8S_ENABLE_TLS"] == "true"


def test_build_env_xcloud_params():
    config = _make_config(
        params={
            "backend": "xcloud",
            "xcloud_provider": "aws",
            "xcloud_env": "staging",
        }
    )
    env = build_env(config)
    assert env["SCT_XCLOUD_PROVIDER"] == "aws"
    assert env["SCT_XCLOUD_ENV"] == "staging"


def test_build_env_pytest_addopts():
    config = _make_config(
        params={
            "backend": "aws",
            "pytest_addopts": "-k test_something",
        }
    )
    env = build_env(config)
    assert env["PYTEST_ADDOPTS"] == "-k test_something"
