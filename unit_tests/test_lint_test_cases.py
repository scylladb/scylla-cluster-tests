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

import pytest
from pathlib import Path
from utils.lint_test_cases import (
    TestCaseAnalyzer,
    Backend,
    TestCaseAnalysis,
)


# Fixtures
@pytest.fixture
def analyzer(tmp_path):
    """Create a TestCaseAnalyzer with a temporary test directory."""
    test_cases_dir = tmp_path / "test-cases"
    test_cases_dir.mkdir()
    return TestCaseAnalyzer(test_cases_dir=test_cases_dir)


# TestCaseAnalyzer initialization tests
def test_analyzer_initialization(tmp_path):
    """Test analyzer initialization with custom directory."""
    test_cases_dir = tmp_path / "test-cases"
    test_cases_dir.mkdir()
    analyzer = TestCaseAnalyzer(test_cases_dir=test_cases_dir)
    assert analyzer.test_cases_dir == test_cases_dir


def test_analyzer_default_initialization():
    """Test analyzer initialization with default directory."""
    analyzer = TestCaseAnalyzer()
    assert analyzer.test_cases_dir.name == "test-cases"


# Default backend detection tests
def test_analyze_file_no_indicators_defaults_to_aws(analyzer, tmp_path):
    """Test that files with no specific backend default to AWS."""
    test_file = tmp_path / "test-cases" / "generic-test.yaml"
    test_file.write_text("# Generic test case\nn_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.AWS in analysis.backends
    assert len(analysis.backends) == 1
    assert "default backend" in analysis.reason


# Backend detection from filename tests
def test_analyze_azure_filename(analyzer, tmp_path):
    """Test detection of Azure backend from filename."""
    test_file = tmp_path / "test-cases" / "test-azure-config.yaml"
    test_file.write_text("n_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.AZURE in analysis.backends
    assert "filename pattern: azure" in analysis.reason


def test_analyze_gce_filename(analyzer, tmp_path):
    """Test detection of GCE backend from filename."""
    test_file = tmp_path / "test-cases" / "test-gce-instance.yaml"
    test_file.write_text("n_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.GCE in analysis.backends
    assert "filename pattern: gce" in analysis.reason


def test_analyze_docker_filename(analyzer, tmp_path):
    """Test detection of Docker backend from filename."""
    test_file = tmp_path / "test-cases" / "test-docker-setup.yaml"
    test_file.write_text("n_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.DOCKER in analysis.backends
    assert "filename pattern: docker" in analysis.reason


def test_analyze_baremetal_filename(analyzer, tmp_path):
    """Test detection of Baremetal backend from filename."""
    test_file = tmp_path / "test-cases" / "test-baremetal-cluster.yaml"
    test_file.write_text("n_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.BAREMETAL in analysis.backends
    assert "filename pattern: baremetal" in analysis.reason


def test_analyze_k8s_local_filename(analyzer, tmp_path):
    """Test detection of K8S_LOCAL_KIND backend from filename."""
    test_file = tmp_path / "test-cases" / "scylla-operator-test.yaml"
    test_file.write_text("n_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.K8S_LOCAL_KIND in analysis.backends or Backend.K8S_EKS in analysis.backends or Backend.K8S_GKE in analysis.backends


# Backend detection from content tests
def test_analyze_aws_content(analyzer, tmp_path):
    """Test detection of AWS backend from content."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text("region_name: us-east-1\ninstance_type_db: i3.large\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.AWS in analysis.backends
    assert "content has aws config" in analysis.reason


def test_analyze_azure_content(analyzer, tmp_path):
    """Test detection of Azure backend from content."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text("azure_region_name: eastus\nazure_instance_type_db: Standard_D4s_v3\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.AZURE in analysis.backends
    assert "content has azure config" in analysis.reason


def test_analyze_gce_content(analyzer, tmp_path):
    """Test detection of GCE backend from content."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text("gce_datacenter: us-east1\ngce_instance_type_db: n1-standard-4\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.GCE in analysis.backends
    assert "content has gce config" in analysis.reason


def test_analyze_docker_content(analyzer, tmp_path):
    """Test detection of Docker backend from content."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text("docker_image: scylladb/scylla:latest\n")

    analysis = analyzer.analyze_file(test_file)

    assert Backend.DOCKER in analysis.backends
    assert "content has docker config" in analysis.reason


def test_analyze_baremetal_content(analyzer, tmp_path):
    """Test detection of Baremetal backend from content."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text('db_nodes_public_ip: ["192.168.1.1", "192.168.1.2"]\n')

    analysis = analyzer.analyze_file(test_file)

    assert Backend.BAREMETAL in analysis.backends
    assert "content has baremetal config" in analysis.reason


# Explicit backend specification tests
def test_analyze_explicit_aws_backend(analyzer, tmp_path):
    """Test explicit AWS backend specification."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text("cluster_backend: aws\nn_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert analysis.explicit_backend == Backend.AWS
    assert Backend.AWS in analysis.backends
    assert "explicit backend: aws" in analysis.reason


def test_analyze_explicit_docker_backend(analyzer, tmp_path):
    """Test explicit Docker backend specification."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text("cluster_backend: docker\nn_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    assert analysis.explicit_backend == Backend.DOCKER
    assert Backend.DOCKER in analysis.backends
    assert "explicit backend: docker" in analysis.reason


def test_analyze_invalid_explicit_backend(analyzer, tmp_path):
    """Test that invalid explicit backend is ignored."""
    test_file = tmp_path / "test-cases" / "test.yaml"
    test_file.write_text("cluster_backend: invalid-backend\nn_db_nodes: 3\n")

    analysis = analyzer.analyze_file(test_file)

    # Should fall back to default AWS
    assert Backend.AWS in analysis.backends
    assert analysis.explicit_backend is None

# DC count detection tests


def test_get_dc_count_from_n_db_nodes(analyzer):
    """Test DC count extraction from n_db_nodes."""
    content = {"n_db_nodes": "3 3 3"}
    assert analyzer._get_dc_count(content) == 3


def test_get_dc_count_from_gce_datacenter_string(analyzer):
    """Test DC count from gce_datacenter string."""
    content = {"gce_datacenter": "us-east1 us-west1"}
    assert analyzer._get_dc_count(content) == 2


def test_get_dc_count_from_gce_datacenter_list(analyzer):
    """Test DC count from gce_datacenter list."""
    content = {"gce_datacenter": ["us-east1", "us-west1", "eu-north1"]}
    assert analyzer._get_dc_count(content) == 3


def test_get_dc_count_returns_none_for_single_dc(analyzer):
    """Test that single DC returns None."""
    content = {"n_db_nodes": "3"}
    assert analyzer._get_dc_count(content) is None


# Backend configuration tests
def test_get_aws_single_region_config(analyzer):
    """Test AWS single region configuration generation."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.AWS},
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.AWS
    assert configs[0].description == "AWS single region"
    assert "SCT_AMI_ID_DB_SCYLLA" in configs[0].env_vars
    assert "SCT_SCYLLA_REPO" in configs[0].env_vars


def test_get_aws_multi_dc_5_regions_config(analyzer):
    """Test AWS multi-DC with 5 regions configuration."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.AWS},
        dc_count=5
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.AWS
    assert configs[0].description == "AWS multi-DC with 5 regions"
    assert configs[0].multi_region is True
    assert configs[0].region_count == 5
    assert " " in configs[0].env_vars["SCT_AMI_ID_DB_SCYLLA"]  # Multiple AMIs
    assert " " in configs[0].env_vars["SCT_REGION_NAME"]  # Multiple regions


def test_get_azure_config(analyzer):
    """Test Azure configuration generation."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.AZURE}
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.AZURE
    assert configs[0].description == "Azure"
    assert "SCT_AZURE_IMAGE_DB" in configs[0].env_vars
    assert "SCT_AZURE_REGION_NAME" in configs[0].env_vars


def test_get_gce_single_region_config(analyzer):
    """Test GCE single region configuration."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.GCE},
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.GCE
    assert configs[0].description == "GCE single region"
    assert "SCT_GCE_IMAGE_DB" in configs[0].env_vars


def test_get_gce_multi_dc_2_regions_config(analyzer):
    """Test GCE multi-DC with 2 regions configuration."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.GCE},
        dc_count=2
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.GCE
    assert configs[0].description == "GCE multi-DC with 2 regions"
    assert configs[0].multi_region is True
    assert configs[0].region_count == 2
    assert "SCT_GCE_DATACENTER" in configs[0].env_vars
    assert " " in configs[0].env_vars["SCT_GCE_DATACENTER"]


def test_get_gce_multi_dc_3_regions_config(analyzer):
    """Test GCE multi-DC with 3 regions configuration."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.GCE},
        dc_count=3
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.GCE
    assert configs[0].description == "GCE multi-DC with 3 regions"
    assert configs[0].region_count == 3


def test_get_docker_config(analyzer):
    """Test Docker configuration generation."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.DOCKER}
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.DOCKER
    assert configs[0].description == "Docker"
    assert "SCT_DOCKER_IMAGE" in configs[0].env_vars
    assert "SCT_USE_MGMT" in configs[0].env_vars


def test_get_baremetal_config(analyzer):
    """Test Baremetal configuration generation."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.BAREMETAL}
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.BAREMETAL
    assert configs[0].description == "Baremetal"
    assert "SCT_DB_NODES_PUBLIC_IP" in configs[0].env_vars


def test_get_k8s_eks_config(analyzer):
    """Test K8S_EKS configuration generation."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.K8S_EKS}
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 1
    assert configs[0].backend == Backend.K8S_EKS
    assert "Kubernetes" in configs[0].description
    assert "SCT_AMI_ID_DB_SCYLLA" in configs[0].env_vars


def test_get_multiple_backend_configs(analyzer):
    """Test configuration generation for multiple backends."""
    analysis = TestCaseAnalysis(
        file_path=Path("test.yaml"),
        backends={Backend.AWS, Backend.DOCKER}
    )
    configs = analyzer.get_backend_configs(analysis)

    assert len(configs) == 2
    backend_types = {config.backend for config in configs}
    assert Backend.AWS in backend_types
    assert Backend.DOCKER in backend_types


# YAML loading tests
def test_load_valid_yaml(analyzer, tmp_path):
    """Test loading a valid YAML file."""
    test_file = tmp_path / "test.yaml"
    test_file.write_text("key: value\nlist:\n  - item1\n  - item2\n", encoding='utf-8')

    content = analyzer._load_yaml(test_file)

    assert content is not None
    assert content["key"] == "value"
    assert len(content["list"]) == 2


def test_load_yaml_with_utf8_encoding(analyzer, tmp_path):
    """Test loading YAML file with UTF-8 characters."""
    test_file = tmp_path / "test.yaml"
    test_file.write_text("comment: '# Unicode: café, 日本語'\nkey: value\n", encoding='utf-8')

    content = analyzer._load_yaml(test_file)

    assert content is not None
    assert "café" in content["comment"] or "日本語" in content["comment"]


def test_load_invalid_yaml(analyzer, tmp_path):
    """Test loading an invalid YAML file returns None."""
    test_file = tmp_path / "invalid.yaml"
    test_file.write_text("invalid: yaml: content: [unclosed\n")

    content = analyzer._load_yaml(test_file)

    assert content is None


def test_load_nonexistent_file(analyzer, tmp_path):
    """Test loading a non-existent file returns None."""
    test_file = tmp_path / "nonexistent.yaml"

    content = analyzer._load_yaml(test_file)

    assert content is None


# Edge cases and error handling tests
def test_analyze_empty_yaml_file(analyzer, tmp_path):
    """Test analyzing an empty YAML file."""
    test_file = tmp_path / "test-cases" / "empty.yaml"
    test_file.write_text("")

    analysis = analyzer.analyze_file(test_file)

    # Should default to AWS with empty content
    assert Backend.AWS in analysis.backends


def test_analyze_yaml_with_only_comments(analyzer, tmp_path):
    """Test analyzing YAML with only comments."""
    test_file = tmp_path / "test-cases" / "comments.yaml"
    test_file.write_text("# This is a comment\n# Another comment\n")

    analysis = analyzer.analyze_file(test_file)

    # Should default to AWS
    assert Backend.AWS in analysis.backends


def test_analyze_file_with_conflicting_indicators(analyzer, tmp_path):
    """Test file with both AWS and GCE indicators."""
    test_file = tmp_path / "test-cases" / "test-gce.yaml"
    test_file.write_text("region_name: us-east-1\ngce_datacenter: us-east1\n")

    analysis = analyzer.analyze_file(test_file)

    # Filename pattern should take precedence
    assert Backend.GCE in analysis.backends


def test_find_all_test_cases(analyzer, tmp_path):
    """Test finding all YAML test case files."""
    test_cases_dir = tmp_path / "test-cases"

    # Create some test files
    (test_cases_dir / "test1.yaml").write_text("test: 1\n")
    (test_cases_dir / "test2.yaml").write_text("test: 2\n")
    (test_cases_dir / "subdir").mkdir()
    (test_cases_dir / "subdir" / "test3.yaml").write_text("test: 3\n")
    (test_cases_dir / "notayaml.txt").write_text("not a yaml\n")

    files = analyzer.find_all_test_cases()

    # Should find 3 YAML files
    assert len(files) == 3
    assert all(f.suffix == ".yaml" for f in files)


# Multi-DC integration tests
def test_analyze_multi_dc_aws_file(analyzer, tmp_path):
    """Test complete analysis of multi-DC AWS file."""
    test_file = tmp_path / "test-cases" / "multi-dc-5dcs-test.yaml"
    test_file.write_text(
        "n_db_nodes: '3 3 3 3 3'\nregion_name: 'us-east-1 us-west-2 eu-west-1 eu-central-1 ap-southeast-1'\n")

    analysis = analyzer.analyze_file(test_file)

    assert analysis.dc_count == 5
    assert Backend.AWS in analysis.backends

    configs = analyzer.get_backend_configs(analysis)
    assert len(configs) == 1
    assert configs[0].description == "AWS multi-DC with 5 regions"
    assert configs[0].region_count == 5


def test_analyze_multi_dc_gce_file(analyzer, tmp_path):
    """Test complete analysis of multi-DC GCE file."""
    test_file = tmp_path / "test-cases" / "multi-dc-gce-3dcs.yaml"
    test_file.write_text("gce_datacenter: 'us-east1 us-west1 eu-north1'\nn_db_nodes: '3 3 3'\n")

    analysis = analyzer.analyze_file(test_file)

    assert analysis.dc_count == 3
    assert Backend.GCE in analysis.backends

    configs = analyzer.get_backend_configs(analysis)
    assert len(configs) == 1
    assert configs[0].description == "GCE multi-DC with 3 regions"
    assert configs[0].region_count == 3
