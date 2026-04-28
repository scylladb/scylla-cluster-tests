"""Unit tests for the Jenkins pipeline file parser."""

from pathlib import Path

import pytest

from sdcm.utils.lint.jenkins_parser import (
    PipelineConfig,
    _parse_test_config,
    discover_pipeline_files,
    parse_jenkinsfile,
)


# --- Fixtures ---


@pytest.fixture()
def create_jenkinsfile(tmp_path):
    """Helper to create a temporary jenkinsfile with given content."""

    def _create(content: str, name: str = "test.jenkinsfile") -> Path:
        path = tmp_path / name
        path.write_text(content)
        return path

    return _create


# --- Tests for _parse_test_config ---


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param(
            "test-cases/longevity/longevity-50gb-36h.yaml",
            ["test-cases/longevity/longevity-50gb-36h.yaml"],
            id="single-file",
        ),
        pytest.param(
            '["test-cases/a.yaml", "configurations/b.yaml"]',
            ["test-cases/a.yaml", "configurations/b.yaml"],
            id="json-array-double-quotes",
        ),
        pytest.param(
            "['test-cases/a.yaml', 'configs/b.yaml']",
            ["test-cases/a.yaml", "configs/b.yaml"],
            id="python-list-single-quotes",
        ),
        pytest.param(
            "",
            [],
            id="empty-string",
        ),
        pytest.param(
            "  test-cases/foo.yaml  ",
            ["test-cases/foo.yaml"],
            id="whitespace-trimmed",
        ),
    ],
)
def test_parse_test_config(raw, expected):
    assert _parse_test_config(raw) == expected


# --- Tests for parse_jenkinsfile ---


def test_longevity_pipeline_single_config(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "longevityPipeline(\n"
        "    backend: 'aws',\n"
        "    region: 'eu-west-1',\n"
        "    test_name: 'longevity_test.LongevityTest.test_custom_time',\n"
        "    test_config: 'test-cases/longevity/longevity-50gb-36h.yaml'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.pipeline_function == "longevityPipeline"
    assert result.backend == "aws"
    assert result.region == "eu-west-1"
    assert result.test_config == ["test-cases/longevity/longevity-50gb-36h.yaml"]


def test_longevity_pipeline_triple_quoted_array(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "longevityPipeline(\n"
        "    backend: 'aws',\n"
        "    region: 'eu-west-1',\n"
        "    test_name: 'longevity_test.LongevityTest.test_custom_time',\n"
        "    test_config: '''[\"test-cases/a.yaml\", \"configurations/b.yaml\"]'''\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.test_config == ["test-cases/a.yaml", "configurations/b.yaml"]


def test_multi_region_pipeline(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "longevityPipeline(\n"
        "    backend: 'aws',\n"
        "    region: '''[\"eu-west-1\", \"eu-west-2\", \"eu-north-1\"]''',\n"
        "    test_name: 'longevity_test.LongevityTest.test_custom_time',\n"
        "    test_config: '''[\"test-cases/multi-dc.yaml\"]'''\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.region == '["eu-west-1", "eu-west-2", "eu-north-1"]'


def test_manager_pipeline(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "managerPipeline(\n"
        "    backend: 'aws',\n"
        "    region: 'us-east-1',\n"
        "    test_name: 'mgmt_cli_test.ManagerSanityTests.test_manager_sanity',\n"
        "    test_config: 'test-cases/manager/manager-sanity.yaml'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.pipeline_function == "managerPipeline"
    assert result.backend == "aws"


def test_artifacts_pipeline_with_timeout(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "artifactsPipeline(\n"
        "    test_config: 'test-cases/artifacts/ami.yaml',\n"
        "    backend: 'aws',\n"
        "    provision_type: 'spot',\n"
        "    timeout: [time: 45, unit: 'MINUTES'],\n"
        "    post_behavior_db_nodes: 'destroy'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.pipeline_function == "artifactsPipeline"
    assert result.params["provision_type"] == "spot"
    assert result.params["post_behavior_db_nodes"] == "destroy"


def test_k8s_eks_pipeline(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "rollingOperatorUpgradePipeline(\n"
        "    backend: 'k8s-eks',\n"
        "    region: 'eu-north-1',\n"
        "    test_name: 'upgrade_test.UpgradeTest.test_kubernetes_operator_upgrade',\n"
        "    test_config: 'test-cases/scylla-operator/kubernetes-operator-upgrade.yaml',\n"
        "    k8s_version: '1.32',\n"
        "    k8s_scylla_operator_helm_repo: 'https://storage.googleapis.com/scylla-operator-charts/stable',\n"
        "    k8s_scylla_operator_chart_version: 'latest'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.pipeline_function == "rollingOperatorUpgradePipeline"
    assert result.backend == "k8s-eks"
    assert result.params["k8s_version"] == "1.32"


def test_perf_pipeline_with_sub_tests(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "perfSearchBestConfigParallelPipeline(\n"
        '    backend: "aws",\n'
        '    region: "us-east-1",\n'
        '    test_name: "performance_test.PerfTest",\n'
        '    test_config: "test-cases/performance/perf.yaml",\n'
        '    sub_tests: ["test_read", "test_write"],\n'
        '    timeout: [time: 3500, unit: "MINUTES"]\n'
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.pipeline_function == "perfSearchBestConfigParallelPipeline"


def test_pipeline_with_extra_env_vars(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "artifactsPipeline(\n"
        "    test_config: 'test-cases/artifacts/docker.yaml',\n"
        "    backend: 'docker',\n"
        "    timeout: [time: 30, unit: 'MINUTES'],\n"
        "    post_behavior_db_nodes: 'destroy',\n"
        "    extra_environment_variables: 'SCT_USE_MGMT=false'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.params["extra_environment_variables"] == "SCT_USE_MGMT=false"


def test_rolling_upgrade_gce(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "rollingUpgradePipeline(\n"
        "    backend: 'gce',\n"
        "    base_versions: '',\n"
        "    linux_distro: 'ubuntu-jammy',\n"
        "    gce_image_db: 'https://example.com/image',\n"
        "    test_name: 'upgrade_test.UpgradeTest.test_generic_cluster_upgrade',\n"
        "    test_config: 'test-cases/upgrades/generic-rolling-upgrade.yaml'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.pipeline_function == "rollingUpgradePipeline"
    assert result.backend == "gce"
    assert result.params["linux_distro"] == "ubuntu-jammy"
    assert result.params["gce_image_db"] == "https://example.com/image"


def test_byo_pipeline_returns_none(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\ndef lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\nbyoLongevityPipeline( )\n"
    )
    assert parse_jenkinsfile(path) is None


def test_create_test_job_returns_none(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\ndef lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\ncreateTestJobPipeline()\n"
    )
    assert parse_jenkinsfile(path) is None


def test_raw_pipeline_block_returns_none(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "pipeline {\n"
        "    agent { label 'aws-builder' }\n"
        "    stages {\n"
        "        stage('Build') {\n"
        "            steps { sh 'echo hello' }\n"
        "        }\n"
        "    }\n"
        "}\n"
    )
    assert parse_jenkinsfile(path) is None


def test_boolean_parameter(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "longevityPipeline(\n"
        "    backend: 'k8s-eks',\n"
        "    functional_test: true,\n"
        "    region: 'eu-north-1',\n"
        "    test_name: 'functional_tests/scylla_operator',\n"
        "    test_config: 'test-cases/scylla-operator/functional.yaml',\n"
        "    post_behavior_db_nodes: 'destroy',\n"
        "    post_behavior_k8s_cluster: 'destroy'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.params["functional_test"] == "true"


def test_double_quoted_triple_string(create_jenkinsfile):
    path = create_jenkinsfile(
        "#!groovy\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "perfRegressionParallelPipeline(\n"
        '    backend: "aws",\n'
        '    region: "us-east-1",\n'
        '    test_name: "perf_test.PerfTest",\n'
        '    test_config: """["test-cases/a.yaml", "configurations/b.yaml"]""",\n'
        '    sub_tests: ["test_latency"],\n'
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.test_config == ["test-cases/a.yaml", "configurations/b.yaml"]


def test_jepsen_pipeline_single_string_config(create_jenkinsfile):
    path = create_jenkinsfile(
        "#! groovy\n\n"
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)\n\n"
        "jepsenPipeline(\n"
        "    test_config: 'test-cases/jepsen/jepsen.yaml',\n"
        "    test_name: 'jepsen_test',\n"
        "    provision_type: 'on_demand',\n"
        "    post_behavior_db_nodes: 'destroy',\n"
        "    post_behavior_loader_nodes: 'destroy',\n"
        "    post_behavior_monitor_nodes: 'destroy'\n"
        ")\n"
    )
    result = parse_jenkinsfile(path)
    assert result is not None
    assert result.pipeline_function == "jepsenPipeline"
    assert result.test_config == ["test-cases/jepsen/jepsen.yaml"]


# --- Tests for discover_pipeline_files ---


def test_discovers_jenkinsfiles(tmp_path):
    (tmp_path / "a.jenkinsfile").touch()
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "b.jenkinsfile").touch()
    (tmp_path / "not-a-pipeline.txt").touch()
    files = discover_pipeline_files(tmp_path)
    assert len(files) == 2
    assert all(f.suffix == ".jenkinsfile" for f in files)


def test_discover_raises_for_missing_dir():
    with pytest.raises(FileNotFoundError):
        discover_pipeline_files(Path("/nonexistent/directory"))


def test_discovers_real_pipeline_files():
    """Verify discovery works on the actual jenkins-pipelines directory."""
    root = Path("jenkins-pipelines")
    if not root.is_dir():
        pytest.skip("jenkins-pipelines directory not found")
    files = discover_pipeline_files(root)
    assert len(files) > 900


# --- Tests for PipelineConfig properties ---


def test_pipeline_config_backend_property():
    config = PipelineConfig(
        file_path="test.jenkinsfile",
        pipeline_function="longevityPipeline",
        test_config=["test.yaml"],
        params={"backend": "aws"},
    )
    assert config.backend == "aws"


def test_pipeline_config_backend_none():
    config = PipelineConfig(
        file_path="test.jenkinsfile",
        pipeline_function="longevityPipeline",
        test_config=["test.yaml"],
        params={},
    )
    assert config.backend is None


def test_pipeline_config_region_property():
    config = PipelineConfig(
        file_path="test.jenkinsfile",
        pipeline_function="longevityPipeline",
        test_config=["test.yaml"],
        params={"region": "eu-west-1"},
    )
    assert config.region == "eu-west-1"


def test_pipeline_config_gce_datacenter_property():
    config = PipelineConfig(
        file_path="test.jenkinsfile",
        pipeline_function="longevityPipeline",
        test_config=["test.yaml"],
        params={"gce_datacenter": "us-east1"},
    )
    assert config.gce_datacenter == "us-east1"
