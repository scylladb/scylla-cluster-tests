"""Unit tests for utils.staging_trigger package."""

import json
from unittest.mock import MagicMock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

import utils.staging_trigger.constants as constants_mod
from utils.staging_trigger.cache import _load_cache, _save_cache, lookup_pr
from utils.staging_trigger.cli import cli
from utils.staging_trigger.constants import (
    CUSTOM_VALUE_SENTINEL,
    DTEST_TOPOLOGY_FLAGS,
    KNOWN_PARAM_CHOICES,
    PIPELINE_TO_PRESET,
    PRESET_NAMES,
    ParamDefinition,
    Preset,
    TriggeredJob,
    get_presets,
)
from utils.staging_trigger.interactive import (
    _fetch_latest_release,
    _get_version_suggestions,
    _prompt_for_value,
    _ruff_format,
    _write_jobs_yaml_config,
    generate_python_code,
    generate_yaml_config,
    maybe_set_docker_image,
)
from utils.staging_trigger.jenkins_client import parse_parameter_definitions
from utils.staging_trigger.job_generation import _collect_folder_tree
from utils.staging_trigger.trigger import (
    StagingTrigger,
    _auto_set_docker_image,
    _make_dry_run_job,
    detect_preset_from_jenkinsfile,
    detect_preset_from_job_name,
    find_jenkinsfile,
    format_checklist,
    parse_set_params,
    run_from_config,
)


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------


class TestPreset:
    def test_preset_fields(self):
        p = Preset(params={"key": "val"}, folder_prefix="test-folder")
        assert p.params == {"key": "val"}
        assert p.folder_prefix == "test-folder"

    def test_preset_default_folder(self):
        with patch("utils.staging_trigger.constants.get_username", return_value="testuser"):
            p = Preset(params={})
            assert p.folder_prefix == "scylla-staging/testuser"


class TestTriggeredJob:
    def test_fields(self):
        job = TriggeredJob(job_url="http://j/job/x/", build_number=42, job_name="x-test", description="desc")
        assert job.job_url == "http://j/job/x/"
        assert job.build_number == 42
        assert job.job_name == "x-test"
        assert job.description == "desc"

    def test_default_description(self):
        job = TriggeredJob(job_url="http://j/job/x/", build_number=1, job_name="x")
        assert job.description == ""


class TestGetPresets:
    @patch("utils.staging_trigger.constants.get_username", return_value="alice")
    def test_presets_contain_expected_keys(self, _mock_user):
        # Reset cached presets
        constants_mod._PRESETS = None
        presets = get_presets()
        assert set(presets.keys()) == set(PRESET_NAMES)
        for name in PRESET_NAMES:
            assert isinstance(presets[name], Preset)
            assert isinstance(presets[name].params, dict)
        # Restore
        constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="alice")
    def test_preset_email_contains_username(self, _mock_user):
        constants_mod._PRESETS = None
        presets = get_presets()
        for name in ["longevity", "manager", "artifacts", "perf"]:
            assert presets[name].params["email_recipients"] == "alice@scylladb.com"
        constants_mod._PRESETS = None


class TestPipelineToPreset:
    def test_known_pipeline_functions(self):
        assert PIPELINE_TO_PRESET["longevityPipeline"] == "longevity"
        assert PIPELINE_TO_PRESET["managerPipeline"] == "manager"
        assert PIPELINE_TO_PRESET["artifactsPipeline"] == "artifacts"
        assert PIPELINE_TO_PRESET["perfRegressionParallelPipeline"] == "perf"

    def test_dtest_topology_flags(self):
        assert "no-tablets" in DTEST_TOPOLOGY_FLAGS
        assert "tablets" in DTEST_TOPOLOGY_FLAGS
        assert "gossip" in DTEST_TOPOLOGY_FLAGS


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


class TestCache:
    def test_load_missing_file(self, tmp_path):
        with patch("utils.staging_trigger.cache._CACHE_FILE", tmp_path / "missing.json"):
            assert _load_cache() == {}

    def test_save_and_load(self, tmp_path):
        cache_file = tmp_path / "cache.json"
        with patch("utils.staging_trigger.cache._CACHE_FILE", cache_file):
            _save_cache({"last_branch": "my-branch"})
            assert cache_file.exists()
            data = _load_cache()
            assert data["last_branch"] == "my-branch"

    def test_load_corrupt_json(self, tmp_path):
        cache_file = tmp_path / "cache.json"
        cache_file.write_text("not json{{{", encoding="utf-8")
        with patch("utils.staging_trigger.cache._CACHE_FILE", cache_file):
            assert _load_cache() == {}


class TestLookupPr:
    @patch("utils.staging_trigger.cache.subprocess.run")
    def test_lookup_pr_success(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout=json.dumps(
                {
                    "headRefName": "fix-branch",
                    "headRepositoryOwner": {"login": "alice"},
                    "headRepository": {"name": "scylla-cluster-tests"},
                }
            )
        )
        repo, branch = lookup_pr(123)
        assert repo == "git@github.com:alice/scylla-cluster-tests.git"
        assert branch == "fix-branch"
        mock_run.assert_called_once()


# ---------------------------------------------------------------------------
# trigger helpers
# ---------------------------------------------------------------------------


class TestDetectPresetFromJobName:
    @pytest.mark.parametrize(
        "name,expected",
        [
            pytest.param("longevity-100gb-4h-test", "longevity", id="longevity"),
            pytest.param("manager-ubuntu20-sanity-test", "manager", id="manager"),
            pytest.param("artifacts-ami-test", "artifacts", id="artifacts"),
            pytest.param("perf-regression-latency-test", "perf", id="perf"),
            pytest.param("dtest-pytest-gating", "dtest", id="dtest"),
            pytest.param("gemini-3h-test", "longevity", id="gemini"),
            pytest.param("rolling-upgrade-test", "longevity", id="rolling-upgrade"),
            pytest.param("jepsen-test", "longevity", id="jepsen"),
            pytest.param("unknown-job-name", None, id="unknown"),
        ],
    )
    def test_detect(self, name, expected):
        assert detect_preset_from_job_name(name) == expected


class TestDetectPresetFromJenkinsfile:
    def test_longevity_pipeline(self, tmp_path):
        jf = tmp_path / "test.jenkinsfile"
        jf.write_text("longevityPipeline(\n  params: ...\n)", encoding="utf-8")
        assert detect_preset_from_jenkinsfile(jf) == "longevity"

    def test_manager_pipeline(self, tmp_path):
        jf = tmp_path / "test.jenkinsfile"
        jf.write_text("managerPipeline(\n  params: ...\n)", encoding="utf-8")
        assert detect_preset_from_jenkinsfile(jf) == "manager"

    def test_unknown_pipeline(self, tmp_path):
        jf = tmp_path / "test.jenkinsfile"
        jf.write_text("unknownPipeline(\n  params: ...\n)", encoding="utf-8")
        assert detect_preset_from_jenkinsfile(jf) is None


class TestParseSetParams:
    def test_valid_pairs(self):
        result = parse_set_params(("key1=val1", "key2=val2"))
        assert result == {"key1": "val1", "key2": "val2"}

    def test_value_with_equals(self):
        result = parse_set_params(("key=a=b=c",))
        assert result == {"key": "a=b=c"}

    def test_invalid_pair(self):
        with pytest.raises(click.BadParameter, match="Expected KEY=VALUE"):
            parse_set_params(("no_equals_here",))

    def test_empty_value(self):
        result = parse_set_params(("key=",))
        assert result == {"key": ""}


class TestFormatChecklist:
    def test_single_job(self):
        jobs = [
            TriggeredJob(
                job_url="http://jenkins/job/test/", build_number=42, job_name="test-job", description="longevity"
            )
        ]
        md = format_checklist(jobs)
        assert "- [ ]" in md
        assert "test-job #42" in md
        assert "(longevity)" in md
        assert "http://jenkins/job/test/42/" in md

    def test_no_description(self):
        jobs = [TriggeredJob(job_url="http://jenkins/job/test/", build_number=1, job_name="test-job")]
        md = format_checklist(jobs)
        assert "()" not in md

    def test_multiple_jobs(self):
        jobs = [
            TriggeredJob(job_url="http://j/job/a/", build_number=1, job_name="a"),
            TriggeredJob(job_url="http://j/job/b/", build_number=2, job_name="b"),
        ]
        md = format_checklist(jobs)
        assert md.count("- [ ]") == 2


class TestMakeDryRunJob:
    @patch("utils.staging_trigger.trigger._get_jenkins_url", return_value="http://jenkins.example.com")
    def test_dry_run_job(self, _mock_url):
        job = _make_dry_run_job("staging/folder/test-job-test", "test-job-test", "desc")
        assert job.build_number == 0
        assert job.job_name == "test-job-test"
        assert job.description == "desc"
        assert "staging/job/folder/job/test-job-test" in job.job_url

    def test_dry_run_with_explicit_url(self):
        job = _make_dry_run_job("folder/job-test", "job-test", jenkins_url="http://custom")
        assert job.job_url.startswith("http://custom")


# ---------------------------------------------------------------------------
# interactive helpers
# ---------------------------------------------------------------------------


class TestRuffFormat:
    def test_fallback_on_missing_ruff(self):
        code = "x=1\n"
        with patch("utils.staging_trigger.interactive.subprocess.run", side_effect=FileNotFoundError):
            assert _ruff_format(code) == code

    def test_formats_code(self):
        code = "x=1\ny=2\n"
        result = _ruff_format(code)
        # Should either format or return original
        assert "x" in result
        assert "y" in result


class TestGeneratePythonCode:
    @patch("utils.staging_trigger.interactive._default_folder", return_value="scylla-staging/testuser")
    @patch("utils.staging_trigger.interactive._ruff_format", side_effect=lambda c: c)
    def test_basic_generation(self, _fmt, _folder):
        code = generate_python_code(
            ["longevity-100gb-4h-test"],
            branch="my-branch",
            folder="scylla-staging/testuser",
            repo="git@github.com:scylladb/scylla-cluster-tests.git",
        )
        assert "from staging_trigger import StagingTrigger" in code
        assert "from_preset" in code
        assert '"longevity"' in code
        assert '"my-branch"' in code
        assert "select_jobs" in code
        assert "trigger.run()" in code

    @patch("utils.staging_trigger.interactive._default_folder", return_value="scylla-staging/testuser")
    @patch("utils.staging_trigger.interactive._ruff_format", side_effect=lambda c: c)
    def test_dtest_generation(self, _fmt, _folder):
        code = generate_python_code(
            ["dtest-pytest-gating"],
            branch="master",
            folder="scylla-staging/testuser",
            repo="git@github.com:scylladb/scylla-cluster-tests.git",
            dtest_topologies=["no-tablets", "tablets"],
        )
        assert "run_dtest_variants" in code
        assert "dtest" in code

    @patch("utils.staging_trigger.interactive._default_folder", return_value="scylla-staging/testuser")
    @patch("utils.staging_trigger.interactive._ruff_format", side_effect=lambda c: c)
    def test_empty_params_excluded(self, _fmt, _folder):
        code = generate_python_code(
            ["longevity-100gb-4h-test"],
            branch="my-branch",
            folder="scylla-staging/testuser",
            repo="git@github.com:scylladb/scylla-cluster-tests.git",
            param_overrides={"key1": "val1", "empty_key": ""},
        )
        assert "key1" in code
        assert "empty_key" not in code

    @patch("utils.staging_trigger.interactive._default_folder", return_value="scylla-staging/testuser")
    @patch("utils.staging_trigger.interactive._ruff_format", side_effect=lambda c: c)
    def test_output_to_file(self, _fmt, _folder, tmp_path):
        out = tmp_path / "gen.py"
        generate_python_code(
            ["longevity-100gb-4h-test"],
            branch="master",
            folder="scylla-staging/testuser",
            repo="git@github.com:scylladb/scylla-cluster-tests.git",
            output_path=str(out),
        )
        assert out.exists()
        assert "StagingTrigger" in out.read_text()


# ---------------------------------------------------------------------------
# job_generation helpers
# ---------------------------------------------------------------------------


class TestCollectFolderTree:
    def test_single_jenkinsfile_in_subdir(self, tmp_path):
        pipelines_dir = tmp_path / "jenkins-pipelines"
        sub = pipelines_dir / "oss" / "longevity"
        sub.mkdir(parents=True)
        jf = sub / "test.jenkinsfile"
        jf.write_text("longevityPipeline()", encoding="utf-8")

        tree = _collect_folder_tree([jf], pipelines_dir)
        paths = [t[0] for t in tree]
        assert "oss" in paths
        assert "oss/longevity" in paths
        # Parent first
        assert paths.index("oss") < paths.index("oss/longevity")

    def test_reads_folder_definitions(self, tmp_path):
        pipelines_dir = tmp_path / "jenkins-pipelines"
        sub = pipelines_dir / "mycat"
        sub.mkdir(parents=True)
        (sub / "_folder_definitions.yaml").write_text(
            "folder-name: My Category\nfolder-description: Custom desc\n", encoding="utf-8"
        )
        jf = sub / "test.jenkinsfile"
        jf.write_text("", encoding="utf-8")

        tree = _collect_folder_tree([jf], pipelines_dir)
        assert len(tree) == 1
        assert tree[0] == ("mycat", "My Category", "Custom desc")

    def test_reads_display_name_file(self, tmp_path):
        pipelines_dir = tmp_path / "jenkins-pipelines"
        sub = pipelines_dir / "mycat"
        sub.mkdir(parents=True)
        (sub / "_display_name").write_text("  Fancy Name  ", encoding="utf-8")
        jf = sub / "test.jenkinsfile"
        jf.write_text("", encoding="utf-8")

        tree = _collect_folder_tree([jf], pipelines_dir)
        assert tree[0][1] == "Fancy Name"

    def test_empty_list(self, tmp_path):
        pipelines_dir = tmp_path / "jenkins-pipelines"
        pipelines_dir.mkdir()
        assert _collect_folder_tree([], pipelines_dir) == []

    def test_root_level_jenkinsfile(self, tmp_path):
        pipelines_dir = tmp_path / "jenkins-pipelines"
        pipelines_dir.mkdir()
        jf = pipelines_dir / "test.jenkinsfile"
        jf.write_text("", encoding="utf-8")
        # Root-level file has no parent dirs to create
        tree = _collect_folder_tree([jf], pipelines_dir)
        assert tree == []


# ---------------------------------------------------------------------------
# CLI smoke tests
# ---------------------------------------------------------------------------


class TestCli:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "trigger" in result.output
        assert "generate" in result.output
        assert "run-config" in result.output

    def test_trigger_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["trigger", "--help"])
        assert result.exit_code == 0
        assert "--branch" in result.output
        assert "--dry-run" in result.output

    def test_generate_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["generate", "--help"])
        assert result.exit_code == 0
        assert "--branch" in result.output

    def test_run_config_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["run-config", "--help"])
        assert result.exit_code == 0
        assert "CONFIG_FILE" in result.output


# ---------------------------------------------------------------------------
# find_jenkinsfile
# ---------------------------------------------------------------------------


class TestFindJenkinsfile:
    @patch("utils.staging_trigger.trigger.get_sct_root_path")
    def test_find_by_full_path(self, mock_root, tmp_path):
        pipelines = tmp_path / "jenkins-pipelines"
        jf = pipelines / "oss" / "longevity" / "longevity-100gb-4h.jenkinsfile"
        jf.parent.mkdir(parents=True)
        jf.write_text("longevityPipeline()", encoding="utf-8")
        mock_root.return_value = tmp_path

        result = find_jenkinsfile("oss/longevity/longevity-100gb-4h")
        assert result == jf

    @patch("utils.staging_trigger.trigger.get_sct_root_path")
    def test_find_by_stem_single_match(self, mock_root, tmp_path):
        pipelines = tmp_path / "jenkins-pipelines"
        jf = pipelines / "oss" / "unique-test.jenkinsfile"
        jf.parent.mkdir(parents=True)
        jf.write_text("longevityPipeline()", encoding="utf-8")
        mock_root.return_value = tmp_path

        result = find_jenkinsfile("unique-test")
        assert result == jf

    @patch("utils.staging_trigger.trigger.get_sct_root_path")
    def test_find_no_match(self, mock_root, tmp_path):
        pipelines = tmp_path / "jenkins-pipelines"
        pipelines.mkdir(parents=True)
        mock_root.return_value = tmp_path

        result = find_jenkinsfile("nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# StagingTrigger
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_jenkins():
    """Create a mocked JenkinsJobTrigger for StagingTrigger tests."""
    with patch("utils.staging_trigger.trigger.JenkinsJobTrigger") as mock_cls:
        mock_client = MagicMock()
        mock_client.url = "http://jenkins.example.com"
        mock_cls.return_value = mock_client
        yield mock_client


class TestStagingTrigger:
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_from_preset(self, _mock_user, mock_jenkins):
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="my-branch", repo="git@github.com:user/repo.git")
            assert trigger.git_branch == "my-branch"
            assert trigger.git_repo == "git@github.com:user/repo.git"
            assert trigger.preset.params["email_recipients"] == "testuser@scylladb.com"
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_from_preset_with_folder_override(self, _mock_user, mock_jenkins):
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="my-branch", folder="custom-folder")
            assert trigger.preset.folder_prefix == "custom-folder"
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_select_jobs(self, _mock_user, mock_jenkins):
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="master")
            trigger.select_jobs("job-a", "job-b")
            assert trigger.selected_jobs == ["job-a", "job-b"]
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_dry_run(self, _mock_user, mock_jenkins):
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
            trigger.select_jobs("longevity-100gb-4h-test")
            results = trigger.run(dry_run=True)
            assert len(results) == 1
            assert results[0].build_number == 0
            assert results[0].job_name == "longevity-100gb-4h-test"
            # Dry run should NOT call trigger or update_scm
            mock_jenkins.trigger.assert_not_called()
            mock_jenkins.update_scm.assert_not_called()
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_triggers_jobs(self, _mock_user, mock_jenkins):
        constants_mod._PRESETS = None
        mock_jenkins.get_last_build_params.return_value = {"old_param": "old_val"}
        mock_jenkins.get_job_url.return_value = "http://jenkins/job/test/"
        mock_jenkins.trigger.return_value = 42
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="my-branch", folder="staging/testuser")
            trigger.select_jobs("my-job-test")
            results = trigger.run(dry_run=False)
            assert len(results) == 1
            assert results[0].build_number == 42
            assert results[0].job_name == "my-job-test"
            mock_jenkins.update_scm.assert_called_once()
            mock_jenkins.trigger.assert_called_once()
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_multi_dry_run(self, _mock_user, mock_jenkins):
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
            jobs_with_params = [
                ("job-a", {"extra": "val_a"}),
                ("job-b", {"extra": "val_b"}),
            ]
            results = trigger.run_multi(jobs_with_params, dry_run=True)
            assert len(results) == 2
            assert results[0].job_name == "job-a"
            assert results[1].job_name == "job-b"
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_dtest_variants_dry_run(self, _mock_user, mock_jenkins):
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("dtest", branch="master", folder="staging/testuser")
            results = trigger.run_dtest_variants(
                "dtest-pytest-gating", topologies=["no-tablets", "tablets"], dry_run=True
            )
            assert len(results) == 2
            assert results[0].description == "no-tablets"
            assert results[1].description == "tablets"
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_param_override_precedence(self, _mock_user, mock_jenkins):
        """Verify that param_overrides override preset params."""
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset(
                "longevity", branch="master", folder="staging/testuser", param_overrides={"scylla_version": "2025.1.0"}
            )
            assert trigger.param_overrides["scylla_version"] == "2025.1.0"
        finally:
            constants_mod._PRESETS = None


# ---------------------------------------------------------------------------
# run_from_config
# ---------------------------------------------------------------------------


class TestRunFromConfig:
    @patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_from_config_dry_run(self, _mock_user, mock_jenkins_cls, tmp_path):
        constants_mod._PRESETS = None

        mock_client = MagicMock()
        mock_client.url = "http://jenkins.example.com"
        mock_jenkins_cls.return_value = mock_client

        config = {
            "branch": "my-branch",
            "folder": "staging/testuser",
            "params": {"scylla_version": "master:latest"},
            "jobs": [
                {"name": "longevity-100gb-4h-test", "preset": "longevity"},
            ],
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config), encoding="utf-8")

        try:
            results = run_from_config(str(config_path), dry_run=True)
            assert len(results) == 1
            assert results[0].build_number == 0
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_from_config_missing_branch_raises(self, _mock_user, mock_jenkins_cls, tmp_path):
        constants_mod._PRESETS = None

        config = {"jobs": [{"name": "test-job"}]}
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config), encoding="utf-8")

        try:
            with pytest.raises(click.UsageError, match="branch.*or.*pr"):
                run_from_config(str(config_path))
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_from_config_dtest_topologies(self, _mock_user, mock_jenkins_cls, tmp_path):
        constants_mod._PRESETS = None

        mock_client = MagicMock()
        mock_client.url = "http://jenkins.example.com"
        mock_jenkins_cls.return_value = mock_client

        config = {
            "branch": "master",
            "folder": "staging/testuser",
            "jobs": [
                {
                    "name": "dtest-pytest-gating",
                    "preset": "dtest",
                    "dtest_topologies": ["no-tablets", "tablets"],
                },
            ],
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config), encoding="utf-8")

        try:
            results = run_from_config(str(config_path), dry_run=True)
            assert len(results) == 2
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_from_config_auto_detects_preset(self, _mock_user, mock_jenkins_cls, tmp_path):
        constants_mod._PRESETS = None

        mock_client = MagicMock()
        mock_client.url = "http://jenkins.example.com"
        mock_jenkins_cls.return_value = mock_client

        config = {
            "branch": "master",
            "folder": "staging/testuser",
            "jobs": [
                {"name": "manager-ubuntu20-sanity-test"},
            ],
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config), encoding="utf-8")

        try:
            results = run_from_config(str(config_path), dry_run=True)
            assert len(results) == 1
        finally:
            constants_mod._PRESETS = None


# ---------------------------------------------------------------------------
# generate_yaml_config / _write_jobs_yaml_config
# ---------------------------------------------------------------------------


class TestGenerateYamlConfig:
    @patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_generates_valid_yaml(self, _mock_user1, _mock_user2, tmp_path):
        constants_mod._PRESETS = None

        pipelines_dir = tmp_path / "jenkins-pipelines"
        sub = pipelines_dir / "oss" / "longevity"
        sub.mkdir(parents=True)
        jf = sub / "longevity-100gb-4h.jenkinsfile"
        jf.write_text("longevityPipeline()", encoding="utf-8")

        out_path = tmp_path / "output.yaml"

        try:
            with patch("utils.staging_trigger.interactive.get_sct_root_path", return_value=tmp_path):
                yaml_text = generate_yaml_config(
                    [jf],
                    branch="my-branch",
                    folder="staging/testuser",
                    repo="git@github.com:scylladb/scylla-cluster-tests.git",
                    output_path=str(out_path),
                )

            parsed = yaml.safe_load(yaml_text)
            assert parsed["branch"] == "my-branch"
            assert parsed["folder"] == "staging/testuser"
            assert len(parsed["jobs"]) == 1
            assert parsed["jobs"][0]["preset"] == "longevity"
            assert out_path.exists()
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_includes_custom_repo(self, _mock_user1, _mock_user2, tmp_path):
        constants_mod._PRESETS = None

        pipelines_dir = tmp_path / "jenkins-pipelines"
        sub = pipelines_dir / "oss"
        sub.mkdir(parents=True)
        jf = sub / "test.jenkinsfile"
        jf.write_text("longevityPipeline()", encoding="utf-8")

        try:
            with patch("utils.staging_trigger.interactive.get_sct_root_path", return_value=tmp_path):
                yaml_text = generate_yaml_config(
                    [jf], branch="master", folder="staging/testuser", repo="git@github.com:fork/repo.git"
                )

            parsed = yaml.safe_load(yaml_text)
            assert parsed["repo"] == "git@github.com:fork/repo.git"
        finally:
            constants_mod._PRESETS = None


class TestWriteJobsYamlConfig:
    @patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_generates_yaml_from_job_names(self, _mock_user1, _mock_user2, tmp_path):
        constants_mod._PRESETS = None

        out_path = tmp_path / "jobs.yaml"
        try:
            yaml_text = _write_jobs_yaml_config(
                ["longevity-100gb-4h-test", "manager-sanity-test"],
                branch="feature-branch",
                folder="staging/testuser",
                repo="git@github.com:scylladb/scylla-cluster-tests.git",
                output_path=str(out_path),
            )

            parsed = yaml.safe_load(yaml_text)
            assert parsed["branch"] == "feature-branch"
            assert len(parsed["jobs"]) == 2
            assert parsed["jobs"][0]["preset"] == "longevity"
            assert parsed["jobs"][1]["preset"] == "manager"
            assert out_path.exists()
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_param_overrides_applied(self, _mock_user1, _mock_user2, tmp_path):
        constants_mod._PRESETS = None

        try:
            yaml_text = _write_jobs_yaml_config(
                ["longevity-100gb-4h-test"],
                branch="master",
                folder="staging/testuser",
                repo="git@github.com:scylladb/scylla-cluster-tests.git",
                param_overrides={"scylla_version": "2025.1.0"},
            )

            parsed = yaml.safe_load(yaml_text)
            assert parsed["params"]["scylla_version"] == "2025.1.0"
        finally:
            constants_mod._PRESETS = None


# ---------------------------------------------------------------------------
# ParamDefinition + choice extraction
# ---------------------------------------------------------------------------


class TestParamDefinition:
    def test_basic_fields(self):
        p = ParamDefinition(default="val")
        assert p.default == "val"
        assert p.choices is None

    def test_with_choices(self):
        p = ParamDefinition(default="a", choices=["a", "b", "c"])
        assert p.choices == ["a", "b", "c"]


class TestParseParameterDefinitions:
    def test_string_param(self):
        xml = """<project>
          <properties><hudson.model.ParametersDefinitionProperty>
            <parameterDefinitions>
              <hudson.model.StringParameterDefinition>
                <name>scylla_version</name>
                <defaultValue>master:latest</defaultValue>
              </hudson.model.StringParameterDefinition>
            </parameterDefinitions>
          </hudson.model.ParametersDefinitionProperty></properties>
        </project>"""
        result = parse_parameter_definitions(xml)
        assert "scylla_version" in result
        assert result["scylla_version"].default == "master:latest"
        assert result["scylla_version"].choices is None

    def test_choice_param(self):
        xml = """<project>
          <properties><hudson.model.ParametersDefinitionProperty>
            <parameterDefinitions>
              <hudson.model.ChoiceParameterDefinition>
                <name>provision_type</name>
                <choices class="java.util.Arrays$ArrayList">
                  <a class="string-array">
                    <string>on_demand</string>
                    <string>spot</string>
                    <string>spot_fleet</string>
                  </a>
                </choices>
              </hudson.model.ChoiceParameterDefinition>
            </parameterDefinitions>
          </hudson.model.ParametersDefinitionProperty></properties>
        </project>"""
        result = parse_parameter_definitions(xml)
        assert "provision_type" in result
        assert result["provision_type"].choices == ["on_demand", "spot", "spot_fleet"]
        # Default should be first choice when no defaultValue element
        assert result["provision_type"].default == "on_demand"

    def test_choice_param_with_default(self):
        xml = """<project>
          <properties><hudson.model.ParametersDefinitionProperty>
            <parameterDefinitions>
              <hudson.model.ChoiceParameterDefinition>
                <name>backend</name>
                <defaultValue>aws</defaultValue>
                <choices class="java.util.Arrays$ArrayList">
                  <a class="string-array">
                    <string>aws</string>
                    <string>gce</string>
                    <string>docker</string>
                  </a>
                </choices>
              </hudson.model.ChoiceParameterDefinition>
            </parameterDefinitions>
          </hudson.model.ParametersDefinitionProperty></properties>
        </project>"""
        result = parse_parameter_definitions(xml)
        assert result["backend"].default == "aws"
        assert result["backend"].choices == ["aws", "gce", "docker"]

    def test_mixed_params(self):
        xml = """<project>
          <properties><hudson.model.ParametersDefinitionProperty>
            <parameterDefinitions>
              <hudson.model.StringParameterDefinition>
                <name>branch</name>
                <defaultValue>master</defaultValue>
              </hudson.model.StringParameterDefinition>
              <hudson.model.ChoiceParameterDefinition>
                <name>region</name>
                <choices class="java.util.Arrays$ArrayList">
                  <a class="string-array">
                    <string>us-east-1</string>
                    <string>eu-west-1</string>
                  </a>
                </choices>
              </hudson.model.ChoiceParameterDefinition>
            </parameterDefinitions>
          </hudson.model.ParametersDefinitionProperty></properties>
        </project>"""
        result = parse_parameter_definitions(xml)
        assert len(result) == 2
        assert result["branch"].choices is None
        assert result["region"].choices == ["us-east-1", "eu-west-1"]

    def test_empty_xml(self):
        xml = "<project></project>"
        result = parse_parameter_definitions(xml)
        assert result == {}

    def test_separator_definitions_are_filtered_out(self):
        """ParameterSeparatorDefinition elements are visual headers, not real params."""
        xml = """<project>
          <properties><hudson.model.ParametersDefinitionProperty>
            <parameterDefinitions>
              <jenkins.plugins.parameter__separator.ParameterSeparatorDefinition>
                <name>CLOUD_PROVIDER</name>
              </jenkins.plugins.parameter__separator.ParameterSeparatorDefinition>
              <hudson.model.StringParameterDefinition>
                <name>backend</name>
                <defaultValue>aws</defaultValue>
              </hudson.model.StringParameterDefinition>
              <jenkins.plugins.parameter__separator.ParameterSeparatorDefinition>
                <name>POST_BEHAVIOR</name>
              </jenkins.plugins.parameter__separator.ParameterSeparatorDefinition>
              <hudson.model.StringParameterDefinition>
                <name>post_behavior_db_nodes</name>
                <defaultValue>destroy</defaultValue>
              </hudson.model.StringParameterDefinition>
            </parameterDefinitions>
          </hudson.model.ParametersDefinitionProperty></properties>
        </project>"""
        result = parse_parameter_definitions(xml)
        assert "CLOUD_PROVIDER" not in result
        assert "POST_BEHAVIOR" not in result
        assert "backend" in result
        assert "post_behavior_db_nodes" in result
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Version suggestions
# ---------------------------------------------------------------------------


class TestGetVersionSuggestions:
    @patch("utils.staging_trigger.interactive.get_latest_scylla_release", return_value="2025.1.3")
    def test_suggestions_with_release(self, _mock_release):
        _fetch_latest_release.cache_clear()
        suggestions = _get_version_suggestions("master:latest")
        values = [c.value for c in suggestions]
        assert "master:latest" in values
        assert "2025.1.3" in values
        assert CUSTOM_VALUE_SENTINEL in values
        _fetch_latest_release.cache_clear()

    @patch("utils.staging_trigger.interactive.get_latest_scylla_release", side_effect=Exception("network error"))
    def test_suggestions_when_release_fetch_fails(self, _mock_release):
        _fetch_latest_release.cache_clear()
        suggestions = _get_version_suggestions("5.4.0")
        values = [c.value for c in suggestions]
        assert "master:latest" in values
        assert "5.4.0" in values  # current value preserved
        assert CUSTOM_VALUE_SENTINEL in values
        _fetch_latest_release.cache_clear()

    @patch("utils.staging_trigger.interactive.get_latest_scylla_release", return_value="2025.1.3")
    def test_no_duplicate_current_value(self, _mock_release):
        _fetch_latest_release.cache_clear()
        suggestions = _get_version_suggestions("master:latest")
        values = [c.value for c in suggestions]
        # master:latest should appear only once
        assert values.count("master:latest") == 1
        _fetch_latest_release.cache_clear()


# ---------------------------------------------------------------------------
# _prompt_for_value with KNOWN_PARAM_CHOICES fallback
# ---------------------------------------------------------------------------


class TestPromptForValue:
    """Test that _prompt_for_value uses KNOWN_PARAM_CHOICES when Jenkins metadata has no choices."""

    @patch("utils.staging_trigger.interactive.questionary")
    def test_known_param_shows_select(self, mock_q):
        """provision_type should show a select menu from KNOWN_PARAM_CHOICES."""
        mock_q.select.return_value.ask.return_value = "on_demand"
        result = _prompt_for_value("provision_type", "spot", param_meta={})
        mock_q.select.assert_called_once()
        assert result == "on_demand"

    @patch("utils.staging_trigger.interactive.questionary")
    def test_known_param_custom_value_fallback(self, mock_q):
        """Picking 'Custom value...' should fall back to text input."""
        mock_q.select.return_value.ask.return_value = CUSTOM_VALUE_SENTINEL
        mock_q.text.return_value.ask.return_value = "spot_fleet"
        result = _prompt_for_value("provision_type", "spot", param_meta={})
        mock_q.text.assert_called_once()
        assert result == "spot_fleet"

    @patch("utils.staging_trigger.interactive.questionary")
    def test_unknown_param_shows_text(self, mock_q):
        """Parameters not in KNOWN_PARAM_CHOICES should use text input."""
        mock_q.text.return_value.ask.return_value = "my-value"
        result = _prompt_for_value("email_recipients", "qa@scylladb.com", param_meta={})
        mock_q.text.assert_called_once()
        assert result == "my-value"

    @patch("utils.staging_trigger.interactive.questionary")
    def test_jenkins_choices_take_precedence(self, mock_q):
        """Jenkins metadata choices should be used over KNOWN_PARAM_CHOICES."""
        jenkins_choices = ["alpha", "beta", "gamma"]
        meta = {"my_param": ParamDefinition(default="alpha", choices=jenkins_choices)}
        mock_q.select.return_value.ask.return_value = "beta"
        result = _prompt_for_value("my_param", "alpha", param_meta=meta)
        # Verify the select was called (not text)
        mock_q.select.assert_called_once()
        assert result == "beta"

    @patch("utils.staging_trigger.interactive.questionary")
    def test_post_behavior_shows_select(self, mock_q):
        """post_behavior_db_nodes should show destroy/keep/keep-on-failure."""
        mock_q.select.return_value.ask.return_value = "keep"
        result = _prompt_for_value("post_behavior_db_nodes", "destroy", param_meta={})
        mock_q.select.assert_called_once()
        assert result == "keep"

    @pytest.mark.parametrize(
        "param_name",
        [pytest.param(k, id=k) for k in KNOWN_PARAM_CHOICES],
    )
    def test_all_known_params_have_choices(self, param_name):
        """Every key in KNOWN_PARAM_CHOICES must have a non-empty list."""
        assert len(KNOWN_PARAM_CHOICES[param_name]) >= 2


# ---------------------------------------------------------------------------
# Docker image auto-derivation
# ---------------------------------------------------------------------------


class TestMaybeSetDockerImage:
    def test_sets_nightly_for_master_latest(self):
        params = {"scylla_version": "master:latest"}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla-nightly"

    def test_sets_release_repo_for_release_version(self):
        params = {"scylla_version": "2025.1.3"}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla"

    def test_skips_when_no_version(self):
        params = {"scylla_version": ""}
        maybe_set_docker_image(params)
        assert "scylla_docker_image" not in params

    def test_skips_when_backend_not_docker(self):
        params = {"scylla_version": "master:latest", "backend": "aws"}
        maybe_set_docker_image(params)
        assert "scylla_docker_image" not in params

    def test_skips_when_already_set(self):
        params = {"scylla_version": "master:latest", "scylla_docker_image": "my-custom-image"}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "my-custom-image"

    def test_allows_empty_backend(self):
        params = {"scylla_version": "master:latest", "backend": ""}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla-nightly"

    def test_handles_invalid_version(self):
        params = {"scylla_version": "not-a-version"}
        maybe_set_docker_image(params)
        # Should not crash; image not set
        assert "scylla_docker_image" not in params


class TestAutoSetDockerImageTrigger:
    """Test _auto_set_docker_image from trigger module."""

    def test_sets_nightly_for_master_latest(self):
        params = {"scylla_version": "master:latest"}
        _auto_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla-nightly"

    def test_skips_when_backend_not_docker(self):
        params = {"scylla_version": "master:latest", "backend": "aws"}
        _auto_set_docker_image(params)
        assert "scylla_docker_image" not in params

    def test_skips_when_already_set(self):
        params = {"scylla_version": "master:latest", "scylla_docker_image": "custom"}
        _auto_set_docker_image(params)
        assert params["scylla_docker_image"] == "custom"

    def test_enterprise_nightly(self):
        params = {"scylla_version": "enterprise:latest"}
        _auto_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla-enterprise-nightly"
