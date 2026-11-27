"""Unit tests for utils.staging_trigger package."""

import json
import re
import subprocess
from unittest.mock import MagicMock, patch

import click
import jenkins
import pytest
import yaml
from click.shell_completion import CompletionItem
from click.testing import CliRunner

import utils.staging_trigger.constants as constants_mod
from utils.staging_trigger.cache import _load_cache, _save_cache, lookup_pr
from utils.staging_trigger.cli import _JobNameType, _show_trigger_examples, cli
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
from utils.staging_trigger.jenkins_client import (
    JenkinsJobTrigger,
    _login,
    _run_secret_command,
    iter_jenkins_credentials,
    parse_parameter_definitions,
)
from utils.staging_trigger.job_generation import _collect_folder_tree, generate_job, generate_jobs
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
    """Preset construction and its default staging folder."""

    def test_preset_fields(self):
        """A Preset keeps the given params and folder prefix."""
        p = Preset(params={"key": "val"}, folder_prefix="test-folder")
        assert p.params == {"key": "val"}
        assert p.folder_prefix == "test-folder"

    def test_preset_default_folder(self):
        """A Preset without a folder prefix defaults to scylla-staging/<username>."""
        with patch("utils.staging_trigger.constants.get_username", return_value="testuser"):
            p = Preset(params={})
            assert p.folder_prefix == "scylla-staging/testuser"


class TestTriggeredJob:
    """TriggeredJob fields and defaults."""

    def test_fields(self):
        """TriggeredJob keeps the given URL, build number, job name and description."""
        job = TriggeredJob(job_url="http://j/job/x/", build_number=42, job_name="x-test", description="desc")
        assert job.job_url == "http://j/job/x/"
        assert job.build_number == 42
        assert job.job_name == "x-test"
        assert job.description == "desc"

    def test_default_description(self):
        """TriggeredJob defaults to an empty description."""
        job = TriggeredJob(job_url="http://j/job/x/", build_number=1, job_name="x")
        assert job.description == ""


class TestGetPresets:
    """Building the preset map from get_presets."""

    @patch("utils.staging_trigger.constants.get_username", return_value="alice")
    def test_presets_contain_expected_keys(self, _mock_user):
        """get_presets returns a Preset with a params dict for every name in PRESET_NAMES."""
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
        """The longevity, manager, artifacts and perf presets email <username>@scylladb.com."""
        constants_mod._PRESETS = None
        presets = get_presets()
        for name in ["longevity", "manager", "artifacts", "perf"]:
            assert presets[name].params["email_recipients"] == "alice@scylladb.com"
        constants_mod._PRESETS = None


class TestPipelineToPreset:
    """Pipeline-function-to-preset mapping and the dtest topology flags."""

    def test_known_pipeline_functions(self):
        """The longevity, manager, artifacts and perfRegressionParallel pipeline functions map to their presets."""
        assert PIPELINE_TO_PRESET["longevityPipeline"] == "longevity"
        assert PIPELINE_TO_PRESET["managerPipeline"] == "manager"
        assert PIPELINE_TO_PRESET["artifactsPipeline"] == "artifacts"
        assert PIPELINE_TO_PRESET["perfRegressionParallelPipeline"] == "perf"

    def test_dtest_topology_flags(self):
        """DTEST_TOPOLOGY_FLAGS holds the no-tablets, tablets and gossip flags."""
        assert "no-tablets" in DTEST_TOPOLOGY_FLAGS
        assert "tablets" in DTEST_TOPOLOGY_FLAGS
        assert "gossip" in DTEST_TOPOLOGY_FLAGS


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


class TestCache:
    """Loading and saving the local staging trigger cache file."""

    def test_load_missing_file(self, tmp_path):
        """A missing cache file loads as an empty dict."""
        with patch("utils.staging_trigger.cache._CACHE_FILE", tmp_path / "missing.json"):
            assert _load_cache() == {}

    def test_save_and_load(self, tmp_path):
        """A saved cache is written to disk and loads back with the same data."""
        cache_file = tmp_path / "cache.json"
        with patch("utils.staging_trigger.cache._CACHE_FILE", cache_file):
            _save_cache({"last_branch": "my-branch"})
            assert cache_file.exists()
            data = _load_cache()
            assert data["last_branch"] == "my-branch"

    def test_load_corrupt_json(self, tmp_path):
        """A cache file with invalid JSON loads as an empty dict."""
        cache_file = tmp_path / "cache.json"
        cache_file.write_text("not json{{{", encoding="utf-8")
        with patch("utils.staging_trigger.cache._CACHE_FILE", cache_file):
            assert _load_cache() == {}


class TestLookupPr:
    """Resolving a PR number to its head repository and branch via gh."""

    @patch("utils.staging_trigger.cache.subprocess.run")
    def test_lookup_pr_success(self, mock_run):
        """lookup_pr returns the SSH URL of the PR's head repository and its head branch."""
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
    """Preset detection from a Jenkins job name."""

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
        """Job names map to the preset whose keyword they contain; gemini, rolling-upgrade, jepsen map to longevity."""
        assert detect_preset_from_job_name(name) == expected


class TestDetectPresetFromJenkinsfile:
    """Preset detection from the pipeline function a Jenkinsfile calls."""

    def test_longevity_pipeline(self, tmp_path):
        """A Jenkinsfile calling longevityPipeline maps to the longevity preset."""
        jf = tmp_path / "test.jenkinsfile"
        jf.write_text("longevityPipeline(\n  params: ...\n)", encoding="utf-8")
        assert detect_preset_from_jenkinsfile(jf) == "longevity"

    def test_manager_pipeline(self, tmp_path):
        """A Jenkinsfile calling managerPipeline maps to the manager preset."""
        jf = tmp_path / "test.jenkinsfile"
        jf.write_text("managerPipeline(\n  params: ...\n)", encoding="utf-8")
        assert detect_preset_from_jenkinsfile(jf) == "manager"

    def test_unknown_pipeline(self, tmp_path):
        """A Jenkinsfile calling an unknown pipeline function maps to None."""
        jf = tmp_path / "test.jenkinsfile"
        jf.write_text("unknownPipeline(\n  params: ...\n)", encoding="utf-8")
        assert detect_preset_from_jenkinsfile(jf) is None


class TestParseSetParams:
    """Parsing KEY=VALUE pairs given with --set."""

    def test_valid_pairs(self):
        """Each KEY=VALUE pair becomes a dict entry."""
        result = parse_set_params(("key1=val1", "key2=val2"))
        assert result == {"key1": "val1", "key2": "val2"}

    def test_value_with_equals(self):
        """Only the first "=" splits key from value; the rest stays in the value."""
        result = parse_set_params(("key=a=b=c",))
        assert result == {"key": "a=b=c"}

    def test_invalid_pair(self):
        """A pair without "=" raises click.BadParameter."""
        with pytest.raises(click.BadParameter, match="Expected KEY=VALUE"):
            parse_set_params(("no_equals_here",))

    def test_empty_value(self):
        """An entry `KEY=` maps the key to an empty string."""
        result = parse_set_params(("key=",))
        assert result == {"key": ""}


class TestFormatChecklist:
    """Rendering triggered jobs as a Markdown checklist."""

    def test_single_job(self):
        """A job renders as an unchecked item with its name, build number, description and build URL."""
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
        """A job without a description renders without empty parentheses."""
        jobs = [TriggeredJob(job_url="http://jenkins/job/test/", build_number=1, job_name="test-job")]
        md = format_checklist(jobs)
        assert "()" not in md

    def test_multiple_jobs(self):
        """Each job renders as its own checklist item."""
        jobs = [
            TriggeredJob(job_url="http://j/job/a/", build_number=1, job_name="a"),
            TriggeredJob(job_url="http://j/job/b/", build_number=2, job_name="b"),
        ]
        md = format_checklist(jobs)
        assert md.count("- [ ]") == 2


class TestMakeDryRunJob:
    """Building the placeholder TriggeredJob reported in dry-run mode."""

    @patch("utils.staging_trigger.trigger._get_jenkins_url", return_value="http://jenkins.example.com")
    def test_dry_run_job(self, _mock_url):
        """A dry-run job has build number 0 and a URL with "/job/" between the folder path parts."""
        job = _make_dry_run_job("staging/folder/test-job-test", "test-job-test", "desc")
        assert job.build_number == 0
        assert job.job_name == "test-job-test"
        assert job.description == "desc"
        assert "staging/job/folder/job/test-job-test" in job.job_url

    def test_dry_run_with_explicit_url(self):
        """A dry-run job uses the given Jenkins URL as its URL base."""
        job = _make_dry_run_job("folder/job-test", "job-test", jenkins_url="http://custom")
        assert job.job_url.startswith("http://custom")


# ---------------------------------------------------------------------------
# interactive helpers
# ---------------------------------------------------------------------------


class TestRuffFormat:
    """Formatting generated code with ruff."""

    def test_fallback_on_missing_ruff(self):
        """_ruff_format returns the code unchanged when ruff is missing."""
        code = "x=1\n"
        with patch("utils.staging_trigger.interactive.subprocess.run", side_effect=FileNotFoundError):
            assert _ruff_format(code) == code

    def test_formats_code(self):
        """_ruff_format returns code that keeps its statements."""
        code = "x=1\ny=2\n"
        result = _ruff_format(code)
        # Should either format or return original
        assert "x" in result
        assert "y" in result


class TestGeneratePythonCode:
    """Generating a StagingTrigger Python script from the selected jobs."""

    @patch("utils.staging_trigger.interactive._default_folder", return_value="scylla-staging/testuser")
    @patch("utils.staging_trigger.interactive._ruff_format", side_effect=lambda c: c)
    def test_basic_generation(self, _fmt, _folder):
        """The script builds a StagingTrigger from the job's preset and branch, selects the jobs and runs them."""
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
        """A dtest job generates a script that runs the dtest topology variants."""
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
        """Param overrides with empty values are left out of the script."""
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
        """With output_path, the script is written to that file."""
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
    """Collecting the Jenkins folders to create for a set of Jenkinsfiles."""

    def test_single_jenkinsfile_in_subdir(self, tmp_path):
        """A nested Jenkinsfile yields each of its parent folders, parents before children."""
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
        """A folder's name and description come from its _folder_definitions.yaml."""
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
        """A folder's display name comes from its stripped _display_name file."""
        pipelines_dir = tmp_path / "jenkins-pipelines"
        sub = pipelines_dir / "mycat"
        sub.mkdir(parents=True)
        (sub / "_display_name").write_text("  Fancy Name  ", encoding="utf-8")
        jf = sub / "test.jenkinsfile"
        jf.write_text("", encoding="utf-8")

        tree = _collect_folder_tree([jf], pipelines_dir)
        assert tree[0][1] == "Fancy Name"

    def test_empty_list(self, tmp_path):
        """No Jenkinsfiles yield no folders."""
        pipelines_dir = tmp_path / "jenkins-pipelines"
        pipelines_dir.mkdir()
        assert _collect_folder_tree([], pipelines_dir) == []

    def test_root_level_jenkinsfile(self, tmp_path):
        """A Jenkinsfile at the pipelines root yields no folders."""
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
    """Smoke tests of the staging_trigger CLI help and examples."""

    def test_help(self):
        """The top-level help lists the trigger, generate and run-config commands."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "trigger" in result.output
        assert "generate" in result.output
        assert "run-config" in result.output

    def test_trigger_help(self):
        """The trigger help lists the --branch and --dry-run options."""
        runner = CliRunner()
        result = runner.invoke(cli, ["trigger", "--help"])
        assert result.exit_code == 0
        assert "--branch" in result.output
        assert "--dry-run" in result.output

    def test_generate_help(self):
        """The generate help lists the --branch option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["generate", "--help"])
        assert result.exit_code == 0
        assert "--branch" in result.output

    def test_run_config_help(self):
        """The run-config help names the CONFIG_FILE argument."""
        runner = CliRunner()
        result = runner.invoke(cli, ["run-config", "--help"])
        assert result.exit_code == 0
        assert "CONFIG_FILE" in result.output

    def test_trigger_example_keeps_group_path(self, capsys):
        """The trigger example keeps the job's group path below the staging folder."""
        _show_trigger_examples(["scylla-staging/u/oss/artifacts/artifacts-ami-test"], folder="scylla-staging/u", pr=1)
        assert "-f scylla-staging/u trigger --pr 1 oss/artifacts/artifacts-ami-test\n" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# find_jenkinsfile
# ---------------------------------------------------------------------------


class TestFindJenkinsfile:
    """Locating a Jenkinsfile under jenkins-pipelines by path or stem."""

    @patch("utils.staging_trigger.trigger.get_sct_root_path")
    def test_find_by_full_path(self, mock_root, tmp_path):
        """A path relative to jenkins-pipelines, without extension, finds its Jenkinsfile."""
        pipelines = tmp_path / "jenkins-pipelines"
        jf = pipelines / "oss" / "longevity" / "longevity-100gb-4h.jenkinsfile"
        jf.parent.mkdir(parents=True)
        jf.write_text("longevityPipeline()", encoding="utf-8")
        mock_root.return_value = tmp_path

        result = find_jenkinsfile("oss/longevity/longevity-100gb-4h")
        assert result == jf

    @patch("utils.staging_trigger.trigger.get_sct_root_path")
    def test_find_by_stem_single_match(self, mock_root, tmp_path):
        """A unique file stem finds its Jenkinsfile anywhere under jenkins-pipelines."""
        pipelines = tmp_path / "jenkins-pipelines"
        jf = pipelines / "oss" / "unique-test.jenkinsfile"
        jf.parent.mkdir(parents=True)
        jf.write_text("longevityPipeline()", encoding="utf-8")
        mock_root.return_value = tmp_path

        result = find_jenkinsfile("unique-test")
        assert result == jf

    @patch("utils.staging_trigger.trigger.get_sct_root_path")
    def test_find_no_match(self, mock_root, tmp_path):
        """A name matching no Jenkinsfile returns None."""
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
    """StagingTrigger construction from presets and triggering of selected jobs."""

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_from_preset(self, _mock_user, mock_jenkins):
        """from_preset sets the git branch and repo and fills the preset's email with the username."""
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
        """from_preset with a folder uses it as the preset's folder prefix."""
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="my-branch", folder="custom-folder")
            assert trigger.preset.folder_prefix == "custom-folder"
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_select_jobs(self, _mock_user, mock_jenkins):
        """select_jobs stores the selected job names in order."""
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="master")
            trigger.select_jobs("job-a", "job-b")
            assert trigger.selected_jobs == ["job-a", "job-b"]
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_dry_run(self, _mock_user, mock_jenkins):
        """A dry run returns a build-0 job per selected job, leaving SCM and triggering untouched."""
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
        """A real run updates the job's SCM, triggers it, and returns its build number."""
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
        """run_multi in dry run returns one job per entry, in order."""
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
    def test_run_multi_with_3_tuple_descriptions(self, _mock_user, mock_jenkins):
        """3-tuple entries carry per-job descriptions into the checklist."""
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
            jobs_with_params = [
                ("job-a", {"extra": "val_a"}, "first job"),
                ("job-b", {"extra": "val_b"}, "second job"),
            ]
            results = trigger.run_multi(jobs_with_params, dry_run=True)
            assert len(results) == 2
            assert results[0].description == "first job"
            assert results[1].description == "second job"
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_multi_mixed_2_and_3_tuples(self, _mock_user, mock_jenkins):
        """Mixing 2-tuples and 3-tuples: 2-tuples get empty description."""
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
            jobs_with_params = [
                ("job-a", {"extra": "val_a"}),
                ("job-b", {"extra": "val_b"}, "has desc"),
                ("job-c", {}),
            ]
            results = trigger.run_multi(jobs_with_params, dry_run=True)
            assert len(results) == 3
            assert results[0].description == ""
            assert results[1].description == "has desc"
            assert results[2].description == ""
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_multi_duplicate_names_different_descriptions(self, _mock_user, mock_jenkins):
        """Same job name with different descriptions produces distinct checklist entries."""
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
            jobs_with_params = [
                ("artifacts-rocky9-test", {}, "repo install"),
                ("artifacts-rocky9-test", {"unified_package": "http://example.com/pkg"}, "unified pkg"),
                ("artifacts-rocky9-test", {"nonroot_offline_install": "true"}, "offline install"),
            ]
            results = trigger.run_multi(jobs_with_params, dry_run=True)
            assert len(results) == 3
            assert results[0].description == "repo install"
            assert results[1].description == "unified pkg"
            assert results[2].description == "offline install"
            checklist = format_checklist(results)
            assert "(repo install)" in checklist
            assert "(unified pkg)" in checklist
            assert "(offline install)" in checklist
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_dtest_variants_dry_run(self, _mock_user, mock_jenkins):
        """run_dtest_variants in dry run returns one job per topology, described by the topology name."""
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
    def test_run_dtest_variants_with_description_prefix(self, _mock_user, mock_jenkins):
        """description_prefix is combined with the topology key."""
        constants_mod._PRESETS = None
        try:
            trigger = StagingTrigger.from_preset("dtest", branch="master", folder="staging/testuser")
            results = trigger.run_dtest_variants(
                "dtest-pytest-gating",
                topologies=["no-tablets", "tablets"],
                dry_run=True,
                description_prefix="gating",
            )
            assert len(results) == 2
            assert results[0].description == "gating (no-tablets)"
            assert results[1].description == "gating (tablets)"
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
    """Running staging jobs from a YAML config file."""

    @patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_from_config_dry_run(self, _mock_user, mock_jenkins_cls, tmp_path):
        """A dry run of a config with one job returns a single build-0 job."""
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
        """A config without a branch or PR raises click.UsageError."""
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
        """A dtest job with dtest_topologies runs one job per topology."""
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
        """A dry run of a config job without a preset, whose name implies the manager preset, returns one job."""
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

    @patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_from_config_with_description(self, _mock_user, mock_jenkins_cls, tmp_path):
        """YAML description field propagates into the checklist."""
        constants_mod._PRESETS = None

        mock_client = MagicMock()
        mock_client.url = "http://jenkins.example.com"
        mock_jenkins_cls.return_value = mock_client

        config = {
            "branch": "master",
            "folder": "staging/testuser",
            "jobs": [
                {
                    "name": "longevity-100gb-4h-test",
                    "preset": "longevity",
                    "description": "4h longevity",
                },
            ],
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config), encoding="utf-8")

        try:
            results = run_from_config(str(config_path), dry_run=True)
            assert len(results) == 1
            assert results[0].description == "4h longevity"
        finally:
            constants_mod._PRESETS = None

    @patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_run_from_config_dtest_description_prefix(self, _mock_user, mock_jenkins_cls, tmp_path):
        """YAML description is used as prefix for dtest topology descriptions."""
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
                    "description": "gating",
                    "dtest_topologies": ["no-tablets", "tablets"],
                },
            ],
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config), encoding="utf-8")

        try:
            results = run_from_config(str(config_path), dry_run=True)
            assert len(results) == 2
            assert results[0].description == "gating (no-tablets)"
            assert results[1].description == "gating (tablets)"
        finally:
            constants_mod._PRESETS = None


# ---------------------------------------------------------------------------
# generate_yaml_config / _write_jobs_yaml_config
# ---------------------------------------------------------------------------


class TestGenerateYamlConfig:
    """Generating a run-config YAML from selected Jenkinsfiles."""

    @patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_generates_valid_yaml(self, _mock_user1, _mock_user2, tmp_path):
        """The YAML holds the branch, folder and a job with its detected preset, and is written to output_path."""
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
        """The YAML records the given fork repo."""
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
    """Generating a run-config YAML from job names."""

    @patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
    @patch("utils.staging_trigger.constants.get_username", return_value="testuser")
    def test_generates_yaml_from_job_names(self, _mock_user1, _mock_user2, tmp_path):
        """Each job name becomes a job entry with its detected preset, and the YAML is written to output_path."""
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
        """Param overrides go into the YAML's params section."""
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
    """ParamDefinition fields and defaults."""

    def test_basic_fields(self):
        """A ParamDefinition keeps its default and has no choices unless given."""
        p = ParamDefinition(default="val")
        assert p.default == "val"
        assert p.choices is None

    def test_with_choices(self):
        """A ParamDefinition keeps the given choices."""
        p = ParamDefinition(default="a", choices=["a", "b", "c"])
        assert p.choices == ["a", "b", "c"]


class TestParseParameterDefinitions:
    """Parsing Jenkins job config XML into ParamDefinitions."""

    def test_string_param(self):
        """A string parameter yields its default value and no choices."""
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
        """A choice parameter yields its choices and, without a defaultValue, the first choice as default."""
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
        """A choice parameter with a defaultValue keeps that default alongside its choices."""
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
        """String and choice parameters in one config are parsed side by side."""
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
        """A config without parameters yields an empty dict."""
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
    """Building the scylla_version suggestions for the interactive prompt."""

    @patch("utils.staging_trigger.interactive.get_latest_scylla_release", return_value="2025.1.3")
    def test_suggestions_with_release(self, _mock_release):
        """Suggestions include master:latest, the latest release and the custom value entry."""
        _fetch_latest_release.cache_clear()
        suggestions = _get_version_suggestions("master:latest")
        values = [c.value for c in suggestions]
        assert "master:latest" in values
        assert "2025.1.3" in values
        assert CUSTOM_VALUE_SENTINEL in values
        _fetch_latest_release.cache_clear()

    @patch("utils.staging_trigger.interactive.get_latest_scylla_release", side_effect=Exception("network error"))
    def test_suggestions_when_release_fetch_fails(self, _mock_release):
        """With the release lookup failing, suggestions hold master:latest, the current value and the custom entry."""
        _fetch_latest_release.cache_clear()
        suggestions = _get_version_suggestions("5.4.0")
        values = [c.value for c in suggestions]
        assert "master:latest" in values
        assert "5.4.0" in values  # current value preserved
        assert CUSTOM_VALUE_SENTINEL in values
        _fetch_latest_release.cache_clear()

    @patch("utils.staging_trigger.interactive.get_latest_scylla_release", return_value="2025.1.3")
    def test_no_duplicate_current_value(self, _mock_release):
        """A current value equal to a standard suggestion appears only once."""
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
    """Deriving scylla_docker_image from scylla_version in the interactive flow."""

    def test_sets_nightly_for_master_latest(self):
        """master:latest sets the scylla-nightly image."""
        params = {"scylla_version": "master:latest"}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla-nightly"

    def test_sets_release_repo_for_release_version(self):
        """A release version sets the scylladb/scylla image."""
        params = {"scylla_version": "2025.1.3"}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla"

    def test_skips_when_no_version(self):
        """An empty scylla_version leaves scylla_docker_image unset."""
        params = {"scylla_version": ""}
        maybe_set_docker_image(params)
        assert "scylla_docker_image" not in params

    def test_skips_when_backend_not_docker(self):
        """A non-docker backend leaves scylla_docker_image unset."""
        params = {"scylla_version": "master:latest", "backend": "aws"}
        maybe_set_docker_image(params)
        assert "scylla_docker_image" not in params

    def test_skips_when_already_set(self):
        """An already set scylla_docker_image is kept."""
        params = {"scylla_version": "master:latest", "scylla_docker_image": "my-custom-image"}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "my-custom-image"

    def test_allows_empty_backend(self):
        """An empty backend counts as docker and gets the image set."""
        params = {"scylla_version": "master:latest", "backend": ""}
        maybe_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla-nightly"

    def test_handles_invalid_version(self):
        """An unparsable scylla_version leaves scylla_docker_image unset."""
        params = {"scylla_version": "not-a-version"}
        maybe_set_docker_image(params)
        # Should not crash; image not set
        assert "scylla_docker_image" not in params


class TestAutoSetDockerImageTrigger:
    """Test _auto_set_docker_image from trigger module."""

    def test_sets_nightly_for_master_latest(self):
        """master:latest sets the scylla-nightly image."""
        params = {"scylla_version": "master:latest"}
        _auto_set_docker_image(params)
        assert params["scylla_docker_image"] == "scylladb/scylla-nightly"

    def test_skips_when_backend_not_docker(self):
        """A non-docker backend leaves scylla_docker_image unset."""
        params = {"scylla_version": "master:latest", "backend": "aws"}
        _auto_set_docker_image(params)
        assert "scylla_docker_image" not in params

    def test_skips_when_already_set(self):
        """An already set scylla_docker_image is kept."""
        params = {"scylla_version": "master:latest", "scylla_docker_image": "custom"}
        _auto_set_docker_image(params)
        assert params["scylla_docker_image"] == "custom"


JOB_CONFIG_XML = """<flow-definition><properties><hudson.model.ParametersDefinitionProperty>
<parameterDefinitions>
  <hudson.model.StringParameterDefinition><name>backend</name><defaultValue>aws</defaultValue></hudson.model.StringParameterDefinition>
  <hudson.model.StringParameterDefinition><name>scylla_version</name><defaultValue></defaultValue></hudson.model.StringParameterDefinition>
  <hudson.model.StringParameterDefinition><name>requested_by_user</name><defaultValue></defaultValue></hudson.model.StringParameterDefinition>
</parameterDefinitions>
</hudson.model.ParametersDefinitionProperty></properties></flow-definition>"""


@patch("utils.staging_trigger.jenkins_client.get_username", MagicMock(return_value="someone"))
class TestJenkinsJobTriggerTrigger:
    """trigger() sends only the parameters the job defines, and always a requested_by_user."""

    @staticmethod
    def _client(config_xml):
        client = JenkinsJobTrigger.__new__(JenkinsJobTrigger)
        client.jenkins = MagicMock()
        client.jenkins.get_job_config.return_value = config_xml
        client.jenkins.get_job_info.return_value = {"nextBuildNumber": 7}
        return client

    def test_drops_undefined_parameters(self):
        """Params the job does not define are dropped, and the next build number is returned."""
        client = self._client(JOB_CONFIG_XML)
        params = {"scylla_version": "master:latest", "scylla_docker_image": "scylladb/scylla-nightly"}

        assert client.trigger("folder/job", params) == 7

        client.jenkins.build_job.assert_called_once_with(
            name="folder/job", parameters={"scylla_version": "master:latest", "requested_by_user": "someone"}
        )

    def test_keeps_parameters_when_definitions_unknown(self):
        """When the job config cannot be read, all params are sent as given."""
        client = self._client(None)
        client.jenkins.get_job_config.side_effect = Exception("no config")
        params = {"scylla_version": "master:latest", "scylla_docker_image": "x"}

        client.trigger("folder/job", params)

        client.jenkins.build_job.assert_called_once_with(
            name="folder/job", parameters={**params, "requested_by_user": "someone"}
        )

    @pytest.mark.parametrize("params", [{}, {"requested_by_user": ""}], ids=["missing", "empty"])
    def test_fills_requested_by_user(self, params):
        """A missing or empty requested_by_user is filled with the current username."""
        client = self._client(JOB_CONFIG_XML)

        client.trigger("folder/job", {"backend": "aws", **params})

        client.jenkins.build_job.assert_called_once_with(
            name="folder/job", parameters={"backend": "aws", "requested_by_user": "someone"}
        )

    def test_keeps_explicit_requested_by_user(self):
        """An explicit requested_by_user is sent as given."""
        client = self._client(JOB_CONFIG_XML)

        client.trigger("folder/job", {"requested_by_user": "other"})

        client.jenkins.build_job.assert_called_once_with(name="folder/job", parameters={"requested_by_user": "other"})

    def test_drops_requested_by_user_when_job_lacks_it(self):
        """A job without a requested_by_user parameter gets none sent."""
        client = self._client(JOB_CONFIG_XML.replace("requested_by_user", "other_param"))

        client.trigger("folder/job", {"backend": "aws"})

        client.jenkins.build_job.assert_called_once_with(name="folder/job", parameters={"backend": "aws"})


def test_jenkins_job_trigger_uses_caller_credentials_from_env(monkeypatch):
    """JenkinsJobTrigger logs in with JENKINS_URL, JENKINS_USERNAME and JENKINS_API_TOKEN from the environment."""
    monkeypatch.setenv("JENKINS_URL", "https://jenkins.example.com/")
    monkeypatch.setenv("JENKINS_USERNAME", "alice")
    monkeypatch.setenv("JENKINS_API_TOKEN", "alice-token")
    with patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins") as jenkins_cls:
        client = JenkinsJobTrigger()

    jenkins_cls.assert_called_once_with("https://jenkins.example.com", username="alice", password="alice-token")
    assert client.url == "https://jenkins.example.com"


# `secret-tool search` prints the secret on stdout and the rest on stderr
KEYRING_SEARCH_OUTPUT = (
    "secret = keyring-token\n",
    "[/16]\nlabel = Jenkins API token\nattribute.user = bob@example.com\nattribute.service = jenkins.scylladb.com\n",
)

ONEPASSWORD_OUTPUT = (
    json.dumps(
        [
            {"id": "username", "label": "username", "value": "carol"},
            {"id": "credential", "label": "credential", "value": "op-token"},
        ]
    ),
    "[WARN] a warning that is not JSON\n",
)

NOTHING = ("", "")


def _fake_secret_stores(keyring=NOTHING, onepassword=NOTHING):
    def run(cmd):
        return {"secret-tool": keyring, "op": onepassword}[cmd[0]]

    return patch("utils.staging_trigger.jenkins_client._run_secret_command", side_effect=run)


@pytest.fixture()
def no_jenkins_env(monkeypatch):
    """Remove JENKINS_USERNAME and JENKINS_API_TOKEN from the environment."""
    monkeypatch.delenv("JENKINS_USERNAME", raising=False)
    monkeypatch.delenv("JENKINS_API_TOKEN", raising=False)


@pytest.fixture(autouse=True)
def fresh_jenkins_login(monkeypatch):
    """The login is cached per process; each test resolves its own, from all sources unless it limits them."""
    monkeypatch.delenv("JENKINS_CREDENTIALS_SOURCE", raising=False)
    _login.cache_clear()
    yield
    _login.cache_clear()


def test_jenkins_job_trigger_looks_up_credentials_once_per_process(capsys, no_jenkins_env):
    """Two JenkinsJobTriggers share one Jenkins login, looked up and verified once."""
    with (
        _fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT) as run,
        patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins") as jenkins_cls,
    ):
        first, second = JenkinsJobTrigger(), JenkinsJobTrigger()

    assert first.jenkins is second.jenkins
    run.assert_called_once()
    jenkins_cls.return_value.get_whoami.assert_called_once()
    assert capsys.readouterr().err.count("Jenkins: calling") == 1


def test_job_name_completion_keeps_credential_messages_off_the_prompt(capsys, monkeypatch):
    """Job name shell completion lists matching jobs and prints nothing to stderr."""
    monkeypatch.setenv("JENKINS_USERNAME", "alice")
    monkeypatch.setenv("JENKINS_API_TOKEN", "alice-token")
    ctx = MagicMock(parent=None)
    with patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins") as jenkins_cls:
        jenkins_cls.return_value.get_job_info.return_value = {"jobs": [{"name": "longevity-a"}, {"name": "other"}]}
        items = _JobNameType().shell_complete(ctx, None, "long")

    assert [(type(item), item.value) for item in items] == [(CompletionItem, "longevity-a")]
    assert capsys.readouterr().err == ""


@pytest.mark.usefixtures("no_jenkins_env")
@pytest.mark.parametrize(
    ("keyring", "onepassword", "expected"),
    [
        pytest.param(
            KEYRING_SEARCH_OUTPUT,
            ONEPASSWORD_OUTPUT,
            [("carol", "op-token", "1Password"), ("bob@example.com", "keyring-token", "OS keyring")],
            id="1password-then-keyring",
        ),
        pytest.param(NOTHING, ONEPASSWORD_OUTPUT, [("carol", "op-token", "1Password")], id="1password-only"),
        pytest.param(
            KEYRING_SEARCH_OUTPUT, NOTHING, [("bob@example.com", "keyring-token", "OS keyring")], id="keyring-only"
        ),
        pytest.param(
            NOTHING,
            (json.dumps({"id": "credential", "label": "credential", "value": "op-token"}), ""),
            [],
            id="1password-token-without-user-is-skipped",
        ),
        pytest.param(NOTHING, ("not json", ""), [], id="1password-garbage"),
        pytest.param(NOTHING, NOTHING, [], id="nothing-stored"),
    ],
)
def test_iter_jenkins_credentials_reads_secret_stores(keyring, onepassword, expected):
    """iter_jenkins_credentials yields 1Password then keyring logins, skipping entries without a user or valid JSON."""
    with _fake_secret_stores(keyring, onepassword):
        assert list(iter_jenkins_credentials()) == expected


def test_iter_jenkins_credentials_env_comes_first_and_overrides_stored_user(monkeypatch):
    """Environment credentials come first, and JENKINS_USERNAME replaces the user stored in the keyring."""
    monkeypatch.setenv("JENKINS_USERNAME", "alice")
    monkeypatch.setenv("JENKINS_API_TOKEN", "alice-token")
    with _fake_secret_stores(KEYRING_SEARCH_OUTPUT):
        assert list(iter_jenkins_credentials()) == [
            ("alice", "alice-token", "environment"),
            ("alice", "keyring-token", "OS keyring"),
        ]


def test_iter_jenkins_credentials_skips_keyring_when_1password_is_enough(no_jenkins_env):
    """A full 1Password login comes first, found with a single secret-store call."""
    with _fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT) as run:
        assert next(iter_jenkins_credentials())[2] == "1Password"

    run.assert_called_once()


@pytest.mark.parametrize("status", [401, 403])
def test_jenkins_job_trigger_falls_back_when_a_store_is_rejected(capsys, no_jenkins_env, status):
    """A login rejected with 401 or 403 is reported, and the next credential source is tried."""

    def make_client(url, username, password):
        client = MagicMock()
        if password == "op-token":
            # python-jenkins' message for a 401 or 403
            client.get_whoami.side_effect = jenkins.JenkinsException(
                f"Error in request. Possibly authentication failed [{status}]: Forbidden"
            )
        return client

    with (
        _fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT),
        patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins", side_effect=make_client),
    ):
        JenkinsJobTrigger()

    err = capsys.readouterr().err
    assert "carol from 1Password was rejected" in err
    assert "as bob@example.com, token from OS keyring" in err


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(jenkins.TimeoutException("Error in request: read timed out"), id="timeout"),
        pytest.param(jenkins.JenkinsException("Possibly authentication failed [500]: Server Error"), id="server-error"),
        # get_whoami turns a 503 (or any status python-jenkins doesn't map) into BadHTTPException
        pytest.param(
            jenkins.BadHTTPException("Error communicating with server[https://jenkins.example.com/]"), id="503"
        ),
    ],
)
def test_jenkins_job_trigger_stops_when_jenkins_fails_for_another_reason(capsys, no_jenkins_env, error):
    """A Jenkins timeout or server error raises ClickException naming the whoAmI failure; other logins are skipped."""
    with (
        _fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT) as run,
        patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins") as jenkins_cls,
        pytest.raises(click.ClickException, match=f"did not answer whoAmI: {re.escape(str(error))}"),
    ):
        jenkins_cls.return_value.get_whoami.side_effect = error
        JenkinsJobTrigger()

    run.assert_called_once()  # the keyring is not asked after 1Password's token hit a Jenkins failure
    assert "was rejected" not in capsys.readouterr().err


LATTE_PRELOAD_JENKINSFILE = "cassandra-preload-1tb-latte"  # jenkins-pipelines/oss/longevity/
LATTE_PRELOAD_JOB = "scylla-staging/alice/oss/longevity/cassandra-preload-1tb-latte-test"


@pytest.fixture()
def fake_job_creation():
    """Fake the caller's Jenkins login and JenkinsPipelines; yield (login client, JenkinsPipelines class)."""
    with (
        patch("utils.staging_trigger.job_generation.JenkinsJobTrigger") as trigger_cls,
        patch("utils.build_system.create_test_release_jobs.JenkinsPipelines") as pipelines_cls,
    ):
        pipelines_cls.return_value.jenkins.job_exists.return_value = True
        yield trigger_cls.return_value.jenkins, pipelines_cls


def _generate_one(how):
    if how == "generate_job":
        return [generate_job(LATTE_PRELOAD_JENKINSFILE, folder="scylla-staging/alice")]
    return generate_jobs([find_jenkinsfile(LATTE_PRELOAD_JENKINSFILE)], folder="scylla-staging/alice")


@pytest.mark.parametrize("how", ["generate_job", "generate_jobs"])
def test_generate_creates_jobs_with_the_callers_login(fake_job_creation, how):
    """generate_job and generate_jobs create the job through the caller's Jenkins login and verify it exists."""
    login, pipelines_cls = fake_job_creation

    assert _generate_one(how) == [LATTE_PRELOAD_JOB]

    assert pipelines_cls.call_args.kwargs["client"] is login
    pipelines_cls.return_value.jenkins.job_exists.assert_any_call(LATTE_PRELOAD_JOB)


@pytest.mark.parametrize("how", ["generate_job", "generate_jobs"])
def test_generate_stops_when_jenkins_did_not_create_the_job(capsys, fake_job_creation, how):
    """Job generation raises ClickException naming the jobs Jenkins did not create, without reporting success."""
    _, pipelines_cls = fake_job_creation
    # JenkinsPipelines logs a refused create (e.g. 403) and returns; the job just isn't there
    pipelines_cls.return_value.jenkins.job_exists.return_value = False

    with pytest.raises(click.ClickException, match=f"did not create 1 job\\(s\\): {LATTE_PRELOAD_JOB}"):
        _generate_one(how)

    assert "Successfully created" not in capsys.readouterr().out


@pytest.mark.usefixtures("no_jenkins_env")
@pytest.mark.parametrize(
    ("keyring", "onepassword"),
    [
        pytest.param(NOTHING, NOTHING, id="nothing-stored"),
        pytest.param(KEYRING_SEARCH_OUTPUT, NOTHING, id="all-rejected"),
    ],
)
def test_jenkins_job_trigger_refuses_to_run_without_working_credentials(keyring, onepassword):
    """With no stored credentials or only rejected ones, JenkinsJobTrigger raises ClickException naming the env vars."""
    rejected = MagicMock()
    rejected.get_whoami.side_effect = jenkins.JenkinsException("Possibly authentication failed [401]")
    with (
        _fake_secret_stores(keyring, onepassword),
        patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins", return_value=rejected),
        pytest.raises(click.ClickException, match="JENKINS_USERNAME and JENKINS_API_TOKEN"),
    ):
        JenkinsJobTrigger()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(
            subprocess.CalledProcessError(1, ["op"], stderr="[ERROR] 2026/11/18 account is locked\n"),
            "Jenkins: op failed: [ERROR] 2026/11/18 account is locked",
            id="cli-error",
        ),
        pytest.param(subprocess.TimeoutExpired(["op"], 60), "Jenkins: op failed: Command", id="timeout"),
        pytest.param(FileNotFoundError("op"), "", id="cli-not-installed-is-quiet"),
    ],
)
def test_run_secret_command_says_why_a_store_failed(capsys, error, expected):
    """_run_secret_command returns empty output and prints why the store failed; a missing CLI prints nothing."""
    with patch("utils.staging_trigger.jenkins_client.subprocess.run", side_effect=error):
        assert _run_secret_command(["op", "item", "get"]) == ("", "")

    err = capsys.readouterr().err
    assert err.startswith(expected) if expected else err == ""


@pytest.mark.usefixtures("no_jenkins_env")
@pytest.mark.parametrize(
    ("wanted", "expected"),
    [
        pytest.param("1password", [("carol", "op-token", "1Password")], id="1password"),
        pytest.param("KEYRING", [("bob@example.com", "keyring-token", "OS keyring")], id="keyring-any-case"),
        pytest.param("env", [], id="env-not-set"),
    ],
)
def test_jenkins_credentials_source_limits_lookup_to_one_source(monkeypatch, wanted, expected):
    """JENKINS_CREDENTIALS_SOURCE limits the lookup to the named source, case-insensitively."""
    monkeypatch.setenv("JENKINS_CREDENTIALS_SOURCE", wanted)
    with _fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT):
        assert list(iter_jenkins_credentials()) == expected


def test_jenkins_credentials_source_rejects_unknown_value(monkeypatch):
    """An unknown JENKINS_CREDENTIALS_SOURCE raises ClickException listing the valid sources."""
    monkeypatch.setenv("JENKINS_CREDENTIALS_SOURCE", "vault")
    with pytest.raises(click.ClickException, match="not one of: env, 1password, keyring"):
        list(iter_jenkins_credentials())
