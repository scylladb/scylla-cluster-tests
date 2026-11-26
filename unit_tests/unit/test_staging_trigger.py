"""Unit tests for utils.staging_trigger package."""

import json
import re
import subprocess
from unittest.mock import MagicMock, call, patch

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
    SCT_REPO,
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
    ONEPASSWORD_ITEM,
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
# fixtures and data
# ---------------------------------------------------------------------------

JOB_CONFIG_XML = """<flow-definition><properties><hudson.model.ParametersDefinitionProperty>
<parameterDefinitions>
  <hudson.model.StringParameterDefinition><name>backend</name><defaultValue>aws</defaultValue></hudson.model.StringParameterDefinition>
  <hudson.model.StringParameterDefinition><name>scylla_version</name><defaultValue></defaultValue></hudson.model.StringParameterDefinition>
  <hudson.model.StringParameterDefinition><name>requested_by_user</name><defaultValue></defaultValue></hudson.model.StringParameterDefinition>
</parameterDefinitions>
</hudson.model.ParametersDefinitionProperty></properties></flow-definition>"""

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

ONEPASSWORD_COMMAND = [
    "op",
    "item",
    "get",
    ONEPASSWORD_ITEM,
    "--fields",
    "label=username,label=credential",
    "--reveal",
    "--format",
    "json",
]

LATTE_PRELOAD_JENKINSFILE = "cassandra-preload-1tb-latte"  # jenkins-pipelines/oss/longevity/
LATTE_PRELOAD_JOB = "scylla-staging/alice/oss/longevity/cassandra-preload-1tb-latte-test"


@pytest.fixture()
def mock_jenkins():
    """Create a mocked JenkinsJobTrigger for StagingTrigger tests."""
    with patch("utils.staging_trigger.trigger.JenkinsJobTrigger") as mock_cls:
        mock_client = MagicMock()
        mock_client.url = "http://jenkins.example.com"
        mock_cls.return_value = mock_client
        yield mock_client


@pytest.fixture()
def fresh_presets(monkeypatch):
    """Clear the cached presets so each test rebuilds them; pytest restores the previous cache afterwards."""
    monkeypatch.setattr(constants_mod, "_PRESETS", None)


@pytest.fixture()
def make_client():
    """Factory for a JenkinsJobTrigger with a mocked Jenkins API; the current user is 'someone'."""
    with patch("utils.staging_trigger.jenkins_client.get_username", return_value="someone"):

        def build(config_xml):
            """Build a client whose get_job_config returns config_xml and next build number is 7."""
            client = JenkinsJobTrigger.__new__(JenkinsJobTrigger)
            client.jenkins = MagicMock()
            client.jenkins.get_job_config.return_value = config_xml
            client.jenkins.get_job_info.return_value = {"nextBuildNumber": 7}
            return client

        yield build


@pytest.fixture()
def no_jenkins_env(monkeypatch):
    """Remove the caller's Jenkins credentials from the environment."""
    monkeypatch.delenv("JENKINS_USERNAME", raising=False)
    monkeypatch.delenv("JENKINS_API_TOKEN", raising=False)


@pytest.fixture(autouse=True)
def fresh_jenkins_login(monkeypatch):
    """The login is cached per process; each test resolves its own, from all sources unless it limits them."""
    monkeypatch.delenv("JENKINS_CREDENTIALS_SOURCE", raising=False)
    _login.cache_clear()
    yield
    _login.cache_clear()


@pytest.fixture()
def fake_job_creation():
    """Fake the caller's Jenkins login and JenkinsPipelines; yield (login client, JenkinsPipelines class)."""
    with (
        patch("utils.staging_trigger.job_generation.JenkinsJobTrigger") as trigger_cls,
        patch("utils.build_system.create_test_release_jobs.JenkinsPipelines") as pipelines_cls,
    ):
        pipelines_cls.return_value.jenkins.job_exists.return_value = True
        yield trigger_cls.return_value.jenkins, pipelines_cls


def fake_secret_stores(keyring=NOTHING, onepassword=NOTHING):
    """Patch _run_secret_command to answer `secret-tool` with keyring and `op` with onepassword; return the patcher."""

    def run(cmd):
        return {"secret-tool": keyring, "op": onepassword}[cmd[0]]

    return patch("utils.staging_trigger.jenkins_client._run_secret_command", side_effect=run)


def generate_one(how):
    """Create the latte preload job through generate_job or generate_jobs; return the created job names."""
    if how == "generate_job":
        return [generate_job(LATTE_PRELOAD_JENKINSFILE, folder="scylla-staging/alice")]
    return generate_jobs([find_jenkinsfile(LATTE_PRELOAD_JENKINSFILE)], folder="scylla-staging/alice")


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------


def test_preset_fields():
    """Preset stores the params and folder prefix it was built with."""
    p = Preset(params={"key": "val"}, folder_prefix="test-folder")
    assert p.params == {"key": "val"}
    assert p.folder_prefix == "test-folder"


def test_preset_default_folder():
    """Preset without a folder defaults to scylla-staging/<username>."""
    with patch("utils.staging_trigger.constants.get_username", return_value="testuser"):
        p = Preset(params={})
        assert p.folder_prefix == "scylla-staging/testuser"


def test_fields():
    """TriggeredJob exposes the fields it was built with."""
    job = TriggeredJob(job_url="http://j/job/x/", build_number=42, job_name="x-test", description="desc")
    assert job.job_url == "http://j/job/x/"
    assert job.build_number == 42
    assert job.job_name == "x-test"
    assert job.description == "desc"


def test_default_description():
    """TriggeredJob description defaults to an empty string."""
    job = TriggeredJob(job_url="http://j/job/x/", build_number=1, job_name="x")
    assert job.description == ""


@patch("utils.staging_trigger.constants.get_username", return_value="alice")
def test_presets_contain_expected_keys(_mock_user, fresh_presets):
    """get_presets returns a Preset with a params dict for every preset name."""
    presets = get_presets()
    assert set(presets.keys()) == set(PRESET_NAMES)
    for name in PRESET_NAMES:
        assert isinstance(presets[name], Preset)
        assert isinstance(presets[name].params, dict)


@patch("utils.staging_trigger.constants.get_username", return_value="alice")
def test_preset_email_contains_username(_mock_user, fresh_presets):
    """Preset email recipients are built from the current username."""
    presets = get_presets()
    for name in ["longevity", "manager", "artifacts", "perf"]:
        assert presets[name].params["email_recipients"] == "alice@scylladb.com"


def test_known_pipeline_functions():
    """PIPELINE_TO_PRESET maps Jenkinsfile pipeline functions to preset names."""
    assert PIPELINE_TO_PRESET["longevityPipeline"] == "longevity"
    assert PIPELINE_TO_PRESET["managerPipeline"] == "manager"
    assert PIPELINE_TO_PRESET["artifactsPipeline"] == "artifacts"
    assert PIPELINE_TO_PRESET["perfRegressionParallelPipeline"] == "perf"


def test_dtest_topology_flags():
    """DTEST_TOPOLOGY_FLAGS lists the known dtest topology flags."""
    assert "no-tablets" in DTEST_TOPOLOGY_FLAGS
    assert "tablets" in DTEST_TOPOLOGY_FLAGS
    assert "gossip" in DTEST_TOPOLOGY_FLAGS


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------


def test_load_missing_file(tmp_path):
    """_load_cache returns an empty dict when the cache file is absent."""
    with patch("utils.staging_trigger.cache._CACHE_FILE", tmp_path / "missing.json"):
        assert _load_cache() == {}


def test_save_and_load(tmp_path):
    """_save_cache writes data that _load_cache reads back."""
    cache_file = tmp_path / "cache.json"
    with patch("utils.staging_trigger.cache._CACHE_FILE", cache_file):
        _save_cache({"last_branch": "my-branch"})
        assert cache_file.exists()
        data = _load_cache()
        assert data["last_branch"] == "my-branch"


def test_load_corrupt_json(tmp_path):
    """_load_cache returns an empty dict when the cache file is not valid JSON."""
    cache_file = tmp_path / "cache.json"
    cache_file.write_text("not json{{{", encoding="utf-8")
    with patch("utils.staging_trigger.cache._CACHE_FILE", cache_file):
        assert _load_cache() == {}


@patch("utils.staging_trigger.cache.subprocess.run")
def test_lookup_pr_success(mock_run):
    """lookup_pr builds the fork SSH URL and branch from the gh CLI output."""
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
    mock_run.assert_called_once_with(
        [
            "gh",
            "pr",
            "view",
            "123",
            "--repo",
            "scylladb/scylla-cluster-tests",
            "--json",
            "headRefName,headRepository,headRepositoryOwner",
        ],
        capture_output=True,
        text=True,
        check=True,
    )


# ---------------------------------------------------------------------------
# trigger helpers
# ---------------------------------------------------------------------------


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
def test_detect(name, expected):
    """detect_preset_from_job_name maps job names to presets, and None when unknown."""
    assert detect_preset_from_job_name(name) == expected


def test_longevity_pipeline(tmp_path):
    """A Jenkinsfile calling longevityPipeline maps to the longevity preset."""
    jf = tmp_path / "test.jenkinsfile"
    jf.write_text("longevityPipeline(\n  params: ...\n)", encoding="utf-8")
    assert detect_preset_from_jenkinsfile(jf) == "longevity"


def test_manager_pipeline(tmp_path):
    """A Jenkinsfile calling managerPipeline maps to the manager preset."""
    jf = tmp_path / "test.jenkinsfile"
    jf.write_text("managerPipeline(\n  params: ...\n)", encoding="utf-8")
    assert detect_preset_from_jenkinsfile(jf) == "manager"


def test_unknown_pipeline(tmp_path):
    """A Jenkinsfile with an unknown pipeline function maps to None."""
    jf = tmp_path / "test.jenkinsfile"
    jf.write_text("unknownPipeline(\n  params: ...\n)", encoding="utf-8")
    assert detect_preset_from_jenkinsfile(jf) is None


def test_valid_pairs():
    """parse_set_params turns KEY=VALUE pairs into a dict."""
    result = parse_set_params(("key1=val1", "key2=val2"))
    assert result == {"key1": "val1", "key2": "val2"}


def test_value_with_equals():
    """parse_set_params splits on the first '=' only."""
    result = parse_set_params(("key=a=b=c",))
    assert result == {"key": "a=b=c"}


def test_invalid_pair():
    """parse_set_params rejects a pair without '='."""
    with pytest.raises(click.BadParameter, match="Expected KEY=VALUE"):
        parse_set_params(("no_equals_here",))


def test_empty_value():
    """parse_set_params keeps a key with an empty value."""
    result = parse_set_params(("key=",))
    assert result == {"key": ""}


def test_single_job():
    """format_checklist renders one unchecked line with name, build number, description and URL."""
    jobs = [
        TriggeredJob(job_url="http://jenkins/job/test/", build_number=42, job_name="test-job", description="longevity")
    ]
    md = format_checklist(jobs)
    assert "- [ ]" in md
    assert "test-job #42" in md
    assert "(longevity)" in md
    assert "http://jenkins/job/test/42/" in md


def test_no_description():
    """format_checklist omits the empty description parentheses."""
    jobs = [TriggeredJob(job_url="http://jenkins/job/test/", build_number=1, job_name="test-job")]
    md = format_checklist(jobs)
    assert "()" not in md


def test_multiple_jobs():
    """format_checklist renders one unchecked line per job."""
    jobs = [
        TriggeredJob(job_url="http://j/job/a/", build_number=1, job_name="a"),
        TriggeredJob(job_url="http://j/job/b/", build_number=2, job_name="b"),
    ]
    md = format_checklist(jobs)
    assert md.count("- [ ]") == 2


@patch("utils.staging_trigger.trigger._get_jenkins_url", return_value="http://jenkins.example.com")
def test_dry_run_job(_mock_url):
    """_make_dry_run_job builds a build-0 job URL under the Jenkins host."""
    job = _make_dry_run_job("staging/folder/test-job-test", "test-job-test", "desc")
    assert job.build_number == 0
    assert job.job_name == "test-job-test"
    assert job.description == "desc"
    assert "staging/job/folder/job/test-job-test" in job.job_url


def test_dry_run_with_explicit_url():
    """_make_dry_run_job uses the jenkins_url argument when it is given."""
    job = _make_dry_run_job("folder/job-test", "job-test", jenkins_url="http://custom")
    assert job.job_url.startswith("http://custom")


# ---------------------------------------------------------------------------
# interactive helpers
# ---------------------------------------------------------------------------


def test_fallback_on_missing_ruff():
    """_ruff_format returns the code unchanged when ruff is not installed."""
    code = "x=1\n"
    with patch("utils.staging_trigger.interactive.subprocess.run", side_effect=FileNotFoundError):
        assert _ruff_format(code) == code


def test_formats_code():
    """_ruff_format returns code that still contains the assigned names."""
    code = "x=1\ny=2\n"
    result = _ruff_format(code)
    # Should either format or return original
    assert "x" in result
    assert "y" in result


@patch("utils.staging_trigger.interactive._default_folder", return_value="scylla-staging/testuser")
@patch("utils.staging_trigger.interactive._ruff_format", side_effect=lambda c: c)
def test_basic_generation(_fmt, _folder):
    """generate_python_code emits a StagingTrigger script that selects the preset jobs and runs them."""
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
def test_dtest_generation(_fmt, _folder):
    """generate_python_code emits run_dtest_variants for dtest jobs."""
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
def test_empty_params_excluded(_fmt, _folder):
    """generate_python_code drops parameter overrides with empty values."""
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
def test_output_to_file(_fmt, _folder, tmp_path):
    """generate_python_code writes the generated script to output_path."""
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


def test_single_jenkinsfile_in_subdir(tmp_path):
    """_collect_folder_tree lists each parent folder before its child."""
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


def test_reads_folder_definitions(tmp_path):
    """_collect_folder_tree reads the display name and description from _folder_definitions.yaml."""
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


def test_reads_display_name_file(tmp_path):
    """_collect_folder_tree reads the display name from the _display_name file, stripped."""
    pipelines_dir = tmp_path / "jenkins-pipelines"
    sub = pipelines_dir / "mycat"
    sub.mkdir(parents=True)
    (sub / "_display_name").write_text("  Fancy Name  ", encoding="utf-8")
    jf = sub / "test.jenkinsfile"
    jf.write_text("", encoding="utf-8")

    tree = _collect_folder_tree([jf], pipelines_dir)
    assert tree[0][1] == "Fancy Name"


def test_empty_list(tmp_path):
    """_collect_folder_tree returns an empty tree for an empty Jenkinsfile list."""
    pipelines_dir = tmp_path / "jenkins-pipelines"
    pipelines_dir.mkdir()
    assert _collect_folder_tree([], pipelines_dir) == []


def test_root_level_jenkinsfile(tmp_path):
    """A Jenkinsfile at the pipelines root adds no folder entries."""
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


def test_help():
    """The cli root help lists the trigger, generate and run-config commands."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "trigger" in result.output
    assert "generate" in result.output
    assert "run-config" in result.output


def test_trigger_help():
    """The trigger subcommand help documents --branch and --dry-run."""
    runner = CliRunner()
    result = runner.invoke(cli, ["trigger", "--help"])
    assert result.exit_code == 0
    assert "--branch" in result.output
    assert "--dry-run" in result.output


def test_generate_help():
    """The generate subcommand help documents --branch."""
    runner = CliRunner()
    result = runner.invoke(cli, ["generate", "--help"])
    assert result.exit_code == 0
    assert "--branch" in result.output


def test_run_config_help():
    """The run-config subcommand help names CONFIG_FILE."""
    runner = CliRunner()
    result = runner.invoke(cli, ["run-config", "--help"])
    assert result.exit_code == 0
    assert "CONFIG_FILE" in result.output


def test_trigger_example_keeps_group_path(capsys):
    """_show_trigger_examples keeps the job group path in the suggested command."""
    _show_trigger_examples(["scylla-staging/u/oss/artifacts/artifacts-ami-test"], folder="scylla-staging/u", pr=1)
    assert "-f scylla-staging/u trigger --pr 1 oss/artifacts/artifacts-ami-test\n" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# find_jenkinsfile
# ---------------------------------------------------------------------------


@patch("utils.staging_trigger.trigger.get_sct_root_path")
def test_find_by_full_path(mock_root, tmp_path):
    """find_jenkinsfile resolves a full group/name path under jenkins-pipelines."""
    pipelines = tmp_path / "jenkins-pipelines"
    jf = pipelines / "oss" / "longevity" / "longevity-100gb-4h.jenkinsfile"
    jf.parent.mkdir(parents=True)
    jf.write_text("longevityPipeline()", encoding="utf-8")
    mock_root.return_value = tmp_path

    result = find_jenkinsfile("oss/longevity/longevity-100gb-4h")
    assert result == jf


@patch("utils.staging_trigger.trigger.get_sct_root_path")
def test_find_by_stem_single_match(mock_root, tmp_path):
    """find_jenkinsfile resolves a bare file stem when exactly one Jenkinsfile matches."""
    pipelines = tmp_path / "jenkins-pipelines"
    jf = pipelines / "oss" / "unique-test.jenkinsfile"
    jf.parent.mkdir(parents=True)
    jf.write_text("longevityPipeline()", encoding="utf-8")
    mock_root.return_value = tmp_path

    result = find_jenkinsfile("unique-test")
    assert result == jf


@patch("utils.staging_trigger.trigger.get_sct_root_path")
def test_find_no_match(mock_root, tmp_path):
    """find_jenkinsfile returns None when no Jenkinsfile matches."""
    pipelines = tmp_path / "jenkins-pipelines"
    pipelines.mkdir(parents=True)
    mock_root.return_value = tmp_path

    result = find_jenkinsfile("nonexistent")
    assert result is None


# ---------------------------------------------------------------------------
# StagingTrigger
# ---------------------------------------------------------------------------


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_from_preset(_mock_user, mock_jenkins, fresh_presets):
    """from_preset applies the branch, repo and username-based email from the preset."""
    trigger = StagingTrigger.from_preset("longevity", branch="my-branch", repo="git@github.com:user/repo.git")
    assert trigger.git_branch == "my-branch"
    assert trigger.git_repo == "git@github.com:user/repo.git"
    assert trigger.preset.params["email_recipients"] == "testuser@scylladb.com"


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_from_preset_with_folder_override(_mock_user, mock_jenkins, fresh_presets):
    """from_preset uses an explicit folder instead of the preset default."""
    trigger = StagingTrigger.from_preset("longevity", branch="my-branch", folder="custom-folder")
    assert trigger.preset.folder_prefix == "custom-folder"


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_select_jobs(_mock_user, mock_jenkins, fresh_presets):
    """select_jobs stores the short job names in order."""
    trigger = StagingTrigger.from_preset("longevity", branch="master")
    trigger.select_jobs("job-a", "job-b")
    assert trigger.selected_jobs == ["job-a", "job-b"]


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_dry_run(_mock_user, mock_jenkins, fresh_presets):
    """run(dry_run=True) reports the job without calling Jenkins trigger or update_scm."""
    trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
    trigger.select_jobs("longevity-100gb-4h-test")
    results = trigger.run(dry_run=True)
    assert len(results) == 1
    assert results[0].build_number == 0
    assert results[0].job_name == "longevity-100gb-4h-test"
    # Dry run should NOT call trigger or update_scm
    mock_jenkins.trigger.assert_not_called()
    mock_jenkins.update_scm.assert_not_called()


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_triggers_jobs(_mock_user, mock_jenkins, fresh_presets):
    """run() updates SCM and triggers each selected job, returning its build number."""
    mock_jenkins.get_last_build_params.return_value = {"old_param": "old_val"}
    mock_jenkins.get_job_url.return_value = "http://jenkins/job/test/"
    mock_jenkins.trigger.return_value = 42
    trigger = StagingTrigger.from_preset("longevity", branch="my-branch", folder="staging/testuser")
    trigger.select_jobs("my-job-test")
    results = trigger.run(dry_run=False)
    assert len(results) == 1
    assert results[0].build_number == 42
    assert results[0].job_name == "my-job-test"
    mock_jenkins.update_scm.assert_called_once_with(
        "staging/testuser/my-job-test", git_repo=trigger.git_repo, git_branch="my-branch"
    )
    mock_jenkins.trigger.assert_called_once_with(
        "staging/testuser/my-job-test",
        {"old_param": "old_val", **trigger.preset.params, "scylla_docker_image": "scylladb/scylla-nightly"},
    )


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_multi_dry_run(_mock_user, mock_jenkins, fresh_presets):
    """run_multi returns one dry-run job per (name, params) entry, in order."""
    trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
    jobs_with_params = [
        ("job-a", {"extra": "val_a"}),
        ("job-b", {"extra": "val_b"}),
    ]
    results = trigger.run_multi(jobs_with_params, dry_run=True)
    assert len(results) == 2
    assert results[0].job_name == "job-a"
    assert results[1].job_name == "job-b"


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_multi_with_3_tuple_descriptions(_mock_user, mock_jenkins, fresh_presets):
    """3-tuple entries carry per-job descriptions into the checklist."""
    trigger = StagingTrigger.from_preset("longevity", branch="master", folder="staging/testuser")
    jobs_with_params = [
        ("job-a", {"extra": "val_a"}, "first job"),
        ("job-b", {"extra": "val_b"}, "second job"),
    ]
    results = trigger.run_multi(jobs_with_params, dry_run=True)
    assert len(results) == 2
    assert results[0].description == "first job"
    assert results[1].description == "second job"


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_multi_mixed_2_and_3_tuples(_mock_user, mock_jenkins, fresh_presets):
    """Mixing 2-tuples and 3-tuples: 2-tuples get empty description."""
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


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_multi_duplicate_names_different_descriptions(_mock_user, mock_jenkins, fresh_presets):
    """Same job name with different descriptions produces distinct checklist entries."""
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


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_dtest_variants_dry_run(_mock_user, mock_jenkins, fresh_presets):
    """run_dtest_variants makes one dry-run job per topology, described by topology name."""
    trigger = StagingTrigger.from_preset("dtest", branch="master", folder="staging/testuser")
    results = trigger.run_dtest_variants("dtest-pytest-gating", topologies=["no-tablets", "tablets"], dry_run=True)
    assert len(results) == 2
    assert results[0].description == "no-tablets"
    assert results[1].description == "tablets"


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_dtest_variants_with_description_prefix(_mock_user, mock_jenkins, fresh_presets):
    """description_prefix is combined with the topology key."""
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


@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_param_override_precedence(_mock_user, mock_jenkins, fresh_presets):
    """Verify that param_overrides override preset params."""
    trigger = StagingTrigger.from_preset(
        "longevity", branch="master", folder="staging/testuser", param_overrides={"scylla_version": "2025.1.0"}
    )
    assert trigger.param_overrides["scylla_version"] == "2025.1.0"


# ---------------------------------------------------------------------------
# run_from_config
# ---------------------------------------------------------------------------


@patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_from_config_dry_run(_mock_user, mock_jenkins_cls, tmp_path, fresh_presets):
    """run_from_config dry-runs each job listed in the YAML config."""
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

    results = run_from_config(str(config_path), dry_run=True)
    assert len(results) == 1
    assert results[0].build_number == 0


@patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_from_config_missing_branch_raises(_mock_user, mock_jenkins_cls, tmp_path, fresh_presets):
    """run_from_config raises UsageError when the config has neither branch nor pr."""
    config = {"jobs": [{"name": "test-job"}]}
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config), encoding="utf-8")

    with pytest.raises(click.UsageError, match="branch.*or.*pr"):
        run_from_config(str(config_path))


@patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_from_config_dtest_topologies(_mock_user, mock_jenkins_cls, tmp_path, fresh_presets):
    """run_from_config expands dtest_topologies into one job per topology."""
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

    results = run_from_config(str(config_path), dry_run=True)
    assert len(results) == 2


@patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_from_config_auto_detects_preset(_mock_user, mock_jenkins_cls, tmp_path, fresh_presets):
    """run_from_config detects the preset from the job name when none is given."""
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

    results = run_from_config(str(config_path), dry_run=True)
    assert len(results) == 1


@patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_from_config_with_description(_mock_user, mock_jenkins_cls, tmp_path, fresh_presets):
    """YAML description field propagates into the checklist."""
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

    results = run_from_config(str(config_path), dry_run=True)
    assert len(results) == 1
    assert results[0].description == "4h longevity"


@patch("utils.staging_trigger.trigger.JenkinsJobTrigger")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_run_from_config_dtest_description_prefix(_mock_user, mock_jenkins_cls, tmp_path, fresh_presets):
    """YAML description is used as prefix for dtest topology descriptions."""
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

    results = run_from_config(str(config_path), dry_run=True)
    assert len(results) == 2
    assert results[0].description == "gating (no-tablets)"
    assert results[1].description == "gating (tablets)"


# ---------------------------------------------------------------------------
# generate_yaml_config / _write_jobs_yaml_config
# ---------------------------------------------------------------------------


@patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_generates_valid_yaml(_mock_user1, _mock_user2, tmp_path, fresh_presets):
    """generate_yaml_config writes a YAML config with the branch, folder and detected preset."""
    pipelines_dir = tmp_path / "jenkins-pipelines"
    sub = pipelines_dir / "oss" / "longevity"
    sub.mkdir(parents=True)
    jf = sub / "longevity-100gb-4h.jenkinsfile"
    jf.write_text("longevityPipeline()", encoding="utf-8")

    out_path = tmp_path / "output.yaml"

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


@patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_includes_custom_repo(_mock_user1, _mock_user2, tmp_path, fresh_presets):
    """generate_yaml_config keeps a custom repo URL in the config."""
    pipelines_dir = tmp_path / "jenkins-pipelines"
    sub = pipelines_dir / "oss"
    sub.mkdir(parents=True)
    jf = sub / "test.jenkinsfile"
    jf.write_text("longevityPipeline()", encoding="utf-8")

    with patch("utils.staging_trigger.interactive.get_sct_root_path", return_value=tmp_path):
        yaml_text = generate_yaml_config(
            [jf], branch="master", folder="staging/testuser", repo="git@github.com:fork/repo.git"
        )

    parsed = yaml.safe_load(yaml_text)
    assert parsed["repo"] == "git@github.com:fork/repo.git"


@patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_generates_yaml_from_job_names(_mock_user1, _mock_user2, tmp_path, fresh_presets):
    """_write_jobs_yaml_config maps each job name to its preset."""
    out_path = tmp_path / "jobs.yaml"
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


@patch("utils.staging_trigger.interactive.get_username", return_value="testuser")
@patch("utils.staging_trigger.constants.get_username", return_value="testuser")
def test_param_overrides_applied(_mock_user1, _mock_user2, fresh_presets):
    """_write_jobs_yaml_config applies param_overrides to the config params."""
    yaml_text = _write_jobs_yaml_config(
        ["longevity-100gb-4h-test"],
        branch="master",
        folder="staging/testuser",
        repo="git@github.com:scylladb/scylla-cluster-tests.git",
        param_overrides={"scylla_version": "2025.1.0"},
    )

    parsed = yaml.safe_load(yaml_text)
    assert parsed["params"]["scylla_version"] == "2025.1.0"


# ---------------------------------------------------------------------------
# ParamDefinition + choice extraction
# ---------------------------------------------------------------------------


def test_basic_fields():
    """ParamDefinition without choices stores None for choices."""
    p = ParamDefinition(default="val")
    assert p.default == "val"
    assert p.choices is None


def test_with_choices():
    """ParamDefinition stores the choices it is given."""
    p = ParamDefinition(default="a", choices=["a", "b", "c"])
    assert p.choices == ["a", "b", "c"]


def test_string_param():
    """parse_parameter_definitions reads a string parameter's default and has no choices."""
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


def test_choice_param():
    """A choice parameter without defaultValue defaults to its first choice."""
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


def test_choice_param_with_default():
    """A choice parameter keeps its explicit defaultValue and its choices."""
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


def test_mixed_params():
    """String and choice parameters are both parsed from one job definition."""
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


def test_empty_xml():
    """A job with no parameter definitions yields an empty dict."""
    xml = "<project></project>"
    result = parse_parameter_definitions(xml)
    assert result == {}


def test_separator_definitions_are_filtered_out():
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


@patch("utils.staging_trigger.interactive.get_latest_scylla_release", return_value="2025.1.3")
def test_suggestions_with_release(_mock_release):
    """Suggestions list master:latest, the latest release and the custom-value sentinel."""
    _fetch_latest_release.cache_clear()
    suggestions = _get_version_suggestions("master:latest")
    values = [c.value for c in suggestions]
    assert "master:latest" in values
    assert "2025.1.3" in values
    assert CUSTOM_VALUE_SENTINEL in values
    _fetch_latest_release.cache_clear()


@patch("utils.staging_trigger.interactive.get_latest_scylla_release", side_effect=Exception("network error"))
def test_suggestions_when_release_fetch_fails(_mock_release):
    """A failed release lookup still offers master:latest, the current value and the sentinel."""
    _fetch_latest_release.cache_clear()
    suggestions = _get_version_suggestions("5.4.0")
    values = [c.value for c in suggestions]
    assert "master:latest" in values
    assert "5.4.0" in values  # current value preserved
    assert CUSTOM_VALUE_SENTINEL in values
    _fetch_latest_release.cache_clear()


@patch("utils.staging_trigger.interactive.get_latest_scylla_release", return_value="2025.1.3")
def test_no_duplicate_current_value(_mock_release):
    """The current value equal to master:latest appears only once in the suggestions."""
    _fetch_latest_release.cache_clear()
    suggestions = _get_version_suggestions("master:latest")
    values = [c.value for c in suggestions]
    # master:latest should appear only once
    assert values.count("master:latest") == 1
    _fetch_latest_release.cache_clear()


# ---------------------------------------------------------------------------
# _prompt_for_value with KNOWN_PARAM_CHOICES fallback
# ---------------------------------------------------------------------------


@patch("utils.staging_trigger.interactive.questionary")
def test_known_param_shows_select(mock_q):
    """provision_type should show a select menu from KNOWN_PARAM_CHOICES."""
    mock_q.select.return_value.ask.return_value = "on_demand"
    result = _prompt_for_value("provision_type", "spot", param_meta={})
    mock_q.select.assert_called_once_with("provision_type:", choices=[mock_q.Choice.return_value] * 5)
    assert mock_q.Choice.call_args_list == [
        call("spot", value="spot"),
        call("on_demand", value="on_demand"),
        call("spot_fleet", value="spot_fleet"),
        call("spot_low_price", value="spot_low_price"),
        call("Custom value...", value=CUSTOM_VALUE_SENTINEL),
    ]
    assert result == "on_demand"


@patch("utils.staging_trigger.interactive.questionary")
def test_known_param_custom_value_fallback(mock_q):
    """Picking 'Custom value...' should fall back to text input."""
    mock_q.select.return_value.ask.return_value = CUSTOM_VALUE_SENTINEL
    mock_q.text.return_value.ask.return_value = "spot_fleet"
    result = _prompt_for_value("provision_type", "spot", param_meta={})
    mock_q.text.assert_called_once_with("provision_type:", default="spot")
    assert result == "spot_fleet"


@patch("utils.staging_trigger.interactive.questionary")
def test_unknown_param_shows_text(mock_q):
    """Parameters not in KNOWN_PARAM_CHOICES should use text input."""
    mock_q.text.return_value.ask.return_value = "my-value"
    result = _prompt_for_value("email_recipients", "qa@scylladb.com", param_meta={})
    mock_q.text.assert_called_once_with("email_recipients:", default="qa@scylladb.com")
    assert result == "my-value"


@patch("utils.staging_trigger.interactive.questionary")
def test_jenkins_choices_take_precedence(mock_q):
    """Jenkins metadata choices should be used over KNOWN_PARAM_CHOICES."""
    jenkins_choices = ["alpha", "beta", "gamma"]
    meta = {"my_param": ParamDefinition(default="alpha", choices=jenkins_choices)}
    mock_q.select.return_value.ask.return_value = "beta"
    result = _prompt_for_value("my_param", "alpha", param_meta=meta)
    mock_q.select.assert_called_once_with("my_param:", choices=[mock_q.Choice.return_value] * 4)
    assert mock_q.Choice.call_args_list == [
        call("alpha", value="alpha"),
        call("beta", value="beta"),
        call("gamma", value="gamma"),
        call("Custom value...", value=CUSTOM_VALUE_SENTINEL),
    ]
    mock_q.text.assert_not_called()
    assert result == "beta"


@patch("utils.staging_trigger.interactive.questionary")
def test_post_behavior_shows_select(mock_q):
    """post_behavior_db_nodes should show destroy/keep/keep-on-failure."""
    mock_q.select.return_value.ask.return_value = "keep"
    result = _prompt_for_value("post_behavior_db_nodes", "destroy", param_meta={})
    mock_q.select.assert_called_once_with("post_behavior_db_nodes:", choices=[mock_q.Choice.return_value] * 4)
    assert mock_q.Choice.call_args_list == [
        call("destroy", value="destroy"),
        call("keep", value="keep"),
        call("keep-on-failure", value="keep-on-failure"),
        call("Custom value...", value=CUSTOM_VALUE_SENTINEL),
    ]
    assert result == "keep"


@pytest.mark.parametrize(
    "param_name",
    [pytest.param(k, id=k) for k in KNOWN_PARAM_CHOICES],
)
def test_all_known_params_have_choices(param_name):
    """Every key in KNOWN_PARAM_CHOICES must have a non-empty list."""
    assert len(KNOWN_PARAM_CHOICES[param_name]) >= 2


# ---------------------------------------------------------------------------
# Docker image auto-derivation
# ---------------------------------------------------------------------------


def test_sets_nightly_for_master_latest():
    """maybe_set_docker_image sets the nightly image for master:latest."""
    params = {"scylla_version": "master:latest"}
    maybe_set_docker_image(params)
    assert params["scylla_docker_image"] == "scylladb/scylla-nightly"


def test_sets_release_repo_for_release_version():
    """maybe_set_docker_image sets the release image for a release version."""
    params = {"scylla_version": "2025.1.3"}
    maybe_set_docker_image(params)
    assert params["scylla_docker_image"] == "scylladb/scylla"


def test_skips_when_no_version():
    """maybe_set_docker_image leaves the image unset when no version is given."""
    params = {"scylla_version": ""}
    maybe_set_docker_image(params)
    assert "scylla_docker_image" not in params


def test_skips_when_backend_not_docker():
    """maybe_set_docker_image leaves the image unset for a non-docker backend."""
    params = {"scylla_version": "master:latest", "backend": "aws"}
    maybe_set_docker_image(params)
    assert "scylla_docker_image" not in params


def test_skips_when_already_set():
    """maybe_set_docker_image keeps an explicit scylla_docker_image."""
    params = {"scylla_version": "master:latest", "scylla_docker_image": "my-custom-image"}
    maybe_set_docker_image(params)
    assert params["scylla_docker_image"] == "my-custom-image"


def test_allows_empty_backend():
    """An empty backend counts as docker, so the nightly image is set."""
    params = {"scylla_version": "master:latest", "backend": ""}
    maybe_set_docker_image(params)
    assert params["scylla_docker_image"] == "scylladb/scylla-nightly"


def test_handles_invalid_version():
    """An unparsable version leaves the image unset instead of raising."""
    params = {"scylla_version": "not-a-version"}
    maybe_set_docker_image(params)
    # Should not crash; image not set
    assert "scylla_docker_image" not in params


def test_auto_set_docker_image_sets_nightly_for_master_latest():
    """Trigger's _auto_set_docker_image sets the nightly image for master:latest."""
    params = {"scylla_version": "master:latest"}
    _auto_set_docker_image(params)
    assert params["scylla_docker_image"] == "scylladb/scylla-nightly"


def test_auto_set_docker_image_skips_when_backend_not_docker():
    """Trigger's _auto_set_docker_image leaves the image unset for a non-docker backend."""
    params = {"scylla_version": "master:latest", "backend": "aws"}
    _auto_set_docker_image(params)
    assert "scylla_docker_image" not in params


def test_auto_set_docker_image_skips_when_already_set():
    """Trigger's _auto_set_docker_image keeps an explicit scylla_docker_image."""
    params = {"scylla_version": "master:latest", "scylla_docker_image": "custom"}
    _auto_set_docker_image(params)
    assert params["scylla_docker_image"] == "custom"


# ---------------------------------------------------------------------------
# JenkinsJobTrigger.trigger
# ---------------------------------------------------------------------------


def test_drops_undefined_parameters(make_client):
    """trigger() sends only the parameters the job defines, plus requested_by_user."""
    client = make_client(JOB_CONFIG_XML)
    params = {"scylla_version": "master:latest", "scylla_docker_image": "scylladb/scylla-nightly"}

    assert client.trigger("folder/job", params) == 7

    client.jenkins.build_job.assert_called_once_with(
        name="folder/job", parameters={"scylla_version": "master:latest", "requested_by_user": "someone"}
    )


def test_keeps_parameters_when_definitions_unknown(make_client):
    """trigger() sends all parameters when the job config cannot be read."""
    client = make_client(None)
    client.jenkins.get_job_config.side_effect = Exception("no config")
    params = {"scylla_version": "master:latest", "scylla_docker_image": "x"}

    client.trigger("folder/job", params)

    client.jenkins.build_job.assert_called_once_with(
        name="folder/job", parameters={**params, "requested_by_user": "someone"}
    )


@pytest.mark.parametrize(
    "params",
    [pytest.param({}, id="missing"), pytest.param({"requested_by_user": ""}, id="empty")],
)
def test_fills_requested_by_user(make_client, params):
    """trigger() fills requested_by_user with the current user when it is missing or empty."""
    client = make_client(JOB_CONFIG_XML)

    client.trigger("folder/job", {"backend": "aws", **params})

    client.jenkins.build_job.assert_called_once_with(
        name="folder/job", parameters={"backend": "aws", "requested_by_user": "someone"}
    )


def test_keeps_explicit_requested_by_user(make_client):
    """trigger() keeps an explicit requested_by_user."""
    client = make_client(JOB_CONFIG_XML)

    client.trigger("folder/job", {"requested_by_user": "other"})

    client.jenkins.build_job.assert_called_once_with(name="folder/job", parameters={"requested_by_user": "other"})


def test_drops_requested_by_user_when_job_lacks_it(make_client):
    """trigger() omits requested_by_user when the job does not define it."""
    client = make_client(JOB_CONFIG_XML.replace("requested_by_user", "other_param"))

    client.trigger("folder/job", {"backend": "aws"})

    client.jenkins.build_job.assert_called_once_with(name="folder/job", parameters={"backend": "aws"})


def test_jenkins_job_trigger_uses_caller_credentials_from_env(monkeypatch):
    """JenkinsJobTrigger logs in to the env Jenkins URL with the env username and token."""
    monkeypatch.setenv("JENKINS_URL", "https://jenkins.example.com/")
    monkeypatch.setenv("JENKINS_USERNAME", "alice")
    monkeypatch.setenv("JENKINS_API_TOKEN", "alice-token")
    with patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins") as jenkins_cls:
        client = JenkinsJobTrigger()

    jenkins_cls.assert_called_once_with("https://jenkins.example.com", username="alice", password="alice-token")
    assert client.url == "https://jenkins.example.com"


def test_jenkins_job_trigger_looks_up_credentials_once_per_process(capsys, no_jenkins_env):
    """Two triggers share one login: stores and whoAmI are queried once and announced once."""
    with (
        fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT) as run,
        patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins") as jenkins_cls,
    ):
        first, second = JenkinsJobTrigger(), JenkinsJobTrigger()

    assert first.jenkins is second.jenkins
    run.assert_called_once_with(ONEPASSWORD_COMMAND)
    jenkins_cls.return_value.get_whoami.assert_called_once_with()
    assert capsys.readouterr().err.count("Jenkins: calling") == 1


def test_job_name_completion_keeps_credential_messages_off_the_prompt(capsys, monkeypatch):
    """Shell completion of job names writes nothing to stderr."""
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
    """iter_jenkins_credentials yields 1Password first, then the keyring, skipping unusable entries."""
    with fake_secret_stores(keyring, onepassword):
        assert list(iter_jenkins_credentials()) == expected


def test_iter_jenkins_credentials_env_comes_first_and_overrides_stored_user(monkeypatch):
    """Environment credentials come first and their user is paired with the stored keyring token."""
    monkeypatch.setenv("JENKINS_USERNAME", "alice")
    monkeypatch.setenv("JENKINS_API_TOKEN", "alice-token")
    with fake_secret_stores(KEYRING_SEARCH_OUTPUT):
        assert list(iter_jenkins_credentials()) == [
            ("alice", "alice-token", "environment"),
            ("alice", "keyring-token", "OS keyring"),
        ]


def test_iter_jenkins_credentials_skips_keyring_when_1password_is_enough(no_jenkins_env):
    """The keyring is not queried while the consumer stops at the 1Password credential."""
    with fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT) as run:
        assert next(iter_jenkins_credentials())[2] == "1Password"

    run.assert_called_once_with(ONEPASSWORD_COMMAND)


@pytest.mark.parametrize("status", [pytest.param(401, id="401"), pytest.param(403, id="403")])
def test_jenkins_job_trigger_falls_back_when_a_store_is_rejected(capsys, no_jenkins_env, status):
    """A 401 or 403 on one store's token moves on to the next store."""

    def make_client(url, username, password):
        client = MagicMock()
        if password == "op-token":
            # python-jenkins' message for a 401 or 403
            client.get_whoami.side_effect = jenkins.JenkinsException(
                f"Error in request. Possibly authentication failed [{status}]: Forbidden"
            )
        return client

    with (
        fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT),
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
    """A timeout or server error stops the lookup without trying further stores."""
    with (
        fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT) as run,
        patch("utils.staging_trigger.jenkins_client.jenkins.Jenkins") as jenkins_cls,
        pytest.raises(click.ClickException, match=f"did not answer whoAmI: {re.escape(str(error))}"),
    ):
        jenkins_cls.return_value.get_whoami.side_effect = error
        JenkinsJobTrigger()

    run.assert_called_once_with(
        ONEPASSWORD_COMMAND
    )  # the keyring is not asked after 1Password's token hit a Jenkins failure
    assert "was rejected" not in capsys.readouterr().err


@pytest.mark.parametrize(
    "how", [pytest.param("generate_job", id="generate_job"), pytest.param("generate_jobs", id="generate_jobs")]
)
def test_generate_creates_jobs_with_the_callers_login(fake_job_creation, how):
    """generate_job and generate_jobs create jobs through JenkinsPipelines using the caller's Jenkins login."""
    login, pipelines_cls = fake_job_creation

    assert generate_one(how) == [LATTE_PRELOAD_JOB]

    pipelines_cls.assert_called_once_with(
        base_job_dir="scylla-staging/alice", sct_branch_name="master", sct_repo=SCT_REPO, client=login
    )
    pipelines_cls.return_value.jenkins.job_exists.assert_any_call(LATTE_PRELOAD_JOB)


@pytest.mark.parametrize(
    "how", [pytest.param("generate_job", id="generate_job"), pytest.param("generate_jobs", id="generate_jobs")]
)
def test_generate_stops_when_jenkins_did_not_create_the_job(capsys, fake_job_creation, how):
    """generate_job and generate_jobs fail when Jenkins does not have the job afterwards."""
    _, pipelines_cls = fake_job_creation
    # JenkinsPipelines logs a refused create (e.g. 403) and returns; the job just isn't there
    pipelines_cls.return_value.jenkins.job_exists.return_value = False

    with pytest.raises(click.ClickException, match=f"did not create 1 job\\(s\\): {LATTE_PRELOAD_JOB}"):
        generate_one(how)

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
    """JenkinsJobTrigger raises when no store has credentials or all are rejected."""
    rejected = MagicMock()
    rejected.get_whoami.side_effect = jenkins.JenkinsException("Possibly authentication failed [401]")
    with (
        fake_secret_stores(keyring, onepassword),
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
    """_run_secret_command returns empty output and reports the failure on stderr, silently if the CLI is missing."""
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
    """JENKINS_CREDENTIALS_SOURCE restricts the lookup to one source, case-insensitively."""
    monkeypatch.setenv("JENKINS_CREDENTIALS_SOURCE", wanted)
    with fake_secret_stores(KEYRING_SEARCH_OUTPUT, ONEPASSWORD_OUTPUT):
        assert list(iter_jenkins_credentials()) == expected


def test_jenkins_credentials_source_rejects_unknown_value(monkeypatch):
    """An unknown JENKINS_CREDENTIALS_SOURCE raises a ClickException listing the valid sources."""
    monkeypatch.setenv("JENKINS_CREDENTIALS_SOURCE", "vault")
    with pytest.raises(click.ClickException, match="not one of: env, 1password, keyring"):
        list(iter_jenkins_credentials())
