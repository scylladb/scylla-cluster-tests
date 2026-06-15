import logging
from unittest.mock import MagicMock, patch

import pytest

from utils.build_system.create_test_release_jobs import JenkinsPipelines


@pytest.fixture()
def mock_jenkins_pipelines(tmp_path):
    with patch("utils.build_system.create_test_release_jobs.KeyStore"):
        with patch("utils.build_system.create_test_release_jobs.jenkins.Jenkins"):
            with patch("utils.build_system.create_test_release_jobs.get_sct_root_path", return_value=tmp_path):
                jp = JenkinsPipelines(
                    base_job_dir="scylla-master",
                    sct_branch_name="master",
                    sct_repo="git@github.com:scylladb/scylla-cluster-tests.git",
                    username="test",
                    password="test",
                )
                jp.base_sct_dir = tmp_path
                jp.jenkins = MagicMock()
                jp.jenkins.job_exists.return_value = False
                yield jp


@pytest.fixture()
def pipeline_tree(tmp_path):
    target_dir = tmp_path / "jenkins-pipelines" / "oss" / "artifacts"
    target_dir.mkdir(parents=True)

    (target_dir / "artifacts-docker.jenkinsfile").write_text("#! groovy\nartifactsPipeline()")
    (target_dir / "artifacts-ami.jenkinsfile").write_text("#! groovy\nartifactsPipeline()")

    symlink_dir = tmp_path / "jenkins-pipelines" / "scylla-doctor"
    symlink_dir.mkdir(parents=True)

    return tmp_path


def test_process_symlinks_creates_jobs(mock_jenkins_pipelines, pipeline_tree):
    symlink_dir = pipeline_tree / "jenkins-pipelines" / "scylla-doctor"
    symlink_dir.joinpath("_symlinks.yaml").write_text(
        "symlinks:\n"
        '  - name: "artifacts-docker"\n'
        '    target: "jenkins-pipelines/oss/artifacts/artifacts-docker.jenkinsfile"\n'
        '  - name: "artifacts-ami"\n'
        '    target: "jenkins-pipelines/oss/artifacts/artifacts-ami.jenkinsfile"\n'
    )

    mock_jenkins_pipelines.process_symlinks(symlink_dir, "scylla-doctor", "-test", {})

    assert mock_jenkins_pipelines.jenkins.create_job.call_count == 2
    created_jobs = [call[0][0] for call in mock_jenkins_pipelines.jenkins.create_job.call_args_list]
    assert "scylla-master/scylla-doctor/artifacts-docker-test" in created_jobs
    assert "scylla-master/scylla-doctor/artifacts-ami-test" in created_jobs


def test_process_symlinks_no_file(mock_jenkins_pipelines, pipeline_tree):
    symlink_dir = pipeline_tree / "jenkins-pipelines" / "scylla-doctor"

    mock_jenkins_pipelines.process_symlinks(symlink_dir, "scylla-doctor", "-test", {})

    mock_jenkins_pipelines.jenkins.create_job.assert_not_called()


def test_process_symlinks_missing_target(mock_jenkins_pipelines, pipeline_tree, caplog):
    symlink_dir = pipeline_tree / "jenkins-pipelines" / "scylla-doctor"
    symlink_dir.joinpath("_symlinks.yaml").write_text(
        'symlinks:\n  - name: "nonexistent"\n    target: "jenkins-pipelines/oss/artifacts/nonexistent.jenkinsfile"\n'
    )

    with caplog.at_level(logging.ERROR):
        mock_jenkins_pipelines.process_symlinks(symlink_dir, "scylla-doctor", "-test", {})

    mock_jenkins_pipelines.jenkins.create_job.assert_not_called()
    assert "does not exist" in caplog.text


def test_process_symlinks_invalid_entry(mock_jenkins_pipelines, pipeline_tree, caplog):
    symlink_dir = pipeline_tree / "jenkins-pipelines" / "scylla-doctor"
    symlink_dir.joinpath("_symlinks.yaml").write_text('symlinks:\n  - name: "missing-target"\n')

    with caplog.at_level(logging.WARNING):
        mock_jenkins_pipelines.process_symlinks(symlink_dir, "scylla-doctor", "-test", {})

    mock_jenkins_pipelines.jenkins.create_job.assert_not_called()
    assert "missing 'name' or 'target'" in caplog.text


def test_process_symlinks_custom_suffix(mock_jenkins_pipelines, pipeline_tree):
    symlink_dir = pipeline_tree / "jenkins-pipelines" / "scylla-doctor"
    symlink_dir.joinpath("_symlinks.yaml").write_text(
        "symlinks:\n"
        '  - name: "artifacts-docker"\n'
        '    target: "jenkins-pipelines/oss/artifacts/artifacts-docker.jenkinsfile"\n'
        '    job_name_suffix: "-gating"\n'
    )

    mock_jenkins_pipelines.process_symlinks(symlink_dir, "scylla-doctor", "-test", {})

    created_jobs = [call[0][0] for call in mock_jenkins_pipelines.jenkins.create_job.call_args_list]
    assert "scylla-master/scylla-doctor/artifacts-docker-gating" in created_jobs


def test_process_symlinks_integrated_in_create_job_tree(mock_jenkins_pipelines, pipeline_tree):
    symlink_dir = pipeline_tree / "jenkins-pipelines" / "scylla-doctor"
    symlink_dir.joinpath("_symlinks.yaml").write_text(
        "symlinks:\n"
        '  - name: "artifacts-docker"\n'
        '    target: "jenkins-pipelines/oss/artifacts/artifacts-docker.jenkinsfile"\n'
    )
    symlink_dir.joinpath("_display_name").write_text("Scylla Doctor")

    mock_jenkins_pipelines.create_job_tree(symlink_dir)

    created_jobs = [call[0][0] for call in mock_jenkins_pipelines.jenkins.create_job.call_args_list]
    assert any("artifacts-docker-test" in job for job in created_jobs)
