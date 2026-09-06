"""Guard the XML rendered for Jenkins pipeline jobs against unescaped markup.

Job descriptions are built from free text (``test_metadata.description`` in the
test-case YAML, ``_folder_definitions.yaml`` dumps) and interpolated into
``template.xml``. Any ``<``/``&`` in that text used to land in the XML verbatim,
which aborted the whole ``create-test-release-jobs`` run on the first offending
job.
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.common import get_sct_root_path
from utils.build_system.create_test_release_jobs import JenkinsPipelines

SCT_ROOT = Path(get_sct_root_path())
PIPELINES_DIR = SCT_ROOT / "jenkins-pipelines"


@pytest.fixture()
def jenkins_pipelines():
    with (
        patch("utils.build_system.create_test_release_jobs.KeyStore"),
        patch("utils.build_system.create_test_release_jobs.jenkins.Jenkins"),
    ):
        jp = JenkinsPipelines(
            base_job_dir="scylla-master",
            sct_branch_name="master",
            sct_repo="git@github.com:scylladb/scylla-cluster-tests.git",
            username="test",
            password="test",
        )
        jp.jenkins = MagicMock()
        jp.jenkins.job_exists.return_value = False
        jp.build_job_and_wait_completion = MagicMock()
        yield jp


def _render(jenkins_pipelines, jenkins_file: Path) -> str:
    jenkins_pipelines.jenkins.create_job.reset_mock()
    jenkins_pipelines.create_pipeline_job(jenkins_file, group_name="")
    return jenkins_pipelines.jenkins.create_job.call_args[0][1]


def test_markup_in_test_metadata_description_is_escaped(jenkins_pipelines, tmp_path):
    """A description holding ``<backend>`` must not open a bogus XML tag."""
    test_case = tmp_path / "test-cases" / "fake" / "fake.yaml"
    test_case.parent.mkdir(parents=True)
    test_case.write_text(
        "test_metadata:\n"
        "  description: run via configurations/<backend>/fake.yaml overlays & friends\n"
        "  tier: tier1\n"
        "  test_type: longevity\n"
        "  duration_class: short\n"
        "  supported_backends:\n"
        "    - aws\n",
        encoding="utf-8",
    )
    jenkins_file = tmp_path / "jenkins-pipelines" / "oss" / "fake" / "fake.jenkinsfile"
    jenkins_file.parent.mkdir(parents=True)
    jenkins_file.write_text(f"longevityPipeline(\n    test_config: '{test_case.relative_to(tmp_path)}',\n)\n")

    with patch("utils.build_system.create_test_release_jobs.get_sct_root_path", return_value=tmp_path):
        xml_data = _render(jenkins_pipelines, jenkins_file)

    root = ET.fromstring(xml_data)
    assert "<backend>" in root.find("description").text
    assert root.find("testMetadata/tier").text == "tier1"


@pytest.mark.parametrize(
    "jenkins_file",
    sorted(PIPELINES_DIR.rglob("*.jenkinsfile")),
    ids=lambda path: str(path.relative_to(PIPELINES_DIR)),
)
def test_every_pipeline_renders_valid_xml(jenkins_pipelines, jenkins_file):
    ET.fromstring(_render(jenkins_pipelines, jenkins_file))
