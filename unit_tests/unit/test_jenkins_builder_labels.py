import re
from unittest.mock import MagicMock, patch

from sdcm.utils.jenkins_builder_labels import GROOVY_LABELS_FILE, builder_labels, unserved_labels


def test_builder_labels_from_repo_groovy():
    groovy_text = GROOVY_LABELS_FILE.read_text()
    labels = builder_labels(groovy_text)

    assert any(label.startswith("oci-sct-builders-us-phoenix-1-") for label in labels)
    assert any(label.startswith("aws-sct-builders-eu-west-1-") for label in labels)
    assert not any("${" in label for label in labels)
    # ${gcp_project} expands to every GCE project the groovy names
    for project in re.findall(r"'(gcp-[\w-]+)'", groovy_text):
        assert any(label.startswith(f"{project}-builders-us-east1-template-") for label in labels)


def test_unserved_labels_needs_a_node_or_a_cloud():
    answers = {
        "has-cloud": {"nodes": [], "clouds": [{}]},
        "has-node": {"nodes": [{"nodeName": "n1"}], "clouds": []},
        "nothing": {"nodes": [], "clouds": []},
    }

    def fake_get(url, **_):
        return MagicMock(json=MagicMock(return_value=answers[url.split("/")[-3]]))

    with (
        patch("sdcm.utils.jenkins_builder_labels.KeyStore") as keystore,
        patch("sdcm.utils.jenkins_builder_labels.requests.get", side_effect=fake_get),
    ):
        keystore.return_value.get_json.return_value = {"url": "https://j", "username": "u", "password": "p"}
        assert unserved_labels(set(answers)) == ["nothing"]
