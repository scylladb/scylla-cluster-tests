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

"""Find builder labels in vars/getJenkinsLabels.groovy that no Jenkins node or cloud can serve."""

import re
from pathlib import Path

import requests

from sdcm.keystore import KeyStore
from sdcm.utils.gce_utils import SUPPORTED_PROJECTS

GROOVY_LABELS_FILE = Path(__file__).parents[2] / "vars" / "getJenkinsLabels.groovy"
GCP_PROJECT_PLACEHOLDER = "${gcp_project}"


def builder_labels(groovy_text: str) -> set[str]:
    """Return every label of the `jenkins_labels` map, with each GCE project the groovy can resolve."""
    block = groovy_text[groovy_text.index("def jenkins_labels") :]
    block = block[: block.index("]")]
    # The groovy resolves gce_project to its default or an alias target; jobs may also pass any configured project.
    projects = set(SUPPORTED_PROJECTS) | set(re.findall(r"'(gcp-[\w-]+)'", groovy_text))
    labels = set()
    for value in re.findall(r":\s*['\"]([^'\"]+)['\"]", block):
        if GCP_PROJECT_PLACEHOLDER in value:
            labels.update(value.replace(GCP_PROJECT_PLACEHOLDER, project) for project in projects)
        else:
            labels.add(value)
    return labels


def unserved_labels(labels: set[str]) -> list[str]:
    """Return the labels that have neither a node nor a cloud able to provision one in Jenkins."""
    jenkins_info = KeyStore().get_json("jenkins.json")
    missing = []
    for label in sorted(labels):
        resp = requests.get(
            f"{jenkins_info['url']}/label/{label}/api/json",
            params={"tree": "nodes[nodeName],clouds[name]"},
            auth=(jenkins_info["username"], jenkins_info["password"]),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data["nodes"] and not data["clouds"]:
            missing.append(label)
    return missing
