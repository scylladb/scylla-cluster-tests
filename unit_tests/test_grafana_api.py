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

"""Integration tests for sdcm.utils.grafana_api against real Grafana containers.

External services: Docker (upstream ``grafana/grafana`` images, no Scylla needed).

The dashboard API moved from ``/api/dashboards/db`` to the Kubernetes-shaped
``/apis/dashboard.grafana.app/...`` surface, and only a real Grafana can answer which version
exists, what status code a create returns, and whether a re-upload conflicts. Every claim
``sdcm/utils/grafana_api.py`` encodes is asserted here against the actual servers:

* ``v1beta1`` is the version to target -- ``v1`` does not exist before Grafana 13 and the
  ``v2*`` schemas reject our legacy dashboard model.
* a create answers ``201``, so a ``status_code == 200`` check would read every success as
  a failure.
* re-uploading the same dashboard must not raise ``409 Conflict`` nor silently pile up
  duplicates, because SCT re-registers dashboards when it reuses a monitoring stack.
* Grafana < 12 has no ``/apis`` surface at all, so the legacy endpoint fallback must engage.

These are the slowest tests in the suite when the images are cold (each version is a ~400MB
pull). ``GRAFANA_TEST_VERSIONS`` narrows the matrix for a quick local loop:

    GRAFANA_TEST_VERSIONS=13.1.3 uv run pytest unit_tests/integration/test_grafana_api.py
"""

import json
import logging
import os
import subprocess
import uuid

import pytest
import requests

from sdcm.utils.common import get_data_dir_path, get_free_port
from sdcm.utils.grafana_api import (
    GRAFANA_ANNOTATIONS_API_PATH,
    GRAFANA_DASHBOARD_API_PATH,
    GRAFANA_LEGACY_DASHBOARD_API_PATH,
    GRAFANA_SEARCH_API_PATH,
    convert_dashboard_payload_to_new_api,
    dashboard_uid_from_payload,
    upload_dashboard,
)
from sdcm.wait import wait_for

LOGGER = logging.getLogger(__name__)

GRAFANA_ADMIN = ("admin", "admin")

# One release per API generation, oldest first:
#   11.x  -- no /apis surface, must fall back to the legacy endpoint
#   12.0  -- earliest /apis surface, serves v1beta1 only
#   12.4  -- the version shipped by scylla-monitoring branch-4.15
#   13.1  -- adds v1 and prefers v2, so it proves v1beta1 is still served
DEFAULT_GRAFANA_VERSIONS = ("11.6.6", "12.0.7", "12.4.3", "13.1.3")
VERSIONS_WITHOUT_APIS = ("11.6.6",)


def _grafana_versions() -> tuple[str, ...]:
    if selected := os.environ.get("GRAFANA_TEST_VERSIONS"):
        return tuple(version.strip() for version in selected.split(",") if version.strip())
    return DEFAULT_GRAFANA_VERSIONS


def _docker(*args: str, check: bool = True) -> str:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


@pytest.fixture(name="grafana", scope="module", params=_grafana_versions())
def fixture_grafana(request: pytest.FixtureRequest):
    """A running Grafana container, yielding ``(version, base_url)``."""
    version = request.param
    port = get_free_port()
    name = f"sct-grafana-test-{version.replace('.', '-')}-{uuid.uuid4().hex[:8]}"
    _docker(
        "run",
        "-d",
        "--name",
        name,
        "-p",
        f"{port}:3000",
        "-e",
        f"GF_SECURITY_ADMIN_PASSWORD={GRAFANA_ADMIN[1]}",
        f"grafana/grafana:{version}",
    )
    base_url = f"http://localhost:{port}"
    try:
        _wait_for_grafana(base_url, version)
        yield version, base_url
    finally:
        LOGGER.debug("grafana %s container logs:\n%s", version, _docker("logs", "--tail", "20", name, check=False))
        _docker("rm", "-f", name, check=False)


def _wait_for_grafana(base_url: str, version: str) -> None:
    def healthy():
        try:
            return requests.get(f"{base_url}/api/health", timeout=5).ok
        except requests.RequestException:
            return False

    wait_for(healthy, step=1, timeout=180, throw_exc=True, text=f"Waiting for grafana {version} to come up")


@pytest.fixture(name="dashboard_payload")
def fixture_dashboard_payload() -> dict:
    """A legacy-format dashboard payload with a uid unique to the requesting test."""
    with open(get_data_dir_path("scylla-dash-per-server-nemesis.master.json"), encoding="utf-8") as dashboard_file:
        payload = json.load(dashboard_file)
    # a shared uid would make the tests fight over one resource inside a module-scoped grafana
    payload["dashboard"]["uid"] = f"sct-{uuid.uuid4().hex[:12]}"
    return payload


def _get_dashboard_names(base_url: str) -> list[str]:
    response = requests.get(f"{base_url}{GRAFANA_DASHBOARD_API_PATH}", auth=GRAFANA_ADMIN, timeout=30)
    response.raise_for_status()
    return [item["metadata"]["name"] for item in response.json()["items"]]


@pytest.mark.integration
def test_new_dashboard_api_version_is_served(grafana):
    """The version SCT targets must be the one the server actually serves."""
    version, base_url = grafana
    if version in VERSIONS_WITHOUT_APIS:
        pytest.skip(f"grafana {version} predates the /apis surface")

    served = requests.get(f"{base_url}/apis", auth=GRAFANA_ADMIN, timeout=30).json()["groups"]
    dashboard_group = [group for group in served if group["name"] == "dashboard.grafana.app"]
    assert dashboard_group, "grafana does not serve the dashboard.grafana.app API group"
    versions = [entry["version"] for entry in dashboard_group[0]["versions"]]

    api_version = GRAFANA_DASHBOARD_API_PATH.split("/")[3]
    assert api_version in versions, (
        f"grafana {version} serves dashboard versions {versions} but SCT targets {api_version!r}"
    )


@pytest.mark.integration
def test_upload_dashboard_succeeds_and_is_idempotent(grafana, dashboard_payload):
    """Re-uploading must update in place: no 409, no duplicate dashboards."""
    _version, base_url = grafana
    uid = dashboard_uid_from_payload(dashboard_payload)

    first = upload_dashboard(base_url, dashboard_payload, auth=GRAFANA_ADMIN)
    assert first.ok, f"first upload failed: {first.status_code} {first.text}"

    second = upload_dashboard(base_url, dashboard_payload, auth=GRAFANA_ADMIN)
    assert second.ok, f"re-upload failed instead of updating in place: {second.status_code} {second.text}"
    assert second.status_code != requests.codes.conflict

    names = _get_dashboard_names(base_url) if _version not in VERSIONS_WITHOUT_APIS else []
    if names:
        assert names.count(uid) == 1, f"re-upload duplicated the dashboard: {names}"


@pytest.mark.integration
def test_dashboard_create_returns_201_so_a_200_check_would_break(grafana, dashboard_payload):
    """Guards the ``res.ok`` over ``status_code == 200`` decision this migration rests on."""
    _version, base_url = grafana
    if _version in VERSIONS_WITHOUT_APIS:
        pytest.skip(f"grafana {_version} answers the legacy endpoint, which returns 200")

    uid = dashboard_uid_from_payload(dashboard_payload)
    payload = convert_dashboard_payload_to_new_api(dashboard_payload, uid=uid)
    created = requests.post(f"{base_url}{GRAFANA_DASHBOARD_API_PATH}", json=payload, auth=GRAFANA_ADMIN, timeout=30)
    assert created.status_code == requests.codes.created, (
        f"expected 201 Created from the new API, got {created.status_code}"
    )


@pytest.mark.integration
def test_post_to_collection_conflicts_on_reupload(grafana, dashboard_payload):
    """Documents why ``upload_dashboard`` uses PUT: a plain POST cannot be replayed."""
    _version, base_url = grafana
    if _version in VERSIONS_WITHOUT_APIS:
        pytest.skip(f"grafana {_version} has no /apis surface")

    payload = convert_dashboard_payload_to_new_api(dashboard_payload)
    url = f"{base_url}{GRAFANA_DASHBOARD_API_PATH}"
    assert requests.post(url, json=payload, auth=GRAFANA_ADMIN, timeout=30).ok
    conflict = requests.post(url, json=payload, auth=GRAFANA_ADMIN, timeout=30)
    assert conflict.status_code == requests.codes.conflict, (
        f"expected 409 on a repeated POST, got {conflict.status_code}"
    )


@pytest.mark.integration
def test_legacy_endpoint_fallback_on_old_grafana(grafana, dashboard_payload):
    """On Grafana < 12 the /apis path 404s and the upload must still land."""
    version, base_url = grafana
    new_api = requests.put(
        f"{base_url}{GRAFANA_DASHBOARD_API_PATH}/{dashboard_uid_from_payload(dashboard_payload)}",
        json=convert_dashboard_payload_to_new_api(dashboard_payload),
        auth=GRAFANA_ADMIN,
        timeout=30,
    )
    if version in VERSIONS_WITHOUT_APIS:
        assert new_api.status_code == requests.codes.not_found, (
            f"grafana {version} was expected to lack the /apis surface, got {new_api.status_code}"
        )

    # whichever endpoint is available, the helper must succeed
    assert upload_dashboard(base_url, dashboard_payload, auth=GRAFANA_ADMIN).ok

    legacy = requests.post(
        f"{base_url}{GRAFANA_LEGACY_DASHBOARD_API_PATH}",
        json=dict(dashboard_payload, overwrite=True),
        auth=GRAFANA_ADMIN,
        timeout=30,
    )
    assert legacy.ok, f"legacy endpoint unexpectedly unusable on grafana {version}: {legacy.status_code}"


@pytest.mark.integration
def test_annotations_and_search_stay_on_legacy_api(grafana):
    """These have no /apis replacement, so the migration must leave them on /api."""
    _version, base_url = grafana

    posted = requests.post(
        f"{base_url}{GRAFANA_ANNOTATIONS_API_PATH}",
        json={"text": "sct-integration-probe", "tags": ["sct"]},
        auth=GRAFANA_ADMIN,
        timeout=30,
    )
    assert posted.ok, f"posting an annotation failed: {posted.status_code} {posted.text}"
    assert requests.get(f"{base_url}{GRAFANA_ANNOTATIONS_API_PATH}", auth=GRAFANA_ADMIN, timeout=30).ok
    assert requests.get(f"{base_url}{GRAFANA_SEARCH_API_PATH}?query=scylla", auth=GRAFANA_ADMIN, timeout=30).ok


@pytest.mark.integration
def test_uploaded_dashboard_is_searchable(grafana, dashboard_payload):
    """A dashboard that uploads but does not show up in Grafana would be a silent failure."""
    _version, base_url = grafana
    title = dashboard_payload["dashboard"]["title"] = f"SCT probe {uuid.uuid4().hex[:8]}"

    assert upload_dashboard(base_url, dashboard_payload, auth=GRAFANA_ADMIN).ok

    found = requests.get(
        f"{base_url}{GRAFANA_SEARCH_API_PATH}", params={"query": title}, auth=GRAFANA_ADMIN, timeout=30
    )
    assert found.ok
    assert [hit for hit in found.json() if hit.get("title") == title], (
        f"dashboard {title!r} uploaded but is not searchable in grafana"
    )
