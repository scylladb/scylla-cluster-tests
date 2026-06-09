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

import logging

LOGGER = logging.getLogger(__name__)

# New Grafana /apis endpoints (available since Grafana 12, mandatory from Grafana 13+)
# See: https://grafana.com/whats-new/2026-04-20-deprecation-of--api-path/
GRAFANA_DASHBOARD_API_PATH = "/apis/dashboard.grafana.app/v1beta1/namespaces/default/dashboards"

# Annotations and Search have no /apis replacement yet (as of Grafana 13)
GRAFANA_ANNOTATIONS_API_PATH = "/api/annotations"
GRAFANA_SEARCH_API_PATH = "/api/search"


def convert_dashboard_payload_to_new_api(legacy_payload: dict) -> dict:
    """Convert legacy dashboard payload format to the new /apis format.

    Legacy format: {"dashboard": {...}, "overwrite": bool, "folderId": int, ...}
    New format:    {"metadata": {"generateName": "sct-"}, "spec": {...}}
    """
    dashboard_model = legacy_payload.get("dashboard", legacy_payload)
    metadata = {}
    if uid := dashboard_model.get("uid"):
        metadata["name"] = uid
    else:
        metadata["generateName"] = "sct-"
    folder_uid = legacy_payload.get("folderUid")
    if folder_uid:
        metadata.setdefault("annotations", {})["grafana.app/folder"] = folder_uid
    return {"metadata": metadata, "spec": dashboard_model}
