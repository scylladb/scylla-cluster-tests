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

import hashlib
import http
import logging
import re

import requests

from sdcm.utils.session import create_retry_session

LOGGER = logging.getLogger(__name__)

# every Grafana request must be bounded, so a wedged monitoring stack can't hang a test
DEFAULT_GRAFANA_TIMEOUT = 30

# New Grafana /apis endpoints (available since Grafana 12, mandatory from Grafana 13+)
# See: https://grafana.com/whats-new/2026-04-20-deprecation-of--api-path/
#
# v1beta1 is deliberate: it is the only stable-shaped version served by every Grafana
# that has the /apis surface at all. Verified against real containers -
# 12.0.7 serves [v1beta1, v0alpha1, v2alpha1], 12.4.3 adds v2beta1, and 13.1.3 adds v1;
# `v1` returns 404 on all of 12.x, and the v2* schemas reject our legacy dashboard model.
GRAFANA_DASHBOARD_API_PATH = "/apis/dashboard.grafana.app/v1beta1/namespaces/default/dashboards"

# Legacy dashboard endpoint, kept for Grafana < 12 (no /apis surface at all - verified 404 on 11.6.6)
GRAFANA_LEGACY_DASHBOARD_API_PATH = "/api/dashboards/db"

# Annotations and Search have no /apis replacement yet (as of Grafana 13)
GRAFANA_ANNOTATIONS_API_PATH = "/api/annotations"
GRAFANA_SEARCH_API_PATH = "/api/search"

# Grafana validates metadata.name as a dashboard uid: max 40 chars, no dots.
GRAFANA_UID_MAX_LENGTH = 40
_ILLEGAL_UID_CHARS = re.compile(r"[^a-zA-Z0-9_-]")


def dashboard_uid_from_payload(legacy_payload: dict, prefix: str = "sct-") -> str:
    """Derive a stable, Grafana-legal dashboard uid for a legacy dashboard payload.

    The uid doubles as the new API's ``metadata.name``, which is the resource identity: a
    stable value makes re-uploads idempotent (``PUT`` updates in place) instead of piling up
    duplicate dashboards. When the payload carries no uid, derive one from the dashboard
    title so the same dashboard always maps to the same resource.
    """
    dashboard_model = legacy_payload.get("dashboard", legacy_payload)
    if uid := dashboard_model.get("uid"):
        return sanitize_dashboard_uid(uid)
    title = dashboard_model.get("title") or ""
    return f"{prefix}{hashlib.sha1(title.encode('utf-8')).hexdigest()[:16]}"


def sanitize_dashboard_uid(uid: str) -> str:
    """Coerce a uid into what Grafana accepts: only ``[a-zA-Z0-9_-]``, at most 40 chars."""
    sanitized = _ILLEGAL_UID_CHARS.sub("-", uid)
    if len(sanitized) > GRAFANA_UID_MAX_LENGTH:
        # keep a stable suffix so distinct long uids don't collapse onto the same name
        digest = hashlib.sha1(uid.encode("utf-8")).hexdigest()[:8]
        sanitized = f"{sanitized[: GRAFANA_UID_MAX_LENGTH - len(digest) - 1]}-{digest}"
    return sanitized


def convert_dashboard_payload_to_new_api(legacy_payload: dict, uid: str | None = None) -> dict:
    """Convert legacy dashboard payload format to the new /apis format.

    Legacy format: {"dashboard": {...}, "overwrite": bool, "folderId": int, ...}
    New format:    {"metadata": {"name": <uid>}, "spec": {...}}

    ``metadata.name`` is always set: the new API is Kubernetes-shaped, so ``POST`` to the
    collection *creates* and returns 409 on a name clash, while ``PUT`` to
    ``<collection>/<name>`` upserts. A stable name is what makes the upload replayable,
    replacing the legacy ``"overwrite": true`` flag that has no counterpart here.
    """
    dashboard_model = legacy_payload.get("dashboard", legacy_payload)
    name = sanitize_dashboard_uid(uid) if uid else dashboard_uid_from_payload(legacy_payload)
    metadata = {"name": name}
    if folder_uid := legacy_payload.get("folderUid"):
        metadata["annotations"] = {"grafana.app/folder": folder_uid}
    # the uid must agree with metadata.name, otherwise Grafana rejects the request
    spec = dict(dashboard_model, uid=name)
    return {"metadata": metadata, "spec": spec}


def dashboard_upsert_url(base_url: str, uid: str) -> str:
    """URL of a single dashboard resource, for an idempotent ``PUT`` upsert."""
    return f"{base_url.rstrip('/')}{GRAFANA_DASHBOARD_API_PATH}/{uid}"


def upload_dashboard(
    base_url: str,
    legacy_payload: dict,
    session: requests.Session | None = None,
    timeout: int = DEFAULT_GRAFANA_TIMEOUT,
    **request_kwargs,
) -> requests.Response:
    """Upload a legacy-format dashboard payload, preferring the new /apis endpoint.

    Uses ``PUT`` on the named resource so re-uploads update in place rather than failing
    with 409 Conflict, and falls back to the legacy ``/api/dashboards/db`` endpoint when the
    /apis surface is absent (Grafana < 12), which is reachable when restoring an archived
    monitoring stack pinned to an older Grafana.

    Returns the final :class:`requests.Response` so callers keep their own success handling.
    """
    session = session or create_retry_session()
    uid = dashboard_uid_from_payload(legacy_payload)
    payload = convert_dashboard_payload_to_new_api(legacy_payload, uid=uid)
    response = session.put(
        dashboard_upsert_url(base_url, uid),
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
        **request_kwargs,
    )
    if response.status_code != http.HTTPStatus.NOT_FOUND:
        return response

    LOGGER.debug(
        "Grafana at %s has no %s endpoint, falling back to %s",
        base_url,
        GRAFANA_DASHBOARD_API_PATH,
        GRAFANA_LEGACY_DASHBOARD_API_PATH,
    )
    legacy_body = dict(legacy_payload, overwrite=True)
    legacy_body.setdefault("dashboard", legacy_payload)
    return session.post(
        f"{base_url.rstrip('/')}{GRAFANA_LEGACY_DASHBOARD_API_PATH}",
        json=legacy_body,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
        **request_kwargs,
    )
