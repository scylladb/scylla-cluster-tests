import logging
from dataclasses import replace
from typing import Any
from uuid import UUID

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from argus.client.tunnel.models import DEFAULT_TUNNEL_TIMEOUT, TunnelClientError, TunnelConfig
from argus.client.tunnel.state import (
    find_existing_key_dir,
    generate_and_register_key,
    read_cached_tunnel_config,
    sweep_stale_tunnel_keys,
    write_tunnel_cache,
)


LOGGER = logging.getLogger(__name__)
TUNNEL_API_RETRIES = 3


def _create_api_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=TUNNEL_API_RETRIES,
        backoff_factor=0.5,
        allowed_methods=["GET", "POST"],
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def resolve_tunnel_config(
    auth_token: str,
    base_url: str,
    force_refresh: bool = False,
    ttl_seconds: int | None = None,
    session: requests.Session | None = None,
    extra_headers: dict[str, str] | None = None,
    run_id: UUID | str | None = None,
) -> TunnelConfig | None:
    config, _key_path, _reason = resolve_tunnel_key(
        auth_token=auth_token,
        base_url=base_url,
        force_refresh=force_refresh,
        ttl_seconds=ttl_seconds,
        session=session,
        extra_headers=extra_headers,
        run_id=run_id,
    )
    return config


def resolve_tunnel_config_with_reason(
    auth_token: str,
    base_url: str,
    force_refresh: bool = False,
    ttl_seconds: int | None = None,
    session: requests.Session | None = None,
    extra_headers: dict[str, str] | None = None,
    run_id: UUID | str | None = None,
) -> tuple[TunnelConfig | None, str | None]:
    config, _key_path, reason = resolve_tunnel_key(
        auth_token=auth_token,
        base_url=base_url,
        force_refresh=force_refresh,
        ttl_seconds=ttl_seconds,
        session=session,
        extra_headers=extra_headers,
        run_id=run_id,
    )
    return config, reason


def resolve_tunnel_key(
    auth_token: str,
    base_url: str,
    force_refresh: bool = False,
    ttl_seconds: int | None = None,
    session: requests.Session | None = None,
    extra_headers: dict[str, str] | None = None,
    run_id: UUID | str | None = None,
) -> tuple[TunnelConfig | None, str | None, str | None]:
    """
    Resolve tunnel configuration while keeping Cloudflare bootstrap calls minimal.

    Order:
    1. Use cached config when key/cache are still valid and refresh is not forced.
    2. Use GET /client/ssh/tunnel when key exists and remains valid.
    3. Register/re-register via POST /client/ssh/tunnel.

    Returns ``(config, private_key_path, reason)``. ``run_id`` names the
    on-disk key directory so two runs on the same host never share or race on
    one keypair. Without a ``run_id``, every caller on the host shares one key.
    """
    try:
        existing = find_existing_key_dir(run_id)
    except ValueError as exc:
        return None, None, str(exc)

    if existing is not None:
        cached = read_cached_tunnel_config(existing)
        if cached is not None and not force_refresh:
            return cached, existing.private_key, None

        try:
            config = _get_tunnel_connection(
                auth_token=auth_token, base_url=base_url, session=session, extra_headers=extra_headers
            )
            if cached is not None:
                config = replace(
                    config,
                    key_id=config.key_id or cached.key_id,
                    expires_at=config.expires_at or cached.expires_at,
                )
            write_tunnel_cache(existing, config)
            return config, existing.private_key, None
        except TunnelClientError as exc:
            LOGGER.warning("Unable to refresh tunnel connection details via API: %s", exc)

    sweep_stale_tunnel_keys()

    def _register(public_key: str) -> TunnelConfig:
        return _register_tunnel(
            auth_token=auth_token,
            base_url=base_url,
            public_key=public_key,
            ttl_seconds=ttl_seconds,
            session=session,
            extra_headers=extra_headers,
        )

    try:
        config, key_path = generate_and_register_key(run_id, _register)
        return config, key_path, None
    except (OSError, TunnelClientError) as exc:
        LOGGER.warning("Unable to resolve SSH tunnel configuration: %s", exc)
        return None, None, str(exc)


def _register_tunnel(
    auth_token: str,
    base_url: str,
    public_key: str,
    ttl_seconds: int | None = None,
    session: requests.Session | None = None,
    extra_headers: dict[str, str] | None = None,
) -> TunnelConfig:
    payload: dict[str, Any] = {"public_key": public_key}
    if ttl_seconds is not None:
        payload["ttl_seconds"] = ttl_seconds
    response = _call_tunnel_api(
        method="POST",
        url=f"{base_url}/api/v1/client/ssh/tunnel",
        auth_token=auth_token,
        payload=payload,
        session=session,
        extra_headers=extra_headers,
    )
    return TunnelConfig.from_api_response(response)


def _get_tunnel_connection(
    auth_token: str,
    base_url: str,
    session: requests.Session | None = None,
    extra_headers: dict[str, str] | None = None,
) -> TunnelConfig:
    response = _call_tunnel_api(
        method="GET",
        url=f"{base_url}/api/v1/client/ssh/tunnel",
        auth_token=auth_token,
        payload=None,
        session=session,
        extra_headers=extra_headers,
    )
    return TunnelConfig.from_api_response(response)


def _call_tunnel_api(
    method: str,
    url: str,
    auth_token: str,
    payload: dict[str, Any] | None,
    session: requests.Session | None = None,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    headers = {
        "Authorization": f"token {auth_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)

    own_session = session is None
    if own_session:
        session = _create_api_session()

    try:
        try:
            if method == "POST":
                response = session.post(url=url, json=payload, headers=headers, timeout=DEFAULT_TUNNEL_TIMEOUT)
            elif method == "GET":
                response = session.get(url=url, headers=headers, timeout=DEFAULT_TUNNEL_TIMEOUT)
            else:
                raise TunnelClientError(f"Unsupported tunnel API method: {method}")
        except requests.RequestException as exc:
            raise TunnelClientError(f"Tunnel API call failed ({method} {url}): {exc}") from exc

        if response.status_code != 200:
            raise TunnelClientError(
                f"Tunnel API call returned unexpected status code {response.status_code} ({method} {url})"
            )

        try:
            response_payload = response.json()
        except ValueError as exc:
            content_type = response.headers.get("Content-Type", "<missing>")
            body_preview = response.text[:500] if response.text else "<empty>"
            LOGGER.debug(
                "Tunnel API JSON parse failure: Content-Type=%s, body=%s",
                content_type,
                body_preview,
            )
            raise TunnelClientError(
                f"Tunnel API response is not JSON ({method} {url}): "
                f"Content-Type={content_type}, body_preview={body_preview!r}"
            ) from exc

        if not isinstance(response_payload, dict):
            raise TunnelClientError(f"Tunnel API response payload has invalid format ({method} {url})")

        if response_payload.get("status") != "ok":
            response_error = response_payload.get("response")
            message = response_error.get("message") if isinstance(response_error, dict) else response_error
            raise TunnelClientError(f"Tunnel API returned error: {message}")

        response_data = response_payload.get("response")
        if not isinstance(response_data, dict):
            raise TunnelClientError("Tunnel API response payload has invalid format")

        return response_data
    finally:
        if own_session:
            session.close()
