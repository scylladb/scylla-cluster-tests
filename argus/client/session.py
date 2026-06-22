import atexit
import logging
import os
import threading
import time
import weakref
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from argus.client.tunnel import (
    SSHTunnel,
    TunnelConfig,
    resolve_tunnel_config_with_reason,
)

LOGGER = logging.getLogger(__name__)
TUNNEL_COOLDOWN_SECONDS = 30
_DEFAULT_MONITOR_INTERVAL = 5.0


def _resolve_use_tunnel(use_tunnel: bool | None) -> bool:
    if use_tunnel is not None:
        return use_tunnel
    return os.environ.get("ARGUS_USE_TUNNEL", "").strip().lower() in ("1", "true", "yes", "on")


_MAX_BUILD_ID_LEN = 256


def _resolve_build_id() -> str | None:
    """Resolve a human-identifiable build id for tunnel attribution.

    Combines the Jenkins ``JOB_NAME`` (full folder path) with ``BUILD_NUMBER``
    when available, formatted as ``job/path#42``. Returns ``None`` outside CI
    (header omitted) or when the value exceeds ``_MAX_BUILD_ID_LEN``.
    """
    job_name = os.environ.get("JOB_NAME", "").strip()
    if not job_name:
        return None
    build_number = os.environ.get("BUILD_NUMBER", "").strip()
    build_id = f"{job_name}#{build_number}" if build_number else job_name
    if len(build_id) > _MAX_BUILD_ID_LEN:
        LOGGER.warning(
            "Jenkins build id is %d chars (> %d); omitting tunnel attribution header",
            len(build_id), _MAX_BUILD_ID_LEN,
        )
        return None
    return build_id


def _resolve_build_url() -> str | None:
    """Jenkins ``BUILD_URL`` for the run.

    Carried as ``X-Argus-Build-Url`` so the Grafana ``build_id`` series can link
    straight back to the originating build. ``None`` when not running in CI.
    """
    return os.environ.get("BUILD_URL", "").strip() or None


def _resolve_monitor_interval() -> float:
    raw = os.environ.get("ARGUS_TUNNEL_MONITOR_INTERVAL")
    if raw is None:
        return _DEFAULT_MONITOR_INTERVAL
    try:
        value = float(raw)
        if value <= 0:
            raise ValueError("interval must be positive")
        return value
    except ValueError:
        LOGGER.warning(
            "Invalid ARGUS_TUNNEL_MONITOR_INTERVAL=%r, using default %.1fs",
            raw,
            _DEFAULT_MONITOR_INTERVAL,
        )
        return _DEFAULT_MONITOR_INTERVAL


def _build_retry_adapter(max_retries: int) -> HTTPAdapter:
    retry_strategy = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=0,
        backoff_factor=1,
        status_forcelist=(),
        allowed_methods=["GET", "POST"],
    )
    return HTTPAdapter(max_retries=retry_strategy)


def _build_retry_session(max_retries: int) -> requests.Session:
    session = requests.Session()
    adapter = _build_retry_adapter(max_retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class TunneledSession(requests.Session):
    """``requests.Session`` that transparently routes traffic through an SSH tunnel.

    All HTTP verbs work out of the box because we subclass ``requests.Session``
    and only override :meth:`request` to inject URL rewriting plus single-shot
    reconnect-and-retry on connection errors.
    """

    def __init__(self, auth_token: str, original_base_url: str, max_retries: int = 3) -> None:
        super().__init__()
        adapter = _build_retry_adapter(max_retries)
        self.mount("http://", adapter)
        self.mount("https://", adapter)

        self._auth_token = auth_token
        self._original_base_url = original_base_url
        self._build_id = _resolve_build_id()
        self._build_url = _resolve_build_url()

        self._tunnel: SSHTunnel | None = None
        self._tunnel_config: TunnelConfig | None = None
        self._tunnel_port: int | None = None
        self._tunnel_established_at: str | None = None
        self._tunnel_warning_emitted = False
        self._tunnel_disabled_until = 0.0
        # RLock is held while reconnecting; this can stall a request thread for
        # up to ~100s during the SSH retry budget. Acceptable: concurrent
        # request callers should observe a single coherent tunnel state and not
        # race multiple SSH spawns.
        self._lock = threading.RLock()

        monitor_interval = _resolve_monitor_interval()
        self._monitor_stop = threading.Event()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(monitor_interval,),
            name="argus-tunnel-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        # Ensure the monitor thread is stopped at interpreter exit even if a
        # caller forgets to invoke ``close()``. The atexit registration uses a
        # weak reference so the session can be garbage collected normally.
        self._atexit_ref = weakref.ref(self)
        atexit.register(self._atexit_close, self._atexit_ref)

    @staticmethod
    def _atexit_close(session_ref: weakref.ref) -> None:
        session = session_ref()
        if session is not None:
            try:
                session.close()
            except Exception:  # noqa: BLE001
                LOGGER.debug("SSH tunnel atexit close failed", exc_info=True)

    def _active_tunnel_url(self) -> str | None:
        if self._tunnel_port is not None:
            return f"http://127.0.0.1:{self._tunnel_port}"
        return None

    def _rewrite_url(self, url: str) -> str:
        tunnel_url = self._active_tunnel_url()
        if tunnel_url and url.startswith(self._original_base_url):
            return tunnel_url + url[len(self._original_base_url):]
        return url

    def _ensure_tunnel(self) -> None:
        with self._lock:
            if time.monotonic() < self._tunnel_disabled_until:
                return

            if self._tunnel and self._tunnel.is_alive() and self._tunnel.local_port is not None:
                self._tunnel_port = self._tunnel.local_port
                return

            if self._tunnel and self._tunnel_config:
                reconnected_port, reconnect_reason = self._tunnel.reconnect(self._tunnel_config)
                if reconnected_port is not None:
                    self._tunnel_port = reconnected_port
                    self._tunnel_warning_emitted = False
                    return
                if reconnect_reason:
                    LOGGER.warning("SSH tunnel reconnect failed: %s", reconnect_reason)

            force_refresh = self._tunnel is not None
            # Use session=None (creates a plain requests.Session inside
            # tunnel_api) to avoid infinite recursion: passing `self` would
            # trigger _ensure_tunnel() again when the tunnel API call invokes
            # session.post(). We forward session-level headers (e.g.
            # Cloudflare Access tokens) via extra_headers instead.
            extra_headers = dict(self.headers) if self.headers else None
            config, config_reason = resolve_tunnel_config_with_reason(
                auth_token=self._auth_token,
                base_url=self._original_base_url,
                force_refresh=force_refresh,
                session=None,
                extra_headers=extra_headers,
            )
            if config is None:
                self._backoff(config_reason or "failed to resolve tunnel configuration")
                return

            tunnel = SSHTunnel()
            local_port, establish_reason = tunnel.establish(config)

            if local_port is None and not force_refresh:
                config, config_reason = resolve_tunnel_config_with_reason(
                    auth_token=self._auth_token,
                    base_url=self._original_base_url,
                    force_refresh=True,
                    session=None,
                    extra_headers=extra_headers,
                )
                if config is not None:
                    local_port, establish_reason = tunnel.establish(config)
                else:
                    establish_reason = config_reason

            if local_port is None:
                self._backoff(establish_reason or "failed to establish tunnel")
                return

            assert config is not None  # guaranteed: local_port is set only when config is valid
            self._tunnel = tunnel
            self._tunnel_config = config
            self._tunnel_port = local_port
            self._tunnel_established_at = datetime.now(UTC).isoformat()
            self._tunnel_warning_emitted = False
            self._tunnel_disabled_until = 0.0
            LOGGER.info(
                "SSH tunnel established: proxy=%s:%d, user=%s, key_id=%s, local_port=%d",
                config.proxy_host,
                config.proxy_port,
                config.proxy_user,
                config.key_id or "unknown",
                local_port,
            )

    def _backoff(self, reason: str) -> None:
        # We deliberately do NOT delete the cached keypair here. The keypair
        # remains valid even when the proxy host is briefly unreachable, and
        # regenerating it on every 30s cooldown would force an unnecessary
        # re-registration round-trip. ``resolve_tunnel_config_with_reason``
        # already issues ``force_refresh=True`` after the first failure, which
        # re-fetches the live config without dropping the keypair.
        if not self._tunnel_warning_emitted:
            LOGGER.warning(
                "SSH tunnel unavailable (%s); falling back to direct connection: %s",
                reason,
                self._original_base_url,
            )
            self._tunnel_warning_emitted = True

        if self._tunnel:
            self._tunnel.shutdown()

        self._tunnel = None
        self._tunnel_config = None
        self._tunnel_port = None
        self._tunnel_established_at = None
        self._tunnel_disabled_until = time.monotonic() + TUNNEL_COOLDOWN_SECONDS

    def _monitor_loop(self, interval: float) -> None:
        while not self._monitor_stop.wait(interval):
            try:
                self._check_tunnel_health()
            except Exception:  # noqa: BLE001
                LOGGER.debug("SSH tunnel monitor failed", exc_info=True)

    def _check_tunnel_health(self) -> None:
        tunnel = self._tunnel
        if tunnel is None or tunnel.is_alive():
            return
        LOGGER.warning("SSH tunnel monitor detected dead tunnel; reconnecting")
        self._ensure_tunnel()

    def _tunnel_headers(self) -> dict[str, str]:
        """Return headers to attach when traffic flows through the SSH tunnel."""
        if self._tunnel_port is None or self._tunnel_config is None:
            return {}
        headers = {
            "User-Agent": "argus-client-ssh-tunnel",
            "X-SSH-Tunnel-Origin": self._tunnel_config.proxy_host,
            "X-Tunnel-Established-At": self._tunnel_established_at or "",
        }
        if self._tunnel_config.key_id:
            headers["X-Forwarded-Key-ID"] = self._tunnel_config.key_id
        if self._build_id:
            headers["X-Argus-Build-Id"] = self._build_id
        if self._build_url:
            headers["X-Argus-Build-Url"] = self._build_url
        return headers

    def request(self, method: str, url: str, *args, **kwargs) -> requests.Response:
        self._ensure_tunnel()
        rewritten = self._rewrite_url(url)
        if rewritten != url:
            # Traffic is going through the tunnel — inject tunnel headers
            headers = kwargs.get("headers") or {}
            headers.update(self._tunnel_headers())
            kwargs["headers"] = headers
        try:
            return super().request(method, rewritten, *args, **kwargs)
        except requests.ConnectionError:
            if rewritten == url:
                raise
            LOGGER.warning("%s through SSH tunnel failed; reconnecting and retrying", method)
            self._ensure_tunnel()
            rewritten = self._rewrite_url(url)
            if rewritten != url:
                headers = kwargs.get("headers") or {}
                headers.update(self._tunnel_headers())
                kwargs["headers"] = headers
            try:
                return super().request(method, rewritten, *args, **kwargs)
            except requests.ConnectionError as exc:
                self._backoff(f"request retry failed: {exc}")
                return super().request(method, self._rewrite_url(url), *args, **kwargs)

    def close(self) -> None:
        self._monitor_stop.set()
        with self._lock:
            if self._tunnel:
                self._tunnel.shutdown()
                self._tunnel = None
                self._tunnel_config = None
                self._tunnel_port = None
        try:
            atexit.unregister(self._atexit_close)
        except Exception:  # noqa: BLE001
            pass
        super().close()


def create_session(
    auth_token: str,
    base_url: str,
    use_tunnel: bool | None,
    max_retries: int = 3,
) -> requests.Session:
    if _resolve_use_tunnel(use_tunnel):
        return TunneledSession(auth_token=auth_token, original_base_url=base_url, max_retries=max_retries)
    return _build_retry_session(max_retries)
