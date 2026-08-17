import atexit
import functools
import logging
import os
import random
import threading
import time
import weakref
from datetime import datetime, timezone
from importlib import metadata

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from argus.client.tunnel import (
    SSHTunnel,
    TunnelConfig,
    resolve_tunnel_config_with_reason,
)

LOGGER = logging.getLogger(__name__)
_DEFAULT_MONITOR_INTERVAL = 5.0
# Delay before the first re-attempt after a failure, doubling up to the maximum.
TUNNEL_RETRY_MIN_SECONDS = 30.0
TUNNEL_RETRY_MAX_SECONDS = 600.0
# Jitter keeps a fleet of CI runners from retrying against one proxy in lockstep.
TUNNEL_RETRY_JITTER = 0.2
# Requests a session must make before the tunnel is worth building. Standing one
# up costs a registration call plus one authorized-keys lookup, both of which
# travel direct, so a process that exits after a couple of requests adds public
# traffic instead of removing it. A long run passes this in seconds.
TUNNEL_MIN_REQUESTS = 10


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
            len(build_id),
            _MAX_BUILD_ID_LEN,
        )
        return None
    return build_id


def _resolve_build_url() -> str | None:
    """Jenkins ``BUILD_URL`` for the run.

    Carried as ``X-Argus-Build-Url`` so the Grafana ``build_id`` series can link
    straight back to the originating build. ``None`` when not running in CI.
    """
    return os.environ.get("BUILD_URL", "").strip() or None


def _resolve_client_version() -> str:
    """The installed argus-alm version, or "unknown" when it cannot be read.

    Tunnel support starts at 0.16.0. A release line that pins an older client
    cannot tunnel at all, and until the backend can see the version it looks
    identical to a job whose tunnel is failing.
    """
    try:
        return metadata.version("argus-alm")
    except metadata.PackageNotFoundError:
        return "unknown"


def build_attribution_headers() -> dict[str, str]:
    """Jenkins attribution for every request, tunneled or not.

    ``TunneledSession._tunnel_headers`` adds the same two headers, but only to a
    request the tunnel actually carried. That left tunnel adoption unmeasurable:
    a job that never opened a tunnel, or that fell back to a direct connection,
    sent no attribution at all, and its traffic became indistinguishable from a
    browser hitting the UI. Setting them at session level lets the backend name
    the job behind direct traffic too.
    """
    headers = {"X-Argus-Client-Version": _resolve_client_version()}
    if build_id := _resolve_build_id():
        headers["X-Argus-Build-Id"] = build_id
    if build_url := _resolve_build_url():
        headers["X-Argus-Build-Url"] = build_url
    return headers


def _resolve_non_negative_float(env_name: str, default: float) -> float:
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        value = float(raw)
        if value < 0:
            raise ValueError("value must not be negative")
        return value
    except ValueError:
        LOGGER.warning("Invalid %s=%r, using default %s", env_name, raw, default)
        return default


def _resolve_positive_float(env_name: str, default: float) -> float:
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        value = float(raw)
        if value <= 0:
            raise ValueError("value must be positive")
        return value
    except ValueError:
        LOGGER.warning("Invalid %s=%r, using default %.1fs", env_name, raw, default)
        return default


def _resolve_monitor_interval() -> float:
    return _resolve_positive_float("ARGUS_TUNNEL_MONITOR_INTERVAL", _DEFAULT_MONITOR_INTERVAL)


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
    and only override :meth:`request` to rewrite the URL.

    The ``argus-tunnel-monitor`` thread owns every tunnel state transition. It
    establishes the tunnel, watches its health, and keeps re-attempting after a
    failure with an escalating jittered delay. :meth:`request` never spawns SSH
    and never blocks on it, so a dead proxy costs a direct request instead of a
    stalled caller. Once the monitor lands a tunnel, later requests pick it up
    with no restart.
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
        # Set by a request thread when traffic through the tunnel fails. The
        # monitor tears the tunnel down on its next tick instead of trusting
        # the liveness probe.
        self._tunnel_suspect = False

        self._retry_min = _resolve_positive_float("ARGUS_TUNNEL_RETRY_MIN_SECONDS", TUNNEL_RETRY_MIN_SECONDS)
        self._retry_max = _resolve_positive_float("ARGUS_TUNNEL_RETRY_MAX_SECONDS", TUNNEL_RETRY_MAX_SECONDS)
        self._retry_delay = self._retry_min
        self._next_retry_at = 0.0

        # Held only by the monitor thread while it mutates tunnel state. The
        # request path reads ``_tunnel_port`` without it.
        self._lock = threading.RLock()

        self._min_requests = int(_resolve_non_negative_float("ARGUS_TUNNEL_MIN_REQUESTS", TUNNEL_MIN_REQUESTS))
        self._request_count = 0
        self._first_attempt_done = threading.Event()

        monitor_interval = _resolve_monitor_interval()
        self._monitor_stop = threading.Event()
        self._wake = threading.Event()
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
        # Per-instance callable. ``_atexit_close`` is a staticmethod, so every
        # session shares one function object and one unregister would drop the
        # hook of every other live session.
        self._atexit_callback = functools.partial(self._atexit_close, self._atexit_ref)
        atexit.register(self._atexit_callback)

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
            return tunnel_url + url[len(self._original_base_url) :]
        return url

    def _attempt_tunnel(self) -> None:
        """Establish a tunnel. Called by the monitor thread only."""
        with self._lock:
            force_refresh = self._tunnel_config is not None
            # Use session=None (creates a plain requests.Session inside
            # tunnel_api) to avoid infinite recursion: passing `self` would
            # rewrite the URL through the tunnel we are trying to build. We
            # forward session-level headers (e.g. Cloudflare Access tokens)
            # via extra_headers instead.
            extra_headers = dict(self.headers) if self.headers else None
            config, config_reason = resolve_tunnel_config_with_reason(
                auth_token=self._auth_token,
                base_url=self._original_base_url,
                force_refresh=force_refresh,
                session=None,
                extra_headers=extra_headers,
            )
            if config is None:
                self._teardown(config_reason or "failed to resolve tunnel configuration")
                return

            tunnel = SSHTunnel()
            config, local_port, establish_reason = self._establish_any(tunnel, config)

            if local_port is None and not force_refresh:
                # The cached config may name a proxy that has since been
                # retired. Re-fetch the live list once before giving up.
                fresh, config_reason = resolve_tunnel_config_with_reason(
                    auth_token=self._auth_token,
                    base_url=self._original_base_url,
                    force_refresh=True,
                    session=None,
                    extra_headers=extra_headers,
                )
                if fresh is not None:
                    config, local_port, establish_reason = self._establish_any(tunnel, fresh)
                else:
                    establish_reason = config_reason

            if local_port is None or config is None:
                tunnel.shutdown()
                self._teardown(establish_reason or "failed to establish tunnel")
                return

            was_down = self._tunnel_warning_emitted
            self._tunnel = tunnel
            self._tunnel_config = config
            self._tunnel_port = local_port
            self._tunnel_established_at = datetime.now(timezone.utc).isoformat()
            self._tunnel_suspect = False
            self._tunnel_warning_emitted = False
            self._retry_delay = self._retry_min
            self._next_retry_at = 0.0
            LOGGER.info(
                "SSH tunnel %s: proxy=%s:%d, user=%s, key_id=%s, local_port=%d",
                "restored" if was_down else "established",
                config.proxy_host,
                config.proxy_port,
                config.proxy_user,
                config.key_id or "unknown",
                local_port,
            )

    @staticmethod
    def _establish_any(
        tunnel: SSHTunnel,
        config: TunnelConfig,
    ) -> tuple[TunnelConfig | None, int | None, str | None]:
        """Try each proxy in turn and return the one that came up.

        A dead primary otherwise costs the whole backoff window on the public
        endpoint, which is the cost this tunnel exists to avoid.
        """
        candidates = config.candidates()
        last_reason: str | None = None
        for index, candidate in enumerate(candidates, start=1):
            local_port, reason = tunnel.establish(candidate)
            if local_port is not None:
                if index > 1:
                    LOGGER.info(
                        "SSH tunnel failed over to proxy %s:%d (candidate %d of %d)",
                        candidate.proxy_host,
                        candidate.proxy_port,
                        index,
                        len(candidates),
                    )
                return candidate, local_port, None
            last_reason = f"{candidate.proxy_host}:{candidate.proxy_port}: {reason}"
            if index < len(candidates):
                LOGGER.warning("SSH tunnel proxy unreachable, trying the next one — %s", last_reason)
        return None, None, last_reason

    def _teardown(self, reason: str) -> None:
        """Drop the tunnel, route traffic direct, and schedule the next attempt.

        The cached keypair stays on disk. It remains valid while the proxy host
        is unreachable, and regenerating it on every retry would force a
        pointless re-registration round-trip over Cloudflare.
        """
        if not self._tunnel_warning_emitted:
            LOGGER.warning(
                "SSH tunnel unavailable (%s); using a direct connection to %s",
                reason,
                self._original_base_url,
            )
            self._tunnel_warning_emitted = True
        else:
            LOGGER.debug("SSH tunnel attempt failed: %s", reason)

        if self._tunnel:
            self._tunnel.shutdown()

        self._tunnel = None
        self._tunnel_config = None
        self._tunnel_port = None
        self._tunnel_established_at = None
        self._tunnel_suspect = False
        self._schedule_retry()

    def _schedule_retry(self) -> None:
        delay = min(self._retry_delay, self._retry_max)
        jitter = delay * TUNNEL_RETRY_JITTER
        wait = random.uniform(delay - jitter, delay + jitter)
        self._next_retry_at = time.monotonic() + wait
        self._retry_delay = min(delay * 2, self._retry_max)
        LOGGER.debug("Next SSH tunnel attempt in %.1fs", wait)

    def _monitor_loop(self, interval: float) -> None:
        while not self._monitor_stop.is_set():
            try:
                self._monitor_tick()
            except Exception:  # noqa: BLE001
                LOGGER.debug("SSH tunnel monitor failed", exc_info=True)
            finally:
                self._first_attempt_done.set()

            if self._monitor_stop.is_set():
                break
            if self._wake.wait(self._next_wait(interval)):
                self._wake.clear()

        # close() may have timed out waiting for this thread while a tunnel was
        # coming up. Do not leave the SSH process behind.
        with self._lock:
            if self._tunnel is not None:
                self._tunnel.shutdown()
                self._tunnel = None
                self._tunnel_port = None

    def _next_wait(self, interval: float) -> float:
        """Seconds to sleep before the next tick."""
        if self._tunnel is not None or not self._worth_tunnelling():
            return interval
        return max(0.1, min(interval, self._next_retry_at - time.monotonic()))

    def _monitor_tick(self) -> None:
        tunnel = self._tunnel
        if tunnel is not None:
            if not self._tunnel_suspect and tunnel.is_alive() and tunnel.local_port is not None:
                self._tunnel_port = tunnel.local_port
                return
            with self._lock:
                self._teardown("tunnel stopped responding")
                # A tunnel that was working earns a fresh ladder: one immediate
                # re-attempt, then the delay escalates from the minimum again.
                self._retry_delay = self._retry_min
                self._next_retry_at = 0.0

        # Nothing below costs anything until the session proves it will make
        # enough requests to pay for the handshake.
        if not self._worth_tunnelling():
            return
        if time.monotonic() < self._next_retry_at:
            return
        self._attempt_tunnel()

    def _report_tunnel_failure(self) -> None:
        """Flag a broken tunnel from a request thread.

        Deliberately lock-free. The monitor can hold the lock for tens of
        seconds while it spawns SSH, and a request must never wait on that.
        """
        # Unlocked, so a monitor tick can still read a stale port between these
        # two writes and re-publish it for one tick. The next tick tears it down.
        self._tunnel_suspect = True
        self._tunnel_port = None
        self._wake.set()

    def _worth_tunnelling(self) -> bool:
        """Whether this session has made enough requests to repay a handshake."""
        return self._request_count >= self._min_requests

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
        self._request_count += 1
        if self._request_count == self._min_requests:
            # Tell the monitor now rather than letting it find out on its next
            # tick, so a busy session starts tunnelling without a 5s delay.
            self._wake.set()
        rewritten = self._rewrite_url(url)
        if rewritten == url:
            return super().request(method, url, *args, **kwargs)

        # Copy rather than mutate: kwargs["headers"] belongs to the caller, and
        # the direct retry below must not carry the tunnel headers.
        tunnel_kwargs = dict(kwargs)
        headers = dict(tunnel_kwargs.get("headers") or {})
        headers.update(self._tunnel_headers())
        tunnel_kwargs["headers"] = headers

        try:
            return super().request(method, rewritten, *args, **tunnel_kwargs)
        except requests.ConnectionError as exc:
            LOGGER.warning("%s through the SSH tunnel failed (%s); using a direct connection", method, exc)
            self._report_tunnel_failure()
            return super().request(method, url, *args, **kwargs)

    def close(self) -> None:
        self._monitor_stop.set()
        self._wake.set()
        if self._monitor_thread.is_alive() and threading.current_thread() is not self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        with self._lock:
            if self._tunnel:
                self._tunnel.shutdown()
                self._tunnel = None
                self._tunnel_config = None
                self._tunnel_port = None
        try:
            atexit.unregister(self._atexit_callback)
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
        session = TunneledSession(auth_token=auth_token, original_base_url=base_url, max_retries=max_retries)
    else:
        session = _build_retry_session(max_retries)
    # Both branches, so that a job which never opens a tunnel, or which falls
    # back to a direct connection, is still named in the backend metrics.
    session.headers.update(build_attribution_headers())
    return session
