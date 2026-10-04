from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from typing import Any, TypedDict


class TunnelClientError(Exception):
    pass


DEFAULT_TUNNEL_TIMEOUT = 10
MAX_PORT_BIND_ATTEMPTS = 10
ALLOWED_HOST_KEY_TYPES = (
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
)


# Required/optional keys are split across a base class plus a ``total=False``
# subclass instead of ``typing.NotRequired`` so the module imports on Python
# 3.10 (``NotRequired`` only landed in ``typing`` in 3.11).
class _TunnelApiResponseRequired(TypedDict):
    """Required fields of the ``/client/ssh/tunnel`` response (POST register / GET fetch)."""

    proxy_host: str
    proxy_port: int
    proxy_user: str
    target_host: str
    target_port: int
    host_key_fingerprint: str


class _TunnelApiResponse(_TunnelApiResponseRequired, total=False):
    """Live response shape, with optional fields layered on the required base."""

    expires_at: str | None
    key_id: str | None
    tunnel_id: str | None
    # Every active proxy, best choice first. Absent on older backends.
    proxies: list[dict[str, Any]] | None


class _TunnelCachePayloadRequired(TypedDict):
    """Required fields of the on-disk cache written by :meth:`TunnelConfig.to_cache_payload`."""

    proxy_host: str
    proxy_port: int
    proxy_user: str
    target_host: str
    target_port: int
    host_key_fingerprint: str


class _TunnelCachePayload(_TunnelCachePayloadRequired, total=False):
    """On-disk cache shape, with optional fields layered on the required base.

    Mirrors :class:`_TunnelApiResponse` but is independently typed so future
    cache-only fields don't leak into the API contract.
    """

    expires_at: str | None
    key_id: str | None
    tunnel_id: str | None
    proxies: list[dict[str, Any]] | None


# Required keys listed explicitly to avoid relying on TypedDict.__required_keys__
# runtime behaviour, which changed in Python 3.14.
_TUNNEL_API_REQUIRED_KEYS: tuple[str, ...] = (
    "proxy_host",
    "proxy_port",
    "proxy_user",
    "target_host",
    "target_port",
    "host_key_fingerprint",
)


@dataclass(frozen=True, slots=True)
class TunnelConfig:
    proxy_host: str
    proxy_port: int
    proxy_user: str
    target_host: str
    target_port: int
    host_key_fingerprint: str
    expires_at: datetime | None = None
    key_id: str | None = None
    tunnel_id: str | None = None
    # Further proxies to try when this one is unreachable. Empty against a
    # backend that does not send ``proxies``.
    alternates: tuple["TunnelConfig", ...] = ()

    def candidates(self) -> tuple["TunnelConfig", ...]:
        """This proxy first, then every failover proxy in server-given order.

        Alternates are stored as bare endpoints. The key is the same whichever
        proxy answers, so carry it across. Without this the tunnel loses its
        ``X-Forwarded-Key-ID`` attribution the moment it fails over.
        """
        return (
            self,
            *(replace(alternate, key_id=self.key_id, expires_at=self.expires_at) for alternate in self.alternates),
        )

    @classmethod
    def from_api_response(cls, response: "_TunnelApiResponse | _TunnelCachePayload") -> "TunnelConfig":
        missing = [k for k in _TUNNEL_API_REQUIRED_KEYS if not response.get(k)]
        if missing:
            raise TunnelClientError(f"Missing required tunnel response fields: {', '.join(missing)}")

        expires_at = response.get("expires_at")
        primary = (str(response["proxy_host"]), int(response["proxy_port"]))
        return cls(
            proxy_host=primary[0],
            proxy_port=primary[1],
            proxy_user=str(response["proxy_user"]),
            target_host=str(response["target_host"]),
            target_port=int(response["target_port"]),
            host_key_fingerprint=str(response["host_key_fingerprint"]),
            expires_at=parse_datetime(expires_at) if expires_at else None,
            key_id=str(response["key_id"]) if response.get("key_id") else None,
            tunnel_id=str(response["tunnel_id"]) if response.get("tunnel_id") else None,
            alternates=cls._parse_alternates(response.get("proxies"), primary),
        )

    @classmethod
    def _parse_alternates(
        cls,
        raw: list[dict[str, Any]] | None,
        primary: tuple[str, int],
    ) -> tuple["TunnelConfig", ...]:
        """Build the failover list, dropping the primary and any bad entry.

        A malformed alternate must not sink an otherwise usable response, so
        entries that fail to parse are skipped rather than raised.
        """
        alternates: list[TunnelConfig] = []
        for entry in raw or []:
            if not isinstance(entry, dict):
                continue
            try:
                host = str(entry["proxy_host"])
                port = int(entry["proxy_port"])
                if (host, port) == primary:
                    continue
                alternates.append(
                    cls(
                        proxy_host=host,
                        proxy_port=port,
                        proxy_user=str(entry["proxy_user"]),
                        target_host=str(entry["target_host"]),
                        target_port=int(entry["target_port"]),
                        host_key_fingerprint=str(entry["host_key_fingerprint"]),
                        tunnel_id=str(entry["tunnel_id"]) if entry.get("tunnel_id") else None,
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return tuple(alternates)

    def to_cache_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("alternates", None)
        if self.expires_at is not None:
            payload["expires_at"] = self.expires_at.astimezone(timezone.utc).isoformat()
        # Same shape as the API response, so the cache re-parses through
        # ``from_api_response`` with no second code path.
        payload["proxies"] = [
            {
                "proxy_host": candidate.proxy_host,
                "proxy_port": candidate.proxy_port,
                "proxy_user": candidate.proxy_user,
                "target_host": candidate.target_host,
                "target_port": candidate.target_port,
                "host_key_fingerprint": candidate.host_key_fingerprint,
                "tunnel_id": candidate.tunnel_id,
            }
            for candidate in self.candidates()
        ]
        return payload


@dataclass(frozen=True, slots=True)
class TunnelStatePaths:
    state_dir: str
    private_key: str
    public_key: str
    config_cache: str


def parse_datetime(value: str) -> datetime:
    if not value:
        raise ValueError("datetime value is required")
    # Python 3.10's ``datetime.fromisoformat`` rejects the ``Z`` (Zulu) UTC suffix
    # that the tunnel API emits (e.g. ``2026-04-16T12:00:00Z``); normalise it to the
    # explicit ``+00:00`` offset that all supported versions accept.
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
