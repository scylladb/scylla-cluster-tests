from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NotRequired, TypedDict


class TunnelClientError(Exception):
    pass


DEFAULT_TUNNEL_TIMEOUT = 10
DEFAULT_RECONNECT_RETRIES = 3
MAX_PORT_BIND_ATTEMPTS = 10
ALLOWED_HOST_KEY_TYPES = (
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
)


class _TunnelApiResponse(TypedDict):
    """Live response shape from ``/client/ssh/tunnel`` (POST register / GET fetch)."""
    proxy_host: str
    proxy_port: int
    proxy_user: str
    target_host: str
    target_port: int
    host_key_fingerprint: str
    expires_at: NotRequired[str | None]
    key_id: NotRequired[str | None]
    tunnel_id: NotRequired[str | None]


class _TunnelCachePayload(TypedDict):
    """On-disk cache shape written by :meth:`TunnelConfig.to_cache_payload`.

    Mirrors :class:`_TunnelApiResponse` but is independently typed so future
    cache-only fields don't leak into the API contract.
    """
    proxy_host: str
    proxy_port: int
    proxy_user: str
    target_host: str
    target_port: int
    host_key_fingerprint: str
    expires_at: NotRequired[str | None]
    key_id: NotRequired[str | None]
    tunnel_id: NotRequired[str | None]


# Required keys are derived from the TypedDict at runtime via ``__required_keys__``;
# kept as a module-level set so the dunder access is centralised and easy to swap
# out if/when ``typing.get_type_hints``-based introspection becomes preferable.
_TUNNEL_API_REQUIRED_KEYS: frozenset[str] = frozenset(_TunnelApiResponse.__required_keys__)


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

    @classmethod
    def from_api_response(cls, response: "_TunnelApiResponse | _TunnelCachePayload") -> "TunnelConfig":
        missing = [k for k in _TUNNEL_API_REQUIRED_KEYS if not response.get(k)]
        if missing:
            raise TunnelClientError(f"Missing required tunnel response fields: {', '.join(missing)}")

        expires_at = response.get("expires_at")
        return cls(
            proxy_host=str(response["proxy_host"]),
            proxy_port=int(response["proxy_port"]),
            proxy_user=str(response["proxy_user"]),
            target_host=str(response["target_host"]),
            target_port=int(response["target_port"]),
            host_key_fingerprint=str(response["host_key_fingerprint"]),
            expires_at=parse_datetime(expires_at) if expires_at else None,
            key_id=str(response["key_id"]) if response.get("key_id") else None,
            tunnel_id=str(response["tunnel_id"]) if response.get("tunnel_id") else None,
        )

    def to_cache_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.expires_at is not None:
            payload["expires_at"] = self.expires_at.astimezone(UTC).isoformat()
        return payload


@dataclass(frozen=True, slots=True)
class TunnelStatePaths:
    state_dir: Path
    private_key: Path
    public_key: Path
    key_meta: Path
    config_cache: Path


def parse_datetime(value: str) -> datetime:
    if not value:
        raise ValueError("datetime value is required")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
