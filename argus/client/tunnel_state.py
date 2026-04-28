import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        PublicFormat,
    )
except ImportError:
    Ed25519PrivateKey = None
    Encoding = None
    NoEncryption = None
    PrivateFormat = None
    PublicFormat = None

from argus.client.tunnel_models import TunnelClientError, TunnelConfig, TunnelStatePaths, parse_datetime


LOGGER = logging.getLogger(__name__)


def get_tunnel_state_paths() -> TunnelStatePaths:
    state_dir = _resolve_state_dir()
    return TunnelStatePaths(
        state_dir=state_dir,
        private_key=state_dir / "id_argus_proxy",
        public_key=state_dir / "id_argus_proxy.pub",
        key_meta=state_dir / "id_argus_proxy.meta.json",
        config_cache=state_dir / "tunnel_config.json",
    )


def delete_cached_tunnel_state() -> None:
    """Delete cached tunnel key/config state used by the client."""
    try:
        paths = get_tunnel_state_paths()
    except OSError:
        return

    for file_path in (paths.private_key, paths.public_key, paths.key_meta, paths.config_cache):
        try:
            file_path.unlink(missing_ok=True)
        except OSError:
            LOGGER.debug("Failed removing cached tunnel state file: %s", file_path, exc_info=True)


def generate_keypair_if_needed(paths: TunnelStatePaths) -> None:
    if is_key_valid(paths):
        return

    if _generate_keypair_with_cryptography(paths):
        return

    if shutil.which("ssh-keygen") is None:
        raise TunnelClientError("ssh-keygen binary is required to generate SSH keypair")

    paths.private_key.unlink(missing_ok=True)
    paths.public_key.unlink(missing_ok=True)

    result = subprocess.run(  # noqa: S603
        [
            "ssh-keygen",
            "-q",
            "-t",
            "ed25519",
            "-N",
            "",
            "-f",
            str(paths.private_key),
            "-C",
            "argus-proxy",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise TunnelClientError(f"ssh-keygen failed: {stderr or 'unknown error'}")

    paths.private_key.chmod(0o600)
    paths.public_key.chmod(0o644)


def _generate_keypair_with_cryptography(paths: TunnelStatePaths) -> bool:
    if Ed25519PrivateKey is None:
        return False

    try:
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key()

        private_bytes = private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.OpenSSH,
            encryption_algorithm=NoEncryption(),
        )
        public_bytes = public_key.public_bytes(
            encoding=Encoding.OpenSSH,
            format=PublicFormat.OpenSSH,
        )

        paths.private_key.write_bytes(private_bytes)
        paths.public_key.write_bytes(public_bytes + b"\n")
        paths.private_key.chmod(0o600)
        paths.public_key.chmod(0o644)
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Falling back to ssh-keygen due to cryptography key generation failure: %s", exc)
        return False


def is_key_valid(paths: TunnelStatePaths) -> bool:
    if not paths.private_key.exists() or not paths.public_key.exists() or not paths.key_meta.exists():
        return False

    try:
        key_meta = json.loads(paths.key_meta.read_text(encoding="utf-8"))
        expires_at = parse_datetime(key_meta.get("expires_at"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return False

    now = datetime.now(tz=UTC)
    return now < expires_at


def write_key_meta(paths: TunnelStatePaths, expires_at: datetime | None) -> None:
    if expires_at is None:
        return
    payload = {"expires_at": expires_at.astimezone(UTC).isoformat()}
    paths.key_meta.write_text(json.dumps(payload), encoding="utf-8")
    paths.key_meta.chmod(0o600)


def read_cached_tunnel_config(paths: TunnelStatePaths) -> TunnelConfig | None:
    if not paths.config_cache.exists():
        return None

    try:
        payload = json.loads(paths.config_cache.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    try:
        config = TunnelConfig.from_api_response(payload)
    except TunnelClientError:
        return None

    if config.expires_at is not None and datetime.now(tz=UTC) >= config.expires_at:
        return None

    return config


def write_tunnel_cache(paths: TunnelStatePaths, config: TunnelConfig) -> None:
    paths.config_cache.write_text(json.dumps(config.to_cache_payload()), encoding="utf-8")
    paths.config_cache.chmod(0o600)


def _resolve_state_dir() -> Path:
    candidates: list[Path] = []

    if configured_dir := os.environ.get("ARGUS_TUNNEL_STATE_DIR"):
        candidates.append(Path(configured_dir).expanduser())

    candidates.append(Path.home() / ".ssh")

    if runtime_dir := os.environ.get("XDG_RUNTIME_DIR"):
        candidates.append(Path(runtime_dir) / "argus-tunnel")

    candidates.append(Path(tempfile.gettempdir()) / f"argus-tunnel-{os.getuid()}")

    for candidate in candidates:
        if _prepare_state_dir(candidate):
            return candidate

    raise OSError("No writable directory available for SSH tunnel state")


def _prepare_state_dir(path: Path) -> bool:
    try:
        if path.exists():
            if not path.is_dir():
                return False
            if not os.access(path, os.W_OK | os.X_OK):
                return False
            return True

        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        path.chmod(0o700)
        return True
    except OSError:
        return False
