import json
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime

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
        private_key=os.path.join(state_dir, "id_argus_proxy"),
        public_key=os.path.join(state_dir, "id_argus_proxy.pub"),
        key_meta=os.path.join(state_dir, "id_argus_proxy.meta.json"),
        config_cache=os.path.join(state_dir, "tunnel_config.json"),
    )


def delete_cached_tunnel_state() -> None:
    """Delete cached tunnel key/config state used by the client."""
    try:
        paths = get_tunnel_state_paths()
    except OSError:
        return

    for file_path in (paths.private_key, paths.public_key, paths.key_meta, paths.config_cache):
        try:
            _unlink(file_path)
        except OSError:
            LOGGER.debug("Failed removing cached tunnel state file: %s", file_path, exc_info=True)


def generate_keypair_if_needed(paths: TunnelStatePaths) -> None:
    if is_key_valid(paths):
        return

    if _generate_keypair_with_cryptography(paths):
        return

    if shutil.which("ssh-keygen") is None:
        raise TunnelClientError("ssh-keygen binary is required to generate SSH keypair")

    _unlink(paths.private_key)
    _unlink(paths.public_key)

    result = subprocess.run(  # noqa: S603
        [
            "ssh-keygen",
            "-q",
            "-t",
            "ed25519",
            "-N",
            "",
            "-f",
            paths.private_key,
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

    os.chmod(paths.private_key, 0o600)
    os.chmod(paths.public_key, 0o644)


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

        _write_bytes(paths.private_key, private_bytes)
        _write_bytes(paths.public_key, public_bytes + b"\n")
        os.chmod(paths.private_key, 0o600)
        os.chmod(paths.public_key, 0o644)
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Falling back to ssh-keygen due to cryptography key generation failure: %s", exc)
        return False


def is_key_valid(paths: TunnelStatePaths) -> bool:
    if not os.path.exists(paths.private_key) or not os.path.exists(paths.public_key) or not os.path.exists(paths.key_meta):
        return False

    try:
        key_meta = json.loads(_read_text(paths.key_meta))
        expires_at = parse_datetime(key_meta.get("expires_at"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return False

    now = datetime.now(tz=UTC)
    return now < expires_at


def write_key_meta(paths: TunnelStatePaths, expires_at: datetime | None) -> None:
    if expires_at is None:
        return
    payload = {"expires_at": expires_at.astimezone(UTC).isoformat()}
    _write_text(paths.key_meta, json.dumps(payload))
    os.chmod(paths.key_meta, 0o600)


def read_cached_tunnel_config(paths: TunnelStatePaths) -> TunnelConfig | None:
    if not os.path.exists(paths.config_cache):
        return None

    try:
        payload = json.loads(_read_text(paths.config_cache))
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
    _write_text(paths.config_cache, json.dumps(config.to_cache_payload()))
    os.chmod(paths.config_cache, 0o600)


def _resolve_state_dir() -> str:
    candidates: list[str] = []

    if configured_dir := os.environ.get("ARGUS_TUNNEL_STATE_DIR"):
        candidates.append(os.path.expanduser(configured_dir))

    candidates.append(os.path.join(os.path.expanduser("~"), ".ssh"))

    if runtime_dir := os.environ.get("XDG_RUNTIME_DIR"):
        candidates.append(os.path.join(runtime_dir, "argus-tunnel"))

    candidates.append(os.path.join(tempfile.gettempdir(), f"argus-tunnel-{os.getuid()}"))

    for candidate in candidates:
        if _prepare_state_dir(candidate):
            return candidate

    raise OSError("No writable directory available for SSH tunnel state")


def _prepare_state_dir(path: str) -> bool:
    try:
        if os.path.exists(path):
            if not os.path.isdir(path):
                return False
            if not os.access(path, os.W_OK | os.X_OK):
                return False
            return True

        os.makedirs(path, mode=0o700, exist_ok=True)
        os.chmod(path, 0o700)
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Small helpers to replace pathlib method calls with plain os / builtins
# ---------------------------------------------------------------------------

def _unlink(path: str) -> None:
    """Remove a file; silently ignore if it does not exist."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _read_text(path: str, encoding: str = "utf-8") -> str:
    with open(path, encoding=encoding) as fh:
        return fh.read()


def _write_text(path: str, text: str, encoding: str = "utf-8") -> None:
    with open(path, "w", encoding=encoding) as fh:
        fh.write(text)


def _write_bytes(path: str, data: bytes) -> None:
    with open(path, "wb") as fh:
        fh.write(data)
