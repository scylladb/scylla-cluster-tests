import json
import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from uuid import UUID

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

from argus.client.tunnel.models import TunnelClientError, TunnelConfig, TunnelStatePaths


LOGGER = logging.getLogger(__name__)

TUNNELING_SUBDIR = "argus_tunneling"
DIRNAME_EXPIRY_SEPARATOR = ".exp"
STAGING_DIR_PREFIX = ".staging-"
SHARED_KEY_NAME = "shared"
LOCAL_FALLBACK_TTL = timedelta(hours=24)


def canonical_run_id(run_id: UUID | str) -> str:
    try:
        return str(UUID(str(run_id)))
    except ValueError as exc:
        raise ValueError(f"run_id must be a UUID, got {run_id!r}") from exc


def _key_name(run_id: UUID | str | None) -> str:
    return SHARED_KEY_NAME if run_id is None else canonical_run_id(run_id)


def _dirname_for(run_id: UUID | str | None, expires_at: datetime) -> str:
    return f"{_key_name(run_id)}{DIRNAME_EXPIRY_SEPARATOR}{int(expires_at.timestamp())}"


def _parse_dirname(entry: str) -> tuple[str, datetime] | None:
    prefix, separator, suffix = entry.rpartition(DIRNAME_EXPIRY_SEPARATOR)
    if not separator or not prefix or not suffix.isdigit():
        return None
    return prefix, datetime.fromtimestamp(int(suffix), tz=timezone.utc)


def _paths_for_dir(dir_path: str) -> TunnelStatePaths:
    return TunnelStatePaths(
        state_dir=dir_path,
        private_key=os.path.join(dir_path, "key"),
        public_key=os.path.join(dir_path, "key.pub"),
        config_cache=os.path.join(dir_path, "tunnel_config.json"),
    )


def tunneling_root() -> str:
    return os.path.join(_resolve_state_dir(), TUNNELING_SUBDIR)


def _iter_key_entries(root: str) -> Iterator[tuple[str, str, datetime]]:
    try:
        entries = os.listdir(root)
    except OSError:
        return
    for entry in entries:
        parsed = _parse_dirname(entry)
        if parsed is not None:
            yield (entry, *parsed)


def find_existing_key_dir(run_id: UUID | str | None = None) -> TunnelStatePaths | None:
    root = tunneling_root()
    canonical = _key_name(run_id)
    now = datetime.now(tz=timezone.utc)

    newest: tuple[datetime, str] | None = None
    for entry, prefix, expires_at in _iter_key_entries(root):
        if prefix != canonical or now >= expires_at:
            continue
        if not _holds_keypair(_paths_for_dir(os.path.join(root, entry))):
            continue
        if newest is None or expires_at > newest[0]:
            newest = (expires_at, entry)

    if newest is None:
        return None
    return _paths_for_dir(os.path.join(root, newest[1]))


def _holds_keypair(paths: TunnelStatePaths) -> bool:
    return os.path.isfile(paths.private_key) and os.path.isfile(paths.public_key)


def build_key_location(run_id: UUID | str | None, expires_at: datetime | None) -> TunnelStatePaths:
    resolved_expiry = expires_at or (datetime.now(tz=timezone.utc) + LOCAL_FALLBACK_TTL)
    root = tunneling_root()
    return _paths_for_dir(os.path.join(root, _dirname_for(run_id, resolved_expiry)))


def delete_key_dir(paths: TunnelStatePaths) -> None:
    shutil.rmtree(paths.state_dir, ignore_errors=True)


def delete_key_dir_of(private_key: str) -> bool:
    root = tunneling_root()
    state_dir = os.path.dirname(os.path.abspath(private_key))
    if os.path.dirname(state_dir) != os.path.abspath(root) or _parse_dirname(os.path.basename(state_dir)) is None:
        LOGGER.warning("Refusing to delete %s: it is not an SSH tunnel key directory", state_dir)
        return False
    if not os.path.isdir(state_dir):
        return False
    shutil.rmtree(state_dir, ignore_errors=True)
    return True


def delete_cached_tunnel_state(run_id: UUID | str | None = None) -> None:
    try:
        paths = find_existing_key_dir(run_id)
    except ValueError:
        return
    if paths is not None:
        delete_key_dir(paths)


def sweep_stale_tunnel_keys() -> None:
    root = tunneling_root()
    now = datetime.now(tz=timezone.utc)
    for entry, _prefix, expires_at in _iter_key_entries(root):
        if now < expires_at:
            continue
        delete_key_dir(_paths_for_dir(os.path.join(root, entry)))


def generate_and_register_key(
    run_id: UUID | str | None, register: Callable[[str], TunnelConfig]
) -> tuple[TunnelConfig, str]:
    _key_name(run_id)
    root = tunneling_root()
    os.makedirs(root, mode=0o700, exist_ok=True)
    staging_dir = tempfile.mkdtemp(prefix=STAGING_DIR_PREFIX, dir=root)
    staging_paths = _paths_for_dir(staging_dir)
    try:
        _generate_keypair(staging_paths)
        with open(staging_paths.public_key, encoding="utf-8") as fh:
            public_key = fh.read().strip()

        config = register(public_key)
        write_tunnel_cache(staging_paths, config)

        final_paths = build_key_location(run_id, config.expires_at)
        try:
            os.rename(staging_dir, final_paths.state_dir)
        except OSError:
            if not os.path.isdir(final_paths.state_dir):
                raise
            published = read_cached_tunnel_config(final_paths)
            if published is None or not os.path.exists(final_paths.private_key):
                raise
            return published, final_paths.private_key
        return config, final_paths.private_key
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)


def _generate_keypair(paths: TunnelStatePaths) -> None:
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

    if config.expires_at is not None and datetime.now(tz=timezone.utc) >= config.expires_at:
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


def _unlink(path: str) -> None:
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
