import atexit
import base64
import hashlib
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time

from argus.client.tunnel_models import (
    ALLOWED_HOST_KEY_TYPES,
    DEFAULT_RECONNECT_RETRIES,
    DEFAULT_TUNNEL_TIMEOUT,
    MAX_PORT_BIND_ATTEMPTS,
    TunnelClientError,
    TunnelConfig,
)
from argus.client.tunnel_state import get_tunnel_state_paths


LOGGER = logging.getLogger(__name__)


class SSHTunnel:
    def __init__(self, key_path: str | None = None) -> None:
        state_paths = get_tunnel_state_paths()
        self._key_path = key_path or state_paths.private_key
        self._process: subprocess.Popen[str] | None = None
        self._local_port: int | None = None
        self._known_hosts_path: str | None = None
        self._atexit_registered = False

    @property
    def local_port(self) -> int | None:
        return self._local_port

    def establish(self, config: TunnelConfig) -> tuple[int | None, str | None]:
        ssh_bin = shutil.which("ssh")
        if ssh_bin is None:
            reason = "ssh binary was not found on PATH"
            LOGGER.warning(reason)
            return None, reason
        if shutil.which("ssh-keyscan") is None:
            reason = "ssh-keyscan binary was not found on PATH"
            LOGGER.warning(reason)
            return None, reason
        if not os.path.exists(self._key_path):
            reason = f"SSH private key does not exist: {self._key_path}"
            LOGGER.warning(reason)
            return None, reason

        self.shutdown()
        try:
            known_hosts_path = self._prepare_known_hosts_file(config)
        except TunnelClientError as exc:
            reason = f"strict host verification failed: {exc}"
            LOGGER.warning("SSH tunnel %s", reason)
            return None, reason

        for attempt in range(1, MAX_PORT_BIND_ATTEMPTS + 1):
            reserve_socket, local_port = self._reserve_local_port()
            command = self._build_ssh_command(
                config=config,
                local_port=local_port,
                known_hosts_path=known_hosts_path,
                ssh_bin=ssh_bin,
            )

            try:
                reserve_socket.close()
                process = subprocess.Popen(  # noqa: S603
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except OSError as exc:
                reason = f"failed to spawn ssh process: {exc}"
                LOGGER.warning(reason)
                _unlink(known_hosts_path)
                return None, reason

            ready, error_text = self._wait_for_port_ready(process=process, local_port=local_port)
            if ready:
                self._process = process
                self._local_port = local_port
                self._known_hosts_path = known_hosts_path
                self._register_atexit()
                return local_port, None

            self._terminate_process(process)

            if "Address already in use" in error_text:
                LOGGER.warning(
                    "SSH tunnel local bind conflict on attempt %s/%s, retrying with a new local port",
                    attempt,
                    MAX_PORT_BIND_ATTEMPTS,
                )
                continue

            reason = f"establish attempt {attempt} failed: {error_text or 'unknown error'}"
            LOGGER.warning("SSH tunnel %s", reason)
            _unlink(known_hosts_path)
            return None, reason

        reason = f"establish failed after {MAX_PORT_BIND_ATTEMPTS} attempts"
        LOGGER.warning("SSH tunnel %s", reason)
        _unlink(known_hosts_path)
        return None, reason

    def is_alive(self) -> bool:
        if not self._process or self._local_port is None:
            return False
        if self._process.poll() is not None:
            return False
        return is_local_port_open(self._local_port)

    def reconnect(self, config: TunnelConfig) -> tuple[int | None, str | None]:
        self.shutdown()
        last_reason: str | None = None
        for attempt in range(1, DEFAULT_RECONNECT_RETRIES + 1):
            local_port, reason = self.establish(config)
            if local_port is not None:
                return local_port, None
            last_reason = reason
            time.sleep(2 ** (attempt - 1))
        return None, last_reason or "reconnect exhausted"

    def shutdown(self) -> None:
        if self._process is not None:
            self._terminate_process(self._process)
            self._process = None

        self._local_port = None

        if self._known_hosts_path is not None:
            _unlink(self._known_hosts_path)
            self._known_hosts_path = None

    def _register_atexit(self) -> None:
        if self._atexit_registered:
            return
        atexit.register(self.shutdown)
        self._atexit_registered = True

    def _build_ssh_command(self, config: TunnelConfig, local_port: int, known_hosts_path: str, ssh_bin: str = "ssh") -> list[str]:
        return [
            ssh_bin,
            "-N",
            "-L",
            f"127.0.0.1:{local_port}:{config.target_host}:{config.target_port}",
            "-i",
            str(self._key_path),
            "-p",
            str(config.proxy_port),
            f"{config.proxy_user}@{config.proxy_host}",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            f"UserKnownHostsFile={known_hosts_path}",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519,ecdsa-sha2-nistp256,ecdsa-sha2-nistp384,ecdsa-sha2-nistp521",
            "-o",
            "PubkeyAcceptedAlgorithms=ssh-ed25519",
            "-o",
            f"ConnectTimeout={DEFAULT_TUNNEL_TIMEOUT}",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "TCPKeepAlive=yes",
            "-o",
            "ControlMaster=no",
            "-o",
            "LogLevel=ERROR",
        ]

    @staticmethod
    def _reserve_local_port() -> tuple[socket.socket, int]:
        reserve_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        reserve_socket.bind(("127.0.0.1", 0))
        local_port = reserve_socket.getsockname()[1]
        return reserve_socket, local_port

    @staticmethod
    def _wait_for_port_ready(process: subprocess.Popen[str], local_port: int) -> tuple[bool, str]:
        deadline = time.monotonic() + DEFAULT_TUNNEL_TIMEOUT
        while time.monotonic() < deadline:
            if process.poll() is not None:
                return False, read_process_stderr(process)
            if is_local_port_open(local_port):
                return True, ""
            time.sleep(0.1)
        return False, "Timed out waiting for local SSH tunnel port to become reachable"

    @staticmethod
    def _prepare_known_hosts_file(config: TunnelConfig) -> str:
        """Build a temporary known_hosts file covering ``config.proxy_host``.

        Two input shapes are supported, both validated strictly:

        1. **Full known_hosts entry** (``"host keytype keydata"``) — the format
           returned once the backend stores entries directly. We rewrite the
           leading host token to match the connection target (using
           ``[host]:port`` for non-default ports).
        2. **SHA256 fingerprint** (``"SHA256:..."``) — current backend format;
           we run ``ssh-keyscan`` and match by fingerprint.

        Anything else raises :class:`TunnelClientError` so strict host
        verification stays enforced.
        """
        raw = (config.host_key_fingerprint or "").strip()
        if not raw:
            raise TunnelClientError("host_key_fingerprint is empty; refusing to skip host verification")

        if _looks_like_known_hosts_entry(raw):
            normalised = _normalise_known_hosts_entry(raw, config.proxy_host, config.proxy_port)
            return write_temp_known_hosts(normalised)

        if raw.startswith("SHA256:"):
            host_lines = scan_host_keys(config.proxy_host, config.proxy_port)
            matched_line = match_known_host_line(
                scanned_lines=host_lines,
                expected_fingerprint=raw,
            )
            return write_temp_known_hosts(matched_line)

        raise TunnelClientError(
            f"host_key_fingerprint has unrecognised format: {raw[:32]!r}"
        )

    @staticmethod
    def _terminate_process(process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def scan_host_keys(host: str, port: int) -> list[str]:
    try:
        result = subprocess.run(  # noqa: S603
            ["ssh-keyscan", "-T", "5", "-p", str(port), "-t", "ed25519,ecdsa", host],
            check=False,
            capture_output=True,
            text=True,
            timeout=DEFAULT_TUNNEL_TIMEOUT,
        )
    except FileNotFoundError as exc:
        raise TunnelClientError("ssh-keyscan binary is required for host verification") from exc
    except subprocess.TimeoutExpired as exc:
        raise TunnelClientError(f"ssh-keyscan timed out for {host}:{port}") from exc

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip() and not line.startswith("#")]
    if lines:
        return lines

    stderr = (result.stderr or "").strip()
    raise TunnelClientError(f"Failed to scan SSH host key for {host}:{port}: {stderr or 'no host keys returned'}")


def match_known_host_line(scanned_lines: list[str], expected_fingerprint: str) -> str:
    for line in scanned_lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        key_type = parts[1]
        key_blob = parts[2]
        if key_type not in ALLOWED_HOST_KEY_TYPES:
            continue

        derived = derive_fingerprint(f"{key_type} {key_blob}")
        if derived == expected_fingerprint:
            return line

    raise TunnelClientError(
        "Host key fingerprint mismatch during strict verification "
        f"(expected {expected_fingerprint}, accepted key types: ed25519/ecdsa only)"
    )


def derive_fingerprint(key_text: str) -> str:
    parts = key_text.strip().split()
    if len(parts) < 2:
        raise TunnelClientError("Invalid host key text format")

    try:
        key_blob = base64.b64decode(parts[1].encode("ascii"), validate=True)
    except ValueError as exc:
        raise TunnelClientError("Invalid base64 in scanned host key") from exc

    digest = hashlib.sha256(key_blob).digest()
    b64 = base64.b64encode(digest).rstrip(b"=").decode("ascii")
    return f"SHA256:{b64}"


def _looks_like_known_hosts_entry(raw: str) -> bool:
    parts = raw.split()
    return len(parts) >= 3 and parts[1] in ALLOWED_HOST_KEY_TYPES


def _normalise_known_hosts_entry(raw: str, host: str, port: int) -> str:
    """Rewrite a known_hosts entry so the host token matches the connect address.

    SSH matches the entry by the literal host string used to connect — so a
    backend-provided entry of ``"some-other-name keytype keydata"`` would be
    ignored. We canonicalise the leading host token to match what
    :func:`SSHTunnel._build_ssh_command` connects to (and use ``[host]:port``
    for non-default ports).
    """
    parts = raw.split()
    if len(parts) < 3:
        raise TunnelClientError("known_hosts entry must contain host, key type, and key data")
    key_type = parts[1]
    key_blob = parts[2]
    if key_type not in ALLOWED_HOST_KEY_TYPES:
        raise TunnelClientError(f"unsupported known_hosts key type: {key_type}")

    host_token = host if port == 22 else f"[{host}]:{port}"
    return f"{host_token} {key_type} {key_blob}"


def write_temp_known_hosts(known_host_line: str) -> str:
    fd, temp_path = tempfile.mkstemp(prefix="argus-known-hosts-")
    os.close(fd)
    with open(temp_path, "w", encoding="utf-8") as fh:
        fh.write(f"{known_host_line}\n")
    os.chmod(temp_path, 0o600)
    return temp_path


def _unlink(path: str) -> None:
    """Remove a file; silently ignore if it does not exist."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def read_process_stderr(process: subprocess.Popen[str]) -> str:
    if process.stderr is None:
        return ""
    try:
        return process.stderr.read().strip()
    except Exception:  # noqa: BLE001
        return ""


def is_local_port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.5)
        return probe.connect_ex(("127.0.0.1", port)) == 0
