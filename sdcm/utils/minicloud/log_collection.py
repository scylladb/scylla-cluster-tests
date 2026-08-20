"""Minicloud log/forensics collection into the test logdir, with credential redaction."""

import json
import logging
import os
import re
import subprocess

from sdcm.utils.minicloud.config import MINICLOUD_CONTAINER_NAME

LOGGER = logging.getLogger(__name__)

_SENSITIVE_ENV_NAME_RE = re.compile(r"(KEY|SECRET|TOKEN|PASSWORD|CREDENTIAL)", re.IGNORECASE)


def _decode_exit(code: int | None) -> str:
    """Human-readable cause for a container exit code."""
    match code:
        case 0:
            return "clean exit"
        case 137:
            return "SIGKILL - `docker rm -f`/`docker kill` from outside, or cgroup OOM"
        case 143:
            return "SIGTERM - `docker stop` from outside"
        case _:
            return "minicloud process exited on its own"


def redact_docker_inspect(raw: bytes) -> bytes:
    """Redact credential values from ``docker inspect`` output.

    The container is started with AWS credentials in its environment, and ``Config.Env``
    reproduces them verbatim — inspect snapshots land in the collected (uploaded) test
    logdir, so every write must go through this. If the JSON cannot be parsed, nothing of
    it is written: better to lose the snapshot than to upload unredacted credentials.
    """
    try:
        data = json.loads(raw)
        for entry in data:
            config = entry.get("Config") or {}
            env = config.get("Env") or []
            config["Env"] = [
                f"{name}=***REDACTED***" if _SENSITIVE_ENV_NAME_RE.search(name) else var
                for var in env
                for name in [var.split("=", 1)[0]]
            ]
        return json.dumps(data, indent=2).encode()
    except json.JSONDecodeError, TypeError, AttributeError:
        LOGGER.warning("Could not parse docker inspect output for redaction — dropping the snapshot")
        return b'{"error": "docker inspect output could not be parsed for credential redaction"}\n'


def collect_minicloud_logs(logdir: str, container_name: str = MINICLOUD_CONTAINER_NAME) -> None:
    """Dump minicloud container logs and inspect state into the test logdir.

    Produces: minicloud.log, minicloud-stderr.log, minicloud-inspect.json.
    Never raises — each collector runs independently.

    Runs after the manager has already streamed the log and (on death or teardown)
    recorded minicloud-inspect.json, so anything already written here wins: those files
    were captured while the container still existed, this one runs after `docker rm -f`.

    ``container_name`` defaults to the standard name because the log-collection entry points
    run without an SCTConfiguration; pass the configured ``minicloud_container_name`` from
    anywhere that has one, or the collector inspects the wrong container.
    """
    os.makedirs(logdir, exist_ok=True)

    # 1. Collect container logs (works on stopped/exited containers, fails only if removed)
    log_path = os.path.join(logdir, "minicloud.log")
    if os.path.exists(log_path) and os.path.getsize(log_path):
        LOGGER.info("minicloud logs already streamed to %s, keeping the streamed copy", log_path)
    else:
        result = subprocess.run(["docker", "logs", container_name], capture_output=True, check=False)
        if result.returncode == 0:
            with open(log_path, "wb") as fh:
                fh.write(result.stdout)
                fh.write(result.stderr)
            LOGGER.info("Collected minicloud logs to %s (%d bytes)", log_path, len(result.stdout) + len(result.stderr))

            # Write stderr separately for crash diagnostics
            if result.stderr:
                stderr_path = os.path.join(logdir, "minicloud-stderr.log")
                with open(stderr_path, "wb") as fh:
                    fh.write(result.stderr)
                LOGGER.info("Collected minicloud stderr to %s (%d bytes)", stderr_path, len(result.stderr))
        else:
            LOGGER.warning(
                "Failed to collect minicloud container logs (container removed?): %s",
                result.stderr.decode(errors="replace").strip(),
            )

    # 2. Collect container inspect (state, exit code, health, config)
    inspect_path = os.path.join(logdir, "minicloud-inspect.json")
    if os.path.exists(inspect_path):
        LOGGER.info("minicloud state already recorded to %s while the container existed", inspect_path)
        return
    inspect_result = subprocess.run(
        ["docker", "inspect", container_name],
        capture_output=True,
        check=False,
    )
    if inspect_result.returncode == 0:
        with open(inspect_path, "wb") as fh:
            fh.write(redact_docker_inspect(inspect_result.stdout))
        LOGGER.info("Collected minicloud inspect to %s", inspect_path)

        # Log key state info for quick debugging
        try:
            inspect_data = json.loads(inspect_result.stdout)
            if inspect_data:
                state = inspect_data[0].get("State", {})
                LOGGER.info(
                    "Minicloud container state: Status=%s, ExitCode=%s (%s), OOMKilled=%s",
                    state.get("Status"),
                    state.get("ExitCode"),
                    _decode_exit(state.get("ExitCode")),
                    state.get("OOMKilled"),
                )
        except json.JSONDecodeError, IndexError, KeyError:
            # best-effort convenience logging only — the snapshot file above is the record
            LOGGER.debug("Could not parse docker inspect output for state logging", exc_info=True)
    else:
        LOGGER.warning(
            "Failed to collect minicloud inspect (container removed?): %s",
            inspect_result.stderr.decode(errors="replace").strip(),
        )
