"""Minicloud log/forensics collection into the test logdir, with credential redaction."""

import glob
import json
import logging
import os
import re
import shutil
import subprocess

from sdcm.utils.minicloud.config import MINICLOUD_CONTAINER_NAME, MINICLOUD_STATE_DIR_DEFAULT

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


def collect_minicloud_guest_serial_logs(logdir: str, state_dir: str = MINICLOUD_STATE_DIR_DEFAULT) -> int:
    """Copy every QEMU guest's serial console log out of the minicloud state dir.

    minicloud runs each guest with `-serial file:<state_dir>/instances/<instance-id>/serial.log`
    (scylladb/minicloud src/vm/mod.rs), and deliberately keeps that file when it removes the
    guest's disks so it survives a terminated instance. It is the ONLY record of what happened
    inside a guest that SCT could never SSH into: cloud-init output, sshd startup, kernel
    messages. Everything else SCT collects needs a working SSH or SSM channel to the guest, so a
    node that never came up produces empty per-node archives — exactly the case where the
    console is the whole story.

    Copied as ``minicloud-serial-<instance-id>.log`` into the test logdir, where
    BaseSCTLogCollector's glob picks them up. Returns the number of files copied.

    Never raises: this runs on the failure path, where losing the evidence is bad but breaking
    the rest of log collection is worse.
    """
    instances_dir = os.path.join(os.path.expanduser(state_dir), "instances")
    if not os.path.isdir(instances_dir):
        LOGGER.debug("No minicloud instances dir at %s — nothing to collect", instances_dir)
        return 0

    os.makedirs(logdir, exist_ok=True)
    copied = 0
    for serial_log in sorted(glob.glob(os.path.join(instances_dir, "*", "serial.log"))):
        instance_id = os.path.basename(os.path.dirname(serial_log))
        destination = os.path.join(logdir, f"minicloud-serial-{instance_id}.log")
        try:
            shutil.copyfile(serial_log, destination)
        except OSError as exc:
            LOGGER.warning("Could not collect guest serial log %s: %s", serial_log, exc)
            continue
        copied += 1
        LOGGER.info("Collected guest serial log %s (%d bytes)", destination, os.path.getsize(destination))

    if not copied:
        LOGGER.warning("No guest serial logs found under %s", instances_dir)
    return copied


def collect_minicloud_logs(logdir: str, container_name: str = MINICLOUD_CONTAINER_NAME) -> None:
    """Dump minicloud container logs and inspect state into the test logdir.

    Produces: minicloud.log or minicloud-teardown.log, minicloud-stderr.log,
    minicloud-inspect.json. Never raises — each collector runs independently.

    Runs after the manager has (on death or teardown) recorded minicloud-inspect.json, so
    that snapshot wins: it was captured while the container still existed, this runs after
    `docker rm -f`.

    The container log is the exception — see the note on minicloud-teardown.log below.

    ``container_name`` defaults to the standard name because the log-collection entry points
    run without an SCTConfiguration; pass the configured ``minicloud_container_name`` from
    anywhere that has one, or the collector inspects the wrong container.
    """
    os.makedirs(logdir, exist_ok=True)

    # 1. Collect container logs (works on stopped/exited containers, fails only if removed)
    #
    # The manager's `docker logs -f` streamer dies with the test process, so the copy it
    # streamed into the run dir always stops short of teardown. `docker logs` here returns
    # the container's whole log, so it must never be skipped just because that partial copy
    # exists — that is precisely the run where the missing ending is worth having.
    #
    # It goes to its own name rather than over the streamed copy: the manager can adopt or
    # restart a container mid-run, and then `docker logs` holds only the surviving
    # container's output. Overwriting would trade one truncation for a worse one.
    log_path = os.path.join(logdir, "minicloud.log")
    streamed_copy_exists = os.path.exists(log_path) and os.path.getsize(log_path)
    if streamed_copy_exists:
        log_path = os.path.join(logdir, "minicloud-teardown.log")
        LOGGER.info("minicloud logs already streamed; collecting the complete log to %s", log_path)

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
