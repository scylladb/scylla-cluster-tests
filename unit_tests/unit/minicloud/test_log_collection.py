"""Tests for minicloud log collection: guest serial consoles and docker-inspect redaction."""

import subprocess

from sdcm.utils.minicloud import (
    collect_minicloud_guest_serial_logs,
    collect_minicloud_logs,
    redact_docker_inspect,
)


def test_redact_docker_inspect_strips_credentials():
    raw = (
        b'[{"Config": {"Env": ["AWS_ACCESS_KEY_ID=AKIAXXXX", "AWS_SECRET_ACCESS_KEY=verysecret", '
        b'"AWS_SESSION_TOKEN=tok", "AWS_REGION=eu-west-1", "PATH=/usr/bin"]}, "State": {"ExitCode": 137}}]'
    )
    redacted = redact_docker_inspect(raw).decode()
    assert "verysecret" not in redacted
    assert "AKIAXXXX" not in redacted
    assert "tok" not in redacted.split("AWS_SESSION_TOKEN=")[1].split('"')[0]
    assert "AWS_REGION=eu-west-1" in redacted
    assert "PATH=/usr/bin" in redacted
    assert '"ExitCode": 137' in redacted


def test_redact_docker_inspect_drops_unparseable_output():
    """If the JSON cannot be parsed it must not be written verbatim — it may hold creds."""
    redacted = redact_docker_inspect(b"AWS_SECRET_ACCESS_KEY=verysecret \x00 not json")
    assert b"verysecret" not in redacted


def test_collect_guest_serial_logs_copies_every_instance(tmp_path):
    """Each guest's serial.log lands in the logdir under a name the SCT collector globs."""
    state_dir = tmp_path / "state"
    for instance_id, body in (("i-aaa", "node one console"), ("i-bbb", "node two console")):
        instance_dir = state_dir / "instances" / instance_id
        instance_dir.mkdir(parents=True)
        (instance_dir / "serial.log").write_text(body)
    logdir = tmp_path / "logdir"

    copied = collect_minicloud_guest_serial_logs(str(logdir), state_dir=str(state_dir))

    assert copied == 2
    assert (logdir / "minicloud-serial-i-aaa.log").read_text() == "node one console"
    assert (logdir / "minicloud-serial-i-bbb.log").read_text() == "node two console"


def test_collect_guest_serial_logs_ignores_instances_without_a_console(tmp_path):
    """A guest whose dir holds only disks (serial.log not yet created) is skipped, not fatal."""
    state_dir = tmp_path / "state"
    (state_dir / "instances" / "i-nodisk").mkdir(parents=True)
    (state_dir / "instances" / "i-nodisk" / "root.qcow2").write_bytes(b"disk")
    logdir = tmp_path / "logdir"

    assert collect_minicloud_guest_serial_logs(str(logdir), state_dir=str(state_dir)) == 0


def test_collect_guest_serial_logs_survives_a_missing_state_dir(tmp_path):
    """Runner topology has no state dir on this host — collection must not raise."""
    assert collect_minicloud_guest_serial_logs(str(tmp_path / "logdir"), state_dir=str(tmp_path / "absent")) == 0


def _fake_docker_logs(stdout=b"", stderr=b"", returncode=0):
    """Stand in for subprocess.run(['docker', 'logs', ...]) inside collect_minicloud_logs."""

    def run(cmd, capture_output=False, check=False):  # noqa: ARG001
        if cmd[:2] == ["docker", "logs"]:
            return subprocess.CompletedProcess(cmd, returncode, stdout, stderr)
        # docker inspect — irrelevant here, report it as unavailable
        return subprocess.CompletedProcess(cmd, 1, b"", b"no such container")

    return run


def test_collect_logs_keeps_streamed_copy_and_still_captures_the_ending(tmp_path, monkeypatch):
    """The streamed copy stops at teardown, so the full `docker logs` must still be collected.

    Regression: pointing collect_minicloud_logs() at the run dir made the pre-existing streamed
    minicloud.log suppress the `docker logs` dump entirely, so the ending never reached Argus.
    """
    logdir = tmp_path / "run"
    logdir.mkdir()
    (logdir / "minicloud.log").write_bytes(b"streamed up to teardown\n")
    monkeypatch.setattr(subprocess, "run", _fake_docker_logs(stdout=b"streamed up to teardown\nplus the ending\n"))

    collect_minicloud_logs(str(logdir))

    # the streamed copy is left untouched — a restarted container would make the dump a subset
    assert (logdir / "minicloud.log").read_bytes() == b"streamed up to teardown\n"
    assert (logdir / "minicloud-teardown.log").read_bytes() == b"streamed up to teardown\nplus the ending\n"


def test_collect_logs_writes_minicloud_log_when_nothing_was_streamed(tmp_path, monkeypatch):
    """With no streamed copy (container adopted, or a bare collect-logs run) it keeps the plain name."""
    logdir = tmp_path / "run"
    monkeypatch.setattr(subprocess, "run", _fake_docker_logs(stdout=b"whole log\n"))

    collect_minicloud_logs(str(logdir))

    assert (logdir / "minicloud.log").read_bytes() == b"whole log\n"
    assert not (logdir / "minicloud-teardown.log").exists()


def test_collect_logs_leaves_streamed_copy_alone_when_container_is_gone(tmp_path, monkeypatch):
    """`docker rm -f` already ran: keep what was streamed, write no empty teardown file."""
    logdir = tmp_path / "run"
    logdir.mkdir()
    (logdir / "minicloud.log").write_bytes(b"streamed\n")
    monkeypatch.setattr(subprocess, "run", _fake_docker_logs(returncode=1, stderr=b"No such container: minicloud"))

    collect_minicloud_logs(str(logdir))

    assert (logdir / "minicloud.log").read_bytes() == b"streamed\n"
    assert not (logdir / "minicloud-teardown.log").exists()
