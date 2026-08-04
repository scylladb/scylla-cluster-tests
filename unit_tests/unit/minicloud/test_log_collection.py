"""Tests for minicloud log collection: docker-inspect credential redaction."""

from sdcm.utils.minicloud import redact_docker_inspect


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
