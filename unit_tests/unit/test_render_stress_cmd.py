"""Tests for LoaderUtilsMixin.render_stress_cmd."""

import pytest
from unittest.mock import MagicMock
from jinja2 import TemplateSyntaxError

from sdcm.utils.loader_utils import LoaderUtilsMixin


class FakeLoaderMixin(LoaderUtilsMixin):
    """Minimal LoaderUtilsMixin stand-in with a controllable effective_disk_size_bytes."""

    def __init__(self, effective_disk_size: int = 1_000_000_000):
        self.fake_effective_disk_size = effective_disk_size
        self.params = {}
        self.log = MagicMock()

    @property
    def effective_disk_size_bytes(self) -> int:
        return self.fake_effective_disk_size


@pytest.mark.parametrize(
    "cmd",
    [
        "cassandra-stress write cl=QUORUM n=5000000 -rate threads=80",
        "scylla-bench -workload=sequential -mode=write -partition-count=10000",
        "cassandra-stress write -schema 'compression={}'",
    ],
)
def test_literal_command_passes_through_unchanged(cmd):
    """Commands with no Jinja syntax are returned unchanged."""
    assert FakeLoaderMixin().render_stress_cmd(cmd) == cmd


@pytest.mark.parametrize(
    "disk_size, template, expected_substring",
    [
        (1_073_741_824, "cassandra-stress write n={{ effective_disk_size_bytes }}", "1073741824"),
        (500_000_000, "n={{ (effective_disk_size_bytes // 1000) | int }}", "500000"),
        (999, "{% set rows = effective_disk_size_bytes %}n={{ rows }}", "999"),
    ],
)
def test_template_renders_correctly(disk_size, template, expected_substring):
    result = FakeLoaderMixin(effective_disk_size=disk_size).render_stress_cmd(template)
    assert expected_substring in result
    assert "{{" not in result
    assert "{%" not in result


def test_unknown_variable_raises_runtime_error():
    with pytest.raises(RuntimeError, match="unknown Jinja variable"):
        FakeLoaderMixin().render_stress_cmd("cassandra-stress write n={{ unknown_variable }}")


def test_syntax_error_raises_before_stress_starts():
    with pytest.raises(TemplateSyntaxError):
        FakeLoaderMixin().render_stress_cmd("cassandra-stress write n={{ unclosed")


@pytest.mark.parametrize(
    "cmd, expected",
    [
        (
            "  {% set rows = effective_disk_size_bytes %}\n  cassandra-stress write n={{ rows }}\n  -rate threads=80  ",
            "cassandra-stress write n=1000 -rate threads=80",
        ),
    ],
)
def test_multiline_yaml_block_collapsed_to_single_line(cmd, expected):
    result = FakeLoaderMixin(effective_disk_size=1000).render_stress_cmd(cmd)
    assert "\n" not in result
    assert result == expected
