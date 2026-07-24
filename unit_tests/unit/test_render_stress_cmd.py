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

    @property
    def some_other_template_value(self) -> int:
        return 17


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


def test_generic_loader_mixin_has_no_owner_attribute_whitelist_by_default():
    result = FakeLoaderMixin().render_stress_cmd("cassandra-stress write n={{ some_other_template_value }}")
    assert result == "cassandra-stress write n=17"


def test_stress_template_context_supports_ordered_reuse_and_scalar_coercion():
    instance = FakeLoaderMixin(effective_disk_size=1_000)
    instance.params["stress_template_context"] = {
        "chunk_count": "4",
        "rows_total": "{{ effective_disk_size_bytes // 10 }}",
        "rows_per_cmd": "{{ rows_total // chunk_count }}",
    }

    rendered = instance.render_stress_cmd("cassandra-stress write n={{ rows_per_cmd }}")
    assert rendered == "cassandra-stress write n=25"


def test_stress_template_context_rejects_unknown_variable():
    instance = FakeLoaderMixin()
    instance.params["stress_template_context"] = {"rows_per_cmd": "{{ missing_value // 4 }}"}

    with pytest.raises(RuntimeError, match="stress_template_context entry 'rows_per_cmd' references an unknown"):
        instance.render_stress_cmd("cassandra-stress write n={{ rows_per_cmd }}")


def test_stress_template_context_must_reference_earlier_keys_only():
    instance = FakeLoaderMixin()
    instance.params["stress_template_context"] = {
        "rows_per_cmd": "{{ rows_total // 4 }}",
        "rows_total": "{{ effective_disk_size_bytes }}",
    }

    with pytest.raises(RuntimeError, match="stress_template_context entry 'rows_per_cmd' references an unknown"):
        instance.render_stress_cmd("cassandra-stress write n={{ rows_per_cmd }}")


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
