"""Tests for MixinLazyContext — the lazy Jinja2 context that resolves variables from an owner."""

import pytest
from jinja2 import Environment, StrictUndefined, UndefinedError

from sdcm.utils.loader_utils import MixinLazyContext


class Owner:
    """Simple owner with a plain attribute, a call-tracked property, and nothing else."""

    plain = "plain_value"

    def __init__(self):
        self.call_count = 0

    @property
    def expensive(self):
        self.call_count += 1
        return 42


def render(owner, template_str, whitelist=None):
    """Render *template_str* using MixinLazyContext against *owner*."""
    env = Environment(undefined=StrictUndefined)
    env.context_class = MixinLazyContext
    env.owner = owner
    if whitelist is not None:
        env.whitelist = whitelist
    return env.from_string(template_str).render()


@pytest.mark.parametrize(
    "template_str, expected",
    [
        ("{{ plain }}", "plain_value"),
        ("{{ expensive }}", "42"),
        ("{{ plain }} {{ expensive }}", "plain_value 42"),
    ],
)
def test_resolves_owner_attributes(template_str, expected):
    assert render(Owner(), template_str) == expected


def test_property_not_called_when_not_referenced():
    owner = Owner()
    render(owner, "{{ plain }}")
    assert owner.call_count == 0


def test_property_called_once_per_render():
    owner = Owner()
    render(owner, "{{ expensive }}{{ expensive }}")
    assert owner.call_count == 1


def test_missing_attribute_raises_undefined_error():
    with pytest.raises(UndefinedError):
        render(Owner(), "{{ nonexistent }}")


@pytest.mark.parametrize(
    "template_str, whitelist, expected",
    [
        ("{{ plain }}", {"plain"}, "plain_value"),
        ("{{ plain }}", {"plain", "expensive"}, "plain_value"),
    ],
)
def test_whitelist_allows_listed_keys(template_str, whitelist, expected):
    assert render(Owner(), template_str, whitelist=whitelist) == expected


@pytest.mark.parametrize(
    "template_str, whitelist",
    [
        ("{{ expensive }}", {"plain"}),
        ("{{ plain }}", set()),
    ],
)
def test_whitelist_blocks_unlisted_keys(template_str, whitelist):
    with pytest.raises(UndefinedError):
        render(Owner(), template_str, whitelist=whitelist)


def test_no_whitelist_allows_all_attributes():
    result = render(Owner(), "{{ plain }} {{ expensive }}", whitelist=None)
    assert result == "plain_value 42"
