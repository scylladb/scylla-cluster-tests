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
        """Return 42 and count each evaluation in call_count."""
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
        pytest.param("{{ plain }}", "plain_value", id="plain-attribute"),
        pytest.param("{{ expensive }}", "42", id="property"),
        pytest.param("{{ plain }} {{ expensive }}", "plain_value 42", id="mixed"),
    ],
)
def test_resolves_owner_attributes(template_str, expected):
    """Template variables resolve to the owner's attribute and property values."""
    assert render(Owner(), template_str) == expected


def test_property_not_called_when_not_referenced():
    """A property is not evaluated when the template does not reference it."""
    owner = Owner()
    render(owner, "{{ plain }}")
    assert owner.call_count == 0


def test_property_called_once_per_render():
    """A property referenced twice in one render is evaluated once."""
    owner = Owner()
    render(owner, "{{ expensive }}{{ expensive }}")
    assert owner.call_count == 1


def test_missing_attribute_raises_undefined_error():
    """A variable missing from the owner raises UndefinedError under StrictUndefined."""
    with pytest.raises(UndefinedError):
        render(Owner(), "{{ nonexistent }}")


@pytest.mark.parametrize(
    "template_str, whitelist, expected",
    [
        pytest.param("{{ plain }}", {"plain"}, "plain_value", id="exact-whitelist"),
        pytest.param("{{ plain }}", {"plain", "expensive"}, "plain_value", id="superset-whitelist"),
    ],
)
def test_whitelist_allows_listed_keys(template_str, whitelist, expected):
    """Keys listed in the whitelist resolve from the owner."""
    assert render(Owner(), template_str, whitelist=whitelist) == expected


@pytest.mark.parametrize(
    "template_str, whitelist",
    [
        pytest.param("{{ expensive }}", {"plain"}, id="property-not-listed"),
        pytest.param("{{ plain }}", set(), id="empty-whitelist"),
    ],
)
def test_whitelist_blocks_unlisted_keys(template_str, whitelist):
    """Keys outside the whitelist raise UndefinedError."""
    with pytest.raises(UndefinedError):
        render(Owner(), template_str, whitelist=whitelist)


def test_no_whitelist_allows_all_attributes():
    """Without a whitelist every owner attribute and property resolves."""
    result = render(Owner(), "{{ plain }} {{ expensive }}", whitelist=None)
    assert result == "plain_value 42"
