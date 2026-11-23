# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Reference documentation of the SCT configuration options, generated from the config model.

`ConfigDocs` reads the config model and the defaults files into `GroupDoc`s holding `OptionDoc`s.
A `Renderer` turns those into text; the renderers live in the submodules: `markdown` for the
pages under `docs/configuration_options/` and a single markdown string, `yaml` for a YAML
listing::

    from sdcm.sct_config.docs.markdown import MarkdownPagesRenderer

    ConfigDocs(SCTConfiguration).render(MarkdownPagesRenderer())
"""

import copy
import inspect
import re
from abc import ABC, abstractmethod
from textwrap import dedent
from typing import Annotated, Literal, NamedTuple, Union, get_args, get_origin

import anyconfig
from pydantic import BaseModel, RootModel

from sdcm import sct_abs_path
from sdcm.sct_config.mixins import CONFIG_GROUPS
from sdcm.sct_config.types import InputType, is_ignored_field


class OptionDoc(NamedTuple):
    """What the docs show about one option: a root field, a section, or a field inside a section."""

    #: Dotted path of the option, e.g. ``agent.port``.
    path: str
    #: Environment variable setting it; ``None`` for a field set only through its section's variable.
    env: str | None
    #: Variable of the closest enclosing section that has one, for an option without its own.
    section_env: str | None
    help_lines: tuple[str, ...]
    #: Whether the base defaults file sets the option.
    has_default: bool
    #: The base default as shown in the docs; ``N/A`` when unset or empty.
    default_text: str
    type: str
    appendable: bool
    is_section: bool
    #: Default shown per backend whose defaults differ from the base: ``{value: [backend, ...]}``.
    backend_overrides: dict[str, list[str]]

    @property
    def name(self) -> str:
        """The option's own key, the last part of its path."""
        return self.path.rsplit(".", 1)[-1]

    @property
    def depth(self) -> int:
        """How many sections the option is nested in."""
        return self.path.count(".")


class GroupDoc(NamedTuple):
    """One option group: the options a mixin declares, each section followed by its fields."""

    title: str
    #: Prose from the mixin's docstring.
    blurb: str
    options: list[OptionDoc]


class ConfigDocs:
    """Every option of a config *model*, described with its defaults from the defaults files."""

    DEFAULTS_FILE = "defaults/test_default.yaml"
    #: Marks an option a defaults file does not set.
    UNSET = object()

    def __init__(self, model: type[BaseModel]):
        self.model = model
        self.defaults = anyconfig.load(sct_abs_path(self.DEFAULTS_FILE))
        self.backend_defaults = {}
        for backend, config_files in model.model_fields["defaults_config_files"].default.items():
            merged = copy.deepcopy(self.defaults)
            anyconfig.merge(merged, anyconfig.load(list(config_files)), ac_merge=anyconfig.MS_DICTS)
            self.backend_defaults[backend] = merged
        self.groups = [
            GroupDoc(
                title, self.group_blurb(title), [doc for name, field in fields for doc in self.describe(name, field)]
            )
            for title, fields in self.fields_by_group()
        ]

    def render[T](self, renderer: "Renderer[T]") -> T:
        """Return the documentation as *renderer* formats it."""
        return renderer.render(self.groups)

    def fields_by_group(self) -> list[tuple[str, list]]:
        """Return ``(group_title, [(field_name, field), ...])`` in `CONFIG_GROUPS` order.

        A field belongs to the mixin that declares it, which is what makes the generated docs
        browsable by domain. Each mixin's own fields come from its `model_fields` -- mixins
        inherit only from `BaseModel`, so that is exactly its own set. (Reading
        `__dict__["__annotations__"]` does not work here: Python 3.14 defers annotations, so the
        key is absent until something forces evaluation.) Anything declared on the model
        itself falls into a trailing "Other" section, so a field can never silently vanish from
        the documentation.
        """
        documented = {
            name: field
            for name, field in self.model.model_fields.items()
            if not (field.exclude or is_ignored_field(field))
        }
        grouped = []
        claimed = set()
        for mixin in CONFIG_GROUPS:
            if names := [n for n in mixin.model_fields if n in documented and n not in claimed]:
                claimed.update(names)
                grouped.append((mixin.config_group, [(n, documented[n]) for n in names]))
        if leftover := [(n, f) for n, f in documented.items() if n not in claimed]:
            grouped.append(("Other", leftover))
        return grouped

    def describe(self, path: str, field, env: str | None = None, section_env: str | None = None) -> list[OptionDoc]:
        """Describe the option at *path*, followed by the fields of a section, recursively.

        A root option's variable is ``SCT_<OPTION>``, a section field's ``SCT_<SECTION>__<FIELD>``; a
        field of a nested section has none and is set within its enclosing section's variable.
        """
        if env is None and "." not in path:
            env = f"SCT_{path.upper()}"
        default = self.default_at(self.defaults, path)
        docs = [
            OptionDoc(
                path=path,
                env=env,
                section_env=section_env,
                help_lines=tuple(self.strip_help_text(field.description).splitlines()) if field.description else (),
                has_default=default is not self.UNSET,
                default_text=self.default_text(default),
                type=self.type_text(field),
                appendable=bool((field.json_schema_extra or {}).get("appendable")),
                is_section=self.is_section_model(field.annotation),
                backend_overrides=self.backend_overrides(path),
            )
        ]
        if self.is_section_model(field.annotation):
            for name, sub_field in field.annotation.model_fields.items():
                sub_env = f"{env}__{name.upper()}" if "." not in path else None
                docs += self.describe(f"{path}.{name}", sub_field, sub_env, env or section_env)
        return docs

    def backend_overrides(self, path: str) -> dict[str, list[str]]:
        """Return ``{value: [backend, ...]}`` for the backends whose default of *path* differs from the base default."""
        base_text = self.default_text(self.default_at(self.defaults, path))
        grouped = {}
        for backend, defaults in self.backend_defaults.items():
            if (value := self.default_at(defaults, path)) is self.UNSET:
                continue
            if (text := self.default_text(value)) != base_text:
                grouped.setdefault(text, []).append(backend)
        return grouped

    @classmethod
    def default_at(cls, defaults: dict, path: str):
        """Return the value at dotted *path* of a loaded defaults file, or ``UNSET`` when it sets none."""
        value = defaults
        for key in path.split("."):
            if not isinstance(value, dict) or key not in value:
                return cls.UNSET
            value = value[key]
        return value

    @classmethod
    def default_text(cls, value) -> str:
        """A default as shown in the docs: ``N/A`` when unset, empty or ``None``."""
        return "N/A" if value is cls.UNSET or value in ("", None) else str(value)

    @staticmethod
    def is_section_model(annotation) -> bool:
        """Whether an option's *annotation* is a pydantic model with named fields, documented as a section.

        A `RootModel` holds a single value, so it is documented as a plain option.
        """
        return (
            inspect.isclass(annotation) and issubclass(annotation, BaseModel) and not issubclass(annotation, RootModel)
        )

    @classmethod
    def type_text(cls, field) -> str:
        """The type of an option as shown in the docs; a section shows how many options it holds."""
        if cls.is_section_model(field.annotation):
            return f"section ({len(field.annotation.model_fields)} options below)"
        input_type_meta = next((m for m in (field.metadata or []) if isinstance(m, InputType)), None)
        output_type = cls.annotation_text(field.annotation)
        if not input_type_meta:
            return output_type
        if len(output_type) > 100:
            return input_type_meta.description
        return f"{input_type_meta.description} → {output_type}"

    @classmethod
    def annotation_text(cls, field_type) -> str:  # noqa: PLR0911
        """Convert a type annotation to a human-readable string.

        ``Annotated`` shows its underlying type, ``Literal`` its values, a ``Union`` its non-``None``
        members joined with ``|``, a generic its origin and arguments, recursively; a basic type is
        shown without ``typing.`` / ``<class '…'>`` prefixes.
        """
        origin = get_origin(field_type)
        args = get_args(field_type)

        def clear_class(type_str):
            return (
                type_str.replace("typing.", "")
                .replace("<class '", "")
                .replace("'>", "")
                .replace("types.", "")
                .replace("UnionType", "")
            )

        if origin is Annotated and args:
            return cls.annotation_text(args[0])
        if origin is Literal and args:
            return f"Literal[{', '.join(repr(arg) for arg in args)}]"
        if origin is Union and args:
            non_none_args = [arg for arg in args if arg is not type(None)]
            if len(non_none_args) == 1:
                return cls.annotation_text(non_none_args[0])
            return " | ".join(cls.annotation_text(arg) for arg in non_none_args)
        if origin:
            if args:
                return f"{clear_class(str(origin))}[{', '.join(cls.annotation_text(arg) for arg in args)}]"
            return clear_class(str(origin))
        return clear_class(str(field_type))

    @staticmethod
    def group_blurb(group_title: str) -> str:
        """The prose from a mixin's docstring, between its summary line and the "See ``sdcm.sct_config.mixins``" pointer.

        This is how per-group guidance (e.g. which stress option belongs to which tool) reaches the
        generated docs instead of only the source.
        """
        mixin = next((m for m in CONFIG_GROUPS if m.config_group == group_title), None)
        if not mixin or not mixin.__doc__:
            return ""
        out = []
        for line in dedent(mixin.__doc__).strip().splitlines()[1:]:  # the summary line is the page heading
            if line.strip().startswith("See ``sdcm.sct_config.mixins``"):
                break
            out.append(line)
        # docstrings mix RST double-backticks with markdown; the pages are markdown
        return re.sub(r"(?<!`)``(?!`)([^`\n]+)(?<!`)``(?!`)", r"`\1`", "\n".join(out)).strip()

    @staticmethod
    def strip_help_text(text: str) -> str:
        """Return *text* with each line stripped and without its leading and trailing blank line."""
        output = [line.strip() for line in text.splitlines()]
        return "\n".join(output[1 if not output[0] else 0 : -1 if not output[-1] else None])


class Renderer[T](ABC):
    """Formats the option groups `ConfigDocs` builds."""

    @abstractmethod
    def render(self, groups: list[GroupDoc]) -> T:
        """Return the documentation of *groups*."""
