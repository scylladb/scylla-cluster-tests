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

"""The option reference as markdown: an index page plus one page per option group, or one string."""

import pathlib
import re
from textwrap import dedent

from sdcm.sct_config.docs import GroupDoc, OptionDoc, Renderer


class MarkdownPagesRenderer(Renderer[dict[str, str]]):
    """The index page plus one markdown page per group, as ``{path: markdown}``, with options cross-linked."""

    #: Where the per-group option pages live, relative to the repo root.
    DIR = "docs/configuration_options"
    #: The index page, kept at the historical path so existing links keep working.
    INDEX = "docs/configuration_options.md"

    INDEX_HEADER = """
        # scylla-cluster-tests configuration options

        Every option can be set in a config file, or as an environment variable named
        `SCT_<OPTION>` (upper-cased). Options are grouped by what they configure: cross-cutting
        concerns first, then one page per backend, then one per test type.

        #### Sections
        * Some groups hold their options in a section, e.g. `agent`: in YAML they nest under
               the section key (`agent: {enabled: true}`), and they are listed by their dotted
               path (`agent.enabled`).
        * A section field is set from the environment as `SCT_<SECTION>__<FIELD>`
               (`SCT_AGENT__ENABLED=true`); a whole section, or a field holding a list or a
               nested section, takes a YAML or JSON string (`SCT_AGENT='{enabled: true}'`).
        * Each layer (defaults, config files, environment) merges into a section field by
               field, so a config file setting `agent: {enabled: true}` keeps every other default.

        #### Appending with environment variables or with config files
        * **strings:** can be appended with adding `++` at the beginning of the string:
               `export SCT_APPEND_SCYLLA_ARGS="++ --overprovisioned 1"`
        * **list:** can be appended by adding `++` as the first item of the list
               `export SCT_SCYLLA_D_OVERRIDES_FILES='["++", "extra_file/scylla.d/io.conf"]'`

        #### Nested (dict/list) options
        * A single sub-key of a dict/list option can be set on its own, without
               quoting the whole value, using either dot-notation or double-underscore
               notation: `SCT_STRESS_IMAGE.ycsb=...` or `SCT_STRESS_IMAGE__ycsb=...`.
        * `__` is the bash-exportable form (dots are invalid in bash variable names),
               so prefer it with plain `export`, e.g. `export SCT_STRESS_IMAGE__ycsb=...`.
        * Sub-keys containing `-` (e.g. `cassandra-stress`) still require the dot form,
               set via `env 'SCT_STRESS_IMAGE.cassandra-stress=...' ...`, since `-` is not
               a valid bash identifier character either.
        * **Case matters:** the `__` form lower-cases the sub-key (e.g.
               `SCT_INSTANCE_TYPE_DB__ARCH` becomes sub-key `arch`), while the `.` form
               preserves case verbatim (`SCT_INSTANCE_TYPE_DB.ARCH` stays `ARCH`). This
               matters for sub-keys consumed by case-sensitive lookups -- prefer
               uppercase sub-keys with `__` (they'll be lowered, matching the common
               convention) rather than the case-preserving dot form.
    """

    def render(self, groups: list[GroupDoc]) -> dict[str, str]:
        links = {
            option.path: f"{self.slug(group.title)}.md#{option.path}" for group in groups for option in group.options
        }
        pages = {f"{self.DIR}/{self.slug(group.title)}.md": self.group_page(group, links) for group in groups}
        pages[self.INDEX] = self.index_page(groups)
        return pages

    @staticmethod
    def slug(group_title: str) -> str:
        """Filename stem of a group's page."""
        return re.sub(r"[^a-z0-9]+", "-", group_title.lower()).strip("-")

    @staticmethod
    def yaml_example(path: str, value: str) -> str:
        """A YAML snippet setting the option at dotted *path* to *value*, in its nested form."""
        *parents, name = path.split(".")
        lines = [f"{'  ' * depth}{parent}:" for depth, parent in enumerate(parents)]
        lines.append(f"{'  ' * len(parents)}{name}: {value}")
        return "```yaml\n" + "\n".join(lines) + "\n```"

    @classmethod
    def entry(cls, option: OptionDoc) -> str:
        """The markdown of one option on its group's page: anchor, heading, help, default, type, overrides."""
        default_text = "the defaults of its options" if option.is_section else option.default_text
        appendable = " (appendable)" if option.appendable else ""
        # explicit anchor: GitHub slugs this heading as "<name>--sct_<name>", so the bare
        # "#<name>" used by the index and by every cross-link needs a target of its own
        heading = f"## **{option.path}** / {option.env}" if option.env else f"## **{option.path}**"
        text = (
            f'\n\n<a id="{option.path}"></a>\n\n'
            + dedent(f"""
            {heading}

            {"<br>".join(option.help_lines)}

            **default:** {default_text}

            **type:** {option.type}{appendable}
            """).strip()
        )
        if option.env is None:
            text += f"\n\n**env:** within the YAML/JSON of `{option.section_env}`"
        if option.depth and not option.is_section:
            text += "\n\n" + cls.yaml_example(option.path, option.default_text if option.has_default else "...")
        if option.backend_overrides:
            overrides = "\n".join(
                f"- `{value}`: {', '.join(backends)}" for value, backends in option.backend_overrides.items()
            )
            text += f"\n\n**backend overrides:**\n{overrides}"
        return text + "\n"

    def group_page(self, group: GroupDoc, links: dict[str, str]) -> str:
        """The markdown page of one option group, its option mentions linked through *links*."""
        page = f"# {group.title}\n\n[← All configuration options](../{pathlib.Path(self.INDEX).name})\n"
        if group.blurb:
            page += f"\n{group.blurb}\n"
        page += f"\n**{len(group.options)} options.**\n"
        page += "".join(self.entry(option) for option in group.options)
        return self.link_option_mentions(page, links, f"{self.slug(group.title)}.md")

    def index_page(self, groups: list[GroupDoc]) -> str:
        """The index page: how options are set, the groups, and every option alphabetically."""
        docs_dir = pathlib.Path(self.DIR).name
        index = dedent(self.INDEX_HEADER).strip()
        total = sum(len(group.options) for group in groups)
        index += f"\n\n## Groups\n\n{total} options across {len(groups)} groups.\n\n"
        index += "| Group | Options | What it covers |\n|---|---:|---|\n"
        for group in groups:
            summary = " ".join(group.blurb.split())
            if len(summary) > 130:
                summary = summary[:127].rsplit(" ", 1)[0] + "..."
            index += f"| [{group.title}]({docs_dir}/{self.slug(group.title)}.md) | {len(group.options)} | {summary} |\n"

        index += "\n## All options, alphabetically\n\n| Option | Environment variable | Group |\n|---|---|---|\n"
        by_path = sorted(((option, group) for group in groups for option in group.options), key=lambda kv: kv[0].path)
        for option, group in by_path:
            page = f"{docs_dir}/{self.slug(group.title)}.md"
            env = f"`{option.env}`" if option.env else "within its section's variable"
            index += f"| [`{option.path}`]({page}#{option.path}) | {env} | [{group.title}]({page}) |\n"
        return index

    @staticmethod
    def link_option_mentions(text: str, links: dict[str, str], self_page: str) -> str:
        """Turn mentions of other options in prose into links to those options.

        Both quoted (`name`, 'name') and bare mentions are linked; bare ones only for names
        containing an underscore, so ordinary prose words are never rewritten. Fenced code blocks,
        headings, table rows and already-formed links are left alone -- a sample command line must
        stay copy-pasteable.
        """

        def target_for(name):
            target = links.get(name)
            if target and target.startswith(f"{self_page}#"):
                return target[len(self_page) :]
            return target

        def link_quoted(match):
            target = target_for(match.group(2))
            return f"[`{match.group(2)}`]({target})" if target else match.group(0)

        def link_bare(match):
            target = target_for(match.group(0))
            return f"[`{match.group(0)}`]({target})" if target else match.group(0)

        bare_names = sorted((n for n in links if "_" in n), key=len, reverse=True)
        bare_pattern = re.compile(rf"(?<![\w`/#.-])({'|'.join(map(re.escape, bare_names))})(?![\w`\]/-])")

        out, in_fence = [], False
        for line in text.splitlines():
            if line.lstrip().startswith("```"):
                in_fence = not in_fence
                out.append(line)
                continue
            if in_fence or line.startswith(("#", "| ", "<a id=", "**default:**", "**type:**")):
                out.append(line)
                continue
            linked = re.sub(r"(`|')([a-z][a-z0-9_]{3,})\1", link_quoted, line)
            # protect the links just made, plus any pre-existing ones, from the bare pass
            parts = re.split(r"(\[[^\]]*\]\([^)]*\))", linked)
            out.append("".join(p if i % 2 else bare_pattern.sub(link_bare, p) for i, p in enumerate(parts)))
        return "\n".join(out)


class MarkdownRenderer(Renderer[str]):
    """All markdown pages as one string, index first."""

    def render(self, groups: list[GroupDoc]) -> str:
        pages = MarkdownPagesRenderer().render(groups)
        index = pages.pop(MarkdownPagesRenderer.INDEX)
        return index + "\n\n" + "\n\n".join(pages[k] for k in sorted(pages))
