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
# Copyright (c) 2025 ScyllaDB

"""Structural validation tests for the AI skills framework.

Validates that all skills in the skills/ directory follow the required
structure, naming conventions, frontmatter format, and line count limits
as defined in the AI skills framework plan.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).parent.parent
SKILLS_DIR = REPO_ROOT / "skills"

# Line count limits from skills/designing-skills/references/skill-structure.md
MAX_SKILL_MD_LINES = 500
MAX_REFERENCE_LINES = 400
MAX_WORKFLOW_LINES = 300

KEBAB_CASE_PATTERN = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")


def get_skill_dirs():
    """Return all skill directories that contain a SKILL.md file."""
    if not SKILLS_DIR.exists():
        return []
    return [d for d in SKILLS_DIR.iterdir() if d.is_dir() and (d / "SKILL.md").exists()]


def parse_frontmatter(skill_md_path):
    """Parse YAML frontmatter from a SKILL.md file.

    Returns:
        dict or None: Parsed frontmatter, or None if not found.
    """
    content = skill_md_path.read_text()
    parts = content.split("---", 2)
    if len(parts) < 3:
        return None
    try:
        return yaml.safe_load(parts[1])
    except yaml.YAMLError:
        return None


def extract_internal_links(md_path):
    """Extract relative file links from a Markdown file.

    Returns links like [text](path) where path doesn't start with http:// or https://.
    Strips anchor fragments (e.g., 'file.md#section' → 'file.md').

    Returns:
        list[str]: List of relative file paths referenced in Markdown links.
    """
    content = md_path.read_text()
    link_pattern = re.compile(r"\[.*?\]\((?!https?://)(.*?)\)")
    links = link_pattern.findall(content)
    return [link.split("#")[0] for link in links if link.split("#")[0]]


skill_dirs = get_skill_dirs()
skill_ids = [d.name for d in skill_dirs]


@pytest.mark.parametrize("skill_dir", skill_dirs, ids=skill_ids)
class TestSkillStructure:
    """Validate the directory structure and required files for each skill."""

    def test_skill_md_exists(self, skill_dir):
        """Every skill must have a SKILL.md file."""
        assert (skill_dir / "SKILL.md").exists(), f"Missing SKILL.md in {skill_dir.name}"

    def test_directory_name_is_kebab_case(self, skill_dir):
        """Skill directory names must be kebab-case."""
        assert KEBAB_CASE_PATTERN.match(skill_dir.name), f"Skill directory '{skill_dir.name}' is not kebab-case"

    def test_directory_name_length(self, skill_dir):
        """Skill directory names must be max 64 characters."""
        assert len(skill_dir.name) <= 64, f"Skill directory '{skill_dir.name}' exceeds 64 characters"


@pytest.mark.parametrize("skill_dir", skill_dirs, ids=skill_ids)
class TestSkillFrontmatter:
    """Validate YAML frontmatter in SKILL.md files."""

    def test_frontmatter_exists(self, skill_dir):
        """SKILL.md must have YAML frontmatter between --- markers."""
        frontmatter = parse_frontmatter(skill_dir / "SKILL.md")
        assert frontmatter is not None, f"No valid frontmatter in {skill_dir.name}/SKILL.md"

    def test_frontmatter_has_name(self, skill_dir):
        """Frontmatter must include a 'name' field."""
        frontmatter = parse_frontmatter(skill_dir / "SKILL.md")
        assert frontmatter and "name" in frontmatter, f"Missing 'name' in {skill_dir.name}/SKILL.md frontmatter"

    def test_frontmatter_has_description(self, skill_dir):
        """Frontmatter must include a 'description' field."""
        frontmatter = parse_frontmatter(skill_dir / "SKILL.md")
        assert frontmatter and "description" in frontmatter, (
            f"Missing 'description' in {skill_dir.name}/SKILL.md frontmatter"
        )

    def test_frontmatter_name_matches_directory(self, skill_dir):
        """The 'name' field must match the skill directory name."""
        frontmatter = parse_frontmatter(skill_dir / "SKILL.md")
        assert frontmatter and frontmatter.get("name") == skill_dir.name, (
            f"Frontmatter name '{frontmatter.get('name') if frontmatter else None}' "
            f"does not match directory '{skill_dir.name}'"
        )

    def test_description_is_third_person(self, skill_dir):
        """The description should use third-person voice (not start with 'I')."""
        frontmatter = parse_frontmatter(skill_dir / "SKILL.md")
        if frontmatter and "description" in frontmatter:
            desc = frontmatter["description"].strip()
            assert not desc.startswith("I "), (
                f"Description in {skill_dir.name}/SKILL.md should use third-person voice, not start with 'I'"
            )


@pytest.mark.parametrize("skill_dir", skill_dirs, ids=skill_ids)
class TestSkillLineCounts:
    """Validate line count limits for progressive disclosure."""

    def test_skill_md_line_count(self, skill_dir):
        """SKILL.md must stay under 500 lines."""
        line_count = len((skill_dir / "SKILL.md").read_text().splitlines())
        assert line_count <= MAX_SKILL_MD_LINES, (
            f"{skill_dir.name}/SKILL.md has {line_count} lines (max {MAX_SKILL_MD_LINES})"
        )

    def test_reference_files_line_count(self, skill_dir):
        """Reference files must stay under 400 lines each."""
        refs_dir = skill_dir / "references"
        if not refs_dir.exists():
            pytest.skip("No references/ directory")
        for ref_file in refs_dir.glob("*.md"):
            line_count = len(ref_file.read_text().splitlines())
            assert line_count <= MAX_REFERENCE_LINES, (
                f"{skill_dir.name}/references/{ref_file.name} has {line_count} lines (max {MAX_REFERENCE_LINES})"
            )

    def test_workflow_files_line_count(self, skill_dir):
        """Workflow files must stay under 300 lines each."""
        wf_dir = skill_dir / "workflows"
        if not wf_dir.exists():
            pytest.skip("No workflows/ directory")
        for wf_file in wf_dir.glob("*.md"):
            line_count = len(wf_file.read_text().splitlines())
            assert line_count <= MAX_WORKFLOW_LINES, (
                f"{skill_dir.name}/workflows/{wf_file.name} has {line_count} lines (max {MAX_WORKFLOW_LINES})"
            )


@pytest.mark.parametrize("skill_dir", skill_dirs, ids=skill_ids)
class TestSkillLinks:
    """Validate that all internal file references resolve correctly."""

    def test_skill_md_internal_links_resolve(self, skill_dir):
        """All relative links in SKILL.md must point to existing files."""
        skill_md = skill_dir / "SKILL.md"
        links = extract_internal_links(skill_md)
        for link in links:
            target = (skill_dir / link).resolve()
            assert target.exists(), (
                f"Broken link in {skill_dir.name}/SKILL.md: '{link}' does not resolve to an existing file"
            )


class TestSkillDiscovery:
    """Validate that skills are registered for platform discovery."""

    def test_agents_md_has_skills_table(self):
        """Root AGENTS.md must have a Skills section with a table."""
        agents_md = REPO_ROOT / "AGENTS.md"
        assert agents_md.exists(), "Missing AGENTS.md at repository root"
        content = agents_md.read_text()
        assert "## Skills" in content, "AGENTS.md missing '## Skills' section"

    def test_claude_md_exists(self):
        """CLAUDE.md must exist at repository root."""
        claude_md = REPO_ROOT / "CLAUDE.md"
        assert claude_md.exists(), "Missing CLAUDE.md at repository root"

    @pytest.mark.parametrize("skill_dir", skill_dirs, ids=skill_ids)
    def test_skill_registered_in_agents_md(self, skill_dir):
        """Each skill must be listed in AGENTS.md Skills table."""
        agents_md = (REPO_ROOT / "AGENTS.md").read_text()
        assert skill_dir.name in agents_md, f"Skill '{skill_dir.name}' not found in AGENTS.md"

    @pytest.mark.parametrize("skill_dir", skill_dirs, ids=skill_ids)
    def test_skill_imported_in_claude_md(self, skill_dir):
        """Each skill must be imported in CLAUDE.md."""
        claude_md = (REPO_ROOT / "CLAUDE.md").read_text()
        expected_import = f"@skills/{skill_dir.name}/SKILL.md"
        assert expected_import in claude_md, f"Missing import '{expected_import}' in CLAUDE.md"
