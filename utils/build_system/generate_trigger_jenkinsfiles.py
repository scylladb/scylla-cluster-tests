#!/usr/bin/env python3
"""Generate trigger Jenkinsfiles from YAML matrix configurations.

This script ensures Jenkinsfiles stay in sync with their YAML source-of-truth.
Run it via pre-commit or manually:

    python utils/build_system/generate_trigger_jenkinsfiles.py

Each YAML file in configurations/triggers/ declares its Jenkinsfile output path(s)
under a top-level `jenkinsfiles` key. This script discovers all such YAMLs
and regenerates the Jenkinsfiles automatically.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TRIGGERS_DIR = PROJECT_ROOT / "configurations" / "triggers"

HEADER = "// AUTO-GENERATED from {yaml} — do not edit manually.\n// Run: python utils/build_system/generate_trigger_jenkinsfiles.py\n"


def get_parameterized_cron(yaml_path: Path) -> str:
    """Extract parameterized cron spec from a trigger YAML file."""
    data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Invalid trigger YAML {yaml_path}: expected mapping, got {type(data).__name__}")
    cron_triggers = data.get("cron_triggers", [])
    if not cron_triggers:
        return ""

    lines = []
    for trigger in cron_triggers:
        schedule = trigger["schedule"]
        params = trigger.get("params", {})
        if params:
            param_str = ";".join(f"{k}={v}" for k, v in params.items())
            lines.append(f"{schedule} % {param_str}")
        else:
            lines.append(schedule)

    return "\n".join(lines)


def generate_jenkinsfile(yaml_rel_path: str, cron_spec: str, labels_selector: str = "") -> str:
    lines = [
        HEADER.format(yaml=yaml_rel_path),
        "def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)",
        "triggerMatrixPipeline(",
        f"    matrix_file: '{yaml_rel_path}',",
    ]

    if labels_selector:
        lines.append(f"    labels_selector: '{labels_selector}',")

    if cron_spec:
        if "\n" in cron_spec:
            escaped = cron_spec.replace("'''", "\\'\\'\\'")
            lines.append(f"    cron: '''{escaped}'''")
        else:
            escaped = cron_spec.replace("'", "\\'")
            lines.append(f"    cron: '{escaped}'")
    else:
        lines[-1] = lines[-1].rstrip(",")

    lines.append(")")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    changed = 0

    for yaml_path in sorted(TRIGGERS_DIR.glob("*.yaml")):
        data = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"Invalid trigger YAML {yaml_path}: expected mapping, got {type(data).__name__}")
        jenkinsfiles = data.get("jenkinsfiles", [])
        if not jenkinsfiles:
            continue

        yaml_rel = str(yaml_path.relative_to(PROJECT_ROOT))
        cron_spec = get_parameterized_cron(yaml_path)

        for entry in jenkinsfiles:
            jenkinsfile_path = PROJECT_ROOT / entry["path"]
            labels_selector = entry.get("labels_selector", "")

            content = generate_jenkinsfile(yaml_rel, cron_spec, labels_selector=labels_selector)

            if jenkinsfile_path.exists() and jenkinsfile_path.read_text() == content:
                continue

            jenkinsfile_path.parent.mkdir(parents=True, exist_ok=True)
            jenkinsfile_path.write_text(content)
            print(f"Updated: {entry['path']}")
            changed += 1

    if changed:
        print(f"\n{changed} Jenkinsfile(s) regenerated. Please stage the changes.")
    return 1 if changed else 0


if __name__ == "__main__":
    sys.exit(main())
