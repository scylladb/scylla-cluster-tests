"""Defined pipelines and test-cases in the SCT tree, their owning teams, and the ones with no run."""

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import yaml

QUARANTINE_DIRS = ("jenkins-pipelines/quarantined/",)
TEST_CASE_RE = re.compile(r"test-cases/[\w./-]+\.ya?ml")
# Trigger jobs start other jobs and report no run: "<name>-trigger" or "<name>-trigger-<arch>".
TRIGGER_RE = re.compile(r"-trigger(-[a-z0-9_]+)?$")


@dataclass(frozen=True)
class Pipeline:
    key: str
    file: str
    jenkinsfile: str
    quarantined: bool
    trigger: bool


def defined_pipelines(sct_root: Path) -> list[Pipeline]:
    """Return every jenkinsfile and every _symlinks.yaml entry under jenkins-pipelines/.

    The key is the folder and the job name, as in analysis.test_key. A symlink entry points to the
    jenkinsfile in its target, so it is quarantined when its target is.
    """
    root = sct_root / "jenkins-pipelines"
    pipelines = []
    for path in sorted(root.rglob("*.jenkinsfile")):
        rel = path.relative_to(sct_root).as_posix()
        pipelines.append(
            Pipeline(
                key=f"{path.parent.name}/{path.stem}",
                file=rel,
                jenkinsfile=rel,
                quarantined=rel.startswith(QUARANTINE_DIRS),
                trigger=bool(TRIGGER_RE.search(path.stem)),
            )
        )
    for path in sorted(root.rglob("_symlinks.yaml")):
        for entry in (yaml.safe_load(path.read_text()) or {}).get("symlinks") or []:
            target = entry.get("target", "")
            pipelines.append(
                Pipeline(
                    key=f"{path.parent.name}/{entry['name']}",
                    file=path.relative_to(sct_root).as_posix(),
                    jenkinsfile=target,
                    quarantined=target.startswith(QUARANTINE_DIRS),
                    trigger=False,
                )
            )
    return pipelines


def deprecated_keys(sct_root: Path) -> set[str]:
    keys = set()
    for path in (sct_root / "jenkins-pipelines").rglob("_deprecated_jobs.yaml"):
        for job in (yaml.safe_load(path.read_text()) or {}).get("jobs") or []:
            keys.add(f"{path.parent.name}/{job['name']}".removesuffix("-test"))
    return keys


def jenkinsfile_test_cases(sct_root: Path, pipelines: list[Pipeline]) -> dict[str, list[str]]:
    """Return the test-case files that each jenkinsfile names, in order."""
    cases = {}
    for pipeline in pipelines:
        path = sct_root / pipeline.jenkinsfile
        if pipeline.jenkinsfile not in cases and path.is_file():
            cases[pipeline.jenkinsfile] = TEST_CASE_RE.findall(path.read_text(errors="replace"))
    return cases


def owning_teams(sct_root: Path, pipelines: list[Pipeline], cases: dict[str, list[str]]) -> dict[str, list[str]]:
    """Return the teams of each pipeline key, from test_metadata.team_ownership of its test-cases."""
    team_of_case = {}
    for case in {case for names in cases.values() for case in names}:
        path = sct_root / case
        if path.is_file():
            metadata = (yaml.safe_load(path.read_text()) or {}).get("test_metadata") or {}
            if metadata.get("team_ownership"):
                team_of_case[case] = metadata["team_ownership"]
    teams = defaultdict(list)
    for pipeline in pipelines:
        for case in cases.get(pipeline.jenkinsfile, []):
            team = team_of_case.get(case)
            if team and team not in teams[pipeline.key]:
                teams[pipeline.key].append(team)
    return dict(teams)


def pipelines_with_no_run(pipelines: list[Pipeline], ran_keys: set[str]) -> list[Pipeline]:
    """Return the pipelines with no run, without trigger jobs and pipelines in a quarantine folder."""
    seen = set()
    result = []
    for pipeline in pipelines:
        if pipeline.trigger or pipeline.quarantined or pipeline.key in ran_keys or pipeline.key in seen:
            continue
        seen.add(pipeline.key)
        result.append(pipeline)
    return result


def unreferenced_test_cases(sct_root: Path, cases: dict[str, list[str]]) -> list[str]:
    referenced = {case for names in cases.values() for case in names}
    all_cases = sorted(path.relative_to(sct_root).as_posix() for path in (sct_root / "test-cases").rglob("*.y*ml"))
    return [case for case in all_cases if case not in referenced]
