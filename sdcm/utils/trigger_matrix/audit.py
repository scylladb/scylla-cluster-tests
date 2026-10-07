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

"""Check that each matrix entry's arch matches what its job really runs on, per release.

A matrix entry's arch never reaches the Jenkins job: the job gets `scylla_version` and runs on
whatever arch its own config resolves to, from the SCT branch of that release (`scylla-2026.1/*`
jobs run `branch-2026.1`). The entry's arch only decides which per-arch release run triggers the
job. When the two disagree the job runs on the wrong arch, or twice (SCT-1168).

This reads every release's jenkinsfiles and test configs straight from git, resolves the DB
instance the same way SCT does, and reports where the declared arch is wrong. Jobs it cannot map
to a jenkinsfile, or whose instance it cannot resolve, are reported as unchecked.
"""

import ast
import io
import json
import logging
import re
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import anyconfig
import yaml

from sdcm import sct_abs_path
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog
from sdcm.utils.cloud_catalog.instance_matcher import NoMatchingInstanceError, select_instance
from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.filters import _is_version_excluded, _is_version_included
from sdcm.utils.trigger_matrix.models import JobConfig, job_arch

logger = logging.getLogger(__name__)

# Everything that decides a job's DB instance type.
TREE_PATHS = ("jenkins-pipelines", "test-cases", "configurations", "defaults", "data/instance_catalog")
INSTANCE_TYPE_PARAM = {
    "aws": "instance_type_db",
    "gce": "gce_instance_type_db",
    "azure": "azure_instance_type_db",
    "oci": "oci_instance_type_db",
}
ARCH_ALIASES = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64", "amd64": "x86_64"}


@dataclass
class ArchDrift:
    matrix: str
    job_name: str
    declared: str
    actual: str
    instance: str
    versions: list[str] = field(default_factory=list)


@dataclass
class Unchecked:
    matrix: str
    job_name: str
    reason: str
    versions: list[str] = field(default_factory=list)


@dataclass
class AuditReport:
    drifts: list[ArchDrift] = field(default_factory=list)
    unchecked: list[Unchecked] = field(default_factory=list)
    versions: list[str] = field(default_factory=list)


def branch_ref(version: str, remote: str) -> str:
    return f"{remote}/master" if version == "master" else f"{remote}/branch-{version}"


def extract_tree(ref: str, dest: Path) -> bool:
    """Extract the paths that decide a job's instance type at `ref` into `dest`."""
    present = [
        p
        for p in TREE_PATHS
        if subprocess.run(["git", "cat-file", "-e", f"{ref}:{p}"], capture_output=True, check=False).returncode == 0
    ]
    if not present:
        return False
    archive = subprocess.run(["git", "archive", ref, *present], check=True, capture_output=True).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(dest, filter="data")
    return True


def find_jenkinsfile(root: Path, job_name: str) -> Path | None:
    """Map a matrix job name to the jenkinsfile Jenkins generated it from.

    Jobs are named after the jenkinsfile plus a "-test" suffix, under a folder named after its
    directory (`tier1/foo-test` <- `jenkins-pipelines/oss/tier1/foo.jenkinsfile`). Prefer `oss/`,
    which is what the `scylla-*` folders are built from, over its `vnodes/` copies.
    """
    parts = job_name.strip("/").split("/")
    name, folder = parts[-1], parts[-2] if len(parts) > 1 else ""
    stems = {name, name.removesuffix("-test")}
    found = [p for p in (root / "jenkins-pipelines").rglob("*.jenkinsfile") if p.stem in stems]
    found = [p for p in found if p.parent.name == folder] or found
    found.sort(key=lambda p: ("/oss/" not in p.as_posix(), "vnodes" in p.parts, len(p.parts)))
    return found[0] if found else None


def jenkinsfile_test_config(text: str) -> list[str]:
    if match := re.search(r"test_config\s*:\s*(?:'''|\"\"\"|'|\")?\s*(\[.*?\])", text, re.S):
        try:
            return ast.literal_eval(match.group(1))
        except ValueError, SyntaxError:
            return re.findall(r"[\w./-]+\.yaml", match.group(1))
    match = re.search(r"test_config\s*:\s*['\"]([^'\"\[]+)['\"]", text)
    return [match.group(1)] if match else []


def resolve_db_instance(root: Path, job: JobConfig, defaults: dict) -> tuple[str | None, str]:
    """Return (arch, instance type) of the job's DB nodes in the tree at `root`, or (None, reason)."""
    param = INSTANCE_TYPE_PARAM.get(job.backend)
    if not param:
        return None, f"{job.backend} backend has no DB instance type"
    jenkinsfile = find_jenkinsfile(root, job.job_name)
    if not jenkinsfile:
        return None, "no jenkinsfile found for the job"
    text = jenkinsfile.read_text(encoding="utf-8")
    params = {**defaults, **job.params}

    test_config = params.get("test_config")
    if isinstance(test_config, str):
        test_config = json.loads(test_config) if test_config.strip().startswith("[") else [test_config]
    config: dict = {}
    for name in [
        "defaults/test_default.yaml",
        f"defaults/{job.backend}_config.yaml",
        *(test_config or jenkinsfile_test_config(text)),
    ]:
        if (path := root / name).exists():
            anyconfig.merge(config, yaml.safe_load(path.read_text(encoding="utf-8")) or {}, ac_merge=anyconfig.MS_DICTS)

    catalog_dir = root / "data/instance_catalog"
    if not catalog_dir.exists():
        catalog_dir = Path(sct_abs_path("data/instance_catalog"))
    catalog = InstanceCatalog.from_directory(catalog_dir)

    in_jenkinsfile = re.search(rf"\b{param}\s*:\s*['\"]([^'\"]+)['\"]", text)
    instance = params.get(param) or (in_jenkinsfile and in_jenkinsfile.group(1)) or config.get(param)
    if not instance and isinstance(sizing := config.get("sizing_db"), dict):
        try:
            instance = select_instance(catalog, "db", job.backend, sizing).instance_type
        except NoMatchingInstanceError as exc:
            return None, f"sizing_db matches no instance: {exc}"
    if not instance:
        return None, "no DB instance type in the job's config"
    arch = next((i.arch for i in catalog.instances if i.cloud == job.backend and i.instance_type == instance), None)
    if not arch and job.backend == "aws":
        # Not every type is in the catalog (c8g, c8i); a "g" after the generation digit is Graviton.
        arch = "aarch64" if re.match(r"^[a-z]+\d+[a-z]*g[a-z]*\.", instance) else "x86_64"
    if not arch:
        return None, f"{instance} is not in the instance catalog"
    return ARCH_ALIASES.get(str(arch).lower(), str(arch)), instance


def runs_on_version(job: JobConfig, version: str) -> bool:
    if job.disabled:
        return False
    if job.include_versions and not _is_version_included(version, job.include_versions):
        return False
    return not _is_version_excluded(version, job.exclude_versions)


def audit_matrices(matrix_files: list[Path], versions: list[str], remote: str = "origin") -> AuditReport:
    """Compare every entry's declared arch with what its job resolves to on each release it runs for.

    Relative job names live in the release's own folder and run that release's SCT branch.
    Absolute ones (`/scylla-enterprise/...`) are a single job, checked against master.
    """
    report = AuditReport()
    with tempfile.TemporaryDirectory(prefix="trigger-matrix-audit-") as tmp:
        trees: dict[str, Path] = {}
        for version in dict.fromkeys(["master", *versions]):
            dest = Path(tmp) / version
            if extract_tree(branch_ref(version, remote), dest):
                trees[version] = dest
            else:
                logger.warning("Skipping %s: %s has none of %s", version, branch_ref(version, remote), TREE_PATHS)
        report.versions = [v for v in versions if v in trees]

        drifts: dict[tuple, ArchDrift] = {}
        unchecked: dict[tuple, Unchecked] = {}
        for matrix_file in matrix_files:
            config = load_matrix_config(matrix_file)
            for job in config.jobs:
                absolute = job.job_name.startswith("/")
                for version in report.versions:
                    if not runs_on_version(job, version):
                        continue
                    arch, detail = resolve_db_instance(trees["master" if absolute else version], job, config.defaults)
                    if arch is None:
                        finding = unchecked.setdefault(
                            (matrix_file.stem, job.job_name, detail), Unchecked(matrix_file.stem, job.job_name, detail)
                        )
                    elif arch != job_arch(job):
                        finding = drifts.setdefault(
                            (matrix_file.stem, job.job_name, job_arch(job), arch, detail),
                            ArchDrift(matrix_file.stem, job.job_name, job_arch(job), arch, detail),
                        )
                    else:
                        continue
                    finding.versions.append(version)
        report.drifts = list(drifts.values())
        report.unchecked = list(unchecked.values())
    return report
