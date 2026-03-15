"""High-level trigger interface and helpers for staging jobs."""

import logging
import re
from pathlib import Path

import click
import yaml

from sdcm.utils.common import get_sct_root_path
from sdcm.utils.version_utils import get_scylla_docker_repo_from_version

from utils.staging_trigger.cache import lookup_pr
from utils.staging_trigger.constants import (
    DTEST_TOPOLOGY_FLAGS,
    PIPELINE_TO_PRESET,
    Preset,
    SCT_REPO,
    TriggeredJob,
    _default_folder,
    _get_jenkins_url,
    get_presets,
)
from utils.staging_trigger.jenkins_client import JenkinsJobTrigger


def detect_preset_from_jenkinsfile(jenkinsfile_path: Path) -> str | None:
    """Read a .jenkinsfile and detect preset from the pipeline function call."""
    content = jenkinsfile_path.read_text(encoding="utf-8")
    for pipeline_func, preset_name in PIPELINE_TO_PRESET.items():
        if re.search(rf"\b{re.escape(pipeline_func)}\s*\(", content):
            return preset_name
    return None


def detect_preset_from_job_name(job_name: str) -> str | None:
    """Guess the preset from a Jenkins job name."""
    name_lower = job_name.lower()
    if "manager" in name_lower:
        return "manager"
    if "artifacts" in name_lower:
        return "artifacts"
    if "perf" in name_lower:
        return "perf"
    if "dtest" in name_lower:
        return "dtest"
    if any(kw in name_lower for kw in ("longevity", "gemini", "rolling-upgrade", "jepsen")):
        return "longevity"
    return None


def parse_set_params(params: tuple[str, ...]) -> dict[str, str]:
    """Parse KEY=VALUE pairs from --set options."""
    result = {}
    for p in params:
        if "=" not in p:
            raise click.BadParameter(f"Expected KEY=VALUE, got: {p}")
        key, value = p.split("=", 1)
        result[key] = value
    return result


def format_checklist(triggered_jobs: list[TriggeredJob]) -> str:
    """Generate a GitHub-flavored markdown checklist."""
    lines = []
    for job in triggered_jobs:
        build_url = f"{job.job_url}{job.build_number}/"
        desc = f" ({job.description})" if job.description else ""
        lines.append(f"- [ ] :clock1: [{job.job_name} #{job.build_number}]({build_url}){desc}")
    return "\n".join(lines)


def _make_dry_run_job(full_name: str, short_name: str, description: str = "", jenkins_url: str = "") -> TriggeredJob:
    """Create a placeholder TriggeredJob for dry-run checklist preview."""
    base_url = jenkins_url or _get_jenkins_url()
    job_url = f"{base_url}/job/{full_name.replace('/', '/job/')}/"
    return TriggeredJob(job_url=job_url, build_number=0, job_name=short_name, description=description)


def find_jenkinsfile(name: str) -> Path | None:
    """Find a .jenkinsfile by name under jenkins-pipelines/.

    Accepts bare stem ("longevity-100gb-4h"), partial path ("manager/debian12-manager-sanity"),
    or full path ("oss/longevity/longevity-100gb-4h").
    """
    sct_root = get_sct_root_path()
    pipelines_dir = sct_root / "jenkins-pipelines"

    candidate = pipelines_dir / f"{name}.jenkinsfile"
    if candidate.exists():
        return candidate

    stem = Path(name).stem
    matches = sorted(pipelines_dir.rglob(f"{stem}.jenkinsfile"))
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        click.echo(f"Multiple jenkinsfiles match '{name}':")
        for i, m in enumerate(matches, 1):
            click.echo(f"  {i}. {m.relative_to(pipelines_dir)}")
        idx = click.prompt("Pick one", type=click.IntRange(1, len(matches)))
        return matches[idx - 1]
    return None


logger = logging.getLogger(__name__)


def _auto_set_docker_image(params: dict[str, str]) -> None:
    """Auto-derive scylla_docker_image when backend is docker and version is set.

    Modifies ``params`` in place. Skips if scylla_docker_image is already set.
    """
    scylla_version = params.get("scylla_version", "")
    if not scylla_version:
        return
    backend = params.get("backend", "")
    if backend and backend != "docker":
        return
    if params.get("scylla_docker_image"):
        return
    try:
        params["scylla_docker_image"] = get_scylla_docker_repo_from_version(scylla_version)
    except (ValueError, Exception):  # noqa: BLE001
        logger.debug("Could not derive docker image from version %s", scylla_version, exc_info=True)


class StagingTrigger:
    """High-level interface for triggering staging jobs."""

    def __init__(self, git_repo: str, git_branch: str, preset: Preset, param_overrides: dict[str, str] | None = None):
        self.git_repo = git_repo
        self.git_branch = git_branch
        self.preset = preset
        self.param_overrides = param_overrides or {}
        self.selected_jobs: list[str] = []
        self.triggered: list[TriggeredJob] = []
        self._jenkins = JenkinsJobTrigger()

    @classmethod
    def from_preset(
        cls,
        preset_name: str,
        branch: str,
        repo: str = SCT_REPO,
        folder: str | None = None,
        param_overrides: dict[str, str] | None = None,
    ) -> "StagingTrigger":
        """Create from a named preset and branch."""
        preset = get_presets()[preset_name]
        if folder:
            preset = Preset(params=preset.params, folder_prefix=folder)
        return cls(git_repo=repo, git_branch=branch, preset=preset, param_overrides=param_overrides)

    @classmethod
    def from_pr(
        cls,
        pr_number: int,
        preset_name: str,
        gh_repo: str = "scylladb/scylla-cluster-tests",
        folder: str | None = None,
        param_overrides: dict[str, str] | None = None,
    ) -> "StagingTrigger":
        """Create by looking up a PR's repo and branch."""
        git_repo, branch = lookup_pr(pr_number, repo=gh_repo)
        preset = get_presets()[preset_name]
        if folder:
            preset = Preset(params=preset.params, folder_prefix=folder)
        click.secho(f"PR #{pr_number}: {git_repo} @ {branch}", fg="cyan")
        return cls(git_repo=git_repo, git_branch=branch, preset=preset, param_overrides=param_overrides)

    def find_jobs(self, pattern: str = "*") -> list[str]:
        """Find and display jobs matching a pattern."""
        jobs = self._jenkins.find_jobs(self.preset.folder_prefix, pattern)
        if not jobs:
            click.secho(f"No jobs matching '{pattern}' in {self.preset.folder_prefix}", fg="yellow")
        for i, j in enumerate(jobs, 1):
            preset_hint = detect_preset_from_job_name(j) or "?"
            click.echo(f"  {i:3d}. {j}  " + click.style(f"[{preset_hint}]", dim=True))
        return jobs

    def select_interactive(self, pattern: str = "*") -> "StagingTrigger":
        """Interactively list and pick jobs with click prompts."""
        jobs = self._jenkins.find_jobs(self.preset.folder_prefix, pattern)
        if not jobs:
            click.secho(f"No jobs matching '{pattern}' in {self.preset.folder_prefix}", fg="yellow")
            return self

        for i, j in enumerate(jobs, 1):
            preset_hint = detect_preset_from_job_name(j) or "?"
            click.echo(f"  {i:3d}. {j}  " + click.style(f"[{preset_hint}]", dim=True))

        click.echo()
        raw = click.prompt("Select jobs (numbers/ranges like '1 3 5-8', or 'all')", type=str)

        if raw.strip().lower() == "all":
            self.selected_jobs = jobs
        else:
            indices = []
            for part in raw.split():
                if "-" in part:
                    lo, hi = part.split("-", 1)
                    indices.extend(range(int(lo), int(hi) + 1))
                else:
                    indices.append(int(part))
            self.selected_jobs = [jobs[i - 1] for i in indices if 1 <= i <= len(jobs)]

        if self.selected_jobs:
            detected = detect_preset_from_job_name(self.selected_jobs[0])
            if detected and detected in get_presets():
                old_folder = self.preset.folder_prefix
                self.preset = Preset(params=get_presets()[detected].params, folder_prefix=old_folder)
                click.secho(f"Auto-detected preset: {detected}", fg="green")

            click.echo(f"Selected {len(self.selected_jobs)} job(s):")
            for j in self.selected_jobs:
                click.echo(f"  - {j}")
        else:
            click.secho("No jobs selected.", fg="yellow")

        return self

    def select_jobs(self, *job_names: str) -> "StagingTrigger":
        """Select jobs by name (short names, without folder prefix)."""
        self.selected_jobs = list(job_names)
        return self

    def _trigger_one(
        self, full_name: str, short_name: str, extra_params: dict[str, str], description: str, dry_run: bool
    ) -> TriggeredJob | None:
        if not dry_run:
            self._jenkins.update_scm(full_name, git_repo=self.git_repo, git_branch=self.git_branch)

        last_params = {} if dry_run else self._jenkins.get_last_build_params(full_name)
        merged = {**last_params, **self.preset.params, **self.param_overrides, **extra_params}
        _auto_set_docker_image(merged)

        if dry_run:
            non_empty = {k: v for k, v in sorted(merged.items()) if v}
            click.secho(f"[dry-run] {full_name}", fg="yellow")
            click.echo(f"  repo:   {self.git_repo}")
            click.echo(f"  branch: {self.git_branch}")
            for k, v in non_empty.items():
                click.echo(f"  {k}: {v}")
            click.echo()
            return _make_dry_run_job(full_name, short_name, description, jenkins_url=self._jenkins.url)

        job_url = self._jenkins.get_job_url(full_name)
        build_number = self._jenkins.trigger(full_name, merged)

        return TriggeredJob(job_url=job_url, build_number=build_number, job_name=short_name, description=description)

    def run(self, dry_run: bool = False, descriptions: dict[str, str] | None = None) -> list[TriggeredJob]:
        """Trigger all selected jobs.

        Merge order (last wins): last build params < preset < global overrides
        """
        descriptions = descriptions or {}
        self.triggered = []
        for short_name in self.selected_jobs:
            full_name = f"{self.preset.folder_prefix}/{short_name}"
            desc = descriptions.get(short_name, "")
            record = self._trigger_one(full_name, short_name, {}, desc, dry_run)
            if record:
                self.triggered.append(record)
        if self.triggered:
            if dry_run:
                click.secho("=== GitHub Markdown Preview ===", bold=True)
            click.echo(format_checklist(self.triggered))
        if dry_run:
            click.secho(
                f"\n[dry-run] {len(self.selected_jobs)} job(s) would be triggered. No changes were made.",
                fg="yellow",
                bold=True,
            )
        return self.triggered

    def run_multi(
        self, jobs_with_params: list[tuple[str, dict[str, str]]], dry_run: bool = False
    ) -> list[TriggeredJob]:
        """Trigger multiple jobs with per-job param overrides."""
        self.triggered = []
        for short_name, extra_params in jobs_with_params:
            full_name = f"{self.preset.folder_prefix}/{short_name}"
            record = self._trigger_one(full_name, short_name, extra_params, "", dry_run)
            if record:
                self.triggered.append(record)
        if self.triggered:
            if dry_run:
                click.secho("=== GitHub Markdown Preview ===", bold=True)
            click.echo(format_checklist(self.triggered))
        return self.triggered

    def run_dtest_variants(
        self, job_short_name: str, topologies: list[str] | None = None, dry_run: bool = False
    ) -> list[TriggeredJob]:
        """Trigger a dtest job once per topology variant."""
        if topologies is None:
            topologies = ["no-tablets", "tablets"]

        self.triggered = []
        full_name = f"{self.preset.folder_prefix}/{job_short_name}"

        if not dry_run:
            self._jenkins.update_scm(full_name, git_repo=self.git_repo, git_branch=self.git_branch)

        for topo_key in topologies:
            flag = DTEST_TOPOLOGY_FLAGS[topo_key]
            extra = {"PYTEST_EXTRA_COMMANDLINE_OPTIONS": flag}

            last_params = {} if dry_run else self._jenkins.get_last_build_params(full_name)
            merged = {**last_params, **self.preset.params, **self.param_overrides, **extra}
            _auto_set_docker_image(merged)

            if dry_run:
                non_empty = {k: v for k, v in sorted(merged.items()) if v}
                click.secho(f"[dry-run] {full_name} ({topo_key})", fg="yellow")
                for k, v in non_empty.items():
                    click.echo(f"  {k}: {v}")
                click.echo()
                self.triggered.append(
                    _make_dry_run_job(full_name, job_short_name, topo_key, jenkins_url=self._jenkins.url)
                )
                continue

            job_url = self._jenkins.get_job_url(full_name)
            build_number = self._jenkins.trigger(full_name, merged)
            self.triggered.append(
                TriggeredJob(job_url=job_url, build_number=build_number, job_name=job_short_name, description=topo_key)
            )

        if self.triggered:
            if dry_run:
                click.secho("=== GitHub Markdown Preview ===", bold=True)
            click.echo(format_checklist(self.triggered))
        return self.triggered


def run_from_config(config_path: str, dry_run: bool = False) -> list[TriggeredJob]:
    """Run triggers defined in a YAML config file.

    YAML structure:
        folder: scylla-staging/fruch       # optional, default: scylla-staging/fruch
        repo: git@github.com:...           # optional, default: SCT repo
        branch: my-branch                  # required (or pr: 12345)
        pr: 12345                          # alternative to branch

        params:                            # global param overrides applied to all jobs
          scylla_version: "master:latest"
          email_recipients: user@scylladb.com

        jobs:
          - name: longevity-100gb-4h-test
            preset: longevity              # optional, auto-detected from name
            params:                        # per-job overrides (merged on top of global)
              stress_duration: "90"

          - name: dtest-pytest-gating
            preset: dtest
            dtest_topologies: [no-tablets, tablets, gossip]
    """
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    folder = config.get("folder", _default_folder())
    repo = config.get("repo", SCT_REPO)
    global_params = config.get("params", {})
    # Ensure all values are strings
    global_params = {k: str(v) for k, v in global_params.items()}

    # Resolve branch from PR or direct
    pr_number = config.get("pr")
    branch = config.get("branch")
    if pr_number:
        repo, branch = lookup_pr(pr_number)
        click.secho(f"PR #{pr_number}: {repo} @ {branch}", fg="cyan")
    elif not branch:
        raise click.UsageError("YAML config must specify 'branch' or 'pr'")

    all_triggered: list[TriggeredJob] = []

    for job_spec in config.get("jobs", []):
        job_name = job_spec["name"]
        preset_name = job_spec.get("preset") or detect_preset_from_job_name(job_name) or "longevity"
        job_params = {k: str(v) for k, v in job_spec.get("params", {}).items()}

        # Merge: global params + per-job params
        merged_overrides = {**global_params, **job_params}

        trigger = StagingTrigger.from_preset(
            preset_name, branch=branch, repo=repo, folder=folder, param_overrides=merged_overrides
        )

        dtest_topologies = job_spec.get("dtest_topologies")
        if dtest_topologies:
            results = trigger.run_dtest_variants(job_name, topologies=dtest_topologies, dry_run=dry_run)
        else:
            trigger.select_jobs(job_name)
            results = trigger.run(dry_run=dry_run)

        all_triggered.extend(results)

    if all_triggered:
        click.echo()
        header = "=== GitHub Markdown Preview ===" if dry_run else "=== Full checklist ==="
        click.secho(header, bold=True)
        click.echo(format_checklist(all_triggered))

    return all_triggered
