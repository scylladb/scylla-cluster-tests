"""Jenkins job generation from jenkinsfiles."""

from pathlib import Path

import click
import yaml

from sdcm.utils.common import get_sct_root_path

from utils.staging_trigger.constants import SCT_REPO, _default_folder, _get_jenkins_url
from utils.staging_trigger.trigger import detect_preset_from_jenkinsfile, find_jenkinsfile


def _collect_folder_tree(jenkinsfiles: list[Path], pipelines_dir: Path) -> list[tuple[str, str, str]]:
    """Collect the folder tree that needs to be created for a set of jenkinsfiles.

    Returns a sorted list of (relative_path, display_name, description) tuples,
    ordered from shallowest to deepest so parent folders are created first.
    Reads _folder_definitions.yaml and _display_name files just like create_job_tree.
    """
    folders: dict[str, tuple[str, str]] = {}

    for jf in jenkinsfiles:
        rel = jf.relative_to(pipelines_dir)
        current = rel.parent
        while str(current) != ".":
            folder_path = str(current)
            if folder_path not in folders:
                abs_dir = pipelines_dir / current
                defs_file = abs_dir / "_folder_definitions.yaml"
                display_name = current.name
                description = ""
                if defs_file.exists():
                    defs = yaml.safe_load(defs_file.read_text(encoding="utf-8")) or {}
                    display_name = defs.get("folder-name", current.name)
                    description = defs.get("folder-description", "")
                dn_file = abs_dir / "_display_name"
                if dn_file.exists():
                    display_name = dn_file.read_text(encoding="utf-8").strip()
                folders[folder_path] = (display_name, description)
            current = current.parent

    return sorted(
        [(path, dn, desc) for path, (dn, desc) in folders.items()],
        key=lambda t: t[0].count("/"),
    )


def _ensure_folders(server, folder_tree: list[tuple[str, str, str]], dry_run: bool = False) -> None:
    """Create intermediate Jenkins folders for the given folder tree.

    Args:
        server: JenkinsPipelines instance (only used when not dry_run).
        folder_tree: Output of _collect_folder_tree().
        dry_run: If True, only print what would be created.
    """
    for rel_path, display_name, description in folder_tree:
        if dry_run:
            click.echo(f"  [folder] {rel_path}/  ({display_name})")
        else:
            server.create_directory(rel_path, display_name=display_name, description=description)
            click.echo(f"  [folder] {rel_path}/")


def generate_job(
    name: str,
    folder: str = "",
    branch: str = "master",
    repo: str = SCT_REPO,
    job_name_suffix: str = "-test",
    dry_run: bool = False,
) -> str | None:
    """Generate a single Jenkins pipeline job from a jenkinsfile.

    Creates intermediate Jenkins folders as needed, mirroring the directory
    structure under jenkins-pipelines/ (same as create_job_tree does).
    """
    folder = folder or _default_folder()
    jenkinsfile = find_jenkinsfile(name)
    if not jenkinsfile:
        click.secho(f"Could not find jenkinsfile for '{name}'", fg="red")
        return None

    sct_root = get_sct_root_path()
    pipelines_dir = sct_root / "jenkins-pipelines"
    rel = jenkinsfile.relative_to(pipelines_dir)
    group_name = str(rel.parent) if str(rel.parent) != "." else ""

    if group_name:
        full_job_name = f"{folder}/{group_name}/{jenkinsfile.stem}{job_name_suffix}"
    else:
        full_job_name = f"{folder}/{jenkinsfile.stem}{job_name_suffix}"

    preset_name = detect_preset_from_jenkinsfile(jenkinsfile) or "longevity"
    click.echo(f"Jenkinsfile: {click.style(str(rel), fg='cyan')}")
    click.echo(f"Pipeline:    {preset_name}")
    click.echo(f"Job path:    {full_job_name}")
    click.echo(f"SCM:         {repo} @ {branch}")

    folder_tree = _collect_folder_tree([jenkinsfile], pipelines_dir)

    if dry_run:
        if folder_tree:
            click.secho("[dry-run] Would create folders:", fg="yellow")
            _ensure_folders(None, folder_tree, dry_run=True)
        job_url = f"{_get_jenkins_url()}/job/{full_job_name.replace('/', '/job/')}/"
        click.secho(f"[dry-run] Would create: {job_url}", fg="yellow")
        return full_job_name

    from utils.build_system.create_test_release_jobs import JenkinsPipelines  # noqa: PLC0415

    server = JenkinsPipelines(base_job_dir=folder, sct_branch_name=branch, sct_repo=repo)

    if folder_tree:
        click.echo("Creating folders:")
        _ensure_folders(server, folder_tree)

    server.create_pipeline_job(jenkins_file=jenkinsfile, group_name=group_name, job_name_suffix=job_name_suffix)

    job_url = f"{_get_jenkins_url()}/job/{full_job_name.replace('/', '/job/')}/"
    click.secho(f"Created: {job_url}", fg="green")
    return full_job_name


def generate_jobs(
    jenkinsfiles: list[Path],
    folder: str = "",
    branch: str = "master",
    repo: str = SCT_REPO,
    job_name_suffix: str = "-test",
    dry_run: bool = False,
) -> list[str]:
    """Generate Jenkins pipeline jobs from a list of jenkinsfiles.

    Creates the full folder tree first (like create_job_tree), then creates
    each pipeline job. This is the batch equivalent of generate_job and the
    preferred path when creating multiple jobs at once.
    """
    folder = folder or _default_folder()
    if not jenkinsfiles:
        return []

    sct_root = get_sct_root_path()
    pipelines_dir = sct_root / "jenkins-pipelines"

    job_specs: list[tuple[Path, str, str]] = []
    for jf in jenkinsfiles:
        rel = jf.relative_to(pipelines_dir)
        group = str(rel.parent) if str(rel.parent) != "." else ""
        if group:
            full_name = f"{folder}/{group}/{jf.stem}{job_name_suffix}"
        else:
            full_name = f"{folder}/{jf.stem}{job_name_suffix}"
        job_specs.append((jf, group, full_name))

    folder_tree = _collect_folder_tree(jenkinsfiles, pipelines_dir)

    click.echo(f"Folder:  {click.style(folder, fg='cyan')}")
    click.echo(f"Jobs:    {len(job_specs)} pipeline(s)")
    if folder_tree:
        click.echo(f"Folders: {len(folder_tree)} directory(ies)")

    if dry_run:
        if folder_tree:
            click.secho("\n[dry-run] Would create folders:", fg="yellow")
            _ensure_folders(None, folder_tree, dry_run=True)
        click.secho(f"\n[dry-run] Would create {len(job_specs)} job(s) in {folder}:", fg="yellow")
        for _, _, full_name in job_specs:
            click.echo(f"  {full_name}")
        return [full_name for _, _, full_name in job_specs]

    from utils.build_system.create_test_release_jobs import JenkinsPipelines  # noqa: PLC0415

    server = JenkinsPipelines(base_job_dir=folder, sct_branch_name=branch, sct_repo=repo)

    if folder_tree:
        click.echo("\nCreating folders:")
        _ensure_folders(server, folder_tree)

    click.echo("\nCreating jobs:")
    created = []
    for jf, group, full_name in job_specs:
        server.create_pipeline_job(jenkins_file=jf, group_name=group, job_name_suffix=job_name_suffix)
        click.echo(f"  {full_name}")
        created.append(full_name)

    click.secho(f"\nSuccessfully created {len(created)} job(s) in {folder}", fg="green", bold=True)
    return created


def generate_from_path(
    pipeline_path: str,
    folder: str = "",
    branch: str = "master",
    repo: str = SCT_REPO,
    job_name_suffix: str = "-test",
    dry_run: bool = False,
) -> list[str]:
    """Generate all Jenkins jobs from a pipeline directory.

    Uses generate_jobs to create the full folder tree and all pipeline jobs.
    """
    folder = folder or _default_folder()
    sct_root = get_sct_root_path()
    full_path = sct_root / "jenkins-pipelines" / pipeline_path

    if not full_path.is_dir():
        click.secho(f"Directory not found: {full_path}", fg="red")
        return []

    jenkinsfiles = sorted(full_path.rglob("*.jenkinsfile"))
    if not jenkinsfiles:
        click.secho(f"No .jenkinsfile files in {full_path}", fg="yellow")
        return []

    click.echo(f"Found {len(jenkinsfiles)} jenkinsfile(s) in {pipeline_path}")
    return generate_jobs(
        jenkinsfiles, folder=folder, branch=branch, repo=repo, job_name_suffix=job_name_suffix, dry_run=dry_run
    )
