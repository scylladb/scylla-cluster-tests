"""Click CLI for the staging trigger tool."""

import os
from pathlib import Path

import click
import questionary

from sdcm.utils.common import get_sct_root_path

from utils.staging_trigger.cache import _load_cache, prompt_for_source
from utils.staging_trigger.constants import (
    DTEST_TOPOLOGY_FLAGS,
    PRESET_NAMES,
    _default_folder,
)
from utils.staging_trigger.interactive import (
    _write_jobs_yaml_config,
    browse_jenkinsfiles,
    generate_python_code,
    generate_yaml_config,
    prompt_for_params,
)
from utils.staging_trigger.job_generation import generate_from_path, generate_job, generate_jobs
from utils.staging_trigger.jenkins_client import JenkinsJobTrigger
from utils.staging_trigger.trigger import (
    StagingTrigger,
    detect_preset_from_job_name,
    parse_set_params,
    run_from_config,
)


# ---------------------------------------------------------------------------
# Shell completion helpers
# ---------------------------------------------------------------------------


class _JobNameType(click.ParamType):
    """Click parameter type with shell completion for Jenkins job names."""

    name = "job"

    def shell_complete(self, ctx, param, incomplete):
        folder = (ctx.parent.params.get("folder") if ctx.parent else None) or _default_folder()
        try:
            client = JenkinsJobTrigger()
            jobs = client.list_jobs_in_folder(folder)
        except Exception:  # noqa: BLE001
            return []
        return [click.shell_completion.CompletionItem(j) for j in jobs if j.startswith(incomplete)]


def _complete_branches(ctx, param, incomplete):
    """Complete git branches from cache of recent selections."""
    cache = _load_cache()
    candidates = {"master"}
    last = cache.get("last_branch")
    if last:
        candidates.add(last)
    return [click.shell_completion.CompletionItem(b) for b in sorted(candidates) if b.startswith(incomplete)]


def _complete_jenkinsfiles(ctx, param, incomplete):
    """Complete jenkinsfile stems under jenkins-pipelines/."""
    try:
        sct_root = get_sct_root_path()
    except Exception:  # noqa: BLE001
        return []
    pipelines_dir = sct_root / "jenkins-pipelines"
    if not pipelines_dir.is_dir():
        return []
    stems = sorted({jf.stem for jf in pipelines_dir.rglob("*.jenkinsfile")})
    return [click.shell_completion.CompletionItem(s) for s in stems if s.startswith(incomplete)]


def _complete_yaml_files(ctx, param, incomplete):
    """Complete YAML files in the current directory."""
    cwd = Path.cwd()
    return [
        click.shell_completion.CompletionItem(f.name) for f in cwd.glob("*.yaml") if f.name.startswith(incomplete)
    ] + [click.shell_completion.CompletionItem(f.name) for f in cwd.glob("*.yml") if f.name.startswith(incomplete)]


# ---------------------------------------------------------------------------
# Click CLI
# ---------------------------------------------------------------------------


@click.group()
@click.option(
    "--folder",
    "-f",
    default=None,
    show_default=False,
    help="Jenkins staging folder (default: scylla-staging/<username>).",
)
@click.pass_context
def cli(ctx, folder):
    """SCT staging Jenkins job trigger and generator."""
    ctx.ensure_object(dict)
    ctx.obj["folder"] = folder or _default_folder()


@cli.command()
@click.argument("jobs", nargs=-1, type=_JobNameType())
@click.option("--branch", "-b", default=None, help="Git branch.", shell_complete=_complete_branches)
@click.option("--repo", default=None, help="Git repo SSH URL.")
@click.option("--pr", default=None, type=int, help="GitHub PR number (auto-detects repo/branch).")
@click.option(
    "--preset",
    "-p",
    type=click.Choice(PRESET_NAMES),
    default=None,
    help="Force a preset (auto-detected from job name if omitted).",
)
@click.option(
    "--set", "-s", "set_params", multiple=True, metavar="KEY=VALUE", help="Override any Jenkins parameter. Repeatable."
)
@click.option(
    "--filter", "find_pattern", default="*", metavar="PATTERN", help="Glob pattern to filter jobs in interactive mode."
)
@click.option("--dry-run", "-n", is_flag=True, help="Print what would be triggered.")
@click.option("--list-only", "-l", is_flag=True, help="Just list matching jobs, no selection.")
@click.option("--edit-params", "-e", is_flag=True, help="Interactively edit parameters before triggering.")
@click.option("--output-yaml", "-o", default=None, metavar="FILE", help="Write selected jobs as a YAML config file.")
@click.option(
    "--dtest-topologies",
    "-t",
    multiple=True,
    type=click.Choice(list(DTEST_TOPOLOGY_FLAGS.keys())),
    help="Dtest topology variants. Repeatable.",
)
@click.pass_context
def trigger(  # noqa: PLR0913, PLR0911, PLR0914
    ctx,
    jobs,
    branch,
    repo,
    pr,
    preset,
    set_params,
    find_pattern,
    dry_run,
    list_only,
    edit_params,
    output_yaml,
    dtest_topologies,
):
    """Trigger staging Jenkins jobs.

    Interactive by default: finds jobs, lets you select via checkbox, then
    trigger, dry-run, or export to YAML. Pass job names as arguments to
    skip interactive selection and trigger directly.

    \b
    Examples:
      trigger                                         # interactive browse & trigger
      trigger "longevity*"                            # filter, select, trigger
      trigger -b my-branch longevity-100gb-4h-test    # direct trigger (no interaction)
      trigger --pr 12345 manager-ubuntu20-sanity-test # trigger from PR
      trigger -e                                      # edit params before triggering
      trigger -n                                      # dry run
      trigger -o my_tests.yaml                        # select and export YAML config
      trigger -l "manager*"                           # just list matching jobs
      trigger -b my-branch longevity-100gb-4h-test -s scylla_version=2025.1
      trigger -b master dtest-pytest-gating -t no-tablets -t tablets
    """
    folder = ctx.obj["folder"]
    overrides = parse_set_params(set_params)

    if jobs and len(jobs) == 1 and any(c in jobs[0] for c in "*?"):
        find_pattern = jobs[0]
        jobs = ()

    if jobs:
        if not branch and not pr:
            raise click.UsageError("Provide --branch or --pr when specifying jobs directly")

        if not preset:
            preset = detect_preset_from_job_name(jobs[0]) or "longevity"

        repo, branch = prompt_for_source(branch=branch, repo=repo, pr=pr)

        if edit_params:
            overrides = prompt_for_params(preset, job_name=jobs[0], folder=folder)

        strigger = StagingTrigger.from_preset(
            preset, branch=branch, repo=repo, folder=folder, param_overrides=overrides or None
        )

        if dtest_topologies:
            for job_name in jobs:
                strigger.run_dtest_variants(job_name, topologies=list(dtest_topologies), dry_run=dry_run)
        else:
            strigger.select_jobs(*jobs)
            strigger.run(dry_run=dry_run)
        return

    client = JenkinsJobTrigger()
    found_jobs = client.find_jobs(folder, find_pattern)
    if not found_jobs:
        click.secho(f"No jobs matching '{find_pattern}' in {folder}", fg="yellow")
        return

    if list_only:
        for i, j in enumerate(found_jobs, 1):
            preset_hint = detect_preset_from_job_name(j) or "?"
            click.echo(f"  {i:3d}. {j}  " + click.style(f"[{preset_hint}]", dim=True))
        click.echo(f"\n{len(found_jobs)} job(s) found.")
        return

    choices = []
    for j in found_jobs:
        preset_hint = detect_preset_from_job_name(j) or "?"
        choices.append(
            questionary.Choice(
                title=f"{j}  [{preset_hint}]",
                value=j,
            )
        )

    selected = questionary.checkbox(
        f"Select jobs ({len(found_jobs)} available, Space to toggle, Enter to confirm):",
        choices=choices,
    ).ask()

    if not selected:
        click.secho("No jobs selected.", fg="yellow")
        return

    click.echo(f"\nSelected {len(selected)} job(s):")
    for j in selected:
        preset_hint = detect_preset_from_job_name(j) or "?"
        click.echo(f"  - {j}  [{preset_hint}]")

    if output_yaml:
        repo, branch = prompt_for_source(branch=branch, repo=repo, pr=pr)
        _write_jobs_yaml_config(selected, branch=branch, folder=folder, repo=repo, output_path=output_yaml)
        return

    action = questionary.select(
        "What would you like to do with the selected jobs?",
        choices=[
            questionary.Choice("Trigger them now", value="trigger"),
            questionary.Choice("Dry run (show what would be triggered)", value="dry"),
            questionary.Choice("Generate YAML config (for run-config)", value="yaml"),
            questionary.Choice("Generate Python code (use as module)", value="code"),
        ],
    ).ask()
    if not action:
        return

    repo, branch = prompt_for_source(branch=branch, repo=repo, pr=pr)

    first_preset = preset or detect_preset_from_job_name(selected[0]) or "longevity"
    want_edit = (
        edit_params
        or questionary.confirm(
            "Edit job parameters before proceeding?",
            default=False,
        ).ask()
    )
    if want_edit:
        all_overrides = prompt_for_params(first_preset, job_name=selected[0], folder=folder)
    else:
        all_overrides = overrides or {}

    if action == "yaml":
        out_path = questionary.text(
            "Output YAML file path:",
            default="staging_jobs.yaml",
        ).ask()
        _write_jobs_yaml_config(
            selected, branch=branch, folder=folder, repo=repo, output_path=out_path, param_overrides=all_overrides
        )
        return

    if action == "code":
        out_path = questionary.text(
            "Output Python file path (empty for stdout):",
            default="",
        ).ask()
        generate_python_code(
            selected,
            branch=branch,
            folder=folder,
            repo=repo,
            pr=pr,
            param_overrides=all_overrides or None,
            dtest_topologies=list(dtest_topologies) if dtest_topologies else None,
            output_path=out_path or None,
        )
        return

    is_dry = action == "dry" or dry_run
    for job_name in selected:
        preset_name = preset or detect_preset_from_job_name(job_name) or "longevity"

        strigger = StagingTrigger.from_preset(
            preset_name, branch=branch, repo=repo, folder=folder, param_overrides=all_overrides or None
        )
        strigger.select_jobs(job_name)
        strigger.run(dry_run=is_dry)


@cli.command("run-config")
@click.argument("config_file", type=click.Path(exists=True), shell_complete=_complete_yaml_files)
@click.option("--dry-run", "-n", is_flag=True, help="Print what would be triggered.")
def run_config(config_file, dry_run):
    """Trigger jobs defined in a YAML config file.

    \b
    Example YAML:
      folder: scylla-staging/<username>   # optional, defaults to your username
      branch: my-feature-branch
      params:
        scylla_version: "master:latest"
      jobs:
        - name: longevity-100gb-4h-test
          preset: longevity
        - name: manager-ubuntu20-sanity-test
          preset: manager
          params:
            manager_version: "3.8"
        - name: dtest-pytest-gating
          preset: dtest
          dtest_topologies: [no-tablets, tablets]
    """
    run_from_config(config_file, dry_run=dry_run)


def _show_trigger_examples(job_names: list[str], branch: str = "", folder: str = "", pr: int | None = None) -> None:
    """Show example CLI commands for triggering the generated jobs."""
    click.echo()
    click.secho("To trigger these jobs, use:", fg="cyan", bold=True)

    source_flag = f"--pr {pr}" if pr else f"-b {branch}" if branch else "-b <branch>"
    folder_flag = f"-f {folder} " if folder else ""

    if len(job_names) == 1:
        name = job_names[0].rsplit("/", 1)[-1]
        click.echo(f"  python staging_trigger.py {folder_flag}trigger {source_flag} {name}")
    else:
        names = " ".join(j.rsplit("/", 1)[-1] for j in job_names[:3])
        if len(job_names) > 3:
            names += " ..."
        click.echo(f"  python staging_trigger.py {folder_flag}trigger {source_flag} {names}")

    click.echo()
    click.echo("  Add -n for dry run, -e to edit parameters first.")


@cli.command()
@click.argument("pipelines", nargs=-1, shell_complete=_complete_jenkinsfiles)
@click.option(
    "--branch", "-b", default=None, help="Git branch for the job's SCM config.", shell_complete=_complete_branches
)
@click.option("--repo", default=None, help="Git repo SSH URL.")
@click.option("--pr", default=None, type=int, help="GitHub PR number (auto-detects repo/branch).")
@click.option(
    "--path",
    "pipeline_path",
    default=None,
    metavar="PATH",
    help="Directory under jenkins-pipelines/ to create all jobs from.",
)
@click.option("--suffix", default="-test", show_default=True, help="Job name suffix.")
@click.option("--dry-run", "-n", is_flag=True, help="Show what would be created without creating.")
@click.option(
    "--output-yaml", "-o", default=None, metavar="FILE", help="Write a YAML config instead of creating jobs on Jenkins."
)
@click.pass_context
def generate(ctx, pipelines, branch, repo, pr, pipeline_path, suffix, dry_run, output_yaml):
    """Generate Jenkins pipeline jobs from jenkinsfiles.

    Interactive by default: browses jenkins-pipelines/ categories and lets you
    select jenkinsfiles, then creates the jobs on Jenkins. After creation,
    shows example CLI commands for triggering the generated jobs.

    PIPELINES are jenkinsfile stems (e.g. "longevity-100gb-4h") or paths
    (e.g. "manager/debian12-manager-sanity") for direct (non-interactive) mode.

    \b
    Examples:
      generate -b my-branch                           # interactive browse
      generate --pr 12345                             # use PR branch
      generate -b my-branch --path oss/longevity      # all jobs in path
      generate -b my-branch longevity-100gb-4h        # direct, no interaction
      generate -b my-branch --path oss/longevity -n   # batch dry run
      generate -b my-branch -o my_tests.yaml          # export YAML instead

    Note: Use -f/--folder BEFORE the subcommand:
      staging_trigger.py -f scylla-staging/yulia generate -b my-branch
    """
    folder = ctx.obj["folder"]

    repo, branch = prompt_for_source(branch=branch, repo=repo, pr=pr)

    click.echo(f"Folder:  {click.style(folder, fg='cyan')}")
    click.echo(f"Source:  {repo} @ {branch}")
    click.echo()

    if pipelines:
        created = []
        for name in pipelines:
            result = generate_job(
                name, folder=folder, branch=branch, repo=repo, job_name_suffix=suffix, dry_run=dry_run
            )
            if result:
                created.append(result)
        if created:
            click.echo()
            click.secho(f"Successfully created {len(created)} job(s) in {folder}", fg="green", bold=True)
            _show_trigger_examples(created, branch=branch, folder=folder, pr=pr)
        return

    if pipeline_path and not output_yaml:
        created = generate_from_path(
            pipeline_path, folder=folder, branch=branch, repo=repo, job_name_suffix=suffix, dry_run=dry_run
        )
        if created:
            _show_trigger_examples(created, branch=branch, folder=folder, pr=pr)
        return

    selected = browse_jenkinsfiles(pipeline_path)
    if not selected:
        click.secho("No jenkinsfiles selected.", fg="yellow")
        return

    click.echo(f"\nSelected {len(selected)} jenkinsfile(s):")
    sct_root = get_sct_root_path()
    pipelines_dir = sct_root / "jenkins-pipelines"
    for jf in selected:
        click.echo(f"  {jf.relative_to(pipelines_dir)}")

    if output_yaml:
        generate_yaml_config(selected, branch=branch, folder=folder, repo=repo, output_path=output_yaml)
        return

    created = generate_jobs(selected, folder=folder, branch=branch, repo=repo, job_name_suffix=suffix, dry_run=dry_run)

    if created:
        _show_trigger_examples(created, branch=branch, folder=folder, pr=pr)


@cli.command("setup-completion")
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish"]), default=None, required=False)
def setup_completion(shell):
    """Print shell completion setup instructions and install the completion script.

    \b
    Detects your current shell automatically, or specify one explicitly:
      setup-completion bash
      setup-completion zsh
      setup-completion fish
    """
    if shell is None:
        current_shell = os.path.basename(os.environ.get("SHELL", ""))
        if current_shell in ("bash", "zsh", "fish"):
            shell = current_shell
        else:
            click.secho(f"Could not detect shell (SHELL={current_shell}). Specify: bash, zsh, or fish.", fg="red")
            raise SystemExit(1)

    prog_name = "staging-trigger"
    sct_root = get_sct_root_path()
    wrapper = sct_root / prog_name

    # Create wrapper script if it doesn't exist
    if not wrapper.exists():
        wrapper.write_text(
            "#!/usr/bin/env bash\n"
            "# Auto-generated wrapper for tab completion support\n"
            f'exec uv run python "{sct_root}/staging_trigger.py" "$@"\n'
        )
        wrapper.chmod(0o755)
        click.secho(f"Created wrapper script: {wrapper}", fg="green")

    env_var = f"_{prog_name.replace('-', '_').upper()}_COMPLETE"

    if shell == "bash":
        rc_file = "~/.bashrc"
        eval_cmd = f'eval "$({env_var}=bash_source {wrapper})"'
    elif shell == "zsh":
        rc_file = "~/.zshrc"
        eval_cmd = f'eval "$({env_var}=zsh_source {wrapper})"'
    else:
        rc_file = "~/.config/fish/completions/staging-trigger.fish"
        eval_cmd = f"{env_var}=fish_source {wrapper} > {rc_file}"

    click.echo()
    click.secho(f"Shell completion for {shell}:", bold=True)
    click.echo()
    click.echo(f"  Add this to {rc_file}:")
    click.echo()
    click.secho(f"    {eval_cmd}", fg="cyan")
    click.echo()
    click.echo("  Then restart your shell or run:")
    click.echo()
    click.secho(f"    source {rc_file}", fg="cyan")
    click.echo()
    click.echo(f"  After that, use '{prog_name}' instead of 'python staging_trigger.py':")
    click.echo()
    click.secho(f"    {prog_name} trigger <TAB>", fg="cyan")
    click.secho(f"    {prog_name} generate <TAB>", fg="cyan")
    click.echo()

    # Try adding to rc file
    rc_path = Path(os.path.expanduser(rc_file))
    if rc_path.exists():
        content = rc_path.read_text()
        if eval_cmd not in content:
            if click.confirm(f"Add completion to {rc_file} automatically?", default=True):
                with rc_path.open("a") as f:
                    f.write(f"\n# staging-trigger tab completion\n{eval_cmd}\n")
                click.secho(f"Added to {rc_file}. Run 'source {rc_file}' to activate.", fg="green")
        else:
            click.secho(f"Completion already configured in {rc_file}.", fg="green")
