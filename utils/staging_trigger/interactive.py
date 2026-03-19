"""Interactive parameter editing, browsing, YAML and Python code generation."""

import functools
import logging
import subprocess
from pathlib import Path

import click
import questionary
import yaml

from sdcm.utils.common import get_latest_scylla_release, get_sct_root_path
from sdcm.utils.get_username import get_username
from sdcm.utils.version_utils import get_scylla_docker_repo_from_version

from utils.staging_trigger.constants import (
    CUSTOM_VALUE_SENTINEL,
    KNOWN_PARAM_CHOICES,
    ParamDefinition,
    SCT_REPO,
    _default_folder,
    get_presets,
)
from utils.staging_trigger.jenkins_client import JenkinsJobTrigger
from utils.staging_trigger.package_lookup import latest_unified_package
from utils.staging_trigger.trigger import detect_preset_from_jenkinsfile, detect_preset_from_job_name

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _fetch_latest_release() -> str | None:
    """Fetch the latest Scylla release version, cached for the session."""
    try:
        return get_latest_scylla_release("scylla")
    except Exception:  # noqa: BLE001
        logger.debug("Could not fetch latest scylla release", exc_info=True)
        return None


def _get_version_suggestions(current_value: str) -> list[questionary.Choice]:
    """Build a list of version suggestions for the scylla_version parameter."""
    choices = [
        questionary.Choice("master:latest", value="master:latest"),
    ]

    latest = _fetch_latest_release()
    if latest:
        choices.append(questionary.Choice(f"{latest} (latest release)", value=latest))

    if current_value and current_value not in {c.value for c in choices}:
        choices.append(questionary.Choice(f"{current_value} (current)", value=current_value))

    choices.append(questionary.Choice("Custom value...", value=CUSTOM_VALUE_SENTINEL))
    return choices


def _get_unified_package_suggestions(current_value: str) -> list[questionary.Choice] | None:
    """Build a list of unified package suggestions for the unified_package parameter.

    Returns:
        List of choices, or ``None`` if S3 lookup fails entirely.
    """
    choices: list[questionary.Choice] = []
    try:
        x86 = latest_unified_package("x86_64")
        if x86:
            choices.append(questionary.Choice(f"x86_64: {x86}", value=x86))
    except Exception:  # noqa: BLE001
        logger.debug("Could not fetch latest x86_64 unified package", exc_info=True)

    try:
        arm = latest_unified_package("aarch64")
        if arm:
            choices.append(questionary.Choice(f"aarch64: {arm}", value=arm))
    except Exception:  # noqa: BLE001
        logger.debug("Could not fetch latest aarch64 unified package", exc_info=True)

    if not choices:
        return None

    if current_value and current_value not in {c.value for c in choices}:
        choices.append(questionary.Choice(f"{current_value} (current)", value=current_value))

    choices.append(questionary.Choice("Custom value...", value=CUSTOM_VALUE_SENTINEL))
    return choices


def _prompt_for_value(key: str, current_value: str, param_meta: dict[str, ParamDefinition]) -> str:
    """Prompt for a single parameter value using the appropriate widget.

    Args:
        key: Parameter name.
        current_value: Current value of the parameter.
        param_meta: Dict of parameter metadata from Jenkins.

    Returns:
        The new value chosen by the user.
    """
    if key == "scylla_version":
        suggestions = _get_version_suggestions(current_value)
        picked = questionary.select(f"{key}:", choices=suggestions).ask()
        if picked is None:
            raise click.Abort()
        if picked == CUSTOM_VALUE_SENTINEL:
            picked = questionary.text(f"{key}:", default=current_value).ask()
            if picked is None:
                raise click.Abort()
        return picked

    if key == "unified_package":
        suggestions = _get_unified_package_suggestions(current_value)
        if suggestions:
            picked = questionary.select(f"{key}:", choices=suggestions).ask()
            if picked is None:
                raise click.Abort()
            if picked == CUSTOM_VALUE_SENTINEL:
                picked = questionary.text(f"{key}:", default=current_value).ask()
                if picked is None:
                    raise click.Abort()
            return picked
        # S3 unreachable — fall through to plain text input

    meta = param_meta.get(key)
    choices_list = (meta.choices if meta and meta.choices else None) or KNOWN_PARAM_CHOICES.get(key)
    if choices_list:
        select_choices = [questionary.Choice(c, value=c) for c in choices_list]
        select_choices.append(questionary.Choice("Custom value...", value=CUSTOM_VALUE_SENTINEL))
        picked = questionary.select(f"{key}:", choices=select_choices).ask()
        if picked is None:
            raise click.Abort()
        if picked == CUSTOM_VALUE_SENTINEL:
            picked = questionary.text(f"{key}:", default=current_value).ask()
            if picked is None:
                raise click.Abort()
        return picked

    new_value = questionary.text(f"{key}:", default=current_value).ask()
    if new_value is None:
        raise click.Abort()
    return new_value


def maybe_set_docker_image(all_params: dict[str, str]) -> None:
    """Auto-derive scylla_docker_image when backend is docker and scylla_version is set.

    Modifies ``all_params`` in place. An explicit user-set ``scylla_docker_image``
    that is non-empty takes precedence.
    """
    scylla_version = all_params.get("scylla_version", "")
    if not scylla_version:
        return

    # Only auto-set when backend looks like docker (or not specified — safe default)
    backend = all_params.get("backend", "")
    if backend and backend != "docker":
        return

    # Don't overwrite explicit user value
    existing = all_params.get("scylla_docker_image", "")
    if existing:
        return

    try:
        docker_repo = get_scylla_docker_repo_from_version(scylla_version)
        all_params["scylla_docker_image"] = docker_repo
        click.secho(f"  Auto-set scylla_docker_image={docker_repo} (from {scylla_version})", fg="green")
    except (ValueError, Exception):  # noqa: BLE001
        logger.debug("Could not derive docker image from version %s", scylla_version, exc_info=True)


def _display_params_table(params: dict[str, str], preset_keys: set[str]) -> None:
    """Display all parameters as a formatted table."""
    if not params:
        return
    max_key = max(len(k) for k in params)
    for key, value in params.items():
        source = click.style("[preset]", fg="cyan") if key in preset_keys else click.style("[jenkins]", dim=True)
        display_val = value if value else click.style("(empty)", dim=True)
        click.echo(f"  {key:<{max_key}}  {source}  {display_val}")


def prompt_for_params(preset_name: str, job_name: str | None = None, folder: str | None = None) -> dict[str, str]:
    """Interactively edit job parameters.

    Shows all parameters in a table, lets the user select which ones to modify
    via checkbox, edit those values, then review. Loops until the user is done.

    For ``scylla_version``, offers version suggestions via ``questionary.select``.
    For Jenkins choice parameters, offers the choices via ``questionary.select``.
    Otherwise, uses ``questionary.text``.
    """
    presets = get_presets()
    preset_params = dict(presets.get(preset_name, presets["longevity"]).params)
    preset_keys = set(preset_params.keys())

    param_meta: dict[str, ParamDefinition] = {}
    if job_name and folder:
        full_name = f"{folder}/{job_name}"
        click.echo(f"Fetching parameters from Jenkins for {job_name}...")
        client = JenkinsJobTrigger()
        try:
            param_meta = client.get_job_parameter_definitions(full_name)
        except Exception:  # noqa: BLE001
            click.secho("  Could not fetch parameters from Jenkins, using preset only.", fg="yellow")

    all_params = dict(preset_params)
    for k, v in param_meta.items():
        if k not in all_params:
            all_params[k] = v.default

    ordered_keys = list(preset_params.keys()) + [k for k in all_params if k not in preset_params]

    click.echo()
    click.secho(f"Parameters for preset: {preset_name}", bold=True)

    while True:
        click.echo()
        _display_params_table({k: all_params[k] for k in ordered_keys}, preset_keys)
        click.echo()

        action = questionary.select(
            "What would you like to do?",
            choices=[
                questionary.Choice("Select parameters to edit", value="edit"),
                questionary.Choice("Add a custom parameter", value="add"),
                questionary.Choice("Done - use these parameters", value="done"),
            ],
        ).ask()
        if not action or action == "done":
            break

        if action == "add":
            key = questionary.text("Parameter name:").ask()
            if not key:
                continue
            value = questionary.text(f"{key}:", default="").ask()
            if value is None:
                continue
            all_params[key] = value
            if key not in ordered_keys:
                ordered_keys.append(key)
            continue

        choices = []
        for key in ordered_keys:
            val_preview = all_params[key] if all_params[key] else "(empty)"
            if len(val_preview) > 50:
                val_preview = val_preview[:47] + "..."
            choices.append(
                questionary.Choice(
                    title=f"{key} = {val_preview}",
                    value=key,
                )
            )

        to_edit = questionary.checkbox(
            "Select parameters to edit (Space to toggle, Enter to confirm, type to search):",
            choices=choices,
            use_search_filter=True,
            use_jk_keys=False,
        ).ask()

        if not to_edit:
            continue

        for key in to_edit:
            all_params[key] = _prompt_for_value(key, all_params[key], param_meta)

    maybe_set_docker_image(all_params)

    return {k: all_params[k] for k in ordered_keys}


def browse_jenkinsfiles(pipeline_path: str | None = None) -> list[Path]:
    """Interactively browse and select jenkinsfiles under jenkins-pipelines/."""
    sct_root = get_sct_root_path()
    pipelines_dir = sct_root / "jenkins-pipelines"

    if pipeline_path:
        base = pipelines_dir / pipeline_path
        if not base.is_dir():
            click.secho(f"Directory not found: {base}", fg="red")
            return []
    else:
        categories = sorted(d.name for d in pipelines_dir.iterdir() if d.is_dir() and not d.name.startswith("."))
        if not categories:
            click.secho("No categories found under jenkins-pipelines/", fg="red")
            return []

        category = questionary.select(
            "Select pipeline category (type to search):",
            choices=categories,
            use_search_filter=True,
            use_jk_keys=False,
        ).ask()
        if not category:
            return []
        base = pipelines_dir / category

        subdirs = sorted(d.name for d in base.iterdir() if d.is_dir())
        if subdirs:
            choices = ["(all jenkinsfiles in this category)"] + subdirs
            pick = questionary.select(
                f"Select subcategory under {category}/ (type to search):",
                choices=choices,
                use_search_filter=True,
                use_jk_keys=False,
            ).ask()
            if not pick:
                return []
            if pick != choices[0]:
                base = base / pick

    jenkinsfiles = sorted(base.rglob("*.jenkinsfile"))
    if not jenkinsfiles:
        click.secho(f"No .jenkinsfile files found in {base.relative_to(pipelines_dir)}", fg="yellow")
        return []

    choices = []
    for jf in jenkinsfiles:
        rel = jf.relative_to(pipelines_dir)
        preset = detect_preset_from_jenkinsfile(jf) or "?"
        choices.append(
            questionary.Choice(
                title=f"{rel}  [{preset}]",
                value=jf,
            )
        )

    selected = questionary.checkbox(
        f"Select jenkinsfiles ({len(jenkinsfiles)} available):",
        choices=choices,
    ).ask()

    return selected or []


def generate_yaml_config(
    jenkinsfiles: list[Path],
    branch: str,
    folder: str,
    repo: str,
    output_path: str | None = None,
    param_overrides: dict[str, str] | None = None,
) -> str:
    """Generate a YAML config file for run-config from selected jenkinsfiles."""
    sct_root = get_sct_root_path()
    pipelines_dir = sct_root / "jenkins-pipelines"
    presets = get_presets()

    jobs = []
    for jf in jenkinsfiles:
        rel = jf.relative_to(pipelines_dir)
        preset = detect_preset_from_jenkinsfile(jf) or detect_preset_from_job_name(jf.stem) or "longevity"
        group = str(rel.parent) if str(rel.parent) != "." else ""
        if group:
            job_name = f"{group}/{jf.stem}-test"
        else:
            job_name = f"{jf.stem}-test"

        job_entry: dict = {"name": job_name, "preset": preset}
        if preset == "dtest":
            job_entry["dtest_topologies"] = ["no-tablets", "tablets"]
        job_entry["description"] = ""  # optional: shown in the markdown checklist
        preset_params = presets.get(preset, presets["longevity"]).params
        merged = {**preset_params, **(param_overrides or {})}
        non_empty = {k: v for k, v in merged.items() if v}
        if non_empty:
            job_entry["params"] = non_empty
        jobs.append(job_entry)

    global_params: dict[str, str] = {
        "email_recipients": f"{get_username()}@scylladb.com",
    }
    if param_overrides:
        global_params.update(param_overrides)

    config: dict = {
        "folder": folder,
        "branch": branch,
        "params": global_params,
        "jobs": jobs,
    }
    if repo != SCT_REPO:
        config["repo"] = repo

    yaml_text = yaml.dump(config, default_flow_style=False, sort_keys=False)

    if output_path:
        Path(output_path).write_text(yaml_text, encoding="utf-8")
        click.secho(f"Config written to: {output_path}", fg="green")
    else:
        click.echo()
        click.secho("--- Generated YAML config ---", bold=True)
        click.echo(yaml_text)
        click.secho("---", bold=True)
        click.echo("Save to a file and run with: staging_trigger.py run-config <file.yaml>")

    return yaml_text


def _write_jobs_yaml_config(
    job_names: list[str],
    branch: str,
    folder: str,
    repo: str,
    output_path: str | None = None,
    param_overrides: dict[str, str] | None = None,
) -> str:
    """Generate a YAML config from existing Jenkins job names (from find)."""
    presets = get_presets()
    jobs = []
    for name in job_names:
        preset = detect_preset_from_job_name(name) or "longevity"
        job_entry: dict = {"name": name, "preset": preset}
        if preset == "dtest":
            job_entry["dtest_topologies"] = ["no-tablets", "tablets"]
        job_entry["description"] = ""  # optional: shown in the markdown checklist
        preset_params = presets.get(preset, presets["longevity"]).params
        merged = {**preset_params, **(param_overrides or {})}
        non_empty = {k: v for k, v in merged.items() if v}
        if non_empty:
            job_entry["params"] = non_empty
        jobs.append(job_entry)

    global_params: dict[str, str] = {
        "email_recipients": f"{get_username()}@scylladb.com",
    }
    if param_overrides:
        global_params.update(param_overrides)

    config: dict = {
        "folder": folder,
        "branch": branch,
        "params": global_params,
        "jobs": jobs,
    }
    if repo != SCT_REPO:
        config["repo"] = repo

    yaml_text = yaml.dump(config, default_flow_style=False, sort_keys=False)

    if output_path:
        Path(output_path).write_text(yaml_text, encoding="utf-8")
        click.secho(f"Config written to: {output_path}", fg="green")
    else:
        click.echo()
        click.secho("--- Generated YAML config ---", bold=True)
        click.echo(yaml_text)
        click.secho("---", bold=True)
        click.echo("Save to a file and run with: staging_trigger.py run-config <file.yaml>")

    return yaml_text


def _ruff_format(code: str) -> str:
    """Format Python code using ruff, falling back to the original if ruff is unavailable."""
    try:
        result = subprocess.run(
            ["ruff", "format", "--stdin-filename", "generated.py", "-"],
            input=code,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        return code


def generate_python_code(
    job_names: list[str],
    branch: str,
    folder: str,
    repo: str,
    pr: int | None = None,
    param_overrides: dict[str, str] | None = None,
    dtest_topologies: list[str] | None = None,
    output_path: str | None = None,
) -> str:
    """Generate Python code that uses staging_trigger as a module."""
    non_empty_overrides = {k: v for k, v in (param_overrides or {}).items() if v}

    lines = [
        '"""Trigger staging jobs using staging_trigger module.',
        "",
        "Auto-generated by: staging_trigger.py",
        '"""',
        "",
        "from staging_trigger import StagingTrigger",
        "",
    ]

    jobs_by_preset: dict[str, list[str]] = {}
    for name in job_names:
        preset_name = detect_preset_from_job_name(name) or "longevity"
        jobs_by_preset.setdefault(preset_name, []).append(name)

    dtest_jobs = jobs_by_preset.pop("dtest", [])

    for preset_name, jobs in jobs_by_preset.items():
        if pr:
            constructor = f'trigger = StagingTrigger.from_pr({pr}, preset_name="{preset_name}"'
        else:
            constructor = f'trigger = StagingTrigger.from_preset("{preset_name}", branch="{branch}"'
            if repo != SCT_REPO:
                constructor += f', repo="{repo}"'

        if folder != _default_folder():
            constructor += f', folder="{folder}"'

        if non_empty_overrides:
            constructor += f", param_overrides={repr(non_empty_overrides)}"

        constructor += ")"
        lines.append(constructor)

        job_args = ", ".join(f'"{j}"' for j in jobs)
        lines.append(f"trigger.select_jobs({job_args})")
        lines.append("# descriptions: optional dict mapping job name to a checklist label")
        desc_dict = ", ".join(f'"{j}": ""' for j in jobs)
        lines.append(f"# trigger.run(descriptions={{{desc_dict}}})")
        lines.append("trigger.run()")
        lines.append("")

    for job_name in dtest_jobs:
        if pr:
            constructor = f'trigger = StagingTrigger.from_pr({pr}, preset_name="dtest"'
        else:
            constructor = f'trigger = StagingTrigger.from_preset("dtest", branch="{branch}"'
            if repo != SCT_REPO:
                constructor += f', repo="{repo}"'

        if folder != _default_folder():
            constructor += f', folder="{folder}"'

        if non_empty_overrides:
            constructor += f", param_overrides={repr(non_empty_overrides)}"

        constructor += ")"
        lines.append(constructor)

        topos = dtest_topologies or ["no-tablets", "tablets"]
        lines.append(f'trigger.run_dtest_variants("{job_name}", topologies={repr(topos)})')
        lines.append("")

    code = "\n".join(lines).rstrip() + "\n"
    code = _ruff_format(code)

    if output_path:
        Path(output_path).write_text(code, encoding="utf-8")
        click.secho(f"Python code written to: {output_path}", fg="green")
    else:
        click.echo()
        click.secho("--- Generated Python code ---", bold=True)
        click.echo(code)
        click.secho("---", bold=True)
        click.echo("Save to a file and run with: python <file.py>")

    return code


def _generate_python_from_jenkinsfiles(
    jenkinsfiles: list[Path],
    branch: str,
    folder: str,
    repo: str,
    pr: int | None = None,
    suffix: str = "-test",
    output_path: str | None = None,
    param_overrides: dict[str, str] | None = None,
) -> str:
    """Generate Python code from selected jenkinsfiles (for generate command)."""
    sct_root = get_sct_root_path()
    pipelines_dir = sct_root / "jenkins-pipelines"

    job_names = []
    for jf in jenkinsfiles:
        rel = jf.relative_to(pipelines_dir)
        group = str(rel.parent) if str(rel.parent) != "." else ""
        if group:
            job_names.append(f"{group}/{jf.stem}{suffix}")
        else:
            job_names.append(f"{jf.stem}{suffix}")

    return generate_python_code(
        job_names,
        branch=branch,
        folder=folder,
        repo=repo,
        pr=pr,
        param_overrides=param_overrides,
        output_path=output_path,
    )
