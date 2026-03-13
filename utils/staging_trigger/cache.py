"""Recent selections cache and source (repo/branch/PR) prompting."""

import json
import subprocess
from pathlib import Path

import click
import questionary

from utils.staging_trigger.constants import SCT_REPO

_CACHE_FILE = Path.home() / ".cache" / "sct-staging-trigger.json"


def _load_cache() -> dict:
    try:
        return json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_cache(data: dict) -> None:
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def lookup_pr(pr_number: int, repo: str = "scylladb/scylla-cluster-tests") -> tuple[str, str]:
    """Look up a PR's head repo and branch using the gh CLI."""
    result = subprocess.run(
        [
            "gh",
            "pr",
            "view",
            str(pr_number),
            "--repo",
            repo,
            "--json",
            "headRefName,headRepository,headRepositoryOwner",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)
    branch = data["headRefName"]
    owner = data["headRepositoryOwner"]["login"]
    repo_name = data["headRepository"]["name"]
    return f"git@github.com:{owner}/{repo_name}.git", branch


def prompt_for_source(
    branch: str | None = None,
    repo: str | None = None,
    pr: int | None = None,
) -> tuple[str, str, int | None]:
    """Prompt interactively for repo/branch or PR number.

    Returns (repo_url, branch_name, pr_number). The third element is the
    PR number when a PR was used, or None when branch mode was used.
    Uses a cache of recent selections so the user doesn't have to retype
    the same values every time.
    """
    cache = _load_cache()

    if pr:
        repo_url, branch_name = lookup_pr(pr)
        click.secho(f"PR #{pr}: {repo_url} @ {branch_name}", fg="cyan")
        cache["last_repo"] = repo_url
        cache["last_branch"] = branch_name
        _save_cache(cache)
        return repo_url, branch_name, pr

    if branch and repo:
        cache["last_repo"] = repo
        cache["last_branch"] = branch
        _save_cache(cache)
        return repo, branch, None

    # Interactive: ask user to choose PR or repo/branch
    source = questionary.select(
        "How do you want to specify the source?",
        choices=[
            questionary.Choice("Branch (repo + branch name)", value="branch"),
            questionary.Choice("Pull Request number", value="pr"),
        ],
    ).ask()
    if not source:
        raise click.Abort()

    if source == "pr":
        pr_num = questionary.text("PR number:").ask()
        if not pr_num:
            raise click.Abort()
        repo_url, branch_name = lookup_pr(int(pr_num))
        click.secho(f"PR #{pr_num}: {repo_url} @ {branch_name}", fg="cyan")
        cache["last_repo"] = repo_url
        cache["last_branch"] = branch_name
        _save_cache(cache)
        return repo_url, branch_name, int(pr_num)

    # Branch mode
    default_repo = repo or cache.get("last_repo", SCT_REPO)
    default_branch = branch or cache.get("last_branch", "master")

    repo_url = questionary.text(
        "Git repo SSH URL:",
        default=default_repo,
    ).ask()
    if not repo_url:
        raise click.Abort()

    branch_name = questionary.text(
        "Git branch:",
        default=default_branch,
    ).ask()
    if not branch_name:
        raise click.Abort()

    cache["last_repo"] = repo_url
    cache["last_branch"] = branch_name
    _save_cache(cache)
    return repo_url, branch_name, None
