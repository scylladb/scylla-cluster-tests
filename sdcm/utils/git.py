"""
Simple git wrappers that provide useful information about current repository
"""
import subprocess
import logging

LOGGER = logging.getLogger(__name__)


def get_git_commit_id() -> str:
    try:
        proc = subprocess.run(args=["git", "rev-parse", "HEAD"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        LOGGER.warning("Error running git command", exc_info=True)
        return ""
    return proc.stdout.decode(encoding="utf-8").strip()


def get_git_current_branch() -> str:
    try:
        proc = subprocess.run(args=["git", "rev-parse", "--abbrev-ref=loose", "HEAD"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        LOGGER.warning("Error running git command", exc_info=True)
        return "#ERROR"
    return proc.stdout.decode(encoding="utf-8").strip()


def clone_repo(remoter, repo_url: str, destination_dir_name: str = ""):
    # pylint: disable=broad-except
    try:
        LOGGER.debug("Cloning from %s...", repo_url)
        rm_cmd = f"rm -rf ./{repo_url.split('/')[-1].split('.')[0]}"
        remoter.sudo(rm_cmd, ignore_status=False)
        clone_cmd = f"git clone {repo_url} {destination_dir_name}"
        remoter.sudo(clone_cmd)
        LOGGER.debug("Finished cloning from %s.", repo_url)
    except Exception as exc:
        LOGGER.warning("Failed to clone from %s. Failed with: %s", repo_url, exc)
