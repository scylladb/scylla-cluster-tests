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
