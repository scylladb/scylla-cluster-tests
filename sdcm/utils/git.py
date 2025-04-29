"""
Simple git wrappers that provide useful information about current repository
"""
import subprocess
import logging
from typing import TypedDict

LOGGER = logging.getLogger(__name__)


GitStatus = TypedDict('GitStatus', {'branch.oid': str, 'branch.head': str,
                      'branch.upstream': str | None, 'upstream.url': str | None})


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


def get_git_status_info() -> GitStatus:

    try:
        #    Example output:
        #    # branch.oid 0748e3a512b7e698bc4b2e3e8ec8d46d0b3244ae
        #    # branch.head add-argus-branch-tracking
        #    # branch.upstream origin/add-argus-branch-tracking
        #    # branch.ab +0 -0
        status_proc = subprocess.run(args=["git", "status", "-b", "--porcelain=v2",
                                     ".nothing"], check=True, capture_output=True)
        output = status_proc.stdout.decode(encoding="utf-8").strip()
        git_status: GitStatus = {}
        for line in (line for line in output.split("\n") if line.startswith("#")):
            key, value = line[2:].split(" ", maxsplit=1)
            git_status[key] = value

        if "(detached)" in git_status["branch.head"]:
            # Detached HEAD, we need to find the actual heads and remotes
            # We will potentially lose accuracy here as there can be more than one ref
            # pointing to an object id.

            # Example output:
            # e21d30bee0cfcfe546b457671b839b87f0a6da0e refs/remotes/origin/branch-2020.1
            # fafa2877f879ba840cc199001ad23807039c8fc6 refs/remotes/origin/branch-2021.1
            # a7bd7066f25c45365d81cec7fd2bce39e318c7ee refs/remotes/origin/branch-2022.1
            # bd87ca2b6462ba2e78fba2cc45495c0d1cdaf286 refs/remotes/origin/branch-4.2
            proc = subprocess.run(args=["git", "show-ref"], check=True, capture_output=True)
            output = proc.stdout.decode(encoding="utf-8").strip()
            target_oid = git_status["branch.oid"]
            head_found = False
            remote_found = False
            for _, ref in (line.split(" ", maxsplit=1) for line in output.splitlines() if target_oid in line):
                _, ref_type, ref_name = ref.split("/", maxsplit=2)
                match ref_type:
                    case "heads":
                        if head_found:
                            continue
                        git_status["branch.head"] = ref_name
                        head_found = True
                    case "remotes":
                        if remote_found:
                            continue
                        git_status["branch.upstream"] = ref_name
                        remote_found = True

        remote, _ = git_status["branch.upstream"].split("/", maxsplit=1)
        #    Example output:
        #    gh:k0machi/scylla-cluster-tests
        origin_proc = subprocess.run(
            args=["git", "config", "--get", f"remote.{remote}.url"], check=True, capture_output=True)
        output = origin_proc.stdout.decode(encoding="utf-8").strip()
        git_status["upstream.url"] = output.strip()

    except (subprocess.CalledProcessError, KeyError):
        LOGGER.warning("Error parsing git information", exc_info=True)
        return {}
    return git_status


def clone_repo(remoter, repo_url: str, destination_dir_name: str = "", clone_as_root=True, branch=None):

    try:
        LOGGER.debug("Cloning from %s...", repo_url)
        rm_cmd = f"rm -rf ./{repo_url.split('/')[-1].split('.')[0]}"
        remoter.sudo(rm_cmd, ignore_status=False)
        branch = f'--branch {branch}' if branch else ''
        clone_cmd = f"git clone {branch} {repo_url} {destination_dir_name}"
        if clone_as_root:
            remoter.sudo(clone_cmd)
        else:
            remoter.run(clone_cmd)

        LOGGER.debug("Finished cloning from %s.", repo_url)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to clone from %s. Failed with: %s", repo_url, exc)
