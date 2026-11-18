"""Low-level Jenkins API client for staging trigger operations."""

import fnmatch
import functools
import json
import logging
import os
import subprocess
import xml.etree.ElementTree as ET
from collections.abc import Iterator

import click
import jenkins

from sdcm.utils.get_username import get_username
from utils.staging_trigger.constants import ParamDefinition, _get_jenkins_url

logger = logging.getLogger(__name__)

KEYRING_SERVICE = "jenkins.scylladb.com"
ONEPASSWORD_ITEM = "Jenkins API token"


def _run_secret_command(cmd: list[str]) -> tuple[str, str]:
    """Run a secret-store CLI and return (stdout, stderr), or empty strings when the CLI is missing or fails.

    A missing CLI is expected (not everyone has both stores) and stays quiet. A CLI that fails says why on
    stderr, so a locked keyring or a locked 1Password app is not mistaken for a missing entry.
    """
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60)
        return result.stdout, result.stderr
    except FileNotFoundError:
        logger.debug("%s is not installed", cmd[0])
    except subprocess.CalledProcessError as exc:
        reason = (exc.stderr or "").strip().splitlines()
        click.echo(f"Jenkins: {cmd[0]} failed: {reason[-1] if reason else f'exit code {exc.returncode}'}", err=True)
    except (OSError, subprocess.SubprocessError) as exc:
        click.echo(f"Jenkins: {cmd[0]} failed: {exc}", err=True)
    return "", ""


def _credentials_from_keyring() -> tuple[str, str]:
    """Read (user, token) from the OS keyring item with attribute service=jenkins.scylladb.com.

    `secret-tool search` prints the secret on stdout and the item's attributes on stderr.
    """
    stdout, stderr = _run_secret_command(["secret-tool", "search", "service", KEYRING_SERVICE])
    fields = {}
    for line in (stdout + stderr).splitlines():
        key, sep, value = line.partition(" = ")
        if sep:
            fields[key] = value
    return fields.get("attribute.user", ""), fields.get("secret", "")


def _credentials_from_1password() -> tuple[str, str]:
    """Read (username, credential) from the 1Password item titled "Jenkins API token"."""
    output, _ = _run_secret_command(
        [
            "op",
            "item",
            "get",
            ONEPASSWORD_ITEM,
            "--fields",
            "label=username,label=credential",
            "--reveal",
            "--format",
            "json",
        ]
    )
    if not output:
        return "", ""
    try:
        fields = json.loads(output)
        # `op` prints a list for several fields, but a single object when only one of them exists
        if isinstance(fields, dict):
            fields = [fields]
        values = {field.get("label"): field.get("value") or "" for field in fields}
    except (ValueError, AttributeError) as exc:
        logger.warning("Ignoring unexpected 1Password output for %r: %s", ONEPASSWORD_ITEM, exc)
        return "", ""
    return values.get("username", ""), values.get("credential", "")


def _credentials_from_env() -> tuple[str, str]:
    return os.environ.get("JENKINS_USERNAME", ""), os.environ.get("JENKINS_API_TOKEN", "")


# JENKINS_CREDENTIALS_SOURCE value -> (name shown to the user, lookup), in the default order
CREDENTIAL_SOURCES = {
    "env": ("environment", _credentials_from_env),
    "1password": ("1Password", _credentials_from_1password),
    "keyring": ("OS keyring", _credentials_from_keyring),
}


def iter_jenkins_credentials() -> Iterator[tuple[str, str, str]]:
    """Yield the caller's own (Jenkins user id, API token, source), best first.

    Order: JENKINS_USERNAME + JENKINS_API_TOKEN, 1Password, then the OS keyring. JENKINS_USERNAME alone
    overrides the user id stored next to a token. A store is only queried when the ones before it were
    not enough, so the keyring is not asked when 1Password works.

    JENKINS_CREDENTIALS_SOURCE (env, keyring or 1password) limits the lookup to that one source.
    """
    env_user = os.environ.get("JENKINS_USERNAME", "")
    sources = CREDENTIAL_SOURCES
    if wanted := os.environ.get("JENKINS_CREDENTIALS_SOURCE", "").lower():
        if wanted not in CREDENTIAL_SOURCES:
            raise click.ClickException(
                f"JENKINS_CREDENTIALS_SOURCE={wanted!r} is not one of: {', '.join(CREDENTIAL_SOURCES)}"
            )
        sources = {wanted: CREDENTIAL_SOURCES[wanted]}
    for source, lookup in sources.values():
        user, token = lookup()
        if token and (env_user or user):
            yield env_user or user, token, source


@functools.cache
def _login(url: str) -> jenkins.Jenkins:
    """Return a Jenkins client logged in with the first working credentials of the caller.

    Cached per process: a command builds a client for each job, and the secret stores, the `whoAmI` check and
    the messages below must happen once, not once per job.
    """
    # Try each source in turn: a stale or misnamed entry in one store must not hide a good one in the next.
    # Messages go to stderr, so a rejected login can be traced to the store that supplied it.
    for username, token, source in iter_jenkins_credentials():
        client = jenkins.Jenkins(url, username=username, password=token)
        try:
            client.get_whoami()
        except jenkins.JenkinsException as exc:
            click.echo(
                f"Jenkins: {username} from {source} was rejected, trying the next source ({exc!s:.60})", err=True
            )
            continue
        click.echo(f"Jenkins: calling {url} as {username}, token from {source}", err=True)
        return client
    raise click.ClickException(
        "No working Jenkins credentials of your own found. Store your Jenkins user id and API token in the OS "
        f"keyring or 1Password, or set JENKINS_USERNAME and JENKINS_API_TOKEN (create a token at "
        f"{url}/me/security). See docs/contrib.md, 'Jenkins credentials'."
    )


class JenkinsJobTrigger:
    """Core Jenkins job interaction: find, configure, and trigger jobs."""

    def __init__(self):
        self.url = _get_jenkins_url()
        self.jenkins = _login(self.url)

    def get_job_info(self, job_name: str) -> dict:
        return self.jenkins.get_job_info(job_name)

    def list_jobs_in_folder(self, folder: str) -> list[str]:
        """List all job names inside a Jenkins folder."""
        try:
            info = self.jenkins.get_job_info(folder)
        except jenkins.NotFoundException:
            return []
        return [job["name"] for job in info.get("jobs", [])]

    def find_jobs(self, folder: str, pattern: str) -> list[str]:
        """Find jobs in a folder matching a glob pattern."""
        all_jobs = self.list_jobs_in_folder(folder)
        return sorted(j for j in all_jobs if fnmatch.fnmatch(j, pattern))

    def update_scm(self, job_name: str, git_repo: str | None = None, git_branch: str | None = None) -> None:
        """Update the SCM (git repo/branch) of a Jenkins job."""
        config_xml = self.jenkins.get_job_config(job_name)
        et = ET.ElementTree(ET.fromstring(config_xml))

        if git_repo:
            elements = et.findall(".//scm/userRemoteConfigs/*/url")
            if elements:
                elements[0].text = git_repo
        if git_branch:
            elements = et.findall(".//scm/branches/*/name")
            if elements:
                elements[0].text = git_branch

        if git_branch or git_repo:
            self.jenkins.reconfig_job(job_name, config_xml=ET.tostring(et.getroot()).decode())

    def get_last_build_params(self, job_name: str) -> dict[str, str]:
        """Extract parameters from the last build of a job."""
        job_info = self.jenkins.get_job_info(job_name)
        last_build = job_info.get("lastBuild")
        if not last_build:
            return {}
        build_info = self.jenkins.get_build_info(job_name, number=last_build["number"])
        for action in build_info.get("actions", []):
            if "ParametersAction" in action.get("_class", ""):
                return {p["name"]: p.get("value", "") for p in action["parameters"]}
        return {}

    def trigger(self, job_name: str, parameters: dict) -> int:
        """Trigger a job and return the expected build number.

        Fills an empty or missing `requested_by_user` with the caller's username: the build tags
        its cloud resources with this user.

        Parameters the job does not define are dropped: Jenkins rejects the
        whole build with 400 Bad Request when it gets one.
        """
        if not parameters.get("requested_by_user"):
            parameters = {**parameters, "requested_by_user": get_username()}
        if defined := self.get_job_parameter_definitions(job_name):
            if unknown := sorted(set(parameters) - set(defined)):
                logger.warning("Dropping parameters that %s does not define: %s", job_name, ", ".join(unknown))
            parameters = {k: v for k, v in parameters.items() if k in defined}
        job_info = self.jenkins.get_job_info(job_name)
        build_number = job_info["nextBuildNumber"]
        self.jenkins.build_job(name=job_name, parameters=parameters)
        return build_number

    def get_job_url(self, job_name: str) -> str:
        return self.jenkins.get_job_info(job_name)["url"]

    def get_job_parameter_definitions(self, job_name: str) -> dict[str, ParamDefinition]:
        """Extract parameter definitions (name -> ParamDefinition) from a job's config XML."""
        try:
            config_xml = self.jenkins.get_job_config(job_name)
        except Exception:  # noqa: BLE001
            return {}
        return parse_parameter_definitions(config_xml)


def parse_parameter_definitions(config_xml: str) -> dict[str, ParamDefinition]:
    """Parse Jenkins job config XML and extract parameter definitions.

    Handles ChoiceParameterDefinition by extracting the list of choices from
    the ``<choices><a><string>...</string></a></choices>`` structure.
    """
    et = ET.ElementTree(ET.fromstring(config_xml))
    params: dict[str, ParamDefinition] = {}
    for param_def in et.findall(".//parameterDefinitions/*"):
        # Skip separator elements — they are visual section headers, not real parameters
        if "SeparatorDefinition" in param_def.tag:
            continue
        name_el = param_def.find("name")
        if name_el is None or not name_el.text:
            continue
        default_el = param_def.find("defaultValue")
        default_value = (default_el.text or "") if default_el is not None else ""

        choices = None
        tag = param_def.tag
        if "ChoiceParameterDefinition" in tag:
            choices_el = param_def.find("choices")
            if choices_el is not None:
                # Jenkins XML: <choices><a class="..."><string>val</string>...</a></choices>
                a_el = choices_el.find("a")
                container = a_el if a_el is not None else choices_el
                choices = [s.text or "" for s in container.findall("string")]
            if choices and not default_value:
                default_value = choices[0]

        params[name_el.text] = ParamDefinition(default=default_value, choices=choices)
    return params
