"""Low-level Jenkins API client for staging trigger operations."""

import fnmatch
import xml.etree.ElementTree as ET

import jenkins

from sdcm.keystore import KeyStore


class JenkinsJobTrigger:
    """Core Jenkins job interaction: find, configure, and trigger jobs."""

    def __init__(self):
        creds = KeyStore().get_json("jenkins.json")
        self.url = creds["url"].rstrip("/")
        self.jenkins = jenkins.Jenkins(**creds)

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
        """Trigger a job and return the expected build number."""
        job_info = self.jenkins.get_job_info(job_name)
        build_number = job_info["nextBuildNumber"]
        self.jenkins.build_job(name=job_name, parameters=parameters)
        return build_number

    def get_job_url(self, job_name: str) -> str:
        return self.jenkins.get_job_info(job_name)["url"]

    def get_job_parameter_definitions(self, job_name: str) -> dict[str, str]:
        """Extract parameter definitions (name -> default) from a job's config XML."""
        try:
            config_xml = self.jenkins.get_job_config(job_name)
        except Exception:  # noqa: BLE001
            return {}
        et = ET.ElementTree(ET.fromstring(config_xml))
        params = {}
        for param_def in et.findall(".//parameterDefinitions/*"):
            name_el = param_def.find("name")
            default_el = param_def.find("defaultValue")
            if name_el is not None and name_el.text:
                params[name_el.text] = (default_el.text or "") if default_el is not None else ""
        return params
