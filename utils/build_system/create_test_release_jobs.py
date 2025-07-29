# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2021 ScyllaDB

import logging
from pathlib import Path

import jenkins

from sdcm.wait import wait_for


DIR_TEMPLATE = Path(__file__).parent.joinpath("folder-template.xml").read_text(encoding="utf-8")
JOB_TEMPLATE = Path(__file__).parent.joinpath("template.xml").read_text(encoding="utf-8")
LOGGER = logging.getLogger(__name__)


class JenkinsPipelines:
    def __init__(self, username, password, base_job_dir, sct_branch_name, sct_repo):  # pylint: disable=too-many-arguments
        self.jenkins = jenkins.Jenkins("https://jenkins.scylladb.com", username=username, password=password)
        self.base_sct_dir = Path(__file__).parent.parent.parent
        self.base_job_dir = base_job_dir
        self.sct_branch_name = sct_branch_name
        self.sct_repo = sct_repo

    def create_directory(self, name, display_name):
        try:
            dir_xml_data = DIR_TEMPLATE % dict(sct_display_name=display_name)
            self.jenkins.create_job(f"{self.base_job_dir}/{name}", dir_xml_data)
        except jenkins.JenkinsException as ex:
            self._log_jenkins_exception(ex)

    def create_pipeline_job(self, jenkins_file, group_name, job_name=None, job_name_suffix="-test"):
        base_name = job_name or Path(jenkins_file).stem
        sct_jenkinsfile = jenkins_file.split("scylla-cluster-tests/")[-1]
        LOGGER.info("%s is used to create job", sct_jenkinsfile)
        xml_data = JOB_TEMPLATE % dict(
            sct_display_name=f"{base_name}{job_name_suffix}",
            sct_description=sct_jenkinsfile,
            sct_repo=self.sct_repo,
            sct_branch_name=self.sct_branch_name,
            sct_jenkinsfile=sct_jenkinsfile,
        )
        try:
            if group_name:
                group_name = "/" + group_name
            self.jenkins.create_job(f"{self.base_job_dir}{group_name}/{base_name}{job_name_suffix}", xml_data)
            self.build_job_and_wait_completion(f"{self.base_job_dir}{group_name}/{base_name}{job_name_suffix}")
        except jenkins.JenkinsException as ex:
            self._log_jenkins_exception(ex)

    def build_job_and_wait_completion(self, name):
        """start job first time

        Need to start job first time
        so jenkins will read all job parameters
        Job started one by one, to avoid situation
        when all jenkins resources will be allocated
        """
        LOGGER.info("Start first build %s", name)
        job_id = self.jenkins.build_job(name)

        # wait while worker will be found
        def check_job_is_started(job_id):
            return self.jenkins.get_queue_item(job_id).get("executable")

        wait_for(check_job_is_started, step=5, text="Job is starting", timeout=120, throw_exc=True, job_id=job_id)

        LOGGER.info("First build finished")

    @staticmethod
    def _log_jenkins_exception(exc):
        if "already exists" in str(exc):
            LOGGER.info(exc)
        else:
            LOGGER.error(exc)
