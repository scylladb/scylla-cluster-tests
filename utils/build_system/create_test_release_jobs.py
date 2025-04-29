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

import os
import logging
from pathlib import Path
from xml.etree import ElementTree as etree
from copy import deepcopy

import jenkins

from sdcm.wait import wait_for
from sdcm.utils.common import get_sct_root_path
from sdcm.keystore import KeyStore

DIR_TEMPLATE = Path(__file__).parent.joinpath("folder-template.xml").read_text(encoding="utf-8")
JOB_TEMPLATE = Path(__file__).parent.joinpath("template.xml").read_text(encoding="utf-8")
LOGGER = logging.getLogger(__name__)


def tree_merge(element_a: etree.Element, element_b: etree.Element):
    """
    merge two xml trees A and B, so that each recursively found leaf element of B is added to A.
    If the element already exists in A, it is replaced with B's version.
    Tree structure is created in A as required to reflect the position of the leaf element in B.

    Given <top><first><a/><b/></first></top> and  <top><first><c/></first></top>, a merge results in
    <top><first><a/><b/><c/></first></top> (order not guaranteed)
    """

    def inner(a_parent: etree.Element, b_parent: etree.Element):
        for b_child in b_parent:
            a_child = a_parent.findall('./' + b_child.tag)
            if not a_child:
                a_parent.append(b_child)
            elif b_child.items():
                inner(a_child[0], b_child)

    res = deepcopy(element_a)
    inner(res, element_b)
    return res


class JenkinsPipelines:
    def __init__(self, base_job_dir, sct_branch_name, sct_repo, username=None, password=None):
        if not username and not password:
            creds = KeyStore().get_json("jenkins.json")
            username, password = creds.get('username'), creds.get('password')

        self.jenkins = jenkins.Jenkins('https://jenkins.scylladb.com', username=username, password=password)
        self.base_sct_dir = Path(__file__).parent.parent.parent
        self.base_job_dir = base_job_dir
        self.sct_branch_name = sct_branch_name
        self.sct_repo = sct_repo

    def reconfig_job(self, new_path, dir_xml_data):
        current_config = self.jenkins.get_job_config(new_path)
        dir_xml_data = etree.tostring(tree_merge(etree.fromstring(current_config),
                                      etree.fromstring(dir_xml_data))).decode()
        self.jenkins.reconfig_job(new_path, dir_xml_data)

    def create_directory(self, name: Path | str, display_name: str):
        try:
            dir_xml_data = DIR_TEMPLATE % dict(sct_display_name=display_name)
            new_path = str(Path(self.base_job_dir) / name)

            if self.jenkins.job_exists(new_path):
                LOGGER.info("reconfig folder [%s]", new_path)
                self.reconfig_job(new_path, dir_xml_data)
            else:
                LOGGER.info("creating folder [%s]", new_path)
                self.jenkins.create_job(new_path, dir_xml_data)
        except jenkins.JenkinsException as ex:
            self._log_jenkins_exception(ex)

    def create_freestyle_job(self, xml_temple: Path | str, group_name: Path | str, job_name: str = None, template_context: dict = None):
        xml_temple = Path(xml_temple)
        base_name = job_name or xml_temple.stem

        context = template_context if template_context else {}
        context = {'sct_branch': self.sct_branch_name,
                   'sct_repo': self.sct_repo,
                   **context}
        base_name = base_name % context

        if group_name:
            group_name = "/" + str(group_name)

        xml_data = xml_temple.read_text(encoding='utf-8') % context
        job_name = f'{self.base_job_dir}{group_name}/{base_name}'
        try:
            if self.jenkins.job_exists(job_name):
                LOGGER.info("%s is used to reconfig job", job_name)
                self.reconfig_job(job_name, xml_data)
            else:
                LOGGER.info("%s is used to create job", job_name)
                self.jenkins.create_job(job_name, xml_data)
        except jenkins.JenkinsException as ex:
            self._log_jenkins_exception(ex)

    def create_pipeline_job(self, jenkins_file: Path | str, group_name: Path | str, job_name: str = None, job_name_suffix="-test"):
        jenkins_file = Path(jenkins_file)
        base_name = job_name or jenkins_file.stem
        sct_jenkinsfile = jenkins_file.relative_to(get_sct_root_path())
        xml_data = JOB_TEMPLATE % dict(sct_display_name=f"{base_name}{job_name_suffix}",
                                       sct_description=sct_jenkinsfile,
                                       sct_repo=self.sct_repo,
                                       sct_branch_name=self.sct_branch_name,
                                       sct_jenkinsfile=sct_jenkinsfile)
        if group_name:
            group_name = "/" + str(group_name)
        _job_name = f'{self.base_job_dir}{group_name}/{base_name}{job_name_suffix}'
        try:
            if self.jenkins.job_exists(_job_name):
                LOGGER.info("%s is used to reconfig job", _job_name)

                self.reconfig_job(_job_name, xml_data)
            else:
                LOGGER.info("%s is used to create job", _job_name)
                self.jenkins.create_job(_job_name, xml_data)
                self.build_job_and_wait_completion(_job_name)
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

    def create_job_tree(self, local_path: str | Path,
                        create_freestyle_jobs: bool = True,
                        create_pipelines_jobs: bool = True,
                        template_context: dict | None = None,
                        job_name_suffix: str = '-test'):
        for root, _, job_files in os.walk(local_path):
            jenkins_path = Path(root).relative_to(local_path)

            # get display names, if available
            display_name = jenkins_path.name
            if '_display_name' in job_files:
                display_name = (Path(root) / '_display_name').read_text().strip()

            if str(jenkins_path) == '.':
                jenkins_path = ''
                display_name = self.base_job_dir.split('/')[-1]

            if jenkins_path and display_name:
                self.create_directory(jenkins_path, display_name=display_name)

            for job_file in job_files:
                job_file = Path(root) / job_file  # noqa: PLW2901
                if (job_file.suffix == '.jenkinsfile') and create_pipelines_jobs:
                    self.create_pipeline_job(job_file, group_name=jenkins_path, job_name_suffix=job_name_suffix)
                if (job_file.suffix == '.xml') and create_freestyle_jobs:
                    self.create_freestyle_job(job_file, group_name=jenkins_path, template_context=template_context)
