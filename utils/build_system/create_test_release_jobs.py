from pathlib import Path

import jenkins  # pylint: disable=import-error

from sdcm.wait import wait_for

DIR_TEMPLATE = open(Path(__file__).parent / 'folder-template.xml').read()
JOB_TEMPLATE = open(Path(__file__).parent / 'template.xml').read()


class JenkinsPipelines:
    def __init__(self, username, password, base_job_dir, sct_branch_name, sct_repo):  # pylint: disable=too-many-arguments
        self.jenkins = jenkins.Jenkins('https://jenkins.scylladb.com', username=username, password=password)
        self.base_sct_dir = Path(__file__).parent.parent.parent
        self.base_job_dir = base_job_dir
        self.sct_branch_name = sct_branch_name
        self.sct_repo = sct_repo

    def create_directory(self, name, display_name):
        try:
            dir_xml_data = DIR_TEMPLATE % dict(sct_display_name=display_name)
            self.jenkins.create_job(f'{self.base_job_dir}/{name}', dir_xml_data)
        except jenkins.JenkinsException as ex:
            print(ex)

    def create_pipeline_job(self, jenkins_file, group_name, job_name=None, job_name_suffix="-test"):
        base_name = job_name or Path(jenkins_file).stem
        sct_jenkinsfile = jenkins_file.split("scylla-cluster-tests/")[-1]
        print(sct_jenkinsfile)
        xml_data = JOB_TEMPLATE % dict(sct_display_name=f"{base_name}{job_name_suffix}",
                                       sct_description=sct_jenkinsfile,
                                       sct_repo=self.sct_repo,
                                       sct_branch_name=self.sct_branch_name,
                                       sct_jenkinsfile=sct_jenkinsfile)
        try:
            if group_name:
                group_name = "/" + group_name
            self.jenkins.create_job(
                f'{self.base_job_dir}{group_name}/{base_name}{job_name_suffix}', xml_data)
            self.build_job_first_time(f'{self.base_job_dir}{group_name}/{base_name}{job_name_suffix}')
        except jenkins.JenkinsException as ex:
            print(ex)

    def build_job_first_time(self, name):
        """start job first time

        Need to start job first time
        so jenkins will read all job parameters
        Job started one by one, to avoid situation
        when all jenkins resources will be allocated
        """
        print(f"Start first build {name}")
        job_id = self.jenkins.build_job(name)

        # wait while worker will be found
        def check_job_is_started(job_id):
            return self.jenkins.get_queue_item(job_id).get("executable")
        wait_for(check_job_is_started, step=5, text="Job is starting", timeout=60, throw_exc=True, job_id=job_id)

        # wait while job will be executed
        def check_job_is_finished(job_name):
            return not self.jenkins.get_build_info(name, 1).get("building")

        wait_for(check_job_is_finished, step=5, text="Check job is finished",
                 timeout=120, throw_exc=True, job_name=name)

        print("First build finished")
