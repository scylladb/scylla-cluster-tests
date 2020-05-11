from pathlib import Path
import jenkins  # pylint: disable=import-error

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

    def create_pipeline_job(self, jenkins_file, group_name):
        base_name = Path(jenkins_file).stem
        sct_jenkinsfile = 'jenkins-pipelines/{}'.format(Path(jenkins_file).name)
        print(sct_jenkinsfile)
        xml_data = JOB_TEMPLATE % dict(sct_display_name=f"{base_name}-test",
                                       sct_description=sct_jenkinsfile,
                                       sct_repo=self.sct_repo,
                                       sct_branch_name=self.sct_branch_name,
                                       sct_jenkinsfile=sct_jenkinsfile)
        try:
            self.jenkins.create_job(f'{self.base_job_dir}/{group_name}/{base_name}', xml_data)
        except jenkins.JenkinsException as ex:
            print(ex)
