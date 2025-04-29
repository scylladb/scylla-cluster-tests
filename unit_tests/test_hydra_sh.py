import contextlib
import dataclasses
import os
import re
import tempfile
import unittest
from functools import cached_property
from typing import Dict, Union, Tuple, Iterable, Sequence, List

from parameterized import parameterized

from sdcm import sct_abs_path
from sdcm.remote import LocalCmdRunner


class HydraTestCaseTmpDir:
    def __init__(self, home_dir: str, aws_creds: bool):
        self.home_dir = home_dir
        self.aws_creds = aws_creds

    @staticmethod
    def _touch_file(file_path: str):
        if not os.path.exists(file_path):
            with open(file_path, 'w', encoding="utf-8") as token_file:
                token_file.write(' ')
                token_file.flush()

    @cached_property
    def aws_token_dir_path(self):
        return os.path.join(self.home_dir, '.aws')

    @cached_property
    def aws_token_path(self):
        return os.path.join(self.aws_token_dir_path, 'credentials')

    def setup(self):
        if not os.path.exists(self.home_dir):
            os.makedirs(self.home_dir)
        if self.aws_creds:
            if not os.path.exists(self.aws_token_dir_path):
                os.makedirs(self.aws_token_dir_path)
            self._touch_file(self.aws_token_path)

    def teardown(self):
        with contextlib.suppress(Exception):
            os.unlink(self.home_dir)

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()


@dataclasses.dataclass
class HydraTestCaseParams:
    name: str
    cmd: str
    expected: List[Union[str, re.Pattern]]
    not_expected: List[Union[str, re.Pattern]]
    return_code: int
    env: Dict[str, str]

    def __str__(self):
        return self.name

    __repr__ = __str__

    @property
    def as_tuple(self):
        return self.cmd, self.expected, self.not_expected, self.return_code, self.env

    def __enter__(self):
        pass


class LongevityPipelineTest:
    """
    This class takes pipeline parameters and produces hydra test cases parameters as a tuple in hydra_test_cases
    """
    sct_base_path = sct_abs_path("").rstrip('/')
    test_id = '11111111-1111-1111-1111-111111111111'
    runner_arg = '--execute-on-runner 1.1.1.1 '

    def __init__(self, backend: str, runner: bool, aws_creds: bool):
        self.home_dir = tempfile.mkdtemp()
        self.backend = backend
        self.runner = runner
        self.aws_creds = aws_creds
        self.home_dir_postfix = ''

    def set_test_home_dir_postfix(self, postfix: str):
        self.home_dir_postfix = postfix

    @staticmethod
    def docker_run_prefix(runner: bool):
        if runner:
            return "docker -H ssh://ubuntu@1.1.1.1 run --rm -it --privileged -h ip-1.1.1.1.*"
        return "docker run --rm -it --privileged .*"

    def sct_path(self, runner: bool):
        if runner:
            return '/home/ubuntu/scylla-cluster-tests'
        return self.sct_base_path

    def expected(self, runner):
        docker_run_prefix = self.docker_run_prefix(runner)
        sct_dir = self.sct_path(runner)
        expected = (
            re.compile(f"{docker_run_prefix} -l TestId=11111111-1111-1111-1111-111111111111"),
            re.compile(f"{docker_run_prefix} -v {sct_dir}:{sct_dir} .* -v /var/run:/run"),
            re.compile(f"{docker_run_prefix} --group-add 1 --group-add 2 --group-add 3"),
        )
        if not runner:
            return expected

        expected += (
            self.pattern_remove_known_key,
            self.pattern_rsync_aws_token,
            self.pattern_rsync_sct_dir,
        )
        return expected

    def not_expected(self, runner: bool):
        # Hydra arguments should not leak
        not_expected = ('--dry-run-hydra', '--execute-on-runner')
        if not runner:
            # No sync if no runner is used
            return not_expected + (
                self.pattern_remove_known_key,
                'rsync -ar -e ssh -o StrictHostKeyChecking=no --delete ',
            )

        return not_expected

    @cached_property
    def pattern_remove_known_key(self):
        return 'ssh-keygen -R "1.1.1.1" || true'

    @cached_property
    def pattern_rsync_aws_token(self):
        return "rsync -ar -e 'ssh -o StrictHostKeyChecking=no' --delete ~/.aws ubuntu@1.1.1.1:/home/ubuntu/"

    @cached_property
    def pattern_rsync_sct_dir(self):
        return f"rsync -ar -e 'ssh -o StrictHostKeyChecking=no' --delete " \
               f"{self.sct_base_path} ubuntu@1.1.1.1:/home/ubuntu/"

    @cached_property
    def step_name_prefix(self):
        return f'{self.backend}_{self.runner}_{self.aws_creds}'

    @cached_property
    def show_conf_cmd(self):
        return f'output-conf -b {self.backend}'

    @cached_property
    def create_runner_cmd(self):
        return f'create-runner-instance --cloud-provider {self.backend} --region eu-north-1 --availability-zone a ' \
               f'--test-id {self.test_id} --duration 465'

    @cached_property
    def run_test_cmd(self):
        # Command line of the hydra it self
        if self.runner:
            return f'{self.runner_arg}{self.run_test_cmd_docker}'
        return self.run_test_cmd_docker

    @cached_property
    def run_test_cmd_docker(self):
        # Command line that should be run in the docker
        return f'run-test longevity_test.LongevityTest.test_custom_time --backend {self.backend}'

    @cached_property
    def collect_logs_cmd(self):
        # Command line of the hydra it self
        if self.runner:
            return f'{self.runner_arg}{self.collect_logs_cmd_docker}'
        return self.collect_logs_cmd_docker

    @cached_property
    def collect_logs_cmd_docker(self):
        # Command line that should be run in the docker
        return 'collect-logs'

    @cached_property
    def clean_resources_cmd(self):
        # Command line of the hydra it self
        if self.runner:
            return f'{self.runner_arg}{self.clean_resources_cmd_docker}'
        return self.clean_resources_cmd_docker

    @cached_property
    def clean_resources_cmd_docker(self):
        # Command line that should be run in the docker
        return f'clean-resources --post-behavior --test-id {self.test_id}'

    @cached_property
    def send_email_cmd(self):
        # Command line of the hydra it self
        if self.runner:
            return f'{self.runner_arg}{self.send_email_cmd_docker}'
        return self.send_email_cmd_docker

    @cached_property
    def send_email_cmd_docker(self):
        # Command line that should be run in the docker
        return 'send-email --test-status SUCCESS --start-time 1627268929 --email-recipients qa@scylladb.com'

    @property
    def test_home_dir(self) -> str:
        return os.path.join(self.home_dir, self.home_dir_postfix)

    @cached_property
    def is_gce_or_gke(self) -> bool:
        return 'gce' in self.backend or 'gke' in self.backend

    @property
    def get_longevity_env(self) -> Dict[str, str]:
        longevity_end = {
            'SCT_TEST_ID': self.test_id,
            'HOME': self.test_home_dir,
            'USER': 'root',
            'SCT_CLUSTER_BACKEND': self.backend,
            'SCT_CONFIG_FILES': '["/jenkins/slave/workspace/siren-tests/longevity-tests/cloud-longevity-small-data-'
                                'set-1h-gcp/siren-tests/sct_plugin/configurations/scylla_cloud_nemesis_small_set.yaml"]'
        }
        return longevity_end

    @property
    def before_runner_not_expected(self):
        # All steps before runner is created should not have runner related steps
        return self.not_expected(runner=False)

    @property
    def before_runner_expected(self):
        # All steps before runner is created should not have runner related steps
        return self.expected(runner=False)

    @property
    def before_runner_docker_run_prefix(self):
        # All steps before runner is created do not have runner parameter
        return self.docker_run_prefix(runner=False)

    @property
    def after_runner_not_expected(self):
        # All steps before runner is created should not have runner related steps
        return self.not_expected(runner=self.runner)

    @property
    def after_runner_expected(self):
        # All steps before runner is created should not have runner related steps
        return self.expected(runner=self.runner)

    @property
    def after_runner_docker_run_prefix(self):
        # All steps before runner is created do not have runner parameter
        return self.docker_run_prefix(runner=self.runner)

    @property
    def test_tmp_dir(self) -> HydraTestCaseTmpDir:
        return HydraTestCaseTmpDir(
            home_dir=self.test_home_dir,
            aws_creds=self.aws_creds)

    @property
    def test_case_show_conf(self):
        self.set_test_home_dir_postfix('show_conf')
        return HydraTestCaseParams(
            name=f'{self.step_name_prefix}_show_conf',
            cmd=self.show_conf_cmd,
            expected=[
                *self.before_runner_expected,
                re.compile(f"{self.before_runner_docker_run_prefix} eval './sct.py  {self.show_conf_cmd}'")
            ],
            not_expected=[*self.before_runner_not_expected],
            return_code=0,
            env=self.get_longevity_env,
        ), self.test_tmp_dir

    @property
    def test_case_create_runner(self):
        self.set_test_home_dir_postfix('create_runner')
        return HydraTestCaseParams(
            name=f'{self.step_name_prefix}_create_runner',
            cmd=self.create_runner_cmd,
            expected=[
                *self.before_runner_expected,
                re.compile(f"{self.before_runner_docker_run_prefix} eval './sct.py  {self.create_runner_cmd}'")
            ],
            not_expected=[*self.before_runner_not_expected],
            return_code=0,
            env=self.get_longevity_env
        ), self.test_tmp_dir

    @property
    def test_case_run_test(self):
        self.set_test_home_dir_postfix('run_test')
        return HydraTestCaseParams(
            name=f'{self.step_name_prefix}_run_test',
            cmd=self.run_test_cmd,
            expected=[
                *self.after_runner_expected,
                re.compile(f"{self.after_runner_docker_run_prefix} eval './sct.py  {self.run_test_cmd_docker}'")],
            not_expected=[*self.after_runner_not_expected],
            return_code=0,
            env=self.get_longevity_env
        ), self.test_tmp_dir

    @property
    def test_case_collect_logs(self):
        self.set_test_home_dir_postfix('collect_logs')
        return HydraTestCaseParams(
            name=f'{self.step_name_prefix}_collect_logs',
            cmd=self.collect_logs_cmd,
            expected=[
                *self.after_runner_expected,
                re.compile(f"{self.after_runner_docker_run_prefix} eval './sct.py  {self.collect_logs_cmd_docker}'")],
            not_expected=[*self.after_runner_not_expected],
            return_code=0,
            env=self.get_longevity_env
        ), self.test_tmp_dir

    @property
    def test_case_clean_resources(self):
        self.set_test_home_dir_postfix('clean_resources')
        return HydraTestCaseParams(
            name=f'{self.step_name_prefix}_clean_resources',
            cmd=self.clean_resources_cmd,
            expected=[
                *self.after_runner_expected,
                re.compile(f"{self.after_runner_docker_run_prefix} "
                           f"eval './sct.py  {self.clean_resources_cmd_docker}'")],
            not_expected=[*self.after_runner_not_expected],
            return_code=0,
            env=self.get_longevity_env
        ), self.test_tmp_dir

    @property
    def test_case_send_email(self):
        self.set_test_home_dir_postfix('send_email')
        return HydraTestCaseParams(
            name=f'{self.step_name_prefix}_send_email',
            cmd=self.send_email_cmd,
            expected=[
                *self.after_runner_expected,
                re.compile(f"{self.after_runner_docker_run_prefix} eval './sct.py  {self.send_email_cmd_docker}'")],
            not_expected=[*self.after_runner_not_expected],
            return_code=0,
            env=self.get_longevity_env
        ), self.test_tmp_dir

    @property
    def hydra_test_cases(self) -> Iterable[Tuple[HydraTestCaseParams, HydraTestCaseTmpDir]]:
        """
        Creates list of test case parameters that represent steps in longevity pipeline steps
        """
        return (self.test_case_show_conf, self.test_case_create_runner, self.test_case_run_test,
                self.test_case_collect_logs, self.test_case_clean_resources)


class TestHydraSh(unittest.TestCase):
    cmd_runner = LocalCmdRunner()

    @staticmethod
    def prepare_environment(env):
        for name in os.environ:
            if any(name.startswith(prefix) for prefix in ['SCT_', 'AWS_', 'GOOGLE_']):
                del os.environ[name]

        for name, value in env.items():
            os.environ[name] = value

    @staticmethod
    @contextlib.contextmanager
    def environ():
        old_environment = os.environ.copy()
        yield
        os.environ.clear()
        for name, value in old_environment.items():
            os.environ[name] = value

    @staticmethod
    def validate_result(
            result,
            expected_status: int,
            expected: Sequence[Union[str, re.Pattern]],
            not_expected: Sequence[Union[str, re.Pattern]],
    ):
        errors = []
        if expected_status is not None:
            if result.return_code != expected_status:
                errors.append(f'Returned status {result.return_code}, while expected {expected_status}')

        for pattern_expected in expected:
            if isinstance(pattern_expected, re.Pattern):
                if not pattern_expected.search(result.stdout):
                    errors.append(f"Can't find regex {pattern_expected.pattern}")
            elif isinstance(pattern_expected, str):
                if pattern_expected not in result.stdout:
                    errors.append(f"Can't find {pattern_expected}")

        for pattern_not_expected in not_expected:
            if isinstance(pattern_not_expected, re.Pattern):
                if pattern_not_expected.search(result.stdout):
                    errors.append(f"Found pattern that should not be there: {pattern_not_expected}")
            elif isinstance(pattern_not_expected, str):
                if pattern_not_expected in result.stdout:
                    errors.append(f"Found pattern that should not be there: {pattern_not_expected}")
        return errors

    @parameterized.expand(
        LongevityPipelineTest(backend='aws', runner=False, aws_creds=True).hydra_test_cases +
        LongevityPipelineTest(backend='aws', runner=True, aws_creds=True).hydra_test_cases +
        LongevityPipelineTest(backend='gce', runner=False, aws_creds=True).hydra_test_cases +
        LongevityPipelineTest(backend='gce', runner=True, aws_creds=True).hydra_test_cases +
        LongevityPipelineTest(backend='gce-siren', runner=False, aws_creds=True).hydra_test_cases +
        LongevityPipelineTest(backend='gce-siren', runner=True, aws_creds=True).hydra_test_cases
    )
    def test_run_test(self, test_case_params: HydraTestCaseParams, tmp_dir: HydraTestCaseTmpDir):
        with tmp_dir, self.environ():
            cmd, expected, not_expected, expected_status, env = test_case_params.as_tuple
            self.prepare_environment(env)
            result = self.cmd_runner.run(
                sct_abs_path('docker/env/hydra.sh') + ' --dry-run-hydra ' + cmd, ignore_status=True)
            errors = self.validate_result(
                result=result,
                expected_status=expected_status,
                expected=expected,
                not_expected=not_expected,
            )

        assert not errors, f'Case: {cmd}\nReturned:\n{result}\nFound following errors:\n' + ('\n'.join(errors))
