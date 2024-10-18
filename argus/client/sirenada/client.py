import json
import logging
from glob import glob, escape
from pathlib import Path
from typing import TypedDict
from xml.etree import ElementTree

from argus.common.sirenada_types import RawSirenadaRequest, RawSirenadaTestCase

from argus.client.base import ArgusClient


LOGGER = logging.getLogger(__name__)

class SirenadaEnv(TypedDict):
    SIRENADA_JOB_ID: str
    SIRENADA_BROWSER: str
    SIRENADA_REGION: str
    SIRENADA_CLUSTER: str
    WORKSPACE: str
    BUILD_NUMBER: str
    BUILD_URL: str
    JOB_NAME: str


class TestCredentials(TypedDict):
    SIRENADA_TEST_ID: str
    SIRENADA_USER_NAME: str
    SIRENADA_USER_PASS: str
    SIRENADA_OTP_SECRET: str
    ClusterID: str
    region: str

class ArgusSirenadaClient(ArgusClient):
    test_type = "sirenada"
    schema_version: None = "v1"
    _junit_xml_filename = "junit_results.xml"
    _credentials_filename = "test_credentials.json"
    _bucket_url_template = "https://sirenada-results.s3.us-east-1.amazonaws.com/{s3_id}/{test_results_dir}/{filename}"
    TEST_TAG_MAPPING = {
        "failure": "failed",
        "error": "error",
        "skipped": "skipped"
    }

    def __init__(self, auth_token: str, base_url: str, api_version="v1") -> None:
        self.results_path: Path | None = None
        super().__init__(auth_token, base_url, api_version)

    def _verify_required_files_exist(self, results_path: Path):
        assert (results_path / self._junit_xml_filename).exists(), "Missing jUnit XML results file!"
        assert (results_path / self._credentials_filename).exists(), "Missing test credentials file!"

    @staticmethod
    def _verify_env(env: SirenadaEnv):
        for key in SirenadaEnv.__annotations__.keys():
            assert env.get(key), f"Missing required key {key} in the environment"

    def _set_files_for_failed_test(self, test_case: RawSirenadaTestCase):
        test_results_dir = f"sirenada-{test_case['sirenada_test_id']}"
        results_dir: Path = self.results_path / test_results_dir

        just_class_name = test_case.get("class_name").split(".")[-1]
        requests_pattern = f"{just_class_name}-{test_case.get('test_name')}.requests.json"
        screenshot_pattern = f"{just_class_name}-{escape(test_case.get('test_name'))}-*.png"

        if not results_dir.exists():
            return

        if not (s3_bucket_id_path := (results_dir / "s3_id")).exists():
            return

        with open(s3_bucket_id_path, "rt") as s3_id_file:
            test_case["s3_folder_id"] = s3_id_file.read().strip()

        test_case["requests_file"] = self._bucket_url_template.format(
            s3_id=test_case.get("s3_folder_id"),
            test_results_dir=test_results_dir,
            filename=requests_pattern,
        )

        screenshots = glob(pathname=screenshot_pattern, root_dir=results_dir)
        if len(screenshots) > 0:
            test_case["screenshot_file"] = self._bucket_url_template.format(
                s3_id=test_case.get("s3_folder_id"),
                test_results_dir=test_results_dir,
                filename=screenshots[0],
            )

    def _set_case_status(self, raw_case: RawSirenadaTestCase, junit_case: ElementTree.Element):
        subelements = list(junit_case.iter())
        if len(subelements) == 2:
            status_node = subelements[1]
            raw_case["status"] = self.TEST_TAG_MAPPING.get(status_node.tag, "failed")
            raw_case["message"] = status_node.get("message", "#NO_MESSAGE")
            raw_case["stack_trace"] = status_node.text
            self._set_files_for_failed_test(raw_case)
        else:
            raw_case["status"] = "passed"

    def _parse_junit_results(self, junit_xml: Path, credentials: TestCredentials, env: SirenadaEnv) -> list[RawSirenadaTestCase]:
        etree = ElementTree.parse(source=junit_xml)
        cases = list(etree.iter("testcase"))
        assert len(cases) > 0, "No test cases found in test results XML."

        parsed_cases: list[RawSirenadaTestCase] = []
        for case in cases:
            raw_case: RawSirenadaTestCase = {}
            raw_case["class_name"] = case.get("classname", "#NO_CLASSNAME")
            raw_case["file_name"] = case.get("file", "#NO_FILE")
            raw_case["test_name"] = case.get("name", "#NO_TEST_NAME")
            raw_case["browser_type"] = env.get("SIRENADA_BROWSER")
            raw_case["cluster_type"] = env.get("SIRENADA_CLUSTER")
            raw_case["duration"] = float(case.attrib.get("time", "0.0"))
            raw_case["sirenada_test_id"] = credentials.get("SIRENADA_TEST_ID")
            raw_case["sirenada_user"] = credentials.get("SIRENADA_USER_NAME")
            raw_case["sirenada_password"] = credentials.get("SIRENADA_USER_PASS")
            self._set_case_status(raw_case, case)
            parsed_cases.append(raw_case)

        return parsed_cases

    @staticmethod
    def _read_credentials(credentials_path: Path) -> TestCredentials:
        with open(credentials_path, "rt") as creds_file:
            credentials: TestCredentials = json.load(creds_file)

        assert credentials.get("SIRENADA_TEST_ID"), "Credentials are missing required field: SIRENADA_TEST_ID"
        return credentials

    def submit_sirenada_run(self, env: SirenadaEnv, test_results_path: Path):
        self.results_path = test_results_path
        try:
            self._verify_env(env)
            self._verify_required_files_exist(self.results_path)
        except AssertionError as exc:
            LOGGER.critical("%s", exc.args[0])
            raise exc

        credentials = self._read_credentials(self.results_path / self._credentials_filename)
        results = self._parse_junit_results(junit_xml=self.results_path / self._junit_xml_filename, credentials=credentials, env=env)
        request_body: RawSirenadaRequest = {}

        request_body["run_id"] = env.get("SIRENADA_JOB_ID")
        request_body["build_id"] = env.get("JOB_NAME")
        request_body["build_job_url"] = env.get("BUILD_URL")
        request_body["region"] = env.get("SIRENADA_REGION")
        request_body["results"] = results

        response = self.submit_run(run_type=self.test_type, run_body=request_body)
        self.check_response(response)
