from typing import TypedDict


class RawSirenadaTestCase(TypedDict):
    test_name: str
    class_name: str
    file_name: str
    browser_type: str
    cluster_type: str
    status: str
    duration: float
    message: str
    start_time: str
    stack_trace: str
    screenshot_file: str
    s3_folder_id: str
    requests_file: str
    sirenada_test_id: str
    sirenada_user: str
    sirenada_password: str


class RawSirenadaRequest(TypedDict):
    build_id: str
    build_job_url: str
    run_id: str # UUID
    region: list[str]
    browsers: list[str]
    clusters: list[str]
    sct_test_id: str
    results: list[RawSirenadaTestCase]


class SirenadaPluginException(Exception):
    pass
