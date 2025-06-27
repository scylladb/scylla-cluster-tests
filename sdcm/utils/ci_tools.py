"""
Functions related to CI pipelines (Jenkins/GH Actions)
"""

import logging
import os

LOGGER = logging.getLogger(__name__)


def get_job_name() -> str:
    return os.environ.get("JOB_NAME", "local_run")


def get_job_url() -> str:
    return os.environ.get("BUILD_URL", "")


def get_test_name() -> str:
    job_name = get_job_name()
    if job_name and job_name != "local_run":
        return job_name.split("/")[-1]

    if config_files := os.environ.get("SCT_CONFIG_FILES"):
        # Example: 'test-cases/gemini/gemini-1tb-10h.yaml,test-cases/gemini/gemini-10tb-10h.yaml'
        config_file = config_files[0] if isinstance(config_files, list) else config_files.split(",")[0].strip()
        return config_file.split("/")[-1].replace(".yaml", "").replace('"', "").replace("'", "").rstrip("]")

    return "#TEST_NAME_NOT_FOUND"
