"""
Functions related to CI pipelines (Jenkins/GH Actions)
"""
import logging
import os

LOGGER = logging.getLogger(__name__)


def get_job_name() -> str:
    return os.environ.get('JOB_NAME', 'local_run')


def get_job_url() -> str:
    return os.environ.get('BUILD_URL', '')
