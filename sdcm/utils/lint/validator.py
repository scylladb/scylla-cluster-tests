"""Validation engine for Jenkins pipeline configurations.

Validates SCT configurations derived from Jenkins pipeline files by
instantiating SCTConfiguration with the pipeline-derived environment.
"""

import logging
import os
import traceback
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_pipeline(pipeline_path: Path, env: dict[str, str]) -> tuple[bool, str]:
    """Validate a single pipeline's SCT configuration.

    Sets the given environment variables, instantiates SCTConfiguration,
    and runs verify_configuration() and check_required_files().

    Args:
        pipeline_path: Path to the .jenkinsfile (for error messages).
        env: Environment variables to set (from build_env()).

    Returns:
        Tuple of (is_error, error_message). Empty message on success.
    """
    # Isolate environment: clear everything and set only our env vars
    while os.environ:
        os.environ.popitem()
    for key, value in env.items():
        os.environ[key] = value

    # Suppress logging noise from SCTConfiguration
    logging.getLogger().handlers = []
    logging.getLogger().disabled = True

    try:
        # Deferred import: runs in worker process to avoid importing heavy deps in main process
        from sdcm.sct_config import SCTConfiguration  # noqa: PLC0415

        config = SCTConfiguration()
        config.verify_configuration()
        config.check_required_files()
        return False, ""
    except Exception as exc:  # noqa: BLE001
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return True, tb
