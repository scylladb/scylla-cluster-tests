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
# Copyright (c) 2026 ScyllaDB

"""Loading and validation of trigger matrix YAML files.

Layout validation runs before pydantic so that a typo in a matrix file is reported with all
its siblings and a "did you mean" hint, rather than one pydantic error at a time."""

import difflib
import logging
from pathlib import Path

import pydantic
import yaml

from sdcm.utils.trigger_matrix.errors import MatrixValidationError
from sdcm.utils.trigger_matrix.models import JOB_LEVEL_KEYS, MATRIX_LEVEL_KEYS, MatrixConfig

logger = logging.getLogger(__name__)


def get_parameterized_cron(path: str | Path) -> str:
    """Extract parameterizedCron spec from a matrix YAML file.

    Returns a string suitable for the Jenkins parameterizedCron trigger,
    with one line per cron_triggers entry in the format:
        schedule % key1=val1\\nkey2=val2
    """
    config = load_matrix_config(path)
    lines = []
    for cron in config.cron_triggers:
        param_parts = ";".join(f"{k}={v}" for k, v in cron.params.items())
        lines.append(f"{cron.schedule} % {param_parts}" if param_parts else cron.schedule)
    return "\n".join(lines)


def _did_you_mean(key: str, candidates: frozenset[str]) -> str:
    """Return a ' — did you mean ...' hint when `key` looks like a typo of a known key."""
    close = difflib.get_close_matches(key, sorted(candidates), n=1, cutoff=0.8)
    return f" — did you mean '{close[0]}'?" if close else ""


def _validate_params_block(params: object, where: str, errors: list[str]) -> None:
    """Validate a `params`/`defaults` mapping: no job-level keys, scalar values only.

    Every entry ends up as a Jenkins StringParameterValue, so nested lists/mappings
    are always a mistake — multi-valued parameters are passed as JSON strings.
    """
    if not isinstance(params, dict):
        errors.append(f"{where}: must be a mapping of Jenkins parameters, got {type(params).__name__}")
        return

    for key, value in params.items():
        if key in JOB_LEVEL_KEYS:
            errors.append(
                f"{where}: '{key}' is a job-level key and must not be nested under 'params:' — "
                f"move it up, next to 'job_name:'"
            )
        if isinstance(value, (list, dict)):
            hint = (
                f' (for a multi-region job use a JSON string: {key}: \'["eu-west-1", "eu-west-2"]\')'
                if key == "region"
                else " (pass multi-valued parameters as a JSON string)"
            )
            errors.append(
                f"{where}: '{key}' must be a scalar — Jenkins parameters are strings, got {type(value).__name__}{hint}"
            )


def _validate_job_entry(index: int, job: object, errors: list[str]) -> None:
    """Validate a single raw job entry before pydantic parsing, for locatable errors."""
    if not isinstance(job, dict):
        errors.append(f"jobs[{index}]: must be a mapping, got {type(job).__name__}")
        return

    where = f"job '{job.get('job_name', '<missing job_name>')}' (jobs[{index}])"
    for key in job:
        if key not in JOB_LEVEL_KEYS:
            errors.append(
                f"{where}: unknown job-level key '{key}' — Jenkins job parameters belong under 'params:'"
                f"{_did_you_mean(key, JOB_LEVEL_KEYS)}"
            )

    if "params" in job:
        _validate_params_block(job["params"], f"{where}: params", errors)


def validate_matrix_layout(raw: dict) -> None:
    """Check that every key in a raw matrix mapping sits where it belongs.

    Pydantic already rejects unknown keys, but its errors don't say *where* a key
    should have gone. This pass collects all misplacements at once so a YAML with
    several mistakes reports them in one go.

    Raises:
        MatrixValidationError: If any key is unknown or in the wrong section.
    """
    errors: list[str] = []

    for key in raw:
        if key not in MATRIX_LEVEL_KEYS:
            errors.append(
                f"unknown top-level key '{key}' — expected one of: {', '.join(sorted(MATRIX_LEVEL_KEYS))}"
                f"{_did_you_mean(key, MATRIX_LEVEL_KEYS)}"
            )

    if "defaults" in raw:
        _validate_params_block(raw["defaults"], "defaults", errors)

    for index, cron in enumerate(raw.get("cron_triggers") or []):
        if isinstance(cron, dict) and "params" in cron:
            _validate_params_block(cron["params"], f"cron_triggers[{index}]: params", errors)

    for index, job in enumerate(raw.get("jobs") or []):
        _validate_job_entry(index, job, errors)

    if errors:
        raise MatrixValidationError("Invalid trigger matrix layout:\n  - " + "\n  - ".join(errors))


def load_matrix_config(path: str | Path) -> MatrixConfig:
    """Load and validate a trigger matrix YAML file.

    Args:
        path: Path to the YAML file.

    Returns:
        MatrixConfig with validated data.

    Raises:
        MatrixValidationError: If the YAML is malformed or missing required fields.
        FileNotFoundError: If the YAML file does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Matrix file not found: {path}")

    with open(path, encoding="utf-8") as fobj:
        raw = yaml.safe_load(fobj)

    if not isinstance(raw, dict):
        raise MatrixValidationError(f"Matrix file must be a YAML mapping, got {type(raw).__name__}")

    if "jobs" not in raw:
        raise MatrixValidationError("Matrix file must contain a 'jobs' key")

    raw_jobs = raw["jobs"]
    if not isinstance(raw_jobs, list):
        raise MatrixValidationError(f"'jobs' must be a list, got {type(raw_jobs).__name__}")

    raw_email = raw.get("email_recipients", [])
    if isinstance(raw_email, str):
        raw["email_recipients"] = [e.strip() for e in raw_email.split(",") if e.strip()]

    validate_matrix_layout(raw)

    try:
        return MatrixConfig.model_validate(raw)
    except pydantic.ValidationError as exc:
        raise MatrixValidationError(f"Invalid trigger matrix {path}:\n{exc}") from exc
