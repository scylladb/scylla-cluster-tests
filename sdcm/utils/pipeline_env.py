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

"""The SCT_* environment of a Jenkins job, generated from the SCT configuration model (SCT-1492).

Every SCT option `<option>` is exported as `SCT_<OPTION>` from the job parameter of the same name, or from
the parameter its `SctField(jenkins_param=...)` names. The job parameters (`params`) come first, then the
jenkinsfile arguments (`pipelineParams`). Unset values (Groovy truth: null, "", false) are not exported, and
a variable that the build already has in its environment is left as it is.

Every `vars/` step gets its environment from here, through `vars/pipelineEnv.groovy`.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

from sdcm.sct_config.config import SCTConfiguration
from sdcm.sct_config.types import is_ignored_field


@dataclass(frozen=True)
class PipelineInput:
    """What a `vars/` step receives: the job `params`, the jenkinsfile `pipelineParams`, and the builder region."""

    params: dict[str, Any] = field(default_factory=dict)
    pipeline_params: dict[str, Any] = field(default_factory=dict)
    builder_region: str = ""

    @classmethod
    def from_file(cls, path: str | Path) -> "PipelineInput":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            params=data.get("params") or {},
            pipeline_params=data.get("pipeline_params") or {},
            builder_region=data.get("builder_region") or "",
        )


@cache
def option_params() -> dict[str, tuple[str, ...]]:
    """Each SCT option and the job parameters that set it: its own name, then its `jenkins_param`."""
    options = {}
    for name, info in SCTConfiguration.model_fields.items():
        if is_ignored_field(info):
            continue
        extra = info.json_schema_extra if isinstance(info.json_schema_extra, dict) else {}
        aliases = extra.get("jenkins_param") or ()
        options[name] = (name, *([aliases] if isinstance(aliases, str) else aliases))
    return options


def to_str(value: Any) -> str:
    """Format a value the way the Groovy steps did: booleans in lower case, lists and maps as JSON."""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value)


def pipeline_env(job: PipelineInput, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """The SCT_* variables of a job; those already in `environ` are left out, so the build's own values win."""
    environ = environ or {}
    env = {}
    for option, params in option_params().items():
        name = f"SCT_{option.upper()}"
        if name in environ:
            continue
        sources = (job.params, job.pipeline_params)
        value = next((source[param] for source in sources for param in params if source.get(param)), None)
        if value is None:
            continue
        if value == "random" and option.endswith(("region_name", "regions")):
            value = job.builder_region  # the builder's region, which only Jenkins knows (getJenkinsLabels)
        env[name] = to_str(value)
    return env
