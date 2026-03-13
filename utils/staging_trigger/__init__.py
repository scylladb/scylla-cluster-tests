"""SCT staging Jenkins job trigger and generator.

This package provides tools to trigger, generate, and manage Jenkins staging
jobs for SCT (Scylla Cluster Tests).

Library usage:
    from utils.staging_trigger import StagingTrigger

    trigger = StagingTrigger.from_preset("longevity", branch="my-branch")
    trigger.select_jobs("longevity-100gb-4h-test")
    trigger.run()

CLI usage:
    python staging_trigger.py trigger -b my-branch longevity-100gb-4h-test
    python staging_trigger.py generate -b my-branch
    python staging_trigger.py run-config my_tests.yaml
"""

from utils.staging_trigger.cache import lookup_pr, prompt_for_source
from utils.staging_trigger.cli import cli
from utils.staging_trigger.constants import (
    ARGUS_URL,
    DTEST_TOPOLOGY_FLAGS,
    PIPELINE_TO_PRESET,
    PRESET_NAMES,
    Preset,
    SCT_REPO,
    TriggeredJob,
    get_presets,
)
from utils.staging_trigger.interactive import (
    browse_jenkinsfiles,
    generate_python_code,
    generate_yaml_config,
    prompt_for_params,
)
from utils.staging_trigger.jenkins_client import JenkinsJobTrigger
from utils.staging_trigger.job_generation import generate_from_path, generate_job, generate_jobs
from utils.staging_trigger.package_lookup import aws_bucket_ls, latest_unified_package
from utils.staging_trigger.trigger import (
    StagingTrigger,
    detect_preset_from_jenkinsfile,
    detect_preset_from_job_name,
    find_jenkinsfile,
    format_checklist,
    parse_set_params,
    run_from_config,
)

__all__ = [
    "ARGUS_URL",
    "DTEST_TOPOLOGY_FLAGS",
    "JenkinsJobTrigger",
    "aws_bucket_ls",
    "PIPELINE_TO_PRESET",
    "PRESET_NAMES",
    "Preset",
    "SCT_REPO",
    "StagingTrigger",
    "TriggeredJob",
    "browse_jenkinsfiles",
    "cli",
    "detect_preset_from_jenkinsfile",
    "detect_preset_from_job_name",
    "find_jenkinsfile",
    "format_checklist",
    "generate_from_path",
    "generate_job",
    "generate_jobs",
    "generate_python_code",
    "generate_yaml_config",
    "get_presets",
    "latest_unified_package",
    "lookup_pr",
    "parse_set_params",
    "prompt_for_params",
    "prompt_for_source",
    "run_from_config",
]
