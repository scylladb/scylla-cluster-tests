"""Reusable Jenkins job trigger module for SCT staging tests.

This is a backward-compatible shim that re-exports everything from
utils.staging_trigger. See utils/staging_trigger/ for the actual implementation.

Features:
- Click CLI with interactive prompts and colored output
- YAML config files for mass triggering (see example at bottom of file)
- All Jenkins parameters overridable via --set KEY=VALUE
- PR-based branch auto-detection
- Job generation from jenkinsfiles
- GitHub-friendly markdown checklists

CLI examples:
    # Interactive: browse jobs, select, trigger (default mode)
    python staging_trigger.py trigger

    # Filter jobs interactively
    python staging_trigger.py trigger "longevity*"

    # Direct trigger (no interaction)
    python staging_trigger.py trigger -b my-branch longevity-100gb-4h-test

    # Trigger from PR (auto-detects repo/branch)
    python staging_trigger.py trigger --pr 12345 manager-ubuntu20-sanity-test

    # Edit parameters interactively before triggering
    python staging_trigger.py trigger -e

    # Dry run
    python staging_trigger.py trigger -n

    # Export selected jobs as YAML config
    python staging_trigger.py trigger -o my_tests.yaml

    # Override any Jenkins parameter
    python staging_trigger.py trigger -b my-branch longevity-100gb-4h-test \\
        --set scylla_version=2025.1 --set provision_type=spot

    # Trigger from YAML config
    python staging_trigger.py run-config my_tests.yaml

    # Generate jobs from jenkinsfiles (interactive, shows trigger examples after)
    python staging_trigger.py generate -b my-branch

Library usage:
    from staging_trigger import StagingTrigger

    trigger = StagingTrigger.from_preset("longevity", branch="my-branch")
    trigger.select_jobs("longevity-100gb-4h-test")
    trigger.run()
"""

# Re-export everything from the package
from utils.staging_trigger import *  # noqa: F401, F403
from utils.staging_trigger import __all__  # noqa: F401
from utils.staging_trigger.cli import cli

if __name__ == "__main__":
    cli()
