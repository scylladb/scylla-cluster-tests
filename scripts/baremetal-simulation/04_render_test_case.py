#!/usr/bin/env python3
"""Step 4 -- render the test-case YAML for the simulated bare-metal run.

    uv run python scripts/baremetal-simulation/04_render_test_case.py

Generates SIM_TEST_CASE (default test-cases/artifacts/baremetal-fedora.yaml).
Modelled on test-cases/artifacts/rocky9.yaml, with everything that is inert on
this backend removed:

  * no instance_type_db / sizing_db -- 'baremetal' is in _SIZING_SKIP_BACKENDS
    (sdcm/sct_config/config.py:167), those options are silently ignored.
  * no db_nodes_public_ip / loaders_public_ip / monitor_nodes_* -- they exist as
    config options (sdcm/sct_config/mixins/baremetal.py) but get_cluster_baremetal()
    never reads them.  Only the JSON from step 2 is used.

And with the defaults that bite on a hand-run overridden:

  * logs_transport: ssh      -- the default 'vector' makes the node push logs *to*
    the runner, which never arrives when the runner is a laptop behind NAT.
  * use_mgmt / run_scylla_doctor: false -- both default to true; keep the first
    run down to the one thing being measured.
  * scylla_repo is set explicitly even though _check_version_supplied() exempts
    'baremetal' from requiring it (gap #1 on SCT-901).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import REPO_ROOT, cfg, cfg_int, log  # noqa: E402

TEMPLATE = """\
test_metadata:
  description: >-
    Artifacts validation test running against pre-provisioned hosts through the baremetal
    backend. Used by the SCT-901 bare-metal simulation, where the "physical" hosts are plain
    EC2 Fedora instances provisioned outside SCT by scripts/baremetal-simulation/.
  test_type: artifacts
  tier: sanity
  duration_class: short
  supported_backends:
    - baremetal
  stress_tools: []
  nemesis_labels:
    - NoOpMonkey
  team_ownership: core-test-infra

cluster_backend: 'baremetal'
s3_baremetal_config: '{baremetal_config_name}'
user_credentials_path: '{ssh_key}'

use_preinstalled_scylla: false
{install_source}
scylla_linux_distro: 'centos'

n_db_nodes: {n_db_nodes}
n_loaders: {n_loaders}
n_monitor_nodes: {n_monitor_nodes}
nemesis_class_name: 'NoOpMonkey'

ip_ssh_connections: 'public'
ssh_transport: 'fabric'
logs_transport: 'ssh'

use_mgmt: false
run_scylla_doctor: false
backtrace_decoding: false
run_db_node_benchmarks: false

test_duration: 90
user_prefix: '{user_prefix}'
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--force", action="store_true", help="overwrite an existing file")
    args = parser.parse_args()

    target = REPO_ROOT / cfg("SIM_TEST_CASE")
    if target.exists() and not args.force:
        log(f"{target} already exists (use --force to regenerate)")
        return 0

    if unified_package := cfg("SIM_UNIFIED_PACKAGE", ""):
        # Fallback 1: the relocatable unified package, which is the install approach
        # SCT-901 proposes to adopt.  _scylla_install() routes to offline_install_scylla()
        # and scylla_setup gets --no-verify-package automatically.
        install_source = f"unified_package: '{unified_package}'"
    else:
        install_source = f"scylla_repo: '{cfg('SIM_SCYLLA_REPO')}'"

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        TEMPLATE.format(
            baremetal_config_name=cfg("SIM_BAREMETAL_CONFIG_NAME"),
            ssh_key=cfg("SIM_SSH_KEY"),
            install_source=install_source,
            n_db_nodes=cfg_int("SIM_DB_COUNT"),
            n_loaders=cfg_int("SIM_LOADER_COUNT"),
            n_monitor_nodes=cfg_int("SIM_MONITOR_COUNT"),
            user_prefix=f"{cfg('SIM_TEST_TAG')}",
        ),
        encoding="utf-8",
    )
    log(f"wrote {target}")
    log(f"validate it with: hydra conf -b baremetal {cfg('SIM_TEST_CASE')}")
    log("next: 05_run_artifact_test.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
