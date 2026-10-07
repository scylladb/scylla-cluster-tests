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
import subprocess

from sdcm.sct_provision.user_data_objects.docker_service import DockerUserDataObject


def test_docker_user_data_script_defines_backoff_and_waits_for_apt_lock():
    """The script runs alone under bash -eux, so a missing backoff() aborts it on the first retry."""
    script = DockerUserDataObject(
        test_config=None, params=None, instance_name="loader", node_type="loader"
    ).script_to_run

    assert "backoff() {" in script
    assert script.index("backoff() {") < script.index("$(backoff")
    assert "/var/lib/apt/lists/lock" in script.split("sh get-docker.sh")[0]
    subprocess.run(["bash", "-n"], input=script, text=True, check=True)
