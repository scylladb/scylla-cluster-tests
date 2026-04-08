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
# Copyright (c) 2025 ScyllaDB

import os

from sdcm.utils.sct_agent_installer import (
    generate_agent_api_key,
    save_agent_api_key,
    load_agent_api_key,
    AGENT_API_KEY_FILENAME,
)


def test_generate_agent_api_key():
    api_key = generate_agent_api_key()
    assert api_key, "API should be generated and not empty"

    api_key2 = generate_agent_api_key()
    assert api_key != api_key2, "Generated keys should be unique"


def test_save_and_load_agent_api_key(tmp_path):
    api_key = "test-api-key-12345"
    save_agent_api_key(str(tmp_path), api_key)

    key_path = tmp_path / AGENT_API_KEY_FILENAME
    assert os.path.exists(key_path), "API key file should exist"
    file_stat = os.stat(key_path)
    file_mode = file_stat.st_mode & 0o777
    assert file_mode == 0o600, f"File should have 600 permissions, got {oct(file_mode)}"

    loaded_key = load_agent_api_key(str(tmp_path))
    assert api_key == loaded_key, "Loaded key should match saved key"


def test_load_nonexistent_api_key(tmp_path):
    nonexistent_dir = os.path.join(str(tmp_path), "nonexistent")
    assert load_agent_api_key(nonexistent_dir) is None, "Loading from nonexistent directory should return None"
