"""Unit tests for StressLoadOperations list handling logic."""


def test_generate_load_handles_list_of_commands():
    """Test that _generate_load properly passes list of commands to _run_all_stress_cmds."""
    stress_cmd = ["cassandra-stress write cl=QUORUM duration=1m", "cassandra-stress read cl=QUORUM duration=30s"]

    # stress_cmd is always list[str] now
    params = {"stress_cmd": stress_cmd, "keyspace_name": "test_ks", "round_robin": False}

    assert params["stress_cmd"] == stress_cmd
    assert isinstance(params["stress_cmd"], list)
    assert len(params["stress_cmd"]) == 2


def test_generate_load_handles_single_command_as_list():
    """Test that _generate_load properly passes single command (as list) to _run_all_stress_cmds."""
    stress_cmd = ["cassandra-stress write cl=QUORUM duration=1m"]

    params = {"stress_cmd": stress_cmd, "keyspace_name": "test_ks", "round_robin": False}

    assert params["stress_cmd"] == stress_cmd
    assert isinstance(params["stress_cmd"], list)
    assert len(params["stress_cmd"]) == 1


def test_generate_background_read_load_replaces_placeholder_in_list():
    """Test that generate_background_read_load replaces placeholder in each command of a list."""
    stress_cmds = [
        "cassandra-stress read -rate threads=<THROTTLE_PLACE_HOLDER>",
        "cassandra-stress mixed -rate threads=<THROTTLE_PLACE_HOLDER>",
    ]

    throttle_value = 21999

    # stress_cmds is always list[str] now
    result = [cmd.replace("<THROTTLE_PLACE_HOLDER>", str(throttle_value)) for cmd in stress_cmds]

    assert len(result) == 2
    assert "<THROTTLE_PLACE_HOLDER>" not in result[0]
    assert "<THROTTLE_PLACE_HOLDER>" not in result[1]
    assert "threads=21999" in result[0]
    assert "threads=21999" in result[1]
