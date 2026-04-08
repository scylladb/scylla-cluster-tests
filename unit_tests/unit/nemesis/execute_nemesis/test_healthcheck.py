def test_execute_nemesis_skips_health_check_when_previous_skipped(nemesis, nemesis_runner):
    """Test health check is skipped when previous nemesis was skipped."""
    nemesis_runner.last_nemesis_event = type(
        "MockEvent", (), {"is_skipped": True, "nemesis_name": "PreviousNemesis", "skip_reason": "Previous skip reason"}
    )()
    nemesis_runner.execute_nemesis(nemesis)
    nemesis_runner.cluster.check_cluster_health.assert_not_called()


def test_execute_nemesis_runs_health_check_when_previous_not_skipped(nemesis, nemesis_runner):
    """Test health check runs when previous nemesis was not skipped."""
    nemesis_runner.last_nemesis_event = type("MockEvent", (), {"is_skipped": False})()
    nemesis_runner.execute_nemesis(nemesis)
    nemesis_runner.cluster.check_cluster_health.assert_called_once()


def test_execute_nemesis_no_previous_event(nemesis, nemesis_runner):
    """Test health check runs when there's no previous nemesis event."""
    nemesis_runner.last_nemesis_event = None
    nemesis_runner.execute_nemesis(nemesis)
    nemesis_runner.cluster.check_cluster_health.assert_called_once()
