"""Conftest for lint tests — patches Docker early when daemon is unavailable."""

from unittest.mock import MagicMock


def pytest_configure(config):
    """Patch Docker client before any sdcm imports to handle missing daemon."""
    try:
        import docker.client  # noqa: PLC0415

        docker.client.DockerClient.from_env = MagicMock(return_value=MagicMock())
        import docker  # noqa: PLC0415

        docker.DockerClient.from_env = MagicMock(return_value=MagicMock())
    except ImportError:
        pass
