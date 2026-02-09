"""Shared fixtures for the diagnostic_collector unit tests."""

import pytest

from unit_tests.unit.diagnostic_collector import MockDiagnosticCollector


@pytest.fixture
def mock_collector():
    """A MockDiagnosticCollector with a fixed name, so tests can assert on it."""
    return MockDiagnosticCollector(name="TestCollector")
