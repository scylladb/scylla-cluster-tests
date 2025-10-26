#!/usr/bin/env python
"""
Example test file demonstrating the use of labels in test method docstrings.

This shows how to add labels to test methods for better categorization
and filtering by Argus and other automation tools.
"""

from sdcm.tester import ClusterTester


class ExampleLabeledTest(ClusterTester):
    """
    Example test class showing label usage.
    """

    def test_performance_critical(self):
        """
        Test critical performance scenarios under high load.

        This test validates that the cluster can handle sustained
        high throughput without degradation.

        Labels: performance, critical, high-priority, smoke-test
        """
        # Test implementation would go here

    def test_stability_long_running(self):
        """
        Test cluster stability over extended time period.

        Runs for 24 hours with various nemesis operations to ensure
        the cluster remains stable under stress.

        Labels: stability, long-running, nemesis, regression
        """
        # Test implementation would go here

    def test_feature_materialized_views(self):
        """
        Test materialized views functionality.

        Labels: feature-test, materialized-views, functional
        """
        # Test implementation would go here

    def test_upgrade_scenario(self):
        """
        Test rolling upgrade from older version.

        This test performs a rolling upgrade and validates
        that the cluster remains available throughout.

        Labels:
            - upgrade
            - rolling-upgrade
            - availability
            - multi-version
        """
        # Test implementation would go here
