"""
Unit tests for tier1ParallelPipeline.groovy

This test validates the tier1 test matrix configuration and job definitions.
"""

import re
import pytest
from pathlib import Path


class TestTier1Pipeline:
    """Test suite for tier1 parallel pipeline configuration."""

    @pytest.fixture
    def pipeline_file_path(self):
        """Get the path to the tier1ParallelPipeline.groovy file."""
        return Path(__file__).parent.parent / "vars" / "tier1ParallelPipeline.groovy"

    @pytest.fixture
    def pipeline_content(self, pipeline_file_path):
        """Read the pipeline file content."""
        with open(pipeline_file_path, 'r') as f:
            return f.read()

    def test_pipeline_file_exists(self, pipeline_file_path):
        """Test that the tier1 pipeline file exists."""
        assert pipeline_file_path.exists(), "tier1ParallelPipeline.groovy should exist in vars/"

    def test_pipeline_has_required_parameters(self, pipeline_content):
        """Test that the pipeline defines all required parameters."""
        required_params = [
            'scylla_version',
            'scylla_repo',
            'use_job_throttling',
            'labels_selector',
            'skip_jobs',
            'requested_by_user'
        ]
        for param in required_params:
            assert f"name: '{param}'" in pipeline_content, f"Pipeline should have '{param}' parameter"

    def test_pipeline_has_weekly_trigger(self, pipeline_content):
        """Test that the pipeline has a weekly cron trigger."""
        # Check for cron trigger pattern (Saturdays at 6:00 AM)
        assert "00 6 * * 6" in pipeline_content, "Pipeline should have weekly Saturday trigger"
        assert "scylla_version=master:latest" in pipeline_content, "Trigger should set scylla_version to master:latest"
        assert "labels_selector=master-weekly" in pipeline_content, "Trigger should set labels_selector"

    def test_pipeline_has_job_matrix(self, pipeline_content):
        """Test that the pipeline defines a tier1TestMatrix."""
        assert "tier1TestMatrix" in pipeline_content, "Pipeline should define tier1TestMatrix"
        assert "job_name:" in pipeline_content, "Matrix should have job_name entries"
        assert "backend:" in pipeline_content, "Matrix should have backend entries"
        assert "versions:" in pipeline_content, "Matrix should have version entries"

    def test_pipeline_includes_all_tier1_jobs(self, pipeline_content):
        """Test that all known tier1 jobs are included in the matrix."""
        expected_jobs = [
            'longevity-50gb-3days-test',
            'longevity-150gb-asymmetric-cluster-12h-test',
            'longevity-twcs-48h-test',
            'longevity-multidc-schema-topology-changes-12h-test',
            'longevity-mv-si-4days-streaming-test',
            'longevity-schema-topology-changes-12h-test',
            'gemini-1tb-10h-test',
            'longevity-harry-2h-test',
            'longevity-1tb-5days-azure-test',
            'longevity-large-partition-200k-pks-4days-gce-test',
            'jepsen-all-test'
        ]
        for job in expected_jobs:
            assert job in pipeline_content, f"Pipeline should include job: {job}"

    def test_pipeline_includes_multiple_backends(self, pipeline_content):
        """Test that the pipeline includes AWS, Azure, and GCE backends."""
        backends = ['aws', 'azure', 'gce']
        for backend in backends:
            assert f"backend: '{backend}'" in pipeline_content, f"Pipeline should include {backend} backend"

    def test_pipeline_supports_version_ranges(self, pipeline_content):
        """Test that the pipeline defines version ranges for jobs."""
        # Check for recent version years
        versions = ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master']
        for version in versions:
            assert f"'{version}'" in pipeline_content, f"Pipeline should support version {version}"

    def test_pipeline_has_skip_jobs_logic(self, pipeline_content):
        """Test that the pipeline implements job skipping logic."""
        assert "skip_jobs" in pipeline_content, "Pipeline should have skip_jobs parameter"
        assert "skip_jobs_list" in pipeline_content, "Pipeline should parse skip_jobs into a list"
        assert "skip_jobs_list.contains" in pipeline_content, "Pipeline should check if job is in skip list"
        assert "Skipping job" in pipeline_content, "Pipeline should log when skipping jobs"

    def test_pipeline_has_labels_selector_logic(self, pipeline_content):
        """Test that the pipeline implements labels selector logic."""
        assert "labels_selector" in pipeline_content, "Pipeline should have labels_selector parameter"
        assert "master-weekly" in pipeline_content, "Pipeline should support master-weekly label"
        assert "labels.contains(labels_selector)" in pipeline_content, "Pipeline should check labels"

    def test_pipeline_has_image_fetching_logic(self, pipeline_content):
        """Test that the pipeline fetches AMI images for master builds."""
        assert "hydra.sh list-images" in pipeline_content, "Pipeline should fetch images using hydra"
        assert "image_name" in pipeline_content, "Pipeline should handle image names"

    def test_pipeline_handles_backend_specific_parameters(self, pipeline_content):
        """Test that the pipeline sets backend-specific parameters."""
        # AWS parameters
        assert "scylla_ami_id" in pipeline_content, "Pipeline should set scylla_ami_id for AWS"
        assert "region" in pipeline_content, "Pipeline should set region for AWS"
        assert "availability_zone" in pipeline_content, "Pipeline should set availability_zone"

        # Azure parameters
        assert "azure_image_db" in pipeline_content, "Pipeline should set azure_image_db for Azure"

        # GCE parameters
        assert "gce_image_db" in pipeline_content, "Pipeline should set gce_image_db for GCE"

    def test_pipeline_sets_common_parameters(self, pipeline_content):
        """Test that the pipeline sets common test parameters."""
        common_params = [
            'provision_type',
            'use_job_throttling',
            'requested_by_user',
            'post_behavior_db_nodes',
            'post_behavior_monitor_nodes'
        ]
        for param in common_params:
            assert f"name: '{param}'" in pipeline_content, f"Pipeline should set {param}"

    def test_pipeline_has_error_handling(self, pipeline_content):
        """Test that the pipeline has error handling."""
        assert "catchError" in pipeline_content, "Pipeline should have error handling"
        assert "error" in pipeline_content, "Pipeline should handle errors"

    def test_pipeline_has_credentials_setup(self, pipeline_content):
        """Test that the pipeline sets up AWS credentials."""
        assert "AWS_ACCESS_KEY_ID" in pipeline_content, "Pipeline should set AWS_ACCESS_KEY_ID"
        assert "AWS_SECRET_ACCESS_KEY" in pipeline_content, "Pipeline should set AWS_SECRET_ACCESS_KEY"
        assert "credentials(" in pipeline_content, "Pipeline should use Jenkins credentials"

    def test_pipeline_builds_jobs_without_wait(self, pipeline_content):
        """Test that the pipeline builds jobs without waiting."""
        assert "wait: false" in pipeline_content, "Pipeline should build jobs without waiting"

    def test_pipeline_has_test_configs(self, pipeline_content):
        """Test that the pipeline defines test_config for each job."""
        assert "test_config:" in pipeline_content, "Pipeline should have test_config entries"
        assert "test-cases/" in pipeline_content, "Pipeline should reference test-cases"


class TestTier1JobMatrix:
    """Test suite for validating the tier1 job matrix structure."""

    @pytest.fixture
    def pipeline_content(self):
        """Read the pipeline file content."""
        pipeline_file = Path(__file__).parent.parent / "vars" / "tier1ParallelPipeline.groovy"
        with open(pipeline_file, 'r') as f:
            return f.read()

    def extract_job_entries(self, pipeline_content):
        """Extract job entries from the tier1TestMatrix."""
        # Simple pattern matching to count job entries
        # Each entry starts with a '[' and has 'job_name:'
        pattern = r'\[[\s\S]*?job_name:\s*["\']([^"\']+)["\']'
        matches = re.findall(pattern, pipeline_content)
        return matches

    def test_job_matrix_has_minimum_jobs(self, pipeline_content):
        """Test that the job matrix has at least 11 jobs."""
        jobs = self.extract_job_entries(pipeline_content)
        assert len(jobs) >= 11, f"Job matrix should have at least 11 jobs, found {len(jobs)}"

    def test_all_jobs_have_unique_names(self, pipeline_content):
        """Test that all jobs in the matrix have unique names."""
        jobs = self.extract_job_entries(pipeline_content)
        assert len(jobs) == len(set(jobs)), "All jobs should have unique names"

    def test_jobs_have_required_backends(self, pipeline_content):
        """Test that jobs are distributed across different backends."""
        # Count backend occurrences
        aws_count = pipeline_content.count("backend: 'aws'")
        azure_count = pipeline_content.count("backend: 'azure'")
        gce_count = pipeline_content.count("backend: 'gce'")

        assert aws_count > 0, "Should have AWS jobs"
        assert azure_count > 0, "Should have Azure jobs"
        assert gce_count > 0, "Should have GCE jobs"

        print(f"Backend distribution - AWS: {aws_count}, Azure: {azure_count}, GCE: {gce_count}")


class TestTier1PipelineIntegration:
    """Integration tests for tier1 pipeline behavior."""

    @pytest.fixture
    def pipeline_content(self):
        """Read the pipeline file content."""
        pipeline_file = Path(__file__).parent.parent / "vars" / "tier1ParallelPipeline.groovy"
        with open(pipeline_file, 'r') as f:
            return f.read()

    def test_pipeline_supports_release_branches(self, pipeline_content):
        """Test that the pipeline supports release branch versions."""
        # The pipeline should handle version matching like "2025.4" or "2025.4.1"
        assert "startsWith(ver + '.')" in pipeline_content, "Pipeline should support version prefix matching"

    def test_pipeline_emergency_skip_functionality(self, pipeline_content):
        """Test that the skip_jobs parameter can be used to skip specific jobs."""
        # Verify the skip_jobs parameter and logic exists
        assert "skip_jobs" in pipeline_content, "Pipeline should have skip_jobs parameter"
        assert "skip_jobs_list.contains(job_name)" in pipeline_content, "Pipeline should check skip list"

    def test_pipeline_logging(self, pipeline_content):
        """Test that the pipeline has adequate logging."""
        log_messages = [
            "tier1TestMatrix:",
            "Skip jobs list:",
            "Jobs names:",
            "Job name:",
            "Skipping job",
            "Found for job",
            "Building job:"
        ]
        for msg in log_messages:
            assert msg in pipeline_content, f"Pipeline should log: {msg}"

    def test_pipeline_handles_master_version_specially(self, pipeline_content):
        """Test that the pipeline handles 'master:latest' version specially."""
        assert "master:latest" in pipeline_content, "Pipeline should handle master:latest"
        assert "scylla_version = \"master\"" in pipeline_content, "Pipeline should convert master:latest to master"

    def test_pipeline_validates_labels_selector_for_master(self, pipeline_content):
        """Test that the pipeline requires labels_selector for master builds."""
        assert "if (!labels_selector)" in pipeline_content, "Pipeline should check labels_selector"
        assert "error" in pipeline_content, "Pipeline should error if labels_selector is missing for master"
