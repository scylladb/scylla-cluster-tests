#!/usr/bin/env python3
"""
Module to assess test case configuration files and determine which backend(s) should be used.

This module analyzes YAML test case files to determine the appropriate backend(s) for linting
based on filename patterns, content, and configuration values.
"""

import logging
import multiprocessing
import os
import re
import sys
import traceback
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Set, Optional, Dict, Any, Tuple

import click
import yaml

from sdcm.sct_config import SCTConfiguration


class Backend(str, Enum):
    """Supported test backends."""

    AWS = "aws"
    AZURE = "azure"
    GCE = "gce"
    DOCKER = "docker"
    BAREMETAL = "baremetal"
    K8S_LOCAL_KIND = "k8s-local-kind"
    K8S_GKE = "k8s-gke"
    K8S_EKS = "k8s-eks"
    XCLOUD = "xcloud"


@dataclass
class BackendConfig:
    """Configuration for a specific backend test run."""

    backend: Backend
    env_vars: Dict[str, str] = field(default_factory=dict)
    description: str = ""
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    multi_region: bool = False
    region_count: Optional[int] = None


@dataclass
class TestCaseAnalysis:
    """Analysis result for a test case file."""

    file_path: Path
    backends: Set[Backend]
    dc_count: Optional[int] = None
    explicit_backend: Optional[Backend] = None
    reason: str = ""


class TestCaseAnalyzer:
    """Analyzes test case YAML files to determine appropriate backends."""

    # Filename patterns that indicate specific backends
    FILENAME_PATTERNS = {
        Backend.AZURE: [
            r"azure",
        ],
        Backend.GCE: [
            r"gce",
            r"custom-d3",
        ],
        Backend.DOCKER: [
            r"docker",
        ],
        Backend.BAREMETAL: [
            r"baremetal",
        ],
        Backend.K8S_LOCAL_KIND: [
            r"scylla-operator",
            r"kubernetes",
        ],
        Backend.K8S_EKS: [
            r"scylla-operator",
            r"eks",
        ],
        Backend.K8S_GKE: [
            r"scylla-operator",
            r"gke",
        ],
    }

    # Content keys that indicate specific backends
    CONTENT_BACKEND_KEYS = {
        Backend.AWS: ["region_name", "instance_type_db", "ami_id_db_scylla"],
        Backend.AZURE: ["azure_region_name", "azure_instance_type_db", "azure_image_db"],
        Backend.GCE: ["gce_datacenter", "gce_instance_type_db", "gce_image_db"],
        Backend.DOCKER: ["docker_image", "docker_network"],
        Backend.BAREMETAL: ["db_nodes_public_ip", "db_nodes_private_ip", "s3_baremetal_config"],
        Backend.K8S_LOCAL_KIND: ["k8s_n_scylla_pods_per_cluster", "k8s_scylla_operator_docker_image", "k8s_loader_run_type"],
        Backend.K8S_EKS: ["k8s_n_scylla_pods_per_cluster", "k8s_scylla_operator_docker_image", "k8s_loader_run_type", "k8s_minio_storage_size"],
        Backend.K8S_GKE: ["k8s_n_scylla_pods_per_cluster", "k8s_scylla_operator_docker_image", "k8s_loader_run_type", "k8s_minio_storage_size"],
    }

    def __init__(self, test_cases_dir: Optional[Path] = None):
        if test_cases_dir is None:
            # Try to find test-cases directory relative to this file
            script_dir = Path(__file__).resolve().parent
            project_root = script_dir.parent
            self.test_cases_dir = project_root / "test-cases"
        else:
            self.test_cases_dir = test_cases_dir

        # Store project root for relative path display
        self.project_root = Path(__file__).resolve().parent.parent

    def _run_yaml_test(self, backend: str, full_path: str, env: Dict[str, str]) -> Tuple[bool, List[str]]:
        """
        Run YAML test validation directly using SCTConfiguration.

        This is a direct port of the _run_yaml_test function from sct.py,
        avoiding subprocess calls.

        Args:
            backend: Backend name (aws, gce, azure, etc.)
            full_path: Full path to the YAML test case file
            env: Environment variables to set

        Returns:
            Tuple of (error: bool, output: List[str])
        """
        output = []
        error = False
        output.append(f"---- linting: {full_path} -----")

        try:
            # Clear and set environment variables
            for key in os.environ.keys():
                if key.startswith("SCT_"):
                    del os.environ[key]
            os.environ.update(env)
            os.environ["SCT_CLUSTER_BACKEND"] = backend
            os.environ["SCT_CONFIG_FILES"] = str(full_path)

            # Disable logging for this test
            logging.getLogger().handlers = []
            logging.getLogger().disabled = True

            try:
                config = SCTConfiguration()
                config.verify_configuration()
                config.check_required_files()
                output.append("✅ Configuration validated successfully")
            except Exception as exc:  # noqa: BLE001
                output.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
                error = True
        finally:
            # Restore original environment
            logging.getLogger().disabled = False

        return error, output

    def run_yaml_test_for_config(
        self,
        config: BackendConfig,
        file_path: Path,
        verbose: bool = True,
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """
        Run YAML test validation for a specific backend configuration.

        Args:
            config: BackendConfig with backend and environment variables
            file_path: Path to the test case file
            verbose: If True, print detailed output

        Returns:
            Tuple of (exit_code, failure_info_dict or None)
        """
        # Run the test
        error, output = self._run_yaml_test(backend=config.backend.value,
                                            full_path=str(file_path.resolve()), env=config.env_vars)

        # Get relative path from project root
        try:
            rel_path = file_path.relative_to(self.project_root)
        except ValueError:
            rel_path = file_path

        failure_info = None

        # Print output - concise for success, detailed for failure
        if verbose:
            if error:
                # Show full details for failures
                if config.description:
                    click.echo(f"\n{'=' * 60}")
                    click.echo(f"{config.description}")
                    click.echo(f"{'=' * 60}")
                click.echo(f"Testing: {rel_path}")
                for line in output:
                    click.echo(line)

                # Store failure info for summary
                failure_info = {"file": str(
                    rel_path), "config": config.description or config.backend.value, "output": output}
            else:
                # Just one line for success
                config_desc = config.description or config.backend.value
                click.secho(f"✅ {rel_path} - {config_desc}", fg="green")

        return 1 if error else 0, failure_info

    def analyze_file(self, file_path: Path) -> TestCaseAnalysis:
        """
        Analyze a single test case file to determine appropriate backends.

        Args:
            file_path: Path to the test case YAML file

        Returns:
            TestCaseAnalysis with backend recommendations
        """
        # Resolve path to absolute
        file_path = file_path.resolve()

        backends = set()
        explicit_backend = None
        reason_parts = []

        # Load YAML content
        content = self._load_yaml(file_path)

        # Check for explicit cluster_backend in content
        if content and "cluster_backend" in content:
            backend_value = content["cluster_backend"].lower()
            try:
                explicit_backend = Backend(backend_value)
                backends.add(explicit_backend)
                reason_parts.append(f"explicit backend: {backend_value}")
            except ValueError:
                # Ignore invalid backend values; not all test cases specify a valid backend.
                pass

        # Analyze filename patterns
        filename = file_path.name.lower()

        # Get relative path for pattern matching
        try:
            filepath_str = str(file_path.relative_to(self.test_cases_dir)).lower()
        except ValueError:
            # If not in test_cases_dir, use the filename
            filepath_str = filename

        # Check for backend-specific patterns in filename
        for backend, patterns in self.FILENAME_PATTERNS.items():
            if any(re.search(pattern, filepath_str) for pattern in patterns):
                backends.add(backend)
                reason_parts.append(f"filename pattern: {backend.value}")

        # Check for multi-DC patterns
        if dc_count := self._get_dc_count(content):
            reason_parts.append(f"multi-DC with {dc_count} DCs")

        # Check content for backend indicators
        has_k8s_config = False
        has_aws_config = False
        has_gce_config = False

        if content and not backends:
            for backend, keys in self.CONTENT_BACKEND_KEYS.items():
                if any(key in content for key in keys):
                    backends.add(backend)
                    reason_parts.append(f"content has {backend.value} config")

                    # Track specific backend types for K8S detection
                    if backend == Backend.K8S_LOCAL_KIND:
                        has_k8s_config = True
                    elif backend == Backend.GCE:
                        has_gce_config = True
                    elif backend == Backend.AWS:
                        has_aws_config = True

        # Convert cloud backends to K8S backends when K8S config is present
        if has_k8s_config:
            # Remove K8S_LOCAL_KIND if we have cloud provider configs
            if has_aws_config or has_gce_config:
                backends.discard(Backend.K8S_LOCAL_KIND)

        # If no specific backend identified, assume AWS as default
        if not backends and not explicit_backend:
            backends.add(Backend.AWS)
            reason_parts.append("default backend")

        return TestCaseAnalysis(
            file_path=file_path,
            backends=backends,
            dc_count=dc_count,
            explicit_backend=explicit_backend,
            reason="; ".join(reason_parts),
        )

    def _load_yaml(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Load and parse YAML file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except (OSError, yaml.YAMLError) as e:
            click.secho(f"Warning: Failed to load {file_path}: {e}", fg="yellow", err=True)
            return None

    def _get_dc_count(self, content: Optional[Dict]) -> Optional[int]:
        """Determine the number of datacenters."""
        # Check content
        if content:
            if "n_db_nodes" in content:
                n_db_nodes = str(content["n_db_nodes"])
                if " " in n_db_nodes:
                    return len(n_db_nodes.split())

            if "gce_datacenter" in content:
                gce_dc = content["gce_datacenter"]
                if isinstance(gce_dc, str) and " " in gce_dc:
                    return len(gce_dc.split())
                elif isinstance(gce_dc, list):
                    return len(gce_dc)

        return None

    def get_backend_configs(self, analysis: TestCaseAnalysis) -> List[BackendConfig]:
        """
        Generate backend configurations for linting based on analysis.

        Args:
            analysis: TestCaseAnalysis result

        Returns:
            List of BackendConfig objects for running lint tests
        """
        configs = []

        for backend in analysis.backends:
            if backend == Backend.AWS:
                configs.extend(self._get_aws_configs(analysis))
            elif backend == Backend.AZURE:
                configs.append(self._get_azure_config(analysis))
            elif backend == Backend.GCE:
                configs.extend(self._get_gce_configs(analysis))
            elif backend == Backend.DOCKER:
                configs.append(self._get_docker_config(analysis))
            elif backend == Backend.BAREMETAL:
                configs.append(self._get_baremetal_config(analysis))
            elif backend in [Backend.K8S_LOCAL_KIND, Backend.K8S_GKE, Backend.K8S_EKS]:
                configs.append(self._get_k8s_config(analysis, backend))

        return configs

    def _get_aws_configs(self, analysis: TestCaseAnalysis) -> List[BackendConfig]:
        """Generate AWS backend configurations."""
        configs = []

        if analysis.dc_count:
            # multi DC configuration
            configs.append(
                BackendConfig(
                    backend=Backend.AWS,
                    env_vars={
                        "SCT_AMI_ID_DB_SCYLLA": " ".join(f"ami-{i}" for i in range(analysis.dc_count)),
                        "SCT_REGION_NAME": " ".join(SCTConfiguration.aws_supported_regions[: analysis.dc_count]),
                        "SCT_SCYLLA_REPO": "http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo",
                    },
                    description=f"AWS multi-DC with {analysis.dc_count} regions",
                    multi_region=True,
                    region_count=analysis.dc_count,
                )
            )
        else:
            # Single DC AWS
            configs.append(
                BackendConfig(
                    backend=Backend.AWS,
                    env_vars={
                        "SCT_AMI_ID_DB_SCYLLA": "ami-1234",
                        "SCT_SCYLLA_REPO": "http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo",
                    },
                    description="AWS single region",
                )
            )

        return configs

    def _get_azure_config(self, analysis: TestCaseAnalysis) -> BackendConfig:
        """Generate Azure backend configuration."""
        return BackendConfig(
            backend=Backend.AZURE,
            env_vars={
                "SCT_AZURE_IMAGE_DB": "image",
                "SCT_AZURE_REGION_NAME": "eastus",
            },
            description="Azure",
        )

    def _get_gce_configs(self, analysis: TestCaseAnalysis) -> List[BackendConfig]:
        """Generate GCE backend configurations."""
        configs = []
        base_env = {
            "SCT_GCE_IMAGE_DB": "image",
            "SCT_SCYLLA_REPO": "http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo",
        }

        if analysis.dc_count:
            env = base_env.copy()
            env["SCT_GCE_DATACENTER"] = " ".join(["us-east1", "us-west1", "eu-north1"][0: analysis.dc_count])
            configs.append(
                BackendConfig(
                    backend=Backend.GCE,
                    env_vars=env,
                    description=f"GCE multi-DC with {analysis.dc_count} regions",
                    multi_region=True,
                    region_count=analysis.dc_count,
                )
            )
        else:
            # Single region GCE
            configs.append(
                BackendConfig(
                    backend=Backend.GCE,
                    env_vars=base_env,
                    description="GCE single region",
                )
            )

        return configs

    def _get_docker_config(self, analysis: TestCaseAnalysis) -> BackendConfig:
        """Generate Docker backend configuration."""
        return BackendConfig(
            backend=Backend.DOCKER,
            env_vars={"SCT_DOCKER_IMAGE": "scylladb/scylla", "SCT_USE_MGMT": "false"},
            description="Docker",
        )

    def _get_baremetal_config(self, analysis: TestCaseAnalysis) -> BackendConfig:
        """Generate Baremetal backend configuration."""
        return BackendConfig(
            backend=Backend.BAREMETAL,
            env_vars={
                "SCT_DB_NODES_PUBLIC_IP": '["127.0.0.1", "127.0.0.2"]',
                "SCT_SCYLLA_REPO": "http://downloads.scylladb.com.s3.amazonaws.com/rpm/centos/scylla-2021.1.repo",
            },
            description="Baremetal",
        )

    def _get_k8s_config(self, analysis: TestCaseAnalysis, backend: Backend) -> BackendConfig:
        """Generate Kubernetes backend configuration."""
        env_vars = {}
        description = f"Kubernetes ({backend.value})"

        if backend == Backend.K8S_EKS:
            # EKS requires AWS config
            env_vars["SCT_AMI_ID_DB_SCYLLA"] = "ami-1234"
            if analysis.dc_count:
                env_vars["SCT_REGION_NAME"] = " ".join(SCTConfiguration.aws_supported_regions[: analysis.dc_count])
                description = f"Kubernetes EKS multi-DC with {analysis.dc_count} regions"
        elif backend == Backend.K8S_GKE:
            # GKE requires GCE config
            env_vars["SCT_GCE_IMAGE_DB"] = "image"
            if analysis.dc_count:
                env_vars["SCT_GCE_DATACENTER"] = " ".join(["us-east1", "us-west1", "eu-north1"][0: analysis.dc_count])
                description = f"Kubernetes GKE multi-DC with {analysis.dc_count} regions"
        else:
            # K8S_LOCAL_KIND or other K8S variants
            env_vars["SCT_AMI_ID_DB_SCYLLA"] = "ami-1234"

        return BackendConfig(
            backend=backend,
            env_vars=env_vars,
            description=description,
        )

    def find_all_test_cases(self, pattern: str = "**/*.yaml") -> List[Path]:
        """
        Find all test case YAML files in the test-cases directory.

        Args:
            pattern: Glob pattern for finding files

        Returns:
            List of Path objects for test case files
        """
        return sorted(self.test_cases_dir.glob(pattern))

    def group_by_backend_config(self, test_cases: Optional[List[Path]] = None) -> Dict[str, List[Path]]:
        """
        Group test cases by their backend configuration requirements.

        Args:
            test_cases: List of test case paths, or None to find all

        Returns:
            Dictionary mapping backend config keys to list of test case paths
        """
        if test_cases is None:
            test_cases = self.find_all_test_cases()

        grouped = {}

        for test_case in test_cases:
            analysis = self.analyze_file(test_case)
            configs = self.get_backend_configs(analysis)

            for config in configs:
                key = self._config_key(config)
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(test_case)

        return grouped

    def _config_key(self, config: BackendConfig) -> str:
        """Generate a unique key for a backend configuration."""
        parts = [config.backend.value]
        if config.multi_region:
            parts.append(f"regions-{config.region_count}")
        return "-".join(parts)

    def run_tests_for_file(
        self,
        file_path: Path,
        verbose: bool = True,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Analyze a test case file and run appropriate YAML validation tests.

        Args:
            file_path: Path to test case file
            verbose: If True, print detailed output

        Returns:
            Tuple of (total_exit_code, list of failure info dicts)
        """
        analysis = self.analyze_file(file_path)
        configs = self.get_backend_configs(analysis)

        total_exit_code = 0
        failures = []

        for config in configs:
            exit_code, failure_info = self.run_yaml_test_for_config(config, file_path, verbose=verbose)
            total_exit_code += exit_code
            if failure_info:
                failures.append(failure_info)

        return total_exit_code, failures


def _worker_process_file(file_path: Path) -> Tuple[int, List[Dict[str, Any]], Path]:
    """
    Worker function to process a single test file in a separate process.

    This function is designed to be called by multiprocessing.Pool and must be
    at module level (not a method) to be picklable.

    Args:
        file_path: Path to test case file

    Returns:
        Tuple of (exit_code, failures, file_path)
    """
    try:
        analyzer = TestCaseAnalyzer()
        exit_code, failures = analyzer.run_tests_for_file(file_path, verbose=True)
        return exit_code, failures, file_path
    except Exception as e:  # noqa: BLE001
        # If something goes wrong, return error info
        error_msg = f"Worker process error: {str(e)}\n{traceback.format_exc()}"
        failure_info = {"file": str(file_path), "config": "process-error", "output": [error_msg]}
        return 1, [failure_info], file_path


def _worker_analyze_file(file_path: Path) -> Tuple[Path, TestCaseAnalysis]:
    """
    Worker function to analyze a single test file in a separate process.

    This function is designed to be called by multiprocessing.Pool for
    non-test analysis mode.

    Args:
        file_path: Path to test case file

    Returns:
        Tuple of (file_path, analysis_result)
    """
    try:
        analyzer = TestCaseAnalyzer()
        analysis = analyzer.analyze_file(file_path)
        return file_path, analysis
    except Exception as e:  # noqa: BLE001
        # If something goes wrong, create a minimal analysis result
        error_analysis = TestCaseAnalysis(file_path=file_path, backends=set(), reason=f"Analysis error: {str(e)}")
        return file_path, error_analysis


def _display_test_failure(failure: Dict[str, Any], file_path: Path) -> None:
    """Display a test failure with details."""
    click.echo(f"\n{'=' * 60}")
    click.secho(f"❌ {failure.get('file', file_path)} - {failure.get('config', 'unknown')}", fg="red", bold=True)
    click.echo(f"{'=' * 60}")

    # Show output
    for line in failure.get("output", []):
        click.echo(line)
    click.echo("")  # Empty line after failure details


def _display_failure_summary(all_failures: List[Dict[str, Any]]) -> None:
    """Display a summary of all test failures."""
    click.echo(f"\n{'=' * 70}")
    click.secho("FAILURE SUMMARY:", fg="red", bold=True)
    click.echo(f"{'=' * 70}")

    for i, failure in enumerate(all_failures, 1):
        click.secho(f"\n{i}. {failure['file']} - {failure['config']}", fg="red", bold=True)

        # Find and display the error message (last non-empty line or exception)
        error_lines = [line for line in failure["output"] if line.strip()]
        if error_lines:
            # Try to find the actual error message
            for line in reversed(error_lines):
                if any(keyword in line for keyword in ["Error:", "Exception:", "Failed", "ERROR"]):
                    click.echo(f"   Error: {line.strip()}")
                    break
            else:
                # If no error keyword found, show last line
                click.echo(f"   {error_lines[-1].strip()}")

    click.echo(f"\n{'=' * 70}")
    click.secho(f"Total failures: {len(all_failures)}", fg="red", bold=True)
    click.echo(f"{'=' * 70}")


def _process_tests_sequential(analyzer: TestCaseAnalyzer, test_cases: List[Path]) -> Tuple[int, List[Dict[str, Any]]]:
    """Process tests sequentially."""
    total_exit_code = 0
    all_failures = []

    for test_case in test_cases:
        exit_code, failures = analyzer.run_tests_for_file(test_case, verbose=True)
        total_exit_code += exit_code
        all_failures.extend(failures)

    return total_exit_code, all_failures


def _process_tests_parallel(analyzer: TestCaseAnalyzer, test_cases: List[Path], workers: int) -> Tuple[int, List[Dict[str, Any]]]:
    """Process tests in parallel."""
    total_exit_code = 0
    all_failures = []

    click.echo(f"Processing {len(test_cases)} files with {workers} workers...")
    click.echo("")  # Empty line for better readability

    with multiprocessing.Pool(processes=workers) as pool:
        # Process files in parallel and show results as they complete
        for exit_code, failures, file_path in pool.imap_unordered(_worker_process_file, test_cases):
            total_exit_code += exit_code
            all_failures.extend(failures)

    return total_exit_code, all_failures


def _run_test_mode(analyzer: TestCaseAnalyzer, test_cases: List[Path], workers: Optional[int]) -> int:
    """Run test validation mode."""
    # Determine number of workers
    if workers is None:
        workers = multiprocessing.cpu_count()

    # Use single process if workers=1 or only one test case
    if workers == 1 or len(test_cases) == 1:
        total_exit_code, all_failures = _process_tests_sequential(analyzer, test_cases)
    else:
        total_exit_code, all_failures = _process_tests_parallel(analyzer, test_cases, workers)

    # Display results
    click.echo(f"\n{'=' * 70}")
    if total_exit_code == 0:
        click.secho("✅ All tests passed!", fg="green", bold=True)
    else:
        click.secho(f"❌ Tests failed: {total_exit_code}", fg="red", bold=True)
        if all_failures:
            _display_failure_summary(all_failures)

    return total_exit_code


def _run_group_mode(analyzer: TestCaseAnalyzer, test_cases: List[Path]) -> None:
    """Run grouping mode to show test cases grouped by backend."""
    grouped = analyzer.group_by_backend_config(test_cases)

    for config_key, config_files in grouped.items():
        click.echo(f"\n{config_key}:")
        for file in config_files:
            click.echo(f"  - {file.relative_to(analyzer.test_cases_dir)}")


def _display_analysis_result(file_path: Path, analysis: TestCaseAnalysis, analyzer: TestCaseAnalyzer, verbose: bool) -> None:
    """Display analysis result for a single file."""
    # Get relative path for display
    try:
        rel_path = file_path.resolve().relative_to(analyzer.test_cases_dir)
    except ValueError:
        rel_path = file_path

    click.echo(f"\n{rel_path}")
    click.echo(f"  Backends: {', '.join(b.value for b in analysis.backends)}")

    if verbose:
        if analysis.dc_count:
            click.echo(f"  DC Count: {analysis.dc_count}")
        if analysis.explicit_backend:
            click.echo(f"  Explicit Backend: {analysis.explicit_backend.value}")
        click.echo(f"  Reason: {analysis.reason}")

        configs = analyzer.get_backend_configs(analysis)
        for config in configs:
            click.echo(f"\n  Config: {config.description}")
            click.echo(f"    Backend: {config.backend.value}")


def _run_analysis_mode(analyzer: TestCaseAnalyzer, test_cases: List[Path], workers: Optional[int], verbose: bool) -> None:
    """Run analysis mode to show backend recommendations."""
    # Determine number of workers for analysis
    if workers is None:
        workers = multiprocessing.cpu_count()

    # Use parallel processing for analysis if multiple files and workers > 1
    if workers > 1 and len(test_cases) > 1:
        click.echo(f"Analyzing {len(test_cases)} files with {workers} workers...")
        click.echo("")  # Empty line for better readability

        with multiprocessing.Pool(processes=workers) as pool:
            # Display results as they complete
            for file_path, analysis in pool.imap_unordered(_worker_analyze_file, test_cases):
                _display_analysis_result(file_path, analysis, analyzer, verbose)
    else:
        # Sequential processing for single file or workers=1
        for test_case in test_cases:
            analysis = analyzer.analyze_file(test_case)
            _display_analysis_result(test_case, analysis, analyzer, verbose)


@click.command("lint-yamls", help="Test yaml in test-cases directory")
@click.argument("files", nargs=-1, type=click.Path(exists=True, path_type=Path))
@click.option("--group", is_flag=True, help="Group test cases by backend configuration")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed analysis")
@click.option("--run", "run_tests", is_flag=True, help="Run YAML validation tests for the analyzed files")
@click.option("--test", "run_tests_alias", is_flag=True, help="Alias for --run (run YAML validation tests)")
@click.option("--workers", "-j", type=int, default=None, help="Number of parallel workers (default: CPU count)")
def main(files, group, verbose, run_tests, run_tests_alias, workers):
    """Analyze test case files and determine appropriate backends.

    FILES: Test case files to analyze (default: all in test-cases/)
    """
    analyzer = TestCaseAnalyzer()

    if files:
        test_cases = list(files)
    else:
        test_cases = analyzer.find_all_test_cases()

    # Handle test execution
    if run_tests or run_tests_alias:
        exit_code = _run_test_mode(analyzer, test_cases, workers)
        sys.exit(exit_code)
    elif group:
        _run_group_mode(analyzer, test_cases)
    else:
        _run_analysis_mode(analyzer, test_cases, workers, verbose)


if __name__ == "__main__":
    main()
