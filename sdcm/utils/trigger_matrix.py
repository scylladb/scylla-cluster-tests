"""
Tier1 test job triggering utilities.

This module provides functionality to trigger tests from a YAML matrix definition.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field

import yaml
import click
import requests
import jenkins
from jenkins import JenkinsException
import boto3
from google.cloud import compute_v1

from sdcm.keystore import KeyStore
from sdcm.utils.aws_utils import AwsArchType, get_arch_from_instance_type, aws_arch_to_vm_arch
from sdcm.utils.common import get_ami_images, find_equivalent_ami, get_gce_images, get_ami_tags, get_branched_gce_images
import sdcm.provision.azure.utils as azure_utils
from sdcm.provision.azure.utils import get_image_tags
from sdcm.utils.common import get_scylla_images_ec2_resource


@dataclass
class JobConfig:
    """Configuration for a single test job from the matrix YAML."""

    job_name: str
    backend: str
    region: str
    exclude_versions: List[str] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    test_config: Optional[str] = None
    params: Dict[str, any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict) -> "JobConfig":
        """Create JobConfig from dictionary (YAML data)."""
        return cls(
            job_name=data["job_name"],
            backend=data["backend"],
            region=data["region"],
            exclude_versions=data.get("exclude_versions", []),
            labels=data.get("labels", []),
            test_config=data.get("test_config"),
            params=data.get("params", {}),
        )

    def to_dict(self) -> Dict:
        """Convert JobConfig back to dictionary format."""
        return {
            "job_name": self.job_name,
            "backend": self.backend,
            "region": self.region,
            "exclude_versions": self.exclude_versions,
            "labels": self.labels,
            "test_config": self.test_config,
            "params": self.params,
        }


@dataclass
class CronTriggerConfig:
    """Configuration for cron-based triggering."""

    schedule: str
    params: Dict[str, str]

    @classmethod
    def from_dict(cls, data: Dict) -> "CronTriggerConfig":
        """Create CronTriggerConfig from dictionary (YAML data)."""
        return cls(schedule=data["schedule"], params=data.get("params", {}))


@dataclass
class MatrixConfig:
    """Complete matrix configuration from YAML file."""

    jobs: List[JobConfig]
    cron_triggers: List[CronTriggerConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict) -> "MatrixConfig":
        """Create MatrixConfig from dictionary (YAML data)."""
        jobs = [JobConfig.from_dict(job_data) for job_data in data["jobs"]]
        cron_triggers = [CronTriggerConfig.from_dict(cron_data) for cron_data in data.get("cron_triggers", [])]
        return cls(jobs=jobs, cron_triggers=cron_triggers)


def load_matrix_config(matrix_file: str) -> MatrixConfig:
    """Load matrix configuration from YAML file.

    Args:
        matrix_file: Path to the YAML matrix file

    Returns:
        MatrixConfig object with parsed and validated configuration

    Raises:
        FileNotFoundError: If the matrix file doesn't exist
        ValueError: If the YAML structure is invalid
    """
    matrix_path = Path(matrix_file)
    if not matrix_path.exists():
        raise FileNotFoundError(f"Matrix file not found: {matrix_file}")

    with open(matrix_path, "r") as f:
        data = yaml.safe_load(f)

    if "jobs" not in data:
        raise ValueError("Matrix file must contain 'jobs' key")

    try:
        return MatrixConfig.from_dict(data)
    except (KeyError, TypeError) as e:
        raise ValueError(f"Invalid matrix YAML structure: {e}") from e


def determine_job_folder(scylla_version: str, explicit_folder: Optional[str]) -> str:
    """
    Determine job folder based on scylla_version.

    Args:
        scylla_version: Scylla version (e.g., 'master', '2025.4')
        explicit_folder: Explicitly specified folder (takes precedence)

    Returns:
        Job folder string (e.g., 'scylla-master', 'branch-2025.4')
    """
    if explicit_folder:
        return explicit_folder

    # Handle master:latest
    version = scylla_version or "master"
    if version in {"master:latest", "master"}:
        return "scylla-master"

    # Extract version prefix for release branches (e.g., "2025.4" -> "branch-2025.4")
    version_match = re.match(r"^(\d+\.\d+)", version)
    if version_match:
        return f"branch-{version_match.group(1)}"

    # Fallback to master
    return "scylla-master"


def fetch_ami_for_master(backend: str, region: str, arch: AwsArchType = "x86_64") -> Optional[str]:
    """
    Fetch the latest AMI for master builds using internal Python functions.

    Args:
        backend: Cloud backend (aws, azure, gce)
        region: Cloud region
        arch: Architecture (x86_64 or arm64), default x86_64

    Returns:
        AMI ID or None if not applicable/found
    """
    if backend != "aws":
        return None

    try:
        # Call internal get_ami_images function directly
        rows = get_ami_images(branch="master:latest", region=region, arch=arch)

        if rows:
            # Row format: ["Backend", "Name", "ImageId", "CreationDate", "Name Tag", "BuildId", "Arch", "ScyllaVersion", "OwnerId"]
            # ImageId is at index 2
            return rows[0][2]
    except (IndexError, KeyError, ValueError, TypeError) as e:
        click.echo(f"Warning: Could not fetch AMI: {e}", err=True)

    return None


def fetch_gce_image_for_master(backend: str, arch: AwsArchType = "x86_64") -> Optional[str]:
    """
    Fetch the latest GCE image for master builds using internal Python functions.

    Args:
        backend: Cloud backend (aws, azure, gce)
        arch: Architecture (x86_64 or arm64), default x86_64

    Returns:
        GCE image self_link or None if not applicable/found
    """
    if backend != "gce":
        return None

    try:
        # Call internal get_gce_images function directly
        rows = get_gce_images(branch="master:latest", arch=arch)

        if rows:
            # Row format: ["Backend", "Name", "ImageId (self_link)", "CreationDate", "BuildId", "Arch", "ScyllaVersion"]
            # ImageId/self_link is at index 2
            return rows[0][2]
    except (IndexError, KeyError, ValueError, TypeError) as e:
        click.echo(f"Warning: Could not fetch GCE image: {e}", err=True)

    return None


def fetch_azure_image_for_master(backend: str, region: str, arch: AwsArchType = "x86_64") -> Optional[str]:
    """
    Fetch the latest Azure image for master builds using internal Python functions.

    Args:
        backend: Cloud backend (aws, azure, gce)
        region: Azure region
        arch: Architecture (x86_64 or arm64), default x86_64

    Returns:
        Azure image ID or None if not applicable/found
    """
    if backend != "azure":
        return None

    try:
        # Convert AwsArchType to VmArch
        vm_arch = aws_arch_to_vm_arch(arch)

        # Call internal get_scylla_images function directly
        images = azure_utils.get_scylla_images(scylla_version="master:latest", region_name=region, arch=vm_arch)

        if images:
            # Return the image ID (image.id) of the first (latest) image
            return images[0].id
    except (IndexError, KeyError, ValueError, TypeError, AttributeError) as e:
        click.echo(f"Warning: Could not fetch Azure image: {e}", err=True)

    return None


def fetch_arm64_ami(x86_ami: str, source_region: str, target_region: str) -> Optional[str]:
    """
    Find ARM64 equivalent AMI using internal Python function.

    Args:
        x86_ami: Source x86_64 AMI ID
        source_region: Source AWS region
        target_region: Target AWS region

    Returns:
        ARM64 AMI ID or None if not found
    """
    try:
        # Call internal find_equivalent_ami function directly
        results = find_equivalent_ami(
            ami_id=x86_ami,
            source_region=source_region,
            target_regions=[target_region] if target_region != source_region else None,
            target_arch="arm64",
        )

        if results:
            # Result format: list of dicts with 'ami_id', 'region', 'architecture', etc.
            # Return the first ARM64 AMI found for the target region
            for result in results:
                if result["region"] == target_region and result["architecture"] == "arm64":
                    return result["ami_id"]
    except (IndexError, KeyError, ValueError, TypeError) as e:
        click.echo(f"Warning: Could not find ARM64 AMI: {e}", err=True)

    return None


def get_ami_architecture(ami_id: str, region: str = "us-east-1") -> Optional[AwsArchType]:
    """Get the architecture of an AMI from AWS EC2 API."""
    try:
        for ec2_resource in (
            boto3.resource("ec2", region_name=region),
            get_scylla_images_ec2_resource(region_name=region),
        ):
            try:
                ami = ec2_resource.Image(ami_id)
                ami.load()
                if hasattr(ami, "architecture") and ami.architecture:
                    arch = ami.architecture
                    click.echo(f"  Detected AMI architecture: {arch}")
                    return arch
            except (KeyError, AttributeError, TypeError):
                continue
    except (KeyError, AttributeError, TypeError, ValueError) as e:
        click.echo(f"Warning: Could not detect architecture for AMI {ami_id}: {e}", err=True)

    return None


def extract_scylla_version_from_ami(ami_id: str, region: str = "us-east-1") -> Optional[str]:
    """Extract Scylla version from AMI tags."""
    try:
        for ec2_resource in (
            boto3.resource("ec2", region_name=region),
            get_scylla_images_ec2_resource(region_name=region),
        ):
            try:
                ami = ec2_resource.Image(ami_id)
                ami.load()
                if ami.tags:
                    for tag in ami.tags:
                        if tag.get("Key") == "ScyllaVersion":
                            version = tag.get("Value")
                            click.echo(f"  Extracted version from AMI tags: {version}")
                            return version
            except (KeyError, AttributeError, TypeError):
                continue
    except (KeyError, AttributeError, TypeError, ValueError) as e:
        click.echo(f"Warning: Could not extract version from AMI {ami_id}: {e}", err=True)

    return None


def find_equivalent_gce_image(ami_id: str, region: str, arch: AwsArchType = "x86_64") -> Optional[str]:
    """
    Find equivalent GCE image based on AMI tags (build-id, branch, ScyllaVersion).

    Args:
        ami_id: Source AWS AMI ID
        region: AWS region where AMI is located
        arch: Architecture (x86_64 or arm64)

    Returns:
        GCE image self_link or None if not found
    """
    try:
        # Get AMI tags
        ami_tags = get_ami_tags(ami_id, region)
        if not ami_tags:
            click.echo(f"Warning: Could not get tags for AMI {ami_id}", err=True)
            return None

        # Extract key information
        build_id = ami_tags.get("build-id") or ami_tags.get("build_id")
        branch = ami_tags.get("branch")

        if not branch:
            click.echo(f"Warning: AMI {ami_id} has no branch tag", err=True)
            return None

        # Construct version string for GCE search
        if build_id:
            version_str = f"{branch}:{build_id}"
        else:
            version_str = f"{branch}:latest"

        click.echo(f"  Searching for GCE image with version={version_str}, arch={arch}")

        # Search for GCE images
        gce_images = get_branched_gce_images(scylla_version=version_str, arch=arch)

        if gce_images:
            gce_image = gce_images[0]  # Take the latest
            click.echo(f"  Found equivalent GCE image: {gce_image.name}")
            return gce_image.self_link
        else:
            click.echo(f"  No matching GCE image found for {version_str}", err=True)
            return None

    except (KeyError, AttributeError, TypeError, ValueError, IndexError) as e:
        click.echo(f"Warning: Could not find equivalent GCE image for AMI {ami_id}: {e}", err=True)
        return None


def find_equivalent_azure_image(
    ami_id: str, region: str, target_region: str, arch: AwsArchType = "x86_64"
) -> Optional[str]:
    """
    Find equivalent Azure image based on AMI tags (build-id, branch, ScyllaVersion).

    Args:
        ami_id: Source AWS AMI ID
        region: AWS region where AMI is located
        target_region: Azure region to search in
        arch: Architecture (x86_64 or arm64)

    Returns:
        Azure image ID or None if not found
    """
    try:
        # Get AMI tags
        ami_tags = get_ami_tags(ami_id, region)
        if not ami_tags:
            click.echo(f"Warning: Could not get tags for AMI {ami_id}", err=True)
            return None

        # Extract key information
        build_id = ami_tags.get("build-id") or ami_tags.get("build_id")
        branch = ami_tags.get("branch")

        if not branch:
            click.echo(f"Warning: AMI {ami_id} has no branch tag", err=True)
            return None

        # Construct version string for Azure search
        if build_id:
            version_str = f"{branch}:{build_id}"
        else:
            version_str = f"{branch}:latest"

        click.echo(f"  Searching for Azure image with version={version_str}, arch={arch}, region={target_region}")

        # Convert arch to VmArch
        vm_arch = aws_arch_to_vm_arch(arch)

        # Search for Azure images
        azure_images = azure_utils.get_scylla_images(
            scylla_version=version_str, region_name=target_region, arch=vm_arch
        )

        if azure_images:
            azure_image = azure_images[0]  # Take the latest
            click.echo(f"  Found equivalent Azure image: {azure_image.name}")
            return azure_image.id
        else:
            click.echo(f"  No matching Azure image found for {version_str}", err=True)
            return None

    except (KeyError, AttributeError, TypeError, ValueError, IndexError) as e:
        click.echo(f"Warning: Could not find equivalent Azure image for AMI {ami_id}: {e}", err=True)
        return None


def extract_scylla_version_from_gce_image(image_link: str) -> Optional[str]:
    """
    Extract Scylla version from GCE image labels.

    Args:
        image_link: GCE image self_link

    Returns:
        Scylla version string or None if not found
    """
    try:
        # Parse image link to get project and image name
        # Format: https://www.googleapis.com/compute/v1/projects/{project}/global/images/{image}
        match = re.search(r"/projects/([^/]+)/global/images/([^/]+)", image_link)
        if not match:
            click.echo(f"Warning: Could not parse GCE image link: {image_link}", err=True)
            return None

        project_id = match.group(1)
        image_name = match.group(2)

        # Get image details
        client = compute_v1.ImagesClient()
        image = client.get(project=project_id, image=image_name)

        if image.labels and "scylla_version" in image.labels:
            version = image.labels["scylla_version"]
            click.echo(f"  Extracted version from GCE image labels: {version}")
            return version

    except (KeyError, AttributeError, TypeError, ValueError) as e:
        click.echo(f"Warning: Could not extract version from GCE image {image_link}: {e}", err=True)

    return None


def extract_scylla_version_from_azure_image(image_id: str, region: str = "eastus") -> Optional[str]:
    """
    Extract Scylla version from Azure image tags.

    Args:
        image_id: Azure image resource ID
        region: Azure region

    Returns:
        Scylla version string or None if not found
    """
    try:
        tags = get_image_tags(image_id)
        if tags:
            version = tags.get("scylla_version") or tags.get("ScyllaVersion")
            if version:
                click.echo(f"  Extracted version from Azure image tags: {version}")
                return version

    except (KeyError, AttributeError, TypeError, ValueError) as e:
        click.echo(f"Warning: Could not extract version from Azure image {image_id}: {e}", err=True)

    return None


def get_image_backend(
    ami_id: Optional[str] = None, gce_image: Optional[str] = None, azure_image: Optional[str] = None
) -> Optional[str]:
    """
    Detect which backend an image belongs to.

    Args:
        ami_id: AWS AMI ID
        gce_image: GCE image self_link
        azure_image: Azure image resource ID

    Returns:
        Backend name ('aws', 'gce', 'azure') or None if no image provided
    """
    if ami_id:
        return "aws"
    if gce_image:
        return "gce"
    if azure_image:
        return "azure"
    return None


def get_image_architecture(
    ami_id: Optional[str] = None,
    gce_image: Optional[str] = None,
    azure_image: Optional[str] = None,
    region: str = "us-east-1",
) -> Optional[AwsArchType]:
    """
    Detect the architecture of a provided image.

    Currently only supports AWS AMI architecture detection.
    For GCE and Azure, returns None (would need to implement similar detection).

    Args:
        ami_id: AWS AMI ID
        gce_image: GCE image self_link
        azure_image: Azure image resource ID
        region: AWS region for AMI lookup

    Returns:
        Architecture type (x86_64 or arm64) or None if not detected
    """
    if ami_id:
        return get_ami_architecture(ami_id, region)

    # TODO: Implement GCE and Azure architecture detection if needed
    # For now, return None for non-AWS images
    if gce_image or azure_image:
        click.echo("  Note: Architecture detection not yet implemented for GCE/Azure images", err=True)
        return None

    return None


def extract_version_from_images(
    ami_id: Optional[str] = None, gce_image: Optional[str] = None, azure_image: Optional[str] = None
) -> Optional[str]:
    """
    Extract Scylla version from any provided backend image.

    Tries to extract from the first available image in order: AMI, GCE, Azure.

    Args:
        ami_id: AWS AMI ID
        gce_image: GCE image self_link
        azure_image: Azure image resource ID

    Returns:
        Extracted Scylla version or None if not found
    """
    if ami_id:
        version = extract_scylla_version_from_ami(ami_id)
        if version:
            return version

    if gce_image:
        version = extract_scylla_version_from_gce_image(gce_image)
        if version:
            return version

    if azure_image:
        version = extract_scylla_version_from_azure_image(azure_image)
        if version:
            return version

    return None


def detect_job_architecture(job: JobConfig) -> Optional[AwsArchType]:
    """
    Detect the architecture of a job from its test_config.

    Only works for AWS jobs where instance_type_db is specified in test config.

    Args:
        job: Job configuration from matrix

    Returns:
        Architecture type (x86_64 or arm64) or None if not detected
    """
    if job.backend != "aws":
        return None  # Only AWS jobs have instance types

    test_config_path = job.test_config
    if not test_config_path:
        return None

    # Parse test_config - could be a single file path or JSON list of files
    config_files = []
    try:
        # Try parsing as JSON list first
        parsed = json.loads(test_config_path)
        if isinstance(parsed, list):
            config_files = parsed
        else:
            config_files = [test_config_path]
    except (json.JSONDecodeError, TypeError):
        # Not JSON, treat as single file path
        config_files = [test_config_path]

    # Load and merge all config files (later files override earlier ones)
    merged_config = {}
    repo_root = Path.cwd()  # Assume we're running from repo root

    for config_file in config_files:
        # Try both absolute and relative paths
        config_path = Path(config_file)
        if not config_path.is_absolute():
            config_path = repo_root / config_file

        if not config_path.exists():
            # Silently return None - file might not exist in this environment
            return None

        with open(config_path, "r") as f:
            file_data = yaml.safe_load(f) or {}
            merged_config.update(file_data)  # Later files override earlier

    # Extract instance_type_db from merged configuration
    instance_type_db = merged_config.get("instance_type_db")
    if instance_type_db:
        region = job.region
        # Deduce architecture from instance type
        arch = get_arch_from_instance_type(instance_type_db, region)
        return arch

    return None


def filter_matching_jobs(
    matrix: MatrixConfig,
    scylla_version: str,
    labels_selector: Optional[str] = None,
    backend: Optional[str] = None,
    skip_jobs: Optional[str] = None,
) -> List[JobConfig]:
    """
    Filter jobs from matrix based on version, labels, backend, and skip list.

    By default, all jobs run on all versions unless explicitly excluded.

    Args:
        matrix: Loaded matrix configuration
        scylla_version: Target Scylla version
        labels_selector: Label to filter by (optional)
        backend: Backend to filter by (e.g., aws, gce, azure, docker) (optional)
        skip_jobs: Comma-separated list of job names to skip (optional)

    Returns:
        List of matching job configurations
    """
    matching_jobs = []
    skip_list = [j.strip() for j in skip_jobs.split(",")] if skip_jobs else []

    # Normalize version
    version = scylla_version.replace(":latest", "")

    for job in matrix.jobs:
        job_name = job.job_name

        # Check skip list
        if any(skip in job_name for skip in skip_list):
            click.echo(f"Skipping job: {job_name} (in skip list)")
            continue

        # Check backend filter
        if backend:
            if job.backend != backend:
                click.echo(f"Skipping job: {job_name} (backend {job.backend} != {backend})")
                continue

        # Check if version is excluded
        version_excluded = False

        for excluded_version in job.exclude_versions:
            if version == excluded_version or version.startswith(f"{excluded_version}."):
                version_excluded = True
                break

        if version_excluded:
            click.echo(f"Skipping job: {job_name} (version {version} in exclude_versions {job.exclude_versions})")
            continue

        # Check labels if specified
        if labels_selector:
            if labels_selector not in job.labels:
                click.echo(f"Skipping job: {job_name} (label {labels_selector} not in {job.labels})")
                continue

        matching_jobs.append(job)

    return matching_jobs


def build_job_parameters(  # noqa: PLR0912, PLR0914
    job: JobConfig,
    scylla_version: str,
    scylla_repo: Optional[str] = None,
    ami_id: Optional[str] = None,
    gce_image: Optional[str] = None,
    azure_image: Optional[str] = None,
    update_db_packages: Optional[str] = None,
    job_folder: Optional[str] = None,
    use_job_throttling: Optional[bool] = None,
    requested_by_user: Optional[str] = None,
    stress_duration: Optional[str] = None,
) -> Dict:
    """
    Build Jenkins job parameters for a specific job.

    Args:
        job: Job configuration from matrix
        scylla_version: Scylla version
        scylla_repo: Optional Scylla repo URL
        ami_id: Optional AMI ID (for AWS)
        gce_image: Optional GCE image self_link (for GCE)
        azure_image: Optional Azure image ID (for Azure)
        update_db_packages: Cloud path for RPMs, s3:// or gs://
        job_folder: Job folder prefix
        use_job_throttling: Whether to throttle jobs
        requested_by_user: User requesting the build
        stress_duration: Stress duration to override the default test duration

    Returns:
        Dictionary of Jenkins job parameters
    """
    backend = job.backend
    region = job.region

    # Deduce architecture from test_config instance_type_db (only for AWS)
    arch: AwsArchType = "x86_64"  # Default architecture
    if backend == "aws":
        test_config_path = job.test_config
        if test_config_path:
            try:
                # Parse test_config - could be a single file path or JSON list of files
                config_files = []
                try:
                    # Try parsing as JSON list first
                    parsed = json.loads(test_config_path)
                    if isinstance(parsed, list):
                        config_files = parsed
                    else:
                        config_files = [test_config_path]
                except (json.JSONDecodeError, TypeError):
                    # Not JSON, treat as single file path
                    config_files = [test_config_path]

                # Load and merge all config files (later files override earlier ones)
                merged_config = {}
                repo_root = Path.cwd()  # Assume we're running from repo root

                for config_file in config_files:
                    # Try both absolute and relative paths
                    config_path = Path(config_file)
                    if not config_path.is_absolute():
                        config_path = repo_root / config_file

                    if not config_path.exists():
                        error_msg = f"Test config file not found: {config_file} (tried {config_path})"
                        click.echo(f"ERROR: {error_msg}", err=True)
                        raise FileNotFoundError(error_msg)

                    with open(config_path, "r") as f:
                        file_data = yaml.safe_load(f) or {}
                        merged_config.update(file_data)  # Later files override earlier

                # Extract instance_type_db from merged configuration
                instance_type_db = merged_config.get("instance_type_db")
                if instance_type_db:
                    # Deduce architecture from instance type
                    arch = get_arch_from_instance_type(instance_type_db, region if region else "us-east-1")
                    click.echo(f"Deduced architecture from instance_type_db '{instance_type_db}': {arch}")
                else:
                    click.echo("Warning: No instance_type_db found in test config, using default x86_64", err=True)

            except (FileNotFoundError, yaml.YAMLError) as e:
                # These are critical errors - fail the job
                error_msg = f"Failed to load test_config or extract instance_type_db: {e}"
                click.echo(f"ERROR: {error_msg}", err=True)
                raise RuntimeError(error_msg) from e
            except Exception as e:
                # Unexpected errors should also fail the job
                error_msg = f"Unexpected error processing test_config: {e}"
                click.echo(f"ERROR: {error_msg}", err=True)
                raise RuntimeError(error_msg) from e

    # Replace job folder placeholder
    job_name = job.job_name.replace("{JOB_FOLDER}", job_folder)

    params = {
        "provision_type": "on_demand",
        "use_job_throttling": use_job_throttling,
        "requested_by_user": requested_by_user,
        "post_behavior_db_nodes": "destroy",
        "post_behavior_monitor_nodes": "destroy",
    }
    params.update(job.params)

    # Add stress_duration if provided
    if stress_duration:
        params["stress_duration"] = stress_duration

    # Handle ARM64 AMI if needed
    final_ami = ami_id
    if arch == "arm64" and ami_id:
        click.echo(f"Job {job_name} requires ARM64, finding equivalent AMI...")
        source_region = "us-east-1"  # AMI was fetched from us-east-1
        final_ami = fetch_arm64_ami(ami_id, source_region, region or "us-east-1")
        if final_ami:
            click.echo(f"  Found ARM64 AMI: {final_ami}")
        else:
            click.echo(f"  Warning: Could not find ARM64 AMI, using x86_64: {ami_id}")
            raise ValueError(f"Could not find ARM64 AMI for job {job_name}")

    # If AMI is provided, find equivalent images for other backends
    job_gce_image = gce_image
    job_azure_image = azure_image

    if ami_id:
        ami_region = "us-east-1"  # AMI was fetched from us-east-1

        # Find equivalent GCE image if job uses GCE backend and no GCE image is provided
        if backend == "gce" and not gce_image:
            click.echo(f"Job {job_name} uses GCE backend, finding equivalent GCE image for AMI {ami_id}...")
            job_gce_image = find_equivalent_gce_image(ami_id, ami_region, arch)
            if not job_gce_image:
                click.echo(f"  Warning: Could not find equivalent GCE image for AMI {ami_id}")

        # Find equivalent Azure image if job uses Azure backend and no Azure image is provided
        if backend == "azure" and not azure_image:
            azure_region = region or "eastus"
            click.echo(f"Job {job_name} uses Azure backend, finding equivalent Azure image for AMI {ami_id}...")
            job_azure_image = find_equivalent_azure_image(ami_id, ami_region, azure_region, arch)
            if not job_azure_image:
                click.echo(f"  Warning: Could not find equivalent Azure image for AMI {ami_id}")

    # Backend-specific parameters
    if backend == "aws":
        params["scylla_version"] = "" if final_ami else scylla_version
        params["scylla_ami_id"] = final_ami or ""
        params["region"] = region or "us-east-1"
        params["availability_zone"] = "c"
    elif backend == "azure":
        # If no Azure image found yet, try to fetch for master builds
        if not job_azure_image and scylla_version in {"master", "master:latest"} and region:
            # Try to fetch image for this specific region
            job_azure_image = fetch_azure_image_for_master("azure", region, arch)
            if job_azure_image:
                click.echo(f"  Fetched Azure image for region {region}: {job_azure_image}")

        params["scylla_version"] = "" if job_azure_image else scylla_version
        params["azure_image_db"] = job_azure_image or ""
    elif backend == "gce":
        params["scylla_version"] = "" if job_gce_image else scylla_version
        params["gce_image_db"] = job_gce_image or ""
        params["availability_zone"] = "a"

    if scylla_repo:
        params["scylla_repo"] = scylla_repo

    if update_db_packages:
        params["update_db_packages"] = update_db_packages

    return {"job_name": job_name, "parameters": params}


def trigger_jenkins_job(job_name: str, parameters: Dict, dry_run: bool) -> bool:
    """
    Trigger a Jenkins job with parameters.

    Args:
        job_name: Full Jenkins job path
        parameters: Job parameters dictionary
        dry_run: If True, only show what would be done

    Returns:
        True if successful (or dry run), False otherwise
    """
    if dry_run:
        click.echo(f"\n[DRY RUN] Would trigger job: {job_name}")
        click.echo(f"  Parameters: {parameters}")
        return True

    _jenkins = jenkins.Jenkins(**KeyStore().get_json("jenkins.json"))

    try:
        _jenkins.build_job(name=job_name, parameters=parameters)
        click.echo(f"✓ Triggered: {job_name}")
        return True

    except (requests.RequestException, JenkinsException) as e:
        click.echo(f"✗ Error triggering {job_name}: {e}", err=True)
        return False


def trigger_matrix_jobs(  # noqa: PLR0912, PLR0913, PLR0914, PLR0915
    matrix_file: str,
    scylla_version: Optional[str],
    job_folder: Optional[str],  # noqa: PLR0913
    scylla_repo: Optional[str],
    scylla_ami_id: Optional[str],
    azure_image_db: Optional[str],
    gce_image_db: Optional[str],
    update_db_packages: Optional[str],
    labels_selector: Optional[str],
    backend: Optional[str],
    skip_jobs: Optional[str],
    requested_by_user: str,
    use_job_throttling: bool,
    dry_run: bool,
    stress_duration: Optional[str] = None,
):
    """
    Main function to trigger tier1 jobs from matrix.

    This orchestrates the entire process:
    1. Load matrix from YAML
    2. Determine job folder
    3. Fetch AMI/GCE/Azure images for master builds (or use provided ones)
    4. Filter matching jobs
    5. Build parameters for each job
    6. Trigger Jenkins jobs

    Args:
        matrix_file: Path to matrix YAML file
        scylla_version: Scylla version to test (optional if backend images are provided)
        job_folder: Job folder prefix (auto-detected if not provided)
        scylla_repo: Scylla repo URL
        scylla_ami_id: Specific AWS AMI ID (overrides auto-detection)
        azure_image_db: Specific Azure image ID (overrides auto-detection)
        gce_image_db: Specific GCE image self_link (overrides auto-detection)
        update_db_packages: Cloud path for RPMs, s3:// or gs://
        labels_selector: Filter jobs by label
        backend: Filter jobs by backend (e.g., aws, gce, azure, docker)
        skip_jobs: Comma-separated list of job names to skip
        requested_by_user: User requesting the build
        use_job_throttling: Whether to throttle jobs
        dry_run: If True, only show what would be done
        stress_duration: Stress duration to override the default test duration
    """
    click.echo(f"Loading matrix from: {matrix_file}")
    matrix = load_matrix_config(matrix_file)

    # If scylla_version is not provided, try to infer it or default to 'master'
    version_to_use = scylla_version
    click.echo(f"Scylla version: {version_to_use}")

    # Determine job folder
    folder = determine_job_folder(version_to_use, job_folder)
    click.echo(f"Job folder: {folder}")

    # Use provided images or fetch for master builds
    ami_id = scylla_ami_id  # Use provided AMI if given
    gce_image = gce_image_db  # Use provided GCE image if given
    azure_image = azure_image_db  # Use provided Azure image if given

    # Only auto-fetch images if not provided and version is master
    if version_to_use in {"master", "master:latest"}:
        click.echo("Fetching latest images for master builds ...")

        if not ami_id:
            ami_id = fetch_ami_for_master("aws", "us-east-1")
            if ami_id:
                click.echo(f"  AMI: {ami_id}")
            else:
                click.echo("  No AMI fetched (will use version)")
        else:
            click.echo(f"  Using provided AMI: {ami_id}")

        if not gce_image:
            gce_image = fetch_gce_image_for_master("gce")
            if gce_image:
                click.echo(f"  GCE Image: {gce_image}")
            else:
                click.echo("  No GCE image fetched (will use version)")
        else:
            click.echo(f"  Using provided GCE image: {gce_image}")

        if not azure_image:
            azure_image = fetch_azure_image_for_master("azure", "eastus")
            if azure_image:
                click.echo(f"  Azure Image: {azure_image}")
            else:
                click.echo("  No Azure image fetched (will use version)")
        else:
            click.echo(f"  Using provided Azure image: {azure_image}")
    elif ami_id or gce_image or azure_image:
        # If images are provided for non-master versions, just report them
        if ami_id:
            click.echo(f"Using provided AMI: {ami_id}")
        if gce_image:
            click.echo(f"Using provided GCE image: {gce_image}")
        if azure_image:
            click.echo(f"Using provided Azure image: {azure_image}")

    # Extract Scylla version from image tags if version not provided
    if not version_to_use and (ami_id or gce_image or azure_image):
        click.echo("\nExtracting Scylla version from image tags...")
        extracted_version = extract_version_from_images(ami_id, gce_image, azure_image)
        if extracted_version:
            version_to_use = extracted_version
            click.echo(f"Using extracted version: {version_to_use}")
            # Re-determine job folder with extracted version
            folder = determine_job_folder(version_to_use, job_folder)
            click.echo(f"Updated job folder: {folder}")
        else:
            raise Exception("Could not extract version from images provided")

    # Detect backend and architecture from provided images
    image_backend = None
    image_arch = None
    if ami_id or gce_image or azure_image:
        click.echo("\nDetecting image backend and architecture...")
        image_backend = get_image_backend(ami_id, gce_image, azure_image)
        if image_backend:
            click.echo(f"  Detected backend: {image_backend}")

        image_arch = get_image_architecture(ami_id, gce_image, azure_image, region="us-east-1")
        if image_arch:
            click.echo(f"  Detected architecture: {image_arch}")
        else:
            click.echo("  Note: Could not detect architecture from image (will use default x86_64)", err=True)
            image_arch = "x86_64"  # Default to x86_64 if detection fails

    # Filter matching jobs
    click.echo("\nFiltering jobs...")
    matching_jobs = filter_matching_jobs(
        matrix=matrix,
        scylla_version=version_to_use,
        labels_selector=labels_selector,
        backend=backend,
        skip_jobs=skip_jobs,
    )

    # Additional filtering by backend and architecture if image is provided
    if image_backend or image_arch:
        click.echo("Applying additional filtering based on provided image...")
        filtered_jobs = []
        for job in matching_jobs:
            job_backend = job.backend.lower()

            # Filter by backend
            if image_backend and job_backend != image_backend:
                click.echo(
                    f"  Skipping job {job.job_name} - backend mismatch (job: {job_backend}, image: {image_backend})"
                )
                continue

            # Filter by architecture (only for AWS jobs where we can detect arch from instance types)
            if image_arch and job_backend == "aws":
                # Detect job architecture from test_config
                job_arch = detect_job_architecture(job)
                if job_arch and job_arch != image_arch:
                    click.echo(
                        f"  Skipping job {job.job_name} - architecture mismatch (job: {job_arch}, image: {image_arch})"
                    )
                    continue

            filtered_jobs.append(job)

        matching_jobs = filtered_jobs
        click.echo(f"After backend/arch filtering: {len(matching_jobs)} jobs remaining")

    click.echo(f"Found {len(matching_jobs)} matching jobs")

    if not matching_jobs:
        click.echo("No jobs to trigger!")
        return

    # Build parameters and trigger jobs
    click.echo("\nTriggering jobs...")
    success_count = 0
    fail_count = 0

    for job in matching_jobs:
        job_params = build_job_parameters(
            job=job,
            scylla_version=version_to_use,
            scylla_repo=scylla_repo,
            ami_id=ami_id,
            gce_image=gce_image,
            azure_image=azure_image,
            update_db_packages=update_db_packages,
            job_folder=folder,
            use_job_throttling=use_job_throttling,
            requested_by_user=requested_by_user,
            stress_duration=stress_duration,
        )

        success = trigger_jenkins_job(job_params["job_name"], job_params["parameters"], dry_run)

        if success:
            success_count += 1
        else:
            fail_count += 1

    # Summary
    click.echo(f"\n{'=' * 60}")
    click.echo("Summary:")
    click.echo(f"  Total jobs: {len(matching_jobs)}")
    click.echo(f"  Successful: {success_count}")
    click.echo(f"  Failed: {fail_count}")
    click.echo(f"{'=' * 60}")
