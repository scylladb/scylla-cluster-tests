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

"""Unit tests for K8s cluster cleanup functionality."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add the cloud_cleanup directory to the path
cloud_cleanup_path = Path(__file__).parent.parent / "utils" / "cloud_cleanup"
sys.path.insert(0, str(cloud_cleanup_path / "k8s_eks"))
sys.path.insert(0, str(cloud_cleanup_path / "k8s_gke"))

import clean_eks
import clean_gke


@pytest.fixture
def mock_eks_client():
    """Mock EKS boto3 client."""
    with patch("clean_eks.boto3.client") as mock_client:
        mock_eks = MagicMock()
        mock_client.return_value = mock_eks
        yield mock_eks


@pytest.fixture
def mock_eks_cluster():
    """Mock EKS cluster for cleaner."""
    with patch("clean_eks.EksClusterForCleaner") as mock_cluster_class:
        mock_cluster = MagicMock()
        mock_cluster_class.return_value = mock_cluster
        yield mock_cluster


@pytest.fixture
def mock_gke_cleaner():
    """Mock GKE cleaner."""
    with patch("clean_gke.GkeCleaner") as mock_cleaner_class, patch("clean_gke.environment"):
        mock_cleaner = MagicMock()
        mock_cleaner_class.return_value = mock_cleaner
        yield mock_cleaner


@pytest.mark.parametrize(
    "cluster_age_hours,metadata_items,dry_run,should_destroy",
    [
        # Recent cluster should be kept
        (0, [{"key": "TestId", "value": "test-123"}, {"key": "CreatedBy", "value": "SCT"}], False, False),
        # Old cluster should be destroyed
        (20, [{"key": "TestId", "value": "test-456"}, {"key": "CreatedBy", "value": "SCT"}], False, True),
        # Old cluster with dry run should not be destroyed
        (20, [{"key": "CreatedBy", "value": "SCT"}], True, False),
        # Old cluster with keep:alive should not be destroyed
        (
            20,
            [{"key": "keep", "value": "alive"}, {"key": "CreatedBy", "value": "SCT"}],
            False,
            False,
        ),
    ],
    ids=["recent_cluster", "old_cluster", "dry_run", "keep_alive"],
)
def test_clean_eks_clusters(
    mock_eks_client, mock_eks_cluster, cluster_age_hours, metadata_items, dry_run, should_destroy
):
    """Test EKS cluster cleanup with various scenarios."""
    # Setup mock client
    mock_eks_client.list_clusters.return_value = {"clusters": ["test-cluster"]}

    # Setup mock cluster with specified age and metadata
    mock_eks_cluster.create_time = datetime.now(timezone.utc) - timedelta(hours=cluster_age_hours)
    mock_eks_cluster.metadata = {"items": metadata_items}

    # Run cleanup
    clean_eks.clean_eks_clusters(regions=["us-east-1"], dry_run=dry_run)

    # Verify destroy was called or not based on expected behavior
    if should_destroy:
        mock_eks_cluster.destroy.assert_called_once()
    else:
        mock_eks_cluster.destroy.assert_not_called()


@pytest.mark.parametrize(
    "cluster_age_hours,resource_labels,dry_run,should_destroy",
    [
        # Recent cluster should be kept
        (0, {"testid": "test-123", "createdby": "SCT"}, False, False),
        # Old cluster should be destroyed
        (20, {"testid": "test-456", "createdby": "SCT"}, False, True),
        # Old cluster with dry run should not be destroyed
        (20, {"createdby": "SCT"}, True, False),
        # Old cluster with keep:alive should not be destroyed
        (20, {"keep": "alive", "createdby": "SCT"}, False, False),
    ],
    ids=["recent_cluster", "old_cluster", "dry_run", "keep_alive"],
)
def test_clean_gke_clusters(mock_gke_cleaner, cluster_age_hours, resource_labels, dry_run, should_destroy):
    """Test GKE cluster cleanup with various scenarios."""
    # Calculate cluster creation time
    cluster_time = datetime.now(timezone.utc) - timedelta(hours=cluster_age_hours)

    # Setup mock cluster
    mock_cluster = MagicMock()
    mock_cluster.name = "test-cluster"
    mock_cluster.cluster_info = {
        "createTime": cluster_time.isoformat(),
        "resourceLabels": resource_labels,
    }
    mock_gke_cleaner.list_gke_clusters.return_value = [mock_cluster]

    # Run cleanup
    clean_gke.clean_gke_clusters(gke_cleaner=mock_gke_cleaner, project_id="test-project", dry_run=dry_run)

    # Verify destroy was called or not based on expected behavior
    if should_destroy:
        mock_cluster.destroy.assert_called_once()
    else:
        mock_cluster.destroy.assert_not_called()
