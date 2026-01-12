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

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add the cloud_cleanup directory to the path
cloud_cleanup_path = Path(__file__).parent.parent / "utils" / "cloud_cleanup"
sys.path.insert(0, str(cloud_cleanup_path / "k8s_eks"))
sys.path.insert(0, str(cloud_cleanup_path / "k8s_gke"))

import clean_eks
import clean_gke


class CleanEksTest(unittest.TestCase):
    """Test EKS cluster cleanup functionality."""

    @patch("clean_eks.boto3.client")
    @patch("clean_eks.EksClusterForCleaner")
    def test_clean_eks_keeps_recent_cluster(self, mock_cluster_class, mock_boto_client):
        """Test that recent clusters are kept."""
        # Mock EKS client
        mock_eks = MagicMock()
        mock_boto_client.return_value = mock_eks
        mock_eks.list_clusters.return_value = {"clusters": ["test-cluster"]}

        # Mock cluster with recent creation time
        mock_cluster = MagicMock()
        mock_cluster.create_time = datetime.utcnow()
        mock_cluster.metadata = {
            "items": [{"key": "TestId", "value": "test-123"}, {"key": "CreatedBy", "value": "SCT"}]
        }
        mock_cluster_class.return_value = mock_cluster

        # Run cleanup
        clean_eks.clean_eks_clusters(regions=["us-east-1"], dry_run=False)

        # Cluster should not be destroyed
        mock_cluster.destroy.assert_not_called()

    @patch("clean_eks.boto3.client")
    @patch("clean_eks.EksClusterForCleaner")
    def test_clean_eks_deletes_old_cluster(self, mock_cluster_class, mock_boto_client):
        """Test that old clusters are deleted."""
        # Mock EKS client
        mock_eks = MagicMock()
        mock_boto_client.return_value = mock_eks
        mock_eks.list_clusters.return_value = {"clusters": ["old-cluster"]}

        # Mock cluster with old creation time (more than DEFAULT_KEEP_HOURS)
        mock_cluster = MagicMock()
        mock_cluster.create_time = datetime.utcnow() - timedelta(hours=20)
        mock_cluster.metadata = {
            "items": [{"key": "TestId", "value": "test-456"}, {"key": "CreatedBy", "value": "SCT"}]
        }
        mock_cluster_class.return_value = mock_cluster

        # Run cleanup
        clean_eks.clean_eks_clusters(regions=["us-east-1"], dry_run=False)

        # Cluster should be destroyed
        mock_cluster.destroy.assert_called_once()

    @patch("clean_eks.boto3.client")
    @patch("clean_eks.EksClusterForCleaner")
    def test_clean_eks_dry_run(self, mock_cluster_class, mock_boto_client):
        """Test that dry run does not delete clusters."""
        # Mock EKS client
        mock_eks = MagicMock()
        mock_boto_client.return_value = mock_eks
        mock_eks.list_clusters.return_value = {"clusters": ["test-cluster"]}

        # Mock cluster with old creation time
        mock_cluster = MagicMock()
        mock_cluster.create_time = datetime.utcnow() - timedelta(hours=20)
        mock_cluster.metadata = {"items": [{"key": "CreatedBy", "value": "SCT"}]}
        mock_cluster_class.return_value = mock_cluster

        # Run cleanup in dry run mode
        clean_eks.clean_eks_clusters(regions=["us-east-1"], dry_run=True)

        # Cluster should not be destroyed in dry run
        mock_cluster.destroy.assert_not_called()

    @patch("clean_eks.boto3.client")
    @patch("clean_eks.EksClusterForCleaner")
    def test_clean_eks_keeps_alive_tag(self, mock_cluster_class, mock_boto_client):
        """Test that clusters with 'keep: alive' tag are kept."""
        # Mock EKS client
        mock_eks = MagicMock()
        mock_boto_client.return_value = mock_eks
        mock_eks.list_clusters.return_value = {"clusters": ["keep-alive-cluster"]}

        # Mock cluster with old creation time but 'keep: alive' tag
        mock_cluster = MagicMock()
        mock_cluster.create_time = datetime.utcnow() - timedelta(hours=20)
        mock_cluster.metadata = {"items": [{"key": "keep", "value": "alive"}, {"key": "CreatedBy", "value": "SCT"}]}
        mock_cluster_class.return_value = mock_cluster

        # Run cleanup
        clean_eks.clean_eks_clusters(regions=["us-east-1"], dry_run=False)

        # Cluster should not be destroyed
        mock_cluster.destroy.assert_not_called()


class CleanGkeTest(unittest.TestCase):
    """Test GKE cluster cleanup functionality."""

    @patch("clean_gke.GkeCleaner")
    @patch("clean_gke.environment")
    def test_clean_gke_keeps_recent_cluster(self, mock_env, mock_cleaner_class):
        """Test that recent clusters are kept."""
        # Mock GKE cleaner
        mock_cleaner = MagicMock()
        mock_cleaner_class.return_value = mock_cleaner

        # Mock cluster with recent creation time
        mock_cluster = MagicMock()
        mock_cluster.name = "test-cluster"
        mock_cluster.cluster_info = {
            "createTime": datetime.utcnow().isoformat() + "Z",
            "resourceLabels": {"testid": "test-123"},
        }
        mock_cleaner.list_gke_clusters.return_value = [mock_cluster]

        # Run cleanup
        clean_gke.clean_gke_clusters(gke_cleaner=mock_cleaner, project_id="test-project", dry_run=False)

        # Cluster should not be destroyed
        mock_cluster.destroy.assert_not_called()

    @patch("clean_gke.GkeCleaner")
    @patch("clean_gke.environment")
    def test_clean_gke_deletes_old_cluster(self, mock_env, mock_cleaner_class):
        """Test that old clusters are deleted."""
        # Mock GKE cleaner
        mock_cleaner = MagicMock()
        mock_cleaner_class.return_value = mock_cleaner

        # Mock cluster with old creation time (more than DEFAULT_KEEP_HOURS)
        old_time = datetime.utcnow() - timedelta(hours=20)
        mock_cluster = MagicMock()
        mock_cluster.name = "old-cluster"
        mock_cluster.cluster_info = {
            "createTime": old_time.isoformat() + "Z",
            "resourceLabels": {"testid": "test-456"},
        }
        mock_cleaner.list_gke_clusters.return_value = [mock_cluster]

        # Run cleanup
        clean_gke.clean_gke_clusters(gke_cleaner=mock_cleaner, project_id="test-project", dry_run=False)

        # Cluster should be destroyed
        mock_cluster.destroy.assert_called_once()

    @patch("clean_gke.GkeCleaner")
    @patch("clean_gke.environment")
    def test_clean_gke_dry_run(self, mock_env, mock_cleaner_class):
        """Test that dry run does not delete clusters."""
        # Mock GKE cleaner
        mock_cleaner = MagicMock()
        mock_cleaner_class.return_value = mock_cleaner

        # Mock cluster with old creation time
        old_time = datetime.utcnow() - timedelta(hours=20)
        mock_cluster = MagicMock()
        mock_cluster.name = "test-cluster"
        mock_cluster.cluster_info = {
            "createTime": old_time.isoformat() + "Z",
            "resourceLabels": {},
        }
        mock_cleaner.list_gke_clusters.return_value = [mock_cluster]

        # Run cleanup in dry run mode
        clean_gke.clean_gke_clusters(gke_cleaner=mock_cleaner, project_id="test-project", dry_run=True)

        # Cluster should not be destroyed in dry run
        mock_cluster.destroy.assert_not_called()

    @patch("clean_gke.GkeCleaner")
    @patch("clean_gke.environment")
    def test_clean_gke_keeps_alive_tag(self, mock_env, mock_cleaner_class):
        """Test that clusters with 'keep: alive' tag are kept."""
        # Mock GKE cleaner
        mock_cleaner = MagicMock()
        mock_cleaner_class.return_value = mock_cleaner

        # Mock cluster with old creation time but 'keep: alive' label
        old_time = datetime.utcnow() - timedelta(hours=20)
        mock_cluster = MagicMock()
        mock_cluster.name = "keep-alive-cluster"
        mock_cluster.cluster_info = {
            "createTime": old_time.isoformat() + "Z",
            "resourceLabels": {"keep": "alive"},
        }
        mock_cleaner.list_gke_clusters.return_value = [mock_cluster]

        # Run cleanup
        clean_gke.clean_gke_clusters(gke_cleaner=mock_cleaner, project_id="test-project", dry_run=False)

        # Cluster should not be destroyed
        mock_cluster.destroy.assert_not_called()
