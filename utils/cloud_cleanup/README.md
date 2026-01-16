# Cloud Cleanup Utilities

This directory contains time-based cleanup utilities for various cloud platforms and backends used by Scylla Cluster Tests.

## Overview

Cloud cleanup scripts automatically remove old test resources based on their age and retention tags/labels. These utilities run periodically via Jenkins to prevent resource accumulation.

## Directory Structure

```
utils/cloud_cleanup/
├── aws/          # AWS EC2 instances and resources
├── azure/        # Azure virtual machines and resources
├── gce/          # GCE instances and resources
├── k8s_eks/      # AWS EKS clusters and associated resources
├── k8s_gke/      # GCP GKE clusters and associated resources
└── xcloud/       # ScyllaDB Cloud clusters
```

## Cleanup Strategy

All cleanup scripts follow a consistent time-based retention strategy:

1. **Default Retention**: Resources younger than 14 hours (configurable) are kept by default
2. **Tag/Label-Based Retention**: Resources can specify custom retention via tags/labels
3. **Keep Action**: Resources can prevent deletion via action tags/labels

## Tags and Labels

Cloud cleanup scripts use tags (AWS/Azure) or labels (GCP) to control resource retention.

### Common Tags/Labels (All Backends)

- **`keep`**: Controls retention duration
  - Set to `"alive"` to keep the resource indefinitely
  - Set to a number (e.g., `"24"`) to keep for that many hours
  - If not set, uses default retention (14 hours)

- **`keep_action`**: Controls deletion behavior
  - Set to `"terminate"` or empty to allow deletion
  - Set to any other value (e.g., `"stop"`) to prevent deletion

### Platform-Specific Naming Conventions

Due to platform restrictions, tag/label names differ slightly between backends:

**AWS (EKS, EC2):**
- Uses PascalCase for tag keys: `TestId`, `RunByUser`, `NodeType`
- Example: `{"TestId": "abc123", "keep": "alive"}`

**GCP (GKE, GCE):**
- Uses lowercase with underscores/hyphens: `testid`, `test_id`, `runbyuser`
- Example: `{"testid": "abc123", "keep": "alive"}`

**Azure:**
- Uses PascalCase similar to AWS: `TestId`, `RunByUser`

**Note**: The `keep` and `keep_action` tags are consistently lowercase across all platforms.

## Resources Cleaned

Each cleanup script handles platform-specific resources:

**AWS (`aws/`):**
- EC2 instances
- EBS volumes
- Elastic IPs
- Capacity reservations
- Dedicated hosts
- Security groups
- Placement groups

**Azure (`azure/`):**
- Virtual machines
- Resource groups
- Network interfaces

**GCE (`gce/`):**
- Compute instances
- Persistent disks
- Static IPs

**EKS (`k8s_eks/`):**
- EKS clusters
- Launch templates
- Load balancers
- CloudFormation stacks

**GKE (`k8s_gke/`):**
- GKE clusters
- Orphaned persistent disks

**ScyllaDB Cloud (`xcloud/`):**
- ScyllaDB Cloud clusters

## Environment Variables

- `DURATION`: Default retention duration in hours (default: 14)
- `DRY_RUN`: Enable dry-run mode to preview deletions without executing

Platform-specific environment variables are documented in individual cleanup scripts.

## Jenkins Integration

Cleanup scripts are integrated into the `hydra-cleanup-cloud` Jenkins pipeline, which runs them periodically across all platforms with configurable dry-run mode.

## See Also

- Individual cleanup scripts in each subdirectory for platform-specific details
- `sdcm/utils/resources_cleanup.py` for tag-based comprehensive cleanup
- Jenkins pipeline: `jenkins-pipelines/qa/hydra-cleanup-cloud.jenkinsfile`
