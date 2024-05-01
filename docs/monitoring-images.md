# Update monitoring images/branch

This docs show how to update monitor images

* currently we have images for AWS and GCP (and not for Azure)

* AWS image get created on us-east-1, and named like `scylladb-monitor-4-6-2-2024-02-13t08-06-04z`
  `utils/copy_ami_to_all_regions.sh` can be used to copy the AMIs to multiple region

* GCP images are named as following `scylladb-monitor-4-6-2-2024-03-07t12-47-59z

* when updating images, one should also update the `monitor_branch` for the backend that doesn't have images yet.
