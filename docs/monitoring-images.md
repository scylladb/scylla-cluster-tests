# Update monitoring images/branch

This docs show how to update monitor images

* currently we have images for AWS and GCP (and not for Azure)

* AWS image get created on us-east-1, and named like `scylladb-monitor-4-6-2-2024-02-13t08-06-04z`
  `utils/copy_ami_to_all_regions.sh` can be used to copy the AMIs to multiple region

* GCP images are named as following `scylladb-monitor-4-6-2-2024-03-07t12-47-59z

* when updating images, one should also update the `monitor_branch` for the backend that doesn't have images yet.

## Before merging an image bump

`sct.py lint-pipelines` stubs cloud image resolution out so it can run without credentials — it
will **not** tell you whether the image you are pointing at exists. A bump to an unpublished
image therefore lints clean and breaks every AWS and GCE run once merged (SCT-910).

The existence check lives in `unit_tests/integration/test_default_monitor_images.py`, which
resolves `ami_id_monitor` in every region of `AWS_SUPPORTED_REGIONS` and `gce_image_monitor` in
the GCE `scylla-images` project. It runs in the `integration tests` Jenkins stage, which is
gated on the `test-integration` PR label — `.github/workflows/label-monitor-image-updates.yaml`
adds that label automatically when a PR changes `ami_id_monitor` or `gce_image_monitor`.

So on an image bump PR:

1. let the `integration tests` stage run and go green — do not merge on the other checks alone
2. if it reports missing regions, the AMI copy is incomplete: copy it to the remaining regions
   with `utils/copy_ami_to_all_regions.sh` and re-run

To check locally before opening the PR:

```bash
./docker/env/hydra.sh integration-tests -t integration/test_default_monitor_images.py
```
