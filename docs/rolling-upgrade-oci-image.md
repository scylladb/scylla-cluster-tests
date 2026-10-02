# OCI Image-Based Rolling Upgrade

`jenkins-pipelines/oss/rolling-upgrade/rolling-upgrade-oci-image.jenkinsfile` upgrades a 6-node
OCI cluster node-by-node from a preinstalled Scylla image, mirroring the GCE/Azure image rolling
upgrades (test-case `test-cases/upgrades/rolling-upgrade.yaml`, overlay
`configurations/oci/rolling-upgrade.yaml`).

## 2026.2+ only

`base_versions` is pinned to `'2026.2'` instead of the usual `''` (auto mode). Auto mode would pick
the 2026.1 LTS as the upgrade base, and no OCI base image exists for 2026.1 or earlier — there is
nothing to upgrade *from* on OCI before 2026.2.

## Release path

`scylla-pkg` triggers this through the shared `jenkins-pipelines/oss/sct_triggers/rolling-upgrade-trigger.jenkinsfile`
(itself generated from `configurations/triggers/rolling-upgrade.yaml`), passing `oci_image_db=<OCID>`
and `backend=oci`. The matrix entry `rolling-upgrade/rolling-upgrade-oci-image-test` carries labels
`weekly, week-a, image, oci` and excludes `2024.1, 2025.1, 2025.4, 2026.1` (no OCI image to upgrade from).

## Staging

```bash
python staging_trigger.py -f scylla-staging/<user> generate -b <branch> \
    --repo git@github.com:<you>/scylla-cluster-tests.git \
    oss/rolling-upgrade/rolling-upgrade-oci-image

python staging_trigger.py -f scylla-staging/<user> trigger -b <branch> \
    --repo git@github.com:<you>/scylla-cluster-tests.git \
    oss/rolling-upgrade/rolling-upgrade-oci-image-test \
    -s new_scylla_repo=http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list \
    -s base_versions=2026.2 \
    -s availability_zone= \
    -s email_recipients=<user>@scylladb.com \
    --no-update-pr
```

`-n` dry-runs either command, `-e` edits parameters interactively before triggering. The `longevity`
preset (used for all `rollingUpgradePipeline` jobs) sets `availability_zone=a`, which would override
the `"a,b,c"` from the OCI overlay — hence blanking it explicitly above.

To preview the trigger-matrix selection instead of Jenkins job generation:

```bash
uv run sct.py trigger-matrix --matrix configurations/triggers/rolling-upgrade.yaml \
    --backend oci --labels-selector oci --scylla-version 2026.2 --dry-run
```
