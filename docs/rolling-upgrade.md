# Rolling Upgrade

The rolling-upgrade jobs (`jenkins-pipelines/oss/rolling-upgrade/*.jenkinsfile`, all built on
`vars/rollingUpgradePipeline.groovy`) install a base Scylla version on a 6-node cluster and upgrade it
node by node to the version under test. Most of them run `test-cases/upgrades/rolling-upgrade.yaml`,
with a per-backend overlay from `configurations/<backend>/`.

## Base versions

`base_versions` is empty by default ("auto" mode): the pipeline runs `sct.py get-scylla-base-versions`
(`utils/get_supported_scylla_base_versions.py`), which derives the base versions from the target repo,
the Linux distro and the backend, and runs one upgrade per base version. Set `base_versions` only to
force a specific base.

Distros and backends that start supporting Scylla later than the rest declare their first supported
release in `start_support_versions` / `start_support_backend`; older releases are never picked as a base.

## Release path

`scylla-pkg` triggers these jobs through `jenkins-pipelines/oss/sct_triggers/rolling-upgrade-trigger.jenkinsfile`,
generated from `configurations/triggers/rolling-upgrade.yaml`. Each matrix entry has `labels` and
`exclude_versions` (target releases the job must not run for). Preview the selection with:

```bash
uv run sct.py trigger-matrix --matrix configurations/triggers/rolling-upgrade.yaml \
    --backend <backend> --labels-selector <label> --scylla-version <version> --dry-run
```

## Staging

```bash
uv run python staging_trigger.py generate --pr <PR> jenkins-pipelines/oss/rolling-upgrade/<job>.jenkinsfile

uv run python staging_trigger.py -f scylla-staging/<user>/oss/rolling-upgrade trigger --pr <PR> <job>-test \
    -s new_scylla_repo=http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list
```

## Backend specifics

### OCI

- Images exist only from 2026.2, so `start_support_backend` starts OCI there and the trigger matrix
  excludes earlier targets. On master, where the last LTS (2026.1) has no image, the latest STS is
  used as the base.
- The job runs on master only for now: the trigger matrix also excludes 2026.2 and 2026.3, since
  their release branches don't have this jenkinsfile. Remove them from `exclude_versions` once it's
  backported there.
- `configurations/oci/rolling-upgrade.yaml` pins `VM.DenseIO.E5.Flex:8` (the test-case `sizing_db` has
  no OCI match) with `on_demand` provisioning, since DenseIO shapes cannot be spot.
- The overlay spreads nodes over AZs `a,b,c`, but the `longevity` preset used by `rollingUpgradePipeline`
  sets `availability_zone=a`. Pass `-s availability_zone=` when triggering in staging.
