# Upload a minicloud run to Argus

A minicloud run is a local run. It has no Jenkins job, so SCT sends nothing to Argus during the run.
SCT writes each Argus call to a replay log in the run directory instead.
After the run, the `argus` CLI sends that log to Argus, and Argus files the run under your user.

This page shows the full flow on Linux and on macOS: run a test on minicloud, then publish it with `argus` CLI v0.4.0.
For the details of minicloud itself, see [Running SCT against minicloud](minicloud.md).

## How it works

```
scripts/run-minicloud-test.sh                 argus run replay
            │                                         │
            ▼                                         ▼
~/sct-results/<run>/                        Argus: local-runs/<you>/<test>#N
  argus_replay_log_<run_id>_*.jsonl  ─────▶  https://argus.scylladb.com/tests/scylla-cluster-tests/<run_id>
```

- The replay log has one line for each Argus call. It records the run, its status, the SCT configuration, the packages, the resources, the stress commands and the events.
- The replay keeps the run ID of the log. Upload the logs to S3 under the same ID, and Argus attaches them to the run.
- Each replay into the same test gets the next build number: `#1`, `#2`, and so on.

## Linux and macOS

The flow is the same on both platforms. These are the differences:

| | Linux | macOS (Apple silicon) |
| --- | --- | --- |
| How SCT starts minicloud | `minicloud` Docker container | native `minicloud` process |
| Accelerator | KVM | Hypervisor.framework (HVF) |
| Endpoint | `http://localhost:5000` | `http://localhost:5050` |
| Guests | same architecture as the host | arm64 |
| Monitor node | yes on x86_64, no on arm64 | no |
| minicloud log | `docker logs minicloud` | `~/.cache/minicloud/minicloud-native.log` |
| Stop minicloud | `docker rm -f minicloud` | `kill "$(cat ~/.cache/minicloud/minicloud.pid)"` |

`scripts/run-minicloud-test.sh` selects the correct column for you.
On an arm64 host, Linux or macOS, it layers `configurations/minicloud/arm64.yaml`. KVM and HVF accelerate only guests of the host architecture, so that overlay selects arm64 instance types. It also removes the monitor node, because the `scylladb-monitor` AMI is amd64 only.
On macOS, it also layers `configurations/minicloud/macos.yaml`, which turns off guest memory locking. Set the endpoint one time in your shell, because the steps below use it:

```bash
# Linux
export SCT_MINICLOUD_ENDPOINT_URL=http://localhost:5000
# macOS
export SCT_MINICLOUD_ENDPOINT_URL=http://localhost:5050
```

## Before you start

Do these steps one time.

1. Make sure that your AWS credentials for the QA account are valid. SCT needs them for the AMI lookup and the keystore. minicloud needs them to download the AMIs:
   ```bash
   aws sts get-caller-identity
   ```
   The IAM identity must also have `ebs:ListSnapshotBlocks` and `ebs:GetSnapshotBlock`.
2. Install the `argus` CLI v0.4.0 from the [`cli/v0.4.0` release](https://github.com/scylladb/argus/releases/tag/cli/v0.4.0):
   ```bash
   os=$([ "$(uname -s)" = Darwin ] && echo macOS || echo linux)
   arch=$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')
   gh release download cli/v0.4.0 -R scylladb/argus -p "argus_0.4.0_${os}_${arch}.tar.gz"
   tar xzf "argus_0.4.0_${os}_${arch}.tar.gz" argus
   mv argus ~/.local/bin/
   argus version
   ```
   The last command shows `argus 0.4.0`.
3. Log in to Argus:
   ```bash
   argus auth
   ```
4. Set up the host for your platform. See [Linux host](#linux-host) or [macOS host](#macos-host).

### Linux host

1. Make sure that your user can use KVM and Docker:
   ```bash
   ls -l /dev/kvm
   sudo usermod -aG kvm,docker "$USER"
   ```
   Log out and log in again after `usermod`.
2. Make sure that `$HOME` has 80 GiB free. The guest disk cache goes to `~/.cache/minicloud`.
3. On a firewalld host, for example Fedora, let `scripts/run-minicloud-test.sh` run `sudo -n firewall-cmd`. If it cannot, the guests boot but do not get their SSH key.

For the details, see [Running locally](minicloud.md#running-locally).

### macOS host

Docker on macOS cannot give a container access to HVF, so SCT runs minicloud as a native process.

1. Build minicloud and put the binary on your `PATH`:
   ```bash
   gh repo clone scylladb/minicloud ~/Code/minicloud
   cd ~/Code/minicloud
   cargo build --release
   ln -sf "$PWD/target/release/minicloud" ~/.local/bin/minicloud
   ```
2. Install the `minicloud-tund` helper. It holds the `utun` device and the routes to the guests:
   ```bash
   sudo MINICLOUD_VPC_ROUTES=10.160.0.0/11,172.31.0.0/16 ./minicloud-setup.sh
   ```
3. Make sure that the helper socket exists:
   ```bash
   ls -l /var/run/minicloud-tund.sock
   ```
4. Point the Docker SDK at your Docker daemon. OrbStack and Docker Desktop do not serve `/var/run/docker.sock`, and SCT starts the stress tools through the SDK. Add this line to your shell profile:
   ```bash
   export DOCKER_HOST=$(docker context inspect -f '{{.Endpoints.docker.Host}}')
   ```

On macOS, `scripts/run-minicloud-test.sh` accepts only `-m direct`. The native minicloud and its `utun` device are on the host, outside the hydra container.

## Run the test

The `provision` flavor is a short smoke test: 3 DB nodes, 1 loader, a 1-minute write and a non-disruptive nemesis.

1. Give the run an ID, so that you know it before the run starts:
   ```bash
   export SCT_TEST_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
   ```
2. Start the test with a released Scylla version. The snapshots of dev AMIs are not shared with the QA account:
   ```bash
   SCT_SCYLLA_VERSION=2026.1 scripts/run-minicloud-test.sh -f provision
   ```
3. Wait for the end of the test. The pytest summary at the end shows the result.
4. Make sure that the run wrote a replay log:
   ```bash
   ls ~/sct-results/latest/argus_replay_log_*
   ```

The script sets `SCT_ENABLE_ARGUS=false`, so the run writes only the replay log.

The first run with a new AMI is slow. minicloud downloads each new AMI over the EBS direct API and builds a guest disk from it. A 30 GiB Scylla AMI took about 30 minutes on a home connection. The disks go to `~/.cache/minicloud/amis`. Do not remove that directory: the next runs use it and start in minutes. To see the download progress, read the minicloud log.

## Upload the logs

This step is optional. A local run does not upload its logs to S3. Upload them under the run ID, so that Argus shows them with the run:

```bash
uv run sct.py collect-logs --backend aws --test-id "$SCT_TEST_ID" --logdir ~/sct-results/latest
```

minicloud must still run, because `collect-logs` reads the logs from the guests.
If you upload the logs after you publish the run, replay the run again. Argus then attaches the new logs.

## Stop minicloud

The script starts minicloud with `keep_alive`, so minicloud stays up for the next run. When you are done, stop it. This stops every guest:

```bash
docker rm -f minicloud                            # Linux
kill "$(cat ~/.cache/minicloud/minicloud.pid)"    # macOS
```

## Publish the run

1. Do a dry run. A dry run applies nothing and shows where the run goes:
   ```bash
   argus run replay --dir ~/sct-results/latest --run-id "$SCT_TEST_ID" --test my-feature --dry-run
   ```
   The last line shows the place of the run:
   ```
   Run <run_id> would replay as (local-runs/<you>/my-feature#1)
   ```
2. Publish the run. Use the same command without `--dry-run`:
   ```bash
   argus run replay --dir ~/sct-results/latest --run-id "$SCT_TEST_ID" --test my-feature
   ```
3. Open the link in the output:
   ```
   Run <run_id> (local-runs/<you>/my-feature#1): https://argus.scylladb.com/tests/scylla-cluster-tests/<run_id>
   ```

Use the task key or the pull request as the `--test` value, for example `--test sct-1234`. Then the runs of one task stay together.
Without `--test`, the test name is the name of the first config file of the run. For the `provision` flavor, that is `minicloud-provision-test`.

To find your runs in Argus, open the `local-runs` release, then the group with your username.

## Compare two runs

Replay each run with its own command, and give the same `--test`:

```bash
argus run replay --dir ~/sct-results/<baseline> --run-id <baseline_run_id> --test my-feature
argus run replay --dir ~/sct-results/<candidate> --run-id <candidate_run_id> --test my-feature
```

The two runs become build `#1` and build `#2` of `local-runs/<you>/my-feature`.

## Useful flags

| Flag | Effect |
| ---- | ------ |
| `--dry-run` | Validate the log and show the build. Apply nothing. |
| `--test <name>` | File the run under `local-runs/<you>/<name>`. |
| `--file <path>` | Replay one log file or a `.tar.zst` archive, in place of `--dir`. |
| `--keep-run` | Replay the run under its recorded path, in place of `local-runs`. You cannot use it with `--test`. |
| `--as-me=false` | Keep the recorded starter and assignee. |
| `--backfill-logs=false` | Attach only the logs that the replay log records. |
| `--report json` | Write the summary and the links as JSON. |

The [Argus CLI README](https://github.com/scylladb/argus/blob/cli/v0.4.0/cli/README.md#replaying-a-run) has the full reference.

## Troubleshooting

| Symptom | Cause and fix |
| ------- | ------------- |
| `ExpiredToken` or `InvalidClientTokenId` before the test starts | The AWS credentials are not valid. Refresh them, then run `aws sts get-caller-identity`. |
| `/dev/kvm is not available` (Linux) | KVM is off, or your user is not in the `kvm` group. Do step 1 of [Linux host](#linux-host). |
| `'minicloud' is not on PATH` (macOS) | Do step 1 of [macOS host](#macos-host). |
| `/var/run/minicloud-tund.sock does not exist` (macOS) | The `minicloud-tund` helper is not installed. Do step 2 of [macOS host](#macos-host). |
| `Error while fetching server API version: ... FileNotFoundError` in `SetUp` (macOS) | The Docker SDK cannot find the daemon. Do step 4 of [macOS host](#macos-host). |
| `minicloud cannot lock guest memory on macOS` | `configurations/minicloud/macos.yaml` is not in the config list. Use `scripts/run-minicloud-test.sh`, or add `arm64.yaml` and then `macos.yaml` after `configurations/minicloud.yaml`. |
| `SnapshotNotFound` in the minicloud log | The Scylla version is a dev build. Use a released version. |
| SCT waits for nodes that never boot, and the minicloud log shows `background VM launch failed` after `GetSnapshotBlock` timeouts | The network dropped during the AMI download. minicloud downloads with 32 parallel connections, and a phone hotspot or a VPN often cannot keep them open. Stop the run and minicloud. Then run again on a stable network. |
| guests boot, but SSH fails with `AuthenticationError` (Linux) | The firewall blocks the requests of the guests to IMDS. Do step 3 of [Linux host](#linux-host). |
| `failed=1` in the replay summary | Read the table of errors under the summary. A run whose `submit_run` failed gets no link. |
| No replay log in the run directory | The run stopped before it resolved its configuration. Fix the error in `output.log`, then run again. |

Each run has a copy of the minicloud log in `~/sct-results/<run>/minicloud.log`.
For more symptoms, see [Troubleshooting](minicloud.md#troubleshooting) in the minicloud guide.
