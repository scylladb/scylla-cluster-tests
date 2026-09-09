# Workflow: Downscale a Test for minicloud

Five phases from a production test-case to a green local run. Do not skip Phase 1 — every
expensive mistake in this workflow comes from shrinking a param the test never reads, or a
param it asserts on.

---

## Phase 1: Read What the Test Actually Reads

**Entry:** A jenkinsfile, or a test-case yaml plus the test entry point you want to run
locally.

**Actions:**

0. **If you were given a jenkinsfile, start there** — it carries the config list, the test
   name and the backend, and the config list is what actually matters. SCT parses it for you:

   ```bash
   uv run python -c '
   from pathlib import Path
   from sdcm.utils.lint.jenkins_parser import parse_jenkinsfile
   import sys
   c = parse_jenkinsfile(Path(sys.argv[1]))
   print("backend  :", c.params.get("backend"))
   print("test_name:", c.params.get("test_name"))
   print("sub_tests:", c.params.get("sub_tests"))
   print("-c " + " -c ".join(c.test_config))
   ' <path/to.jenkinsfile>
   ```

   The printed `-c` list is the production config stack, in order. Your overlay goes at the
   end of it, followed by `configurations/minicloud.yaml`. A jenkinsfile with `sub_tests`
   runs each one as a separate job — pick a single one to run locally.
1. Resolve the entry point to a class: `longevity_test.LongevityTest.test_custom_time`,
   `upgrade_test.UpgradeTest.test_rolling_upgrade`,
   `grow_cluster_test.GrowClusterTest.test_grow_x_to_y`,
   `artifacts_test.ArtifactsTest.test_scylla_service`, and so on.
2. List the params the method consumes:
   `grep -n "params.get\|params\[" <module>.py` — read the test method body, not just the grep
   output, so you see which keys are read by helpers it calls.
3. Check [test-type-params.md](../references/test-type-params.md) for that entry point. If it
   is not listed, the grep in step 2 is the answer.
4. Note every value the test **asserts on**: keyspace names inside stress commands, `ops(...)`
   sets, profile file paths, consistency levels. These are shape, and shape does not shrink.
5. Note the cluster shape the test needs: does it query Prometheus? does it grow? does it take
   a node down?
6. Check the architecture. Resolve the config and look at `instance_type_db` — if it is an
   arm64 type (`i8g.*`, `im4gn.*`, `c7g.*`) and your host is x86_64, the guest cannot run
   under KVM. Pin the db role to an x86_64 type of the same shape and use an x86_64 image.

**Exit:** A list of params that size the load, and a list of values that must not change.

---

## Phase 2: Compute the Budget

**Entry:** The param list from Phase 1.

**Actions:**

1. Read the host's free memory: `free -g` (the `available` column, not `free`).
2. Decide per-guest memory. `4GiB` is the default and a good starting point; `3GiB` is the
   floor — below it Scylla fails to boot with "memory per shard too low".
3. Solve for the guest count:
   `guests = (available_GiB - 2) / per_guest_GiB`.
4. Split the guest budget into `n_db_nodes` (3 unless the test needs more),
   `n_loaders` (1), `n_monitor_nodes` (1 if the test reads Prometheus, else 0).
5. For a growing test, budget `cluster_target_size`, not `n_db_nodes` — the peak is what has
   to fit.
6. If the shape you need does not fit, stop and move the run to a bigger host rather than
   dropping below a floor. A 2-node "cluster" is a different test.

**Exit:** Concrete numbers for `n_db_nodes`, `n_loaders`, `n_monitor_nodes`,
`minicloud_lightweight_memory`, and the arithmetic written down for the overlay header.

---

## Phase 3: Write the Overlay

**Entry:** The numbers from Phase 2.

**Actions:**

1. Create `configurations/minicloud/<name>.yaml`, named after the test-case it shrinks.
2. Open with a header comment carrying: which test-case it shrinks, the exact `-c` order to
   layer it in, the guest-budget arithmetic, and what the shrink does *not* preserve.
3. Set the cluster shape from Phase 2.
4. Shrink the load params from Phase 1: row counts (`n=`, `-pop seq=`), `threads=`,
   `duration=`, partition and clustering-row counts, `cassandra_stress_population_size`.
   Keep keyspace names, consistency levels, compaction and ops sets byte-identical.
5. Set `test_duration` to cover the shrunk workload plus setup — and remember it doubles as
   the stress duration in `GrowClusterTest`.
6. Add the backend fixups: `gce_n_local_ssd_disk_db: 1` and the three `gce_instance_type_*`
   keys if the test declares GCE support; a realistic `instance_type_db` that exists in the
   emulated catalog.
7. Set `nemesis_selector: "not disruptive"` if the test runs a nemesis.
8. Add `user_prefix` so the local run's resources are recognisable.
9. Comment **why** each value is what it is. The existing overlays are the house style — a
   value with no reason attached gets "corrected" by the next person.
10. Do **not** re-set anything `configurations/minicloud.yaml` already carries.

**Exit:** An overlay file that reads like an explanation, not a diff.

---

## Phase 4: Verify Without Booting Anything

**Entry:** The overlay from Phase 3.

**Actions:**

1. Resolve the merge:
   ```bash
   SCT_SCYLLA_VERSION=2026.2 uv run sct.py conf -b aws \
     '["<test-case>","<overlay>","configurations/minicloud.yaml"]'
   ```
   This runs `verify_configuration()` and `check_required_files()` and prints every resolved
   value. A missing profile file or an invalid instance type fails here, in seconds. A version
   must be supplied (`SCT_SCYLLA_VERSION`, `SCT_AMI_ID_DB_SCYLLA` or `SCT_GCE_IMAGE_DB`) or the
   command stops at `_check_version_supplied` before validating anything else.
2. Read back `n_db_nodes`, `n_loaders`, `n_monitor_nodes`, `cluster_target_size` from the dump
   and check them against the Phase 2 arithmetic. A test-case that sets a count you did not
   expect is exactly what this step is for.
3. Read back the stress commands and confirm the shrunk ones are the ones in effect — the
   merge is last-file-wins, so an overlay layered in the wrong order silently does nothing.
4. Repeat with `-b gce` if the test declares GCE support.

**Exit:** The resolved config matches the budget, with no booting and no cost.

---

## Phase 5: Run, Triage, Land

**Entry:** A verified config list.

**Actions:**

1. Start the emulator with **the same config list** the test will use:
   ```bash
   uv run sct.py start-minicloud -b aws -c <test-case> -c <overlay> -c configurations/minicloud.yaml
   ```
   A mismatch means the memory gate checked a different test than the one that runs.
2. Run the test with the same list:
   ```bash
   uv run sct.py run-test <module.Class.method> --backend aws -c <test-case> -c <overlay> -c configurations/minicloud.yaml
   ```
3. On failure, work through [triage-local-runs.md](../references/triage-local-runs.md) — it
   maps each symptom to the overlay value that fixes it. `docker logs minicloud` and
   `minicloud.log` in the logdir are the emulator's own view of the run.
4. Iterate on the overlay, not on the test-case.
5. Tear down **after** log collection — removing the container kills every guest with it.
6. Generate a phase runner beside the overlay — see
   [run-script-template.md](../references/run-script-template.md). It must export
   `SCT_MINICLOUD_ENDPOINT_URL` and refuse to provision unless the emulator answers, because a
   run that silently targets real AWS costs real money and reads like a local one.
7. If this is a flow anyone will repeat, add a flavor to `scripts/run-minicloud-test.sh` so the
   next person gets the whole config list for free.

**Exit:** A green local run, and an overlay committed with the reasoning that produced it.
