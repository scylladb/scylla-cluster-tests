# Mini-Plan: Prune Zero-Value and Duplicated Unit Tests

**Date:** 2026-09-30
**Estimated LOC:** ~5,500 deleted, ~1,000 added (moved tests)
**Related PR:** TBD

## Problem
An audit of `unit_tests/` found about 120 collected tests (about 100 test functions) that cannot catch a regression. It covered 6,929 unit tests, checked with per-test line coverage and a full read, and 39 integration files, checked by reading only because they need Docker or cloud access. In addition, about 50 collected integration-marked cases never touch a real service, so they run only in the slower integration job. Manual-only tests are scattered across the unit and integration folders behind skip marks or env-var gates, so nothing shows they are manual on purpose. The flagged tests fall into five groups:
- whole files that duplicate other files or are permanently skipped as broken (manual tests are moved, not deleted)
- tests that exercise only their own mocks or re-implement the logic under test
- exact duplicates of other tests
- tests of pydantic, dataclass or namedtuple behaviour
- change-detectors that copy constants from the source

Each one adds runtime and maintenance cost and gives no signal. This plan deletes them. A test is removed only if the behaviour it checks is covered elsewhere or it never checked production behaviour at all.

## Approach
Use one PR with one commit per group, so each commit can be reviewed and reverted on its own.

- **Delete dead and duplicated files**
  - `unit_tests/test_cassandra_docker_cluster.py`: byte-identical copy of `unit_tests/unit/test_cassandra_docker_cluster.py`.
  - `unit_tests/unit/test_mgmt_operations_stress_load.py`: imports no production code.
- **Move manual tests to `unit_tests/manual/` (new directory)**
  - Manual tests stay in the repo. They move out of the unit and integration folders so the reason they don't run in CI is obvious from where they live:
    - `unit/test_manual.py`: skipped as "manual tests"
    - `unit/test_remoter.py:TestRemoteCmdRunners`: skipped as "to be ran manually", together with the `ALL_COMMANDS_WITH_ALL_OPTIONS` table it uses
    - `integration/test_vector_install_script.py`: run with `-p no:skipping`
    - `integration/test_kernel_panic.py`: opt-in via `SCT_TEST_KERNEL_PANIC`
    - `integration/test_azure_vm_provider_reboot.py`: opt-in via `SCT_TEST_AZURE_REBOOT`
  - Each test keeps its skip mark or env-var gate, so `unit-tests` and `integration-tests` still don't run it. Fixtures they need from `integration/conftest.py` must still resolve, e.g. through a `manual/conftest.py`.
  - Update run instructions that point at the old paths (module docstrings, `docs/kernel-panic-detection.md`). Add a short `unit_tests/manual/README.md` that explains how to run each file.
- **Delete permanently skipped tests and dead helpers**
  - `test_sct_events_continuous_events_registry.py`: `test_get_compact_events_by_continues_hash_from_log`, skipped since compaction continuous events were disabled.
  - `test_nvme.py`: text-parser output constants that no test uses.
- **Delete tautological and mock-only tests**
  - These tests assert on values the test itself built or on mocks it configured, so no production branch decides the outcome:
    - `test_health_check_skip.py`: the four `test_skip_condition_*` / `test_multiple_nemesis_cycles_*` tests
    - `test_version_utils.py`: `test_version_routing_logic`, `test_azure_version_routing_logic`, `test_gce_version_normalization`, `test_full_version_string_preservation`
    - `test_sct_runner.py`: `test_keep_tag_calculation_from_duration`, `test_clean_sct_runners_force`, `test_clean_sct_runners_no_runners_found`
    - `test_config.py`: the two `test_12_scylla_version_repo_ubuntu*` tests
    - `test_remoter.py`: `test_sudo_root`
    - `test_keystore.py`: `test_concurrent_s3_client_creation`, `test_cache_thread_safe`
    - `test_sct_events_base.py`: `test_sct_event_is_in_registry`
    - `test_ip_to_node_map.py`: `test_none_ips_excluded`
    - `test_emr_provisioner.py`: `test_create_emr_cluster_no_sct_security_groups`
    - `test_utils_common.py`: `test_redact_cli_secrets_does_not_match_substring_of_unrelated_flag`
    - `test_log_archive.py`: `test_disk_efficiency_original_not_duplicated`
    - `rest/test_rest_client.py`: `test_get_uses_session`, `test_post_uses_session`
- **Delete exact duplicates**
  - Keep the stronger test of each pair. Coverage confirms each pair runs the same lines with equivalent assertions:
    - `execute_nemesis/test_argus.py`: `test_argus_submit_captures_target_node_info` and `test_argus_submit_parameters_consistency_across_calls`, both covered by `test_argus_submit_on_success`
    - `execute_nemesis/test_healthcheck.py`: `test_execute_nemesis_no_previous_event`, covered by `test_basic.py`
    - `test_config.py`: `test_05_docker`, covered by the docker case of `test_38_verify_scylla_version_lookup_k8s`
    - `trigger_matrix/test_filtering.py`: `test_filter_by_version_exclusion_master` and `test_no_selector_returns_all_eligible`
    - `trigger_matrix/test_yaml_files.py`: the three load/required-field tests, covered by `test_layout_validation.py` and `test_load_config.py`
    - `test_cluster_oci.py`: the `test_restart_*` tests. `OciNode` does not override `restart()`, so `test_cluster.py` already covers them.
    - `test_ip_to_node_map.py`: the four `test_oci_node_get_all_ip_addresses_*` tests, which mirror the AWS ones through the same `BaseNode` method
    - `test_clean_cloud_resources_func.py`: the three `test_tag*_runbyuser*` variants of the truthy-tag branch
    - `test_keystore.py`: `test_get_file_contents_missing_key`, `test_get_json_invalid_content`, `test_get_ssh_key_pair_missing_key`
    - `test_common_get_db_tables.py`: `test_get_db_tables_with_compact_storage_filter_returns_empty_list`
    - `test_instance_matcher.py`: `test_select_deterministic`, `test_select_smallest_vcpu`, `test_non_flex_instance_unchanged`
    - `test_s3_storage_download.py`: `test_download_retries_exhausted_fails`
    - `test_spark_migrator.py`: `test_submit_migration_job_still_uses_cluster_deploy_mode`
    - `test_argus_replay_log.py`: `test_write_is_synchronous`
    - `rest/test_rest_client.py`: `test_session_has_retry_adapter_https`
    - `test_cluster_cassandra.py`: `test_compute_jvm_heap_min_2gb_on_large_system`, already a parametrize case
- **Delete library and constant tests**
  - `test_scylla_yaml.py`: the pydantic `model_dump` default tests, including the full `ScyllaYaml` default-dict test
  - `test_keystore.py:TestSSHKeyNamedTuple`
  - `test_spark_migrator.py`: `test_migrator_config_defaults` and `test_migrator_config_custom`
  - `test_uda.py` and `test_udf.py`: the `test_create_*_instance` tests
  - `test_argus_replay_log.py`: `test_replay_only_attribute` and `test_normal_mode_replay_only_is_false`
  - `rest/test_rest_client.py`: `test_prepare_request_still_works`
  - `test_cluster_cassandra.py`: the no-op `test_update_seed_provider_is_noop` and `test_validate_seeds_is_noop`
- **Delete zero-value integration tests**
  - Permanently skipped: `test_alternator_streams_kcl.py` and `test_cassandra_harry.py` (whole files, each holding one skipped test), `test_kafka.py:test_01_kafka_cdc_source_connector`, and `test_ndbench_thread.py:test_03_dynamodb_api`, which pins "BUILD FAILED" events from an unsupported mode.
  - Duplicates:
    - `test_config.py`: `test_12_scylla_version_ami_case1` and `test_12_scylla_version_repo_case1`
    - `test_docker_simulated_racks.py`: `test_rack_visibility` and `test_keyspace_creation_with_rf2`, covered by `test_reuse_cluster_preserves_racks`
    - `test_oci_utils_integration.py`: `test_get_compute_client` and `test_list_instances_empty`
  - Tautologies and library checks:
    - `test_catalog_generator.py`: the `*_returns_instance_type_info_objects` and `*_cloud_field` tests
    - `test_aws_services.py`: `test_01_keystore`, which reads back its fixture
    - `test_oci_utils_integration.py`: `test_get_compute_client_with_region`
  - Vacuous tests: in `test_find_ami_equivalent.py`, the three tests whose asserts sit behind `if results:` and whose source AMI is gone. `test_integration_find_equivalent_ami` stays and covers the function.
  - Dead code: the unused `gemini_thread` and `gemini_thread_no_oracle` fixtures in `test_gemini_thread.py`.
- **Move mislabeled integration tests to `unit_tests/unit/`**
  - These tests are marked integration but mock every external call. Moving them runs them in the fast unit job and does not change what they check:
    - `test_provisioning.py:test_init_resources` (whole file, 12 cases)
    - `test_base_version.py` (3 tests)
    - `test_config_get_version_based_on_conf.py`: the three `*_missing_scylla_version_tag` tests and `test_relocatable_version_resolves_unified_package`
    - `test_config.py`: the two `test_xcloud_replication_factor_*` tests
    - `test_utils_issues.py`: the two `test_parse_issue_*` tests
  - Delete the tautological `unit/test_sct_events_filters.py:test_events_severity_changer_filter_gce_first_boot_bind_race`, which rebuilds the filters itself instead of calling the production code. The real-events default-filter tests stay in `integration/test_events.py`: each pays ~4 s of event device setup, which would slow the unit job.
- **Check each deletion against coverage**
  - Re-run per-test coverage.
  - Confirm that no `sdcm/` or `utils/` line covered before the change is left uncovered afterwards.
  - Restore any deleted test whose lines lose all coverage.

Separate follow-ups, not in this PR:
- **Fix tests that never fail.** These are fixes, not deletions:
  - `try/except` without `pytest.raises` in `test_config.py`
  - the inverted nvme-default assertion
  - `test_uda.py:test_load_all_udas` asserting on UDFs
  - the `test_decode_backtrace.py` case that asserts known-buggy behaviour
- **Trim over-parametrized cases** (about 1,700 cases that repeat a code path another case already runs), e.g. `test_scylla_versions_decorator_positive`, `test_base_node_cpuset`, and the precheck matrix in `test_sla.py`.
- **Give integration smoke tests a real assertion.** These pass even when the tool fails, because stress failures become events rather than exceptions:
  - `test_latte_thread.py`: `test_01` and `test_02`
  - `test_ndbench_thread.py`: `test_01` and `test_02`
  - `test_kafka.py`: `test_02`
  - `test_aws_services.py`: `test_03_provision`
  - `test_config.py`: `test_20_user_data_format_version_azure`
  - `test_utils_database_query_utils.py`: `test_fetch_all_rows` (all rows share one key, so paging never runs)

## Files to Modify
- `unit_tests/test_cassandra_docker_cluster.py` -- delete
- `unit_tests/unit/test_manual.py` -- move to `unit_tests/manual/`
- `unit_tests/unit/test_mgmt_operations_stress_load.py` -- delete
- `unit_tests/unit/test_remoter.py` -- move the manual class and its command table to `unit_tests/manual/`, remove the stub-only sudo test
- `unit_tests/integration/test_vector_install_script.py` -- move to `unit_tests/manual/`
- `unit_tests/integration/test_kernel_panic.py` -- move to `unit_tests/manual/`
- `unit_tests/integration/test_azure_vm_provider_reboot.py` -- move to `unit_tests/manual/`
- `unit_tests/manual/` (new directory) -- the moved manual tests, a conftest for the fixtures they need, and a README with run instructions
- `docs/kernel-panic-detection.md` -- update the test path and run command
- `unit_tests/unit/test_sct_events_continuous_events_registry.py` -- remove the skipped test
- `unit_tests/unit/test_nvme.py` -- remove the unused constants
- `unit_tests/unit/test_health_check_skip.py` -- remove the tautological tests
- `unit_tests/unit/test_version_utils.py` -- remove the tautological routing/normalization tests
- `unit_tests/unit/test_sct_runner.py` -- remove the tautological, mock-only and duplicate tests
- `unit_tests/unit/test_config.py` -- remove the mock-echo and duplicate tests
- `unit_tests/unit/test_keystore.py` -- remove the tautological, duplicate and namedtuple tests
- `unit_tests/unit/test_sct_events_base.py` -- remove the fixture-echo test
- `unit_tests/unit/test_ip_to_node_map.py` -- remove the mock-only and OCI-mirror tests
- `unit_tests/unit/test_emr_provisioner.py` -- remove the tautological security-group test
- `unit_tests/unit/test_utils_common.py` -- remove the always-true redaction test
- `unit_tests/unit/test_log_archive.py` -- remove the wrong-directory test
- `unit_tests/unit/rest/test_rest_client.py` -- remove the mock-only, library and duplicate tests
- `unit_tests/unit/nemesis/execute_nemesis/test_argus.py` -- remove the duplicates
- `unit_tests/unit/nemesis/execute_nemesis/test_healthcheck.py` -- remove the duplicate
- `unit_tests/trigger_matrix/test_filtering.py` -- remove the duplicates
- `unit_tests/trigger_matrix/test_yaml_files.py` -- remove the duplicates
- `unit_tests/unit/test_cluster_oci.py` -- remove the `restart()` duplicates
- `unit_tests/unit/test_clean_cloud_resources_func.py` -- remove the tag-branch duplicates
- `unit_tests/unit/test_common_get_db_tables.py` -- remove the duplicate
- `unit_tests/unit/test_instance_matcher.py` -- remove the duplicates
- `unit_tests/unit/test_s3_storage_download.py` -- remove the duplicate
- `unit_tests/unit/test_spark_migrator.py` -- remove the duplicate and dataclass tests
- `unit_tests/unit/test_argus_replay_log.py` -- remove the duplicate and isinstance tests
- `unit_tests/test_cluster_cassandra.py` -- remove the duplicate and no-op tests
- `unit_tests/unit/test_scylla_yaml.py` -- remove the pydantic default tests
- `unit_tests/unit/test_uda.py`, `unit_tests/unit/test_udf.py` -- remove the constructor-echo tests
- `unit_tests/unit/test_sct_events_filters.py` -- remove the tautological GCE bind-race test
- `unit_tests/integration/test_alternator_streams_kcl.py` -- delete
- `unit_tests/integration/test_cassandra_harry.py` -- delete
- `unit_tests/integration/test_kafka.py` -- remove the skipped CDC-source test
- `unit_tests/integration/test_ndbench_thread.py` -- remove the dynamodb test
- `unit_tests/integration/test_config.py` -- remove the `*_case1` duplicates, move the xcloud tests to unit
- `unit_tests/integration/test_docker_simulated_racks.py` -- remove the duplicates
- `unit_tests/integration/test_oci_utils_integration.py` -- remove the duplicate and library tests
- `unit_tests/integration/test_catalog_generator.py` -- remove the isinstance/cloud-field tests
- `unit_tests/integration/test_aws_services.py` -- remove the fixture-echo keystore test
- `unit_tests/integration/test_find_ami_equivalent.py` -- remove the three vacuous tests
- `unit_tests/integration/test_gemini_thread.py` -- remove the unused fixtures
- `unit_tests/integration/test_provisioning.py` -- move to `unit_tests/unit/`
- `unit_tests/integration/test_base_version.py` -- move to `unit_tests/unit/`
- `unit_tests/integration/test_utils_issues.py` -- move to `unit_tests/unit/`
- `unit_tests/integration/test_config_get_version_based_on_conf.py` -- move the mocked tests to unit
- `unit_tests/unit/test_config.py` -- receives the moved xcloud tests

## Verification
- [ ] `uv run sct.py unit-tests` passes, with about 50 fewer collected tests than on `upstream/master`: about 99 deleted, about 49 moved in from `integration/`.
- [ ] The moved tests pass without network access: `uv run pytest unit_tests/unit -m "not integration"` with no cloud credentials set.
- [ ] `uv run sct.py integration-tests` runs on CI, with about 60 fewer passing tests than on `upstream/master` (moved to `unit/` or deleted), and no new failures.
- [ ] Per-test coverage: the `sdcm/` + `utils/` covered-line set after the change equals the set before it. Run `COVERAGE_CORE=ctrace uv run --with pytest-cov pytest unit_tests -m "not integration" -n 20 --cov=sdcm --cov=utils --cov-report=` before and after, then diff `coverage json` outputs. `COVERAGE_CORE=ctrace` is required on Python 3.14.
- [ ] `git grep` finds no remaining references to the deleted files, classes or constants, or to the old paths of the moved manual tests.
- [ ] Collection of `unit_tests/manual/` succeeds, i.e. `uv run pytest unit_tests/manual --collect-only -p no:skipping` finds every manual test. Under the default `unit-tests` and `integration-tests` runs they all report as skipped.
- [ ] `uv run sct.py pre-commit` passes.
