# Asserts and Inputs

What to assert, where inputs and expected values come from, and how to prove a test can fail. Read it before Phases 3 and 4 of the workflow. Each rule shows only the shape to follow.

## UT-21: Assert Exact Calls, Errors, and Results

Check what the code produces, not how it gets there: feed real input and assert on the output.

When the effect itself is a call to an external boundary (a remote command, a cloud API, a CQL statement), the mock call is the observable effect: assert the full call with `assert_called_once_with(...)`. Don't check only `call_count`, `assert_called_once()`, or one argument picked out of `call_args` (#14884, #15591, #15607). The exact call catches a wrong argument, and the failure message shows the difference.

```python
def test_modify_table_property_forwards_filter_out_counter(runner):
    """filter_out_table_with_counter is forwarded to get_non_system_ks_cf_list."""
    monkey = ModifyTableCommentMonkey(runner)
    monkey.modify_table_property(name="x", val=1, filter_out_table_with_counter=True)

    monkey.runner.cluster.get_non_system_ks_cf_list.assert_called_once_with(
        db_node=monkey.runner.target_node,
        filter_out_table_with_counter=True,
        filter_out_mv=True,
    )
```

For several calls, compare the whole list, written out literally: `assert mock.call_args_list == [call(...), call(...)]`. For calls that take no arguments, the order is the effect: attach the mocks to one parent with `attach_mock` and compare `parent.mock_calls` (#13718, #15160).

The same exactness applies to every result:

- **Errors**: `pytest.raises(SpecificError, match="...")`. `pytest.raises(Exception)` also catches the `AttributeError` of a method that doesn't exist, so the test passes for the wrong reason (#13527, #14818, #15686). Validation cases become one parametrized test of input, exception, and pattern.
- **Generated text**: one `==` on the whole command or string, not several `in` checks (#13509).
- **Everything the test put in**: a test that feeds ten fields asserts all ten; a test that provokes three errors asserts the exact list (#14818).
- **Counts**: `assert len(results) == 1`, not `assert results`, so a duplicate match fails (#16457).
- **Cleanup**: compare the state before and after the act, not the number of commands issued (#16090).
- **The path not taken**: the test of the generic path asserts the special behavior did not happen, with `assert_not_called()`. Keep those asserts when editing a test (#16180, #16535).
- **The real cause first**: when the code collects errors instead of raising, assert `not errors` before the derived check, so a failure shows the cause (#16450).
- **The goal, once**: pick the assert that states what the test is for, and drop asserts it already implies (#14062, #14182).

## UT-22: Every Assert Checks the Case Under Test

Each assert must be able to fail because of the code under test, in this case (#15604, #16063, #16518, #16603, #16735).

- Assert on values the code produced, not on preconditions the test arranged itself.
- A mock's return value is input. Assert on what the code does with it, not on the value the mock was given.
- Assert on this case's result, not on a fact that holds for every case (such as the default severity of an event type).
- Assert on outcomes (what was chosen, returned, raised, or run), not on internal state left untouched. For seeded randomness, two runs with the same seed make the same choices.
- Aim at the effect that matters. For a network nemesis, that is the firewall rules it adds, not whether it was discovered.

Adapted from `test_sct_events_database.py`: the line is classified, and the classified event carries the suppressed severity:

```python
def test_info_level_runtime_error_line_is_suppressed():
    """Real example (SCT-772) that used to become RUNTIME_ERROR/ERROR."""
    line = (
        "2026-07-30T11:37:43.815 longevity-10gb-3h-master-db-node-41da73ad-eastus-1  !INFO | scylla[12796]  "
        "[shard 0: gms] rpc - client 10.0.0.9:7000: ignoring error response: seastar::rpc::remote_verb_error "
        "(std::runtime_error (Got stream_blob_cmd::error from peer 7336616b-4533-4c59-a3d1-f6d825de4d8a))"
    )
    event = classify(line)

    assert event.type == DatabaseLogEvent.INFO.type
    assert event.severity == Severity.SUPPRESS
```

## UT-23: Assert Every Claim in the Test Name

Reviewers trust the name. "Never reaches the network", "does not retry", and "logs a warning" each need an assert that fails when the claim breaks (#16093). Abridged from `unit_tests/unit/lint/test_validator.py`:

```python
def test_validate_pipeline_cloud_lookup_never_reaches_the_network(
    pipeline, test_data_dir, remote_connections, restore_root_logger
):
    """Linting resolves images from stubs and opens no outbound connection."""
    ...
    is_error, message = validate_pipeline(pipeline_path, env)

    assert remote_connections == [], f"linting {pipeline} reached the network: {remote_connections}"
    assert not is_error, message
```

The `remote_connections` fixture in `unit_tests/unit/lint/conftest.py` monkeypatches `socket.socket.connect` and `connect_ex`, lets loopback through, and returns the list of refused addresses. It refuses as well as records, so a missing stub never makes a real call.

## UT-24: Inputs From the Test, Expected Values From Intent

- **A bug found in a test run**: use the real log lines or command output from that run as input (#16063, #16665). Keep them inline if short, under `unit_tests/test_data/` if long, and name the issue or run in the docstring.
- **Expected values**: derive them from the intended behavior (the issue, the docs, the reasoning in the PR), never by running the code and copying its output. #14983 found a bug that an earlier test had pinned as expected.
- **Expected numbers**: write the worked-out literal for each scenario. A named production constant may be an input (#14304), but the test never re-runs the formula under test to get its expectation (#16324).
- **Negative cases**: use values that are obviously invalid, such as `"nonexistent_option"`, so the reader knows they are not real (#16710).
- **Arbitrary URLs, hosts, and ids**: obvious stubs such as `https://example.com/build.tar.gz`. A production URL can reach the real service and pollute its logs (#15695).
- **Input shape**: build the input the way the real caller builds it, in the same argument order (#15769).
- **Copies of production data**: a pattern list copied into a test keeps the production order, since order decides which pattern matches. Better, import the shared constant (#15382).
- **External tool formats**: pin the output format of a CLI with a real sample in a test, not with prose in a docstring. The test fails when the format changes (#15591).
- **Constants**: inline a value used once. Data several tests share is a fixture (UT-6); module constants are for fixed strings such as a patch-target `MODULE` (#16589, #16633, #16651).

**Inputs belong to the test.** Load inputs from `unit_tests/test_data/` through the `test_data_dir` fixture. A minimal test-only file keeps the test stable when production jobs change, and keeps covering the path when they stop using it (#16093).

```groovy
// unit_tests/test_data/lint/aws-capacity-reservation.jenkinsfile
// Test-only pipeline for unit_tests/unit/lint/test_validator.py -- not a real job.
longevityPipeline(
    backend: 'aws',
    region: 'eu-west-1',
    test_name: 'longevity_test.LongevityTest.test_custom_time',
    test_config: '''["unit_tests/test_data/lint/base.yaml", "unit_tests/test_data/lint/capacity-reservation.yaml"]''',
)
```

Keep each file to the few keys that select the path under test, and say in a comment that it is not a real job. Code whose purpose is to process every production file (the `lint-pipelines` command) is not a unit test.

The same holds for `configurations/`, `test-cases/`, and the trigger yamls: unit tests read their own minimal copies, so the tests check code and don't fail when a production file changes (#15380).

## UT-25: Mutation-Check Every New Test

A passing test proves its assertions held. A mutation check proves the code under test is what made them hold. Do this for each new test before calling it done:

1. Break the code under test the way the test claims to catch it: delete the stub entry, invert the condition, return early.
2. Run the test and confirm it **fails**, with a message that points at the break.
3. Revert the break and confirm the test passes again.

```bash
git add sdcm/utils/lint/validator.py                                    # snapshot the file as it is now, committed or not
# comment out one entry in _CLOUD_API_PATCHES, then:
uv run python -m pytest unit_tests/unit/lint/test_validator.py -n0 -q   # must FAIL
git restore sdcm/utils/lint/validator.py                                # back to the snapshot
```

`git restore <file>` restores from the index. The code under test is often uncommitted, and the snapshot in the index keeps it.

For each stub, list what the real code does that the stub skips, and cover the parts the fix depends on with their own test (#16142). Name the mutations you checked in the PR description, for example: "with the `_shutdown_pool()` calls removed, both parametrizations fail".

**For a bug fix, run every new case against the pre-fix code** (#15727, #16408, #16545, #16665). Save the fix as a patch, so it survives whether it is committed or not. `<base>` is the commit the branch started from, such as `$(git merge-base HEAD master)`:

```bash
git diff --output=/tmp/fix.patch <base> -- sdcm/                         # the fix
git apply -R /tmp/fix.patch                                              # pre-fix code
uv run python -m pytest unit_tests/unit/test_<subject>.py -n0 -q         # every new case FAILS
git apply /tmp/fix.patch                                                 # restore the fix
```

Each case must fail on an assert about the bug. A case that passes on the old code proves nothing; add the input that triggers the bug. A case that fails only with an `AttributeError` because the fixed method is new, or a `TypeError` from a mock's child attribute, doesn't pin the bug either. Check those cases with a mutation of the new code instead.
