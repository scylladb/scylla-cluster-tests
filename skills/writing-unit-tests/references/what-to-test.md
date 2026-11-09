# What to Test

Which behaviors get a test, and which are not test subjects. Read it before Phase 1 of the workflow. Each rule shows only the shape to follow.

## UT-1: Cover Every Behavior the Change Adds

Most "missing tests" review comments name a branch the PR added and no test reaches (#14463, #14561, #15456, #16345, #16491, #16545, #16674, #16769). Before writing tests, list what the diff adds or changes, and give each item a case:

- **Each new branch, guard, and parser path**, including the happy path and the fallback (#14561, #16233).
- **Each changed argument at a call site**, pinned with `assert_called_once_with` (#16836).
- **The caller path, not only the helpers.** Send the input through the entry point the backend uses (#16378, #16462).
- **The behavior the fix is for.** A bug fix tests the motivating behavior, and the PR description names the bug and the test (#14669, #15364).

Logic that sits in a module no unit test can import (a test class such as `mgmt_cli_test.py`) moves to `sdcm/` first (#16251). A branch that only formats an error message, or a one-line guard that a CI stage already runs, may go without a unit test (#16532).

## UT-2: Test Each Behavior Once

- **Before adding a test**, search `unit_tests/` and the linters for one that already checks the behavior. Extend it, or keep the better of the two (#14062, #14370, #14616, #15608).
- **After changing code**, delete tests the change made identical to another or no longer describe, and drop parameter rows that another test already covers (#16568, #16651, #16910).
- **Test what the PR changed**, not unchanged code that other tests cover (#15604, #15608).
- **Rename a test when its asserts outgrow its name**, or split it (#16524).

## UT-3: Assert Behavior, Not Declarations

Test the behavior a definition exists for, through the public entry point. A declaration that is read back unchanged proves nothing about the code that uses it. These are declarations:

- **Constants and lookup tables**, e.g. `x in CONSTANT` (#16093).
- **Nemesis flags** (`disruptive`, `kubernetes`, ...). They change as nemeses evolve and are a configuration choice, not behavior (#14884, #15486, #15607).
- **Registry discovery and `__all__`** for a nemesis you added or moved. Discovery is covered once in `unit_tests/unit/nemesis/test_registry.py` (#15604, #15857).
- **Behavior tied to an internal constant nobody can configure**, such as a batch size (#15591).
- **Static yaml and config contents**: keys present, list lengths, a script name in a string. `lint_test_cases` checks test-case files (#15608).
- **Structure**: method signatures, inheritance, or "the class declares X". Test the path that uses them (#14135, #14734, #15160).
- **A default that raises `NotImplementedError`**: make the method `abc.abstractmethod` instead (#15726).
- **Unsupported features**: when support is dropped, delete its params, tests, and references. Don't add a test that it is rejected (#15906).
- **Relationships between constants**: if `A` must exceed `2 * B`, define `A` from `B` in production (#16124).
- **The mock's own setup, or one-line math on a mock** (#16040).
- **Library behavior**, such as `uuid4()` returning unique values (#16455).
- **Scenarios production cannot reach**, such as a test id that changes mid-run (#15418).

An invariant over every declaration (no config field name contains `__`, #15716) may be a test when it fails on the broken state and no linter rule can enforce it. Where a linter rule exists, such as ruff's D rules for docstrings, enable the rule instead (#16604, #16734).

For a nemesis, test what `disrupt()` does: the commands it runs, the nodes it picks, the errors it raises. In #16093, a test sends a pipeline down each cloud lookup path and asserts that it never reaches the network (shown under UT-23). A missing stub now makes the matching pipeline try to connect, so the test fails for the real reason.

## UT-4: Test Non-Python Code With Its Own Runtime or Through the Python Decision

Bash scripts that Python helpers generate, Groovy in `vars/*.groovy` and `*.jenkinsfile`, and `*.sh` files are checked by running them. Their text is not a test subject. Pick the first option that applies (#16165):

1. **Run the code with its own runtime.** Use the language's test tool (bats for shell, JenkinsPipelineUnit for Groovy) or a runnable test script that executes the real code and checks the result, like `.github/workflows/test_cache_issues.sh` and `scripts/test-renovate-local.sh`. A bash script that Python generates gets at least a syntax check, as in `unit_tests/unit/provisioner/test_docker_user_data.py` (#16124):
   ```python
   subprocess.run(["bash", "-n"], input=script, text=True, check=True)
   ```
2. **Test the Python logic around it.** Move the decision (distro checks, version gates, computed timeouts, package lists) into a function that returns plain data and test that function. `should_skip_epel` is an illustration, not an existing helper:
   ```python
   @pytest.mark.parametrize("distro,expected", [
       pytest.param(Distro.FEDORA36, True, id="fedora-skips-epel"),
       pytest.param(Distro.ROCKY9, False, id="rocky-uses-epel"),
   ])
   def test_skip_epel_by_distro(distro, expected):
       """EPEL is skipped on Fedora and used on Rocky."""
       assert should_skip_epel(distro) is expected
   ```
   How Python handles command output is also behavior. Return output through `FakeRemoter.result_map`, then assert on what the code does with it: parsed values, raised errors, retries, events.
3. **Otherwise, test it in a real job and write no unit test.** Run a shell change on a node (a `test-provision-*` label, a Docker integration test from the `writing-integration-tests` skill, or a manual VM run). Run a Groovy change in a real Jenkins job, for example through `staging_trigger.py` (see `docs/contrib.md`). Link the run in the PR description.

This rule does not cover functions whose output *is* a computed command line (stress-tool or `nodetool` argument builders): compare parsed tokens with `shlex.split(cmd)`. It also does not cover Python parsers of non-Python text, like `unit_tests/unit/lint/test_jenkins_parser.py`, where the text is input and the parser's output is checked.
