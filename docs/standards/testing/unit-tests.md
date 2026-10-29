## Unit Tests

### Pytest Functions

Write top-level `def test_*` functions with plain `assert` statements. Do not
write `class Test*` groups, `unittest.TestCase` subclasses, `setUp()`, or
`self.assertEqual()`. Put shared setup in module-level helper functions or
fixtures.

### Fixtures and Parametrize

Use `@pytest.fixture` for setup and teardown, and get the fixture through a
function parameter. Use `scope="module"` for expensive setup. Put the fixtures
that many modules share in `unit_tests/conftest.py`. Use
`@pytest.mark.parametrize` to test more than one input.

### Location and Names

Put unit tests in `unit_tests/unit/` and integration tests in
`unit_tests/integration/`, in files named `test_*.py`. Mark a test that needs
an external service with `@pytest.mark.integration`.

### Run the Tests You Touch

Run `uv run sct.py unit-tests -t unit/<test file>` for each unit test file
that you add or change. The path is relative to `unit_tests/`. Run
`uv run sct.py unit-tests` before you open the pull request. Run the
integration tests with `uv run sct.py integration-tests`.

### A Test for New Logic

New logic gets a unit test that asserts its behavior. A bug fix gets a test
that fails before the fix and passes after it, when a test can reproduce the
bug.

Sources: `AGENTS.md` section Unit Testing Guidelines,
`skills/writing-unit-tests/SKILL.md`, `skills/writing-integration-tests/SKILL.md`,
`skills/code-review/SKILL.md` check 5, and the code: 349 test modules with
top-level test functions.
