## Python Coding Style

### Formatting and Linting

`ruff format` formats the code and `ruff check` lints it, through pre-commit.
The line length is 120. `pyproject.toml` holds the selected rules and the
pylint limits: at most 12 arguments, 15 locals, 24 branches, and 100
statements in a function.

### Imports at the Top

Put all imports at the top of the module, never inside a function. The only
exception is a cyclic dependency that is hard to remove. Put a comment above
that import to tell why. The `cyclic-imports` pre-commit hook checks the
import graph.

### Module Logger

Name the module-level logger `LOGGER`.

```python
LOGGER = logging.getLogger(__name__)
```

### Lazy Logging Arguments

Pass the values to the logger as arguments, not as an f-string. The logger
then formats the message only when it writes it.

```python
LOGGER.info("Node %s is up after %s seconds", node.name, elapsed)
```

### Docstrings

Write Google style docstrings with `Args`, `Returns`, and `Raises` sections
for a public function or method.

### Comments

A comment tells why the code does something, or names a constraint that the
code does not show. Do not repeat what the code says.

Describe the code as it is. Do not describe an earlier version of the code,
or tell why an earlier approach was wrong. The commit message and the pull
request carry that history.

### No Silent Errors

Do not write a bare `except:` or an `except` block that only passes. Catch the
narrowest exception, and log or raise with enough context to debug the run.
A custom exception subclasses the SCT exception of its area.

Sources: `pyproject.toml`, `.pre-commit-config.yaml`, the former `AGENTS.md` sections
Code Style Guidelines and Documentation Standards, `skills/code-review/SKILL.md`
check 4, the review of pull request #16699, and the code: `LOGGER` in 212 of 238 `sdcm` modules with a module
logger, lazy arguments in 1032 of 1142 `LOGGER.info`, `LOGGER.debug`, and
`LOGGER.warning` calls.
