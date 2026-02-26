# Setting up SCT with uv and direnv

This guide provides instructions for setting up the SCT (Scylla Cluster Tests) development environment using [uv](https://docs.astral.sh/uv/) as the Python package manager and [direnv](https://direnv.net/) for automatic environment management.

## Benefits of this setup

1. **Fast dependency resolution**: uv is significantly faster than pip
2. **Automatic environment management**: direnv handles activation/deactivation
3. **Reproducible environments**: Lock files ensure consistency
4. **Isolation**: Each project has its own environment
5. **Modern tooling**: Leverages the latest Python packaging standards
6. **Cross-platform compatibility**: Works on Linux, macOS, and Windows (WSL)

## Prerequisites

Those are proven to work on a Linux based operating system.

Before you begin, ensure you have the following installed on your system:

- Git
- Basic system development tools (build-essential, or equivalent for your distro)

## Installation

### 1. Install uv

[uv](https://docs.astral.sh/uv/) is a fast Python package installer and resolver, written in Rust.

#### On Linux/macOS:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### On Windows (PowerShell):
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Alternative installation methods:
```bash
# Using pip
pip install uv

# Using pipx
pipx install uv

# Using Homebrew (macOS)
brew install uv

# Using apt (Ubuntu/Debian)
sudo apt install uv
```

### 2. Install direnv

[direnv](https://direnv.net/) automatically loads and unloads environment variables based on the current directory.

#### On Linux/macOS:
```bash
# Using package managers
# Ubuntu/Debian
sudo apt install direnv

# Fedora/RHEL
sudo dnf install direnv

# macOS with Homebrew
brew install direnv

# Or using the installer script
curl -sfL https://direnv.net/install.sh | bash
```

#### Configure direnv for your shell:

The following code block is an example `.envrc` file for use with direnv.
**Save this as `.envrc` in your project root directory** to enable automatic environment management with direnv and uv.

```bash
watch_file requirements.txt pyproject.toml .python-version

# make sure to not enable pyenv shims, if it's installed, it would conflict with uv
export PATH=$(echo $PATH | tr : '\n' | grep -v 'shims' | tr '\n' :)

export UV_VENV_CLEAR=1
export UV_PYTHON=`cat .python-version`
export UV_PYTHON_PREFERENCE=only-managed
export UV_PROJECT_ENVIRONMENT=.venv-sct-$UV_PYTHON
uv venv -p $UV_PYTHON $UV_PROJECT_ENVIRONMENT

# activate the virtualenv after syncing; this puts the newly-installed
# binaries on PATH.
venv_path=$(expand_path "${UV_PROJECT_ENVIRONMENT:-.venv}")
if [[ -e $venv_path ]]; then
  # shellcheck source=/dev/null
  source "$venv_path/bin/activate"
fi
if [[ -e requirements.txt ]]; then
  # handle old branch, that we didn't used pyproject.toml yet
  uv pip sync requirements.txt || true
else
  uv sync --all-groups || true
fi

# you can drop this part if you don't find it useful
echo "✅ SCT development environment activated!"
echo "🐍 Python version: $(python --version)"
echo "📦 uv version: $(uv --version)"
echo "📁 Virtual environment: ${VIRTUAL_ENV}"
echo ""
echo "Available commands:"
echo "  uv add <package>     - Add a new dependency"
echo "  uv sync              - Update dependencies"
echo "  hydra bash           - Enter SCT containerized environment"
echo "  pytest               - Run tests"
```

### 4. Initialize the environment

Allow direnv to load the environment:

```bash
direnv allow
```

This will:
- Create a Python virtual environment using uv
- Install all dependencies from `pyproject.toml`
- Set up necessary environment variables
- Add the project to PYTHONPATH

## Usage

### Daily workflow

When you `cd` into the SCT directory, direnv will automatically:
- Activate the virtual environment
- Load environment variables
- Ensure dependencies are up to date

When you leave the directory, the environment will be automatically deactivated.

### Installing new packages

To add new dependencies:

```bash
# Add to pyproject.toml dependencies, then:
uv sync

# Or add directly:
uv add package-name

# For development dependencies:
uv add --dev package-name
```

### Running tests

```bash
# The environment is automatically activated when in the SCT directory
python -m pytest tests/

# Or run SCT tests
uv run python sct.py --help
```

### IDE Integration

Most modern IDEs will automatically detect the virtual environment created by uv. (make sure you have upto-date IDE versions/extensions installed)

If they didn't for example in VS Code, you can:

1. Install the Python extension
2. Open the command palette (Ctrl+Shift+P)
3. Select "Python: Select Interpreter"
4. Choose the interpreter from `${UV_PROJECT_ENVIRONMENT}/bin/python`

## Additional Resources

- [uv Documentation](https://docs.astral.sh/uv/)
- [direnv Documentation](https://direnv.net/)
- [SCT Documentation](./README.md)
- [Python Packaging Guide](https://packaging.python.org/)


# Profiling SCT Code

Use standard Python profiling tools to analyze SCT performance. Below are the recommended approaches.

### Python 3.14 compatibility

| Tool | Python 3.14 | Notes |
|------|:-----------:|-------|
| cProfile (stdlib) | ✅ | Part of Python, always compatible |
| scalene | ✅ | Officially supports 3.14 |
| viztracer | ✅ | Supports 3.14 since v1.1.0 |
| memray | ✅ | Officially supports 3.14 (Linux and macOS only) |
| py-spy | ❌ | Not yet — tracking [issue #750](https://github.com/benfred/py-spy/issues/750) |
| snakeviz | ⚠️ | Pure-Python viewer, likely works but no explicit 3.14 classifiers |
| gprof2dot | ⚠️ | Pure-Python, likely works but minimally maintained |

## cProfile + snakeviz (stdlib deterministic profiler)

Python's built-in `cProfile` is a deterministic profiler that records every function call and return, measuring exact call counts and cumulative time. It is best for identifying which functions consume the most total time. Because it instruments every call, it adds overhead to the profiled code — so results show relative hotspots rather than absolute wall-clock time.

**Workflow:** Run your code under cProfile → sort by `cumtime` (cumulative time) or `tottime` (self time) → drill into the top entries with snakeviz or gprof2dot to find the call chains responsible.

- [cProfile docs](https://docs.python.org/3/library/profile.html)
- [snakeviz docs](https://jiffyclub.github.io/snakeviz/)
- [gprof2dot repo](https://github.com/jrfonseca/gprof2dot)

### Profile a full SCT test run

```bash
python3 -m cProfile -o ./profile.stats sct.py run-test ...
```

### Profile a specific unit test with pytest

```bash
python3 -m cProfile -o ./profile.stats -m pytest -xvs unit_tests/test_config.py::test_config_default
```

### Visualize the results

With [snakeviz](https://jiffyclub.github.io/snakeviz/) (interactive sunburst in the browser):
```bash
uv pip install snakeviz
snakeviz ./profile.stats
```

Or generate a call graph image with [gprof2dot](https://github.com/jrfonseca/gprof2dot):
```bash
uv pip install gprof2dot
gprof2dot -f pstats ./profile.stats | dot -Tpng -o ./profile.png
```

### Finding bottlenecks

1. Open the snakeviz sunburst — the widest arcs are the most expensive functions.
2. Click to zoom into a subtree and trace the call chain down.
3. Switch to the "icicle" view for a top-down call-stack perspective.
4. In the stats table, sort by `cumtime` to see functions that, including callees, take the most time; sort by `tottime` to find functions that are themselves slow.

## scalene (CPU + memory + GPU profiler)

[Scalene](https://github.com/plasma-umass/scalene) is a line-level profiler that simultaneously measures CPU time (split into Python vs. native), memory allocations, memory leaks, and GPU usage — all with low overhead. It is best for identifying line-level hotspots and memory-heavy code paths without separate tools.

**Workflow:** Run under scalene → open the HTML report → look at the "CPU %" and "Memory" columns to find lines with disproportionate cost → focus optimization on those lines.

- [Scalene repo & docs](https://github.com/plasma-umass/scalene)

### Profile a full SCT test run

```bash
uv pip install scalene
scalene run sct.py --- run-test ...
```

### Profile a specific unit test with pytest

```bash
scalene run --- -m pytest -xvs unit_tests/test_config.py::test_config_default
```

### Finding bottlenecks

1. Open the generated HTML report (opens automatically in a browser).
2. Lines highlighted in red are CPU hotspots; the "Memory" column shows allocation intensity.
3. The "Python" vs. "C" split shows whether time is in Python code (optimize-able) or native extensions (harder to change).
4. Use `--cpu-only` for faster profiling when memory is not a concern.

## viztracer (timeline trace visualization)

[VizTracer](https://github.com/gaogaotiantian/viztracer) records every function entry/exit with timestamps, producing a timeline trace. It is best for understanding execution flow, concurrency, and ordering of operations across threads. View traces in Chrome's `chrome://tracing` or [Perfetto](https://ui.perfetto.dev/).

**Workflow:** Run under viztracer → open the trace in Perfetto → zoom into slow regions on the timeline → expand thread lanes to see which calls overlap and which are sequential.

- [VizTracer repo & docs](https://github.com/gaogaotiantian/viztracer)
- [Perfetto trace viewer](https://ui.perfetto.dev/)

### Profile a full SCT test run

```bash
uv pip install viztracer
viztracer --tracer_entries 10000000 -o ./result.json -- python3 sct.py run-test ...
vizviewer ./result.json
```

### Profile a specific unit test with pytest

```bash
viztracer --tracer_entries 10000000 -o ./result.json -- python3 -m pytest -xvs unit_tests/test_config.py::test_config_default
vizviewer ./result.json
```

### Finding bottlenecks

1. Open the trace in Perfetto (`vizviewer` does this automatically).
2. Zoom into the timeline — wide bars are long-running calls.
3. Expand thread lanes to see concurrent activity and identify idle gaps.
4. Use `--min_duration 1ms` to filter out very short calls and reduce noise.
5. Use `--log_func_args` or `--log_func_retval` to capture argument/return values for deeper investigation.

## memray (memory profiler)

[Memray](https://github.com/bloomberg/memray) tracks every memory allocation and deallocation, showing exactly where memory is allocated, how much, and whether it is leaked. It is best for debugging memory leaks, high-memory-usage hotspots, and understanding allocation patterns. Linux and macOS only.

**Workflow:** Run under memray → generate a flamegraph → look for tall stacks with large allocations → check the "leaks" report for objects that were never freed.

- [Memray repo & docs](https://github.com/bloomberg/memray)

### Profile a full SCT test run

```bash
uv pip install memray
memray run -o ./memray.bin python3 sct.py run-test ...
memray flamegraph ./memray.bin -o ./memray.html
```

### Profile a specific unit test with pytest

Using the [pytest-memray](https://github.com/bloomberg/pytest-memray) plugin:
```bash
uv pip install pytest-memray
python3 -m pytest --memray -xvs unit_tests/test_config.py::test_config_default
```

Or with the `memray run` command directly:
```bash
memray run -o ./memray.bin python3 -m pytest -xvs unit_tests/test_config.py::test_config_default
memray flamegraph ./memray.bin -o ./memray.html
```

### Finding bottlenecks

1. Open the flamegraph HTML — the widest bars are the largest allocators.
2. Run `memray stats ./memray.bin` for a text summary of top allocators.
3. Run `memray tree ./memray.bin` for a tree view of allocations by call stack.
4. Run `memray run --leak-report -o ./memray.bin ...` followed by `memray flamegraph --leaks ./memray.bin` to focus specifically on leaked memory.

## py-spy (sampling profiler)

[py-spy](https://github.com/benfred/py-spy) is a sampling profiler that periodically snapshots the call stack of a running Python process. It requires no code changes, adds near-zero overhead, and can attach to already-running processes. It is best for profiling long-running or production-like tests without slowing them down.

> **⚠️ Python 3.14 not yet supported** — see [benfred/py-spy#750](https://github.com/benfred/py-spy/issues/750). Works with Python ≤ 3.13.

**Workflow:** Record a flame graph → open the SVG in a browser → the widest bars at the bottom are where the most time is spent → trace upward to see which callers are responsible.

- [py-spy repo & docs](https://github.com/benfred/py-spy)
- [speedscope viewer](https://www.speedscope.app/)

### Install

```bash
uv pip install py-spy
```

### Profile a full SCT test run (flame graph)

```bash
py-spy record -s -o ./profile.svg -- python3 sct.py run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml
```

Open `profile.svg` in a browser to explore the flame graph interactively.

### Profile a specific unit test with pytest

```bash
py-spy record -s -o ./profile.svg -- python3 -m pytest -xvs unit_tests/test_config.py::test_config_default
```

### Live top-like view

Monitor a running test in real time:
```bash
py-spy top -s -- python3 sct.py run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml
```

### Attach to a running process

If a test is already running, attach to it by PID:
```bash
py-spy record -s -o ./profile.svg --pid <PID>
py-spy top -s --pid <PID>
```

### Generate speedscope format

For use with [speedscope.app](https://www.speedscope.app/):
```bash
py-spy record -s -o ./profile.speedscope.json --format speedscope -- python3 sct.py ...
```

### Finding bottlenecks

1. Open the flame graph SVG in a browser — wider bars = more time spent.
2. Read bottom-to-top: the bottom is the entry point, the top is the leaf function doing the work.
3. Look for "plateaus" — wide flat bars indicate functions that themselves are slow.
4. Use `--format speedscope` and open in [speedscope.app](https://www.speedscope.app/) for a time-ordered view to see when things happened, not just aggregates.
5. Use `py-spy top` for a live view when you want to monitor a long-running test interactively.

## Tips

- Use **cProfile** when you need deterministic call counts — it works on every Python version.
- Use **scalene** for combined CPU + memory profiling in a single run.
- Use **viztracer** to understand execution flow and concurrency across threads.
- Use **memray** specifically for memory leak investigations.
- Use **py-spy** for zero-overhead sampling on Python ≤ 3.13; once [#750](https://github.com/benfred/py-spy/issues/750) is resolved it will be the best general-purpose choice again.
- For long-running SCT tests, prefer attaching to a running process (`py-spy --pid`) rather than wrapping the launch command.
- When profiling unit tests, always use `-xvs` with pytest to stop on first failure and see output immediately.
