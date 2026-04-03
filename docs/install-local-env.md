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


# SCT test profiling (OUTDATED)
-
- set environment variable "SCT_ENABLE_TEST_PROFILING" to 1, or add "enable_test_profiling: true" into yaml file
- run test

After test is done there are following ways to use collected stats:
- `cat ~/latest/profile.stats/stats.txt`
- `snakeviz ~/latest/profile.stats/stats.bin`
- `tuna ~/latest/profile.stats/stats.bin`
- `gprof2dot -f pstats ~/latest/profile.stats/stats.bin | dot -Tpng -o ~/latest/profile.stats/stats.png`

Another way to profile is py-spy:
- `pip install py-spy`
Run recording:
- `py-spy record -s -o ./py-spy.svg -- python3 sct.py ...`
Run 'top' mode:
- `py-spy top -s -- python3 sct.py ...`
