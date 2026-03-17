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

Before you begin, ensure you have the following installed on your system:

- Git
- A supported operating system (Linux, macOS, or Windows with WSL)
- Basic system development tools (build-essential, etc.)

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

Add the following line to your shell configuration file:

```bash
watch_file requirements.txt pyproject.toml .python-version

export PATH=$(echo $PATH | tr : '\n' | grep -v 'shims' | tr '\n' :)

export UV_VENV_CLEAR=1
export UV_PYTHON=`cat .python-version || echo '3.10'`
export UV_PYTHON_PREFERENCE=only-managed
export UV_PROJECT_ENVIRONMENT=.venv
uv venv -p $UV_PYTHON $UV_PROJECT_ENVIRONMENT

# activate the virtualenv after syncing; this puts the newly-installed
# binaries on PATH.
venv_path=$(expand_path "${UV_PROJECT_ENVIRONMENT:-.venv}")
if [[ -e $venv_path ]]; then
  # shellcheck source=/dev/null
  source "$venv_path/bin/activate"
fi
if [[ -e requirements.txt ]]; then
  uv pip sync requirements.txt || true
else
  uv sync --all-groups || true
fi


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

Most modern IDEs will automatically detect the virtual environment created by uv. For VS Code, you can:

1. Install the Python extension
2. Open the command palette (Ctrl+Shift+P)
3. Select "Python: Select Interpreter"
4. Choose the interpreter from `.venv/bin/python`

## Additional Resources

- [uv Documentation](https://docs.astral.sh/uv/)
- [direnv Documentation](https://direnv.net/)
- [SCT Documentation](./README.md)
- [Python Packaging Guide](https://packaging.python.org/)
