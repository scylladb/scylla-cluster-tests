## Setup SCT locally development environment

This is the recommend why of building a local development environment
that isn't depended on the distro python version.

<<<<<<< HEAD
To run SCT tests locally run following::
||||||| parent of a7f1294a7 (build(python): move to Python 3.15)
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
=======
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
- librdkafka headers, version 2.12.1 or newer (`librdkafka-dev` on Ubuntu 26.04, `librdkafka-devel` on Fedora,
  `brew install librdkafka` on macOS). `confluent-kafka` has no Python 3.15 wheels yet and builds from source.
  Older distros (e.g. Debian trixie ships 2.8) can use [Confluent's APT repository](https://docs.confluent.io/platform/current/installation/installing_cp/deb-ubuntu.html#get-the-software).

## Installation

### 1. Install uv

[uv](https://docs.astral.sh/uv/) is a fast Python package installer and resolver, written in Rust.
SCT needs uv 0.13.0 or newer (enforced by `required-version` in `pyproject.toml`); older uv resolves
Python 3.15 to a pre-release that current binary wheels can't load. Run `uv self update` if needed.

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
>>>>>>> a7f1294a7 (build(python): move to Python 3.15)

```bash
sudo ./install-prereqs.sh

# install python3.10 via pyenv
curl https://pyenv.run | bash
exec $SHELL
# go to: https://github.com/pyenv/pyenv/wiki/Common-build-problems#prerequisites
# and follow the instructions for your distribution, to install the prerequisites
# for compiling python from source
PYTHON_VERSION=`./docker/env/hydra.sh  python --version | cut -d' ' -f2 | tail -n1 |  tr -d '\r\n'`
pyenv install ${PYTHON_VERSION}

# create a virtualenv for SCT
pyenv virtualenv ${PYTHON_VERSION} sct-${PYTHON_VERSION}
pyenv activate sct-${PYTHON_VERSION}

pip install uv==0.2.24
uv pip install -r requirements.in
```

### SCT test profiling
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
