## Contribution

### setting pre commit hooks

Since we are trying to keep the code neat, please install this git precommit hooks, that would fix the code style, and run more checks::

```bash
pip install pre-commit==2.15.0
pre-commit install
pre-commit install --hook-type commit-msg
```
If you want to remove the hook::

```bash
pre-commit uninstall
```

Doing a commit without the hook checks::
```
    git commit ... -n
```

### Using .git-blame-ignore-revs for formatting changes

When making formatting changes or adding new linting rules (e.g., ruff), it's helpful to exclude these commits from `git blame` so that the blame shows the original author of the logic rather than the person who reformatted the code.

To use the `.git-blame-ignore-revs` file locally:
```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

For PRs with formatting changes, add the SHAs of the format commits to `.git-blame-ignore-revs` in a follow-up PR after merge (squash-merged SHAs are the ones that matter).

### update the configuration docs

Any time new SCT configuration option is added, the docs should be updated
with the following command:
```bash
hydra update-conf-docs
```

### Building Hydra Docker image

Once you have changes in the requirements.in or in Hydra Dockerfile

- change the version in docker/env/version
- run ``./docker/env/build_n_push.sh`` to build and push to Docker Hub


### Staging Trigger — triggering staging Jenkins jobs

The `staging_trigger.py` tool (backed by the `utils/staging_trigger/` package) lets you
trigger, browse, and generate Jenkins staging jobs from the command line.

#### Jenkins credentials

The tool calls Jenkins as you, with your own API token, so each build shows who started it.
It tries your credentials from these sources, in order:

1. `JENKINS_USERNAME` and `JENKINS_API_TOKEN` environment variables
2. the 1Password item titled `Jenkins API token` (`op` CLI), the recommended store
3. the OS keyring item with attribute `service=jenkins.scylladb.com` (`secret-tool`)

It checks each one against Jenkins. When Jenkins rejects one (a revoked token, a wrong user ID),
it says so and tries the next, so you can keep the token in both 1Password and the keyring.
It stops with an error when none works. Each run prints what it used, for example:

```text
Jenkins: jdoe from 1Password was rejected, trying the next source (... [401] ...)
Jenkins: calling https://jenkins.scylladb.com as jdoe@scylladb.com, token from OS keyring
```

When the 1Password app is locked or closed, the run prints `Jenkins: op failed: ...` and goes on
with the keyring.

##### Create a token

1. Log in to https://jenkins.scylladb.com and open https://jenkins.scylladb.com/me/security
   (your name, top right, then **Security**).
2. Your Jenkins user ID is in the page URL, `/user/<user id>/security`, for example
   `jdoe@scylladb.com`. Use the full ID: a short name fails with 401.
3. Under **API Token**, click **Add new Token**, name it after the machine (`staging-trigger-laptop`),
   and click **Generate**. Jenkins shows the token once. Copy it now.

Store the token in 1Password, in your OS keyring, or in both. Never put it in a file in the repo
or in your shell history.

##### Store it in 1Password

1. Install the 1Password CLI, `op`, version 2 or later:
   - macOS: `brew install 1password-cli`
   - Linux: add the 1Password apt or dnf repository and install the `1password-cli` package, as
     described in https://developer.1password.com/docs/cli/get-started/. On Ubuntu that ends with
     `sudo apt update && sudo apt install 1password-cli`.

   Check it with `op --version`.
2. Start the 1Password desktop app, sign in, and turn on
   **Settings > Developer > Integrate with 1Password CLI**. `op` then uses the app's login, and
   the app asks you to approve access the first time.
3. Create an **API Credential** item titled `Jenkins API token`, with your Jenkins user ID in
   **username** and the token in **credential**. In the app, use **+ New Item > API Credential**.
   From the shell, the token goes to `op` on stdin, so it stays out of history and process lists:

   ```bash
   read -rs JENKINS_TOKEN   # paste the token, press Enter; nothing is shown
   printf '{"title": "Jenkins API token", "category": "API_CREDENTIAL", "fields": [
     {"id": "username", "type": "STRING", "label": "username", "value": "%s"},
     {"id": "credential", "type": "CONCEALED", "label": "credential", "value": "%s"}]}' \
     "<jenkins user id>" "$JENKINS_TOKEN" | op item create --vault <vault> > /dev/null
   unset JENKINS_TOKEN
   ```

   `op vault list` shows your vault names (`Private`, `Employee`).
4. Check it. This prints only the user ID:

   ```bash
   op item get "Jenkins API token" --fields label=username
   ```

##### Store it in the OS keyring

On Linux, with `secret-tool` from `libsecret-tools`. It prompts for the token:

```bash
secret-tool store --label="Jenkins API token" service jenkins.scylladb.com user <jenkins user id>
```

##### Use it

Keep the 1Password app unlocked; the first `op` call may ask you to approve access. Then run the
tool as usual:

```bash
python staging_trigger.py trigger -b my-branch longevity-100gb-4h-test
```

To use other credentials for one run, set `JENKINS_USERNAME` and `JENKINS_API_TOKEN`.
`JENKINS_USERNAME` alone overrides only the user ID. `JENKINS_URL` overrides the default
`https://jenkins.scylladb.com`.

To use only one source, set `JENKINS_CREDENTIALS_SOURCE` to `env`, `keyring` or `1password`.
This is handy to check that a store works:

```bash
JENKINS_CREDENTIALS_SOURCE=1password python staging_trigger.py trigger -n
```

If a source is rejected, check the user ID in its `... was rejected` line first: it must be the
full Jenkins user ID. Check a token by hand with the command below. `curl` reads the login from stdin,
so the token stays off its command line:

```bash
printf 'user = "%s:%s"\n' "<jenkins user id>" "$(op read 'op://<vault>/Jenkins API token/credential')" \
    | curl -s -K - https://jenkins.scylladb.com/whoAmI/api/json
```

It should show your user with `"authenticated":true`.

Use one token per machine, so you can revoke one without the others. Revoke a token on the
Jenkins Security page when you stop using the machine or think it leaked.

#### Quick start

```bash
# Interactive: browse jobs, select, trigger (default mode)
python staging_trigger.py trigger

# Filter jobs interactively
python staging_trigger.py trigger "longevity*"

# Direct trigger (no interaction)
python staging_trigger.py trigger -b my-branch longevity-100gb-4h-test

# Trigger from a PR (auto-detects repo/branch)
python staging_trigger.py trigger --pr 12345 manager-ubuntu20-sanity-test

# Dry run — see what would be triggered without actually triggering
python staging_trigger.py trigger -n

# Override any Jenkins parameter
python staging_trigger.py trigger -b my-branch longevity-100gb-4h-test \
    --set scylla_version=2025.1 --set provision_type=spot

# Export selected jobs as YAML config
python staging_trigger.py trigger -o my_tests.yaml

# Trigger from a YAML config file
python staging_trigger.py run-config my_tests.yaml

# Generate Jenkins pipeline jobs from jenkinsfiles (interactive)
python staging_trigger.py generate -b my-branch
```

#### Library usage

```python
from staging_trigger import StagingTrigger

trigger = StagingTrigger.from_preset("longevity", branch="my-branch")
trigger.select_jobs("longevity-100gb-4h-test")
trigger.run()

# Or from a PR number
trigger = StagingTrigger.from_pr(12345, preset_name="longevity")
trigger.select_jobs("longevity-100gb-4h-test")
trigger.run()
```

#### Presets

Jobs are configured using presets that set sensible defaults for each category:

| Preset | Description |
|--------|-------------|
| `longevity` | Longevity, rolling-upgrade, gemini, and jepsen tests |
| `manager` | Scylla Manager tests |
| `artifacts` | Artifact/package deployment tests |
| `perf` | Performance regression tests |
| `dtest` | Python dtests (with topology variants) |

#### YAML config files

For mass-triggering, create a YAML config file:

```yaml
folder: scylla-staging/<username>
branch: my-feature-branch
params:
  scylla_version: "master:latest"
  email_recipients: user@scylladb.com
jobs:
  - name: longevity-100gb-4h-test
    preset: longevity
  - name: manager-ubuntu20-sanity-test
    preset: manager
    params:
      manager_version: "3.8"
  - name: dtest-pytest-gating
    preset: dtest
    dtest_topologies: [no-tablets, tablets]
```

Then trigger with: `python staging_trigger.py run-config my_tests.yaml`

### Creating pipeline jobs for new branch

Once a new branch is created, we could build all the need job for this branch with the following script ::
```bash
JENKINS_USERNAME=[jenkins username] JENKINS_PASSWORD=[token from jenkins] hydra create-test-release-jobs scylla-2025.1 --sct_branch branch-2025.1
```

### Creating pipeline jobs for new scylla-operator branch/release/tag

Create new set of scylla-operator jobs using following command ::
```bash
hydra create-operator-test-release-jobs \
  operator-1.2 \
  jenkins-username \
  jenkins-user-password-or-api-token \
  --sct_branch dev \
  --sct_repo git@github.com:some-github-username-123321/scylla-cluster-tests.git
```

### [Setup new regions AWS](./aws_configuration.md)

### [Configure new project GCE](./gcp_create_new_project.md)

### [Docker Based Loaders](./docker-loaders.md)

### AI-Assisted Development (Skills & Prompts)

This project includes reusable AI prompts for common development tasks. They are available
for both **Claude Code** and **GitHub Copilot**.

#### Available prompts

| Prompt | Description |
|--------|-------------|
| `fix-backport-conflicts` | Resolve inline merge conflict markers in backport PRs and recommit cleanly with original authorship preserved |

#### Using with Claude Code

Claude Code skills live in `.claude/skills/`. Invoke them as slash commands:

```
/fix-backport-conflicts 13920
```

This will check out the PR, find inline conflict markers, resolve them, and recommit
with the original author and commit messages preserved.

#### Using with GitHub Copilot (VS Code)

Copilot reusable prompts live in `.github/prompts/`. To use them:

1. Open **Copilot Chat** (`Ctrl+Shift+I`)
2. Switch to **Agent mode** (drop-down at the top of the chat panel)
3. Type `#` and select `fix-backport-conflicts` from the list
4. Add context, e.g.: `#fix-backport-conflicts fix PR 13920`

#### Adding new prompts

To add a new reusable prompt:

1. Create a Claude Code skill in `.claude/skills/<name>/SKILL.md` (with YAML frontmatter)
2. Create a matching Copilot prompt in `.github/prompts/<name>.prompt.md` (plain markdown)
3. Add an entry to the table above in this document

### Directory Structure of the project

1. A library, called `sdcm` (stands for scylla distributed cluster
   manager). The word 'distributed' was used here to differentiate
   between that and CCM, since with CCM the cluster nodes are usually
   local processes running on the local machine instead of using actual
   machines running scylla services as cluster nodes. It contains:

   * `sdcm.cluster`: Base classes for Clusters
   * `sdcm.remote`: SSH library
   * `sdcm.nemesis`: Nemesis package — chaos engineering framework with auto-discovery, a registry for filtering by flags, and individual disruption classes (see [docs/nemesis.md](nemesis.md))
   * `sdcm.tester`: Contains the base test class, see below.

2. A data directory, aptly named `data_dir`. It contains:

   * scylla repo file (to prepare a loader node)
   * Files that need to be copied to cluster nodes as part of setup/test procedures
   * yaml file containing test config data:

     * AWS machine image ids
     * Security groups
     * Number of loader nodes
     * Number of cluster nodes
   * SCT dashboards definition files for Grafana

3. Python files with tests. The convention is that test files have the `_test.py` suffix.
4. Utilities directory named `utils`. Contains help utilities for log analyzing
