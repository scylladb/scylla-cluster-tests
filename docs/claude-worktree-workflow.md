# Claude Worktree Workflow

How Claude Code worktree agents work in SCT and how to use complementary tools alongside them.

## How worktrees work in Claude Code

When Claude Code spawns an agent with `isolation: "worktree"`, it:

1. Creates a temporary git worktree from the current branch
2. Runs the agent in that isolated copy of the repo
3. If the agent makes changes, returns the worktree path and branch name
4. If no changes are made, cleans up the worktree automatically

This means multiple agents can work on different tasks in parallel without interfering with each other or the main working tree.

## Environment setup in worktrees

### What happens automatically

The `setup-init.sh` hook runs at session start in every worktree and handles:

- **Python installation** via `uv python install` (if needed)
- **Dependency sync** via `uv sync --all-groups` into `.venv`
- **Pre-commit hooks** installed via `pre-commit install` (works in worktrees where `.git` is a file, not a directory)
- **direnv auto-allow** so the `.envrc` activates without manual intervention

### Venv location

Both `setup-init.sh` and `.envrc` default to `.venv` as the virtual environment directory. This is the uv community convention and avoids the confusion of multiple venv directories.

If you switch between branches with different Python versions and want per-version venvs locally (e.g. `.venv-sct-3.14`), create `touch .venv-per-version` in your main checkout. This marker file is gitignored and won't exist in worktrees, so worktrees always get `.venv`. See [docs/setup-uv-direnv.md](setup-uv-direnv.md#3-per-version-venvs-optional) for details.

## Claude hooks in worktrees

| Hook | Trigger | What it does |
|------|---------|--------------|
| `setup-init.sh` | Session start | Installs Python, syncs deps, installs pre-commit, allows direnv |
| `run-unit-tests.sh` | After editing `unit_tests/test_*` | Runs pytest on the edited file; blocks the edit if tests fail |

The `run-unit-tests.sh` hook uses `--project "$CLAUDE_PROJECT_DIR"` to ensure `uv run` resolves the correct venv even when the shell working directory differs from the project root.

## Complementary tools

These tools can share the same repo setup (venv, direnv, git) and work alongside Claude Code.

### GitHub Copilot CLI

```bash
# Suggest a shell command
gh copilot suggest "find all yaml files referencing ami-"

# Explain a command
gh copilot explain "git log --oneline upstream/master..HEAD"
```

Useful for quick shell command suggestions when you know what you want but not the exact syntax.

### OpenCode

[OpenCode](https://github.com/opencode-ai/opencode) is a terminal UI for working with LLMs on code. It can use the same repo checkout and venv:

```bash
cd /path/to/scylla-cluster-tests
opencode
```

Useful as a second opinion or for quick edits when Claude Code is occupied with a long-running task.

### aider

[aider](https://aider.chat) is another CLI coding assistant that works with git repos:

```bash
cd /path/to/scylla-cluster-tests
aider sdcm/cluster.py
```

Shares the same venv and direnv setup. Useful for focused edits on specific files.

## Tips

### Parallel worktree agents

Launch multiple worktree agents in a single message to parallelize independent tasks:

```
# In Claude Code, a single response can spawn multiple agents:
# Agent 1 (worktree): "Fix the linting error in cluster_aws.py"
# Agent 2 (worktree): "Add unit test for version_utils.parse_version"
```

Each agent gets its own worktree and works independently.

### When to use worktrees vs main context

| Scenario | Use worktree | Use main context |
|----------|:---:|:---:|
| Independent code changes | Yes | |
| Research / reading code | | Yes |
| Changes that depend on each other | | Yes |
| Running full test suites | Yes | |
| Quick single-file edits | | Yes |

### Local permissions

Use `.claude/settings.local.json` (gitignored) to customize permissions for your environment without affecting the team:

```json
{
  "permissions": {
    "allow": ["Bash(uv run:*)", "Bash(git:*)"]
  }
}
```
