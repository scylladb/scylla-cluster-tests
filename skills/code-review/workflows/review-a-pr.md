# Workflow: Review an SCT Pull Request

Step-by-step process for reviewing a pull request on the SCT repository.

---

## Phase 1: Understand the Change

**Entry criteria**: PR URL or diff is available.

1. Read the PR title, description, and linked issues
2. Identify the category of change: bug fix, feature, refactor, test, config, CI
3. List the files modified and which SCT subsystems they touch
4. Note the backends involved (AWS, GCE, Azure, Docker, K8S, baremetal)

**Exit criteria**: You can explain in one sentence what the PR does and why.

---

## Phase 2: Override & Inheritance Safety Audit

**Entry criteria**: Phase 1 complete. You know which files and methods are modified.

1. For each modified method signature (`def` line changed):
   a. Run `grep -rn "def <method_name>" sdcm/ unit_tests/` to find ALL definitions
   b. Compare override signatures against the updated parent signature
   c. Verify `super()` calls forward all new parameters
   d. Check test stubs in `unit_tests/dummy_remote.py`, `test_cluster.py`, `test_scylla_yaml_builders.py`
2. For new parameters threaded through a call chain, trace every caller and callee
3. If ANY override is missing the new parameter, flag it as a **blocking issue**

**Exit criteria**: Every method signature change has been verified against all subclass overrides. No missing parameter propagation.

---

## Phase 3: Convention Compliance

**Entry criteria**: Phase 2 complete (no blocking override issues).

1. **Imports**: Verify no inline imports, correct grouping (stdlib / third-party / internal), alphabetical order
2. **Error handling**: No empty `except` blocks, appropriate use of `silence()`, context in error messages
3. **Code style**: Google docstrings on new public methods, no `as any` / type suppression
4. **Tests**: New logic has corresponding tests in `unit_tests/`, pytest style (not unittest)

**Exit criteria**: All convention checks pass or violations are flagged with specific line references.

---

## Phase 4: Configuration & Backend Impact

**Entry criteria**: Phase 3 complete.

1. **Config changes**: New options in `sct_config.py` have defaults in `defaults/*.yaml`
2. **Backend labels**: Files in `sdcm/cluster_*.py` or `sdcm/provision/` trigger the correct provision test labels
3. **Cross-backend parity**: If one backend is changed, assess whether other backends need the same change

**Exit criteria**: Configuration defaults verified. Backend labels confirmed or requested.

---

## Phase 5: Completeness Assessment

**Entry criteria**: Phase 4 complete.

1. **Missing tests**: Is there a test for the happy path? Edge cases? Error paths?
2. **Missing docs**: Are docstrings updated? Is `docs/configuration_options.md` regenerated (happens via pre-commit)?
3. **Missing changes**: Are there files that SHOULD have been modified but weren't? (Other backends, test stubs, config defaults)
4. **Commit message**: Follows Conventional Commits format with valid type, scope (3+ chars), subject (10-120 chars), and body (30+ chars)

**Exit criteria**: Review is complete. All findings documented with specific file/line references.

---

## Phase 6: Write Review Feedback

**Entry criteria**: All checks complete.

### Output Format

Post a **single top-level PR comment** using `gh pr comment`. Do NOT post inline review comments or use the GitHub review API.

**Comment structure:**

```markdown
## 🤖 Claude Code Review

**PR**: #<number> — <title>
**Status**: <LGTM ✅ | Issues Found ⚠️ | Blocking Issues 🚫>

### Summary

<1-2 sentence summary of what the PR does>

### Findings

<If no issues: "No issues found. The change looks correct and follows SCT conventions.">

<If issues found, group by severity:>

#### 🔴 Blocking
- **<file>:<line>** — <description>. Fix: <suggestion>

#### 🟡 Should Fix
- **<file>:<line>** — <description>. Fix: <suggestion>

#### 🟢 Suggestions
- **<file>:<line>** — <description>

### Good Patterns
<Note any well-structured code, good test coverage, or clever solutions. Omit section if nothing notable.>

---
<sub>🔗 [Action run](https://github.com/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID) · Automated review by Claude Code</sub>
```

### Posting Rules

1. **Never post inline comments** — all findings go in the single top-level comment
2. **Never use `gh api` to create review comments** — only use `gh pr comment`
3. **Always include the action run link** using `$GITHUB_REPOSITORY` and `$GITHUB_RUN_ID` env vars
4. **Use the exact command**: `gh pr comment <PR_NUMBER> --body "<body>"`
5. **If the PR has no issues**, still post the comment with "LGTM ✅" status

**Exit criteria**: Single top-level comment posted with all findings, attribution header, and action link footer.
