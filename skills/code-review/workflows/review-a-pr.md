# Workflow: Review an SCT Pull Request

Step-by-step process for reviewing a pull request on the SCT repository.

---

## Phase 1: Understand the Change

**Entry criteria**: PR URL or diff is available.

In CI there is no `gh`: the prompt names the files that hold the pull request data, the threads, the
comments, and the checks. Read those files where the steps below say `gh`.

1. Read the PR title, description, and linked issues
   - Read `REVIEW.md` at the repository root for the three review passes
   - When the PR carries or links a flow directory `docs/plans/<feature>/<change>/`, read the `intent.md`, `spec.md`, and `plan.md` that exist (SKILL.md Check 11)
2. Read the existing review threads and bot reviews, so that you do not repeat them:
   `gh api repos/<repo>/pulls/<N>/comments --paginate` and `gh api repos/<repo>/pulls/<N>/reviews`
3. Read the whole changed files, not only the hunks, and the callers and callees of each changed function
4. Read the CI status from two sources:
   - **Checks**: `gh pr checks <N>` lists the GitHub Actions checks and the Jenkins statuses
     (`jenkins/precommit`, `jenkins/unittests`, `jenkins/collect_tests`, `jenkins/lint_test_cases`,
     `continuous-integration/jenkins/pr-merge` or `pr-head`)
   - **Test summary comment**: Jenkins posts one comment that starts with `<!-- sct-test-summary -->`
     and updates it on each run. It names each failed test, with its message, its traceback, and the
     build link:
     `gh api repos/<repo>/issues/<N>/comments --jq '.[] | select(.body | contains("sct-test-summary")) | .body'`
   - The summary is current only when its build link has the same build number as the
     `continuous-integration/jenkins/*` status of the head commit. Otherwise it is from an older build:
     say so, and do not report its failures as current. A pending status means the build still runs
   - For each failure, decide whether this PR caused it: the PR touches the failed test, or a file in
     its traceback. A failure that the PR caused gets an inline 🚫 or 🔧 on the responsible line. A
     failure that it did not cause (a network error, a flaky test, a break on master) goes in the
     summary's `CI:` line as "not caused by this PR"
   - The `CI:` line names only failures, pending builds, and checks that did not run. Never list the
     checks that passed. A summary that says FAILED next to a green status still reports its failures
5. Identify the category of change: bug fix, feature, refactor, test, config, CI
6. List the files modified and which SCT subsystems they touch
7. Note the backends involved (AWS, GCE, Azure, Docker, K8S, baremetal)

**Prove a finding when it is cheap.** Run the changed tests, run a short script, or grep for the
callers. A finding that you did not prove or trace in the code is a ❓ question, with what would
confirm it. In CI, never run code from the pull request: trace it in the code instead.

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
4. **Trigger-matrix arch**: If a test's DB instance type or `sizing_db` changed, its `configurations/triggers/*.yaml` entries declare the arch it now runs on, per release (SKILL.md Check 10)

**Exit criteria**: Configuration defaults verified. Backend labels confirmed or requested. Trigger-matrix arch matches.

---

## Phase 5: Completeness Assessment

**Entry criteria**: Phase 4 complete.

1. **Missing tests**: Is there a test for the happy path? Edge cases? Error paths?
2. **Missing docs**: Are docstrings updated? Is `docs/configuration_options.md` regenerated (happens via pre-commit)?
3. **Missing changes**: Are there files that SHOULD have been modified but weren't? (Other backends, test stubs, config defaults)
4. **Commit message**: Follows Conventional Commits format with valid type, scope (3+ chars), subject (10-120 chars), and body (30+ chars)
5. **Spec compliance**: When the PR carries a flow directory, the change matches its documents (SKILL.md Check 11)
6. **Scope**: The main change is in its own commit. Unrelated reformatting and pieces outside the plan are findings
7. **Size**: Ask what could be deleted and still work: a reinvented helper, an abstraction with one user, test scaffolding larger than the code it tests
8. **Ownership and merge order**: The change belongs to the team that owns the code (`CODEOWNERS`). When it depends on another PR, the description says "merge only after #N". A real bug outside the scope is a 📌 follow-up that asks for a Jira link, not a request to fix it in this PR

**Exit criteria**: Review is complete. All findings documented with specific file/line references.

---


## Phase 6: Write the Review

**Entry criteria**: All checks complete.

Write one GitHub review with inline comments. Follow
[writing-review-comments.md](../references/writing-review-comments.md) for the tags, the comment
layout, and the summary. Follow the volume rule of `REVIEW.md`.

1. Anchor each inline comment on a line inside a diff hunk, in the file at the head commit. Check the
   line with `grep -n` on that file, never on the saved diff. A finding outside the diff goes in the
   summary.
2. Write the review as JSON, in the shape of the GitHub API
   (`POST /repos/<repo>/pulls/<N>/reviews`):

   ```json
   {
     "commit_id": "<headRefOid>",
     "event": "COMMENT",
     "body": "<summary>",
     "comments": [
       {"path": "sdcm/foo.py", "line": 42, "side": "RIGHT", "body": "🔧 **important:** ..."},
       {"path": "sdcm/foo.py", "start_line": 10, "line": 14, "side": "RIGHT", "body": "⛏ **nit:** ..."}
     ]
   }
   ```

3. The event is always `COMMENT`. Never `APPROVE` or `REQUEST_CHANGES`: a maintainer decides the merge.
4. Post it:
   - **In CI** (the `claude-review` workflow): write the JSON to the path that the prompt gives. Do not
     post. A later step of the workflow checks the file and posts it.
   - **Locally**: show the draft to the user. On approval, check that `headRefOid` did not change, then
     post it with `gh api repos/<repo>/pulls/<N>/reviews -X POST --input review.json`.

**Exit criteria**: One review with the outcome of each pass in the summary and each finding on its line.
