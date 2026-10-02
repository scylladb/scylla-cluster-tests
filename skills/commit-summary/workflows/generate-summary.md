# Generate Weekly Commit Summary

Step-by-step workflow for producing a curated weekly commit summary.

## Phase 1: Identify Parameters

**Entry criteria:** User asks for a commit summary.

1. Find the latest `commit_summary_issue_*.md` file by number (ignore `_formatted`, `_example`, `_prompt` variants).
2. Read the second line to extract the end SHA from the commit range (the SHA after `...`).
3. Determine the next issue number (previous + 1).
4. Confirm with the user: "Previous report was issue N ending at SHA X. I'll generate issue N+1 starting from X."

**Exit criteria:** Start SHA, issue number, and output filename are known.

## Phase 2: Generate Raw Commit List

**Entry criteria:** Phase 1 complete.

1. Run: `python3 skills/commit-summary/sct_commits_summary.py <start_sha> > commit_summary_issue_<N>.md`
2. Read the generated file to get the full commit list.
3. Note the total commit count, author count, and commit range from the header.

**Exit criteria:** Raw commit file exists with all commits listed.

## Phase 3: Curate Commits

**Entry criteria:** Raw commit list is available.

1. Review each commit and classify as **keep** or **remove** using these criteria:

   **Keep:**
   - New framework capabilities, backends, or infrastructure
   - New tests, test categories or pipelines
   - ScyllaDB-maintained tool updates (scylla-bench, latte, gemini, cassandra-stress, scylla-driver, argus, YCSB)
   - Monitoring version moves and reporting improvements
   - Configuration system changes
   - Nemesis additions or significant nemesis changes
   - Removal of a whole subsystem
   - Performance test additions
   - New implementation plans

   **Remove:**
   - Bug fixes and reliability fixes, whatever their scope or the quality of the commit message
   - Refactors, renames, moves and cleanups
   - General dependency bumps (renovate bot, pip updates, non-ScyllaDB packages)
   - CI, Jenkins and GitHub Actions changes
   - Docs, standards, linter and convention changes
   - Test-case labeling, quarantining or deletion
   - Hydra/container image updates
   - Typo or formatting fixes
   - Pre-commit hook adjustments

2. For borderline commits, lean toward removing. Keep one only if it changes what a test author writes or runs.
3. Group related kept commits (e.g., multiple commits in the same effort, or a tool update + its configuration change). Within a group, select only the **1–3 most representative commits** to link — typically the one that introduces the change, plus the most notable follow-up or the final shape. Do not link every commit in the group. Keep each ScyllaDB tool bump as its own group.
4. Cut the kept list down to **6–10 topics** before writing. The commit count in the range does not raise this number.

**Exit criteria:** 6–10 topics, each with 1–3 commits to link.

## Phase 4: Write the Summary

**Entry criteria:** Curated commit list with groupings.

1. Keep the header exactly as generated (opening line, commit range, counts).
2. Write one paragraph per topic (a single commit or a group of related commits).
3. For any kept commit that bumps a ScyllaDB-maintained tool or driver (scylla-bench, scylla-driver/python-driver, gemini, latte, cassandra-stress, argus, YCSB, scylla-manager, scylla-doctor), fetch the upstream release notes before writing the paragraph:

   ```bash
   gh release view <tag> --repo scylladb/<repo>
   ```

   Tag formats: `vX.Y.Z` for most tools, `X.Y.Z-scylla` for `python-driver`. Fall back to `gh release list --repo scylladb/<repo> --limit 10` if the tag is unclear. Pull 1–3 concrete highlights (bug fixes, new features, behavior changes that matter to SCT test authors) and describe them briefly in plain prose. Read every entry, including renovate-titled ones: a tool release often carries a driver or library bump that changes what the tool talks to the cluster with. Skip only entries about the upstream repository's own CI. **Do not embed links to other repositories** — the only link in the paragraph must be the SCT commit that performed the bump. A bare "was bumped to vX.Y.Z" paragraph is not enough — the reader needs to know *what* changed.

   When the notes are thin, read the compare view:

   ```bash
   gh api repos/scylladb/<repo>/compare/<prev_tag>...<tag> --jq '.commits[].commit.message'
   ```

4. Follow these writing rules:

   **Link placement:**
   - Embed the link on the most descriptive phrase in the paragraph
   - Vary where links appear — some mid-sentence, some near the start, some near the end
   - Never use generic link text like "this commit" or "was updated" — link text should describe the change
   - If a paragraph covers multiple commits, include a link for each

   **Paragraph structure:**
   - State what changed, in 1–2 sentences
   - Cut any sentence that explains a root cause, narrates a failure, or describes how the change works internally
   - Use third-person, factual tone
   - Don't overuse colons — vary sentence structure
   - Don't start every paragraph with a link

   **Ordering:**
   - Lead with the most impactful change
   - Group similar topics together (e.g., all test additions near each other)
   - End with smaller but still noteworthy changes

4. End with exactly: `See you in the next issue of last week in scylla-cluster-tests.git master!`
5. Write the final content to `commit_summary_issue_<N>.md`, replacing the raw content.

**Exit criteria:** File contains the polished summary matching the template and style of previous issues.

## Phase 5: Review

**Entry criteria:** Summary written.

1. Verify the commit range SHA values match between header and actual commits.
2. Verify commit count and author count haven't changed from the raw output.
3. Check that no "removed" commits were accidentally dropped from the link URLs (all kept commits should have valid links).
4. Read the summary aloud mentally — does it flow like previous issues?
5. Present the summary to the user for review.
6. **List the commits that did not get a link**, in the chat response only — never in the report file. Split them into two tables, because they need different decisions from the user:

   **Covered in prose, not linked** — part of a kept group that reached its 1–3 link budget:

   | SHA (8 chars) | Title | Paragraph |
   |---------------|-------|-----------|
   | `abcd1234` | chore(sizing): migrate kafka configs | sizing |

   **Excluded** — not in the report at all:

   | SHA (8 chars) | Title | Reason |
   |---------------|-------|--------|
   | `abcd1234` | chore(deps): update foo | renovate bump |

   Collapse a group that was added and then reverted inside the range into a single row, and say the net effect.

**Exit criteria:** User approves the summary and both tables.
