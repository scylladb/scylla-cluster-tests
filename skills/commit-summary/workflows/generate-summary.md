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
   - New tests or test categories
   - ScyllaDB-maintained tool updates (scylla-bench, latte, gemini, cassandra-stress, scylla-driver, argus, YCSB)
   - Major refactorings that change code organization
   - New implementation plans or documentation
   - New framework capabilities, backends, or infrastructure
   - Configuration system changes
   - Nemesis additions or significant nemesis changes
   - Performance test additions
   - Monitoring/reporting improvements

   **Remove:**
   - Small, narrow-scope bug fixes
   - General dependency bumps (renovate bot, pip updates, non-ScyllaDB packages)
   - Minor refactors (renames, moves, cleanups)
   - Hydra/container image updates
   - Typo or formatting fixes
   - CI pipeline minor tweaks
   - Pre-commit hook minor adjustments

2. For borderline commits, lean toward including if the change affects how developers write or run tests.
3. Group related kept commits (e.g., multiple commits in the same refactoring effort, or a tool update + its configuration change).

**Exit criteria:** List of kept commits with grouping decisions noted.

## Phase 4: Write the Summary

**Entry criteria:** Curated commit list with groupings.

1. Keep the header exactly as generated (opening line, commit range, counts).
2. Write one paragraph per topic (a single commit or a group of related commits).
3. Follow these writing rules:

   **Link placement:**
   - Embed the link on the most descriptive phrase in the paragraph
   - Vary where links appear — some mid-sentence, some near the start, some near the end
   - Never use generic link text like "this commit" or "was updated" — link text should describe the change
   - If a paragraph covers multiple commits, include a link for each

   **Paragraph structure:**
   - Lead with what changed and why it matters
   - Keep paragraphs to 1-3 sentences
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
6. **List all dropped commits** in a table so the user can judge whether any should be added back. Format:

   | SHA (8 chars) | Title | Reason dropped |
   |---------------|-------|----------------|
   | `abcd1234` | chore(deps): update foo | renovate bump |
   | ... | ... | ... |

   This lets the user quickly scan what was excluded and ask to reinstate specific commits.

**Exit criteria:** User approves the summary (including the dropped commits list).
