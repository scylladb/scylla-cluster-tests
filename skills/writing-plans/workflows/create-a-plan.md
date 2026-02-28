# Creating an Implementation Plan

A 5-phase process for writing an SCT implementation plan following the 7-section structure.

---

## Phase 1: Gather Context

**Entry:** You have a feature or change request that needs an implementation plan.

**Actions:**

1. **Read `docs/plans/INSTRUCTIONS.md` completely.** This is the authoritative source for plan structure and guidelines. Do not skip this step.

2. **Identify the scope.** What problem is being solved? What areas of the codebase are affected? Which backends (AWS, GCE, Azure, Docker, K8S) are impacted?

3. **Inspect the codebase.** Use file-reading tools to examine relevant source files. Identify:
   - Current implementations that will change
   - Configuration parameters in `sdcm/sct_config.py`
   - Test files in `unit_tests/` related to the area
   - Existing documentation in `docs/`

4. **Check for existing plans.** Look in `docs/plans/` for related plans that may overlap or provide context.

5. **Review reference plans.** Read at least one existing plan (e.g., `docs/plans/health-check-optimization.md`) to calibrate quality expectations.

**Exit:** You understand the problem, have inspected relevant code, and know the plan structure.

---

## Phase 2: Write the Foundation Sections

**Entry:** Phase 1 complete. Context gathered, code inspected.

**Actions:**

1. **Write the Problem Statement.** Include:
   - The business or technical need
   - Specific pain points (with measurements if available)
   - Justification for why this work is necessary

2. **Write the Current State section.** This is the most research-intensive section:
   - Reference specific files, classes, and methods (verify they exist)
   - Describe how things currently work
   - Identify what needs to change
   - Document technical debt or limitations
   - **Never guess file names** — use tools to locate and verify

3. **Write the Goals section.** Define 3-6 specific, measurable objectives:
   - Number each goal with a bold title
   - Include measurable criteria where possible
   - Keep goals achievable within the plan scope

**Exit:** Problem Statement, Current State, and Goals sections are complete with verified code references.

---

## Phase 3: Design the Implementation

**Entry:** Phase 2 complete. Foundation sections written.

**Actions:**

1. **Break the work into phases.** Each phase should be:
   - Atomic and scoped to a single Pull Request where possible
   - Ordered by dependency (foundational work first)

2. **Keep PRs small and focused.** Large phases should be split into sub-phases. Within a PR, use separate commits for logically distinct changes (e.g., one commit for refactoring, another for new functionality, another for tests).

3. **For each phase, write:**
   - **Description**: What will be implemented and why
   - **Dependencies**: Which phases must be complete first
   - **Deliverables**: Concrete outputs (files, features, configurations)
   - **Definition of Done**: Verifiable criteria using checkboxes — these serve as the success criteria for the phase

4. **Mark uncertain steps.** If a requirement or dependency is unclear, mark it as "Needs Investigation" rather than making assumptions.

5. **Include a documentation update phase.** Every plan should have a phase (or phase deliverable) covering:
   - Updated or new entries in `docs/` for user-facing changes
   - Configuration documentation regenerated via `uv run sct.py pre-commit`
   - README or guide updates if the feature changes user workflows

6. **Include SCT-specific details:**
   - Backend-specific impact (which backends are affected?)
   - Configuration changes (`sdcm/sct_config.py` parameters)
   - Default values in `defaults/test_default.yaml`
   - Test case YAML files in `test-cases/`

7. **Add separation lines** (`---`) between phases for readability.

**Exit:** Implementation Phases section complete with Definition of Done for each phase.

---

## Phase 4: Define Testing and Success

**Entry:** Phase 3 complete. Implementation phases designed.

**Actions:**

1. **Write Testing Requirements.** Focus on unit tests — what the LLM can write and run:
   - **Unit tests**: What to test in isolation, expected location in `unit_tests/`
   - Key scenarios and edge cases to cover
   - Do NOT include integration tests or manual testing procedures — those are handled during review

2. **Write Success Criteria.** Completing all Definition of Done items across phases constitutes success. Only add plan-level criteria that span multiple phases or cannot be captured in any single phase's DoD.

3. **Write Risk Mitigation.** For each risk:
   - **Name**: Short description of the risk
   - **Likelihood**: High/Medium/Low
   - **Impact**: What goes wrong
   - **Mitigation**: How to prevent or handle it

4. **Common SCT risks to consider:**
   - Backend-specific failures (AWS quotas, GCE regions, Azure limits)
   - Backward compatibility with existing test configurations
   - Impact on CI/CD pipeline performance
   - Configuration migration for existing users

**Exit:** Testing Requirements, Success Criteria, and Risk Mitigation sections complete.

---

## Phase 5: Validate the Plan

**Entry:** Phase 4 complete. All 7 sections written.

**Actions:**

1. **Verify the 7-section structure.** Confirm all sections are present:
   - [ ] Problem Statement
   - [ ] Current State (with code references)
   - [ ] Goals
   - [ ] Implementation Phases (with DoD per phase)
   - [ ] Testing Requirements
   - [ ] Success Criteria
   - [ ] Risk Mitigation

2. **Verify all code references.** Every file path mentioned in Current State must point to a real file. Use file-reading tools to confirm.

3. **Check phase dependencies.** Ensure no phase references work from a later phase. Foundational work comes first.

4. **Check for open questions.** If any requirement is unclear, it should be marked as "Needs Investigation" — not assumed.

5. **Check Definition of Done criteria.** Each criterion should be verifiable (someone can check it off), not vague ("it works").

6. **Review against an existing plan.** Compare structure and quality with `docs/plans/health-check-optimization.md` or another reference plan.

7. **Verify filename.** Plan should be saved as `docs/plans/<kebab-case-name>.md` with a descriptive name.

8. **Run pre-commit (if available).** Execute `uv run sct.py pre-commit` to verify no formatting issues. If unavailable, manually check trailing whitespace and end-of-file newlines.

**Exit:** All 7 sections present, code references verified, phases ordered correctly, plan saved in `docs/plans/`.
