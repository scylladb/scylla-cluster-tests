# Implementation Plans - Guidelines and Structure

Major features and refactorings in the Scylla Cluster Tests (SCT) project are documented using implementation plans. This document provides the structure and guidelines for creating and maintaining these plans.

## Purpose

Implementation plans serve to:
- Document complex features before implementation begins
- Provide a roadmap for incremental development
- Enable better code reviews by explaining the "why" behind changes
- Track progress across multiple pull requests
- Identify dependencies and risks early in the process

## Plan Structure

All implementation plans must follow this structure:

### 1. Problem Statement

Clear description of what needs to be implemented and why.
- Explain the business or technical need
- Describe the pain points with the current situation
- Justify why this work is necessary

### 2. Current State

Analysis of existing implementation (if any).
- **Must include references to specific files, classes, and methods**
- **Note to Agent:** You have access to the codebase. **You MUST use your file listing and reading tools** to locate and inspect the actual files before writing this section. Do not guess or hallucinate file names.
- Describe how things currently work
- Identify what needs to change
- Document any technical debt or limitations

### 3. Goals

Specific, measurable objectives.
- List concrete outcomes
- Define success metrics where applicable
- Keep goals focused and achievable

### 4. Implementation Phases

Step-by-step breakdown with Definition of Done for each phase.
- **Phases should be atomic and scoped to single Pull Requests where possible**
- **Sequential Logic**: Ensure phases are ordered by dependency. Foundational refactoring must happen before feature implementation.
- **If a step requires information not present in the context, explicitly mark it as 'Needs Investigation' rather than making assumptions**
- Each phase should have:
  - Clear description of what will be implemented
  - Definition of Done (DoD) criteria
  - Dependencies on other phases
  - Expected deliverables

### 5. Testing Requirements

Unit, integration, and manual testing for each phase.
- Specify test types needed
- Identify test coverage goals
- List manual testing procedures
- Document performance testing requirements (if applicable)

### 6. Success Criteria

How to determine if the implementation is successful.
- Define measurable outcomes
- Specify acceptance criteria
- List validation steps

### 7. Risk Mitigation

Known risks and how to address them.
- Identify potential blockers
- Document rollback strategies
- List dependencies on external systems
- Note compatibility concerns

## Plan Guidelines

### General Guidelines
- Plans are written in Markdown format
- Store plans in `/docs/plans/` directory
- Use descriptive filenames (e.g., `gce-azure-fallback-features.md`)
- Plans should be implementation-focused, not time-bound
- Keep plans living documents - update them as implementation progresses

### Content Guidelines
- Include clear Definition of Done (DoD) for each phase
- Provide comprehensive testing requirements
- Document backend-specific details (AWS, GCE, Azure, etc.)
- Include code examples where helpful
- **If a requirement or dependency is unclear, explicitly list it as an "Open Question"**
- Use bullet points for clarity
- Link to relevant issues, PRs, or documentation

### Best Practices
- Review existing plans for reference (see `/docs/plans/`)
- Get feedback on the plan before starting implementation
- Update the plan as you learn new information during implementation
- Mark completed phases with checkboxes
- Archive plans once fully implemented by moving them to `/docs/plans/archive/` directory

## Rule for Agents

**Role:** Act as a **Senior System Architect** for the SCT project.

**Trigger:** When asked to "generate an implementation plan" or "draft a plan", you MUST:
1. **Context Verification:** Before generating the plan, use your file access tools to read the relevant code. **Do not generate a plan based on assumptions.**
2. Read this file (`docs/plans/INSTRUCTIONS.md`) completely.
3. Follow the structure defined in the "Plan Structure" section exactly.
4. Apply all guidelines from the "Plan Guidelines" section.
5. **No Filler:** Start your response immediately with the `# Plan Title`. Do not include conversational prologues.
6. Do NOT apply this format to regular coding questions or small changes.

## Examples

For reference examples of well-structured implementation plans, see:
- `/docs/plans/health-check-optimization.md` - Example of a performance optimization plan that demonstrates:
  - Clear problem statement with measurable performance issues
  - Detailed current state analysis with specific code references
  - Phased approach with atomic, PR-scoped implementation steps
  - Comprehensive testing requirements for each phase
