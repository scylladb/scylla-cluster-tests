# SCT Plan Templates

Templates and examples for each section of an SCT implementation plan, based on the 7-section structure defined in `docs/plans/INSTRUCTIONS.md`.

---

## Complete Plan Skeleton

Use this as a starting point for any new plan:

```markdown
# <Feature/Change Name> Plan

## Problem Statement

<What needs to change and why. Include business/technical need, pain points, and justification.>

## Current State

<Analysis of existing implementation. MUST reference specific files, classes, and methods.>

### <Subsystem A>

- `path/to/file.py` — <What it does, what needs to change>
- `path/to/other.py:ClassName` — <Current behavior>

### What's Missing

- <Gap 1>
- <Gap 2>

## Goals

1. **<Goal 1>**: <Measurable outcome>
2. **<Goal 2>**: <Measurable outcome>
3. **<Goal 3>**: <Measurable outcome>

## Implementation Phases

### Phase 1: <Name>

**Description**: <What will be implemented>

**Deliverables**:
- <Specific output 1>
- <Specific output 2>

**Definition of Done**:
- [ ] <Criterion 1>
- [ ] <Criterion 2>

---

### Phase 2: <Name>

**Description**: <What will be implemented>

**Dependencies**: Phase 1

**Deliverables**:
- <Specific output>

**Definition of Done**:
- [ ] <Criterion>

## Testing Requirements

### Unit Tests
- <What to test at unit level>

## Success Criteria

Completing all Definition of Done items across phases constitutes success. Add plan-level criteria only if they go beyond individual phase DoD:

1. <Plan-level measurable outcome not captured in any single phase DoD>

## Risk Mitigation

### Risk: <Risk Name>
**Likelihood**: High/Medium/Low
**Impact**: <What goes wrong>
**Mitigation**: <How to prevent or handle it>
```

---

## Section-by-Section Guidance

### 1. Problem Statement

**Purpose**: Explain what needs to change and why.

**Must include**:
- Business or technical need driving the change
- Pain points with the current situation
- Justification for why this work is necessary

**Good example** (from `docs/plans/health-check-optimization.md`):
```markdown
## Problem Statement

Health checks take 2+ hours on 60-node clusters, running even for
skipped nemesis operations. This significantly impacts test run time
and efficiency.

### Current Behavior
- Sequential health checks on all 60 nodes
- Each node check involves 5 expensive operations
- Total time: ~120 minutes for 60-node cluster
```

**Bad example**:
```markdown
## Problem Statement

We need to improve health checks.
```

The bad example lacks specifics: no measurable problem, no root cause, no justification.

---

### 2. Current State

**Purpose**: Analyze the existing implementation with specific code references.

**Critical rule**: You MUST use file-reading tools to inspect actual code. Do not guess or hallucinate file names.

**Must include**:
- References to specific files, classes, and methods
- Description of how things currently work
- What needs to change
- Technical debt or limitations

**Good example** (from `docs/plans/nemesis-rework.md`):
```markdown
## Current State

Currently all nemesis are located in single file with over 7000 lines.
All logic lives in Nemesis class to facilitate sharing of functionality
between nemesis, this results in extremely bloated class.
```

**Pattern for referencing code**:
```markdown
### Cluster Management
- `sdcm/cluster.py:BaseCluster` — Base cluster class, manages node lifecycle
- `sdcm/cluster_aws.py:AWSCluster` — AWS-specific provisioning
- `sdcm/provision/aws/provisioner.py` — Low-level AWS resource creation
```

---

### 3. Goals

**Purpose**: Define specific, measurable objectives.

**Rules**:
- Number each goal with bold title
- Make goals measurable ("reduce by 90%", "support 3 backends")
- Keep goals focused and achievable within the plan scope

**Good example**:
```markdown
## Goals

1. **Reduce health check time by 90%+** for large clusters (60+ nodes)
2. **Skip redundant checks** when nemesis operations are skipped
3. **Maintain split-brain detection** capability
4. **Provide configurable modes** for different validation levels
```

---

### 4. Implementation Phases

**Purpose**: Break the work into atomic, PR-scoped steps.

**Rules**:
- Phases should be scoped to single Pull Requests where possible
- Large phases should be split into separate commits within the PR for easier review
- Order by dependency: foundational work first
- Each phase needs Description, Deliverables, and Definition of Done
- Mark unclear steps as "Needs Investigation"
- Definition of Done items should be verifiable and serve as the success criteria for the phase

**PR scoping guidance**: Keep PRs as small and focused as possible. If a phase is too large for a single review, split it into multiple sub-phases. Within a PR, use separate commits for logically distinct changes (e.g., one commit for refactoring, another for new functionality, another for tests).

**Phase template**:
```markdown
### Phase N: <Name>

**Description**: <What will be implemented and why>

**Dependencies**: <Which phases must be complete first>

**Deliverables**:
- <Concrete output 1>
- <Concrete output 2>

**Adaptation Notes**: <Optional: context for reviewers>

**Definition of Done**:
- [ ] <Verifiable criterion 1>
- [ ] <Verifiable criterion 2>
```

---

### 5. Testing Requirements

**Purpose**: Define what unit tests are needed to verify each phase.

**Focus on unit tests** — these are what the LLM can actually write and run. Integration testing, manual testing, and performance validation are handled during review, not in the plan.

**Must include**:
- What to test at the unit level
- Test file locations in `unit_tests/`
- Key scenarios and edge cases to cover

**Pattern**:
```markdown
## Testing Requirements

### Unit Tests
- Test configuration parsing with valid/invalid inputs
- Mock cluster nodes to test health check logic
- Run with: `uv run sct.py unit-tests -t test_health_check.py`
```

---

### 6. Success Criteria

**Purpose**: Confirm overall plan completion. Completing all Definition of Done items across phases constitutes success.

**Rules**:
- Avoid duplicating DoD items — reference them instead
- Only add plan-level criteria that span multiple phases or cannot be captured in a single phase's DoD
- If all DoD items cover success fully, this section can simply state that

**Good example**:
```markdown
## Success Criteria

All Definition of Done items across phases are met. Additionally:

1. Configuration options documented in `docs/configuration_options.md`
2. No regressions in existing unit tests
```

---

### 7. Risk Mitigation

**Purpose**: Identify risks and mitigation strategies.

**Must include**:
- Potential blockers
- Rollback strategies
- Dependencies on external systems
- Compatibility concerns

**Pattern**:
```markdown
### Risk: <Name>
**Likelihood**: High/Medium/Low
**Impact**: <What goes wrong if this happens>
**Mitigation**: <How to prevent it or what to do if it happens>
```

---

## SCT-Specific Conventions for Plans

| Convention | Rule |
|-----------|------|
| File location | Store in `docs/plans/` directory |
| Filename | kebab-case, descriptive (e.g., `health-check-optimization.md`) |
| Backend details | Document backend-specific impact (AWS, GCE, Azure, Docker, K8S) |
| Configuration | Reference `sdcm/sct_config.py` for config parameters |
| Test commands | Use `uv run sct.py unit-tests -t <file>` and `uv run sct.py integration-tests` |
| Pre-commit | Include `uv run sct.py pre-commit` in testing steps |
| Code references | Always point to real files — verify with file-reading tools |
| Open questions | Mark unclear requirements as "Needs Investigation" |
| Archiving | Move completed plans to `docs/plans/archive/` |

---

## Existing Plan Examples

Reference these for style and quality:

| Plan | Type | Demonstrates |
|------|------|-------------|
| `docs/plans/health-check-optimization.md` | Performance optimization | Measurable metrics, phased approach, configuration design |
| `docs/plans/nemesis-rework.md` | Feature refactoring | Large-scale restructuring, backward compatibility |
| `docs/plans/docker-cleanup-all-backends.md` | Infrastructure cleanup | Multi-backend impact analysis |
| `docs/plans/full-version-tag-lookup.md` | Feature implementation | Focused feature with clear deliverables |
| `docs/plans/ai-skills-framework.md` | Framework design | Multi-phase skill creation with platform compatibility |
