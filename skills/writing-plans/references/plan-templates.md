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

### Phase 1: <Name> (Priority: High)

**Description**: <What will be implemented>

**Deliverables**:
- <Specific output 1>
- <Specific output 2>

**Definition of Done**:
- [ ] <Criterion 1>
- [ ] <Criterion 2>

---

### Phase 2: <Name> (Priority: High/Medium/Low)

**Description**: <What will be implemented>

**Dependencies**: Phase 1

**Deliverables**:
- <Specific output>

**Definition of Done**:
- [ ] <Criterion>

## Testing Requirements

### Unit Tests
- <What to test at unit level>

### Integration Tests
- <What to test at integration level>

### Manual Testing
- <What requires human verification>

## Success Criteria

1. <Measurable outcome 1>
2. <Measurable outcome 2>
3. <Measurable outcome 3>

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
- Order by dependency: foundational work first
- Each phase needs Description, Deliverables, and Definition of Done
- Mark unclear steps as "Needs Investigation"
- Include Priority level (High/Medium/Low) for each phase

**Phase template**:
```markdown
### Phase N: <Name> (Priority: High/Medium/Low)

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

**Purpose**: Define unit, integration, and manual testing for each phase.

**Must include**:
- Test types needed (unit, integration, manual, performance)
- Test coverage goals
- Manual testing procedures
- Performance testing requirements (if applicable)

**Pattern**:
```markdown
## Testing Requirements

### Unit Tests
- Test configuration parsing with valid/invalid inputs
- Mock cluster nodes to test health check logic
- Run with: `uv run sct.py unit-tests -t test_health_check.py`

### Integration Tests
- Test with Docker backend: `--backend docker`
- Verify end-to-end cluster provisioning

### Manual Testing
- 10-node cluster with mixed nemesis operations
- Verify monitoring dashboards display new metrics
```

---

### 6. Success Criteria

**Purpose**: Define how to determine if the implementation is successful.

**Rules**:
- Measurable outcomes (not vague "it works")
- Acceptance criteria that can be verified
- Validation steps someone can follow

**Good example**:
```markdown
## Success Criteria

1. Health check time for 60-node cluster drops from ~120 to <12 minutes
2. No health checks run after skipped nemesis operations
3. Split-brain detection still catches known scenarios
4. Configuration options documented in `docs/configuration_options.md`
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
