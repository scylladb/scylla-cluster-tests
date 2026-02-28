# Plan Anti-Patterns Catalog

Common mistakes when writing SCT implementation plans. Each entry includes the symptom, why it's wrong, and a fix.

---

## Content Anti-Patterns

### PAP-1: Too Much Code in the Plan

**Symptom:** The plan includes full implementation code — complete functions, classes, or modules — instead of describing what to build.

**Why it's wrong:** A plan is a roadmap, not a codebase. Embedding full implementations makes the plan brittle (code changes invalidate the plan), hard to review (reviewers must read code instead of design), and misleading (the code may not compile or handle edge cases). The actual implementation belongs in PRs.

**Fix:** Use short code snippets (5-15 lines) only to illustrate interfaces, configuration formats, or API contracts. Describe behavior in prose; let the implementation phase produce the real code.

**Before:**
```markdown
### Phase 2: Implement Health Check

```python
class HealthChecker:
    def __init__(self, cluster, workers=5):
        self.cluster = cluster
        self.workers = workers
        self.executor = ThreadPoolExecutor(max_workers=workers)

    def check_all_nodes(self):
        futures = {}
        for node in self.cluster.nodes:
            futures[node] = self.executor.submit(self._check_node, node)
        results = {}
        for node, future in futures.items():
            try:
                results[node] = future.result(timeout=300)
            except Exception as e:
                results[node] = HealthResult(healthy=False, error=str(e))
        return results

    def _check_node(self, node):
        # ... 50 more lines of implementation
```
```

**After:**
```markdown
### Phase 2: Implement Parallel Health Check

**Description**: Replace sequential node checks with parallel execution using
`ThreadPoolExecutor`. Configurable worker count via `cluster_health_check_parallel_workers`.

**Key design decisions**:
- Use `ThreadPoolExecutor` (not asyncio) to match existing SCT patterns
- 300-second timeout per node check
- One node failure does not block others

**Configuration**:
```yaml
cluster_health_check_parallel_workers: 5  # default: 5, max: 10
```
```

---

### PAP-2: Missing Diagrams for Complex Interactions

**Symptom:** The plan describes a complex multi-component interaction (cross-service communication, state machines, multi-backend provisioning flows) using only prose, without any visual representation.

**Why it's wrong:** Prose alone cannot convey component relationships, data flows, or state transitions clearly. Readers (human and AI) miss dependencies, race conditions, or circular references that a diagram makes obvious.

**Fix:** Add ASCII diagrams or Mermaid diagrams for any interaction involving 3+ components, state transitions, or data flows across system boundaries.

**Before:**
```markdown
The loader sends stress commands to the Scylla cluster while the monitor
collects metrics from Prometheus. The test runner coordinates the loader
and monitors the cluster health. When nemesis triggers, it disrupts a
node and the health checker validates recovery.
```

**After:**
```markdown
```
Test Runner
    ├── starts ──► Loader ──► stress ──► Scylla Cluster
    ├── starts ──► Monitor ──► scrape ──► Prometheus
    ├── triggers ──► Nemesis ──► disrupts ──► Scylla Node
    └── runs ──► Health Checker ──► validates ──► Scylla Cluster
```

**When to use diagrams**:
- 3+ components interacting
- State machines or lifecycle transitions
- Data flows across system boundaries (e.g., loader → cluster → monitor)
- Provisioning sequences across backends

---

### PAP-3: Overly Granular Phases

**Symptom:** Phases include implementation-level details — specific function signatures, line-by-line changes, exact variable names — instead of design-level deliverables.

**Why it's wrong:** Over-specifying in the plan:
- Constrains the implementer unnecessarily
- Becomes stale as soon as the code evolves
- Buries the design intent under implementation noise
- Makes the plan unreadable for stakeholders who need the "what" and "why", not the "how"

**Fix:** Phases should describe deliverables and design decisions, not line-by-line implementation. Leave the "how" for the PR.

**Before:**
```markdown
### Phase 1: Add Configuration Parameter

1. Open `sdcm/sct_config.py`
2. Add line after line 1842:
   `health_check_workers: Annotated[int, BeforeValidator(int_or_str)] = 5`
3. Add to `defaults/test_default.yaml` line 245:
   `health_check_workers: 5`
4. Update docstring on line 1843 with:
   `"""Number of parallel workers for health checks. Default: 5"""`
5. Run `uv run sct.py pre-commit` to regenerate docs
```

**After:**
```markdown
### Phase 1: Add Configuration Parameter

**Description**: Add `health_check_workers` parameter to `sdcm/sct_config.py`
with default value 5, controlling the number of parallel health check workers.

**Deliverables**:
- New config field in `sdcm/sct_config.py`
- Default value in `defaults/test_default.yaml`
- Auto-generated documentation update

**Definition of Done**:
- [ ] Parameter accepted in test configuration YAML
- [ ] Default value used when not specified
- [ ] `uv run sct.py pre-commit` passes
```

---

### PAP-4: Vague or Unmeasurable Goals

**Symptom:** Goals use words like "improve", "better", "optimize", "enhance" without concrete criteria for what success looks like.

**Why it's wrong:** Vague goals cannot be validated. There is no way to know when the work is done. Different reviewers will have different interpretations of "improve performance." The plan has no accountability.

**Fix:** Every goal must have a measurable criterion or a verifiable condition. If a metric is unknown, state the target condition explicitly.

**Before:**
```markdown
## Goals

1. Improve health check performance
2. Make health checks more reliable
3. Better error handling
4. Optimize resource usage
```

**After:**
```markdown
## Goals

1. **Reduce health check time by 90%+** for 60-node clusters (from ~120min to <12min)
2. **Skip health checks** when the previous nemesis operation was skipped
3. **Isolate node failures** so one unhealthy node does not block checks on others
4. **Limit parallel workers** to a configurable maximum (default: 5, max: 10) to bound resource usage
```

---

### PAP-5: Conflicting or Contradictory Requirements

**Symptom:** Different sections of the plan state incompatible goals, design decisions, or constraints. For example: a goal says "minimize configuration options" while a phase adds 5 new parameters, or one phase requires sequential execution while another assumes parallel.

**Why it's wrong:** Conflicting requirements cause implementation deadlocks — the implementer cannot satisfy both constraints and must guess which one the plan author actually intended. This leads to rework, incorrect implementations, or abandoned phases.

**Fix:** Before finalizing, cross-check the plan for consistency:
- Do all phases support the stated goals?
- Does the risk mitigation section address risks that the implementation creates?
- Are assumptions in one section contradicted by constraints in another?
- If tradeoffs exist, state them explicitly with a chosen resolution.

**Before:**
```markdown
## Goals
3. **Minimize configuration surface** — avoid adding new parameters

## Implementation Phases
### Phase 2: Add Configuration Options
- Add `health_check_mode` parameter (full/lite/sample)
- Add `health_check_parallel_workers` parameter
- Add `health_check_sample_size` parameter
- Add `health_check_timeout` parameter
- Add `health_check_skip_on_nemesis_skip` parameter
```

**After:**
```markdown
## Goals
3. **Provide focused configuration** — add only essential parameters with sensible defaults

## Implementation Phases
### Phase 2: Add Configuration Options
- Add `health_check_parallel_workers` parameter (default: 5)
- The skip-on-nemesis-skip behavior is default, no parameter needed
- Health check mode (full/lite) controlled by existing `cluster_health_check` boolean

**Tradeoff note**: We chose to keep mode control in the existing boolean rather
than adding a new enum parameter, to balance configurability with simplicity.
```

---

## Structure Anti-Patterns

### PAP-6: Missing "Current State" Code References

**Symptom:** The Current State section describes how things work in general terms without pointing to specific files, classes, or methods.

**Why it's wrong:** Without code references, reviewers cannot verify the analysis is accurate. The implementer may look at the wrong code. The plan may describe outdated behavior.

**Fix:** Every claim about current behavior must cite a specific file path and optionally a class or method. Verify each reference exists using file-reading tools.

---

### PAP-7: Phases Without Definition of Done

**Symptom:** Phases describe what to build but have no checkboxes or verifiable criteria for completion.

**Why it's wrong:** Without Definition of Done, there is no agreed boundary for "this phase is complete." PRs may be merged with partial work, or reviewers may block PRs that are actually complete but lack documented criteria.

**Fix:** Every phase needs a `**Definition of Done**:` section with checkbox items (`- [ ] criterion`). Each criterion should be something a reviewer can verify.

---

### PAP-8: Monolithic Phase Spanning Multiple PRs

**Symptom:** A single phase contains too many deliverables to fit in one Pull Request — refactoring + new feature + tests + configuration + documentation all in one phase.

**Why it's wrong:** Large PRs are hard to review, slow to merge, and risky to revert. They also create merge conflicts with other ongoing work.

**Fix:** Split into smaller phases, each scoped to a single PR. Use dependencies to maintain order. A good phase produces 1-3 related deliverables.

---

## Quick Reference

| ID | Anti-Pattern | One-Line Fix |
|----|-------------|-------------|
| PAP-1 | Too much code in the plan | Use short snippets for interfaces only; describe behavior in prose |
| PAP-2 | Missing diagrams for complex interactions | Add ASCII/Mermaid diagrams for 3+ component interactions |
| PAP-3 | Overly granular phases | Describe deliverables, not line-by-line changes |
| PAP-4 | Vague or unmeasurable goals | Add measurable criteria or verifiable conditions |
| PAP-5 | Conflicting requirements | Cross-check goals, phases, and risks for consistency |
| PAP-6 | Missing code references in Current State | Cite specific file paths, verify they exist |
| PAP-7 | Phases without Definition of Done | Add checkbox criteria to every phase |
| PAP-8 | Monolithic phase spanning multiple PRs | Split into single-PR-scoped phases |
