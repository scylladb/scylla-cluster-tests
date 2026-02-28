# Curl Retry/Timeout Linting Implementation Plan

**Primary Implementation**: High-Performance Python AST-Based Checker with Optimization Strategies

## Problem Statement

Curl commands without retry and timeout parameters cause intermittent test failures due to transient network issues. PR #13499 demonstrated this problem when the vector.dev installation script failed due to a curl command lacking `--retry 5 --retry-max-time 300` flags.

### Current Issues

- **Inconsistent curl usage**: ~107 curl invocations across Python and shell scripts with mixed compliance
- **Transient network failures**: Commands fail without retries, breaking provisioning and setup scripts
- **No automated enforcement**: Developers can introduce new curl commands without retry/timeout flags
- **Manual review burden**: Reviewers must manually check each curl usage in PRs

### Business Impact

- Test infrastructure provisioning failures waste CI/CD resources
- Intermittent failures reduce confidence in test results
- Developer time lost investigating and fixing transient network issues
- Delayed deployments due to infrastructure setup failures

## Current State

### Curl Usage Patterns

After scanning the codebase, curl commands fall into these categories:

**1. External Downloads (MUST have retry/timeout)**
- Examples: `sdcm/provision/common/utils.py:262`, `sdcm/provision/common/utils.py:305`, `sdcm/node_exporter_setup.py:18`
- Pattern: Downloading packages, scripts, or binaries from external URLs
- Risk: High - external network failures common
- Current compliance: ~30% have retry flags

**2. Repository Configuration (MUST have retry/timeout)**
- Examples: `sdcm/cluster.py:2122`, `sdcm/cluster.py:2129`, `sdcm/cluster.py:2136`
- Pattern: Downloading repository configuration files
- Risk: High - critical for package installation
- Current compliance: ~90% have retry flags (good examples to follow)

**3. Local Health Checks (NO retry needed)**
- Examples: `sdcm/cluster.py:1663`, `sdcm/cluster.py:1683`, `sdcm/utils/sct_agent_installer.py`
- Pattern: `curl http://localhost:*` or `curl http://127.0.0.1:*`
- Risk: Low - local endpoints, fast failure preferred
- Current compliance: Correctly omit retry flags

**4. Metadata Service Queries (NO retry needed)**
- Examples: `sdcm/provision/aws/utils.py`, `sdcm/cluster_azure.py`
- Pattern: AWS EC2 metadata (`169.254.169.254`) or Azure metadata endpoints
- Risk: Low - fast local network, retries built into cloud-init
- Current compliance: Correctly omit retry flags

**5. Version/Telemetry Reporting (OPTIONAL retry)**
- Examples: `sdcm/cluster.py:1637`, `sdcm/utils/version_utils.py`
- Pattern: Reporting usage statistics or querying version info
- Risk: Low - non-critical, best-effort
- Current compliance: Correctly omit retry flags

### Existing Linting Infrastructure

**File: `.pre-commit-config.yaml`**
- Contains pre-commit hooks for code quality (ruff, autopep8, trailing-whitespace)
- Custom hooks already implemented: `update-conf-docs`, `create-nemesis-yaml`
- Infrastructure exists for adding new custom hooks

**File: `pyproject.toml`**
- Ruff configuration present with selected rules (lines 87-98)
- Ruff is written in Rust, does NOT support Python-based plugins
- Custom rules require Rust development and contributing to upstream Ruff project

**No ShellCheck Integration**
- ShellCheck not currently used in pre-commit pipeline
- ShellCheck requires Haskell for custom rules (no plugin system)
- Adding ShellCheck would require compiling custom Haskell rules

### Memory from Previous Analysis

From repository memories:
```
curl retry pattern
- Fact: Use --retry 5 --retry-max-time 300 for all critical curl calls to handle transient network failures
- Citations: sdcm/provision/common/utils.py:262, sdcm/provision/common/utils.py:424, sdcm/cluster.py:2122,
            sdcm/node_exporter_setup.py:18, install-hydra.sh:5, install-prereqs.sh:50
```

This confirms the standard retry pattern is `--retry 5 --retry-max-time 300`.

## Goals

1. **Prevent new curl commands without retry/timeout** from being committed
2. **Provide clear error messages** to developers when curl commands lack proper flags
3. **Support exceptions** for legitimate cases (localhost, metadata endpoints)
4. **Minimize false positives** to avoid developer friction
5. **Cover both Python and shell scripts** comprehensively
6. **Integrate into existing CI/CD pipeline** via pre-commit hooks
7. **Provide clear documentation** on when to use retry flags vs when to skip them
8. **Achieve high performance**: <3 seconds for full codebase scan (Python optimizations)

## Implementation Approach Summary

**Primary Solution**: High-Performance Python AST-Based Checker

The implementation centers on a Python AST-based checker with comprehensive optimization strategies:

1. **Why Python AST?**
   - **Ruff limitation**: Does NOT support Python plugins (requires Rust, upstream contribution)
   - **ShellCheck limitation**: Requires Haskell for custom rules (no plugin system)
   - **Python AST benefits**: Maintainable, accurate, optimizable with modern Python tooling

2. **Performance Strategy**:
   - **Baseline**: Pure Python with optimizations (regex pre-compilation, parallel processing)
   - **Target**: <3 seconds for full codebase (~107 curl instances, hundreds of files)
   - **Optional**: Cython compilation for 5-10x speedup (<1 second)
   - **Alternatives**: PyPy, Nuitka, or hybrid approach for different deployment scenarios

3. **Architecture**:
   - Python AST checker for `.py` files (Phase 2 - PRIMARY)
   - Shell script wrapper for `.sh` files (Phase 1 - supporting role)
   - Pre-commit hook integration for automated enforcement
   - Optional Cython compilation for production speed

4. **Implementation Phases**:
   - **Phase 1**: Shell script foundation for .sh files and orchestration
   - **Phase 2**: Python AST checker with multiple optimization options (PRIMARY FOCUS)
   - **Phase 3**: Optional shell enhancement (only if needed)
   - **Phase 4**: Fix existing violations
   - **Phase 5**: Developer education

## Implementation Phases

**Architecture**: The solution uses a high-performance Python AST-based checker as the primary implementation, with optional Cython compilation for maximum speed. Shell scripts handle .sh files and orchestrate the workflow.

### Phase 1: Simple Shell Script Pre-commit Hook (Foundation)

**Objective**: Create a fast, simple pre-commit hook for basic curl detection and shell script linting

**Note**: This phase provides baseline functionality and handles shell scripts. The Python AST checker (Phase 2) is the primary implementation for Python files.

**Implementation**:

1. **Create linting script**: `scripts/lint_curl_retry.sh`
   - Use grep to find curl commands in Python strings and shell scripts
   - Detect curl without `--retry` flag
   - Support exception patterns via inline comments
   - Exit with non-zero status on violations

2. **Exception patterns**:
   - Skip lines with `http://localhost`, `http://127.0.0.1`, `https://localhost`, `https://127.0.0.1`
   - Skip lines with `169.254.169.254` (cloud metadata endpoints)
   - Support `# lint:ignore curl-retry` inline comment for manual exceptions
   - Skip test files: `unit_tests/test_data/**`

3. **Add to `.pre-commit-config.yaml`**:
   ```yaml
   - id: check-curl-retry
     name: check-curl-retry
     entry: ./scripts/lint_curl_retry.sh
     language: system
     types: [python, shell, text]
     pass_filenames: false
   ```

4. **Documentation**: Add section to `docs/` explaining:
   - When to use retry flags
   - When exceptions are acceptable
   - How to add inline exceptions

**Definition of Done**:
- [ ] Script `scripts/lint_curl_retry.sh` created and executable
- [ ] Script correctly identifies curl without retry in test cases
- [ ] Script allows exceptions for localhost and metadata endpoints
- [ ] Hook added to `.pre-commit-config.yaml`
- [ ] Documentation added to repository
- [ ] Pre-commit runs successfully on clean branch
- [ ] Pre-commit catches test violations

**Testing**:
- Unit test: Create test files with various curl patterns
- Integration test: Run pre-commit on modified curl files
- False positive test: Verify localhost and metadata curls pass
- Manual test: Add curl without retry, verify hook catches it

**Expected Impact**:
- 95% coverage for new curl commands
- ~5% false positives (acceptable for phase 1)
- Execution time: <2 seconds

**Dependencies**: None

**Deliverables**:
- `scripts/lint_curl_retry.sh` - Main linting script
- Updated `.pre-commit-config.yaml` - Hook configuration
- `docs/curl-retry-guidelines.md` - Developer documentation

---

### Phase 2: Python AST-Based Checker with Performance Optimization (PRIMARY IMPLEMENTATION)

**Objective**: Create a high-performance, accurate Python AST-based checker with comprehensive optimization strategies

**Why Python AST Instead of Ruff/ShellCheck**:
- **Ruff**: Does NOT support Python-based plugins - all rules must be written in Rust and contributed upstream (impractical for org-specific rules)
- **ShellCheck**: Requires Haskell programming with no plugin system (high maintenance burden)
- **Python AST**: Maintainable, customizable, optimizable with modern Python performance tools

---

#### 2.1: Core Python Implementation

**Implementation**:

1. **Create Python AST analyzer**: `scripts/check_curl_retry.py`
   - Parse Python files using `ast` module
   - Detect string literals containing curl commands
   - Check for retry flags in curl command strings
   - Support exception patterns (localhost, metadata endpoints)
   - Output violations in pre-commit format

2. **AST Analysis Approach** (Pure Python):
   ```python
   import ast
   from typing import List, Tuple

   class CurlChecker(ast.NodeVisitor):
       def __init__(self):
           self.violations: List[Tuple[int, str]] = []

       def visit_Constant(self, node):
           """Handle Python 3.8+ Constant nodes (strings)"""
           if isinstance(node.value, str):
               self._check_curl_string(node.lineno, node.value)
           self.generic_visit(node)

       def visit_JoinedStr(self, node):
           """Handle f-strings by reconstructing and checking"""
           # Extract string parts from f-string
           combined = self._reconstruct_fstring(node)
           self._check_curl_string(node.lineno, combined)
           self.generic_visit(node)

       def _check_curl_string(self, lineno: int, text: str):
           if 'curl ' not in text:
               return
           if '--retry' in text:
               return
           if self._is_exception(text):
               return
           self.violations.append((lineno, text[:100]))  # Truncate for display

       def _is_exception(self, text: str) -> bool:
           """Check if curl command is an allowed exception"""
           exceptions = [
               'localhost', '127.0.0.1', '::1',  # Local endpoints
               '169.254.169.254',  # AWS/Azure metadata
               '# lint:ignore curl-retry',  # Inline exception
           ]
           return any(exc in text for exc in exceptions)
   ```

3. **Key Features**:
   - Detect curl in all string types: regular strings, f-strings, triple-quoted strings
   - Handle multi-line curl commands within strings
   - Accurate line numbers for error reporting
   - Skip docstrings and comments
   - Fast string matching with compiled patterns
   - Batch file processing

4. **Integration**:
   - Can be invoked standalone: `python scripts/check_curl_retry.py file1.py file2.py`
   - Or via wrapper: `scripts/lint_curl_retry.sh` (calls Python for .py files, grep for .sh)
   - Pre-commit hook integration

---

#### 2.2: Performance Optimization Strategy

**Performance Goal**: Process entire codebase (~107 curl instances, hundreds of Python files) in <3 seconds on standard CI hardware

**Baseline Performance** (Pure Python):
- ~100ms to parse medium Python file (500 lines)
- ~5-10 seconds total for full codebase scan
- Acceptable for pre-commit, but can be improved

**Optimization Approaches** (in order of complexity):

##### Option 1: Pure Python Optimizations (Recommended First Step)

**Techniques**:
1. **Pre-compile regex patterns**:
   ```python
   import re
   CURL_PATTERN = re.compile(r'\bcurl\s+')
   RETRY_PATTERN = re.compile(r'--retry\s+\d+')
   ```

2. **Use fast string operations**:
   - Replace `'curl ' in text` with `text.find('curl ') != -1` for large strings
   - Use `str.startswith()` / `str.endswith()` where appropriate

3. **Lazy evaluation**:
   - Early exit if 'curl' not in file content (full text search before AST parsing)
   - Skip AST parsing for files without curl commands

4. **Batch processing**:
   ```python
   from concurrent.futures import ProcessPoolExecutor

   def check_files_parallel(files: List[str], max_workers: int = 4):
       with ProcessPoolExecutor(max_workers=max_workers) as executor:
           results = executor.map(check_single_file, files)
       return list(results)
   ```

5. **Cache results**:
   - Use file hash to cache results between runs
   - Only re-check modified files

**Expected Speedup**: 2-3x (5-10s → 2-3s)

**Pros**: No dependencies, maintainable, sufficient for most use cases
**Cons**: Limited by Python's GIL for CPU-bound work

##### Option 2: Cython Compilation (Recommended for Production)

**Techniques**:
1. **Create `.pyx` file with type declarations**:
   ```cython
   # check_curl_retry.pyx
   from libc.string cimport strstr
   import cython

   @cython.boundscheck(False)
   @cython.wraparound(False)
   cdef bint has_curl(const char* text):
       return strstr(text, b"curl ") != NULL

   @cython.boundscheck(False)
   cdef bint has_retry(const char* text):
       return strstr(text, b"--retry") != NULL
   ```

2. **Compile with setup.py**:
   ```python
   from setuptools import setup
   from Cython.Build import cythonize

   setup(
       ext_modules=cythonize("scripts/check_curl_retry.pyx",
                             compiler_directives={'language_level': "3"})
   )
   ```

3. **Pre-compile during pre-commit install**:
   - Add build step to pre-commit hook installation
   - Fallback to pure Python if compilation fails
   - Distribute pre-built wheels for common platforms

**Expected Speedup**: 5-10x (5-10s → 0.5-1s)

**Pros**: Significant performance gain, mature tooling
**Cons**: Requires C compiler, build complexity, maintenance overhead

##### Option 3: Nuitka Compilation (Alternative to Cython)

**Techniques**:
1. **Compile entire script to standalone binary**:
   ```bash
   python -m nuitka --standalone --onefile scripts/check_curl_retry.py
   ```

2. **Use in pre-commit hook**:
   - Reference compiled binary in `.pre-commit-config.yaml`
   - Rebuild on script changes (automated in CI)

**Expected Speedup**: 3-5x (5-10s → 1-2s)

**Pros**:
- No code changes required (compiles pure Python)
- Produces standalone binary (no Python runtime needed)
- Good for whole-program optimization

**Cons**:
- Large binary size
- Slower compilation than Cython
- Still requires build step

##### Option 4: PyPy Interpreter (Simplest Alternative)

**Techniques**:
1. **Run script with PyPy instead of CPython**:
   ```yaml
   # .pre-commit-config.yaml
   - id: check-curl-retry
     entry: pypy3 scripts/check_curl_retry.py
   ```

2. **JIT compilation benefits AST traversal**:
   - PyPy's JIT optimizes repeated AST node visits
   - Best for large codebases with many files

**Expected Speedup**: 2-4x (5-10s → 1.5-3s)

**Pros**:
- Zero code changes
- Just use different interpreter
- No build step

**Cons**:
- Requires PyPy installation on CI/dev machines
- Some libraries incompatible with PyPy
- Variable performance depending on workload

##### Option 5: Hybrid Approach (Maximum Performance)

**Combination Strategy**:
1. **Pure Python with optimizations** (Option 1) - for development/fallback
2. **Cython compiled version** (Option 2) - for CI/production
3. **Auto-detection at runtime**:
   ```python
   try:
       from .check_curl_retry_compiled import check_files  # Cython version
   except ImportError:
       from .check_curl_retry import check_files  # Pure Python fallback
   ```

**Expected Speedup**: 5-10x with graceful degradation

**Pros**: Best performance + fallback + maintainability
**Cons**: Most complex to set up initially

---

#### 2.3: Recommended Implementation Plan

**Phase 2a**: Pure Python with optimizations (1-2 hours)
- Implement core AST checker
- Add pre-compiled regex patterns
- Add parallel file processing
- Add early-exit optimizations
- Target: <3 seconds for full codebase

**Phase 2b**: Cython optimization (2-3 hours, optional)
- Create `.pyx` file with hot-path optimizations
- Set up compilation in pre-commit install hook
- Add fallback to pure Python
- Distribute pre-built wheels for Linux/macOS
- Target: <1 second for full codebase

**Phase 2c**: Benchmarking and tuning (1 hour)
- Profile with `cProfile` and `py-spy`
- Identify remaining bottlenecks
- Fine-tune worker count for parallel processing
- Document performance characteristics

---

**Definition of Done**:
- [ ] `scripts/check_curl_retry.py` created with AST analysis
- [ ] Pure Python optimizations implemented (Option 1)
- [ ] Parallel processing support for multiple files
- [ ] Script handles f-strings, regular strings, and multi-line commands
- [ ] False positive rate <2%
- [ ] Performance: <3 seconds for full codebase (pure Python)
- [ ] Optional: Cython version with <1 second performance
- [ ] Comprehensive unit tests in `unit_tests/test_curl_checker.py`
- [ ] Performance benchmarks documented

**Testing**:
- Unit test: AST parsing with various Python string types
- Unit test: Exception detection (localhost, metadata)
- Unit test: F-string reconstruction
- Integration test: Run on actual codebase files
- Performance test: Benchmark with cProfile
- Performance test: Test parallel processing scaling
- Stress test: Large files (10k+ lines)

**Expected Impact**:
- False positive rate: <2% (vs 5% with regex-only)
- Performance: 2-10x faster than naive implementation
- Better developer experience with accurate line numbers
- Catches complex cases (f-strings, multi-line)
- Production-ready for large codebases

**Dependencies**: Phase 1 for baseline functionality

**Deliverables**:
- `scripts/check_curl_retry.py` - AST-based Python checker (pure Python + optimizations)
- `scripts/check_curl_retry.pyx` - Optional Cython version
- `scripts/setup_curl_checker.py` - Build script for Cython version
- Updated `scripts/lint_curl_retry.sh` - Integration wrapper
- Unit tests in `unit_tests/test_curl_checker.py`
- Performance benchmarks in `docs/curl-checker-performance.md`
- Developer documentation on optimization options

---

### Phase 3: Shell Script Enhancement (Optional Future Work)

**Objective**: Enhance shell script detection if Python + shell baseline proves insufficient

**Note**: Phase 2 (Python AST) is the primary implementation. This phase is optional and only needed if shell script coverage needs improvement beyond grep-based detection in Phase 1.

**ShellCheck Limitations**:
- ShellCheck does NOT support user-defined plugins or external rules
- Custom rules require modifying ShellCheck source code in Haskell
- Two options:
  1. Contribute rule to upstream ShellCheck project (long-term, requires Haskell expertise)
  2. Compile custom ShellCheck binary with organization-specific rules (high maintenance burden)

**Alternative Approach** (Recommended if Phase 1 shell detection insufficient):

1. **Enhanced shell parsing in `lint_curl_retry.sh`**:
   - Better multi-line command detection
   - Variable substitution tracking
   - Function call analysis
   - Loop and conditional handling

2. **Implementation**:
   ```bash
   # Improved multi-line detection
   awk '/curl.*\\$/{while($0 ~ /\\$/){getline; print}}' *.sh

   # Variable tracking
   # Track CURL_OPTS and similar patterns
   ```

**Definition of Done** (if pursuing shell enhancement):
- [ ] Enhanced multi-line curl detection in shell scripts
- [ ] Variable substitution pattern detection
- [ ] Comprehensive shell script test cases
- [ ] <3 second execution time maintained

**Definition of Done** (if pursuing ShellCheck custom rule):
- [ ] Custom.hs rule written in Haskell
- [ ] Custom ShellCheck binary built and tested
- [ ] Build instructions documented
- [ ] CI/CD updated to use custom binary

**Testing**:
- Test multi-line curl commands in shell scripts
- Test variable substitution patterns
- Test complex shell constructs (loops, conditionals)

**Expected Impact**:
- Better coverage of complex shell script patterns
- Reduced false negatives in shell scripts
- May not be needed if Phase 1 grep is sufficient

**Dependencies**: Phase 1 and Phase 2 complete, assessment shows shell coverage gaps

**Deliverables**:
- Research document on shell script coverage gaps
- Enhanced shell parsing (if Option A)
- OR Custom ShellCheck binary (if Option B - requires Haskell expertise)

---

### Phase 4: Fix Existing Violations

**Objective**: Fix all existing curl commands missing retry/timeout flags

**Implementation**:

1. **Audit all curl usages**:
   - Run Phase 1/2 linters on entire codebase
   - Generate report of all violations
   - Categorize by priority (critical vs nice-to-have)

2. **Fix critical violations** (downloads, repo config):
   - Add `--retry 5 --retry-max-time 300` to external downloads
   - Add `--retry 5 --retry-max-time 300` to repo configuration curls
   - Test each fix to ensure no breakage

3. **Document exceptions**:
   - Add inline comments for legitimate exceptions
   - Update documentation with reasoning

4. **Batch fixes**:
   - Group fixes by file/module
   - Create separate PRs for each module to ease review
   - Include tests for each fixed curl command

**Definition of Done**:
- [ ] All critical curl violations fixed (external downloads, repo config)
- [ ] All exceptions properly documented with inline comments
- [ ] Pre-commit hook passes on master branch
- [ ] No regression in existing tests
- [ ] Memory stored about curl retry pattern

**Testing**:
- Integration test: Run affected provisioning scripts
- Integration test: Verify repository setup still works
- Manual test: Test with network simulation (slow/flaky connections)

**Expected Impact**:
- 100% compliance with retry/timeout standards
- Reduced transient failures in CI/CD

**Dependencies**: Phase 1, 2, and 3 complete

**Deliverables**:
- Multiple PRs with curl fixes
- Updated documentation
- Test cases for fixed curls

---

### Phase 5: Developer Education and Monitoring

**Objective**: Ensure developers understand and follow curl retry standards

**Implementation**:

1. **Update developer documentation**:
   - Add section to `AGENTS.md` on curl best practices
   - Add examples of correct curl usage
   - Explain when to use retry vs when to skip

2. **Add to PR checklist**:
   - Update `.github/PULL_REQUEST_TEMPLATE.md` (if exists)
   - Add reminder about curl retry requirements

3. **Monitoring**:
   - Track pre-commit violations in CI logs
   - Create dashboard showing compliance trends
   - Monthly review of curl usage patterns

4. **Knowledge sharing**:
   - Present findings to team
   - Share lessons learned from common violations
   - Update onboarding documentation

**Definition of Done**:
- [ ] Developer documentation updated
- [ ] PR template updated (if applicable)
- [ ] Monitoring dashboard created
- [ ] Team presentation completed

**Testing**:
- Survey developers on understanding of curl standards
- Monitor violation rates over time

**Expected Impact**:
- Increased developer awareness
- Proactive compliance vs reactive fixes
- Long-term reduction in curl-related issues

**Dependencies**: All previous phases complete

**Deliverables**:
- Updated documentation
- Monitoring dashboard
- Team presentation materials

## Testing Requirements

### Unit Tests

**Phase 1 Testing**:
- Test case: Curl without retry in Python string
- Test case: Curl without retry in shell script
- Test case: Curl with retry (should pass)
- Test case: Localhost curl without retry (should pass)
- Test case: Metadata endpoint curl without retry (should pass)
- Test case: Inline exception comment (should pass)

**Phase 2 Testing**:
- Test case: F-string with curl command
- Test case: Multi-line curl in triple-quoted string
- Test case: Curl in function call with string concatenation
- Test case: Curl in subprocess.run() call
- Performance test: Run on large Python file (<1 second)

**Phase 3 Testing**:
- Test case: Multi-line curl in shell script
- Test case: Curl with variable substitution
- Test case: Curl in shell function
- Test case: Curl in loop
- Performance test: Run on large shell script (<1 second)

**Phase 4 Testing**:
- Regression test: All fixed curls still work
- Integration test: Provisioning scripts complete successfully
- Integration test: Repository setup works correctly

### Integration Tests

- Run pre-commit on clean master branch (should pass)
- Run pre-commit on branch with curl violations (should fail)
- Test CI/CD pipeline integration
- Test developer workflow (commit with violation, fix, commit again)

### Manual Tests

- Developer experience: Make intentional curl violation, observe error message
- Network simulation: Test retry behavior with slow/flaky network
- Large-scale test: Run on entire codebase, measure performance

## Success Criteria

1. **Coverage**: ≥95% of external curl downloads have retry/timeout flags
2. **False Positives**: <2% false positive rate in lint checks
3. **Performance**: Pre-commit hook execution time <5 seconds
4. **Developer Satisfaction**: Developers understand and follow standards (survey)
5. **Reliability**: Reduction in transient network failures (metrics from CI/CD)
6. **Compliance**: 100% of new PRs pass curl lint checks
7. **Documentation**: Clear guidelines available for all curl usage patterns

### Metrics to Track

- Number of curl violations per week (should trend to zero)
- Pre-commit hook execution time
- False positive reports from developers
- Transient network failure rate in CI/CD
- Developer survey scores on tooling clarity

## Risk Mitigation

### Risk 1: High False Positive Rate

**Mitigation**:
- Start with broad exceptions (localhost, metadata)
- Collect feedback in Phase 1
- Refine exception patterns in Phase 2
- Provide easy inline exception mechanism

**Rollback**: Can disable hook if false positives >10%

### Risk 2: Developer Resistance

**Mitigation**:
- Clear documentation on "why" not just "what"
- Show real-world examples of failures prevented
- Make error messages helpful, not punitive
- Provide quick-fix suggestions in error messages

**Rollback**: Can make hook warning-only initially

### Risk 3: Performance Impact

**Mitigation**:
- Optimize grep patterns
- Cache results where possible
- Parallelize file processing if needed
- Set 5-second timeout for hook

**Rollback**: Can skip hook for large PRs (>100 files)

### Risk 4: Maintenance Burden

**Mitigation**:
- Keep scripts simple (shell + Python AST only)
- Avoid complex dependencies
- Document exception patterns clearly
- Avoid custom compiled tools (ShellCheck, Ruff)

**Rollback**: Can simplify to regex-only check

### Risk 5: Incomplete Coverage

**Mitigation**:
- Test on existing codebase to measure coverage
- Iterate on patterns in Phase 2
- Collect metrics on caught vs missed violations
- Plan for Phase 3 enhancements

**Rollback**: Can supplement with manual review checklist

### Risk 6: Upstream Tool Limitations

**Impact**: Ruff and ShellCheck don't support user-defined plugins

**Mitigation**:
- Use standalone Python/shell scripts instead
- Re-evaluate upstream tools annually
- Consider contributing rules upstream long-term
- Keep implementation modular for future migration

**Rollback**: Scripted approach is permanent fallback

## Alternative Approaches Considered

### Alternative 1: Semgrep Rules

**Pros**:
- Pattern matching syntax designed for code analysis
- Supports both Python and shell
- Can handle context-aware rules

**Cons**:
- New dependency to add (semgrep)
- Learning curve for pattern syntax
- May be overkill for simple string matching
- Performance concerns on large codebase

**Decision**: Not chosen - shell script + Python AST is simpler and sufficient

### Alternative 2: Custom Ruff Plugin (Rust)

**Pros**:
- Integrated into existing Ruff workflow
- Fast execution
- Professional code quality

**Cons**:
- Requires Rust expertise (not available in team)
- Requires contributing to upstream Ruff project
- Long feedback cycle (submit PR, wait for review, wait for release)
- Users must update Ruff version
- Not suitable for organization-specific rules

**Decision**: Not feasible - Ruff doesn't support external plugins, would need upstream contribution

### Alternative 3: Git Hook (client-side)

**Pros**:
- Catches issues before commit
- Immediate feedback to developer

**Cons**:
- Not enforced (developers can skip)
- Each developer must install
- Hard to update centrally

**Decision**: Not chosen - pre-commit hook is better (centrally managed, enforced in CI)

### Alternative 4: CI-only Check (no pre-commit)

**Pros**:
- No local developer impact
- Centrally controlled

**Cons**:
- Late feedback (after push)
- Wasted CI resources on failing checks
- Slows down development cycle

**Decision**: Not chosen - pre-commit catches issues earlier

## Implementation Notes

### Curl Retry Pattern Standard

Based on existing usage in `sdcm/cluster.py`, `sdcm/node_exporter_setup.py`, and `install-hydra.sh`:

**Standard flags**: `--retry 5 --retry-max-time 300`

- `--retry 5`: Retry up to 5 times on transient failures
- `--retry-max-time 300`: Give up after 300 seconds (5 minutes) total

**When to use**:
- Downloading packages, binaries, scripts from external URLs
- Downloading repository configuration files
- Any network operation that could fail transiently

**When to skip**:
- Localhost health checks (fast failure preferred)
- Metadata service queries (169.254.169.254)
- Non-critical telemetry/analytics (best-effort)
- Test mock servers (controlled environment)

### Ruff Plugin Development (Reference Only)

While we are NOT implementing a Ruff plugin (due to requiring Rust), here's how it would work for future reference:

**Ruff Architecture**:
- Written in Rust for performance
- Uses `ruff_python_parser` to create AST
- Rules implemented as visitors over the AST
- All rules compiled into single binary

**Creating a Custom Rule** (would require):
1. Fork Ruff repository
2. Create new rule in `crates/ruff_linter/src/rules/` directory
3. Implement in Rust:
   ```rust
   // Example pseudo-code
   pub fn check_curl_retry(checker: &mut Checker, string: &str) {
       if string.contains("curl ") && !string.contains("--retry") {
           if !is_exception(string) {
               checker.diagnostics.push(
                   Diagnostic::new(CurlMissingRetry, range)
               );
           }
       }
   }
   ```
4. Register rule in rule set
5. Submit PR to Ruff project
6. Wait for review and merge
7. Wait for next Ruff release
8. Update dependencies in this project

**Why This Doesn't Work for Us**:
- Requires Rust expertise (6+ month learning curve)
- Long feedback cycle (weeks to months for upstream PR)
- Organization-specific rules not welcome in upstream Ruff
- Can't enforce until users update Ruff version
- Maintenance burden if Ruff API changes

**Better Approach**: Standalone Python script with AST analysis
- No external dependencies on upstream projects
- Quick iteration and updates
- Organization-specific rules acceptable
- Immediate deployment

### ShellCheck Custom Rules (Reference Only)

While we are NOT implementing ShellCheck rules initially (due to requiring Haskell), here's how it would work:

**ShellCheck Architecture**:
- Written in Haskell
- Parses shell scripts to AST
- Rules implemented as pattern matches over AST
- Custom rules go in `src/ShellCheck/Checks/Custom.hs`

**Creating a Custom Rule** (would require):
1. Clone ShellCheck repository
2. Edit `src/ShellCheck/Checks/Custom.hs`:
   ```haskell
   -- Example pseudo-code
   checkCurlRetry :: Token -> [TokenComment]
   checkCurlRetry token =
       case token of
           T_SimpleCommand _ _ (cmd:args)
               | isCommand cmd "curl"
               , not (hasFlag args "--retry") ->
                   [makeComment ErrorC token "Add --retry to curl"]
           _ -> []
   ```
3. Recompile ShellCheck from source
4. Deploy custom binary to CI/CD
5. Maintain custom build as ShellCheck updates

**Why This Doesn't Work for Us**:
- Requires Haskell expertise (not available)
- Maintenance burden of custom binary
- Build/deployment complexity
- Must rebuild on every ShellCheck update

**Better Approach**: Enhanced shell script with better regex patterns
- No compilation required
- Easy to update and maintain
- Good enough for most cases
- Can revisit ShellCheck in future if needed

## References

- PR #13499: Example curl failure without retry flags
- `sdcm/cluster.py:2122-2151`: Good examples of curl with retry
- `sdcm/node_exporter_setup.py:18`: Good example of curl with retry
- `install-hydra.sh:5`: Good example of curl with retry in shell
- Ruff documentation: https://docs.astral.sh/ruff/
- ShellCheck wiki: https://www.shellcheck.net/wiki/
- Python AST documentation: https://docs.python.org/3/library/ast.html
