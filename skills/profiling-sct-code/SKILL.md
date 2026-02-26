---
name: Profiling SCT Code
description: Profile Python code in SCT to find CPU, memory, and concurrency bottlenecks using standard 3rd party tools.
---

# Profiling SCT Code

This skill guides you through profiling SCT code to find performance bottlenecks — CPU hotspots, memory leaks, slow imports, or concurrency issues.

## When to Use This Skill

- A test or framework operation is unexpectedly slow
- Memory usage grows unbounded during a long test
- You need to understand the execution flow across threads
- You want to find which functions or lines dominate CPU time
- You need to verify that an optimization actually improved performance

## Tools Overview

| Tool | Best For | Python 3.14 |
|------|----------|:-----------:|
| **cProfile + snakeviz** | Exact call counts, cumulative time, call trees | ✅ |
| **scalene** | Line-level CPU + memory profiling in one run | ✅ |
| **viztracer** | Timeline traces, concurrency analysis, execution flow | ✅ |
| **memray** | Memory allocations, leak detection | ✅ |
| **py-spy** | Zero-overhead sampling, attach to running processes | ❌ ([#750](https://github.com/benfred/py-spy/issues/750)) |

## Choosing the Right Tool

1. **"Which function is slowest?"** → Use **cProfile** (deterministic, exact counts)
2. **"Which line is slowest and is it Python or C?"** → Use **scalene** (line-level, Python vs native split)
3. **"What happens when, across threads?"** → Use **viztracer** (timeline trace)
4. **"Where is memory allocated / leaked?"** → Use **memray** (allocation tracking)
5. **"Profile a running process with zero overhead?"** → Use **py-spy** (sampling, attach by PID)

## Quick Reference

### Profile a unit test

```bash
# cProfile
python3 -m cProfile -o ./profile.stats -m pytest -xvs unit_tests/test_config.py::test_config_default
uv pip install snakeviz && snakeviz ./profile.stats

# scalene
uv pip install scalene
scalene run --- -m pytest -xvs unit_tests/test_config.py::test_config_default

# viztracer
uv pip install viztracer
viztracer -o ./result.json -- python3 -m pytest -xvs unit_tests/test_config.py::test_config_default
vizviewer ./result.json

# memray
uv pip install pytest-memray
python3 -m pytest --memray -xvs unit_tests/test_config.py::test_config_default

# py-spy (Python ≤ 3.13 only)
uv pip install py-spy
py-spy record -s -o ./profile.svg -- python3 -m pytest -xvs unit_tests/test_config.py::test_config_default
```

### Profile a full SCT test run

```bash
# cProfile
python3 -m cProfile -o ./profile.stats sct.py run-test ...

# scalene
scalene run sct.py --- run-test ...

# viztracer
viztracer --tracer_entries 10000000 -o ./result.json -- python3 sct.py run-test ...

# memray
memray run -o ./memray.bin python3 sct.py run-test ...

# py-spy (attach to running process)
py-spy record -s -o ./profile.svg --pid <PID>
```

## Key Principles

- **Profile before optimizing** — always measure to confirm where the bottleneck actually is.
- **Use the simplest tool first** — cProfile is always available and often sufficient.
- **Compare before and after** — save profile outputs to compare after making changes.
- **Profile representative workloads** — profiling a trivial test may miss the real bottleneck.
- **Minimize profiler overhead** — use sampling profilers (py-spy) for production-like scenarios; deterministic profilers (cProfile) add overhead that can distort results.

## Full Documentation

See [docs/install-local-env.md — Profiling SCT Code](../docs/install-local-env.md#profiling-sct-code) for detailed per-tool instructions, workflow guidance, and references.
