# Workflow: Find Performance Bottlenecks in SCT Code

Step-by-step process for identifying and resolving performance issues in SCT.

## Step 1: Reproduce and Quantify

Before profiling, confirm the performance issue and establish a baseline.

```bash
# Time the test to get a baseline
time python3 -m pytest -xvs unit_tests/test_config.py::test_config_default
```

Note the wall-clock time. This is your baseline to compare against after optimization.

## Step 2: Get a High-Level Profile (cProfile)

Start with cProfile for a quick overview of where time is spent.

```bash
python3 -m cProfile -o ./profile.stats -m pytest -xvs unit_tests/test_config.py::test_config_default
```

Inspect the top 20 functions by cumulative time:
```bash
python3 -c "import pstats; p = pstats.Stats('./profile.stats'); p.sort_stats('cumulative'); p.print_stats(20)"
```

If one or two functions dominate, you have your target. If the time is spread across many functions, continue to Step 3.

## Step 3: Get Line-Level Detail (scalene)

Use scalene to see which specific lines are expensive and whether the cost is in Python or native code.

```bash
uv pip install scalene
scalene run --- -m pytest -xvs unit_tests/test_config.py::test_config_default
```

Open the HTML report. Look for:
- Lines with high "Python %" — these are optimizable Python code
- Lines with high "C %" — these are in native extensions (harder to optimize directly)
- Lines with high "Memory" — these allocate heavily

## Step 4: Check for Memory Issues (memray)

If the problem is memory growth or leaks:

```bash
uv pip install pytest-memray
python3 -m pytest --memray -xvs unit_tests/test_config.py::test_config_default
```

Or for detailed analysis:
```bash
uv pip install memray
memray run -o ./memray.bin -m pytest -xvs unit_tests/test_config.py::test_config_default
memray flamegraph ./memray.bin -o ./memray.html
open ./memray.html
memray stats ./memray.bin
```

## Step 5: Optimize and Verify

1. Make the targeted change based on profiling results.
2. Re-run the same profile to confirm the bottleneck is resolved.
3. Compare baseline time (Step 1) with the new time.
4. Run the full test suite to ensure no regressions.

## Tips

- Always profile with `-xvs` in pytest — `-x` stops on first failure, `-v` gives verbose output, `-s` shows print statements.
- For long-running SCT integration tests, use `py-spy --pid` to attach to a running process without restarting it.
- Save profile outputs with timestamps (e.g., `profile-before.stats`, `profile-after.stats`) to compare side by side.
- If a test imports many modules, profile imports separately: `python3 -X importtime -m pytest -xvs ...` and check stderr.
