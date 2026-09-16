# Jepsen tests

[← All configuration options](../configuration_options.md)

Jepsen consistency test runs.

**4 options.**


<a id="jepsen_scylla_repo"></a>

## **jepsen_scylla_repo** / SCT_JEPSEN_SCYLLA_REPO

Link to the git repository with Jepsen Scylla tests

**default:** https://github.com/jepsen-io/scylla.git

**type:** str (appendable)


<a id="jepsen_test_cmd"></a>

## **jepsen_test_cmd** / SCT_JEPSEN_TEST_CMD

Jepsen test command (e.g., 'test-all')

**default:** ['test-all -w cas-register --concurrency 10n', 'test-all -w counter --concurrency 10n', 'test-all -w cmap --concurrency 10n', 'test-all -w cset --concurrency 10n', 'test-all -w write-isolation --concurrency 10n', 'test-all -w list-append --concurrency 10n', 'test-all -w wr-register --concurrency 10n']

**type:** str | list[str] → list[str] (appendable)


<a id="jepsen_test_count"></a>

## **jepsen_test_count** / SCT_JEPSEN_TEST_COUNT

Possible number of reruns of single Jepsen test command

**default:** 1

**type:** int


<a id="jepsen_test_run_policy"></a>

## **jepsen_test_run_policy** / SCT_JEPSEN_TEST_RUN_POLICY

Jepsen test run policy (i.e., what we want to consider as passed for a single test)<br><br>'most' - most test runs are passed<br>'any'  - one pass is enough<br>'all'  - all test runs should pass

**default:** all

**type:** Literal['most', 'any', 'all']
