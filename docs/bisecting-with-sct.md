# Bisecting with SCT
There was an effort to make it easier to bisect with SCT (performance tests). The following is a guide on how to do it.
currently works with ubuntu/debian only.

# steps
## modify test
1. decorate the test you want to bisect with the `@bisect_test` decorator (from `from sdcm.utils.bisect_test import bisect_test`)
2. Decorator will add two class variables to the test class:
    * `bisect_result_value` - defines current test result value as comparable number
    * `bisect_ref_value` - define reference test result value as comparable number
3. Make the test to set `bisect_ref_value` only for the first test run
4. Each run should also update `bisect_result_value` with the result value for further comparison
3. Upon test end, decorator will compare `bisect_result_value` and `bisect_ref_value` - if result value is greater than reference value, will mark current
Scylla version as 'good', otherwise as 'bad'
4. bisect decorator will erase data, reinstall Scylla to next bisected version (based on result) and rerun the test

## run test
Run test with providing `bisect_start_date` and `bisect_end_date` (optionally) sct config variables.
Also adapt `test_duration` to larger value (e.g. 8x current duration).
Based on these dates, decorator will
find available packages and start bisecting process.
Verify logs to see last good and bad Scylla versions.

# example
```python
from sdcm.utils.bisect_test import bisect_test

@bisect_test
def test_write(self):
    base_cmd_w = self.params.get('stress_cmd_w')
    stress_multiplier = self.params.get('stress_multiplier')
    if stress_multiplier_w := self.params.get("stress_multiplier_w"):
        stress_multiplier = stress_multiplier_w

    self.create_test_stats(doc_id_with_timestamp=True)
    self.run_fstrim_on_all_db_nodes()

    stress_queue = self.run_stress_thread(
        stress_cmd=base_cmd_w, stress_num=stress_multiplier, stats_aggregate_cmds=False)
    results = self.get_stress_results(queue=stress_queue)

    # set bisect_ref_value only for the first test run
    self.bisect_ref_value = self.bisect_result_value * 0.95 if self.bisect_ref_value is None else self.bisect_ref_value
    # update bisect_result_value with the result value for further comparison
    self.bisect_result_value = sum([int(result['op rate']) for result in results])
    self.build_histogram(stress_queue.stress_operation, hdr_tags=stress_queue.hdr_tags)
    self.update_test_details(scylla_conf=True)
    self.display_results(results, test_name='test_write')
    self.check_regression()
```
