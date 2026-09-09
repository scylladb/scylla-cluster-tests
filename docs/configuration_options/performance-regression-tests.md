# Performance regression tests

[← All configuration options](../configuration_options.md)

Throughput/latency measurement runs, including gradual-throughput steps and HDR histogram
settings.

**18 options.**


<a id="max_deviation"></a>

## **max_deviation** / SCT_MAX_DEVIATION

Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one

**default:** N/A

**type:** float


<a id="n_stress_process"></a>

## **n_stress_process** / SCT_N_STRESS_PROCESS

Number of stress processes per loader

**default:** N/A

**type:** int


<a id="num_loaders_step"></a>

## **num_loaders_step** / SCT_NUM_LOADERS_STEP

Number of loaders which should be added per step

**default:** N/A

**type:** int


<a id="num_threads_step"></a>

## **num_threads_step** / SCT_NUM_THREADS_STEP

Number of threads which should be added on per step

**default:** N/A

**type:** int


<a id="perf_gradual_step_duration"></a>

## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** dict | YAML/JSON string → dict


<a id="perf_gradual_threads"></a>

## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** dict | YAML/JSON string → dict


<a id="perf_gradual_throttle_steps"></a>

## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Supports three formats: 1) String/int list (cassandra-stress): {'read': ['100000', '150000'], 'mixed': [100, 200]} 2) Dict list (latte/multi-param): {'read': [{'threads': 10, 'concurrency': 128, 'rate': '100000'}, ...]} Dict format allows specifying threads, concurrency, and rate per step. Integers are automatically converted to strings for backward compatibility.

**default:** N/A

**type:** dict | YAML/JSON string → dict


<a id="perf_gradual_write_preload_data"></a>

## **perf_gradual_write_preload_data** / SCT_PERF_GRADUAL_WRITE_PRELOAD_DATA

If true, preload data (via [`prepare_write_cmd`](stress-commands-and-load-generation.md#prepare_write_cmd)) before test_write_gradual_increase_load. Needed for LWT conditional-update workloads (e.g. UPDATE ... IF <cond>) that require existing rows to have a chance of applying; not needed for INSERT-based write workloads on a fresh table.

**default:** False

**type:** bool


<a id="perf_simple_query_extra_command"></a>

## **perf_simple_query_extra_command** / SCT_PERF_SIMPLE_QUERY_EXTRA_COMMAND

Extra command line options to pass to perf_simple_query

**default:** N/A

**type:** str (appendable)


<a id="perf_stress_keyspace"></a>

## **perf_stress_keyspace** / SCT_PERF_STRESS_KEYSPACE

Keyspace name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'keyspace' key in [`latte_schema_parameters`](stress-commands-and-load-generation.md#latte_schema_parameters).

**default:** N/A

**type:** str (appendable)


<a id="perf_stress_table"></a>

## **perf_stress_table** / SCT_PERF_STRESS_TABLE

Table name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'table' key in [`latte_schema_parameters`](stress-commands-and-load-generation.md#latte_schema_parameters).

**default:** N/A

**type:** str (appendable)


<a id="run_db_node_benchmarks"></a>

## **run_db_node_benchmarks** / SCT_RUN_DB_NODE_BENCHMARKS

Flag for running db node benchmarks before the tests

**default:** False

**type:** bool


<a id="stop_on_hw_perf_failure"></a>

## **stop_on_hw_perf_failure** / SCT_STOP_ON_HW_PERF_FAILURE

Stop sct performance test if hardware performance test failed<br><br>Hardware performance tests runs on each node with sysbench and cassandra-fio tools.<br>Results stored in ES. HW perf tests run during cluster setups and not affect<br>SCT Performance tests. Results calculated as average among all results for certain<br>instance type or among all nodes during single run.<br>if results for a single node is not in margin 0.01 of<br>average result for all nodes, hw test considered as Failed.<br>If [`stop_on_hw_perf_failure`](#stop_on_hw_perf_failure) is True, then sct performance test will be terminated<br>after hw perf tests detect node with hw results not in margin with average<br>If [`stop_on_hw_perf_failure`](#stop_on_hw_perf_failure) is False, then sct performance test will be run<br>even after hw perf tests detect node with hw results not in margin with average

**default:** False

**type:** bool


<a id="stress_process_step"></a>

## **stress_process_step** / SCT_STRESS_PROCESS_STEP

add/remove num of process on each round

**default:** N/A

**type:** int


<a id="stress_step_duration"></a>

## **stress_step_duration** / SCT_STRESS_STEP_DURATION

Duration of time for stress round

**default:** 15m

**type:** str (appendable)


<a id="stress_threads_start_num"></a>

## **stress_threads_start_num** / SCT_STRESS_THREADS_START_NUM

Number of threads for c-s command

**default:** N/A

**type:** int


<a id="use_hdrhistogram"></a>

## **use_hdrhistogram** / SCT_USE_HDRHISTOGRAM

Enable hdr histogram logging for cs

**default:** False

**type:** bool


<a id="workload_name"></a>

## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with [`use_hdrhistogram`](#use_hdrhistogram) set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** str (appendable)
