# Performance regression tests

[← All configuration options](configuration_options.md)

Throughput/latency measurement runs, including gradual-throughput steps and HDR histogram
settings.

**17 options.** Jump to: [max_deviation](#max_deviation) · [n_stress_process](#n_stress_process) · [num_loaders_step](#num_loaders_step) · [num_threads_step](#num_threads_step) · [perf_gradual_step_duration](#perf_gradual_step_duration) · [perf_gradual_threads](#perf_gradual_threads) · [perf_gradual_throttle_steps](#perf_gradual_throttle_steps) · [perf_simple_query_extra_command](#perf_simple_query_extra_command) · [perf_stress_keyspace](#perf_stress_keyspace) · [perf_stress_table](#perf_stress_table) · [run_db_node_benchmarks](#run_db_node_benchmarks) · [stop_on_hw_perf_failure](#stop_on_hw_perf_failure) · [stress_process_step](#stress_process_step) · [stress_step_duration](#stress_step_duration) · [stress_threads_start_num](#stress_threads_start_num) · [use_hdrhistogram](#use_hdrhistogram) · [workload_name](#workload_name)


## **max_deviation** / SCT_MAX_DEVIATION

Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one

**default:** N/A

**type:** float


## **n_stress_process** / SCT_N_STRESS_PROCESS

Number of stress processes per loader

**default:** N/A

**type:** int


## **num_loaders_step** / SCT_NUM_LOADERS_STEP

Number of loaders which should be added per step

**default:** N/A

**type:** int


## **num_threads_step** / SCT_NUM_THREADS_STEP

Number of threads which should be added on per step

**default:** N/A

**type:** int


## **perf_gradual_step_duration** / SCT_PERF_GRADUAL_STEP_DURATION

Step duration of c-s load for gradual performance test per sub-test. Example: {'read': '30m', 'write': None, 'mixed': '30m'}

**default:** N/A

**type:** dict | str


## **perf_gradual_threads** / SCT_PERF_GRADUAL_THREADS

Threads amount of stress load for gradual performance test per sub-test. Example: {'read': 100, 'write': [200, 300], 'mixed': 300}

**default:** N/A

**type:** dict | str


## **perf_gradual_throttle_steps** / SCT_PERF_GRADUAL_THROTTLE_STEPS

Used for gradual performance test. Define throttle for load step in ops. Example: {'read': ['100000', '150000'], 'mixed': ['300']}

**default:** N/A

**type:** dict | str


## **perf_simple_query_extra_command** / SCT_PERF_SIMPLE_QUERY_EXTRA_COMMAND

Extra command line options to pass to perf_simple_query

**default:** N/A

**type:** str
* appendable


## **perf_stress_keyspace** / SCT_PERF_STRESS_KEYSPACE

Keyspace name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'keyspace' key in latte_schema_parameters.

**default:** N/A

**type:** str
* appendable


## **perf_stress_table** / SCT_PERF_STRESS_TABLE

Table name used in performance gradual throughput tests.<br>Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).<br>For latte, if not set, falls back to the 'table' key in latte_schema_parameters.

**default:** N/A

**type:** str
* appendable


## **run_db_node_benchmarks** / SCT_RUN_DB_NODE_BENCHMARKS

Flag for running db node benchmarks before the tests

**default:** N/A

**type:** bool


## **stop_on_hw_perf_failure** / SCT_STOP_ON_HW_PERF_FAILURE

Stop sct performance test if hardware performance test failed<br><br>Hardware performance tests runs on each node with sysbench and cassandra-fio tools.<br>Results stored in ES. HW perf tests run during cluster setups and not affect<br>SCT Performance tests. Results calculated as average among all results for certain<br>instance type or among all nodes during single run.<br>if results for a single node is not in margin 0.01 of<br>average result for all nodes, hw test considered as Failed.<br>If stop_on_hw_perf_failure is True, then sct performance test will be terminated<br>after hw perf tests detect node with hw results not in margin with average<br>If stop_on_hw_perf_failure is False, then sct performance test will be run<br>even after hw perf tests detect node with hw results not in margin with average

**default:** N/A

**type:** bool


## **stress_process_step** / SCT_STRESS_PROCESS_STEP

add/remove num of process on each round

**default:** N/A

**type:** int


## **stress_step_duration** / SCT_STRESS_STEP_DURATION

Duration of time for stress round

**default:** 15m

**type:** str
* appendable


## **stress_threads_start_num** / SCT_STRESS_THREADS_START_NUM

Number of threads for c-s command

**default:** N/A

**type:** int


## **use_hdrhistogram** / SCT_USE_HDRHISTOGRAM

Enable hdr histogram logging for cs

**default:** N/A

**type:** bool


## **workload_name** / SCT_WORKLOAD_NAME

Workload name, can be: write|read|mixed|unset. Used for e.g. latency_calculator_decorator (use with [`use_hdrhistogram`](#use_hdrhistogram) set to true). If unset, workload is taken from test name.

**default:** N/A

**type:** str
* appendable
