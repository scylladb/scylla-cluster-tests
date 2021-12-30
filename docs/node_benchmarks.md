# Node Benchmarks for Scylla

Whether you're running performance jobs or investigating an issue, you might
want to now if the hardware Scylla is running on meets your baseline performance
expectations. That's what node benchmarks are for.

### Which benchmarks do we run?

We run:
* sysbench - for CPU benchmarking. Github: https://github.com/akopytov/sysbench
* cassandra-fio - for disk IO benchmarking. Github: https://github.com/KnifeyMoloko/cassandra-fio

### How to run the benchmarks?

Simply add the line `run_db_node_benchmarks: true` in your test case yaml.

### At which point in time of a test run are these benchmarks performed?

Both benchmarks are performed at the end of the "adding nodes" step.
Nodes added after the cluster initialization are not included.

### How are the benchmark results collected and stored?

Benchmarks are performed locally on each node and collected at the end by a
manager class. They are collected in JSON format and uploaded to an
ElasticSearch index `node_benchmarks`.

### How are the benchmark results structured?

Since the benchmarks are meant to compare relative performance of db node
instances, each ES doc represents benchmark results for one node.

Each ElasticSearch doc has the following top-level fields (apart from the regular metadata fields like `_id`):
* node_instance_type - e.g. 'i3.4xlarge'
* test_id - same as SCT `test_id`, e.g. '4e96c762-e16a-49fe-916e-b566ebb9ec3f'
* sysbench_* - fields with sysbench metrics
* cassandra_fio_<name_of_fio_job>_* - metrics for `fio` jobs, e.g. `lcs_64k_write`
* cassandra_fio_global options - global options for the `cassandra-fio`, e.g. `fio-version` or `filesize`
* cassandra_fio_* - other `cassandra-fio` data, e.g. `time` or `timestamp`


#### Sysbench results
Example:

```json
{
  "sysbench_events_per_second": 7317.29,
  "sysbench_latency_min": 1.09,
  "sysbench_latency_max": 1.61,
  "sysbench_latency_avg": 1.09,
  "sysbench_latency_p95": 1.1,
  "sysbench_thread_fairness_avg": 109760.375,
  "sysbench_thread_fairness_stdev": 100.32,
  "sysbench_thread_fairness_time_avg": 119.9706,
  "sysbench_thread_fairness_time_stdev": 0
}
```

#### Cassandra-fio results

Cassandra-fio results have the following keys:

* `cassandra_fio_fio version` - example: 'fio-.3.16'
* `cassandra_fio_timestamp_ms` - example: 1640785143919
* `cassandra_fio_timestamp` - example: 1640785143
* `cassandra_fio_time` - example: 'Wed Dec 29 13:39:03 2021'
* `cassandra_fio_global options` - example:
```json
{
  'fadvise_hint': '0',
  'rw': 'readwrite',
  'direct': '1',
  'invalidate': '1',
  'thread': '1',
  'filesize': '160m',
  'filename_format': 'cassandra.$filenum',
  'directory': './data',
  'fallocate': 'none',
  'file_service_type': 'sequential',
  'ioengine': 'libaio',
  'bs': '64k',
  'iodepth': '8',
  'nrfiles': '10',
  'time_based': '1',
  'randrepeat': '1'
}
```
* `cassandra_fio_disk_util` - example:
```json
[
  {
    'write_merges': 29,
    'in_queue': 626036,
    'write_ticks': 487533,
    'util': 99.864867,
    'name': 'xvda',
    'read_ticks': 477113,
    'read_ios': 64390,
    'read_merges': 0,
    'write_ios': 60874
  }
]
```
###### Fio jobs:
* `cassandra_fio_lcs_64k_write` - the lcs64k job includes the following jobs: 'lcs_setup', 'lcs_64k_write',
'lcs_64k_read'. We do not upload the `lcs_setup` job metrics, we only use write and read metrics. Each job is
described with the same set of metrics. For example, the 'lcs_64_write' job metrics look like this:
```json
{
  'error': 0,
  'latency_ns': {
    '100': 0.0,
    '2': 0.0,
    '750': 0.0,
    '4': 0.0,
    '500': 0.0,
    '1000': 0.0,
    '50': 0.0,
    '250': 0.0,
    '20': 0.0,
    '10': 0.0
  },
  'latency_depth': 8,
  'latency_window': 0,
  'elapsed': 61,
  'eta': 0,
  'job options': {
    'write_bw_log': 'lcs.64k.write',
    'write_iops_log': 'lcs.64k.write',
    'rw': 'write',
    'openfiles': '1',
    'name': 'lcs_64k_write',
    'runtime': '60s',
    'write_lat_log': 'lcs.64k.write'
  },
  'trim': {
    'io_kbytes': 0,
    'drop_ios': 0,
    'iops_max': 0,
    'iops_mean': 0.0,
    'bw_agg': 0.0,
    'runtime': 0,
    'short_ios': 0,
    'clat_ns': {
      'min': 0,
      'percentile': {
        '90.000000': 0,
        '99.500000': 0,
        '80.000000': 0,
        '1.000000': 0,
        '20.000000': 0,
        '99.950000': 0,
        '30.000000': 0,
        '95.000000': 0,
        '99.000000': 0,
        '99.990000': 0,
        '40.000000': 0,
        '50.000000': 0,
        '99.900000': 0,
        '5.000000': 0,
        '70.000000': 0,
        '10.000000': 0,
        '60.000000': 0
      },
      'max': 0,
      'mean': 0.0,
      'stddev': 0.0
    },
    'total_ios': 0,
    'bw_max': 0,
    'bw_mean': 0.0,
    'iops_samples': 0,
    'bw_bytes': 0,
    'iops_min': 0,
    'iops_stddev': 0.0,
    'bw': 0,
    'iops': 0.0,
    'io_bytes': 0,
    'bw_min': 0,
    'bw_samples': 0,
    'slat_ns': {
      'min': 0,
      'max': 0,
      'mean': 0.0,
      'stddev': 0.0
    },
    'lat_ns': {
      'min': 0,
      'max': 0,
      'mean': 0.0,
      'stddev': 0.0
    },
    'bw_dev': 0.0
  },
  'usr_cpu': 0.0,
  'latency_us': {
    '100': 0.0,
    '2': 0.0,
    '750': 0.0,
    '4': 0.0,
    '500': 0.0,
    '1000': 0.0,
    '50': 0.0,
    '250': 0.0,
    '20': 0.0,
    '10': 0.0
  },
  'write': {
    'io_kbytes': 3838208,
    'drop_ios': 0,
    'iops_max': 1,
    'iops_mean': 1.0,
    'bw_agg': 12.954007,
    'runtime': 60008,
    'short_ios': 0,
    'clat_ns': {
      'min': 1085021,
      'percentile': {
        '90.000000': 8290304,
        '99.500000': 11862016,
        '80.000000': 8159232,
        '1.000000': 7110656,
        '20.000000': 7634944,
        '99.950000': 12910592,
        '30.000000': 7700480,
        '95.000000': 8585216,
        '99.000000': 11337728,
        '99.990000': 13434880,
        '40.000000': 7831552,
        '50.000000': 7962624,
        '99.900000': 12386304,
        '5.000000': 7438336,
        '70.000000': 8093696,
        '10.000000': 7569408,
        '60.000000': 8028160
      },
      'max': 14403867,
      'mean': 7970888.317231,
      'stddev': 607156.487881
    },
    'total_ios': 59972,
    'bw_max': 60400,
    'bw_mean': 8285.512256,
    'iops_samples': 59972,
    'bw_bytes': 65496683,
    'iops_min': 1,
    'iops_stddev': 0.0,
    'bw': 63961,
    'iops': 999.40008,
    'io_bytes': 3930324992,
    'bw_min': 4549,
    'bw_samples': 59972,
    'slat_ns': {
      'min': 8695,
      'max': 5810551,
      'mean': 28954.745781,
      'stddev': 28415.189436
    },
    'lat_ns': {
      'min': 1121824,
      'max': 14429349,
      'mean': 8000209.100514,
      'stddev': 606476.899328
    },
    'bw_dev': 1190.721429
  },
  'jobname': 'lcs_64k_write',
  'sys_cpu': 5.4277,
  'latency_target': 0,
  'read': {
    'io_kbytes': 0,
    'drop_ios': 0,
    'iops_max': 0,
    'iops_mean': 0.0,
    'bw_agg': 0.0,
    'runtime': 0,
    'short_ios': 0,
    'clat_ns': {
      'min': 0,
      'percentile': {
        '90.000000': 0,
        '99.500000': 0,
        '80.000000': 0,
        '1.000000': 0,
        '20.000000': 0,
        '99.950000': 0,
        '30.000000': 0,
        '95.000000': 0,
        '99.000000': 0,
        '99.990000': 0,
        '40.000000': 0,
        '50.000000': 0,
        '99.900000': 0,
        '5.000000': 0,
        '70.000000': 0,
        '10.000000': 0,
        '60.000000': 0
      },
      'max': 0,
      'mean': 0.0,
      'stddev': 0.0
    },
    'total_ios': 0,
    'bw_max': 0,
    'bw_mean': 0.0,
    'iops_samples': 0,
    'bw_bytes': 0,
    'iops_min': 0,
    'iops_stddev': 0.0,
    'bw': 0,
    'iops': 0.0,
    'io_bytes': 0,
    'bw_min': 0,
    'bw_samples': 0,
    'slat_ns': {
      'min': 0,
      'max': 0,
      'mean': 0.0,
      'stddev': 0.0
    },
    'lat_ns': {
      'min': 0,
      'max': 0,
      'mean': 0.0,
      'stddev': 0.0
    },
    'bw_dev': 0.0
  },
  'majf': 0,
  'ctx': 55725,
  'groupid': 1,
  'minf': 2346,
  'job_runtime': 60007,
  'iodepth_submit': {
    '0': 0.0,
    '>=64': 0.0,
    '4': 100.0,
    '16': 0.0,
    '8': 0.0,
    '64': 0.0,
    '32': 0.0
  },
  'sync': {
    'lat_ns': {
      'min': 0,
      'percentile': {
        '90.000000': 0,
        '99.500000': 0,
        '80.000000': 0,
        '1.000000': 0,
        '20.000000': 0,
        '99.950000': 0,
        '30.000000': 0,
        '95.000000': 0,
        '99.000000': 0,
        '99.990000': 0,
        '40.000000': 0,
        '50.000000': 0,
        '99.900000': 0,
        '5.000000': 0,
        '70.000000': 0,
        '10.000000': 0,
        '60.000000': 0
      },
      'max': 0,
      'mean': 0.0,
      'stddev': 0.0
    },
    'total_ios': 0
  },
  'latency_ms': {
    '100': 0.0,
    '2': 0.100047,
    '750': 0.0,
    '>=2000': 0.0,
    '4': 0.038351,
    '500': 0.0,
    '1000': 0.0,
    '2000': 0.0,
    '50': 0.0,
    '250': 0.0,
    '20': 1.403989,
    '10': 98.457614
  },
  'iodepth_level': {
    '>=64': 0.0,
    '1': 0.1,
    '2': 0.1,
    '4': 0.1,
    '16': 0.0,
    '8': 99.929967,
    '32': 0.0
  },
  'latency_percentile': 100.0,
  'iodepth_complete': {
    '0': 0.0,
    '>=64': 0.0,
    '4': 99.998332,
    '16': 0.0,
    '8': 0.1,
    '64': 0.0,
    '32': 0.0
  }
}
```

Usually we are most interested with the bandwidth and iops metrics for a given
job, so for the 'lcs_64_write' job, we're interested with the contents of it's
'write' key. (Details about specific fields and how to read them can be found
in the fio documentation below.)


Resources:
* fio docs: https://fio.readthedocs.io/en/latest/
* cassandra-fio repo: https://github.com/KnifeyMoloko/cassandra-fio
* sysbench repo: https://github.com/akopytov/sysbench
