# HTML Template Reference

The page layout, based on a reviewed mixed-workload report. All versions and numbers here are synthetic. [example-report.html](example-report.html) is a complete page built from these pieces. Replace the `<...>` placeholders.

## Page Skeleton

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title><Workload or All Workloads> Version Comparison</title>
<style>
body { font-family: Arial, sans-serif; margin: 40px; background: #f9f9f9; color: #333; }
h1 { color: #1a1a2e; border-bottom: 2px solid #0f3460; padding-bottom: 10px; }
h2 { color: #0f3460; margin-top: 30px; }
h3 { color: #0f3460; margin-top: 20px; }
table { border-collapse: collapse; margin: 15px 0; background: #fff; }
th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; }
th { background: #0f3460; color: #fff; }
tr:nth-child(even) { background: #f2f2f2; }
td.num { text-align: right; font-family: monospace; }
.best { background: #d4edda; font-weight: bold; }
.worst { background: #f8d7da; }
.noise { color: #666; font-size: 0.9em; margin: 4px 0 10px; }
.summary { background: #e8f4fd; padding: 15px; border-left: 4px solid #0f3460; margin: 20px 0; }
code { background: #eee; padding: 2px 5px; border-radius: 3px; font-size: 0.9em; }
.chart-container { margin: 15px 0; max-width: 640px; min-width: 360px; }
canvas { max-width: 100%; }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
</head>
<body>

<h1>All Workloads: P99 Latency and Max Throughput with <v1> and <v2> Scylla versions</h1>
<p><strong>Test:</strong> Predefined Throughput Steps (tablets) on 3 × i8g.4xlarge</p>
<p><strong>Background:</strong> <why the comparison was made>
(<a href="https://scylladb.atlassian.net/browse/<KEY>"><KEY></a>).</p>

<!-- Test Configuration, Scylla Versions & Packages, per-workload sections -->

<script>
<!-- chart code -->
</script>

</body>
</html>
```

Pin the Chart.js version, as above.

## Test Configuration

```html
<h2>Test Configuration</h2>
<table>
<tr><th>Parameter</th><th>Value</th></tr>
<tr><td>Test Method</td><td><code>PerformanceRegressionPredefinedStepsTest.test_&lt;workload&gt;_gradual_increase_load</code></td></tr>
<tr><td>Backend</td><td>AWS</td></tr>
<tr><td>Region</td><td>us-west-2</td></tr>
<tr><td>Instance Type</td><td>i8g.4xlarge</td></tr>
<tr><td>Node Count</td><td>3</td></tr>
<tr><td>Loaders</td><td>4 × c7i.8xlarge</td></tr>
<tr><td>Replication</td><td>tablets</td></tr>
<tr><td>SCT Branch</td><td>origin/branch-perf-v17</td></tr>
<tr><td>Workloads</td><td>read, write, mixed, read_disk_only</td></tr>
</table>
```

## Scylla Versions & Packages

One row per run, grouped by workload, in the same version order as the metric tables:

```html
<h2>Scylla Versions &amp; Packages</h2>
<table>
<tr><th>Workload</th><th>Scylla Version</th><th>Run ID</th><th>Status</th><th>Start</th><th>AMI</th></tr>
<tr><td>read</td><td>2099.1.4-20990105.0a1b2c3d4e5f</td><td><a href="https://argus.scylladb.com/tests/scylla-cluster-tests/<run_id>"><run_id></a></td><td>passed</td><td>2099-01-06</td><td>ami-00000000000000001</td></tr>
</table>
```

## Workload Section

With several workloads, each gets an `<h2>`, and its metrics get `<h3>`. A metric is a table, a noise note and a chart. Versions are rows; steps (sorted numerically, unthrottled last) are columns. Highlight best and worst only beyond the noise threshold:

```html
<h2>Mixed Workload</h2>

<h3>P99 Latency (ms) – Read</h3>
<table>
<tr><th>Version</th><th>100000</th><th>200000</th><th>300000</th><th>400000</th><th>unthrottled</th></tr>
<tr><td>2099.1.4</td><td class="num">1.12</td><td class="num">1.39</td><td class="num">1.95</td><td class="num best">3.05</td><td class="num">160.20</td></tr>
<tr><td>2099.2.1</td><td class="num">1.15</td><td class="num">1.41</td><td class="num">1.99</td><td class="num worst">3.31</td><td class="num">165.70</td></tr>
</table>
<p class="noise">Noise threshold per step: 5% / 5% / 5% / 5% / 9%</p>
<div class="chart-container"><canvas id="chart_mixed_p99_read"></canvas></div>

<h3>Max Throughput (op/s) – Read + Write</h3>
<table>
<tr><th>Version</th><th>unthrottled</th></tr>
<tr><td>2099.1.4</td><td class="num">905,000</td></tr>
<tr><td>2099.2.1</td><td class="num">862,400</td></tr>
</table>
<p class="noise">Noise threshold: 8%</p>
<div class="chart-container"><canvas id="chart_mixed_tput"></canvas></div>

<h3>Summary</h3>
<div class="summary">
<ul>
<li><strong>Best P99 read latency:</strong> 2099.1.4 at 400K (3.05 vs 3.31 ms, +8.5%); other steps within noise</li>
<li><strong>Max throughput:</strong> no difference beyond noise (905,000 vs 862,400 op/s, −4.7%, threshold 8%)</li>
</ul>
</div>
```

## Chart Code

One call per metric, inside `whenChartReady`, so the charts are drawn only once Chart.js has loaded. P99 latency uses a logarithmic y-axis; throughput uses a linear one from zero. Each dataset has the same numbers as its table row:

```html
<script>
const colors = ['#7FB3D3', '#F4A582', '#92C5A9', '#C4A5D6', '#F7D08A', '#B0B0B0'];

function barChart(id, title, labels, datasets, logScale) {
  // On a log axis bars start at the axis minimum: start it below the smallest value so every bar shows.
  const minValue = Math.min(...datasets.flatMap(d => d.data));
  new Chart(document.getElementById(id), {
    type: 'bar',
    data: { labels: labels, datasets: datasets },
    options: {
      responsive: true,
      plugins: { title: { display: true, text: title, font: { size: 14 } } },
      scales: { y: logScale ? { type: 'logarithmic', min: minValue / 2 } : { beginAtZero: true } }
    }
  });
}

// Draw once Chart.js is available: HTML previewers may run this before the CDN script has loaded.
function whenChartReady(draw) { if (window.Chart) { draw(); } else { setTimeout(() => whenChartReady(draw), 50); } }
whenChartReady(() => {
barChart('chart_mixed_p99_read', 'Mixed – P99 Read Latency (ms, log scale)',
  ['100000', '200000', '300000', '400000', 'unthrottled'],
  [
    { label: '2099.1.4', data: [1.12, 1.39, 1.95, 3.05, 160.20], backgroundColor: colors[0] },
    { label: '2099.2.1', data: [1.15, 1.41, 1.99, 3.31, 165.70], backgroundColor: colors[1] }
  ],
  true
);
});
</script>
```

## Microbenchmark Variant

The same blocks, with the variants as columns:

```html
<h2>Median TPS</h2>
<table>
<tr><th>Version</th><th>x86_64</th><th>arm64</th></tr>
<tr><td>2099.1.4</td><td class="num">200,000</td><td class="num">140,000</td></tr>
<tr><td>2099.2.1</td><td class="num">215,000</td><td class="num">139,200</td></tr>
</table>
<p class="noise">Noise threshold: 10% (x86_64) / 1% (arm64)</p>
<div class="chart-container"><canvas id="chart_median_tps"></canvas></div>
```

The Test Configuration lists the instance type per arch (e.g. `c7i.large (x86_64), c7g.large (arm64)`), and the Workloads row gives the benchmark parameters from the result table's `description`.
