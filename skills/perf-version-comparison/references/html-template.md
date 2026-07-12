# HTML Template Reference

Complete CSS and HTML structure for performance version comparison reports.

## CSS Styles

```css
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  color: #172b4d;
  max-width: 1200px;
  margin: 0 auto;
  padding: 24px;
  line-height: 1.5;
}
h1 { font-size: 24px; font-weight: 600; }
h2 { font-size: 20px; font-weight: 600; margin-top: 32px; border-bottom: 1px solid #dfe1e6; padding-bottom: 6px; }
h3 { font-size: 16px; font-weight: 600; margin-top: 24px; }
table { border-collapse: collapse; width: 100%; margin: 12px 0 20px 0; font-size: 13px; }
th, td { border: 1px solid #c1c7d0; padding: 6px 8px; text-align: left; }
th { background: #f4f5f7; font-weight: 600; }
tr.alt td { background: #deebff; }
code { font-family: monospace; font-size: 12px; background: #f4f5f7; padding: 1px 4px; border-radius: 3px; }
a { color: #0052cc; text-decoration: none; }
a:hover { text-decoration: underline; }
ul, ol { padding-left: 22px; }
li { margin: 8px 0; }
hr { border: none; border-top: 1px solid #dfe1e6; margin: 20px 0; }
.green { color: #006644; font-weight: 600; }
.red { color: #de350b; font-weight: 700; }
.orange { color: #ff991f; font-weight: 700; }
```

## Page Structure

### Section Header

```html
<h2>Throughput &amp; Latency - Write workload</h2>
<h3>ScyllaDB Enterprise: 2026.1.5 vs 2026.2.0 vs 2026.3.0.dev</h3>
<p><strong>Test:</strong> <code>test_write_gradual_increase_load</code> (Predefined Throughput Steps)<br>
<strong>Infrastructure:</strong> 3x i8g.4xlarge nodes (AWS), 4x c7i.8xlarge loaders<br>
<strong>Date:</strong> 2026-06-08</p>
<hr>
```

### Summary Results Table

```html
<h3>Summary Results</h3>
<table>
<tr>
  <th>Load Step</th><th>Metric</th>
  <th>2026.1.5</th><th>2026.2.0</th><th>2026.3.0.dev</th>
  <th>Delta (2.0 vs 1.5)</th><th>Delta (3.0.dev vs 1.5)</th><th>Status</th>
</tr>
<tr class="alt">
  <td><strong>350,000 op/s</strong></td><td>P99 write Latency</td>
  <td>2.21 ms</td><td>2.21 ms</td><td>2.33 ms</td>
  <td>+0.0%</td><td>+5.4%</td><td>OK</td>
</tr>
<tr>
  <td><strong>600,000 op/s</strong></td><td>P99 write Latency</td>
  <td>3.22 ms</td><td>3.12 ms</td><td>9.08 ms</td>
  <td>-3.1%</td><td><span class="red">+182.0%</span></td>
  <td><span class="red">REGRESSION</span></td>
</tr>
<tr class="alt">
  <td><strong>Unthrottled (max)</strong></td><td>P99 write Latency</td>
  <td>8.02 ms</td><td>4.38 ms</td><td>5.69 ms</td>
  <td>-45.4%</td><td>-29.1%</td><td>OK</td>
</tr>
<tr class="alt">
  <td></td><td><strong>Max Throughput write</strong></td>
  <td><strong>732,795 op/s</strong></td><td><strong>759,560 op/s</strong></td><td><strong>690,847 op/s</strong></td>
  <td><strong>+3.7%</strong></td><td><strong>-5.7%</strong></td>
  <td><span class="orange">WARNING</span></td>
</tr>
</table>
```

### Key Findings

```html
<hr>
<h3>Key Findings</h3>
<ol>
<li><p><strong>All three versions PASSED the test.</strong></p></li>
<li><p><strong>Max throughput regression detected:</strong> 2026.3.0.dev dropped to 690,847 op/s (-5.7%),
while 2026.2.0 actually improved to 759,560 op/s (+3.7%). The divergence suggests a specific
change in 2026.3.0.dev affecting write saturation capacity.</p></li>
<li><p><strong>Lower load steps (350K) show stable latency</strong> across all versions --
deltas are within acceptable range, confirming that steady-state performance is not
fundamentally degraded.</p></li>
<li><p><strong>Reactor stalls observed:</strong> 2026.1.5 had 0, 2026.2.0 had 0, 2026.3.0.dev had 3.
Increased stalls in 2026.3.0.dev may correlate with latency spikes at 600K.</p></li>
</ol>
```

### Test Outcome Table

```html
<hr>
<h3>Test Outcome</h3>
<table>
<tr><th>Version</th><th>Result</th><th>Issues</th></tr>
<tr><td>2026.1.5</td><td><strong>PASSED</strong></td><td></td></tr>
<tr><td>2026.2.0</td><td><strong>PASSED</strong></td><td></td></tr>
<tr><td>2026.3.0.dev</td><td><strong>PASSED</strong></td><td></td></tr>
</table>
```

### Conclusion Paragraph

```html
<hr>
<p>2026.2.0 delivers the best unthrottled write throughput (+3.7% vs baseline) with the lowest P99.
2026.3.0.dev shows a moderate P99 regression at 600K op/s that approaches the WARNING threshold,
suggesting a scheduling or compaction behavior change under sustained mid-range write load.
Worth monitoring in subsequent builds.</p>
```

### Links Section

```html
<p><em>Links:</em></p>
<ul>
<li>2026.1.5: <a href="https://argus.scylladb.com/tests/scylla-cluster-tests/UUID">https://argus.scylladb.com/tests/scylla-cluster-tests/UUID</a></li>
<li>2026.2.0: <a href="https://argus.scylladb.com/tests/scylla-cluster-tests/UUID">https://argus.scylladb.com/tests/scylla-cluster-tests/UUID</a></li>
<li>2026.3.0.dev: <a href="https://argus.scylladb.com/tests/scylla-cluster-tests/UUID">https://argus.scylladb.com/tests/scylla-cluster-tests/UUID</a></li>
</ul>
```

## Status Label Rules

| Condition | HTML | Visual |
|-----------|------|--------|
| Latency increase > 15% | `<span class="red">REGRESSION</span>` | Red bold |
| Latency increase > 10% | `<span class="orange">WARNING</span>` | Orange bold |
| Throughput decrease > 10% | `<span class="red">REGRESSION</span>` | Red bold |
| Throughput decrease > 5% | `<span class="orange">WARNING</span>` | Orange bold |
| Otherwise | `OK` | Plain text |
