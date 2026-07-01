# HTML Template Reference

Complete CSS and HTML structure for Confluence-compatible comparison pages.

## CSS Styles

```css
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, sans-serif;
  color: #172b4d;
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px;
  line-height: 1.5;
}
h1 { font-size: 24px; font-weight: 600; border-bottom: 1px solid #dfe1e6; padding-bottom: 10px; }
h2 { font-size: 20px; font-weight: 600; margin-top: 32px; border-bottom: 1px solid #dfe1e6; padding-bottom: 6px; }
h3 { font-size: 16px; font-weight: 600; margin-top: 24px; }
table {
  border-collapse: collapse;
  width: 100%;
  margin: 12px 0 20px 0;
  font-size: 14px;
}
th, td {
  border: 1px solid #c1c7d0;
  padding: 7px 10px;
  text-align: left;
}
th {
  background: #f4f5f7;
  font-weight: 600;
}
td:first-child { font-weight: 500; }
code {
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 12px;
  background: #f4f5f7;
  padding: 1px 4px;
  border-radius: 3px;
}
/* Status badges */
.status-passed {
  display: inline-block;
  background: #00875a;
  color: #fff;
  font-size: 11px;
  font-weight: 700;
  padding: 2px 8px;
  border-radius: 3px;
  text-transform: uppercase;
}
.status-failed {
  display: inline-block;
  background: #de350b;
  color: #fff;
  font-size: 11px;
  font-weight: 700;
  padding: 2px 8px;
  border-radius: 3px;
  text-transform: uppercase;
}
/* Callout boxes */
.info-box {
  background: #deebff;
  border-left: 4px solid #0052cc;
  padding: 12px 16px;
  margin: 16px 0;
  border-radius: 3px;
}
.note-box {
  background: #fffae6;
  border-left: 4px solid #ff991f;
  padding: 12px 16px;
  margin: 16px 0;
  border-radius: 3px;
}
.warning-box {
  background: #ffebe6;
  border-left: 4px solid #de350b;
  padding: 12px 16px;
  margin: 16px 0;
  border-radius: 3px;
}
/* Cell highlighting */
.red { color: #de350b; font-weight: 700; }
.green { color: #00875a; font-weight: 700; }
.best { background: #e3fcef; }
.worst { background: #ffebe6; }
a { color: #0052cc; text-decoration: none; }
a:hover { text-decoration: underline; }
ul { padding-left: 22px; }
li { margin: 4px 0; }
```

## Page Structure

### 1. Overall Results (before Overview)

A short info-box immediately after the `<h1>` title, before the Overview section. Gives the reader the key takeaway in 3 seconds without scrolling.

```html
<h1>Report Title</h1>

<div class="info-box">
  <strong>Result:</strong> summary of pass/fail status with
  <span class="status-passed">PASSED</span> and/or
  <span class="status-failed">FAILED</span> badges.<br><br>
  <ul>
    <li><strong>Run label A</strong> &mdash; one-line verdict</li>
    <li><strong>Run label B</strong> &mdash; one-line verdict</li>
  </ul>
  <strong>Conclusion:</strong> one-sentence recommendation.
</div>
```

Guidelines:
- List each run with its key outcome (pass/fail, critical metric value)
- Highlight the winning run or the recommended option
- Keep to 5-8 lines max; detailed analysis belongs in the phase sections

### 2. Overview with Comparison Callout

```html
<h2>Overview</h2>

<div class="info-box">
  <strong>What is compared:</strong> the parameter that varies.<br>
  Run 1 &rarr; <strong>value A</strong> &nbsp;|&nbsp;
  Run 2 &rarr; <strong>value B</strong> &nbsp;|&nbsp;
  Run 3 &rarr; <strong>value C</strong>
</div>

<p>Brief description of what is constant across runs.</p>

<table>
  <tr><th>Parameter</th><th>Value</th></tr>
  <tr><td>Base cluster</td><td>...</td></tr>
  <!-- more rows -->
</table>
```

### 3. Test Runs Table

```html
<h2>Test Runs</h2>
<table>
  <tr>
    <th>Run</th><th>Variable Parameter</th><th>Test ID</th>
    <th>Date</th><th>Status</th><th>Run</th>
  </tr>
  <tr>
    <td>Run 1</td>
    <td><strong>value</strong></td>
    <td><code>test-id-uuid</code></td>
    <td>YYYY-MM-DD</td>
    <td><span class="status-failed">FAILED</span></td>
    <td><a href="https://argus.scylladb.com/tests/scylla-cluster-tests/{test_id}">Run&nbsp;#N</a></td>
  </tr>
</table>
```

### 4. Phase Results Table

Column headers are links to Argus runs. Cells with P99 > 10 ms get `class="worst"` and `<span class="red">`.

```html
<h3>Phase N: Phase Name (duration, workload type)</h3>
<p>Description of what happens during this phase.</p>

<table>
  <tr>
    <th>Metric</th>
    <th><a href="https://argus.scylladb.com/tests/scylla-cluster-tests/{id1}">Run 1 label</a></th>
    <th><a href="https://argus.scylladb.com/tests/scylla-cluster-tests/{id2}">Run 2 label</a></th>
    <th><a href="https://argus.scylladb.com/tests/scylla-cluster-tests/{id3}">Run 3 label</a></th>
  </tr>
  <tr><td>P99 write (ms)</td><td>2.05</td><td>2.13</td><td class="best"><strong>2.11</strong></td></tr>
  <tr><td>P99 read (ms)</td><td>4.70</td><td class="worst"><span class="red">15.3</span></td><td class="best"><strong>3.99</strong></td></tr>
  <tr><td>Throughput write (ops/s)</td><td>65,974</td><td>65,972</td><td>65,994</td></tr>
  <tr><td>Throughput read (ops/s)</td><td>65,977</td><td>65,963</td><td>66,007</td></tr>
  <tr><td><strong>Duration</strong></td><td><strong>45:07</strong></td><td class="best"><strong>33:37</strong></td><td><strong>43:31</strong></td></tr>
</table>
```

### 5. Color Coding Rules

| Condition | CSS class | Visual |
|-----------|-----------|--------|
| P99 > 10 ms (failure) | `class="worst"` + `<span class="red">value</span>` | Red text, pink background |
| Best value in row | `class="best"` + `<strong>value</strong>` | Green background, bold |
| Normal value | no class | Plain text |

### 6. Callout Boxes

Use for key observations after each phase table:

```html
<!-- Informational (blue) - key observations -->
<div class="info-box">
  <strong>Key observations:</strong>
  <ul><li>...</li></ul>
</div>

<!-- Warning (red) - critical failures -->
<div class="warning-box">
  <strong>Critical finding: ...</strong>
  <p>Explanation.</p>
</div>

<!-- Note (yellow) - noteworthy but not critical -->
<div class="note-box">
  <strong>Note:</strong> ...
</div>
```
