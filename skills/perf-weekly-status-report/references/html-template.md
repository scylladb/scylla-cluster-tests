# Gmail-Compatible HTML Template Patterns

## Why Gmail Restrictions Matter

Gmail strips all `<style>` tags, `<link>` stylesheets, and `class` attributes when rendering HTML emails. It also does NOT support:
- `linear-gradient`
- `border-radius`
- `opacity`
- `max-width` on `<body>`
- `<div>` elements for layout (unreliable)
- `<h1>`-`<h6>` tags (inconsistent rendering)
- `<ul>`/`<li>` (inconsistent indentation)

Only inline `style` attributes on supported elements survive. Use `<table>` for ALL layout. Always include `bgcolor` attribute as fallback alongside `background-color` in style.

## Base Document Structure

```html
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Performance Weekly Status Report</title>
</head>
<body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333333;margin:0;padding:0;">

<!-- Outer wrapper table for centering -->
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#ffffff;">
<tr>
<td align="center" style="padding:20px 10px;">

<!-- Main content table (fixed width 700) -->
<table width="700" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333333;">

  <!-- Content rows go here -->

</table>

</td>
</tr>
</table>

</body>
</html>
```

## Header Section

```html
<tr>
<td bgcolor="#1a237e" style="background-color:#1a237e;padding:20px 25px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0">
  <tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:22px;font-weight:bold;color:#ffffff;">ScyllaDB Enterprise - Performance Weekly Status</td></tr>
  <tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#cccccc;padding-top:5px;">Period: YYYY-MM-DD to YYYY-MM-DD | Master (~dev) builds only</td></tr>
  </table>
</td>
</tr>
```

## Spacer Row

Gmail ignores margin/padding between table rows. Use explicit spacer rows:

```html
<tr><td height="15" style="font-size:1px;line-height:1px;">&nbsp;</td></tr>
```

## Summary Box

Counts are **per run** (each workload = 1 run), not per test group. A test with 4 workloads counts as 4 runs. This ensures Total = Passed + Failed/Error always holds. Example: 1 throughput test (4 workloads) + 4 microbenchmarks = 8 total runs.

```html
<tr>
<td bgcolor="#f8f9fa" style="background-color:#f8f9fa;border:1px solid #dee2e6;padding:15px;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#333333;">
  <tr><td colspan="4" style="font-size:16px;font-weight:bold;padding-bottom:10px;">Summary</td></tr>
  <tr>
    <td style="padding:3px 15px 3px 0;"><strong>Total Tests:</strong></td>
    <td style="padding:3px 15px 3px 0;">{total_runs}</td>
    <td style="padding:3px 15px 3px 0;"><strong>Passed:</strong></td>
    <td style="color:#28a745;font-weight:bold;">{passed_runs}</td>
  </tr>
  <tr>
    <td style="padding:3px 15px 3px 0;"><strong>Failed/Error:</strong></td>
    <td style="color:#dc3545;font-weight:bold;padding:3px 15px 3px 0;">{failed_runs}</td>
    <td colspan="2"></td>
  </tr>
  </table>
</td>
</tr>
```

## Section Headings (as table cells, not h-tags)

```html
<!-- Main section heading -->
<tr>
<td style="font-family:Arial,Helvetica,sans-serif;font-size:16px;font-weight:bold;color:#333333;padding-bottom:10px;">Test Overview</td>
</tr>

<!-- Category heading with underline -->
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:15px;font-weight:bold;color:#333333;padding:15px 0 5px 0;border-bottom:2px solid #007bff;">i8g Tablets (New Platform)</td></tr>
</table>

<!-- Sub-heading (test name with full version, NO status badge) -->
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:13px;font-weight:bold;color:#333333;padding:10px 0 5px 0;">predefined-throughput-steps-i8g-tablets (2026.3.0.dev.20260612.91ada5517d59)</td></tr>
</table>
```

## Status Badges

```html
<!-- Passed -->
<span style="background-color:#28a745;color:#ffffff;padding:2px 8px;font-size:11px;font-weight:bold;display:inline-block;">PASSED</span>

<!-- Failed -->
<span style="background-color:#dc3545;color:#ffffff;padding:2px 8px;font-size:11px;font-weight:bold;display:inline-block;">FAILED</span>

<!-- Error -->
<span style="background-color:#fd7e14;color:#ffffff;padding:2px 8px;font-size:11px;font-weight:bold;display:inline-block;">ERROR</span>

<!-- No Runs -->
<span style="background-color:#6c757d;color:#ffffff;padding:2px 8px;font-size:11px;font-weight:bold;display:inline-block;">NO_RUNS</span>
```

## Data Tables

### Overview Table (dark header, grouped by category/test/workload, no version column)

```html
<table width="100%" cellpadding="0" cellspacing="0" border="1" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;border-color:#dee2e6;">
<tr bgcolor="#343a40" style="background-color:#343a40;">
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Category</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Test</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:center;color:#ffffff;font-weight:bold;">Workload</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:center;color:#ffffff;font-weight:bold;">Runs</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:center;color:#ffffff;font-weight:bold;">Status</th>
</tr>
<!-- Category shown only on first row of group; Test shown only on first workload row -->
<tr bgcolor="#ffffff" style="background-color:#ffffff;">
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;font-family:Arial,Helvetica,sans-serif;font-size:12px;">i8g Tablets</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">predefined-throughput-steps-i8g-tablets</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;">mixed</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;">4</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><span style="color:#28a745;">passed 3</span> / <span style="color:#dc3545;">failed 1</span></td>
</tr>
<!-- Subsequent workload row: category and test cells are empty -->
<tr bgcolor="#f8f9fa" style="background-color:#f8f9fa;">
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;"></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;"></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;">read</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;">3</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><span style="color:#28a745;">passed 3</span></td>
</tr>
</table>
```

NOTE: No Argus links or version column in the overview table. Version is in the Summary section. Runs column = just the count. Status column = textual breakdown with colored spans.

### Results Table (light header) -- Per-Workload with Argus Links

```html
<table width="100%" cellpadding="0" cellspacing="0" border="1" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;border-color:#dee2e6;margin-bottom:5px;">
<tr bgcolor="#f8f9fa" style="background-color:#f8f9fa;">
  <th style="border:1px solid #dee2e6;padding:4px 8px;text-align:left;font-weight:bold;">Workload</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">Max Throughput (run)</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">P90 (ms)</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">P99 (ms)</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">Status</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">Link</th>
</tr>
<tr>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">mixed</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:right;font-family:Arial,Helvetica,sans-serif;font-size:12px;">968,649</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:right;font-family:Arial,Helvetica,sans-serif;font-size:12px;">35.16</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:right;font-family:Arial,Helvetica,sans-serif;font-size:12px;">150.99</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><!-- PASS badge --></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><a href="https://argus.scylladb.com/test/TEST_ID/runs?additionalRuns[]=RUN_ID" style="color:#007bff;text-decoration:none;">Argus</a></td>
</tr>
</table>
```

NOTE: Each workload row has its own Argus link pointing to the specific run for that workload.
Argus URL format: `https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}` (singular `/test/`, NOT `/tests/`)

## Bullet Lists (as table rows, not ul/li)

```html
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#555555;">
<tr><td style="padding:2px 0 2px 15px;">&#8226; First bullet point</td></tr>
<tr><td style="padding:2px 0 2px 15px;">&#8226; Second bullet point</td></tr>
</table>
```

## Links

```html
<a href="https://argus.scylladb.com/tests/UUID" style="color:#007bff;text-decoration:none;">Argus</a>
```

## Key Gmail Rules Summary

| DO | DON'T |
|----|-------|
| Use `<table>` for layout | Use `<div>` for layout |
| Use `bgcolor` + `style="background-color:..."` | Use `linear-gradient` |
| Use inline `style` on every element | Use `<style>` blocks or `class` |
| Use `<td>` with font-size for headings | Use `<h1>`-`<h6>` tags |
| Use `&#8226;` bullet in table rows | Use `<ul>`/`<li>` |
| Use spacer `<tr>` for vertical spacing | Use `margin-top`/`margin-bottom` |
| Use full hex colors (`#ffffff`) | Use shorthand (`#fff`) or named colors |
| Set `font-family` on each cell | Rely on inheritance from `<body>` |
| Use `display:inline-block` on spans | Use `border-radius` |
| Fixed `width="700"` on content table | Use `max-width` CSS |

## Color Reference

| Purpose | Hex | Usage |
|---------|-----|-------|
| Header background | #1a237e | Dark navy solid |
| Header subtitle | #cccccc | Light gray text |
| Passed/OK | #28a745 | Green badge |
| Failed | #dc3545 | Red badge |
| Error/Warning | #fd7e14 | Orange badge |
| No data/neutral | #6c757d | Gray badge |
| Link color | #007bff | Blue |
| Table header (dark) | #343a40 | Dark gray |
| Table header (light) | #f8f9fa | Light gray |
| Border color | #dee2e6 | Light border |
| Alternating row | #f8f9fa | Zebra striping |
| Category underline | #007bff | Blue line |
