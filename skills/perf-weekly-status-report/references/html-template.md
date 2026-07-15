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

## Conclusion Box (hierarchical bullet points)

The Conclusion section uses a white-background box with a heading table and a content table.
Structure: heading in one table, hierarchical bullets in a separate table (both inside the same `<td>`).

The number of top-level items and sub-bullets varies per report -- include one row per test/category that had runs, and one sub-bullet per notable observation.

```html
<tr><td height="15" style="font-size:1px;line-height:1px;">&nbsp;</td></tr>
<tr>
<td bgcolor="#ffffff" style="background-color:#ffffff;border:1px solid #dee2e6;padding:15px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#333333;">
<tr><td style="font-size:16px;font-weight:bold;padding-bottom:10px;">Conclusion</td></tr>
</table>
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#555555;">
<!-- Repeat this block for each test/category with runs -->
<tr><td style="padding:2px 0 2px 15px;">- <b>{test_or_category_name}:</b></td></tr>
<!-- Repeat this row for each observation under that test/category -->
<tr><td style="padding:2px 0 2px 30px;">&#8226; {observation}.</td></tr>
<!-- ... more observation rows as needed ... -->
<!-- ... more test/category blocks as needed ... -->
</table>
</td>
</tr>
```

Key details:
- Top-level: `padding:2px 0 2px 15px;` with `- <b>test/category name:</b>` format
- Sub-level: `padding:2px 0 2px 30px;` with `&#8226; observation.` format
- The heading table and content table are siblings inside the same `<td>` container
- The outer `<td>` has `bgcolor="#ffffff"` and `border:1px solid #dee2e6;padding:15px;`

## Section Headings (as table cells, not h-tags)

```html
<!-- Main section heading -->
<tr>
<td style="font-family:Arial,Helvetica,sans-serif;font-size:16px;font-weight:bold;color:#333333;padding-bottom:10px;">Test Overview</td>
</tr>

<!-- Category heading with underline -->
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:15px;font-weight:bold;color:#333333;padding:15px 0 5px 0;border-bottom:2px solid #007bff;">{category_name}</td></tr>
</table>

<!-- Sub-heading (test name with full version, NO status badge) -->
<table width="100%" cellpadding="0" cellspacing="0" border="0">
<tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:13px;font-weight:bold;color:#333333;padding:10px 0 5px 0;">{test_name} ({full_version})</td></tr>
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

### Overview Table (dark header, grouped by category/test/workload, with Link column)

Number of rows varies: one row per workload per test that had runs. Categories and tests with no runs are omitted.

```html
<table width="100%" cellpadding="0" cellspacing="0" border="1" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;border-color:#dee2e6;">
<tr bgcolor="#343a40" style="background-color:#343a40;">
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Category</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Test</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:center;color:#ffffff;font-weight:bold;">Workload</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:center;color:#ffffff;font-weight:bold;">Status</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:center;color:#ffffff;font-weight:bold;">Link</th>
</tr>
<!-- First row of a category group: category name shown, test name shown -->
<tr bgcolor="#ffffff" style="background-color:#ffffff;">
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{category_name}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{test_name}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{workload}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><!-- status badge --></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><a href="https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}" style="color:#007bff;text-decoration:none;">Argus</a></td>
</tr>
<!-- Subsequent workload row within same test: category and test cells are empty -->
<tr bgcolor="#f8f9fa" style="background-color:#f8f9fa;">
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;"></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;"></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{workload}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><!-- status badge --></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><a href="https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}" style="color:#007bff;text-decoration:none;">Argus</a></td>
</tr>
<!-- ... repeat rows for each workload, test, and category ... -->
</table>
```

NOTE: Each workload row has its own Argus link in the Link column. Version is in the Summary section.
Argus URL format: `https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}` (singular `/test/`, NOT `/tests/`)
Status column: just the badge (PASSED/FAILED/ERROR) -- no counts.

### Results Table (light header) -- Per-Workload with Argus Links

Number of rows varies: one per workload that has results.

```html
<table width="100%" cellpadding="0" cellspacing="0" border="1" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;border-color:#dee2e6;margin-bottom:5px;">
<tr bgcolor="#f8f9fa" style="background-color:#f8f9fa;">
  <th style="border:1px solid #dee2e6;padding:4px 8px;text-align:left;font-weight:bold;">Workload</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">Max Throughput (run)</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">P99 (ms)</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">Status</th>
  <th style="border:1px solid #dee2e6;padding:4px 8px;font-weight:bold;">Link</th>
</tr>
<!-- Repeat for each workload -->
<tr>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{workload}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:right;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{max_throughput}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:right;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{p99_value}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><!-- status badge --></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;text-align:center;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><a href="https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}" style="color:#007bff;text-decoration:none;">Argus</a></td>
</tr>
<!-- ... more rows as needed ... -->
</table>
```

NOTE: Each workload row has its own Argus link pointing to the specific run for that workload.
Argus URL format: `https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}` (singular `/test/`, NOT `/tests/`)

## Bullet Lists (as table rows, not ul/li)

### Simple bullet list (single level)

```html
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#555555;">
<tr><td style="padding:2px 0 2px 15px;">&#8226; First bullet point</td></tr>
<tr><td style="padding:2px 0 2px 15px;">&#8226; Second bullet point</td></tr>
</table>
```

### Hierarchical bullet list (Conclusion format)

Uses two levels: bold test name at 15px indent, sub-bullets at 30px indent.
Number of top-level items and sub-bullets varies per report.

```html
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#555555;">
<!-- Repeat for each test/category -->
<tr><td style="padding:2px 0 2px 15px;">- <b>{test_or_category_name}:</b></td></tr>
<!-- Repeat for each observation under that test/category -->
<tr><td style="padding:2px 0 2px 30px;">&#8226; {observation}.</td></tr>
<!-- ... more rows as needed ... -->
</table>
```

Key styling details:
- Top-level items: `padding:2px 0 2px 15px;` with `- <b>test name:</b>` format
- Sub-items: `padding:2px 0 2px 30px;` with `&#8226; observation text.` format
- Both levels are within the same `<table>` (no nested tables)
- Wrapped in a white-background box with border (same container as the "Conclusion" heading)

## Links

```html
<a href="https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}" style="color:#007bff;text-decoration:none;">Argus</a>
```

## New Issues and Reproduced Issues Sections

Issues are split into two sections based on Jira creation date (user-classified):
- **New Issues - Regression**: tickets created during the report period
- **Reproduced Issues**: tickets that existed before the report period

Both sections use a 3-column table with columns: Issue | Issue Subject | Status (width=35%).
The "Status" column contains the Jira custom field "Status Description" value for the issue.

### New Issues - Regression (shown only when new issues exist)

```html
<tr><td height="15" style="font-size:1px;line-height:1px;">&nbsp;</td></tr>
<tr>
<td bgcolor="#ffffff" style="background-color:#ffffff;border:1px solid #dee2e6;padding:15px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#333333;">
<tr><td style="font-size:16px;font-weight:bold;padding-bottom:10px;">New Issues - Regression</td></tr>
</table>
<table width="100%" cellpadding="0" cellspacing="0" border="1" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;border-color:#dee2e6;">
<tr bgcolor="#343a40" style="background-color:#343a40;">
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Issue</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Issue Subject</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;" width="35%">Status</th>
</tr>
<!-- Repeat for each new issue, alternating bgcolor #ffffff / #f8f9fa -->
<tr bgcolor="#ffffff" style="background-color:#ffffff;">
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><a href="{issue_url}" style="color:#007bff;text-decoration:none;">{ISSUE_KEY}</a></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{issue_title}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{status_description}</td>
</tr>
<!-- ... more rows as needed ... -->
</table>
</td>
</tr>
```

### Reproduced Issues (always shown)

```html
<tr><td height="15" style="font-size:1px;line-height:1px;">&nbsp;</td></tr>
<tr>
<td bgcolor="#ffffff" style="background-color:#ffffff;border:1px solid #dee2e6;padding:15px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#333333;">
<tr><td style="font-size:16px;font-weight:bold;padding-bottom:10px;">Reproduced Issues</td></tr>
</table>
<table width="100%" cellpadding="0" cellspacing="0" border="1" style="border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;border-color:#dee2e6;">
<tr bgcolor="#343a40" style="background-color:#343a40;">
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Issue</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;">Issue Subject</th>
  <th style="border:1px solid #dee2e6;padding:6px 10px;text-align:left;color:#ffffff;font-weight:bold;" width="35%">Status</th>
</tr>
<!-- Repeat for each reproduced issue, alternating bgcolor #ffffff / #f8f9fa -->
<tr bgcolor="#ffffff" style="background-color:#ffffff;">
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;"><a href="{issue_url}" style="color:#007bff;text-decoration:none;">{ISSUE_KEY}</a></td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{issue_title}</td>
  <td style="border:1px solid #dee2e6;padding:4px 8px;font-family:Arial,Helvetica,sans-serif;font-size:12px;">{status_description}</td>
</tr>
<!-- ... more rows as needed ... -->
</table>
</td>
</tr>
```

### When no reproduced issues exist

```html
<tr><td height="15" style="font-size:1px;line-height:1px;">&nbsp;</td></tr>
<tr>
<td bgcolor="#ffffff" style="background-color:#ffffff;border:1px solid #dee2e6;padding:15px;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#333333;">
<tr><td style="font-size:16px;font-weight:bold;padding-bottom:10px;">Reproduced Issues</td></tr>
</table>
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#555555;">
<tr><td style="padding:2px 0 2px 15px;">No reproduced issues in this period.</td></tr>
</table>
</td>
</tr>
```

Key details:
- Each section is in its own white-background box with border
- Issues are presented in a 3-column table: Issue (link) | Issue Subject | Status (width=35%)
- The "Status" column contains the Jira "Status Description" custom field value
- Since the Argus CLI does not expose this field, the agent must ask the user to provide the Status Description values, or leave them empty
- Issue links use `color:#007bff;text-decoration:none;` style
- "New Issues - Regression" section is OMITTED entirely if there are no new issues
- "Reproduced Issues" section is ALWAYS shown (with "No reproduced issues..." fallback text)

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
