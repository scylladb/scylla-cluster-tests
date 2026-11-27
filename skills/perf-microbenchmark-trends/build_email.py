#!/usr/bin/env python3
"""Build the Gmail-ready HTML body of the Microbenchmark Trends email from the collector CSV.

Usage:
    python3 build_email.py --csv microbench.csv --dashboard-url URL --out email.html --spark-dir sparks/

Gmail strips SVG, so the master-trend sparklines are PNGs written to --spark-dir. The email
references them as cid:att0, cid:att1, ... in that order; attachments.json in --spark-dir lists the
files in the same order, to attach inline when creating the draft. Needs Pillow.

The summary follows the same rules as the dashboard's `master_trend` and `ladder` calcs, so the
numbers in the email and on the page always agree:
- Master trend: per benchmark, metric and chart (read/write x arm64/x86_64), every master run in
  the data window oldest first, and the change of the last run against the first.
- Across releases: per benchmark, metric and chart, the latest run of each release line from the
  oldest release to master, and its change against the previous release that has a value.
No averages: they hid write-only changes and cql-raw swings in both directions.
Benchmarks with no runs in the CSV are left out of both tables and the intro.
"""

import argparse
import csv
import json
import os

from PIL import Image, ImageDraw

BENCHMARKS = ["perf-simple-query", "perf-cql-raw", "perf-alternator"]
METRICS = ["instructions_per_op", "cpu_cycles_per_op", "allocs_per_op"]
CHARTS = ["read arm64", "read x86_64", "write arm64", "write x86_64"]
LADDER = ["2025.1", "2026.1", "2026.2", "2026.3", "2026.4", "master"]
BORDER = "border:1px solid #c8ccd0;padding:5px 10px;"
TH = "border:1px solid #c8ccd0;padding:6px 10px;color:#333333;background-color:#eef0f2;text-align:left;"
GOOD, BAD, PLAIN = "#3d8b5a", "#c0605e", "#333333"
METRIC_TINT = {"instructions_per_op": "#ffffff", "cpu_cycles_per_op": "#f7f8f9", "allocs_per_op": "#ffffff"}


def runs_of(rows: list, bench: str, chart: str, release: str, metric: str) -> list:
    """Runs of one chart and release that have a value for the metric, oldest first."""
    return sorted(
        (
            r
            for r in rows
            if r["chart"] == f"{bench} {chart}" and r["release"] == release and r.get(metric) not in ("", None)
        ),
        key=lambda r: r["run_start"],
    )


def pct(new: float, old: float) -> float:
    """Percentage change, rounded to one decimal like the dashboard."""
    return round((new / old - 1) * 100, 1)


def runs_label(count: int) -> str:
    """'1 run' or 'N runs'."""
    return f"{count} run" if count == 1 else f"{count} runs"


def fmt_value(metric: str, value: float) -> str:
    """Format a metric value: allocations with two decimals, the rest as whole numbers."""
    return f"{value:.2f}" if metric == "allocs_per_op" else f"{value:,.0f}"


def fmt_short(metric: str, value: float) -> str:
    """Short value for the master run sequence: allocations as is, the rest in thousands."""
    return f"{value:.2f}" if metric == "allocs_per_op" else f"{value / 1000:.1f}K"


def pct_span(value: float, small: bool = False) -> str:
    """Render a change: bold muted green at -1% or below, bold muted red at +1% or above."""
    color = GOOD if value <= -1 else BAD if value >= 1 else PLAIN
    weight = "font-weight:bold;" if abs(value) >= 1 else ""
    size = "font-size:11px;" if small else ""
    sign = "+" if value > 0 else "-" if value < 0 else ""
    return f'<span style="color:{color};{weight}{size}">{sign}{abs(value):.1f}%</span>'


def table_open(headers: list) -> str:
    """Open a summary table with a soft header row."""
    head = "".join(f'<th style="{TH}">{h}</th>' for h in headers)
    return (
        '<table cellpadding="0" cellspacing="0" style="border-collapse:collapse;'
        'font-family:Arial,Helvetica,sans-serif;font-size:12px;">'
        f"<tr>{head}</tr>"
    )


def metric_cell(metric: str, rowspan: int = 1) -> str:
    """The monospace metric-name cell."""
    span = f' rowspan="{rowspan}"' if rowspan > 1 else ""
    return f'<td{span} style="{BORDER}font-family:Consolas,Menlo,monospace;vertical-align:top;">{metric}</td>'


def draw_sparkline(values: list, path: str, background: str = "#ffffff") -> None:
    """Draw a 64x20 sparkline (at 2x for sharpness) like the dashboard: gray line, last point dark.

    The background is filled with the table row's tint, because some mail clients render
    transparent PNGs on black; the image is then reduced to 8 colors to keep it under ~500 bytes.
    """
    scale, width, height = 2, 64, 20
    img = Image.new("RGB", (width * scale, height * scale), background)
    draw = ImageDraw.Draw(img)
    low, high = min(values), max(values)
    span = (high - low) or 1

    def point(i: int, v: float) -> tuple:
        x = 3 + (i * (width - 6) / (len(values) - 1) if len(values) > 1 else (width - 6) / 2)
        y = height - 3 - (v - low) / span * (height - 6)
        return x * scale, y * scale

    pts = [point(i, v) for i, v in enumerate(values)]
    if len(pts) > 1:
        draw.line(pts, fill=(108, 117, 125), width=int(1.3 * scale), joint="curve")
    for i, (x, y) in enumerate(pts):
        last = i == len(pts) - 1
        r = (2.6 if last else 1.6) * scale
        color = (51, 51, 51) if last else (108, 117, 125)
        draw.ellipse([x - r, y - r, x + r, y + r], fill=color)
    img.quantize(colors=8).save(path, optimize=True)


def master_trend_table(rows: list, bench: str, sparks: list, spark_dir: str) -> str:
    """Per metric x chart: a sparkline of every master run, the change of the last against the first, the run count.

    Each sparkline PNG is appended to `sparks`; its position is its cid index (cid:attN).
    A chart with a single master run shows its value instead of a sparkline.
    """
    lines = [table_open(["Metric", *CHARTS])]
    for metric in METRICS:
        cells = metric_cell(metric)
        for chart in CHARTS:
            runs = runs_of(rows, bench, chart, "master", metric)
            if not runs:
                cells += f'<td style="{BORDER}">–</td>'
                continue
            values = [float(r[metric]) for r in runs]
            if len(values) == 1:
                # No trend yet: show the value, not a one-dot image that reads like history.
                cells += (
                    f'<td style="{BORDER}">{fmt_value(metric, values[0])}<br>'
                    f'<span style="color:#6c757d;font-size:11px;">{runs_label(1)}</span></td>'
                )
                continue
            change = pct_span(pct(values[-1], values[0]))
            name = f"spark-{bench}-{metric}-{chart.replace(' ', '_')}.png"
            path = os.path.join(spark_dir, name)
            draw_sparkline(values, path, METRIC_TINT[metric])
            cid = f"att{len(sparks)}"
            sparks.append(path)
            alt = ", ".join(fmt_short(metric, v) for v in values)
            cells += (
                f'<td style="{BORDER}"><table cellpadding="0" cellspacing="0"><tr>'
                f'<td style="padding:0 8px 0 0;vertical-align:middle;">'
                f'<img src="cid:{cid}" width="64" height="20" alt="{alt}" style="display:block;"></td>'
                f'<td style="padding:0;vertical-align:middle;line-height:1.2;">{change}<br>'
                f'<span style="color:#6c757d;font-size:11px;">{runs_label(len(values))}</span></td>'
                "</tr></table></td>"
            )
        lines.append(f'<tr style="background-color:{METRIC_TINT[metric]};">{cells}</tr>')
    lines.append("</table>")
    return "\n".join(lines)


def ladder_table(rows: list, bench: str) -> str:
    """Per metric x chart: latest value of each release line and its change against the previous release."""
    lines = [table_open(["Metric", "Chart", *LADDER])]
    for metric in METRICS:
        chart_rows = []
        for chart in CHARTS:
            cells, prev, any_value = "", None, False
            for release in LADDER:
                runs = runs_of(rows, bench, chart, release, metric)
                if not runs:
                    cells += f'<td style="{BORDER}">–</td>'
                    continue
                value = float(runs[-1][metric])
                delta = f"<br>{pct_span(pct(value, prev), small=True)}" if prev is not None else ""
                cells += f'<td style="{BORDER}">{fmt_value(metric, value)}{delta}</td>'
                prev, any_value = value, True
            if any_value:
                chart_rows.append(f'<td style="{BORDER}white-space:nowrap;">{chart}</td>{cells}')
        for i, body in enumerate(chart_rows):
            first = metric_cell(metric, len(chart_rows)) if i == 0 else ""
            lines.append(f'<tr style="background-color:{METRIC_TINT[metric]};">{first}{body}</tr>')
    lines.append("</table>")
    return "\n".join(lines)


def master_window(rows: list) -> tuple[str, str, int]:
    """First and last master run date, and the most master runs any chart has."""
    master = [r for r in rows if r["release"] == "master"]
    if not master:
        return "", "", 0
    per_chart = {}
    for r in master:
        per_chart[r["chart"]] = per_chart.get(r["chart"], 0) + 1
    dates = sorted(r["run_start"] for r in master)
    return dates[0][:10], dates[-1][:10], max(per_chart.values())


def main() -> None:
    """Write the email HTML body."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True)
    parser.add_argument("--dashboard-url", required=True)
    parser.add_argument("--out", default="email.html")
    parser.add_argument("--spark-dir", required=True, help="directory for the sparkline PNGs")
    args = parser.parse_args()
    os.makedirs(args.spark_dir, exist_ok=True)
    sparks = []

    with open(args.csv, encoding="utf-8") as src:
        rows = list(csv.DictReader(src))
    starts = sorted(r["run_start"] for r in rows)
    m_first, m_last, m_runs = master_window(rows)
    benches = [b for b in BENCHMARKS if any(r["benchmark"] == b for r in rows)]
    bench_list = " and ".join([", ".join(benches[:-1]), benches[-1]]) if len(benches) > 1 else benches[0]
    sub = 'style="font-weight:bold;margin:12px 0 4px;"'
    trend = "\n".join(f"<p {sub}>{b}</p>\n{master_trend_table(rows, b, sparks, args.spark_dir)}" for b in benches)
    ladder = "\n".join(f"<p {sub}>{b}</p>\n{ladder_table(rows, b)}" for b in benches)
    h3 = 'style="font-size:15px;font-weight:bold;margin:18px 0 2px;"'
    html = f"""<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#333333;max-width:900px;">
<p>Hi all,</p>
<p>The Microbenchmark Trends dashboard tracks the weekly microbenchmarks ({bench_list}; read and write; arm64 and x86_64), with Argus-style timeline charts of <b>instructions_per_op</b>, <b>cpu_cycles_per_op</b> and <b>allocs_per_op</b> per release line:<br>
<a href="{args.dashboard_url}" style="color:#007bff;">Microbenchmark Trends</a></p>
<p style="margin:0 0 6px;">Runs from {starts[0][:10]} to {starts[-1][:10]}. Lower is better; changes of 1% or more are green (lower) or red (higher).</p>
<p {h3}>Master trend</p>
<p style="margin:0 0 6px;">Every master run in the window ({m_runs} runs, {m_first} to {m_last}), oldest to newest; the dark dot is the last run. The number is the change of the last run against the first.</p>
{trend}
<p {h3}>Across releases</p>
<p style="margin:0 0 6px;">Latest run of each release line, oldest release to master, with the change against the release before it.</p>
{ladder}
<p style="margin-top:14px;"><b>Notes</b></p>
<ul style="margin-top:0;">
<li>NOTES-GO-HERE</li>
</ul>
<p>Thanks,<br>SIGNATURE</p>
</div>
"""
    with open(args.out, "w", encoding="utf-8") as out:
        out.write(html)
    with open(os.path.join(args.spark_dir, "attachments.json"), "w", encoding="utf-8") as out:
        json.dump([{"cid": f"att{i}", "path": os.path.abspath(p)} for i, p in enumerate(sparks)], out, indent=1)
    print(f"{len(rows)} runs, {m_runs} master runs per chart ({m_first}..{m_last}) -> {args.out}")


if __name__ == "__main__":
    main()
