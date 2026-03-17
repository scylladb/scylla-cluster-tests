#!/usr/bin/env python3
"""Generate an SVG progress roadmap from docs/plans/progress.json.

Reads plan data and produces a GitHub-compatible SVG with:
- Domain-grouped plan nodes in a cluster topology layout
- Pipeline-style progress bar
- Stats panel with counts by status
- ScyllaDB brand-inspired color scheme

Usage:
    python3 scripts/generate_progress_svg.py

Output:
    docs/plans/assets/progress-roadmap.svg
"""

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

# ScyllaDB brand-inspired colors
COLORS = {
    "bg": "#0D1B2A",
    "bg_panel": "#1B2A4A",
    "text": "#E0E6ED",
    "text_dim": "#8899AA",
    "draft": "#607D8B",
    "approved": "#2196F3",
    "in_progress": "#FFC107",
    "blocked": "#F44336",
    "complete": "#4CAF50",
    "pending_pr": "#9E9E9E",
    "accent": "#00BCD4",
    "border": "#2A3F5F",
}

STATUS_LABELS = {
    "draft": "Draft",
    "approved": "Approved",
    "in_progress": "In Progress",
    "blocked": "Blocked",
    "complete": "Complete",
    "pending_pr": "Pending PR",
}

DOMAIN_ORDER = [
    "cluster",
    "nemesis",
    "stress-tools",
    "monitoring",
    "events",
    "ci-cd",
    "config",
    "k8s",
    "framework",
    "ai-tooling",
    "remote",
    "testing",
]

FONT = 'font-family="system-ui, -apple-system, sans-serif"'


@dataclass
class Layout:
    """SVG layout dimensions."""

    margin: int = 30
    header_h: int = 80
    stats_h: int = 60
    pipeline_h: int = 50
    domain_header_h: int = 32
    plan_row_h: int = 28
    col_gap: int = 20
    col_width: int = 380
    footer_h: int = 30


def load_progress():
    path = Path(__file__).resolve().parent.parent / "docs" / "plans" / "progress.json"
    with open(path) as f:
        return json.load(f)


def escape_xml(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _render_header(parts, svg_w, total, num_domains, lay):
    """Render title and subtitle."""
    y = lay.margin + 30
    parts.append(
        f'<text x="{svg_w // 2}" y="{y}" text-anchor="middle" {FONT} font-size="22" '
        f'font-weight="bold" fill="{COLORS["accent"]}">SCT Implementation Plans</text>'
    )
    parts.append(
        f'<text x="{svg_w // 2}" y="{y + 24}" text-anchor="middle" {FONT} font-size="13" '
        f'fill="{COLORS["text_dim"]}">{total} plans across {num_domains} domains</text>'
    )


def _render_stats(parts, svg_w, status_counts, lay):
    """Render the stats panel with status counts."""
    y = lay.margin + lay.header_h + 10
    panel_w = svg_w - lay.margin * 2
    parts.append(
        f'<rect x="{lay.margin}" y="{y}" width="{panel_w}" height="{lay.stats_h - 10}" '
        f'rx="6" fill="{COLORS["bg_panel"]}" stroke="{COLORS["border"]}" stroke-width="1"/>'
    )
    stat_x = lay.margin + 20
    for status in ["draft", "in_progress", "approved", "blocked", "complete", "pending_pr"]:
        count = status_counts.get(status, 0)
        parts.append(f'<circle cx="{stat_x}" cy="{y + 28}" r="5" fill="{COLORS[status]}"/>')
        parts.append(
            f'<text x="{stat_x + 10}" y="{y + 32}" {FONT} font-size="12" '
            f'fill="{COLORS["text"]}">{STATUS_LABELS[status]}: {count}</text>'
        )
        stat_x += 130


def _render_pipeline(parts, svg_w, total, status_counts, lay):
    """Render the pipeline progress bar."""
    y = lay.margin + lay.header_h + lay.stats_h + 5
    bar_x = lay.margin + 10
    bar_w = svg_w - lay.margin * 2 - 20
    bar_h = 20

    parts.append(
        f'<rect x="{bar_x}" y="{y}" width="{bar_w}" height="{bar_h}" '
        f'rx="10" fill="{COLORS["bg_panel"]}" stroke="{COLORS["border"]}" stroke-width="1"/>'
    )
    seg_x = bar_x
    for status in ["complete", "in_progress", "approved", "draft", "blocked", "pending_pr"]:
        count = status_counts.get(status, 0)
        if count == 0:
            continue
        seg_w = (count / total) * bar_w
        rx = "10" if seg_x == bar_x else "0"
        parts.append(f'<rect x="{seg_x}" y="{y}" width="{seg_w}" height="{bar_h}" rx="{rx}" fill="{COLORS[status]}"/>')
        if seg_w > 30:
            parts.append(
                f'<text x="{seg_x + seg_w / 2}" y="{y + 14}" text-anchor="middle" {FONT} font-size="10" '
                f'font-weight="bold" fill="{COLORS["bg"]}">{count}</text>'
            )
        seg_x += seg_w


def _render_column(parts, col_data, x_offset, content_y, domains, lay):
    """Render a column of domain groups."""
    cy = content_y
    for domain, domain_h in col_data:
        domain_plans = domains[domain]
        parts.append(
            f'<rect x="{x_offset}" y="{cy}" width="{lay.col_width}" height="{lay.domain_header_h}" '
            f'rx="4" fill="{COLORS["bg_panel"]}" stroke="{COLORS["accent"]}" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{x_offset + 12}" y="{cy + 21}" {FONT} font-size="13" '
            f'font-weight="bold" fill="{COLORS["accent"]}">{escape_xml(domain)}</text>'
        )
        parts.append(
            f'<text x="{x_offset + lay.col_width - 12}" y="{cy + 21}" text-anchor="end" {FONT} font-size="11" '
            f'fill="{COLORS["text_dim"]}">{len(domain_plans)} plans</text>'
        )
        py = cy + lay.domain_header_h + 4
        for plan in domain_plans:
            color = COLORS[plan["status"]]
            title = escape_xml(plan["title"])
            if len(title) > 40:
                title = title[:37] + "..."
            parts.append(f'<circle cx="{x_offset + 14}" cy="{py + 10}" r="4" fill="{color}"/>')
            parts.append(
                f'<text x="{x_offset + 26}" y="{py + 14}" {FONT} font-size="11" fill="{COLORS["text"]}">{title}</text>'
            )
            status_label = escape_xml(STATUS_LABELS[plan["status"]])
            parts.append(
                f'<text x="{x_offset + lay.col_width - 12}" y="{py + 14}" text-anchor="end" {FONT} font-size="10" fill="{color}">{status_label}</text>'
            )
            py += lay.plan_row_h
        cy += domain_h


def generate_svg(data):
    plans = data["plans"]
    total = len(plans)
    status_counts = Counter(p["status"] for p in plans)
    lay = Layout()

    # Group by domain
    domains = {}
    for plan in plans:
        domains.setdefault(plan["domain"], []).append(plan)

    # Distribute domains into two balanced columns
    left_col, right_col = [], []
    left_h = right_h = 0
    for domain in (d for d in DOMAIN_ORDER if d in domains):
        domain_h = lay.domain_header_h + len(domains[domain]) * lay.plan_row_h + 16
        if left_h <= right_h:
            left_col.append((domain, domain_h))
            left_h += domain_h
        else:
            right_col.append((domain, domain_h))
            right_h += domain_h

    svg_w = lay.margin * 2 + lay.col_width * 2 + lay.col_gap
    svg_h = lay.margin * 2 + lay.header_h + lay.stats_h + lay.pipeline_h + max(left_h, right_h) + lay.footer_h
    content_y = lay.margin + lay.header_h + lay.stats_h + lay.pipeline_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{svg_w}" height="{svg_h}" viewBox="0 0 {svg_w} {svg_h}">',
        f'<rect width="{svg_w}" height="{svg_h}" fill="{COLORS["bg"]}" rx="8"/>',
    ]

    _render_header(parts, svg_w, total, len(domains), lay)
    _render_stats(parts, svg_w, status_counts, lay)
    _render_pipeline(parts, svg_w, total, status_counts, lay)
    _render_column(parts, left_col, lay.margin, content_y, domains, lay)
    _render_column(parts, right_col, lay.margin + lay.col_width + lay.col_gap, content_y, domains, lay)

    parts.append(
        f'<text x="{svg_w // 2}" y="{svg_h - lay.margin - 5}" text-anchor="middle" {FONT} font-size="10" '
        f'fill="{COLORS["text_dim"]}">Generated from docs/plans/progress.json</text>'
    )
    parts.append("</svg>")
    return "\n".join(parts)


def main():
    data = load_progress()
    svg = generate_svg(data)
    output = Path(__file__).resolve().parent.parent / "docs" / "plans" / "assets" / "progress-roadmap.svg"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(svg)
    print(f"Generated {output}")


if __name__ == "__main__":
    main()
