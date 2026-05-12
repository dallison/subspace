#!/usr/bin/env python3
"""Render Subspace latency JSONL records as SVG charts and Markdown."""

from __future__ import annotations

import argparse
import html
import json
from collections import defaultdict
from pathlib import Path
from typing import Iterable


PREFIX = "LATENCY_JSON "
MARKER = "<!-- subspace-latency-report -->"


def iter_input_files(paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if path.is_dir():
            yield from sorted(p for p in path.rglob("*") if p.is_file())
        elif path.is_file():
            yield path


def load_records(paths: Iterable[Path]) -> list[dict]:
    records: list[dict] = []
    for path in iter_input_files(paths):
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                index = line.find(PREFIX)
                if index == -1:
                    continue
                payload = line[index + len(PREFIX) :].strip()
                try:
                    record = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                records.append(record)
    return records


def safe_name(value: str) -> str:
    out = []
    for ch in value.lower():
        out.append(ch if ch.isalnum() else "-")
    name = "".join(out).strip("-")
    while "--" in name:
        name = name.replace("--", "-")
    return name or "chart"


def nice_label(value: str) -> str:
    return value.replace("_", " ")


def chart_title(test: str, metric: str) -> str:
    return f"{test} {nice_label(metric)}"


def svg_chart(test: str, metric: str, records: list[dict], width: int = 900, height: int = 480) -> str:
    margin_left = 76
    margin_right = 24
    margin_top = 54
    margin_bottom = 72
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom

    xs = sorted({int(r["x"]) for r in records})
    ys = [float(r["value_ns"]) for r in records]
    min_x = min(xs)
    max_x = max(xs)
    min_y = 0.0
    max_y = max(ys)
    if max_y <= 0:
        max_y = 1.0

    def px(x: float) -> float:
        if max_x == min_x:
            return margin_left + plot_width / 2
        return margin_left + (x - min_x) * plot_width / (max_x - min_x)

    def py(y: float) -> float:
        return margin_top + plot_height - (y - min_y) * plot_height / (max_y - min_y)

    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        label = f"{record.get('os', 'unknown')} {record.get('series', '')}".strip()
        grouped[label].append(record)

    palette = ["#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c", "#0891b2"]
    title = html.escape(chart_title(test, metric))
    x_name = html.escape(nice_label(str(records[0].get("x_name", "x"))))
    max_y_label = f"{max_y:,.0f}"
    mid_y_label = f"{max_y / 2:,.0f}"

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2}" y="28" text-anchor="middle" font-family="sans-serif" font-size="20" fill="#111827">{title}</text>',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{height - margin_bottom}" stroke="#374151"/>',
        f'<line x1="{margin_left}" y1="{height - margin_bottom}" x2="{width - margin_right}" y2="{height - margin_bottom}" stroke="#374151"/>',
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{width - margin_right}" y2="{margin_top}" stroke="#e5e7eb"/>',
        f'<line x1="{margin_left}" y1="{margin_top + plot_height / 2}" x2="{width - margin_right}" y2="{margin_top + plot_height / 2}" stroke="#e5e7eb"/>',
        f'<text x="{margin_left - 10}" y="{margin_top + 4}" text-anchor="end" font-family="sans-serif" font-size="12" fill="#4b5563">{max_y_label}</text>',
        f'<text x="{margin_left - 10}" y="{margin_top + plot_height / 2 + 4}" text-anchor="end" font-family="sans-serif" font-size="12" fill="#4b5563">{mid_y_label}</text>',
        f'<text x="{margin_left - 10}" y="{height - margin_bottom + 4}" text-anchor="end" font-family="sans-serif" font-size="12" fill="#4b5563">0</text>',
        f'<text x="{width / 2}" y="{height - 22}" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#374151">{x_name}</text>',
        f'<text x="18" y="{height / 2}" transform="rotate(-90 18 {height / 2})" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#374151">nanoseconds</text>',
    ]

    for x in xs:
        x_pos = px(x)
        parts.append(
            f'<line x1="{x_pos:.2f}" y1="{height - margin_bottom}" x2="{x_pos:.2f}" y2="{height - margin_bottom + 5}" stroke="#374151"/>'
        )
        parts.append(
            f'<text x="{x_pos:.2f}" y="{height - margin_bottom + 20}" text-anchor="middle" font-family="sans-serif" font-size="10" fill="#4b5563">{x}</text>'
        )

    legend_x = margin_left
    legend_y = height - 46
    for idx, (label, series_records) in enumerate(sorted(grouped.items())):
        color = palette[idx % len(palette)]
        points = []
        for record in sorted(series_records, key=lambda r: int(r["x"])):
            points.append(f'{px(float(record["x"])):.2f},{py(float(record["value_ns"])):.2f}')
        if len(points) == 1:
            x_str, y_str = points[0].split(",")
            parts.append(f'<circle cx="{x_str}" cy="{y_str}" r="4" fill="{color}"/>')
        else:
            parts.append(
                f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="2"/>'
            )
            for point in points:
                x_str, y_str = point.split(",")
                parts.append(f'<circle cx="{x_str}" cy="{y_str}" r="3" fill="{color}"/>')
        lx = legend_x + (idx % 3) * 250
        ly = legend_y + (idx // 3) * 18
        parts.append(f'<rect x="{lx}" y="{ly - 10}" width="12" height="12" fill="{color}"/>')
        parts.append(
            f'<text x="{lx + 18}" y="{ly}" font-family="sans-serif" font-size="12" fill="#111827">{html.escape(label)}</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


def summarize(records: list[dict]) -> list[tuple[str, str, str, str, float]]:
    latest: dict[tuple[str, str, str, str], dict] = {}
    for record in records:
        key = (
            str(record.get("test", "")),
            str(record.get("metric", "")),
            str(record.get("os", "")),
            str(record.get("series", "")),
        )
        if key not in latest or int(record["x"]) > int(latest[key]["x"]):
            latest[key] = record
    rows = []
    for (test, metric, os_name, series), record in sorted(latest.items()):
        rows.append((test, metric, os_name, series, float(record["value_ns"])))
    return rows


def write_reports(records: list[dict], output_dir: Path, run_url: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    charts_dir = output_dir / "charts"
    charts_dir.mkdir(exist_ok=True)

    by_chart: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for record in records:
        by_chart[(str(record["test"]), str(record["metric"]))].append(record)

    chart_files: list[Path] = []
    for (test, metric), chart_records in sorted(by_chart.items()):
        filename = f"{safe_name(test)}-{safe_name(metric)}.svg"
        path = charts_dir / filename
        path.write_text(svg_chart(test, metric, chart_records), encoding="utf-8")
        chart_files.append(path)

    jsonl_path = output_dir / "latency.jsonl"
    jsonl_path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )

    rows = summarize(records)
    summary_lines = [
        "# Optimized Latency Report",
        "",
        f"Parsed {len(records)} latency records.",
        "",
        "| Test | Metric | OS | Series | Latest Value (ns) |",
        "| --- | --- | --- | --- | ---: |",
    ]
    for test, metric, os_name, series, value in rows:
        summary_lines.append(
            f"| {test} | {metric} | {os_name} | {series} | {value:,.0f} |"
        )
    summary_lines.extend(["", "## Charts", ""])
    for chart_file in chart_files:
        rel = chart_file.relative_to(output_dir)
        summary_lines.append(f"### {chart_file.stem}")
        summary_lines.append("")
        summary_lines.append(f"![{chart_file.stem}]({rel.as_posix()})")
        summary_lines.append("")

    (output_dir / "summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    comment_lines = [
        MARKER,
        "# Optimized Latency Report",
        "",
        f"Parsed {len(records)} latency records from optimized latency runs.",
        "",
        "| Test | Metric | OS | Series | Latest Value (ns) |",
        "| --- | --- | --- | --- | ---: |",
    ]
    for test, metric, os_name, series, value in rows:
        comment_lines.append(
            f"| {test} | {metric} | {os_name} | {series} | {value:,.0f} |"
        )
    comment_lines.extend(
        [
            "",
            f"SVG charts and raw JSONL are attached to the [workflow run]({run_url}).",
        ]
    )
    (output_dir / "comment.md").write_text("\n".join(comment_lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-url", default="")
    args = parser.parse_args()

    records = load_records(args.inputs)
    if not records:
        raise SystemExit("no latency records found")
    write_reports(records, args.output_dir, args.run_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
