#!/usr/bin/env python3
"""
Generate Part 4 plots from mcperf_i.txt and jobs_i.txt result files.

The script intentionally uses only the Python standard library and writes SVG
files, so it can run on a fresh VM without installing plotting packages.
"""

from __future__ import annotations

import argparse
import html
import math
import re
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


SLO_US = 800.0


@dataclass(frozen=True)
class McperfRow:
    index: int
    p95_us: float
    qps: float
    target_qps: float


@dataclass(frozen=True)
class JobEvent:
    timestamp: datetime
    event: str
    job: str
    args: str


def parse_mcperf(path: Path) -> list[McperfRow]:
    rows: list[McperfRow] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split()
        if not parts or parts[0] != "read" or len(parts) < 17:
            continue
        rows.append(
            McperfRow(
                index=len(rows),
                p95_us=float(parts[11]),
                qps=float(parts[15]),
                target_qps=float(parts[16]),
            )
        )
    return rows


def parse_jobs(path: Path) -> list[JobEvent]:
    events: list[JobEvent] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split(maxsplit=3)
        if len(parts) < 3:
            continue
        args = urllib.parse.unquote_plus(parts[3]) if len(parts) == 4 else ""
        events.append(JobEvent(datetime.fromisoformat(parts[0]), parts[1], parts[2], args))
    return events


def _polyline(points: list[tuple[float, float]], color: str, width: float = 2.0) -> str:
    if not points:
        return ""
    coords = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    return f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="{width}"/>'


def _line(x1: float, y1: float, x2: float, y2: float, color: str, width: float = 1.0, dash: str = "") -> str:
    dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
    return f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color}" stroke-width="{width}"{dash_attr}/>'


def _text(x: float, y: float, value: str, size: int = 12, anchor: str = "start", color: str = "#172026") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Arial, sans-serif" '
        f'font-size="{size}" text-anchor="{anchor}" fill="{color}">{html.escape(value)}</text>'
    )


def _rect(x: float, y: float, w: float, h: float, color: str, opacity: float = 1.0) -> str:
    return f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" fill="{color}" opacity="{opacity:.2f}"/>'


def _scale(value: float, low: float, high: float, start: float, end: float) -> float:
    if high <= low:
        return (start + end) / 2
    return start + (value - low) * (end - start) / (high - low)


def job_intervals(events: list[JobEvent]) -> list[tuple[str, float, float]]:
    if not events:
        return []
    base = events[0].timestamp
    starts: dict[str, float] = {}
    intervals: list[tuple[str, float, float]] = []
    for event in events:
        t = (event.timestamp - base).total_seconds()
        if event.event == "start" and event.job not in {"scheduler", "memcached"}:
            starts[event.job] = t
        elif event.event == "end" and event.job in starts:
            intervals.append((event.job, starts.pop(event.job), t))
    return intervals


def memcached_core_series(events: list[JobEvent]) -> list[tuple[float, int]]:
    if not events:
        return []
    base = events[0].timestamp
    series: list[tuple[float, int]] = []
    for event in events:
        if event.job != "memcached" or event.event not in {"start", "update_cores"}:
            continue
        match = re.search(r"\[([^\]]*)\]", event.args)
        if not match:
            continue
        cores = [item for item in match.group(1).split(",") if item.strip()]
        series.append(((event.timestamp - base).total_seconds(), len(cores)))
    return series


def draw_run(policy: str, run_id: int, mcperf_rows: list[McperfRow], events: list[JobEvent], out: Path) -> None:
    width, height = 1100, 720
    left, right = 78, 1040
    latency_top, latency_bottom = 82, 330
    jobs_top, jobs_bottom = 430, 630
    max_x = max((row.index for row in mcperf_rows), default=0)
    max_latency = max([SLO_US, *[row.p95_us for row in mcperf_rows]], default=SLO_US) * 1.12
    max_qps = max((row.target_qps for row in mcperf_rows), default=1.0) * 1.10

    parts: list[str] = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="720" viewBox="0 0 1100 720">',
        _rect(0, 0, width, height, "#ffffff"),
        _text(32, 38, f"Part 4 {policy} run {run_id}", 22),
        _text(32, 62, "mcperf p95 latency and target QPS; batch execution intervals below", 12, color="#4b5b63"),
        _line(left, latency_bottom, right, latency_bottom, "#9aa7ad"),
        _line(left, latency_top, left, latency_bottom, "#9aa7ad"),
        _text(left, latency_top - 14, "p95 latency (us)", 12, color="#4b5b63"),
    ]

    slo_y = _scale(SLO_US, 0, max_latency, latency_bottom, latency_top)
    parts.append(_line(left, slo_y, right, slo_y, "#cc3d3d", 1.4, "6 5"))
    parts.append(_text(right - 4, slo_y - 8, "0.8 ms SLO", 12, "end", "#cc3d3d"))

    latency_points = [
        (
            _scale(row.index, 0, max(max_x, 1), left, right),
            _scale(row.p95_us, 0, max_latency, latency_bottom, latency_top),
        )
        for row in mcperf_rows
    ]
    qps_points = [
        (
            _scale(row.index, 0, max(max_x, 1), left, right),
            _scale(row.target_qps, 0, max_qps, latency_bottom, latency_top),
        )
        for row in mcperf_rows
    ]
    parts.append(_polyline(qps_points, "#7b61b5", 1.6))
    parts.append(_polyline(latency_points, "#1c7c8c", 2.2))
    parts.append(_text(right - 180, latency_top + 18, "p95 latency", 12, color="#1c7c8c"))
    parts.append(_text(right - 88, latency_top + 18, "target QPS", 12, color="#7b61b5"))

    for tick in range(0, int(math.ceil(max_latency / 400.0)) + 1):
        value = tick * 400
        y = _scale(value, 0, max_latency, latency_bottom, latency_top)
        parts.append(_line(left - 5, y, left, y, "#9aa7ad"))
        parts.append(_text(left - 10, y + 4, str(value), 11, "end", "#4b5b63"))

    intervals = job_intervals(events)
    max_t = max([end for _, _, end in intervals], default=max_x)
    series = memcached_core_series(events)
    max_t = max([max_t, *[t for t, _ in series]], default=max_t)
    row_height = 20
    colors = ["#4e79a7", "#f28e2b", "#59a14f", "#e15759", "#76b7b2", "#edc948", "#b07aa1"]
    job_names = sorted({name for name, _, _ in intervals})
    y_by_job = {name: jobs_top + 26 + i * (row_height + 8) for i, name in enumerate(job_names)}

    parts.append(_line(left, jobs_bottom, right, jobs_bottom, "#9aa7ad"))
    parts.append(_text(left, jobs_top, "batch jobs and memcached core count", 12, color="#4b5b63"))
    for i, (name, start, end) in enumerate(intervals):
        x1 = _scale(start, 0, max(max_t, 1), left, right)
        x2 = _scale(end, 0, max(max_t, 1), left, right)
        y = y_by_job[name]
        parts.append(_rect(x1, y - row_height + 4, max(2, x2 - x1), row_height, colors[i % len(colors)], 0.82))
        parts.append(_text(left - 10, y, name, 11, "end", "#4b5b63"))

    if series:
        y_base = jobs_bottom - 18
        core_points: list[tuple[float, float]] = []
        for t, count in series:
            core_points.append((_scale(t, 0, max(max_t, 1), left, right), _scale(count, 0, 4, y_base, jobs_top + 20)))
        parts.append(_polyline(core_points, "#222222", 2.0))
        parts.append(_text(right - 4, y_base - 64, "memcached cores", 11, "end", "#222222"))

    for frac in (0, 0.25, 0.5, 0.75, 1.0):
        x = _scale(frac, 0, 1, left, right)
        label = f"{int(frac * max(max_x, 1))}"
        parts.append(_line(x, latency_bottom, x, latency_bottom + 5, "#9aa7ad"))
        parts.append(_text(x, latency_bottom + 22, label, 11, "middle", "#4b5b63"))
    parts.append(_text((left + right) / 2, latency_bottom + 42, "mcperf interval index", 12, "middle", "#4b5b63"))

    parts.append("</svg>")
    out.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="065")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--out-dir", type=Path, default=None)
    args = parser.parse_args()

    out_dir = args.out_dir or args.root / "part4_plots"
    out_dir.mkdir(parents=True, exist_ok=True)

    generated = 0
    for policy, question in (("static", "3"), ("dynamic", "4")):
        results_dir = args.root / f"part_4_{question}_results_group_{args.group}"
        if not results_dir.exists():
            continue
        for mcperf_path in sorted(results_dir.glob("mcperf_*.txt")):
            run_match = re.search(r"mcperf_(\d+)\.txt$", mcperf_path.name)
            if not run_match:
                continue
            run_id = int(run_match.group(1))
            jobs_path = results_dir / f"jobs_{run_id}.txt"
            if not jobs_path.exists():
                continue
            out = out_dir / f"part4_{policy}_run_{run_id}.svg"
            draw_run(policy, run_id, parse_mcperf(mcperf_path), parse_jobs(jobs_path), out)
            print(out)
            generated += 1

    if generated == 0:
        raise SystemExit("no matching Part 4 result files found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
