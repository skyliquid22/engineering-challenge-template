#!/usr/bin/env python3
"""Summarize collector run logs as markdown rows. Standard library only.

    scripts/summarize.py runs/*.log [--no-header]

Every value comes from the log: the UTC window from the first and last
tracing timestamp, everything else from the 10 s `metrics` lines. A field
the log does not carry is printed as TBD(run); nothing is estimated. Logs
written before the 60 s latency window existed get a note with the last
10 s latency figures instead of a value in the table.
"""
import re
import sys
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?Z")
KV = re.compile(r"(\w+)=([^\s]+)")
LAT = re.compile(r"lat_send_ms=n=(\d+) p50=([\d.]+) p90=([\d.]+) p99=([\d.]+) max=([\d.]+)")

COLUMNS = [
    "label", "UTC start", "UTC end", "sockets", "books live at end",
    "frames/s avg", "frames/s peak", "`received_at - sending_ts_ms` p50/p90/p99 ms, last 60 s",
    "seq gaps", "exchange errors", "resyncs", "failovers", "peak RSS MiB",
]


def label_of(path):
    name = Path(path).stem
    return re.sub(r"^\d{8}T\d{6}Z-", "", name)


def fmt_ts(ts):
    return ts.replace("T", " ") + "Z" if ts else "TBD(run)"


def summarize(path):
    first = last = None
    metrics = []
    for raw in Path(path).read_text(errors="replace").splitlines():
        line = ANSI.sub("", raw)
        m = TS.match(line)
        if not m:
            continue
        first = first or m.group(1)
        last = m.group(1)
        if " metrics: metrics " in line or "frames_s=" in line:
            fields = dict(KV.findall(line))
            lat = LAT.search(line)
            if lat:
                fields["_lat10"] = lat.groups()
            metrics.append(fields)

    def last_field(name):
        for f in reversed(metrics):
            if name in f:
                return f[name]
        return None

    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    frames = [num(f.get("frames_s")) for f in metrics if num(f.get("frames_s")) is not None]
    avg = f"{sum(frames) / len(frames):.0f}" if frames else "TBD(run)"
    peak = f"{max(frames):.0f}" if frames else "TBD(run)"

    note = None
    if last_field("lat60_p50_ms") is not None:
        lat = "{} / {} / {}".format(last_field("lat60_p50_ms"), last_field("lat60_p90_ms"), last_field("lat60_p99_ms"))
    else:
        lat = "TBD(run)"
        for f in reversed(metrics):
            if "_lat10" in f:
                n, p50, p90, p99, _ = f["_lat10"]
                note = f"{label_of(path)}: no 60 s window in this log; last 10 s interval had n={n}, p50/p90/p99 = {p50} / {p90} / {p99} ms"
                break

    rss_values = [num(f.get("rss_mib")) for f in metrics if num(f.get("rss_mib")) is not None]
    if rss_values:
        rss = f"{max(rss_values):.1f}"
    elif any("rss_mib" in f for f in metrics):
        rss = "n/a"
    else:
        rss = "TBD(run)"

    def field_or_tbd(name):
        v = last_field(name)
        return v if v is not None else "TBD(run)"

    row = [
        label_of(path), fmt_ts(first), fmt_ts(last),
        field_or_tbd("sockets"), field_or_tbd("live"), avg, peak, lat,
        field_or_tbd("gaps"), field_or_tbd("exch_err"), field_or_tbd("resyncs"), field_or_tbd("failovers"), rss,
    ]
    return row, note


def main(argv):
    header = "--no-header" not in argv
    paths = [a for a in argv if not a.startswith("--")]
    if not paths:
        print(__doc__)
        return 2
    rows, notes = [], []
    for p in paths:
        row, note = summarize(p)
        rows.append(row)
        if note:
            notes.append(note)
    if header:
        print("| " + " | ".join(COLUMNS) + " |")
        print("|" + "---|" * len(COLUMNS))
    for row in rows:
        print("| " + " | ".join(str(c) for c in row) + " |")
    for n in notes:
        print()
        print("Note: " + n)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
