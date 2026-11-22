#!/usr/bin/env python3
"""Print Markdown decision tables from container-key-locality.sh output."""

import argparse
import json
from itertools import product
from pathlib import Path
from statistics import median


def rows(directory, stem):
    return [json.loads(line) for line in (directory / (stem + ".jsonl")).read_text().splitlines()]


def index(records, fields, expected):
    found = {}
    for record in records:
        key = tuple(record.get(field) for field in fields)
        if key in found:
            raise ValueError(f"duplicate observation {key}")
        found[key] = record
    if set(found) != set(expected):
        raise ValueError(f"incomplete or unexpected observations for {fields}")
    return found


def table(title, note, columns, lines):
    print(f"## {title}\n\n{note}\n")
    for row in (columns, ["---"] * len(columns), *lines):
        print("| " + " | ".join(map(str, row)) + " |")
    print()


def native(directory):
    checkpoints, reads = [], []
    trials, tails = (1, 2, 3), (0, 128, 255)
    traces = ("unchanged", "first", "last", "spread")
    for arm in ("physical", "staged", "sorted"):
        records = rows(directory, "native-" + arm)
        if any(r["arm"] != arm for r in records):
            raise ValueError(f"mixed arms in native-{arm}")
        expected = [("checkpoint", trial, None, None) for trial in trials]
        expected.extend(product(("read",), trials, traces, tails))
        data = index(records, ("kind", "trial", "trace", "tail"), expected)
        checkpoint = [data["checkpoint", i, None, None] for i in trials]
        checkpoints.append([arm, f"{median(r['ns'] for r in checkpoint) / 1e6:.2f}",
                            f"{median(r['allocated_bytes'] for r in checkpoint) / 2**20:.2f}",
                            f"{median(r['snapshot_bytes'] for r in checkpoint):,.0f}"])
        for trace in traces:
            trial_means = []
            for trial in trials:
                group = [data["read", trial, trace, tail] for tail in tails]
                means = {r["mean_file_pages"] for r in group}
                if len(means) != 1 or any(r["matches"] != 64 or r["full_read_control_bytes"] <= 0 for r in group):
                    raise ValueError(f"inconsistent page mean, count or I/O control in {arm}/{trace}/{trial}")
                trial_means.append(means.pop())
            line = [arm, trace, f"{median(trial_means):.2f}"]
            for tail in tails:
                group = [data["read", trial, trace, tail] for trial in trials]
                line.append(f"{median(r['read_bytes'] for r in group) / 1024:.1f} / "
                            f"{median(r['ns'] for r in group) / 1e3:.0f}")
            reads.append(line)
    table("Native checkpoint", "Medians across three independent trials. Allocation is cumulative Go heap allocation during checkpoint.",
          ["Arm", "Elapsed ms", "Allocated MiB", "Snapshot bytes"], checkpoints)
    table("Native prefix reads", "Pages is the median of three all-256-tail mean file-page counts. Each tail cell gives median Linux process read KiB / median elapsed µs after DATA eviction. Full-file read controls were positive.",
          ["Arm", "Trace", "Pages per prefix", "Tail 0", "Tail 128", "Tail 255"], reads)


def policies(directory):
    traces = ("first", "last", "hot", "uniform", "churn")
    arms = ("append", "full", "bounded12p5", "bounded25", "bounded50", "heat25", "heat25budget", "delta50budget")
    epochs = range(1, 17)
    data = index(rows(directory, "policy") + rows(directory, "delta"),
                 ("trace", "policy", "epoch"), product(traces, arms, epochs))
    lines = []
    for arm in arms:
        line = [arm]
        for trace in traces:
            group = [data[trace, arm, e] for e in epochs]
            samples = sum(r["stream_queries"] for r in group)
            pages = sum(r["stream_pages_per_query"] * r["stream_queries"] for r in group) / samples
            extra = sum(r["maintenance_bytes"] for r in group)
            line.append(f"{pages:.3f} / {extra:,}")
        lines.append(line)
    table("Physical layout policies", "Each cell gives the query-weighted mean of distinct 4 KiB file pages during the update stream / total extra maintenance DATA bytes across all 16 epochs. Seed and foreground writes are excluded from the byte total.",
          ["Policy", *traces], lines)


def foreground(directory):
    arms = ("default", "side-near", "side-stress", "compact")
    traces, trials = ("hot-first", "spread"), range(8)
    data = index(rows(directory, "foreground"), ("trace", "arm", "trial"), product(traces, arms, trials))
    lines = []
    for trace, arm in product(traces, arms):
        rates, rates_joined, p99s = [], [], []
        loop_changes, joined_changes, p99_changes = [], [], []
        for trial in trials:
            r, base = data[trace, arm, trial], data[trace, "default", trial]
            if r["trace_sha256"] != base["trace_sha256"]:
                raise ValueError(f"unpaired write traces in {trace}/{arm}/{trial}")
            rate, p99 = r["updates_per_s_during_put_loops"], r["put_latency"]["p99_ns"]
            rates.append(rate)
            joined = r["updates_per_s_including_worker_joins"]
            rates_joined.append(joined)
            p99s.append(p99 / 1e6)
            loop_changes.append(100 * (rate / base["updates_per_s_during_put_loops"] - 1))
            joined_changes.append(100 * (joined / base["updates_per_s_including_worker_joins"] - 1))
            p99_changes.append(100 * (p99 / base["put_latency"]["p99_ns"] - 1))
        lines.append([trace, arm, f"{median(rates):.1f}", f"{median(loop_changes):+.1f}%",
                      f"{median(rates_joined):.1f}", f"{median(joined_changes):+.1f}%",
                      f"{median(p99s):.2f}", f"{median(p99_changes):+.1f}%"])
    table("Durable foreground Put", "Levels are medians of eight trials. Percentage changes are medians of per-trial ratios to default for the same trace and trial. Each p99 is calculated within its trial.",
          ["Trace", "Arm", "Put-loop ops/s", "Paired Put-loop change", "Ops/s incl. joins", "Paired incl. joins change", "Put p99 ms", "Paired p99 change"], lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="decision results directory")
    directory = parser.parse_args().directory
    native(directory)
    policies(directory)
    foreground(directory)
