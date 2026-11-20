"""Extract observations only after every independent scenario process succeeded."""
import json
import sys
from collections import Counter
from pathlib import Path

out, rounds = Path(sys.argv[1]), int(sys.argv[2])
expected = {
    "btree": ["seed", "value-cold-get", *(f"churn-{i}" for i in range(1, 5))],
    "verify": [(op, size) for size in (20_000, 40_000, 100_000)
               for op in ("read_only", "with_scratch", "restore")],
    "open": [f"orphans-{n}" for n in (0, 100, 1000)],
    "overlap": [f"{overlap}-{control}" for overlap in ("disjoint", "overlapping")
                for control in ("compact", "control")],
    "cleaner": ["gap-0s", "gap-1ms"],
}
want = Counter((r, name, arm) for r in range(1, rounds + 1)
               for name, arms in expected.items() for arm in arms)
seen = Counter()
rows = []
high_overlap_fallbacks = 0
for line in (out / "test.log").read_text().splitlines():
    if line.startswith("TARP_SCALE_RESULT "):
        raw = line.removeprefix("TARP_SCALE_RESULT ")
        row = json.loads(raw)
        name, arm = row["scenario"], row["arm"]
        if name == "verify":
            arm = arm, row["metrics"]["keys"]
        if name == "overlap" and arm == "overlapping-compact":
            high_overlap_fallbacks += row["metrics"]["relocation_fallbacks"]
        seen[row["round"], name, arm] += 1
        rows.append(raw)
if seen != want:
    raise ValueError(f"missing {want-seen}; unexpected {seen-want}")
(out / "results.jsonl").write_text("\n".join(rows) + "\n")
if not high_overlap_fallbacks:
    raise SystemExit("Timing retained, but no staged-key conflict occurred in any round")
print(f"Verified {len(rows)} workload observations in {out / 'results.jsonl'}")
