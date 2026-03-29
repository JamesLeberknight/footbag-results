#!/usr/bin/env python3
"""
tools/inject_pre1997_persons.py

Adds PRE1997_ONLY persons from early_data/final_pre1997/persons_pre1997.csv
into both:
  - out/canonical/persons.csv
  - out/Persons_Truth.csv

These persons competed only pre-1997 and are not produced by the post-1997
pipeline. Without this step, they are absent from the canonical CSV set and
the workbook Player Summary.

Idempotent — checks for existing entries before appending.
Run after pipeline/05p5_remediate_canonical.py.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PRE97_PERSONS   = ROOT / "early_data" / "final_pre1997" / "persons_pre1997.csv"
CANONICAL_PF    = ROOT / "out" / "canonical_pf.csv"
CANONICAL_PERS  = ROOT / "out" / "canonical" / "persons.csv"
PT_CSV          = ROOT / "out" / "Persons_Truth.csv"


def load(path: Path) -> tuple[list[dict], list[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def save(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ── Load sources ──────────────────────────────────────────────────────────────

pre97_rows, _ = load(PRE97_PERSONS)
can_rows,  can_fields = load(CANONICAL_PERS)
pt_rows,   pt_fields  = load(PT_CSV)

# Build PF stats per person_id for first_year / last_year / counts
pf_rows, _ = load(CANONICAL_PF)
pf_stats: dict[str, dict] = defaultdict(lambda: {
    "events": set(), "placements": 0, "years": set()
})
for r in pf_rows:
    pid = r.get("person_id", "")
    if not pid:
        continue
    pf_stats[pid]["events"].add(r.get("event_id", ""))
    pf_stats[pid]["placements"] += 1
    yr = r.get("year", "")
    if yr:
        try:
            pf_stats[pid]["years"].add(int(yr))
        except ValueError:
            pass

# Existing person_ids in canonical and PT
can_pids = {r["person_id"] for r in can_rows}
pt_pids  = {r["effective_person_id"] for r in pt_rows}

# ── Filter: PRE1997_ONLY persons not yet in canonical ─────────────────────────

to_inject = [r for r in pre97_rows if r.get("source_scope") == "PRE1997_ONLY"]
print(f"PRE1997_ONLY persons found: {len(to_inject)}")

added_can = 0
added_pt  = 0

for p in to_inject:
    pid  = p["person_id"]
    name = p["person_canon"]

    st   = pf_stats.get(pid, {})
    yrs  = sorted(st.get("years", []))
    evts = st.get("events", set())
    plcs = st.get("placements", 0)

    # ── canonical/persons.csv ──────────────────────────────────────────────
    if pid not in can_pids:
        new_can = {f: "" for f in can_fields}
        new_can["person_id"]        = pid
        new_can["person_name"]      = name
        new_can["player_ids"]       = pid
        new_can["bap_member"]       = "0"
        new_can["fbhof_member"]     = "0"
        new_can["first_year"]       = str(yrs[0])  if yrs else ""
        new_can["last_year"]        = str(yrs[-1]) if yrs else ""
        new_can["event_count"]      = str(len(evts)) if evts else ""
        new_can["placement_count"]  = str(plcs) if plcs else ""
        can_rows.append(new_can)
        can_pids.add(pid)
        added_can += 1

    # ── Persons_Truth.csv ─────────────────────────────────────────────────
    if pid not in pt_pids:
        last_token = name.split()[-1].lower() if name else ""
        new_pt = {f: "" for f in pt_fields}
        new_pt["effective_person_id"]   = pid
        new_pt["person_canon"]          = name
        new_pt["player_ids_seen"]       = pid
        new_pt["player_names_seen"]     = name
        new_pt["source"]                = "pre1997_pipeline"
        new_pt["person_canon_clean"]    = name
        new_pt["last_token"]            = last_token
        new_pt["notes"]                 = "PRE1997_ONLY competitor"
        pt_rows.append(new_pt)
        pt_pids.add(pid)
        added_pt += 1

print(f"  Added to canonical/persons.csv:  {added_can}")
print(f"  Added to Persons_Truth.csv:      {added_pt}")

# ── Save ──────────────────────────────────────────────────────────────────────

save(CANONICAL_PERS, can_rows, can_fields)
save(PT_CSV,         pt_rows,  pt_fields)

print("\nDone.")
print(f"  canonical/persons.csv: {len(can_rows)} rows")
print(f"  Persons_Truth.csv:     {len(pt_rows)} rows")
