#!/usr/bin/env python3
"""
rename_worlds_event_ids.py

Applies authoritative worlds event_id convention:

  YYYY_worlds            — single worlds event, no city data (or after 1983)
  YYYY_worlds_cityname   — single worlds event with known city
  YYYY_worlds_nhsa       — 1983 NHSA co-worlds
  YYYY_worlds_wfa        — 1983 WFA co-worlds
  YYYY_worlds_clackamas  — displaced pre-NHSA generic (1980)
  YYYY_worlds_portland   — displaced pre-NHSA generic (1981, 1982, 1983)

Rules applied:
  1980–1982: NHSA events ARE the authoritative worlds → YYYY_worlds
             Existing YYYY_worlds generic → YYYY_worlds_cityname
  1983:      NHSA + WFA both worlds → 1983_worlds_nhsa / 1983_worlds_wfa
             Existing 1983_worlds generic → 1983_worlds_portland
  1986–1989: WFA worlds with city (Golden, CO) → YYYY_worlds_golden
  1990–1993: WFA/IFAB worlds, no city → YYYY_worlds (single event per year)
  1994:      IFAB worlds (Palo Alto, CA) → 1994_worlds_palo_alto
  1995–1996: IFAB worlds, no city → YYYY_worlds

Updates:
  out/canonical_all_union/  — all 4 tables
  out/canonical_all/        — all 4 tables
  early_data/canonical/     — all 4 PRE1997 tables
  early_data/out/           — early feeds

After running:
  python3 tools/build_appsafe_merged.py
  python3 tools/build_merged_feeds.py
  python3 tools/build_merged_workbook_v14.py
"""

import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)
ROOT = Path(__file__).resolve().parent.parent

UNION       = ROOT / "out" / "canonical_all_union"
CANON_ALL   = ROOT / "out" / "canonical_all"
EARLY_CANON = ROOT / "early_data" / "canonical"
EARLY_OUT   = ROOT / "early_data" / "out"

# ── Rename map ────────────────────────────────────────────────────────────────
# Applied as single-pass substitution — no chaining.
RENAMES = {
    # 1980–1982: NHSA = authoritative worlds → YYYY_worlds
    # Existing YYYY_worlds (generic event) gets city qualifier
    "1980_nhsa":          "1980_worlds",
    "1980_worlds":        "1980_worlds_clackamas",
    "1981_nhsa":          "1981_worlds",
    "1981_worlds":        "1981_worlds_portland",
    "1982_nhsa":          "1982_worlds",
    "1982_worlds":        "1982_worlds_portland",

    # 1983: both NHSA and WFA are worlds — distinguish by org
    # Generic YYYY_worlds gets city qualifier
    "1983_nhsa":          "1983_worlds_nhsa",
    "1983_wfa":           "1983_worlds_wfa",
    "1983_worlds":        "1983_worlds_portland",

    # 1986–1989: WFA worlds in Golden, CO
    "1986_worlds_wfa":    "1986_worlds_golden",
    "1987_worlds_wfa":    "1987_worlds_golden",
    "1988_worlds_wfa":    "1988_worlds_golden",
    "1989_worlds_wfa":    "1989_worlds_golden",

    # 1990–1992: WFA worlds, no city data → simple YYYY_worlds
    "1990_worlds_wfa":    "1990_worlds",
    "1991_worlds_wfa":    "1991_worlds",
    "1992_worlds_wfa":    "1992_worlds",

    # 1993–1996: IFAB worlds
    "1993_worlds_ifab":   "1993_worlds",
    "1994_worlds_ifab":   "1994_worlds_palo_alto",
    "1995_worlds_ifab":   "1995_worlds",
    "1996_worlds_ifab":   "1996_worlds",
}

# Only PRE1997 IDs change (POST1997 already follow YYYY_worlds_cityname)
# Validate no new slug already exists in the wrong place
ALL_NEW = set(RENAMES.values())
assert len(ALL_NEW) == len(RENAMES), "Duplicate target slugs in RENAMES!"


def load(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fields_of(path):
    with open(path, newline="", encoding="utf-8") as f:
        return next(csv.reader(f))


def save(path, rows, fields):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  → {path.relative_to(ROOT)}  ({len(rows):,} rows)")


def remap(val):
    return RENAMES.get(val, val)


def remap_col(rows, col):
    changed = 0
    for r in rows:
        old = r.get(col, "")
        new = remap(old)
        if new != old:
            r[col] = new
            changed += 1
    return changed


# ── 1. canonical_all_union/ ───────────────────────────────────────────────────
print("=" * 60)
print("1. canonical_all_union/")
print("=" * 60)

for fname, id_col in [
    ("events.csv",                    "event_id"),
    ("event_disciplines.csv",         "event_id"),
    ("event_results.csv",             "event_id"),
    ("event_result_participants.csv", "event_id"),
]:
    path = UNION / fname
    rows = load(path)
    fields = fields_of(path)
    n = remap_col(rows, id_col)
    print(f"  {fname}: {n} IDs remapped")
    save(path, rows, fields)

# Also update legacy_hex_id's event_id in overlap candidates
overlap_path = UNION / "early_overlap_candidates.csv"
if overlap_path.exists():
    rows = load(overlap_path)
    fields = fields_of(overlap_path)
    n1 = remap_col(rows, "post1997_event_key")
    n2 = remap_col(rows, "pre1997_event_id")
    if n1 or n2:
        save(overlap_path, rows, fields)
        print(f"  early_overlap_candidates.csv: {n1}+{n2} IDs remapped")

print()


# ── 2. canonical_all/ ────────────────────────────────────────────────────────
print("=" * 60)
print("2. canonical_all/")
print("=" * 60)

for fname, id_col in [
    ("events.csv",                    "event_id"),
    ("event_disciplines.csv",         "event_id"),
    ("event_results.csv",             "event_id"),
    ("event_result_participants.csv", "event_id"),
]:
    path = CANON_ALL / fname
    if not path.exists():
        print(f"  SKIP {fname}")
        continue
    rows = load(path)
    fields = fields_of(path)
    n = remap_col(rows, id_col)
    print(f"  {fname}: {n} IDs remapped")
    save(path, rows, fields)

print()


# ── 3. early_data/canonical/ ────────────────────────────────────────────────
print("=" * 60)
print("3. early_data/canonical/")
print("=" * 60)

for fname, id_col in [
    ("events_pre1997.csv",                    "canonical_event_id"),
    ("event_results_pre1997.csv",             "canonical_event_id"),
    ("event_result_participants_pre1997.csv", "canonical_event_id"),
    ("event_disciplines_pre1997.csv",         "canonical_event_id"),
]:
    path = EARLY_CANON / fname
    if not path.exists():
        print(f"  SKIP {fname}")
        continue
    rows = load(path)
    fields = fields_of(path)
    # Detect actual column name
    if rows and id_col not in rows[0]:
        id_col = "event_id" if "event_id" in rows[0] else list(rows[0].keys())[0]
    n = remap_col(rows, id_col)
    print(f"  {fname}: {n} rows remapped (id_col={id_col})")
    save(path, rows, fields)

print()


# ── 4. early_data/out/ feeds ─────────────────────────────────────────────────
print("=" * 60)
print("4. early_data/out/ feeds")
print("=" * 60)

for fname in ["early_placements_feed.csv", "early_stage2_feed.csv"]:
    path = EARLY_OUT / fname
    if not path.exists():
        print(f"  SKIP {fname}")
        continue
    rows = load(path)
    fields = fields_of(path)
    n = remap_col(rows, "event_id")
    print(f"  {fname}: {n} rows remapped")
    save(path, rows, fields)

print()
print("Done.")
print()
print("Now run:")
print("  python3 tools/build_appsafe_merged.py")
print("  python3 tools/build_merged_feeds.py")
print("  python3 tools/build_merged_workbook_v14.py")
print("  python3 tools/event_comparison_viewerV10.py --stage2 out/merged_stage2.csv "
      "--pf out/merged_placements_flat.csv --output out/merged_event_viewer.html")
