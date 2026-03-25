#!/usr/bin/env python3
"""
cleanup_event_ids.py

Applies 14 surgical event_id renames for readability/consistency:
  - PRE1997: standardize worlds naming (wfa_worlds → worlds_wfa, ifab_worlds → worlds_ifab)
  - PRE1997: shorten 1980_national_world_championships → 1980_worlds
  - POST1997: shorten 2001_worlds_san_francisco_bay_area → 2001_worlds_san_francisco
  - POST1997: shorten 2010_naantalin_seudun_mestaruuskilpailut → 2010_naantali_mestaruuskilpailut

Updates:
  canonical_all_union/  — all 4 event tables
  early_data/canonical/ — PRE1997 events only
  early_data/out/       — early feeds (PRE1997 only)

After running, rebuild:
  python3 tools/build_appsafe_merged.py
  python3 tools/build_merged_feeds.py
  python3 tools/build_merged_workbook_v14.py
  python3 tools/event_comparison_viewerV10.py ...
"""

import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)
ROOT = Path(__file__).resolve().parent.parent

UNION       = ROOT / "out" / "canonical_all_union"
EARLY_CANON = ROOT / "early_data" / "canonical"
EARLY_OUT   = ROOT / "early_data" / "out"

# ── Rename map ────────────────────────────────────────────────────────────────
RENAMES = {
    # PRE1997 worlds standardization
    "1980_national_world_championships": "1980_worlds",
    "1986_wfa_worlds":                   "1986_worlds_wfa",
    "1987_wfa_worlds":                   "1987_worlds_wfa",
    "1988_wfa_worlds":                   "1988_worlds_wfa",
    "1989_wfa_worlds":                   "1989_worlds_wfa",
    "1990_wfa_worlds":                   "1990_worlds_wfa",
    "1991_wfa_worlds":                   "1991_worlds_wfa",
    "1992_wfa_worlds":                   "1992_worlds_wfa",
    "1993_ifab_worlds":                  "1993_worlds_ifab",
    "1994_ifab_worlds":                  "1994_worlds_ifab",
    "1995_ifab_worlds":                  "1995_worlds_ifab",
    "1996_ifab_worlds":                  "1996_worlds_ifab",
    # POST1997 shortening
    "2001_worlds_san_francisco_bay_area":       "2001_worlds_san_francisco",
    "2010_naantalin_seudun_mestaruuskilpailut": "2010_naantali_mestaruuskilpailut",
}

PRE1997_RENAMES  = {k: v for k, v in RENAMES.items() if k[0:2] in ("19",) and int(k[:4]) < 1997}
POST1997_RENAMES = {k: v for k, v in RENAMES.items() if int(k[:4]) >= 1997}


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


def remap(val, rename_map):
    return rename_map.get(val, val)


def remap_col(rows, col, rename_map):
    changed = 0
    for r in rows:
        old = r.get(col, "")
        new = remap(old, rename_map)
        if new != old:
            r[col] = new
            changed += 1
    return changed


# ═══════════════════════════════════════════════════════════════════════════════
# 1. canonical_all_union/
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("1. canonical_all_union/")
print("=" * 60)

for fname, id_col, rename_map in [
    ("events.csv",                    "event_id",  RENAMES),
    ("event_disciplines.csv",         "event_id",  RENAMES),
    ("event_results.csv",             "event_id",  RENAMES),
    ("event_result_participants.csv", "event_id",  RENAMES),
]:
    path = UNION / fname
    rows = load(path)
    fields = fields_of(path)
    n = remap_col(rows, id_col, rename_map)
    # For events.csv, also update legacy_hex_id comment (not needed — hex already preserved)
    print(f"  {fname}: {n} IDs remapped")
    save(path, rows, fields)

print()


# ═══════════════════════════════════════════════════════════════════════════════
# 2. early_data/canonical/ (PRE1997 only)
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("2. early_data/canonical/")
print("=" * 60)

for fname, id_col in [
    ("events_pre1997.csv",                    "canonical_event_id"),
    ("event_results_pre1997.csv",             "canonical_event_id"),
    ("event_result_participants_pre1997.csv", "canonical_event_id"),
    ("event_disciplines_pre1997.csv",         "canonical_event_id"),
]:
    path = EARLY_CANON / fname
    if not path.exists():
        print(f"  SKIP {fname} (not found)")
        continue
    rows = load(path)
    fields = fields_of(path)
    # Detect actual column name
    if rows and id_col not in rows[0]:
        id_col = "event_id" if "event_id" in rows[0] else list(rows[0].keys())[0]
    n = remap_col(rows, id_col, PRE1997_RENAMES)
    print(f"  {fname}: {n} rows remapped (id_col={id_col})")
    save(path, rows, fields)

print()


# ═══════════════════════════════════════════════════════════════════════════════
# 3. early_data/out/ feeds (PRE1997 only)
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("3. early_data/out/ feeds")
print("=" * 60)

for fname in ["early_placements_feed.csv", "early_stage2_feed.csv"]:
    path = EARLY_OUT / fname
    if not path.exists():
        print(f"  SKIP {fname}")
        continue
    rows = load(path)
    fields = fields_of(path)
    n = remap_col(rows, "event_id", PRE1997_RENAMES)
    print(f"  {fname}: {n} rows remapped")
    save(path, rows, fields)

print()
print("Done.")
print()
print("Now run:")
print("  python3 tools/build_appsafe_merged.py")
print("  python3 tools/build_merged_feeds.py")
print("  python3 tools/build_merged_workbook_v14.py")
print("  python3 tools/event_comparison_viewerV10.py --stage2 out/merged_stage2.csv --pf out/merged_placements_flat.csv --output out/merged_event_viewer.html")
