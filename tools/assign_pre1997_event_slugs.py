#!/usr/bin/env python3
"""
assign_pre1997_event_slugs.py

Replaces random hex IDs for PRE1997 events with human-readable slug IDs
matching the convention used by POST1997 events (e.g. 1982_nhsa, 1986_wfa_worlds).

Hex IDs are preserved as legacy_hex_id in events.csv for audit.

Updates:
  out/canonical_all_union/  — all 5 tables
  early_data/canonical/     — all 4 event tables
  early_data/out/           — early_placements_feed, early_stage2_feed
  out/merged_events_normalized.csv, merged_placements_flat.csv, merged_stage2.csv

After running, rebuild:
  python3 tools/build_appsafe_merged.py
  python3 tools/build_merged_feeds.py
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UNION      = ROOT / "out" / "canonical_all_union"
EARLY_CANON = ROOT / "early_data" / "canonical"
EARLY_OUT  = ROOT / "early_data" / "out"
MERGED_OUT = ROOT / "out"

csv.field_size_limit(sys.maxsize)

# ── Slug mapping: hex_id → new_slug ──────────────────────────────────────────
# Only the 29 surviving PRE1997 events (after the 3 merges).
HEX_TO_SLUG = {
    "bb975bff1d": "1980_nhsa",
    "8396ba09ac": "1980_national_world_championships",
    "b9540daaab": "1981_nhsa",
    "af9e865ed5": "1981_worlds",
    "1e36e7ca92": "1982_nhsa",
    "e41e20722d": "1982_western_regionals",
    "f771c4ef26": "1982_worlds",
    "76498d51d2": "1983_nhsa",
    "0a43031545": "1983_oregon_state",
    "00659874b9": "1983_wfa",
    "76ff377548": "1983_worlds",
    "6cd9e5b635": "1984_worlds",
    "77aa0f8390": "1984_euro_champs",
    "0e1499ab21": "1984_wfa_nationals",
    "88ab188635": "1985_worlds",
    "1c74083104": "1985_wfa_nationals",
    "b4b6a194e2": "1986_wfa_worlds",
    "e753e2a15c": "1987_euro_champs",
    "8667de3590": "1987_wfa_worlds",
    "7f20a87202": "1988_us_nationals",
    "d272be24f7": "1988_wfa_worlds",
    "334c632f6e": "1989_wfa_worlds",
    "15782bd1d5": "1990_wfa_worlds",
    "76340820ba": "1991_wfa_worlds",
    "07e711d92a": "1992_wfa_worlds",
    "e04899bee1": "1993_ifab_worlds",
    "b9d35c4646": "1994_ifab_worlds",
    "1faddf6c05": "1995_ifab_worlds",
    "6dc440629d": "1996_ifab_worlds",
}

SLUG_TO_HEX = {v: k for k, v in HEX_TO_SLUG.items()}
ALL_HEX = set(HEX_TO_SLUG.keys())


def load(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save(path, rows, fields=None):
    if not rows:
        return
    fn = fields or list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fn, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  → {path.relative_to(ROOT)}  ({len(rows):,} rows)")


def fields_of(path):
    with open(path, newline="", encoding="utf-8") as f:
        return next(csv.reader(f))


def remap_id(val):
    """Return slug if val is a known hex ID, else return unchanged."""
    return HEX_TO_SLUG.get(val, val)


def remap_pipe_ids(val):
    """Remap pipe-separated person_ids or event_ids (no change needed here — for future use)."""
    return val


# ── Helper: remap event_id column in any table ────────────────────────────────
def remap_event_id_col(rows, id_field="event_id"):
    changed = 0
    for r in rows:
        old = r.get(id_field, "")
        new = remap_id(old)
        if new != old:
            r[id_field] = new
            changed += 1
    return changed


# ═══════════════════════════════════════════════════════════════════════════════
# 1. canonical_all_union/
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("1. canonical_all_union/")
print("=" * 60)

# events.csv — add legacy_hex_id column, remap event_id
events = load(UNION / "events.csv")
ev_fields = fields_of(UNION / "events.csv")
# Add legacy_hex_id after event_id if not already present
if "legacy_hex_id" not in ev_fields:
    idx = ev_fields.index("event_id") + 1
    ev_fields.insert(idx, "legacy_hex_id")
changed = 0
for e in events:
    old = e.get("event_id", "")
    if old in ALL_HEX:
        e["legacy_hex_id"] = old
        e["event_id"] = HEX_TO_SLUG[old]
        changed += 1
    else:
        e.setdefault("legacy_hex_id", "")
print(f"  events.csv: {changed} IDs remapped")
save(UNION / "events.csv", events, ev_fields)

# event_disciplines.csv
discs = load(UNION / "event_disciplines.csv")
d_fields = fields_of(UNION / "event_disciplines.csv")
n = remap_event_id_col(discs)
print(f"  event_disciplines.csv: {n} rows remapped")
save(UNION / "event_disciplines.csv", discs, d_fields)

# event_results.csv
results = load(UNION / "event_results.csv")
r_fields = fields_of(UNION / "event_results.csv")
n = remap_event_id_col(results)
print(f"  event_results.csv: {n} rows remapped")
save(UNION / "event_results.csv", results, r_fields)

# event_result_participants.csv
parts = load(UNION / "event_result_participants.csv")
p_fields = fields_of(UNION / "event_result_participants.csv")
n = remap_event_id_col(parts)
print(f"  event_result_participants.csv: {n} rows remapped")
save(UNION / "event_result_participants.csv", parts, p_fields)

print()


# ═══════════════════════════════════════════════════════════════════════════════
# 2. early_data/canonical/
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
    # Check actual column name — might be event_id not canonical_event_id
    if rows and id_col not in rows[0]:
        id_col = "event_id" if "event_id" in rows[0] else list(rows[0].keys())[0]

    # Add legacy_hex_id to events file
    if fname == "events_pre1997.csv" and "legacy_hex_id" not in fields:
        idx = fields.index(id_col) + 1
        fields.insert(idx, "legacy_hex_id")
        for e in rows:
            old = e.get(id_col, "")
            e["legacy_hex_id"] = old if old in ALL_HEX else ""

    n = remap_event_id_col(rows, id_col)
    print(f"  {fname}: {n} rows remapped (id_col={id_col})")
    save(path, rows, fields)

print()


# ═══════════════════════════════════════════════════════════════════════════════
# 3. early_data/out/ feeds
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
    n = remap_event_id_col(rows, "event_id")
    print(f"  {fname}: {n} rows remapped")
    save(path, rows, fields)

print()


# ═══════════════════════════════════════════════════════════════════════════════
# 4. out/merged_events_normalized.csv
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("4. out/merged_events_normalized.csv")
print("=" * 60)

path = MERGED_OUT / "merged_events_normalized.csv"
rows = load(path)
fields = fields_of(path)
changed = 0
for r in rows:
    old_lid = r.get("legacy_event_id", "")
    if old_lid in HEX_TO_SLUG:
        slug = HEX_TO_SLUG[old_lid]
        r["legacy_event_id"] = slug
        r["event_slug"] = slug
        r["event_key"] = slug          # add modern event_key for PRE1997
        changed += 1
    # For POST1997 events, event_key is already set properly
print(f"  {changed} PRE1997 event IDs updated to slugs")
save(path, rows, fields)

print()


# ═══════════════════════════════════════════════════════════════════════════════
# 5. out/merged_placements_flat.csv
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("5. out/merged_placements_flat.csv")
print("=" * 60)

path = MERGED_OUT / "merged_placements_flat.csv"
rows = load(path)
fields = fields_of(path)
n = remap_event_id_col(rows, "event_id")
print(f"  {n} rows remapped")
save(path, rows, fields)

print()


# ═══════════════════════════════════════════════════════════════════════════════
# 6. out/merged_stage2.csv
# ═══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("6. out/merged_stage2.csv")
print("=" * 60)

path = MERGED_OUT / "merged_stage2.csv"
rows = load(path)
fields = fields_of(path)
n = remap_event_id_col(rows, "event_id")
print(f"  {n} rows remapped")
save(path, rows, fields)

print()
print("Done. Now run:")
print("  python3 tools/build_appsafe_merged.py")
print("  python3 tools/build_merged_feeds.py")
print("  python3 tools/build_merged_workbook_v14.py")
print("  python3 tools/event_comparison_viewerV10.py --stage2 out/merged_stage2.csv --pf out/merged_placements_flat.csv --output out/merged_event_viewer.html")
