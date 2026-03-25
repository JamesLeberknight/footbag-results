#!/usr/bin/env python3
"""
build_merged_feeds.py

Produces merged input feeds for the v14 workbook builder and merged viewer:

  out/merged_events_normalized.csv     — events_normalized + PRE1997 events, minus suppressed stubs
  out/merged_placements_flat.csv       — Placements_Flat + pre-1997 placements, minus suppressed stubs
  out/merged_stage2.csv                — stage2_canonical_events + early_stage2_feed (for viewer)

The suppress set is derived from out/canonical_all_union/early_overlap_candidates.csv.
Matching uses exact name+year (fuzzy fallback ≥0.90) against inputs/events_normalized.csv.

All feeds use the same schemas as the originals so downstream tools need zero logic changes.
"""

import csv
import sys
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

csv.field_size_limit(sys.maxsize)
ROOT = Path(__file__).resolve().parent.parent

# ── Input paths ───────────────────────────────────────────────────────────────
OVERLAP_CSV       = ROOT / "out" / "canonical_all_union" / "early_overlap_candidates.csv"
EVENTS_NORM_CSV   = ROOT / "inputs" / "events_normalized.csv"
PF_CSV            = ROOT / "out" / "Placements_Flat.csv"
STAGE2_CSV        = ROOT / "out" / "stage2_canonical_events.csv"
EARLY_PF_CSV      = ROOT / "early_data" / "out" / "early_placements_feed.csv"
EARLY_STAGE2_CSV  = ROOT / "early_data" / "out" / "early_stage2_feed.csv"
CANON_ALL_EVENTS  = ROOT / "out" / "canonical_all_union" / "events.csv"
# Bridge table: legacy_event_id (numeric) → event_key (canonical slug)
CANON_BRIDGE_CSV  = ROOT / "out" / "canonical" / "events.csv"
# Authoritative event set: only these slugs appear in final outputs
CANON_ALL_APP_CSV = ROOT / "out" / "canonical_all" / "events.csv"

# ── Output paths ──────────────────────────────────────────────────────────────
OUT_EVENTS_NORM   = ROOT / "out" / "merged_events_normalized.csv"
OUT_PF            = ROOT / "out" / "merged_placements_flat.csv"
OUT_STAGE2        = ROOT / "out" / "merged_stage2.csv"


def load(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write(path, rows, fields=None):
    if not rows:
        path.write_text("")
        return
    fields = fields or list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  → {path.relative_to(ROOT)}  ({len(rows):,} rows)")

def sim(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


# ── 0. Build canonical slug lookup (legacy_event_id → canonical event_id) ─────
# out/canonical/events.csv is the bridge: event_key=canonical_slug, legacy_event_id=numeric
# Also apply cleanup_event_ids renames so the two systems stay in sync.
_ID_RENAMES = {
    # POST1997 slug cleanups (canonical/events.csv bridge may still have old slugs)
    "2001_worlds_san_francisco_bay_area":       "2001_worlds_san_francisco",
    "2010_naantalin_seudun_mestaruuskilpailut": "2010_naantali_mestaruuskilpailut",
    # PRE1997 worlds renames (rename_worlds_event_ids.py applied to source tables;
    # these guards handle any stragglers in the POST1997 bridge table)
    "1980_nhsa":        "1980_worlds",
    "1980_worlds":      "1980_worlds_clackamas",
    "1981_nhsa":        "1981_worlds",
    "1981_worlds":      "1981_worlds_portland",
    "1982_nhsa":        "1982_worlds",
    "1982_worlds":      "1982_worlds_portland",
    "1983_nhsa":        "1983_worlds_nhsa",
    "1983_wfa":         "1983_worlds_wfa",
    "1983_worlds":      "1983_worlds_portland",
    "1986_worlds_wfa":  "1986_worlds_golden",
    "1987_worlds_wfa":  "1987_worlds_golden",
    "1988_worlds_wfa":  "1988_worlds_golden",
    "1989_worlds_wfa":  "1989_worlds_golden",
    "1990_worlds_wfa":  "1990_worlds",
    "1991_worlds_wfa":  "1991_worlds",
    "1992_worlds_wfa":  "1992_worlds",
    "1993_worlds_ifab": "1993_worlds",
    "1994_worlds_ifab": "1994_worlds_palo_alto",
    "1995_worlds_ifab": "1995_worlds",
    "1996_worlds_ifab": "1996_worlds",
}

def _apply_rename(slug):
    return _ID_RENAMES.get(slug, slug)

LID_TO_SLUG = {}  # numeric legacy_event_id → canonical slug
if CANON_BRIDGE_CSV.exists():
    with open(CANON_BRIDGE_CSV, newline="", encoding="utf-8") as _f:
        for _r in csv.DictReader(_f):
            lid  = _r.get("legacy_event_id", "")
            slug = _apply_rename(_r.get("event_key", "") or lid)
            if lid:
                LID_TO_SLUG[lid] = slug
print(f"Loaded {len(LID_TO_SLUG)} canonical slug mappings from bridge table")

# Authoritative event ID set — only events present here appear in final outputs
CANONICAL_SLUG_SET = set()
if CANON_ALL_APP_CSV.exists():
    with open(CANON_ALL_APP_CSV, newline="", encoding="utf-8") as _f:
        for _r in csv.DictReader(_f):
            CANONICAL_SLUG_SET.add(_r["event_id"])
print(f"Loaded {len(CANONICAL_SLUG_SET)} authoritative event IDs from canonical_all")


# ── 1. Build suppress set ─────────────────────────────────────────────────────
print("Building suppress set from overlap candidates...")

overlap = load(OVERLAP_CSV)
suppressed_canon_ids = {
    r["post1997_event_key"]
    for r in overlap
    if r.get("resolution_action") == "SUPPRESS_POST1997_FROM_APP_VIEW"
}
print(f"  Suppressed canonical event IDs: {len(suppressed_canon_ids)}")

# Load canonical early events (POST1997, year < 1997) for name+year matching
canon_all = load(CANON_ALL_EVENTS)
canon_post_early = {
    e["event_id"]: e
    for e in canon_all
    if e.get("data_source") == "POST1997"
    and e.get("year") and int(e["year"]) < 1997
}

# Map events_normalized legacy_event_id → suppress flag via name+year similarity
events_norm = load(EVENTS_NORM_CSV)
norm_early = [r for r in events_norm if r.get("year") and int(r["year"]) < 1997]

suppressed_legacy_ids = set()
for nr in norm_early:
    yr = nr["year"]
    best_id, best_score = None, 0.0
    for sid, se in canon_post_early.items():
        if se["year"] != yr:
            continue
        score = sim(nr["event_name"], se["event_name"])
        if score > best_score:
            best_score = score
            best_id = sid
    if best_id in suppressed_canon_ids and best_score >= 0.85:
        suppressed_legacy_ids.add(nr["legacy_event_id"])

print(f"  Suppressed legacy_event_ids (events_normalized): {len(suppressed_legacy_ids)}")


# ── 2. Merged events_normalized ───────────────────────────────────────────────
print("\nBuilding merged_events_normalized.csv...")

# Keep post-1997 events + early events not suppressed
norm_filtered = [r for r in events_norm if r.get("legacy_event_id") not in suppressed_legacy_ids]
suppressed_norm_count = len(events_norm) - len(norm_filtered)
print(f"  events_normalized: {len(events_norm)} → {len(norm_filtered)} (-{suppressed_norm_count} suppressed)")

# Replace event_key with canonical slug for POST1997 events
slug_updated = 0
for r in norm_filtered:
    lid = r.get("legacy_event_id", "")
    if lid in LID_TO_SLUG:
        r["event_key"] = LID_TO_SLUG[lid]
        slug_updated += 1
print(f"  Updated {slug_updated} POST1997 event_key values to canonical slugs")

# Add PRE1997 events from canonical_all (hex IDs)
norm_fields = list(csv.DictReader(open(EVENTS_NORM_CSV)).fieldnames)
pre97_events_added = 0
extra_norm_rows = []
for e in canon_all:
    if e.get("data_source") != "PRE1997":
        continue
    # Map canonical_all fields → events_normalized schema
    row = {f: "" for f in norm_fields}
    row["event_key"]        = e["event_id"]          # slug ID (modern)
    row["legacy_event_id"]  = e["event_id"]          # slug ID (same for PRE1997)
    row["year"]             = e.get("year", "")
    row["event_name"]       = e.get("event_name", "")
    row["event_slug"]       = e.get("event_id", "")
    row["start_date"]       = e.get("start_date", "")
    row["end_date"]         = e.get("end_date", "")
    row["city"]             = e.get("city", "")
    row["region"]           = e.get("region", "")
    row["country"]          = e.get("country", "")
    row["host_club"]        = e.get("host_club", "")
    row["event_type"]       = e.get("event_type", "")
    row["status"]           = e.get("status", "historical")
    row["source"]           = "PRE1997"
    extra_norm_rows.append(row)
    pre97_events_added += 1

print(f"  Added {pre97_events_added} PRE1997 canonical events")
merged_norm = norm_filtered + extra_norm_rows
write(OUT_EVENTS_NORM, merged_norm, norm_fields)


# ── 3. Merged Placements_Flat ─────────────────────────────────────────────────
print("\nBuilding merged_placements_flat.csv...")

pf = load(PF_CSV)
pf_fields = list(csv.DictReader(open(PF_CSV)).fieldnames)

# Remove PF rows for suppressed legacy event IDs; also remove rows for events not in canonical_all
pf_not_canonical = 0
pf_filtered = []
for r in pf:
    if r.get("event_id") in suppressed_legacy_ids:
        continue
    slug = LID_TO_SLUG.get(r["event_id"], r["event_id"])
    if CANONICAL_SLUG_SET and slug not in CANONICAL_SLUG_SET:
        pf_not_canonical += 1
        continue
    pf_filtered.append(r)  # keep numeric event_id for viewer join
suppressed_pf = len(pf) - len(pf_filtered)
print(f"  PF rows: {len(pf):,} → {len(pf_filtered):,} (-{suppressed_pf:,} removed)")

# Add PRE1997 placements from early_placements_feed (already PF-schema-compatible)
early_pf = load(EARLY_PF_CSV)
# Verify fields match; early_pf schema mirrors PF
early_pf_fields = list(csv.DictReader(open(EARLY_PF_CSV)).fieldnames)
missing = [f for f in pf_fields if f not in early_pf_fields]
extra   = [f for f in early_pf_fields if f not in pf_fields]
if missing:
    print(f"  WARNING: early_pf missing PF fields: {missing}")
if extra:
    print(f"  WARNING: early_pf has extra fields not in PF: {extra}")

print(f"  Adding {len(early_pf):,} pre-1997 placements")
merged_pf = pf_filtered + early_pf
write(OUT_PF, merged_pf, pf_fields)


# ── 4. Merged stage2 (for viewer) ─────────────────────────────────────────────
print("\nBuilding merged_stage2.csv...")

stage2 = load(STAGE2_CSV)
stage2_fields = list(csv.DictReader(open(STAGE2_CSV)).fieldnames)
# Add event_key column to carry modern readable ID
if "event_key" not in stage2_fields:
    stage2_fields = ["event_key"] + stage2_fields

# Build event_key lookup: numeric legacy_event_id → canonical slug
# Use the bridge table (LID_TO_SLUG) as primary; fall back to merged_events_normalized
norm_key_lookup = dict(LID_TO_SLUG)  # numeric → slug
for r in load(OUT_EVENTS_NORM):
    lid = r.get("legacy_event_id", "")
    if lid and lid not in norm_key_lookup:
        ek = r.get("event_key", "") or lid
        norm_key_lookup[lid] = ek

# Filter stage2: remove suppressed early event IDs; remove events not in canonical_all
stage2_filtered = []
stage2_not_canonical = 0
for r in stage2:
    if r.get("event_id") in suppressed_legacy_ids:
        continue
    eid  = r.get("event_id", "")
    slug = norm_key_lookup.get(eid, eid)
    # Suppress POST1997 events whose canonical slug is in suppressed_canon_ids —
    # these are covered by PRE1997 equivalents added from early_s2.
    if slug in suppressed_canon_ids:
        continue
    # Only include events present in canonical_all (authoritative set)
    if CANONICAL_SLUG_SET and slug not in CANONICAL_SLUG_SET:
        stage2_not_canonical += 1
        continue
    r["event_key"] = slug
    stage2_filtered.append(r)
n_removed = len(stage2) - len(stage2_filtered)
print(f"  stage2: {len(stage2):,} → {len(stage2_filtered):,} (-{n_removed} removed: "
      f"{n_removed - stage2_not_canonical} suppressed, {stage2_not_canonical} not in canonical_all)")

# Add early stage2 feed rows
early_s2 = load(EARLY_STAGE2_CSV)
early_s2_fields = list(csv.DictReader(open(EARLY_STAGE2_CSV)).fieldnames)
missing_s2 = [f for f in stage2_fields if f not in early_s2_fields]
if missing_s2:
    # Pad missing fields with empty strings; for early events event_key = event_id (slug)
    for row in early_s2:
        for f in missing_s2:
            row.setdefault(f, "")
        if "event_key" in missing_s2:
            row["event_key"] = row.get("event_id", "")

print(f"  Adding {len(early_s2)} pre-1997 stage2 rows")
merged_s2 = stage2_filtered + early_s2
write(OUT_STAGE2, merged_s2, stage2_fields)


# ── Summary ───────────────────────────────────────────────────────────────────
print("\nDone.")
print(f"  merged_events_normalized: {len(merged_norm):,} events "
      f"({pre97_events_added} PRE1997 + {len(norm_filtered)} POST1997)")
print(f"  merged_placements_flat:   {len(merged_pf):,} rows "
      f"({len(early_pf):,} PRE1997 + {len(pf_filtered):,} POST1997)")
print(f"  merged_stage2:            {len(merged_s2):,} events "
      f"({len(early_s2)} PRE1997 + {len(stage2_filtered):,} POST1997)")
