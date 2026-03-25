#!/usr/bin/env python3
"""
apply_pre1997_fixes.py

Applies two classes of fixes to out/canonical_all_union/:

1. DUPLICATE EVENT MERGES (same real-world event captured by multiple sources)
   - 069f7909e7 → bb975bff1d  (1980 Mike Marshall Memorial = 1980 NHSA)
   - 5c386b7dea → 8396ba09ac  (1980 World Footbag Champs = 1980 National Footbag Champs)
   - bc88796233 → b9540daaab  (1981 Mike Marshall Memorial = 1981 NHSA)

2. PERSON NAME CORRECTIONS
   - Steve Femmel → Steve Fennell
   - Sarah Femmel → Sarah Fennell

Operates directly on out/canonical_all_union/ CSV files.
Also updates early_data/canonical/ source files for consistency.

After running, rebuild downstream:
  python3 tools/build_appsafe_merged.py
  python3 tools/build_merged_feeds.py
"""

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UNION = ROOT / "out" / "canonical_all_union"
EARLY_CANON = ROOT / "early_data" / "canonical"

# ── Merge pairs: secondary → primary ─────────────────────────────────────────
# secondary: removed from events, its results/participants/disciplines discarded
# primary:   kept; metadata updated
MERGE_PAIRS = [
    {
        "secondary": "069f7909e7",
        "primary":   "bb975bff1d",
        "reason":    "Same event: 1980 Mike Marshall Memorial = 1980 NHSA Nationals",
        # Updates to apply to the primary event row
        "primary_updates": {
            "event_name":       "1980 NHSA (Mike Marshall Memorial)",
            "source_types":     "FBW|OLD_RESULTS",
            "validation_status": "CONFIRMED_MULTI_SOURCE",
        },
    },
    {
        "secondary": "5c386b7dea",
        "primary":   "8396ba09ac",
        "reason":    "Same event: 1980 World Footbag Champs (IFAB) = 1980 National Footbag Championships (FBW/NHSA)",
        "primary_updates": {
            "event_name":       "1980 National / World Footbag Championships",
            "event_type":       "WORLD_CHAMPIONSHIPS",
            "source_types":     "FBW|IFAB",
            "validation_status": "CONFIRMED_MULTI_SOURCE",
        },
    },
    {
        "secondary": "bc88796233",
        "primary":   "b9540daaab",
        "reason":    "Same event: 1981 Mike Marshall Memorial = 1981 NHSA Nationals (b9540 has more data)",
        "primary_updates": {
            "event_name":       "1981 NHSA (Mike Marshall Memorial)",
            "source_types":     "FBW|OLD_RESULTS",
            "validation_status": "CONFIRMED_MULTI_SOURCE",
        },
    },
]

# ── Person name corrections ───────────────────────────────────────────────────
PERSON_RENAMES = {
    "Steve Femmel": "Steve Fennell",
    "Sarah Femmel": "Sarah Fennell",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def load(path):
    if not path.exists():
        print(f"  WARNING: {path} not found, skipping")
        return []
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


# ── STEP 1: Apply event merges to canonical_all_union/ ────────────────────────

print("=" * 60)
print("STEP 1 — Event merges (canonical_all_union/)")
print("=" * 60)

secondary_ids = {p["secondary"] for p in MERGE_PAIRS}
primary_updates = {p["primary"]: p["primary_updates"] for p in MERGE_PAIRS}
merge_reason   = {p["secondary"]: p["reason"] for p in MERGE_PAIRS}

# 1a. events.csv
events = load(UNION / "events.csv")
ev_fields = fields_of(UNION / "events.csv")
removed_events = [e for e in events if e["event_id"] in secondary_ids]
kept_events = []
for e in events:
    if e["event_id"] in secondary_ids:
        continue
    if e["event_id"] in primary_updates:
        e.update(primary_updates[e["event_id"]])
    kept_events.append(e)

print(f"\nevents.csv: {len(events)} → {len(kept_events)} (-{len(removed_events)} merged)")
for e in removed_events:
    print(f"  removed {e['event_id']}  {e['event_name']}  ({merge_reason[e['event_id']]})")
save(UNION / "events.csv", kept_events, ev_fields)

# 1b. event_results.csv — drop secondary rows (primary already has the data)
results = load(UNION / "event_results.csv")
r_fields = fields_of(UNION / "event_results.csv")
kept_results = [r for r in results if r.get("event_id") not in secondary_ids]
print(f"\nevent_results.csv: {len(results)} → {len(kept_results)} (-{len(results)-len(kept_results)} secondary rows dropped)")
save(UNION / "event_results.csv", kept_results, r_fields)

# 1c. event_result_participants.csv — drop secondary rows
parts = load(UNION / "event_result_participants.csv")
p_fields = fields_of(UNION / "event_result_participants.csv")
kept_parts = [p for p in parts if p.get("event_id") not in secondary_ids]
print(f"\nevent_result_participants.csv: {len(parts)} → {len(kept_parts)} (-{len(parts)-len(kept_parts)} secondary rows dropped)")
save(UNION / "event_result_participants.csv", kept_parts, p_fields)

# 1d. event_disciplines.csv — drop secondary rows
discs = load(UNION / "event_disciplines.csv")
d_fields = fields_of(UNION / "event_disciplines.csv")
kept_discs = [d for d in discs if d.get("event_id") not in secondary_ids]
print(f"\nevent_disciplines.csv: {len(discs)} → {len(kept_discs)} (-{len(discs)-len(kept_discs)} secondary rows dropped)")
save(UNION / "event_disciplines.csv", kept_discs, d_fields)


# ── STEP 2: Person name corrections ───────────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 2 — Person name corrections (canonical_all_union/)")
print("=" * 60)

persons = load(UNION / "persons.csv")
pe_fields = fields_of(UNION / "persons.csv")
renamed_persons = 0
for p in persons:
    old = p["person_canon"]
    if old in PERSON_RENAMES:
        p["person_canon"] = PERSON_RENAMES[old]
        print(f"  persons: {old} → {p['person_canon']}  ({p['person_id']})")
        renamed_persons += 1
save(UNION / "persons.csv", persons, pe_fields)

# Also fix person_canon references in participants
fixed_parts = 0
for p in kept_parts:
    old = p.get("display_name", "")
    if old in PERSON_RENAMES:
        p["display_name"] = PERSON_RENAMES[old]
        fixed_parts += 1
    old2 = p.get("player_name_raw", "")
    if old2 in PERSON_RENAMES:
        p["player_name_raw"] = PERSON_RENAMES[old2]
if fixed_parts:
    print(f"  participants display_name: {fixed_parts} rows updated")
    save(UNION / "event_result_participants.csv", kept_parts, p_fields)

# Also fix player_raw in results
fixed_results = 0
for r in kept_results:
    old = r.get("player_raw", "")
    if old in PERSON_RENAMES:
        r["player_raw"] = PERSON_RENAMES[old]
        fixed_results += 1
    old2 = r.get("team_raw", "")
    if old2 in PERSON_RENAMES:
        r["team_raw"] = PERSON_RENAMES[old2]
if fixed_results:
    print(f"  results player_raw: {fixed_results} rows updated")
    save(UNION / "event_results.csv", kept_results, r_fields)


# ── STEP 3: Update early_data/canonical/ source files ────────────────────────

print("\n" + "=" * 60)
print("STEP 3 — Sync early_data/canonical/ source files")
print("=" * 60)

# 3a. events_pre1997.csv
ep = load(EARLY_CANON / "events_pre1997.csv")
if ep:
    ep_fields = fields_of(EARLY_CANON / "events_pre1997.csv")
    kept_ep = []
    for e in ep:
        eid = e.get("canonical_event_id", "")
        if eid in secondary_ids:
            print(f"  events_pre1997: removed {eid} {e.get('event_name','')}")
            continue
        # Apply name updates
        upd = primary_updates.get(eid, {})
        if "event_name" in upd:
            e["event_name"] = upd["event_name"]
        if "event_type" in upd or "normalized_event_type" in e:
            if "event_type" in upd and "normalized_event_type" in ep_fields:
                e["normalized_event_type"] = upd.get("event_type", e.get("normalized_event_type",""))
        if "source_types" in upd:
            e["source_types"] = upd["source_types"]
        if "validation_status" in upd:
            e["validation_status"] = upd["validation_status"]
        if "num_sources" in ep_fields:
            if eid in primary_updates:
                e["num_sources"] = "2"
        kept_ep.append(e)
    save(EARLY_CANON / "events_pre1997.csv", kept_ep, ep_fields)

# 3b. event_results_pre1997.csv — drop secondary event rows
er = load(EARLY_CANON / "event_results_pre1997.csv")
if er:
    er_fields = fields_of(EARLY_CANON / "event_results_pre1997.csv")
    # Find the event_id field name
    eid_field = "canonical_event_id" if "canonical_event_id" in er_fields else "event_id"
    kept_er = [r for r in er if r.get(eid_field) not in secondary_ids]
    dropped = len(er) - len(kept_er)
    print(f"  event_results_pre1997: {len(er)} → {len(kept_er)} (-{dropped})")
    save(EARLY_CANON / "event_results_pre1997.csv", kept_er, er_fields)

# 3c. event_result_participants_pre1997.csv
erp = load(EARLY_CANON / "event_result_participants_pre1997.csv")
if erp:
    erp_fields = fields_of(EARLY_CANON / "event_result_participants_pre1997.csv")
    eid_field = "canonical_event_id" if "canonical_event_id" in erp_fields else "event_id"
    kept_erp = [r for r in erp if r.get(eid_field) not in secondary_ids]
    # Apply name renames
    for r in kept_erp:
        for fn in ["display_name", "player_name_raw", "person_canon"]:
            if r.get(fn) in PERSON_RENAMES:
                r[fn] = PERSON_RENAMES[r[fn]]
    dropped = len(erp) - len(kept_erp)
    print(f"  event_result_participants_pre1997: {len(erp)} → {len(kept_erp)} (-{dropped})")
    save(EARLY_CANON / "event_result_participants_pre1997.csv", kept_erp, erp_fields)

# 3d. event_disciplines_pre1997.csv
edisc = load(EARLY_CANON / "event_disciplines_pre1997.csv")
if edisc:
    edisc_fields = fields_of(EARLY_CANON / "event_disciplines_pre1997.csv")
    eid_field = "canonical_event_id" if "canonical_event_id" in edisc_fields else "event_id"
    kept_edisc = [r for r in edisc if r.get(eid_field) not in secondary_ids]
    dropped = len(edisc) - len(kept_edisc)
    print(f"  event_disciplines_pre1997: {len(edisc)} → {len(kept_edisc)} (-{dropped})")
    save(EARLY_CANON / "event_disciplines_pre1997.csv", kept_edisc, edisc_fields)

# 3e. persons_pre1997.csv — rename Femmel → Fennell
ppr = load(EARLY_CANON / "persons_pre1997.csv")
if ppr:
    ppr_fields = fields_of(EARLY_CANON / "persons_pre1997.csv")
    for p in ppr:
        for fn in ["person_canon", "display_name", "name"]:
            if p.get(fn) in PERSON_RENAMES:
                old = p[fn]
                p[fn] = PERSON_RENAMES[old]
                print(f"  persons_pre1997: {old} → {p[fn]}")
    save(EARLY_CANON / "persons_pre1997.csv", ppr, ppr_fields)


# ── STEP 4: Update early_data/out/ feeds ─────────────────────────────────────

print("\n" + "=" * 60)
print("STEP 4 — Update early_data/out/ placement feed")
print("=" * 60)

EARLY_OUT = ROOT / "early_data" / "out"
epf = load(EARLY_OUT / "early_placements_feed.csv")
if epf:
    epf_fields = fields_of(EARLY_OUT / "early_placements_feed.csv")
    # Drop secondary event rows
    kept_epf = [r for r in epf if r.get("event_id") not in secondary_ids]
    # Rename Femmel → Fennell
    for r in kept_epf:
        for fn in ["person_canon", "team_display_name"]:
            if r.get(fn) in PERSON_RENAMES:
                r[fn] = PERSON_RENAMES[r[fn]]
            elif r.get(fn) and any(old in r[fn] for old in PERSON_RENAMES):
                for old, new in PERSON_RENAMES.items():
                    if old in r[fn]:
                        r[fn] = r[fn].replace(old, new)
    dropped = len(epf) - len(kept_epf)
    print(f"  early_placements_feed: {len(epf)} → {len(kept_epf)} (-{dropped} secondary rows)")
    save(EARLY_OUT / "early_placements_feed.csv", kept_epf, epf_fields)

# Also update early_stage2_feed.csv
es2 = load(EARLY_OUT / "early_stage2_feed.csv")
if es2:
    es2_fields = fields_of(EARLY_OUT / "early_stage2_feed.csv")
    kept_es2 = [r for r in es2 if r.get("event_id") not in secondary_ids]
    # Update event names in stage2 for primaries
    for r in kept_es2:
        eid = r.get("event_id","")
        upd = primary_updates.get(eid, {})
        if "event_name" in upd:
            r["event_name"] = upd["event_name"]
    dropped = len(es2) - len(kept_es2)
    print(f"  early_stage2_feed: {len(es2)} → {len(kept_es2)} (-{dropped} secondary rows)")
    save(EARLY_OUT / "early_stage2_feed.csv", kept_es2, es2_fields)


print("\nDone. Now run:")
print("  python3 tools/build_appsafe_merged.py")
print("  python3 tools/build_merged_feeds.py")
