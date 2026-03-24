"""
12_build_enrichment_and_merged.py

PART 1 — Person enrichment
  Builds early_data/enrichment/person_enrichment.csv
  Matches pre-1997 persons → post-1997 persons.csv by person_id (exact)
  Pulls IFPA member_id, BAP, HOF data from the existing post-1997 table.
  The 14 PRE1997_ONLY persons have no post-1997 record and remain blank.
  Does NOT modify persons_pre1997.csv or Persons_Truth.csv.

PART 2 — Merged canonical dataset
  Builds out/canonical_all/ combining PRE-1997 and POST-1997.
  Unified schema uses a common field set; blanks where data doesn't apply.
  Adds data_source column (PRE1997 / POST1997) to every row.
  IDs are safe: pre-1997 uses 10-char hex slugs, post-1997 uses year_name slugs.
  No deduplication across datasets.

PART 3 — Validation
  Verifies referential integrity and row count consistency.
"""

import csv
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from datetime import date

ROOT     = Path(__file__).resolve().parents[2]
EARLY    = ROOT / "early_data"
FINAL    = EARLY / "final_pre1997"
ENRICH   = EARLY / "enrichment"
OUT_ALL  = ROOT / "out" / "canonical_all"

ENRICH.mkdir(exist_ok=True)
OUT_ALL.mkdir(exist_ok=True)

TODAY = date.today().isoformat()


# ── helpers ───────────────────────────────────────────────────────────────────

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows):6d} rows → {path.relative_to(ROOT)}")


# ── PART 1 — Person enrichment ────────────────────────────────────────────────

ENRICH_FIELDS = [
    "person_id", "person_canon", "source_scope",
    "ifpa_member_id",
    "bap_member", "bap_year", "bap_nickname",
    "fbhof_member", "fbhof_year",
    "source", "confidence", "notes",
]


def build_enrichment(pre_persons, post_persons_idx):
    """
    For each pre-1997 person, look up enrichment data from post-1997 persons.
    Match strategy: exact person_id lookup only (no name fuzzing).
    """
    rows = []
    matched = 0
    unmatched = []

    for p in pre_persons:
        pid = p["person_id"]
        post = post_persons_idx.get(pid)

        if post:
            # Direct match by person_id — highest confidence
            row = {
                "person_id":     pid,
                "person_canon":  p["person_canon"],
                "source_scope":  p["source_scope"],
                "ifpa_member_id": post.get("member_id", ""),
                "bap_member":    "Y" if post.get("bap_member") == "1" else "N",
                "bap_year":      post.get("bap_induction_year", ""),
                "bap_nickname":  post.get("bap_nickname", ""),
                "fbhof_member":  "Y" if post.get("fbhof_member") == "1" else "N",
                "fbhof_year":    post.get("fbhof_induction_year", ""),
                "source":        "persons.csv (post-1997 canonical)",
                "confidence":    "HIGH",
                "notes":         "",
            }
            matched += 1
        else:
            # PRE1997_ONLY person — no post-1997 record
            row = {
                "person_id":     pid,
                "person_canon":  p["person_canon"],
                "source_scope":  p["source_scope"],
                "ifpa_member_id": "",
                "bap_member":    "",
                "bap_year":      "",
                "bap_nickname":  "",
                "fbhof_member":  "",
                "fbhof_year":    "",
                "source":        "PRE1997_ONLY (no post-1997 record)",
                "confidence":    "N/A",
                "notes":         "Pre-1997 only player; not in post-1997 Persons_Truth",
            }
            unmatched.append(p["person_canon"])

        rows.append(row)

    return rows, matched, unmatched


# ── PART 2 — Merged canonical dataset ─────────────────────────────────────────

# ── 2a: events_all ────────────────────────────────────────────────────────────

EVENTS_ALL_FIELDS = [
    "event_id",           # canonical_event_id (pre) or event_key (post)
    "event_name",
    "year",
    "event_type",         # normalized_event_type (pre) or event_type (post)
    "location",           # location (pre) or city+region+country (post)
    "start_date",
    "end_date",
    "city",
    "region",
    "country",
    "host_club",
    "status",
    "validation_status",  # pre has this; post doesn't → "N/A" for post
    "num_placements",     # pre has this; derived from event_results for post
    "source_types",       # pre has this; "MIRROR" for post
    "data_source",        # PRE1997 / POST1997
]


def build_events_all(pre_events, post_events, post_results):
    """Merge event tables into unified events_all."""
    rows = []

    # Count post-1997 placements per event_key
    post_plc_count = Counter(r["event_key"] for r in post_results)

    # Pre-1997 events
    for e in pre_events:
        # Location: pre stores as single field
        loc = e.get("location", "")
        rows.append({
            "event_id":          e["canonical_event_id"],
            "event_name":        e["event_name"],
            "year":              e["year"],
            "event_type":        e["normalized_event_type"],
            "location":          loc,
            "start_date":        e.get("year", ""),   # only year-level for pre-1997
            "end_date":          "",
            "city":              "",
            "region":            "",
            "country":           "",
            "host_club":         "",
            "status":            "historical",
            "validation_status": e["validation_status"],
            "num_placements":    e["num_placements"],
            "source_types":      e["source_types"],
            "data_source":       "PRE1997",
        })

    # Post-1997 events
    for e in post_events:
        loc_parts = [e.get("city", ""), e.get("region", ""), e.get("country", "")]
        location  = ", ".join(p for p in loc_parts if p)
        rows.append({
            "event_id":          e["event_key"],
            "event_name":        e["event_name"],
            "year":              e["year"],
            "event_type":        e.get("event_type", ""),
            "location":          location,
            "start_date":        e.get("start_date", ""),
            "end_date":          e.get("end_date", ""),
            "city":              e.get("city", ""),
            "region":            e.get("region", ""),
            "country":           e.get("country", ""),
            "host_club":         e.get("host_club", ""),
            "status":            e.get("status", ""),
            "validation_status": "N/A",
            "num_placements":    str(post_plc_count.get(e["event_key"], 0)),
            "source_types":      "MIRROR",
            "data_source":       "POST1997",
        })

    return rows


# ── 2b: event_results_all ─────────────────────────────────────────────────────

RESULTS_ALL_FIELDS = [
    "event_id",
    "discipline",       # division_raw (pre) or discipline_key (post)
    "discipline_name",  # same as discipline for pre; discipline_name for post
    "placement",        # integer
    "player_raw",       # raw source name (pre only; blank for post)
    "team_raw",         # raw source team (pre only; blank for post)
    "score_text",       # post has this; blank for pre
    "source_type",      # FBW / OLD_RESULTS / IFAB (pre); MIRROR (post)
    "data_source",      # PRE1997 / POST1997
    "result_row_id",    # stable row ID within source (result_id for pre; key for post)
]


def build_results_all(pre_results, post_results, disc_name_idx):
    """Merge event_results tables."""
    rows = []

    # Pre-1997 results
    for r in pre_results:
        rows.append({
            "event_id":      r["canonical_event_id"],
            "discipline":    r["division_raw"],
            "discipline_name": r["division_raw"],
            "placement":     r["place"],
            "player_raw":    r.get("player_raw", ""),
            "team_raw":      r.get("team_raw", ""),
            "score_text":    "",
            "source_type":   r.get("source_type", ""),
            "data_source":   "PRE1997",
            "result_row_id": r["result_id"],
        })

    # Post-1997 results (event_key + discipline_key + placement = natural key)
    for r in post_results:
        disc_key  = r["discipline_key"]
        disc_name = disc_name_idx.get((r["event_key"], disc_key), disc_key)
        rows.append({
            "event_id":      r["event_key"],
            "discipline":    disc_key,
            "discipline_name": disc_name,
            "placement":     r["placement"],
            "player_raw":    "",
            "team_raw":      "",
            "score_text":    r.get("score_text", ""),
            "source_type":   "MIRROR",
            "data_source":   "POST1997",
            "result_row_id": f"{r['event_key']}|{disc_key}|{r['placement']}",
        })

    return rows


# ── 2c: event_result_participants_all ─────────────────────────────────────────

PARTICIPANTS_ALL_FIELDS = [
    "event_id",
    "discipline",
    "placement",
    "participant_order",   # post has this; sequential within placement for pre
    "display_name",        # person_canon (pre, resolved) or display_name (post)
    "player_name_raw",     # pre only; blank for post
    "person_id",
    "team_person_key",
    "resolution_status",   # pre has this; "POST1997_LOCKED" for post
    "data_source",
]


def build_participants_all(pre_parts, post_parts):
    """Merge participant tables."""
    rows = []

    # Pre-1997 participants
    # Group by (canonical_event_id, division_raw, place) to assign participant_order
    pre_ord: dict = defaultdict(int)
    for p in pre_parts:
        key = (p["canonical_event_id"], p["division_raw"], p["place"])
        pre_ord[key] += 1
        rows.append({
            "event_id":          p["canonical_event_id"],
            "discipline":        p["division_raw"],
            "placement":         p["place"],
            "participant_order": str(pre_ord[key]),
            "display_name":      p["person_canon"] or p["player_name_raw"],
            "player_name_raw":   p["player_name_raw"],
            "person_id":         p.get("person_id", ""),
            "team_person_key":   "",
            "resolution_status": p.get("resolution_status", ""),
            "data_source":       "PRE1997",
        })

    # Post-1997 participants
    for p in post_parts:
        rows.append({
            "event_id":          p["event_key"],
            "discipline":        p["discipline_key"],
            "placement":         p["placement"],
            "participant_order": p.get("participant_order", ""),
            "display_name":      p.get("display_name", ""),
            "player_name_raw":   "",
            "person_id":         p.get("person_id", ""),
            "team_person_key":   p.get("team_person_key", ""),
            "resolution_status": "POST1997_LOCKED",
            "data_source":       "POST1997",
        })

    return rows


# ── 2b2: event_disciplines_all ────────────────────────────────────────────────

DISCIPLINES_ALL_FIELDS = [
    "event_id",            # canonical_event_id (pre) or event_key (post)
    "discipline",          # division_raw (pre) or discipline_key (post)
    "discipline_name",     # same as discipline for pre; discipline_name for post
    "discipline_category", # blank for pre; from post
    "team_type",           # blank for pre; from post
    "sort_order",          # blank for pre; from post
    "coverage_flag",       # blank for pre; from post
    "total_placements",    # from pre's total_placements; derived from results for post
    "notes",               # blank for pre; from post
    "data_source",         # PRE1997 / POST1997
]


def build_disciplines_all(pre_discs, post_discs, post_results):
    """
    Merge event_disciplines tables into unified disciplines_all.

    Pre-1997 schema:  canonical_event_id, division_raw, total_placements
    Post-1997 schema: event_key, discipline_key, discipline_name,
                      discipline_category, team_type, sort_order,
                      coverage_flag, notes
    """
    rows = []

    # Pre-1997 disciplines — minimal schema, fill blanks for post-only fields
    for d in pre_discs:
        rows.append({
            "event_id":            d["canonical_event_id"],
            "discipline":          d["division_raw"],
            "discipline_name":     d["division_raw"],
            "discipline_category": "",
            "team_type":           "",
            "sort_order":          "",
            "coverage_flag":       "",
            "total_placements":    d["total_placements"],
            "notes":               "",
            "data_source":         "PRE1997",
        })

    # Derive placement counts per (event_key, discipline_key) from post-1997 results
    post_plc_count: Counter = Counter(
        (r["event_key"], r["discipline_key"]) for r in post_results
    )

    # Post-1997 disciplines
    for d in post_discs:
        key = (d["event_key"], d["discipline_key"])
        rows.append({
            "event_id":            d["event_key"],
            "discipline":          d["discipline_key"],
            "discipline_name":     d["discipline_name"],
            "discipline_category": d.get("discipline_category", ""),
            "team_type":           d.get("team_type", ""),
            "sort_order":          d.get("sort_order", ""),
            "coverage_flag":       d.get("coverage_flag", ""),
            "total_placements":    str(post_plc_count.get(key, 0)),
            "notes":               d.get("notes", ""),
            "data_source":         "POST1997",
        })

    return rows


# ── 2d: persons_all ───────────────────────────────────────────────────────────

PERSONS_ALL_FIELDS = [
    "person_id",
    "person_canon",
    "source_scope",       # PRE1997_ONLY / POST1997
    "ifpa_member_id",
    "bap_member",
    "bap_nickname",
    "bap_induction_year",
    "fbhof_member",
    "fbhof_induction_year",
    "first_year",
    "last_year",
    "country",
    "data_source",
]


def build_persons_all(pre_persons, post_persons, post_persons_idx):
    """
    Unified persons table.
    87 pre-1997 persons are also in post-1997 → include once with POST1997 data.
    14 PRE1997_ONLY persons have no post-1997 record → included with blank enrichment.
    All 3,468 post-1997 persons included.
    Persons appearing in both datasets: one row (post-1997 wins for enrichment fields).
    """
    rows = []
    pre_ids = {p["person_id"] for p in pre_persons}

    # All post-1997 persons
    for p in post_persons:
        scope = "PRE1997_AND_POST1997" if p["person_id"] in pre_ids else "POST1997"
        rows.append({
            "person_id":            p["person_id"],
            "person_canon":         p["person_name"],
            "source_scope":         scope,
            "ifpa_member_id":       p.get("member_id", ""),
            "bap_member":           "Y" if p.get("bap_member") == "1" else "N",
            "bap_nickname":         p.get("bap_nickname", ""),
            "bap_induction_year":   p.get("bap_induction_year", ""),
            "fbhof_member":         "Y" if p.get("fbhof_member") == "1" else "N",
            "fbhof_induction_year": p.get("fbhof_induction_year", ""),
            "first_year":           p.get("first_year", ""),
            "last_year":            p.get("last_year", ""),
            "country":              p.get("country", ""),
            "data_source":          "POST1997",
        })

    # PRE1997_ONLY persons (not in post-1997 at all)
    for p in pre_persons:
        if p["person_id"] not in post_persons_idx:
            rows.append({
                "person_id":            p["person_id"],
                "person_canon":         p["person_canon"],
                "source_scope":         "PRE1997_ONLY",
                "ifpa_member_id":       "",
                "bap_member":           "",
                "bap_nickname":         "",
                "bap_induction_year":   "",
                "fbhof_member":         "",
                "fbhof_induction_year": "",
                "first_year":           "",
                "last_year":            "",
                "country":              "",
                "data_source":          "PRE1997",
            })

    return rows


# ── PART 3 — Validation ───────────────────────────────────────────────────────

def validate_merged(events_all, results_all, parts_all, persons_all, discs_all):
    errors, warnings = [], []

    event_ids  = {r["event_id"] for r in events_all}
    person_ids = {r["person_id"] for r in persons_all}
    disc_keys  = {(r["event_id"], r["discipline"]) for r in discs_all}

    # Expected counts
    pre_ev  = sum(1 for r in events_all  if r["data_source"] == "PRE1997")
    post_ev = sum(1 for r in events_all  if r["data_source"] == "POST1997")
    pre_re  = sum(1 for r in results_all if r["data_source"] == "PRE1997")
    post_re = sum(1 for r in results_all if r["data_source"] == "POST1997")
    pre_pa  = sum(1 for r in parts_all   if r["data_source"] == "PRE1997")
    post_pa = sum(1 for r in parts_all   if r["data_source"] == "POST1997")

    # Referential integrity: all results → valid event_id
    orphan_results = {r["event_id"] for r in results_all if r["event_id"] not in event_ids}
    for eid in sorted(orphan_results):
        errors.append(f"result with unknown event_id: {eid}")

    # Referential integrity: all participants → valid event_id
    orphan_parts = {r["event_id"] for r in parts_all if r["event_id"] not in event_ids}
    for eid in sorted(orphan_parts):
        errors.append(f"participant with unknown event_id: {eid}")

    # Person ID integrity: participants with person_id not in persons_all
    orphan_pids = set()
    for r in parts_all:
        pid = r.get("person_id", "")
        if pid and pid not in person_ids:
            orphan_pids.add(pid)
    for pid in sorted(orphan_pids):
        warnings.append(f"participant person_id not in persons_all: {pid}")

    # Duplicate event_ids
    dup_events = {eid for eid, cnt in Counter(r["event_id"] for r in events_all).items() if cnt > 1}
    for eid in sorted(dup_events):
        errors.append(f"duplicate event_id: {eid}")

    # No ID collision between pre/post
    pre_event_ids  = {r["event_id"] for r in events_all if r["data_source"] == "PRE1997"}
    post_event_ids = {r["event_id"] for r in events_all if r["data_source"] == "POST1997"}
    collisions = pre_event_ids & post_event_ids
    for eid in sorted(collisions):
        errors.append(f"event_id collision between PRE1997 and POST1997: {eid}")

    # Discipline integrity: all result (event_id, discipline) pairs in discs_all
    missing_disc_keys: set = set()
    for r in results_all:
        key = (r["event_id"], r["discipline"])
        if key not in disc_keys:
            missing_disc_keys.add(key)
    for eid, disc in sorted(missing_disc_keys):
        warnings.append(f"result references discipline not in disciplines_all: {eid} / {disc}")

    # Duplicate discipline keys within same event+source
    disc_counter: Counter = Counter(
        (r["event_id"], r["discipline"]) for r in discs_all
    )
    for (eid, disc), cnt in disc_counter.items():
        if cnt > 1:
            errors.append(f"duplicate discipline key in disciplines_all: {eid} / {disc}")

    # Disciplines referencing unknown events
    orphan_discs = {r["event_id"] for r in discs_all if r["event_id"] not in event_ids}
    for eid in sorted(orphan_discs):
        errors.append(f"discipline with unknown event_id: {eid}")

    pre_disc  = sum(1 for r in discs_all if r["data_source"] == "PRE1997")
    post_disc = sum(1 for r in discs_all if r["data_source"] == "POST1997")

    return {
        "errors": errors,
        "warnings": warnings,
        "pre_events": pre_ev, "post_events": post_ev,
        "pre_results": pre_re, "post_results": post_re,
        "pre_parts": pre_pa, "post_parts": post_pa,
        "pre_discs": pre_disc, "post_discs": post_disc,
    }


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== 12_build_enrichment_and_merged.py ===\n")

    # Load pre-1997
    pre_events   = read_csv(FINAL / "events_pre1997.csv")
    pre_results  = read_csv(FINAL / "event_results_pre1997.csv")
    pre_parts    = read_csv(FINAL / "event_result_participants_pre1997.csv")
    pre_persons  = read_csv(FINAL / "persons_pre1997.csv")
    pre_discs    = read_csv(EARLY / "canonical" / "event_disciplines_pre1997.csv")

    # Load post-1997
    post_events  = read_csv(ROOT / "out/canonical/events.csv")
    post_results = read_csv(ROOT / "out/canonical/event_results.csv")
    post_parts   = read_csv(ROOT / "out/canonical/event_result_participants.csv")
    post_persons = read_csv(ROOT / "out/canonical/persons.csv")
    post_discs   = read_csv(ROOT / "out/canonical/event_disciplines.csv")

    print(f"Loaded:")
    print(f"  Pre-1997:  {len(pre_events)} events, {len(pre_discs)} disciplines, "
          f"{len(pre_results)} results, {len(pre_parts)} participants, {len(pre_persons)} persons")
    print(f"  Post-1997: {len(post_events)} events, {len(post_discs)} disciplines, "
          f"{len(post_results)} results, {len(post_parts)} participants, {len(post_persons)} persons")

    # Build indexes
    post_persons_idx = {p["person_id"]: p for p in post_persons}
    disc_name_idx    = {(d["event_key"], d["discipline_key"]): d["discipline_name"]
                        for d in post_discs}

    # ── Part 1: Enrichment ─────────────────────────────────────────────────
    print("\n--- PART 1: Build person enrichment ---")
    enrich_rows, matched, unmatched = build_enrichment(pre_persons, post_persons_idx)
    write_csv(ENRICH / "person_enrichment.csv", enrich_rows, ENRICH_FIELDS)
    print(f"  Matched (by person_id):     {matched} of {len(pre_persons)}")
    print(f"  Unmatched (PRE1997_ONLY):   {len(unmatched)}")
    for name in unmatched:
        print(f"    {name}")

    # Show enrichment highlights
    bap = [r for r in enrich_rows if r["bap_member"] == "Y"]
    hof = [r for r in enrich_rows if r["fbhof_member"] == "Y"]
    ifpa= [r for r in enrich_rows if r["ifpa_member_id"]]
    print(f"  BAP members in pre-1997:    {len(bap)}")
    for r in bap:
        print(f"    {r['person_canon']:30s} BAP {r['bap_year']} ({r['bap_nickname']})")
    print(f"  HOF members in pre-1997:    {len(hof)}")
    print(f"  IFPA IDs found:             {len(ifpa)}")

    # ── Part 2: Merged canonical ───────────────────────────────────────────
    print("\n--- PART 2: Build merged canonical dataset ---")

    events_all  = build_events_all(pre_events, post_events, post_results)
    discs_all   = build_disciplines_all(pre_discs, post_discs, post_results)
    results_all = build_results_all(pre_results, post_results, disc_name_idx)
    parts_all   = build_participants_all(pre_parts, post_parts)
    persons_all = build_persons_all(pre_persons, post_persons, post_persons_idx)

    write_csv(OUT_ALL / "events_all.csv",                    events_all,  EVENTS_ALL_FIELDS)
    write_csv(OUT_ALL / "event_disciplines_all.csv",         discs_all,   DISCIPLINES_ALL_FIELDS)
    write_csv(OUT_ALL / "event_results_all.csv",             results_all, RESULTS_ALL_FIELDS)
    write_csv(OUT_ALL / "event_result_participants_all.csv", parts_all,   PARTICIPANTS_ALL_FIELDS)
    write_csv(OUT_ALL / "persons_all.csv",                   persons_all, PERSONS_ALL_FIELDS)

    # ── Part 3: Validation ─────────────────────────────────────────────────
    print("\n--- PART 3: Validation ---")
    val = validate_merged(events_all, results_all, parts_all, persons_all, discs_all)

    for err in val["errors"]:
        print(f"  ERROR: {err}")
    for warn in val["warnings"]:
        print(f"  WARN:  {warn}")
    if not val["errors"] and not val["warnings"]:
        print("  All checks passed.")

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{'='*58}")
    print(f"MERGED CANONICAL SUMMARY")
    print(f"{'='*58}")
    print(f"\nEnrichment file:  early_data/enrichment/person_enrichment.csv")
    print(f"  Rows: {len(enrich_rows)} ({matched} with post-1997 data, {len(unmatched)} PRE1997_ONLY)")
    print(f"  BAP members: {len(bap)}, HOF members: {len(hof)}, IFPA IDs: {len(ifpa)}")

    print(f"\nMerged canonical: out/canonical_all/")
    print(f"  events_all.csv")
    print(f"    PRE1997:  {val['pre_events']:4d}  POST1997: {val['post_events']:4d}  "
          f"Total: {len(events_all)}")
    print(f"  event_disciplines_all.csv")
    print(f"    PRE1997:  {val['pre_discs']:4d}  POST1997: {val['post_discs']:4d}  "
          f"Total: {len(discs_all)}")
    print(f"  event_results_all.csv")
    print(f"    PRE1997:  {val['pre_results']:4d}  POST1997: {val['post_results']:4d}  "
          f"Total: {len(results_all)}")
    print(f"  event_result_participants_all.csv")
    print(f"    PRE1997:  {val['pre_parts']:4d}  POST1997: {val['post_parts']:4d}  "
          f"Total: {len(parts_all)}")
    print(f"  persons_all.csv")
    by_scope = Counter(r["source_scope"] for r in persons_all)
    for scope, cnt in sorted(by_scope.items()):
        print(f"    {scope:25s} {cnt}")
    print(f"    TOTAL: {len(persons_all)}")
    if val["errors"]:
        print(f"\n  {len(val['errors'])} ERROR(s) — review above")
    else:
        print(f"\n  Integrity: PASS (0 errors)")
    print(f"\nDone.")


if __name__ == "__main__":
    main()
