"""
10_build_early_comparison_feed.py
Pre-1997 historical data → viewer-compatible feed files.

Produces two files that plug directly into the existing event comparison viewer
(tools/event_comparison_viewerV10.py) without any data being mixed into the
post-1997 published release:

  early_data/out/early_stage2_feed.csv
      One row per canonical pre-1997 event.
      Fields mirror out/stage2_canonical_events.csv:
        event_id, year, event_name, date, location, host_club,
        event_type, results_raw, placements_json, rejected_division_headers

      results_raw is synthesized from the raw source placements (FBW / IFAB /
      OLD_RESULTS), grouped by source type and division. The viewer parses this
      as the "left" (source) column.

  early_data/out/early_placements_feed.csv
      One row per canonical placement, identity-resolved where possible.
      Fields mirror out/Placements_Flat.csv:
        event_id, year, division_canon, division_category, place,
        competitor_type, person_id, team_person_key, person_canon,
        team_display_name, coverage_flag, person_unresolved, norm, division_raw

      The viewer uses this as the "right" (canonical) column.

Usage:
  python early_data/scripts/10_build_early_comparison_feed.py

Viewer usage (after running this script):
  python tools/event_comparison_viewerV10.py \\
      --stage2 early_data/out/early_stage2_feed.csv \\
      --pf     early_data/out/early_placements_feed.csv \\
      --output out/event_comparison_viewer_pre1997.html
"""

import csv
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT       = Path(__file__).resolve().parents[2]
EARLY      = ROOT / "early_data"
CANONICAL  = EARLY / "canonical"
PLACEMENTS = EARLY / "placements"
OLD        = EARLY / "old_results"
OUT        = EARLY / "out"

OUT.mkdir(exist_ok=True)

STAGE2_OUT = OUT / "early_stage2_feed.csv"
PF_OUT     = OUT / "early_placements_feed.csv"

# ── helpers ────────────────────────────────────────────────────────────────────

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows):4d} rows → {path.relative_to(ROOT)}")


def _norm_div(s: str) -> str:
    """Simple division normalizer for pre-1997 raw names."""
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _div_category(division_raw: str) -> str:
    """Rough division categorization for pre-1997 raw names."""
    n = division_raw.lower()
    if any(k in n for k in ("freestyle", "frstyl", "frestyle", "shred",
                             "sick", "circle", "routine", "battle", "combo")):
        return "freestyle"
    if "golf" in n:
        return "golf"
    if "sideline" in n:
        return "sideline"
    # "net" or bare Open/Mixed/Women's competition → net
    return "net"


def _player_display(person_canon: str, player_name_raw: str) -> str:
    """Return the best display name for a participant."""
    if person_canon and person_canon not in ("", "?"):
        return person_canon
    return player_name_raw or "?"


# ── results_raw synthesis ──────────────────────────────────────────────────────

def _format_player(player_raw: str, team_raw: str) -> str:
    """
    Produce a display string for a placement row from raw source data.
    Singles: player_raw
    Doubles: team_raw or player_raw (may contain '/')
    """
    if team_raw:
        raw = team_raw.replace("/", " / ")
        return raw.strip()
    if player_raw:
        # If player_raw already contains '/' it's an inline team
        raw = player_raw.replace("/", " / ")
        return raw.strip()
    return "?"


def build_results_raw(source_rows: list[dict]) -> str:
    """
    Build a results_raw text string from raw source placement rows.

    Groups by (source_type, source_file, division_raw) and emits:

        [FBW p012.jpg] Open Sgls Net
        1. Kenny Shults
        2. Walt Mason

        [OLD_RESULTS] Singles
        1. Ken Shults
        ...

    The viewer treats every non-placement line as a division header,
    and placement lines must match digit + optional punctuation + name.
    """
    if not source_rows:
        return ""

    # Group: source_type → source_file → division_raw → list of (place, display)
    groups: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for r in source_rows:
        place_num = r.get("placement_num", "").strip()
        if not place_num or not place_num.isdigit():
            continue
        player_disp = _format_player(r.get("player_raw", ""), r.get("team_raw", ""))
        src_type = r.get("source_type", r.get("source_file", "UNKNOWN")).split("/")[-1]
        src_file = Path(r.get("source_file", "")).name if r.get("source_file") else ""
        div_raw  = r.get("division_raw", "") or "(unspecified)"
        groups[src_type][src_file][div_raw].append((int(place_num), player_disp))

    lines = []
    for src_type, file_groups in sorted(groups.items()):
        for src_file, div_groups in sorted(file_groups.items()):
            src_label = f"{src_type}" + (f" [{src_file}]" if src_file and src_file != src_type else "")
            for div_raw, placements in sorted(div_groups.items()):
                # Division header — prefixed with source label
                lines.append(f"{src_label}: {div_raw}")
                for place, display in sorted(set(placements)):
                    lines.append(f"{place}. {display}")
                lines.append("")

    return "\n".join(lines).strip()


# ── Placements_Flat synthesis ──────────────────────────────────────────────────

PF_FIELDS = [
    "event_id", "year", "division_canon", "division_category", "place",
    "competitor_type", "person_id", "team_person_key", "person_canon",
    "team_display_name", "coverage_flag", "person_unresolved", "norm", "division_raw",
]


def build_pf_rows(canonical_event_id: str, year: str, validation_status: str,
                  participants: list[dict]) -> list[dict]:
    """
    Build Placements_Flat-compatible rows for one canonical event.

    Groups participants by (division_raw, place) to detect doubles teams.
    Singles (1 participant) → one row with person_canon set.
    Doubles (2 participants) → one row with person_canon='__NON_PERSON__'
                               and team_display_name='P1 / P2'.
    """
    # coverage_flag from validation_status
    coverage = "complete" if validation_status == "CONFIRMED_MULTI_SOURCE" else "partial"

    # Group by (division_raw, place)
    placement_groups: dict = defaultdict(list)
    for p in participants:
        key = (p["division_raw"], p["place"])
        placement_groups[key].append(p)

    rows = []
    for (div_raw, place_str), pparts in sorted(
            placement_groups.items(), key=lambda x: (x[0][0], int(x[0][1]) if x[0][1].isdigit() else 999)):

        place_int = int(place_str) if place_str.isdigit() else 0
        div_cat   = _div_category(div_raw)
        div_norm  = _norm_div(div_raw)

        if len(pparts) == 1:
            # Singles placement
            p = pparts[0]
            display = _player_display(p["person_canon"], p["player_name_raw"])
            unresolved = "0" if p["resolution_status"] in (
                "MATCHED", "AUTOACCEPTED", "ACCEPTED", "NEW_PLAYER") else "1"
            rows.append({
                "event_id":         canonical_event_id,
                "year":             year,
                "division_canon":   div_raw,
                "division_category": div_cat,
                "place":            place_int,
                "competitor_type":  "player",
                "person_id":        p["person_id"] or "",
                "team_person_key":  "",
                "person_canon":     display,
                "team_display_name": "",
                "coverage_flag":    coverage,
                "person_unresolved": unresolved,
                "norm":             div_norm,
                "division_raw":     div_raw,
            })

        else:
            # Doubles (or 3+) placement — emit as team row
            names = [_player_display(p["person_canon"], p["player_name_raw"])
                     for p in pparts]
            team_display = " / ".join(names)
            person_ids   = [p["person_id"] for p in pparts if p["person_id"]]
            team_key     = "|".join(person_ids) if person_ids else ""
            any_unresolved = any(
                p["resolution_status"] not in ("MATCHED", "AUTOACCEPTED", "ACCEPTED", "NEW_PLAYER")
                for p in pparts
            )
            rows.append({
                "event_id":         canonical_event_id,
                "year":             year,
                "division_canon":   div_raw,
                "division_category": div_cat,
                "place":            place_int,
                "competitor_type":  "team",
                "person_id":        "",
                "team_person_key":  team_key,
                "person_canon":     "__NON_PERSON__",
                "team_display_name": team_display,
                "coverage_flag":    coverage,
                "person_unresolved": "1" if any_unresolved else "0",
                "norm":             div_norm,
                "division_raw":     div_raw,
            })

    return rows


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    print("=== 10_build_early_comparison_feed.py ===\n")

    # Load canonical tables
    events       = read_csv(CANONICAL / "events_pre1997.csv")
    id_mapping   = read_csv(CANONICAL / "event_id_mapping.csv")
    participants = read_csv(CANONICAL / "event_result_participants_pre1997.csv")

    # Load ALL source placement files
    # (FBW + IFAB from placements/placements_flat.csv; OLD_RESULTS separate)
    fbw_placements = read_csv(PLACEMENTS / "placements_flat.csv")
    old_placements = read_csv(OLD / "old_results_placements_flat.csv")

    # Load source event_blocks to get source_type per source event_id
    fbw_blocks = read_csv(EARLY / "event_blocks" / "event_blocks.csv")
    old_blocks = read_csv(OLD / "old_results_event_blocks.csv")

    # Build: source_event_id → source_type
    src_type_map: dict[str, str] = {}
    for b in fbw_blocks + old_blocks:
        src_type_map[b["event_id"]] = b["source_type"]

    # Annotate source placements with source_type
    for row in fbw_placements:
        row["source_type"] = src_type_map.get(row["event_id"], "FBW")
    for row in old_placements:
        row["source_type"] = src_type_map.get(row["event_id"], "OLD_RESULTS")

    all_source_placements = fbw_placements + old_placements

    # Build: source_event_id → canonical_event_id
    src_to_canonical: dict[str, str] = {
        r["event_id"]: r["canonical_event_id"] for r in id_mapping
    }

    # Group source placements by canonical_event_id
    src_by_canonical: dict[str, list] = defaultdict(list)
    for row in all_source_placements:
        ceid = src_to_canonical.get(row["event_id"])
        if ceid:
            src_by_canonical[ceid].append(row)
        # Source placements not in id_mapping are out-of-scope (year≥1997); skip silently.

    # Group canonical participants by canonical_event_id
    parts_by_canonical: dict[str, list] = defaultdict(list)
    for p in participants:
        parts_by_canonical[p["canonical_event_id"]].append(p)

    # Build event index
    event_idx = {e["canonical_event_id"]: e for e in events}

    # Generate feed rows
    stage2_rows = []
    pf_rows_all = []

    for event in sorted(events, key=lambda x: (x["year"], x["event_name"])):
        ceid = event["canonical_event_id"]
        year = event["year"]
        src_rows   = src_by_canonical.get(ceid, [])
        canon_parts = parts_by_canonical.get(ceid, [])

        # Build results_raw from source placements (the "mirror" equivalent)
        results_raw = build_results_raw(src_rows)

        # Build placements_json stub (viewer uses this only for metadata display)
        placements_json = json.dumps({
            "source_types": event["source_types"],
            "validation_status": event["validation_status"],
            "num_placements": event["num_placements"],
        })

        stage2_rows.append({
            "event_id":                  ceid,
            "year":                      year,
            "event_name":                event["event_name"],
            "date":                      year,
            "location":                  event.get("location", ""),
            "host_club":                 "",
            "event_type":                event["normalized_event_type"],
            "results_raw":               results_raw,
            "placements_json":           placements_json,
            "rejected_division_headers": "[]",
        })

        # Build Placements_Flat rows from resolved participants
        pf_rows = build_pf_rows(ceid, year, event["validation_status"], canon_parts)
        pf_rows_all.extend(pf_rows)

    # Write outputs
    print("Writing feed files…")
    write_csv(STAGE2_OUT, stage2_rows, list(stage2_rows[0].keys()) if stage2_rows else [])
    write_csv(PF_OUT, pf_rows_all, PF_FIELDS)

    # Summary
    unmatched_src = sum(
        1 for row in all_source_placements
        if row["event_id"] not in src_to_canonical
    )
    print(f"\n{'='*52}")
    print(f"EARLY COMPARISON FEED SUMMARY")
    print(f"{'='*52}")
    print(f"  Canonical events:          {len(events)}")
    print(f"  Source placement rows:     {len(all_source_placements)}")
    print(f"    FBW/IFAB:                {len(fbw_placements)}")
    print(f"    OLD_RESULTS:             {len(old_placements)}")
    print(f"    Out-of-scope (skipped):  {unmatched_src}")
    print(f"  Stage2 feed rows:          {len(stage2_rows)}")
    print(f"  Placements feed rows:      {len(pf_rows_all)}")
    print(f"\nOutputs:")
    print(f"  {STAGE2_OUT.relative_to(ROOT)}")
    print(f"  {PF_OUT.relative_to(ROOT)}")
    print(f"\nRun the viewer with:")
    print(f"  python tools/event_comparison_viewerV10.py \\")
    print(f"      --stage2 {STAGE2_OUT.relative_to(ROOT)} \\")
    print(f"      --pf     {PF_OUT.relative_to(ROOT)} \\")
    print(f"      --output out/event_comparison_viewer_pre1997.html")
    print(f"\nDone.")


if __name__ == "__main__":
    main()
