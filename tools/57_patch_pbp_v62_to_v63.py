#!/usr/bin/env python3
"""
tools/57_patch_pbp_v62_to_v63.py

Patch Placements_ByPerson v62 → v63.

Adds __NON_PERSON__ rows for every stage2 placement that is not yet
present in PBP v62, excluding quarantined events.

These are placements whose player tokens could not be identity-resolved
(single-name handles, partial doubles entries, etc.) and were silently
omitted from PBP.  Adding them as __NON_PERSON__ rows ensures PBP
coverage matches stage2 and resolves the PARTIAL coverage flags.

Usage:
    python3 tools/57_patch_pbp_v62_to_v63.py [--dry-run]

Outputs:
    inputs/identity_lock/Placements_ByPerson_v63.csv
    out/final_verification/pbp_v63_patch_report.csv
    out/final_verification/placement_restoration_summary.md
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

csv.field_size_limit(10 ** 7)

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parent.parent
PBP_V62  = REPO / "inputs" / "identity_lock" / "Placements_ByPerson_v62.csv"
PBP_V63  = REPO / "inputs" / "identity_lock" / "Placements_ByPerson_v63.csv"
STAGE2   = REPO / "out" / "stage2_canonical_events.csv"
QUARANTINE_CSV = REPO / "inputs" / "review_quarantine_events.csv"
REPORT_DIR = REPO / "out" / "final_verification"
REPORT_CSV = REPORT_DIR / "pbp_v63_patch_report.csv"
SUMMARY_MD = REPORT_DIR / "placement_restoration_summary.md"

# ── Normalisation ─────────────────────────────────────────────────────────────

def norm_div(s: str) -> str:
    s = str(s).lower().strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def divs_match(a: str, b: str) -> bool:
    na, nb = norm_div(a), norm_div(b)
    if na == nb:
        return True
    if na in nb or nb in na:
        return True
    return SequenceMatcher(None, na, nb).ratio() >= 0.85


# ── Derive division_category from division name ───────────────────────────────
_FREESTYLE_KW = re.compile(
    r"\b(freestyle|routines?|shred|circle|sick|battle|trick|combo|request|"
    r"ironman|consecutives?|overall|sick\s*\d|sick\s*trick)\b",
    re.IGNORECASE
)
_GOLF_KW = re.compile(r"\bgolf\b", re.IGNORECASE)
_SIDELINE_KW = re.compile(r"\bsideline\b", re.IGNORECASE)
_NET_KW = re.compile(r"\bnet\b", re.IGNORECASE)


def infer_div_category(div: str) -> str:
    if _FREESTYLE_KW.search(div):
        return "freestyle"
    if _GOLF_KW.search(div):
        return "golf"
    if _SIDELINE_KW.search(div):
        return "sideline"
    if _NET_KW.search(div):
        return "net"
    return "unknown"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Print summary only; do not write output files")
    args = parser.parse_args()

    # ── Load quarantine ───────────────────────────────────────────────────────
    quarantine_ids: set[int] = set()
    with open(QUARANTINE_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            quarantine_ids.add(int(row["event_id"]))
    print(f"Quarantined events: {len(quarantine_ids)}")

    # ── Load PBP v62 ──────────────────────────────────────────────────────────
    pbp = pd.read_csv(PBP_V62, dtype=str)
    pbp["place_int"] = pbp["place"].astype(int)
    pbp["event_id_int"] = pbp["event_id"].astype(int)
    print(f"PBP v62 rows: {len(pbp)}")

    # Build lookup structures
    # For singles: (event_id, div_norm, place) -> True
    # For teams:   (event_id, div_norm, place) -> True
    pbp_player_keys: set[tuple] = set()
    pbp_team_keys: set[tuple] = set()
    # event_id -> list of div_norm strings present in PBP
    pbp_divs_by_event: dict[int, set] = defaultdict(set)

    for _, row in pbp.iterrows():
        eid = int(row["event_id"])
        dn = norm_div(row["division_canon"])
        place = int(row["place"])
        ctype = row["competitor_type"]
        pbp_divs_by_event[eid].add(dn)
        if ctype == "player":
            pbp_player_keys.add((eid, dn, place))
        else:
            pbp_team_keys.add((eid, dn, place))

    def find_matching_pbp_div(event_id: int, stage2_div: str) -> str | None:
        """Return the PBP div_norm that best matches stage2_div, or None."""
        if event_id not in pbp_divs_by_event:
            return None
        sn = norm_div(stage2_div)
        # Exact
        if sn in pbp_divs_by_event[event_id]:
            return sn
        # Fuzzy (prefer containment, then score)
        best, best_score = None, 0.0
        for pbp_dn in pbp_divs_by_event[event_id]:
            if sn in pbp_dn or pbp_dn in sn:
                ratio = SequenceMatcher(None, sn, pbp_dn).ratio()
                if ratio > best_score:
                    best_score, best = ratio, pbp_dn
            else:
                ratio = SequenceMatcher(None, sn, pbp_dn).ratio()
                if ratio >= 0.85 and ratio > best_score:
                    best_score, best = ratio, pbp_dn
        return best

    def placement_in_pbp(event_id: int, div: str, place: int, ctype: str) -> bool:
        """Return True if this placement is already represented in PBP."""
        matched = find_matching_pbp_div(event_id, div)
        sn = norm_div(div)
        if ctype == "player":
            if matched and (event_id, matched, place) in pbp_player_keys:
                return True
            return (event_id, sn, place) in pbp_player_keys
        else:
            if matched and (event_id, matched, place) in pbp_team_keys:
                return True
            return (event_id, sn, place) in pbp_team_keys

    # ── Load stage2 ───────────────────────────────────────────────────────────
    stage2 = pd.read_csv(STAGE2, dtype=str)
    print(f"Stage2 events: {len(stage2)}")

    # ── Collect new rows ──────────────────────────────────────────────────────
    new_rows: list[dict] = []
    report_rows: list[dict] = []

    # Tracking by event+division
    additions_by_evdiv: dict[tuple, list] = defaultdict(list)
    events_skipped_quarantine = 0

    total_stage2 = 0
    covered = 0
    skipped_quarantine = 0

    for _, ev_row in stage2.iterrows():
        event_id = int(ev_row["event_id"])
        year = str(ev_row["year"])

        if event_id in quarantine_ids:
            skipped_quarantine += 1
            continue

        try:
            placements = json.loads(ev_row.get("placements_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            continue

        for p in placements:
            total_stage2 += 1
            div = str(p.get("division_canon", "") or "")
            div_raw = str(p.get("division_raw", "") or "")
            place = int(p.get("place", 0))
            ctype = str(p.get("competitor_type", "player"))
            player1 = str(p.get("player1_name", "") or "")
            player2 = str(p.get("player2_name", "") or "")
            div_cat = str(p.get("division_category", "") or "") or infer_div_category(div)

            if place <= 0:
                continue

            if placement_in_pbp(event_id, div, place, ctype):
                covered += 1
                continue

            # Determine if there's a matched PBP div (drift vs new)
            matched_div = find_matching_pbp_div(event_id, div)
            if matched_div:
                action = "DRIFT_SKIP" if placement_in_pbp(event_id, div, place, ctype) else "PARTIAL_RESTORE"
            else:
                action = "NEW_DIV_RESTORE"

            # Build the new row
            if ctype == "team":
                # Teams get TWO rows (one per person slot), matching v62 pattern
                team_key = _make_team_key(player1, player2, event_id, div, place)
                team_display = f"{player1} / {player2}" if player2 else player1

                for slot_name in (player1, player2) if player2 else (player1,):
                    new_row = _build_nonperson_row(
                        event_id=event_id,
                        year=year,
                        division_canon=div,
                        division_raw=div_raw,
                        division_category=div_cat,
                        place=place,
                        competitor_type=ctype,
                        person_canon=slot_name if slot_name else "__NON_PERSON__",
                        team_display_name=team_display,
                        team_person_key=team_key,
                    )
                    new_rows.append(new_row)
            else:
                new_row = _build_nonperson_row(
                    event_id=event_id,
                    year=year,
                    division_canon=div,
                    division_raw=div_raw,
                    division_category=div_cat,
                    place=place,
                    competitor_type=ctype,
                    person_canon=player1 if player1 else "__NON_PERSON__",
                    team_display_name="",
                    team_person_key="",
                )
                new_rows.append(new_row)

            additions_by_evdiv[(event_id, div)].append(place)

            report_rows.append({
                "event_id": event_id,
                "year": year,
                "division": div,
                "place": place,
                "competitor_type": ctype,
                "player_token": player1 + ("/" + player2 if player2 else ""),
                "action": action,
                "matched_pbp_div": matched_div or "",
            })

    print(f"\nStage2 placements (non-quarantined events): {total_stage2}")
    print(f"Already in PBP v62: {covered}")
    print(f"New rows to add: {len(new_rows)}")
    print(f"Unique event+division combos affected: {len(additions_by_evdiv)}")
    print(f"Quarantined events skipped: {skipped_quarantine}")

    if args.dry_run:
        print("\nDRY RUN — no files written.")
        _print_sample_additions(additions_by_evdiv, report_rows)
        return

    # ── Build v63 ─────────────────────────────────────────────────────────────
    # Append new rows to PBP v62 DataFrame
    new_df = pd.DataFrame(new_rows)

    # Align columns to PBP schema
    for col in pbp.columns:
        if col not in new_df.columns:
            new_df[col] = ""
    new_df = new_df[pbp.columns]  # same column order

    # Remove the helper column from pbp before concat
    pbp_clean = pbp.drop(columns=["place_int", "event_id_int"], errors="ignore")
    new_df_clean = new_df.drop(columns=["place_int", "event_id_int"], errors="ignore")

    v63 = pd.concat([pbp_clean, new_df_clean], ignore_index=True)

    # Deduplication: only deduplicate within the new rows we added, not within v62.
    # v62 intentionally has 2 identical rows per unresolved team (one slot per team member).
    # We must preserve those. Only check for accidental duplicates in new_df.
    before_dedup_new = len(new_df_clean)
    new_df_clean = new_df_clean.drop_duplicates(keep="first")
    after_dedup_new = len(new_df_clean)
    if before_dedup_new != after_dedup_new:
        print(f"Removed {before_dedup_new - after_dedup_new} duplicate rows from new additions")

    # Rebuild v63 with deduped new rows
    v63 = pd.concat([pbp_clean, new_df_clean], ignore_index=True)

    # Sort
    v63["_year_int"] = pd.to_numeric(v63["year"], errors="coerce").fillna(0).astype(int)
    v63["_place_int"] = pd.to_numeric(v63["place"], errors="coerce").fillna(0).astype(int)
    v63 = v63.sort_values(["_year_int", "event_id", "division_canon", "_place_int", "person_id"],
                          na_position="last").reset_index(drop=True)
    v63 = v63.drop(columns=["_year_int", "_place_int"])

    # Save v63
    v63.to_csv(PBP_V63, index=False)
    print(f"\nSaved: {PBP_V63}")
    print(f"  v62 rows: {len(pbp_clean)}")
    print(f"  new rows added: {len(new_df_clean)}")
    print(f"  v63 rows: {len(v63)}")

    # ── Save report ───────────────────────────────────────────────────────────
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(REPORT_CSV, index=False)
    print(f"Report: {REPORT_CSV}")

    # ── Summary markdown ──────────────────────────────────────────────────────
    _write_summary(
        additions_by_evdiv=additions_by_evdiv,
        report_rows=report_rows,
        pbp_v62_count=len(pbp_clean),
        pbp_v63_count=len(v63),
        new_rows_count=len(new_df_clean),
        stage2=stage2,
    )
    print(f"Summary: {SUMMARY_MD}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_team_key(p1: str, p2: str, event_id: int, div: str, place: int) -> str:
    """Short deterministic key for a team entry (matches v62 pattern)."""
    import hashlib
    raw = f"{event_id}|{div}|{place}|{p1}|{p2}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def _build_nonperson_row(
    event_id: int,
    year: str,
    division_canon: str,
    division_raw: str,
    division_category: str,
    place: int,
    competitor_type: str,
    person_canon: str,
    team_display_name: str,
    team_person_key: str,
) -> dict:
    norm = person_canon.lower().strip() if person_canon and person_canon != "__NON_PERSON__" else "nonperson"
    return {
        "event_id": str(event_id),
        "year": str(year),
        "division_canon": division_canon,
        "division_category": division_category,
        "place": str(place),
        "competitor_type": competitor_type,
        "person_id": "",
        "team_person_key": team_person_key,
        "person_canon": person_canon,
        "team_display_name": team_display_name,
        "coverage_flag": "unresolved",
        "person_unresolved": "1",
        "norm": norm,
        "division_raw": division_raw,
    }


def _print_sample_additions(additions_by_evdiv, report_rows):
    print("\nSample additions (first 20 event+division combos):")
    for i, ((eid, div), places) in enumerate(sorted(additions_by_evdiv.items())[:20]):
        print(f"  Event {eid} | {div} | places: {sorted(places)}")


def _write_summary(
    additions_by_evdiv,
    report_rows,
    pbp_v62_count,
    pbp_v63_count,
    new_rows_count,
    stage2,
):
    # Load event names from stage2 for display
    event_names: dict[int, str] = {}
    for _, row in stage2.iterrows():
        eid = int(row["event_id"])
        event_names[eid] = str(row.get("event_name", "") or "")

    partial_rows = [r for r in report_rows if r["action"] == "PARTIAL_RESTORE"]
    new_div_rows = [r for r in report_rows if r["action"] == "NEW_DIV_RESTORE"]

    # Group by event+division
    partial_by_evdiv: dict[tuple, list] = defaultdict(list)
    for r in partial_rows:
        partial_by_evdiv[(r["event_id"], r["division"])].append(r["place"])
    new_div_by_evdiv: dict[tuple, list] = defaultdict(list)
    for r in new_div_rows:
        new_div_by_evdiv[(r["event_id"], r["division"])].append(r["place"])

    lines = [
        "## PBP v63 Patch Summary",
        "",
        f"PBP v62 rows: {pbp_v62_count}",
        f"New rows added: {new_rows_count}",
        f"PBP v63 rows: {pbp_v63_count}",
        f"Unique event+division combos restored: {len(additions_by_evdiv)}",
        "",
    ]

    lines.append(f"### PARTIAL restorations ({len(partial_by_evdiv)} event-divisions)")
    lines.append("")
    for (eid, div), places in sorted(partial_by_evdiv.items(), key=lambda x: (x[0][0], x[0][1])):
        name = event_names.get(int(eid), "")
        places_str = ", ".join(str(p) for p in sorted(set(places)))
        lines.append(f"- Event {eid} {name}: {div} — places {places_str} restored")
    lines.append("")

    lines.append(f"### NEW DIVISION restorations ({len(new_div_by_evdiv)} event-divisions)")
    lines.append("")
    for (eid, div), places in sorted(new_div_by_evdiv.items(), key=lambda x: (x[0][0], x[0][1])):
        name = event_names.get(int(eid), "")
        places_str = ", ".join(str(p) for p in sorted(set(places)))
        lines.append(f"- Event {eid} {name}: {div} — places {places_str} restored")
    lines.append("")

    lines.append("## Validation")
    lines.append("")
    lines.append(f"Total PF rows was (v62): {pbp_v62_count}")
    lines.append(f"Total PF rows now (v63): {pbp_v63_count}")
    lines.append(f"Net gain: +{pbp_v63_count - pbp_v62_count}")

    SUMMARY_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
