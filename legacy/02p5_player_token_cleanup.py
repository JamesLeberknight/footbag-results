#!/usr/bin/env python3
"""
02p5_player_token_cleanup.py

Stage 02.5 — Player token cleanup and Placements_Flat generation.

v1.0 ADDITION:
----------------
Identity-Lock Release Mode.

When --identity_lock_placements_csv is provided, this script:
- DOES NOT perform heuristic identity resolution
- DOES NOT use alias logic
- DOES NOT modify identity
- Generates Placements_Flat.csv directly from authoritative placements
- Preserves all rows (no silent drops)

Derive mode (optional):
- When --stage2_events_csv and --persons_truth_csv are provided (and no PBP CSV),
  Placements_ByPerson is derived from stage2 placements + Persons_Truth.
- PT is ground truth: player_id in PT maps via player_ids_seen to person_id/person_canon.
- New data: player_ids not in PT are emitted as unresolved (person_unresolved=true,
  person_canon=display name) so they can be reviewed and later added to PT; no silent drop.

Append mode (optional):
- When --append is used with --identity_lock_placements_csv and (stage2 + persons_truth),
  locked PBP is preserved and only placements for events not in the lock are derived
  from stage2+PT and appended. Protects the archive while allowing new events and new people.

This satisfies the v1.0 canonical contract.
"""

import argparse
import json
import os
import re
import sys
import pandas as pd


def _norm_eid(v) -> str:
    """Normalize event_id to canonical string (handles int/float from CSV)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    try:
        return str(int(float(v)))
    except (ValueError, TypeError):
        return str(v).strip()


def _build_player_to_person_map(pt_df: pd.DataFrame) -> dict:
    """Build player_id -> (person_id, person_canon) from Persons_Truth.
    PT has effective_person_id, person_canon, and player_ids_seen (pipe-separated player_ids).
    """
    out = {}
    for _, row in pt_df.iterrows():
        person_id = str(row.get("effective_person_id", "")).strip()
        person_canon = str(row.get("person_canon", "")).strip()
        if not person_id or not person_canon:
            continue
        raw = row.get("player_ids_seen") or ""
        for pid in re.split(r"\s*\|\s*", raw):
            pid = pid.strip()
            if pid:
                out[pid] = (person_id, person_canon)
    return out


def _derive_placements_from_stage2_and_pt(args, only_event_ids=None, exclude_event_ids=None) -> pd.DataFrame:
    """Flatten stage2 placements and resolve player_id -> person_id via Persons_Truth.
    If only_event_ids is set, only include those event_ids.
    If exclude_event_ids is set, exclude those event_ids (e.g. events already in lock).
    """
    events_df = pd.read_csv(args.stage2_events_csv, dtype=str)
    pt_df = pd.read_csv(
        args.persons_truth_csv,
        dtype=str,
        usecols=["effective_person_id", "person_canon", "player_ids_seen"],
    )
    player_to_person = _build_player_to_person_map(pt_df)

    required_event_cols = ["event_id", "year", "placements_json"]
    missing = [c for c in required_event_cols if c not in events_df.columns]
    if missing:
        raise RuntimeError(f"Stage2 events missing columns: {missing}")

    if only_event_ids is not None:
        allowed_events = set(_norm_eid(e) for e in only_event_ids)
    elif exclude_event_ids is not None:
        # exclude_event_ids is already normalized (from append: locked_event_ids)
        allowed_events = set(_norm_eid(ev.get("event_id")) for _, ev in events_df.iterrows()) - set(str(e).strip() for e in exclude_event_ids)
    else:
        allowed_events = None

    rows = []
    unmapped_count = 0
    for _, ev in events_df.iterrows():
        event_id = str(ev.get("event_id", "")).strip()
        eid_canon = _norm_eid(event_id)
        if allowed_events is not None and eid_canon not in allowed_events:
            continue
        year = str(ev.get("year", "")).strip()
        try:
            placements = json.loads(ev.get("placements_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            placements = []
        for p in placements:
            div_canon = str(p.get("division_canon") or "").strip() or "Unknown"
            div_cat = str(p.get("division_category") or "").strip() or "unknown"
            place = p.get("place")
            try:
                place = int(place)
            except (TypeError, ValueError):
                place = 0
            comp_type = str(p.get("competitor_type") or "player").strip().lower()
            if comp_type not in ("player", "team"):
                comp_type = "player"

            person_id = ""
            team_person_key = ""
            person_canon = ""
            team_display_name = ""

            person_unresolved = ""
            if comp_type == "player":
                pid1 = str(p.get("player1_id") or "").strip()
                display_name = str(p.get("player1_name") or "").strip()
                if pid1 and pid1 in player_to_person:
                    person_id, person_canon = player_to_person[pid1]
                else:
                    unmapped_count += 1
                    # New/unmapped person: keep as unresolved so they can be added to PT later
                    person_canon = display_name or "__NON_PERSON__"
                    person_unresolved = "true" if display_name else ""
                team_display_name = ""
            else:
                pid1 = str(p.get("player1_id") or "").strip()
                pid2 = str(p.get("player2_id") or "").strip()
                pids = []
                if pid1 and pid1 in player_to_person:
                    pids.append(player_to_person[pid1][0])
                else:
                    unmapped_count += 1
                if pid2 and pid2 in player_to_person:
                    pids.append(player_to_person[pid2][0])
                else:
                    unmapped_count += 1
                team_person_key = "|".join(pids) if pids else ""
                person_canon = "__NON_PERSON__"
                n1 = str(p.get("player1_name") or "").strip()
                n2 = str(p.get("player2_name") or "").strip()
                team_display_name = " / ".join(filter(None, [n1, n2]))

            norm = re.sub(r"\s+", " ", (person_canon or "").lower().strip()) if person_canon else ""
            rows.append({
                "event_id": event_id,
                "year": year,
                "division_canon": div_canon,
                "division_category": div_cat,
                "place": place,
                "competitor_type": comp_type,
                "person_id": person_id,
                "team_person_key": team_person_key,
                "person_canon": person_canon or "",
                "team_display_name": team_display_name or "",
                "coverage_flag": "mostly_complete",
                "person_unresolved": person_unresolved,
                "norm": norm,
            })

    if unmapped_count:
        print(f"[02p5] Derive: {unmapped_count} placement(s) had player_id not in PT (emitted as unresolved for review/append)")

    return pd.DataFrame(rows)


def build_from_derive(args):
    """Derive Placements_ByPerson from stage2 events + Persons_Truth, then normalize and write."""
    print("[02p5] Derive mode: building Placements_ByPerson from stage2 + Persons_Truth")
    print("[02p5] Inputs:")
    print(f"  CSV   {args.stage2_events_csv}  (has placements_json column)")
    print(f"  CSV   {args.persons_truth_csv}")
    print(f"  out_dir: {args.out_dir or 'out'}")

    df = _derive_placements_from_stage2_and_pt(args)
    return _normalize_and_write_placements(df, args)


def build_from_append(args):
    """Append new events to locked PBP: keep lock as-is, add only placements for events not in lock."""
    print("[02p5] Append mode: locked PBP + new events from stage2 + Persons_Truth")
    print("[02p5] Inputs:")
    print(f"  CSV   {args.identity_lock_placements_csv}")
    print(f"  CSV   {args.stage2_events_csv}  (has placements_json column)")
    print(f"  CSV   {args.persons_truth_csv}")
    print(f"  out_dir: {args.out_dir or 'out'}")

    lock_df = pd.read_csv(args.identity_lock_placements_csv, dtype=str)
    required = ["event_id", "division_canon", "place", "person_id", "person_canon"]
    missing = [c for c in required if c not in lock_df.columns]
    if missing:
        raise RuntimeError(f"Locked PBP missing columns: {missing}")

    # Normalize so "1314323534" and "1314323534.0" match; lock events are excluded from derive
    locked_event_ids = set(_norm_eid(x) for x in lock_df["event_id"] if _norm_eid(x))
    new_df = _derive_placements_from_stage2_and_pt(args, exclude_event_ids=locked_event_ids)
    if new_df.empty:
        print("[02p5] No new events in stage2 beyond lock; output is lock only.")
        return _normalize_and_write_placements(lock_df.copy(), args)

    # Align columns: add any lock-only columns to new_df so concat preserves schema
    for c in lock_df.columns:
        if c not in new_df.columns:
            new_df[c] = ""
    combined = pd.concat([lock_df, new_df[lock_df.columns]], ignore_index=True)
    print(f"[02p5] Appended {len(new_df)} rows from {new_df['event_id'].nunique()} new event(s); total {len(combined)}")
    return _normalize_and_write_placements(combined, args)


def _print_outputs(out_dir: str, out_flat: str, out_by_person: str, row_count: int) -> None:
    """Print produced outputs summary."""
    print("[02p5] Outputs:")
    print(f"  out_dir: {out_dir}")
    print(f"  CSV  {out_flat}")
    print(f"  CSV  {out_by_person}")
    print(f"  rows: {row_count}")


def _normalize_and_write_placements(df_flat: pd.DataFrame, args) -> int:
    """Shared: strip soft hyphen, apply PT canon override if provided, add division_raw, write."""
    df_flat = df_flat.copy()
    if "division_canon" in df_flat.columns:
        df_flat["division_canon"] = df_flat["division_canon"].str.replace("\u00ad", "", regex=False)

    if getattr(args, "persons_truth_csv", None):
        print(f"[02p5] Applying PT canon override from: {args.persons_truth_csv}")
        pt = pd.read_csv(args.persons_truth_csv, dtype=str, usecols=["effective_person_id", "person_canon"])
        pt = pt.dropna(subset=["effective_person_id", "person_canon"])
        pt_map = pt.set_index("effective_person_id")["person_canon"].to_dict()
        overrides = 0

        def _override_canon(row):
            nonlocal overrides
            pid = str(row["person_id"]).strip() if pd.notna(row.get("person_id")) else ""
            pt_canon = pt_map.get(pid)
            if pt_canon and str(row.get("person_canon", "")).strip() != pt_canon.strip():
                overrides += 1
                return pt_canon
            return row.get("person_canon", "")

        df_flat["person_canon"] = df_flat.apply(_override_canon, axis=1)
        print(f"[02p5] PT canon overrides applied: {overrides}")

    if "division_raw" not in df_flat.columns:
        df_flat["division_raw"] = df_flat["division_canon"]

    out_dir = args.out_dir or "out"
    os.makedirs(out_dir, exist_ok=True)
    out_flat = os.path.join(out_dir, "Placements_Flat.csv")
    out_by_person = os.path.join(out_dir, "Placements_ByPerson.csv")
    df_flat.to_csv(out_flat, index=False)
    df_flat.to_csv(out_by_person, index=False)
    _print_outputs(out_dir, out_flat, out_by_person, len(df_flat))
    return 0


def build_from_identity_lock(args):
    print("[02p5] Identity-lock mode ENABLED")
    print("[02p5] Inputs:")
    print(f"  CSV   {args.identity_lock_placements_csv}")
    if getattr(args, "persons_truth_csv", None):
        print(f"  CSV   {args.persons_truth_csv}")
    print(f"  stage2_events_csv: not used (lock only)")
    print(f"  out_dir: {args.out_dir or 'out'}")

    df = pd.read_csv(args.identity_lock_placements_csv)
    required_cols = [
        "event_id",
        "division_canon",
        "place",
        "person_id",
        "person_canon",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Identity-lock placements missing columns: {missing}")

    return _normalize_and_write_placements(df.copy(), args)


# Default paths when run from repo root (no arguments required for canonical lock run)
DEFAULT_IDENTITY_LOCK_PBP = "inputs/identity_lock/Placements_ByPerson_v33.csv"
DEFAULT_PERSONS_TRUTH = "inputs/identity_lock/Persons_Truth_Final_v31.csv"
DEFAULT_STAGE2_EVENTS = "out/stage2_canonical_events.csv"


def main():
    parser = argparse.ArgumentParser(
        description="Stage 02.5 — Player token cleanup and Placements_Flat/Placements_ByPerson generation.",
        epilog="""
Modes:
  Default (no args)     Use locked PBP + PT. Reproduces canonical output.
  --append              Add new events: keep locked data, derive placements only for
                        events not in the lock (e.g. after importing new data or running
                        stage2 on new mirror). Use when you have new events/people to add.
  Derive (no lock)      Pass --identity_lock_placements_csv "" to build PBP from
                        stage2 + PT only (e.g. testing or full rebuild).
"""
    )

    parser.add_argument("--identity_lock_placements_csv", default=DEFAULT_IDENTITY_LOCK_PBP,
                        help=f"Authoritative Placements_ByPerson CSV (default: {DEFAULT_IDENTITY_LOCK_PBP})")
    parser.add_argument("--stage2_events_csv", default=None,
                        help=f"Stage2 canonical events CSV (default when using derive/append: {DEFAULT_STAGE2_EVENTS}). Use with --persons_truth_csv to derive PBP.")
    parser.add_argument("--persons_truth_csv", default=DEFAULT_PERSONS_TRUTH,
                        help=f"Persons_Truth CSV for canon override / derive (default: {DEFAULT_PERSONS_TRUTH})")
    parser.add_argument("--out_dir", default="out")
    parser.add_argument("--append", action="store_true",
                        help="Add new events on top of the lock. Use when you have new data (more events or new people); lock is preserved, only new events are derived from stage2+PT.")

    args, _ = parser.parse_known_args()

    # When using derive or append without explicit stage2 path, use default
    if not args.stage2_events_csv and (getattr(args, "append", False) or not args.identity_lock_placements_csv):
        args.stage2_events_csv = DEFAULT_STAGE2_EVENTS

    if getattr(args, "append", False) and args.identity_lock_placements_csv and args.stage2_events_csv and args.persons_truth_csv:
        return build_from_append(args)
    if args.identity_lock_placements_csv:
        return build_from_identity_lock(args)
    if args.stage2_events_csv and args.persons_truth_csv:
        return build_from_derive(args)

    print("ERROR: Provide --identity_lock_placements_csv OR (--stage2_events_csv + --persons_truth_csv). For append: --append with all three.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
