#!/usr/bin/env python3
"""
05_disambiguate_persons.py — Gate 5: Resolve multi-ID collisions.

Mode 1: --generate
  Reads  out/qc/qc02_canon_multiple_person_ids_candidates.csv
  Reads  out/Placements_Flat.csv
  Writes out/collision_review.csv  (human fills in 'decision' and 'merge_target_id')

  Columns in collision_review.csv:
    person_canon        — the ambiguous canon name
    person_id           — one of the conflicting UUIDs
    appearances         — placements count for this person_id
    appearances_all_ids — pipe-separated count per ID for easy visual comparison
    auto_confidence     — 'high', 'medium', or 'needs_review'
    years               — distinct years (pipe-separated)
    events              — distinct event_ids (pipe-separated, first 5)
    divisions           — distinct division_canons (pipe-separated)
    decision            — pre-filled 'merge'; HUMAN: change to 'separate' if needed
    merge_target_id     — pre-filled with highest-appearance ID; HUMAN: override if needed
    notes               — HUMAN: optional notes

Mode 2: --apply
  Reads  out/collision_review.csv (human-filled)
  Updates overrides/person_aliases.csv with merge decisions
  (adds alias rows for each losing UUID's player_names_seen → canonical UUID)

Usage:
  python 05_disambiguate_persons.py --generate
  # ... human reviews needs_review rows in out/collision_review.csv ...
  python 05_disambiguate_persons.py --apply
  python 04_build_analytics.py      # rebuild Persons_Truth
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "out"
QC_DIR = OUT / "qc"
OVERRIDES = ROOT / "overrides"

CANDIDATES_CSV = QC_DIR / "qc02_canon_multiple_person_ids_candidates.csv"  # informational only
PLACEMENTS_CSV = OUT / "Placements_Flat.csv"
COLLISION_REVIEW_CSV = OUT / "collision_review.csv"
ALIASES_CSV = OVERRIDES / "person_aliases.csv"
PERSONS_FULL_CSV = OUT / "Persons_Truth_Full.csv"


# ---------------------------------------------------------------------------
# --generate mode
# ---------------------------------------------------------------------------

def generate_review() -> int:
    """Produce collision_review.csv with auto-suggestions for human review.

    Scans Placements_Flat directly to find all person_canons that have 2+ distinct
    person_ids. Does NOT rely on the (potentially stale) qc02 candidates file.
    """
    if not PLACEMENTS_CSV.exists():
        print(f"ERROR: missing {PLACEMENTS_CSV}", file=sys.stderr)
        return 2

    pf = pd.read_csv(PLACEMENTS_CSV, dtype=str).fillna("")
    print(f"Loaded {len(pf)} placements from {PLACEMENTS_CSV}")

    # Full ID discovery: scan Placements_Flat to find EVERY person_id per canon.
    canon_to_pids: dict[str, set[str]] = {}  # person_canon -> set of person_ids
    person_appearances_parts: list[pd.DataFrame] = []

    for side in ["player1", "player2"]:
        pid_col = f"{side}_person_id"
        canon_col = f"{side}_person_canon"
        if pid_col not in pf.columns or canon_col not in pf.columns:
            continue
        sub = pf[pf[pid_col].str.strip().ne("") & pf[canon_col].str.strip().ne("")].copy()
        # Collect canon→pid mappings (vectorized for speed)
        for _, row in sub[[canon_col, pid_col]].drop_duplicates().iterrows():
            canon = row[canon_col].strip()
            pid = row[pid_col].strip()
            canon_to_pids.setdefault(canon, set()).add(pid)
        # Gather appearance evidence (all pids, filter later)
        app_sub = sub[[pid_col, "event_id", "year", "division_canon"]].rename(
            columns={pid_col: "person_id"}
        )
        person_appearances_parts.append(app_sub)

    if person_appearances_parts:
        all_app = pd.concat(person_appearances_parts, ignore_index=True)
    else:
        all_app = pd.DataFrame(columns=["person_id", "event_id", "year", "division_canon"])

    # Find collision groups: canons with 2+ distinct person_ids
    collision_groups = {c: pids for c, pids in canon_to_pids.items() if len(pids) >= 2}
    print(f"Found {len(collision_groups)} collision groups (same person_canon, 2+ person_ids)")

    if not collision_groups:
        print("No collisions found in Placements_Flat. Nothing to generate.")
        OUT.mkdir(exist_ok=True)
        pd.DataFrame(columns=[
            "person_canon", "person_id", "appearances", "appearances_all_ids", "auto_confidence",
            "years", "events", "divisions", "decision", "merge_target_id", "notes",
        ]).to_csv(COLLISION_REVIEW_CSV, index=False)
        print(f"Wrote empty: {COLLISION_REVIEW_CSV}")
        return 0

    # Build evidence per person_id (only for collision pids)
    all_collision_pids: set[str] = set()
    for pids in collision_groups.values():
        all_collision_pids.update(pids)

    def _agg_evidence(g: pd.DataFrame) -> pd.Series:
        years_sorted = sorted({str(v).strip() for v in g["year"] if str(v).strip()})
        events_sorted = sorted({str(v).strip() for v in g["event_id"] if str(v).strip()})
        divs_sorted = sorted({str(v).strip() for v in g["division_canon"] if str(v).strip()})
        return pd.Series({
            "appearances": len(g),
            "years": " | ".join(years_sorted),
            "events": " | ".join(events_sorted[:5]),
            "divisions": " | ".join(divs_sorted),
        })

    filtered_app = all_app[all_app["person_id"].str.strip().isin(all_collision_pids)].copy()
    if not filtered_app.empty:
        evidence = (
            filtered_app.groupby("person_id", dropna=False)
            .apply(_agg_evidence)
            .reset_index()
        )
    else:
        evidence = pd.DataFrame(columns=["person_id", "appearances", "years", "events", "divisions"])
    evidence["appearances"] = pd.to_numeric(evidence["appearances"], errors="coerce").fillna(0).astype(int)
    pid_appearances: dict[str, int] = dict(zip(evidence["person_id"], evidence["appearances"]))

    # Expand collision groups to one row per (canon, pid), with auto-suggestions
    rows: list[dict] = []
    for canon in sorted(collision_groups.keys()):
        pids = collision_groups[canon]
        app_by_pid = {pid: pid_appearances.get(pid, 0) for pid in pids}
        total_app = sum(app_by_pid.values())
        sorted_pids = sorted(pids, key=lambda p: app_by_pid[p], reverse=True)
        dominant_pid = sorted_pids[0]
        dominant_app = app_by_pid[dominant_pid]
        others_app = total_app - dominant_app

        # Auto-confidence: how sure are we this is the same person?
        if others_app == 0:
            auto_confidence = "high"   # all non-dominant are ghosts (0 appearances)
        elif dominant_app >= 5 * max(others_app, 1):
            auto_confidence = "high"
        elif dominant_app >= 2 * max(others_app, 1):
            auto_confidence = "medium"
        else:
            auto_confidence = "needs_review"   # comparable appearances → verify

        appearances_all_ids = " | ".join(str(app_by_pid[p]) for p in sorted_pids)

        for pid in sorted_pids:
            rows.append({
                "person_canon": canon,
                "person_id": pid,
                "appearances_all_ids": appearances_all_ids,
                "auto_confidence": auto_confidence,
                "decision": "merge",
                "merge_target_id": dominant_pid,
            })

    expanded = pd.DataFrame(rows)
    print(f"Expanded to {len(expanded)} (person_canon, person_id) pairs")

    # Join per-pid evidence
    result = expanded.merge(
        evidence[["person_id", "appearances", "years", "events", "divisions"]],
        on="person_id", how="left",
    )
    result["appearances"] = pd.to_numeric(result["appearances"], errors="coerce").fillna(0).astype(int)
    for col in ["years", "events", "divisions"]:
        result[col] = result[col].fillna("")
    result["notes"] = ""

    # Sort: needs_review first, then medium, then high; within group by appearances DESC
    confidence_order = {"needs_review": 0, "medium": 1, "high": 2}
    result["_conf_order"] = result["auto_confidence"].map(confidence_order).fillna(3)
    result = (
        result
        .sort_values(["_conf_order", "person_canon", "appearances"], ascending=[True, True, False])
        .drop(columns=["_conf_order"])
        .reset_index(drop=True)
    )

    out_cols = [
        "person_canon", "person_id", "appearances", "appearances_all_ids", "auto_confidence",
        "years", "events", "divisions",
        "decision", "merge_target_id", "notes",
    ]
    result = result[[c for c in out_cols if c in result.columns]]

    # Summary stats
    conf_counts = result.drop_duplicates("person_canon")["auto_confidence"].value_counts()
    print(f"  needs_review: {conf_counts.get('needs_review', 0)} groups")
    print(f"  medium:       {conf_counts.get('medium', 0)} groups")
    print(f"  high:         {conf_counts.get('high', 0)} groups")

    OUT.mkdir(exist_ok=True)
    result.to_csv(COLLISION_REVIEW_CSV, index=False)
    print(f"\nWrote: {COLLISION_REVIEW_CSV} ({len(result)} rows, {result['person_canon'].nunique()} groups)")
    print()
    print("Next steps:")
    print("  1. Open out/collision_review.csv")
    print("  2. Review 'needs_review' rows at the top — verify same/different person")
    print("     - Same person  → keep decision='merge', confirm merge_target_id")
    print("     - Different    → change decision='separate'")
    print("  3. 'medium' and 'high' rows can be applied without review.")
    print("  4. Run: python 05_disambiguate_persons.py --apply")
    return 0


# ---------------------------------------------------------------------------
# --apply mode
# ---------------------------------------------------------------------------

def apply_decisions() -> int:
    """Apply human decisions from collision_review.csv to person_aliases.csv."""
    alias_lock = OVERRIDES / "person_aliases.lock"
    if alias_lock.exists():
        print("ERROR: person_aliases.csv is frozen (overrides/person_aliases.lock exists).", file=sys.stderr)
        print("       Remove the lock file to add aliases.", file=sys.stderr)
        raise SystemExit(1)
    for p in [COLLISION_REVIEW_CSV, ALIASES_CSV]:
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr)
            if p == COLLISION_REVIEW_CSV:
                print("       Run --generate first.", file=sys.stderr)
            return 2

    review = pd.read_csv(COLLISION_REVIEW_CSV, dtype=str).fillna("")
    print(f"Loaded {len(review)} review rows from {COLLISION_REVIEW_CSV}")

    # Summarise decisions
    merge_rows = review[review["decision"].str.strip().str.lower() == "merge"]
    separate_rows = review[review["decision"].str.strip().str.lower() == "separate"]
    empty_rows = review[review["decision"].str.strip() == ""]
    print(f"  merge decisions:    {len(merge_rows)}")
    print(f"  separate decisions: {len(separate_rows)}")
    print(f"  undecided:          {len(empty_rows)}")
    if len(empty_rows) > 0:
        print(f"WARNING: {len(empty_rows)} rows have no decision and will be skipped.")

    # Warn about separate decisions (no auto-action required, but human may need to rename)
    if len(separate_rows) > 0:
        sep_canons = separate_rows["person_canon"].unique().tolist()
        print(f"INFO: {len(sep_canons)} person_canon(s) marked 'separate' — no aliases added.")
        print("      Verify these represent genuinely different people in Persons_Truth.")

    if len(merge_rows) == 0:
        print("No merge decisions found. Nothing to do.")
        return 0

    # Build per-person_id name evidence from Persons_Truth_Full (preferred) + Placements_Flat
    pid_to_names: dict[str, list[str]] = {}
    pid_to_canon: dict[str, str] = {}

    if PERSONS_FULL_CSV.exists():
        pf_full = pd.read_csv(PERSONS_FULL_CSV, dtype=str).fillna("")
        if "effective_person_id" in pf_full.columns:
            for _, row in pf_full.iterrows():
                pid = str(row["effective_person_id"]).strip()
                canon = str(row.get("person_canon", "")).strip()
                names_raw = str(row.get("player_names_seen", "")).strip()
                if pid:
                    pid_to_canon[pid] = canon
                    if names_raw:
                        pid_to_names[pid] = [n.strip() for n in names_raw.split("|") if n.strip()]

    # Also scan Placements_Flat for names not in Persons_Truth_Full
    pf: pd.DataFrame | None = None
    if PLACEMENTS_CSV.exists():
        pf = pd.read_csv(PLACEMENTS_CSV, dtype=str).fillna("")

    def names_for_pid(pid: str) -> list[str]:
        if pid in pid_to_names:
            return pid_to_names[pid]
        names: set[str] = set()
        if pf is not None:
            for side in ["player1", "player2"]:
                id_col = f"{side}_person_id"
                nm_col = f"{side}_name"
                if id_col not in pf.columns or nm_col not in pf.columns:
                    continue
                sub = pf[pf[id_col].fillna("").str.strip() == pid]
                for v in sub[nm_col].fillna("").astype(str):
                    if v.strip():
                        names.add(v.strip())
        return sorted(names)

    # Load existing aliases to detect duplicates and enable redirects
    fieldnames_list: list[str] = []
    existing_rows: list[dict] = []
    with ALIASES_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames_list = list(reader.fieldnames or ["alias", "person_id", "person_canon", "status", "notes"])
        for row in reader:
            existing_rows.append(dict(row))

    existing_pairs: set[tuple[str, str]] = set()
    for row in existing_rows:
        a = row.get("alias", "").strip()
        p = row.get("person_id", "").strip()
        if a and p:
            existing_pairs.add((a, p))

    # Group merge decisions by person_canon
    merge_groups: dict[str, dict] = {}
    for _, row in merge_rows.iterrows():
        person_canon = row["person_canon"].strip()
        person_id = row["person_id"].strip()
        merge_target = row["merge_target_id"].strip()
        notes = row["notes"].strip()
        if person_canon not in merge_groups:
            merge_groups[person_canon] = {"target_id": "", "all_ids": [], "notes": notes}
        merge_groups[person_canon]["all_ids"].append(person_id)
        if merge_target:
            merge_groups[person_canon]["target_id"] = merge_target  # last non-blank wins

    # Build a map: losing_id -> target_id (for redirecting existing aliases)
    losing_to_target: dict[str, str] = {}
    losing_to_target_canon: dict[str, str] = {}
    for person_canon, group in sorted(merge_groups.items()):
        all_ids = group["all_ids"]
        target_id = group["target_id"]
        if not target_id:
            best_id = max(all_ids, key=lambda pid: len(names_for_pid(pid)), default=all_ids[0])
            target_id = best_id
        target_canon = pid_to_canon.get(target_id, person_canon)
        for pid in all_ids:
            if pid != target_id:
                losing_to_target[pid] = target_id
                losing_to_target_canon[pid] = target_canon

    # Step 1: Redirect existing alias rows that point to a losing_id → target_id
    redirected = 0
    updated_existing_rows: list[dict] = []
    for row in existing_rows:
        p = row.get("person_id", "").strip()
        if p in losing_to_target:
            new_target = losing_to_target[p]
            new_canon = losing_to_target_canon[p]
            alias_name = row.get("alias", "").strip()
            # Only redirect if the alias doesn't already exist for the target
            if (alias_name, new_target) not in existing_pairs:
                old_notes = row.get("notes", "").strip()
                new_notes = f"redirected:{p}->{new_target[:8]}"
                if old_notes:
                    new_notes += f"; {old_notes}"
                updated_row = dict(row)
                updated_row["person_id"] = new_target
                updated_row["person_canon"] = new_canon
                updated_row["notes"] = new_notes
                updated_existing_rows.append(updated_row)
                existing_pairs.discard((alias_name, p))
                existing_pairs.add((alias_name, new_target))
                redirected += 1
                print(f"  ~ redirect: '{alias_name}' {p[:8]} → {new_target[:8]} ({new_canon})")
            else:
                # Already exists at target — this row is now a duplicate, remove it
                print(f"  - removed duplicate: '{alias_name}' → {p[:8]} (already at {new_target[:8]})")
        else:
            updated_existing_rows.append(row)
    print(f"Redirected {redirected} existing alias rows")

    # Step 2: Add new alias rows for names from Placements_Flat not yet covered
    new_alias_rows: list[dict] = []

    for person_canon, group in sorted(merge_groups.items()):
        all_ids: list[str] = group["all_ids"]
        notes: str = group["notes"]
        target_id: str = group["target_id"]

        if not target_id:
            best_id = max(all_ids, key=lambda pid: len(names_for_pid(pid)), default=all_ids[0])
            target_id = best_id

        target_canon = pid_to_canon.get(target_id, person_canon)
        losing_ids = [pid for pid in all_ids if pid != target_id]

        for losing_id in losing_ids:
            # Get all actual player names seen in Placements_Flat for this losing_id
            pf_names: set[str] = set()
            if pf is not None:
                for side in ["player1", "player2"]:
                    id_col = f"{side}_person_id"
                    for nm_col in [f"{side}_name_clean", f"{side}_name"]:
                        if id_col in pf.columns and nm_col in pf.columns:
                            sub = pf[pf[id_col].fillna("").str.strip() == losing_id]
                            for v in sub[nm_col].fillna("").astype(str):
                                if v.strip():
                                    pf_names.add(v.strip())
                            break

            # Also include names from Persons_Truth_Full
            pt_names = set(names_for_pid(losing_id))
            all_names = pf_names | pt_names
            if not all_names:
                all_names = {pid_to_canon.get(losing_id, person_canon)}

            for name in sorted(all_names):
                if not name:
                    continue
                key = (name, target_id)
                if key not in existing_pairs:
                    alias_notes = f"merge:{losing_id}"
                    if notes:
                        alias_notes += f"; {notes}"
                    new_alias_rows.append({
                        "alias": name,
                        "person_id": target_id,
                        "person_canon": target_canon,
                        "status": "VERIFIED",
                        "notes": alias_notes,
                    })
                    existing_pairs.add(key)
                    print(f"  + alias: '{name}' -> {target_id} ({target_canon})")

    if redirected == 0 and not new_alias_rows:
        print("No changes to make (all may be 'separate' or already present).")
        return 0

    print(f"\nSummary: {redirected} redirected + {len(new_alias_rows)} new aliases")

    # Ensure output has 'notes' column
    if "notes" not in fieldnames_list:
        fieldnames_list.append("notes")

    all_rows = updated_existing_rows + new_alias_rows
    with ALIASES_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_list, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nAdded {len(new_alias_rows)} new alias rows to {ALIASES_CSV}")
    print("Next: python 04_build_analytics.py   # rebuild Persons_Truth with merged IDs")
    return 0


# ---------------------------------------------------------------------------
# --auto mode
# ---------------------------------------------------------------------------

def auto_merge() -> int:
    """
    --auto mode: no human review required.

    Generates collision_review.csv with auto-suggestions, then immediately applies
    all merge decisions. Rows with auto_confidence='needs_review' that have 0
    appearances on all IDs are still merged (ghost UUID cleanup). Others with
    comparable appearances are also merged (trusts the canon name as ground truth).

    Safe for bulk application when canon-name collisions are known to be the same
    real person (i.e., after aliases have been established for all name variants).
    """
    rc = generate_review()
    if rc != 0:
        return rc

    review = pd.read_csv(COLLISION_REVIEW_CSV, dtype=str).fillna("")
    review["appearances"] = pd.to_numeric(review["appearances"], errors="coerce").fillna(0).astype(int)

    # The generate step already pre-filled decision='merge' and merge_target_id.
    # For --auto: keep all merge decisions as-is (including needs_review groups).
    # Set notes to 'auto' to mark these as machine-applied.
    review["notes"] = review["notes"].where(review["notes"].str.strip() != "", "auto")
    review.to_csv(COLLISION_REVIEW_CSV, index=False)
    print(f"Auto-confirmed {len(review)} rows → {COLLISION_REVIEW_CSV}")

    return apply_decisions()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gate 3 Step 3.2: Resolve person_id collisions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--generate", action="store_true",
                     help="Generate out/collision_review.csv for human review")
    grp.add_argument("--apply", action="store_true",
                     help="Apply human decisions from out/collision_review.csv to person_aliases.csv")
    grp.add_argument("--auto", action="store_true",
                     help="Auto-merge: drop 0-appearance ghosts, merge remainder into max-appearances ID per group")
    args = parser.parse_args()

    if args.generate:
        return generate_review()
    if args.auto:
        return auto_merge()
    return apply_decisions()


if __name__ == "__main__":
    raise SystemExit(main())
