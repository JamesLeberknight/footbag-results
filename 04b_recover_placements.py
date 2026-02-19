#!/usr/bin/env python3
"""
04b_recover_placements.py — Recovery layer for rejected ByPerson placements.

Reads:
  - out/Placements_Flat.csv           (canonical placements with person_id)
  - out/Placements_ByPerson.csv       (accepted placements)
  - out/Placements_ByPerson_Rejected.csv  (rejected placements)
  - out/stage2_canonical_events.csv   (event metadata for location/host_club)

Writes:
  - out/Recovery_Candidates.csv       (all recovery attempts with confidence)
  - out/Placements_ByPerson_WithRecovery.csv  (canonical + accepted recoveries)
  - out/Recovery_Summary.json         (counts by method and confidence)

Recovery methods (in priority order):
  1. Same-event exact match (high confidence)
  2. Cross-event exact match (high if freq>=10, medium if freq>=3)
  3. Single-token last-name expansion (medium confidence)
  4. Location/affiliation context attachment (all rows)

Never modifies canonical data.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


OUT_DIR = Path("out")


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all required input CSVs."""
    flat = pd.read_csv(OUT_DIR / "Placements_Flat.csv", dtype=str).fillna("")
    byp = pd.read_csv(OUT_DIR / "Placements_ByPerson.csv", dtype=str).fillna("")
    rejected = pd.read_csv(OUT_DIR / "Placements_ByPerson_Rejected.csv", dtype=str).fillna("")
    events = pd.read_csv(OUT_DIR / "stage2_canonical_events.csv", dtype=str).fillna("")
    return flat, byp, rejected, events


def build_canonical_name_lookup(flat: pd.DataFrame) -> dict[str, dict]:
    """Build name -> {person_id, person_canon, count} from Flat.

    Only includes rows where person_id is non-blank.
    Uses case-insensitive keys. Tracks ambiguity (multiple person_ids per name).
    """
    lookup: dict[str, dict[str, set]] = {}  # name_lower -> {pids: set, canons: set}

    for prefix in ("player1", "player2"):
        pid_col = f"{prefix}_person_id"
        canon_col = f"{prefix}_person_canon"
        name_cols = [canon_col, f"{prefix}_name_clean", f"{prefix}_name"]

        for _, row in flat.iterrows():
            pid = row[pid_col].strip()
            if not pid:
                continue
            canon = row[canon_col].strip()

            for col in name_cols:
                name = row[col].strip()
                if not name or len(name) < 2:
                    continue
                nl = name.lower()
                if nl not in lookup:
                    lookup[nl] = {"pids": set(), "canons": set()}
                lookup[nl]["pids"].add(pid)
                if canon:
                    lookup[nl]["canons"].add(canon)

    # Flatten to final form with counts
    result = {}
    for name_lower, data in lookup.items():
        pids = data["pids"]
        canons = data["canons"]
        result[name_lower] = {
            "person_ids": pids,
            "person_canon": next(iter(canons)) if len(canons) == 1 else (canons.pop() if canons else ""),
            "is_ambiguous": len(pids) > 1,
            "count": 0,  # filled below
        }

    # Count frequency per person_id in Flat
    pid_freq: dict[str, int] = {}
    for prefix in ("player1", "player2"):
        for pid in flat[f"{prefix}_person_id"]:
            pid = pid.strip()
            if pid:
                pid_freq[pid] = pid_freq.get(pid, 0) + 1

    for data in result.values():
        if not data["is_ambiguous"]:
            pid = next(iter(data["person_ids"]))
            data["count"] = pid_freq.get(pid, 0)

    return result


def build_event_person_map(flat: pd.DataFrame) -> dict[str, dict[str, str]]:
    """Build event_id -> {name_lower: person_id} for same-event matching.

    Only includes unambiguous mappings (1 pid per name per event).
    """
    event_names: dict[str, dict[str, set]] = {}  # eid -> {name_lower: set(pids)}
    event_canons: dict[str, dict[str, str]] = {}  # eid -> {pid: canon}

    for prefix in ("player1", "player2"):
        pid_col = f"{prefix}_person_id"
        canon_col = f"{prefix}_person_canon"
        name_cols = [canon_col, f"{prefix}_name_clean", f"{prefix}_name"]

        for _, row in flat.iterrows():
            eid = row["event_id"].strip()
            pid = row[pid_col].strip()
            if not pid or not eid:
                continue
            canon = row[canon_col].strip()

            if eid not in event_names:
                event_names[eid] = {}
                event_canons[eid] = {}
            if canon:
                event_canons[eid][pid] = canon

            for col in name_cols:
                name = row[col].strip()
                if not name or len(name) < 2:
                    continue
                nl = name.lower()
                if nl not in event_names[eid]:
                    event_names[eid][nl] = set()
                event_names[eid][nl].add(pid)

    # Flatten: only keep unambiguous names
    result: dict[str, dict[str, str]] = {}
    canons_out: dict[str, dict[str, str]] = {}
    for eid, names in event_names.items():
        result[eid] = {}
        canons_out[eid] = event_canons.get(eid, {})
        for nl, pids in names.items():
            if len(pids) == 1:
                result[eid][nl] = next(iter(pids))

    return result, canons_out


def build_last_name_lookup(flat: pd.DataFrame) -> dict[str, list[tuple[str, str]]]:
    """Build last_name_lower -> [(person_id, person_canon), ...] from Flat."""
    lookup: dict[str, set] = {}

    for prefix in ("player1", "player2"):
        pid_col = f"{prefix}_person_id"
        canon_col = f"{prefix}_person_canon"

        for _, row in flat.iterrows():
            pid = row[pid_col].strip()
            canon = row[canon_col].strip()
            if not pid or not canon or " " not in canon:
                continue
            last = canon.split()[-1].lower()
            if len(last) < 2:
                continue
            if last not in lookup:
                lookup[last] = set()
            lookup[last].add((pid, canon))

    return {k: list(v) for k, v in lookup.items()}


def get_rejected_name(row: pd.Series, player: str) -> str:
    """Get the best available name for a rejected player."""
    for col in [f"{player}_person_canon", f"{player}_name_clean",
                f"{player}_name", f"{player}_name_raw"]:
        val = row.get(col, "")
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def recover_placements(
    rejected: pd.DataFrame,
    event_map: dict,
    event_canons: dict,
    name_lookup: dict,
    last_name_lookup: dict,
    event_persons_by_event: dict,
) -> pd.DataFrame:
    """Apply recovery methods to rejected placements.

    Returns DataFrame with recovery metadata columns added.
    """
    records = []

    for idx, row in rejected.iterrows():
        reason = row["reject_reason"].strip()
        eid = row["event_id"].strip()

        rec = row.to_dict()
        rec["recovered_player1_id"] = ""
        rec["recovered_player1_person_canon"] = ""
        rec["recovered_player2_id"] = ""
        rec["recovered_player2_person_canon"] = ""
        rec["recovery_method"] = ""
        rec["recovery_confidence"] = ""
        rec["recovery_evidence"] = ""

        if reason == "missing_player1_id":
            name = get_rejected_name(row, "player1")
            if not name:
                records.append(rec)
                continue

            nl = name.lower()
            recovered = False

            # Method 1: Same-event exact match
            if not recovered and eid in event_map:
                pid = event_map[eid].get(nl)
                if pid:
                    canon = event_canons.get(eid, {}).get(pid, name)
                    rec["recovered_player1_id"] = pid
                    rec["recovered_player1_person_canon"] = canon
                    rec["recovery_method"] = "same_event_exact"
                    rec["recovery_confidence"] = "high"
                    rec["recovery_evidence"] = f"same_event:{eid}, matched:{canon}"
                    recovered = True

            # Method 2: Cross-event exact match
            if not recovered and nl in name_lookup:
                entry = name_lookup[nl]
                if not entry["is_ambiguous"]:
                    pid = next(iter(entry["person_ids"]))
                    canon = entry["person_canon"] or name
                    freq = entry["count"]
                    if freq >= 3:
                        rec["recovered_player1_id"] = pid
                        rec["recovered_player1_person_canon"] = canon
                        rec["recovery_method"] = "cross_event_exact"
                        rec["recovery_confidence"] = "high" if freq >= 10 else "medium"
                        rec["recovery_evidence"] = f"cross_event_freq:{freq}, person:{canon}"
                        recovered = True

            # Method 3: Single-token last-name expansion
            if not recovered and " " not in name and nl in last_name_lookup:
                matches = last_name_lookup[nl]
                event_pids = event_persons_by_event.get(eid, set())

                # Prefer same-event context
                same_event_matches = [(pid, canon) for pid, canon in matches if pid in event_pids]
                if len(same_event_matches) == 1:
                    pid, canon = same_event_matches[0]
                    rec["recovered_player1_id"] = pid
                    rec["recovered_player1_person_canon"] = canon
                    rec["recovery_method"] = "last_name_same_event"
                    rec["recovery_confidence"] = "medium"
                    rec["recovery_evidence"] = f"last_name_match:{canon}, context:same_event:{eid}"
                    recovered = True
                elif len(same_event_matches) == 0 and len(matches) == 1:
                    pid, canon = matches[0]
                    rec["recovered_player1_id"] = pid
                    rec["recovered_player1_person_canon"] = canon
                    rec["recovery_method"] = "last_name_global"
                    rec["recovery_confidence"] = "medium"
                    rec["recovery_evidence"] = f"last_name_match:{canon}, context:global_unique"
                    recovered = True

            # Also recover player2 for team entries if player2 has name but no ID
            if reason == "missing_player1_id":
                ct = row.get("competitor_type", "").strip().lower()
                if ct == "team":
                    p2_name = get_rejected_name(row, "player2")
                    p2_id = str(row.get("player2_id", "")).strip()
                    if p2_name and not p2_id:
                        _try_recover_player(rec, "player2", p2_name, eid,
                                            event_map, event_canons, name_lookup,
                                            last_name_lookup, event_persons_by_event)

        elif reason == "team_missing_player2_id":
            name = get_rejected_name(row, "player2")
            if not name:
                records.append(rec)
                continue

            _try_recover_player(rec, "player2", name, eid,
                                event_map, event_canons, name_lookup,
                                last_name_lookup, event_persons_by_event)

        records.append(rec)

    return pd.DataFrame(records)


def _try_recover_player(
    rec: dict, player: str, name: str, eid: str,
    event_map: dict, event_canons: dict, name_lookup: dict,
    last_name_lookup: dict, event_persons_by_event: dict,
):
    """Try to recover a specific player slot (player1 or player2)."""
    nl = name.lower()
    pid_key = f"recovered_{player}_id"
    canon_key = f"recovered_{player}_person_canon"

    # Method 1: Same-event
    if eid in event_map:
        pid = event_map[eid].get(nl)
        if pid:
            canon = event_canons.get(eid, {}).get(pid, name)
            rec[pid_key] = pid
            rec[canon_key] = canon
            if not rec["recovery_method"]:
                rec["recovery_method"] = "same_event_exact"
                rec["recovery_confidence"] = "high"
                rec["recovery_evidence"] = f"same_event:{eid}, matched:{canon}"
            else:
                rec["recovery_evidence"] += f" | {player}:same_event:{eid}, matched:{canon}"
            return

    # Method 2: Cross-event
    if nl in name_lookup:
        entry = name_lookup[nl]
        if not entry["is_ambiguous"]:
            pid = next(iter(entry["person_ids"]))
            canon = entry["person_canon"] or name
            freq = entry["count"]
            if freq >= 3:
                rec[pid_key] = pid
                rec[canon_key] = canon
                if not rec["recovery_method"]:
                    rec["recovery_method"] = "cross_event_exact"
                    rec["recovery_confidence"] = "high" if freq >= 10 else "medium"
                    rec["recovery_evidence"] = f"cross_event_freq:{freq}, person:{canon}"
                else:
                    rec["recovery_evidence"] += f" | {player}:cross_event_freq:{freq}, person:{canon}"
                return

    # Method 3: Last-name
    if " " not in name and nl in last_name_lookup:
        matches = last_name_lookup[nl]
        event_pids = event_persons_by_event.get(eid, set())
        same_event_matches = [(pid, canon) for pid, canon in matches if pid in event_pids]
        if len(same_event_matches) == 1:
            pid, canon = same_event_matches[0]
            rec[pid_key] = pid
            rec[canon_key] = canon
            if not rec["recovery_method"]:
                rec["recovery_method"] = "last_name_same_event"
                rec["recovery_confidence"] = "medium"
                rec["recovery_evidence"] = f"last_name_match:{canon}, context:same_event:{eid}"
            else:
                rec["recovery_evidence"] += f" | {player}:last_name:{canon}"
        elif len(same_event_matches) == 0 and len(matches) == 1:
            pid, canon = matches[0]
            rec[pid_key] = pid
            rec[canon_key] = canon
            if not rec["recovery_method"]:
                rec["recovery_method"] = "last_name_global"
                rec["recovery_confidence"] = "medium"
                rec["recovery_evidence"] = f"last_name_match:{canon}, context:global_unique"
            else:
                rec["recovery_evidence"] += f" | {player}:last_name:{canon}"


def attach_event_context(candidates: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Attach event_location and event_host_club from event metadata."""
    event_loc = events.set_index("event_id")[["location", "host_club"]].rename(
        columns={"location": "event_location", "host_club": "event_host_club"}
    )
    candidates = candidates.merge(event_loc, left_on="event_id", right_index=True, how="left")
    candidates["event_location"] = candidates["event_location"].fillna("")
    candidates["event_host_club"] = candidates["event_host_club"].fillna("")
    return candidates


def is_recovery_accepted(row: pd.Series) -> bool:
    """Check if a recovered row meets acceptance criteria."""
    conf = row.get("recovery_confidence", "").strip()
    if conf not in ("high", "medium"):
        return False

    reason = row.get("original_reject_reason", row.get("reject_reason", "")).strip()

    if reason == "missing_player1_id":
        return bool(row.get("recovered_player1_id", "").strip())
    elif reason == "team_missing_player2_id":
        return bool(row.get("recovered_player2_id", "").strip())

    return False


def merge_recovery(byp: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    """Merge accepted recoveries into ByPerson to produce WithRecovery surface."""
    accepted = candidates[candidates.apply(is_recovery_accepted, axis=1)].copy()
    if accepted.empty:
        return byp.copy()

    # Build rows matching ByPerson schema
    recovery_rows = []
    byp_cols = list(byp.columns)

    # Build dedup set from canonical
    dedup_keys = set()
    for _, row in byp.iterrows():
        key = (
            row["event_id"].strip(),
            row["division_canon"].strip(),
            str(row["place"]).strip(),
            row["player1_id"].strip(),
        )
        dedup_keys.add(key)

    for _, row in accepted.iterrows():
        reason = row.get("reject_reason", "").strip()
        new_row = {}

        for col in byp_cols:
            new_row[col] = row.get(col, "")

        # Apply recovered IDs
        if reason == "missing_player1_id":
            new_row["player1_id"] = row["recovered_player1_id"]
            new_row["player1_person_canon"] = row["recovered_player1_person_canon"]
            new_row["player1_name"] = row["recovered_player1_person_canon"] or row.get("player1_name", "")

        if reason == "team_missing_player2_id" or (
            reason == "missing_player1_id" and row.get("recovered_player2_id", "").strip()
        ):
            if row.get("recovered_player2_id", "").strip():
                new_row["player2_id"] = row["recovered_player2_id"]
                new_row["player2_person_canon"] = row["recovered_player2_person_canon"]
                new_row["player2_name"] = row["recovered_player2_person_canon"] or row.get("player2_name", "")

        # Dedup check
        key = (
            str(new_row["event_id"]).strip(),
            str(new_row["division_canon"]).strip(),
            str(new_row["place"]).strip(),
            str(new_row["player1_id"]).strip(),
        )
        if key in dedup_keys:
            continue
        dedup_keys.add(key)

        # Presentability gate: must have non-blank player1_name
        p1_name = str(new_row.get("player1_name", "")).strip()
        if not p1_name:
            continue

        # Team gate: teams must have player2_name
        ct = str(new_row.get("competitor_type", "")).strip().lower()
        if ct == "team":
            p2_name = str(new_row.get("player2_name", "")).strip()
            if not p2_name:
                continue

        recovery_rows.append(new_row)

    if not recovery_rows:
        return byp.copy()

    recovery_df = pd.DataFrame(recovery_rows, columns=byp_cols)
    merged = pd.concat([byp, recovery_df], ignore_index=True)
    return merged


def build_summary(candidates: pd.DataFrame) -> dict:
    """Build recovery summary statistics."""
    total = len(candidates)
    has_method = candidates["recovery_method"].str.strip() != ""
    recovered = candidates[has_method]
    accepted = candidates[candidates.apply(is_recovery_accepted, axis=1)]

    by_method = {}
    for method, grp in recovered.groupby("recovery_method"):
        by_conf = {}
        for conf, cgrp in grp.groupby("recovery_confidence"):
            acc = cgrp.apply(is_recovery_accepted, axis=1).sum()
            by_conf[conf] = {"total": len(cgrp), "accepted": int(acc)}
        by_method[method] = by_conf

    by_reason = {}
    for reason, grp in candidates.groupby("reject_reason"):
        rec = (grp["recovery_method"].str.strip() != "").sum()
        acc = grp.apply(is_recovery_accepted, axis=1).sum()
        by_reason[reason] = {"total": len(grp), "recovered": int(rec), "accepted": int(acc)}

    return {
        "total_rejected": total,
        "total_recovery_attempted": int(has_method.sum()),
        "total_accepted": int(len(accepted)),
        "by_method": by_method,
        "by_reject_reason": by_reason,
    }


def main():
    print("=== 04b: Recovery Layer ===")

    # Load data
    flat, byp, rejected, events = load_data()
    print(f"Loaded: Flat={len(flat)}, ByPerson={len(byp)}, Rejected={len(rejected)}, Events={len(events)}")

    if rejected.empty:
        print("No rejected placements to recover.")
        # Write empty outputs
        pd.DataFrame().to_csv(OUT_DIR / "Recovery_Candidates.csv", index=False)
        byp.to_csv(OUT_DIR / "Placements_ByPerson_WithRecovery.csv", index=False, na_rep="")
        (OUT_DIR / "Recovery_Summary.json").write_text(json.dumps({"total_rejected": 0}, indent=2))
        return

    # Build lookup structures
    print("Building canonical name lookup...")
    name_lookup = build_canonical_name_lookup(flat)
    print(f"  {len(name_lookup)} unique names in lookup")

    print("Building same-event person map...")
    event_map, event_canons = build_event_person_map(flat)
    print(f"  {len(event_map)} events with person mappings")

    print("Building last-name lookup...")
    last_name_lookup = build_last_name_lookup(flat)
    print(f"  {len(last_name_lookup)} unique last names")

    # Build per-event person set for last-name context
    event_persons: dict[str, set] = {}
    for prefix in ("player1", "player2"):
        for _, row in flat.iterrows():
            eid = row["event_id"].strip()
            pid = row[f"{prefix}_person_id"].strip()
            if pid and eid:
                if eid not in event_persons:
                    event_persons[eid] = set()
                event_persons[eid].add(pid)

    # Run recovery
    print("Running recovery methods...")
    candidates = recover_placements(
        rejected, event_map, event_canons, name_lookup,
        last_name_lookup, event_persons,
    )

    # Rename reject_reason to original_reject_reason in candidates
    if "reject_reason" in candidates.columns:
        candidates = candidates.rename(columns={"reject_reason": "original_reject_reason"})

    # Attach event context (method 4)
    print("Attaching event context...")
    candidates = attach_event_context(candidates, events)

    # Select output columns
    output_cols = [
        "event_id", "year", "division_canon", "division_category", "place",
        "competitor_type", "player1_name", "player2_name", "team_display_name",
        "original_reject_reason",
        "recovered_player1_id", "recovered_player1_person_canon",
        "recovered_player2_id", "recovered_player2_person_canon",
        "recovery_method", "recovery_confidence", "recovery_evidence",
        "event_location", "event_host_club",
    ]
    # Keep only columns that exist
    output_cols = [c for c in output_cols if c in candidates.columns]
    candidates_out = candidates[output_cols]

    # Write Recovery_Candidates.csv
    candidates_out.to_csv(OUT_DIR / "Recovery_Candidates.csv", index=False, na_rep="")
    print(f"Wrote: {OUT_DIR / 'Recovery_Candidates.csv'} ({len(candidates_out)} rows)")

    # Build summary
    # Use the full candidates (with original_reject_reason) for summary
    # Need to handle renamed column
    summary_candidates = candidates.copy()
    if "original_reject_reason" in summary_candidates.columns and "reject_reason" not in summary_candidates.columns:
        summary_candidates = summary_candidates.rename(columns={"original_reject_reason": "reject_reason"})
    summary = build_summary(summary_candidates)

    # Merge recovery into ByPerson
    # Need full candidates with reject_reason for merge
    merge_candidates = candidates.copy()
    if "original_reject_reason" in merge_candidates.columns:
        merge_candidates = merge_candidates.rename(columns={"original_reject_reason": "reject_reason"})
    with_recovery = merge_recovery(byp, merge_candidates)
    with_recovery.to_csv(OUT_DIR / "Placements_ByPerson_WithRecovery.csv", index=False, na_rep="")

    # Add actual merged count to summary
    summary["total_merged"] = len(with_recovery) - len(byp)

    # Write summary
    (OUT_DIR / "Recovery_Summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Wrote: {OUT_DIR / 'Placements_ByPerson_WithRecovery.csv'} ({len(with_recovery)} rows)")

    # Print summary
    print()
    print("=== Recovery Summary ===")
    print(f"Total rejected: {summary['total_rejected']}")
    print(f"Recovery attempted: {summary['total_recovery_attempted']}")
    print(f"Accepted into WithRecovery: {summary['total_accepted']}")
    print(f"  New total: {len(byp)} canonical + {len(with_recovery) - len(byp)} recovered = {len(with_recovery)}")
    print()
    print("By method:")
    for method, confs in sorted(summary["by_method"].items()):
        for conf, counts in sorted(confs.items()):
            print(f"  {method} [{conf}]: {counts['total']} recovered, {counts['accepted']} accepted")
    print()
    print("By reject reason:")
    for reason, counts in sorted(summary["by_reject_reason"].items()):
        print(f"  {reason}: {counts['total']} total, {counts['recovered']} recovered, {counts['accepted']} accepted")


if __name__ == "__main__":
    main()
