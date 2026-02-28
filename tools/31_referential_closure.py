#!/usr/bin/env python3
"""
31_referential_closure.py — Audit and fix referential integrity gaps.

Three gaps addressed:

  A. 15 orphan events: in Placements_ByPerson_v31 but not in
     stage2_canonical_events.csv. These are synthetic pre-mirror events
     (1980-1986) + event 857879540 (1997) + 1770923654 (2026).

  B. ~91 orphan person_ids: in PBP but not in Persons_Truth_Final_v29 as
     effective_person_id. Split into:
       - Remap candidates: person_canon exactly matches an existing PT canonical name
         → update PBP person_id to the canonical ID
       - Non-person orphans: person_canon is __NON_PERSON__ or team-name concatenation
         → clear person_id (set NaN) in PBP
       - New persons: genuinely new person not in PT
         → append to PT with source=placements_registry

  C. 47 QC07 placeholder rows: source contains 'coverage_closure'. Audit each:
       - Has placements in Placements_Flat → upgrade source to 'data_only'
       - No placements in Placements_Flat → remove from Truth

  D. Lessard canonical rename:
       - 87216aed "Phillip Lessard" → "Philippe Lessard" (corrects French spelling)
       - 705ad942 "Philippe Lessard" (QC07 placeholder, 0 PF rows) → removed
       - person_aliases.csv: update person_canon from "Phillip" → "Philippe" for
         the two alias rows pointing to 87216aed

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v29.csv
  inputs/identity_lock/Placements_ByPerson_v31.csv
  out/Placements_Flat.csv
  out/stage2_canonical_events.csv
  overrides/person_aliases.csv

Outputs (with --apply):
  inputs/identity_lock/Persons_Truth_Final_v30.csv
  inputs/identity_lock/Placements_ByPerson_v32.csv
  overrides/person_aliases.csv  (updated in-place)
  out/referential_closure_report.csv
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"
OUT = ROOT / "out"
OVERRIDES = ROOT / "overrides"

TRUTH_IN   = IDENTITY_LOCK / "Persons_Truth_Final_v29.csv"
PBP_IN     = IDENTITY_LOCK / "Placements_ByPerson_v31.csv"
SCE_CSV    = OUT / "stage2_canonical_events.csv"
PF_CSV     = OUT / "Placements_Flat.csv"
ALIASES_CSV = OVERRIDES / "person_aliases.csv"

TRUTH_OUT  = IDENTITY_LOCK / "Persons_Truth_Final_v30.csv"
PBP_OUT    = IDENTITY_LOCK / "Placements_ByPerson_v32.csv"
REPORT_CSV = OUT / "referential_closure_report.csv"

# person_id that should be renamed (Phillip → Philippe)
LESSARD_RENAME_ID    = "87216aed-3048-50f7-8c54-d7e9e7bb52f3"
LESSARD_OLD_CANON    = "Phillip Lessard"
LESSARD_NEW_CANON    = "Philippe Lessard"

# Orphan person_ids that are __NON_PERSON__ or team-name concatenation artifacts.
# For these we clear person_id=NaN in PBP instead of adding them to Truth.
NON_PERSON_ORPHAN_IDS: set[str] = {
    "682100c3-88ab-51f5-be3e-c9d8f3a2eaf7",  # __NON_PERSON__ (Novice Circle Jamz 2002)
    "c812f3a0-ddaa-5d93-9d3b-fb00e90c9090",  # __NON_PERSON__ (Single Net 2005)
    "62a5b705-1e5b-5aef-ac1b-be65846257a0",  # "Karim Daouk Arthur Ledain" — concat artifact
}

# QC07 placeholder IDs to ALWAYS remove, even if an orphan remaps to them.
# For these, orphan remaps are redirected to LESSARD_RENAME_ID (the surviving canonical).
# 705ad942 is the phantom 'Philippe Lessard' added by coverage_closure; the real
# canonical is 87216aed which is being renamed from 'Phillip' → 'Philippe'.
QC07_EXPLICIT_REMOVE_IDS: set[str] = {
    "705ad942-9627-5f81-a24c-3fbf4a4b655a",  # Philippe Lessard (QC07 phantom)
}


def _norm(s: str) -> str:
    """Normalize string for comparison (lower, NFC, collapse whitespace)."""
    s = unicodedata.normalize("NFC", str(s or ""))
    return re.sub(r"\s+", " ", s).strip().lower()


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load PT, PBP, PF, SCE. Return (pt, pbp, pf, sce)."""
    pt  = pd.read_csv(TRUTH_IN,  low_memory=False)
    pbp = pd.read_csv(PBP_IN,   low_memory=False)
    pf  = pd.read_csv(PF_CSV,   low_memory=False)
    sce = pd.read_csv(SCE_CSV,  low_memory=False)
    return pt, pbp, pf, sce


def find_orphan_events(pbp: pd.DataFrame, sce: pd.DataFrame) -> pd.DataFrame:
    """Events in PBP but not in stage2_canonical_events."""
    sce_events = set(sce["event_id"].astype(str).unique())
    pbp_events = pbp["event_id"].astype(str).unique()

    rows = []
    for eid in sorted(pbp_events):
        if eid not in sce_events:
            sub = pbp[pbp["event_id"].astype(str) == eid]
            year = int(sub["year"].iloc[0]) if len(sub) > 0 else 0
            cats = sorted(sub["division_category"].dropna().unique())
            rows.append({
                "gap_type": "orphan_event",
                "event_id": eid,
                "year": year,
                "categories": ",".join(cats),
                "pbp_rows": len(sub),
            })
    return pd.DataFrame(rows)


def find_orphan_persons(
    pt: pd.DataFrame, pbp: pd.DataFrame, pf: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns (remap_df, nonperson_df, new_persons_df).

    remap_df:      orphan IDs whose person_canon exactly matches a PT canonical name
    nonperson_df:  orphan IDs that are __NON_PERSON__ or known concat artifacts
    new_persons_df: genuinely new person orphans
    """
    pt_ids = set(pt["effective_person_id"].dropna().astype(str).unique())
    pt_canon_to_id = dict(zip(pt["person_canon"].astype(str), pt["effective_person_id"].astype(str)))

    # Aggregate PBP orphans (one row per person_id)
    pbp_persons = (
        pbp[pbp["person_id"].notna()]
        .groupby("person_id")
        .agg(
            person_canon=("person_canon", "first"),
            pbp_rows=("person_id", "count"),
        )
        .reset_index()
    )
    pbp_persons["person_id"] = pbp_persons["person_id"].astype(str)

    orphans = pbp_persons[~pbp_persons["person_id"].isin(pt_ids)].copy()

    remap_rows = []
    nonperson_rows = []
    new_rows = []

    for _, row in orphans.iterrows():
        pid = row["person_id"]
        canon = str(row["person_canon"])
        n = int(row["pbp_rows"])
        pf_count = int((pf["person_id"].astype(str) == pid).sum())

        if pid in NON_PERSON_ORPHAN_IDS or canon == "__NON_PERSON__":
            nonperson_rows.append({
                "gap_type": "orphan_person_nonperson",
                "orphan_id": pid,
                "orphan_canon": canon,
                "canonical_id": "",
                "canonical_canon": "",
                "pbp_rows": n,
                "pf_rows": pf_count,
                "action": "clear_person_id",
            })
        elif canon in pt_canon_to_id:
            remap_rows.append({
                "gap_type": "orphan_person_remap",
                "orphan_id": pid,
                "orphan_canon": canon,
                "canonical_id": pt_canon_to_id[canon],
                "canonical_canon": canon,
                "pbp_rows": n,
                "pf_rows": pf_count,
                "action": "remap_to_canonical",
            })
        else:
            new_rows.append({
                "gap_type": "orphan_person_new",
                "orphan_id": pid,
                "orphan_canon": canon,
                "canonical_id": "",
                "canonical_canon": "",
                "pbp_rows": n,
                "pf_rows": pf_count,
                "action": "add_to_truth",
            })

    return (
        pd.DataFrame(remap_rows),
        pd.DataFrame(nonperson_rows),
        pd.DataFrame(new_rows),
    )


def find_qc07_placeholders(pt: pd.DataFrame, pf: pd.DataFrame) -> pd.DataFrame:
    """QC07 placeholder rows: source contains 'coverage_closure'."""
    pf_ids = set(pf["person_id"].dropna().astype(str).unique())

    mask = pt["source"].fillna("").str.contains("coverage_closure", case=False)
    qc07 = pt[mask].copy()

    rows = []
    for _, row in qc07.iterrows():
        pid = str(row["effective_person_id"])
        canon = str(row["person_canon"])
        pf_count = int((pf["person_id"].astype(str) == pid).sum())
        has_pf = pid in pf_ids
        action = "upgrade_to_data_only" if has_pf else "remove"
        rows.append({
            "gap_type": "qc07_placeholder",
            "effective_person_id": pid,
            "person_canon": canon,
            "source": str(row.get("source", "")),
            "pf_rows": pf_count,
            "action": action,
        })

    return pd.DataFrame(rows)


def run_report(pt, pbp, pf, sce) -> None:
    """Print audit report and write out/referential_closure_report.csv."""
    print("=" * 70)
    print("REFERENTIAL CLOSURE AUDIT REPORT")
    print("=" * 70)

    orphan_events_df = find_orphan_events(pbp, sce)
    remap_df, nonperson_df, new_df = find_orphan_persons(pt, pbp, pf)
    qc07_df = find_qc07_placeholders(pt, pf)

    # ── Orphan Events ──────────────────────────────────────────────────────
    print(f"\n[A] Orphan Events (in PBP not in stage2): {len(orphan_events_df)}")
    for _, row in orphan_events_df.iterrows():
        print(f"  {row['event_id']:12s}  year={row['year']}  rows={row['pbp_rows']:3d}  cats={row['categories']}")

    # ── Orphan Persons ─────────────────────────────────────────────────────
    total_orphan_persons = len(remap_df) + len(nonperson_df) + len(new_df)
    print(f"\n[B] Orphan person_ids (in PBP not in PT): {total_orphan_persons}")
    print(f"    Remap candidates (exact canon match):  {len(remap_df)}")
    print(f"    Non-person / artifact (clear id):      {len(nonperson_df)}")
    print(f"    Genuinely new persons (add to Truth):  {len(new_df)}")

    if len(nonperson_df):
        print("\n  NON-PERSON / ARTIFACT orphans:")
        for _, row in nonperson_df.iterrows():
            print(f"    {row['orphan_id'][:8]}  {row['orphan_canon']!r}")

    if len(new_df):
        print("\n  NEW PERSONS to add to Truth:")
        for _, row in new_df.iterrows():
            print(f"    {row['orphan_id'][:8]}  {row['orphan_canon']!r}  (pbp={row['pbp_rows']})")

    # ── QC07 Placeholders ─────────────────────────────────────────────────
    remove_count   = (qc07_df["action"] == "remove").sum()
    upgrade_count  = (qc07_df["action"] == "upgrade_to_data_only").sum()
    print(f"\n[C] QC07 Placeholders (source=coverage_closure): {len(qc07_df)}")
    print(f"    Remove (0 PF rows):        {remove_count}")
    print(f"    Upgrade to data_only (>0): {upgrade_count}")

    if upgrade_count:
        print("\n  UPGRADE rows (have placements):")
        for _, row in qc07_df[qc07_df["action"] == "upgrade_to_data_only"].iterrows():
            print(f"    {row['effective_person_id'][:8]}  {row['person_canon']!r:45s}  pf={row['pf_rows']}")

    # ── Lessard Rename ─────────────────────────────────────────────────────
    print(f"\n[D] Lessard canonical rename:")
    print(f"    {LESSARD_RENAME_ID[:8]}  '{LESSARD_OLD_CANON}' → '{LESSARD_NEW_CANON}'")
    print(f"    705ad942  'Philippe Lessard' (QC07 placeholder, 0 PF) → in remove list above")

    # ── Summary ────────────────────────────────────────────────────────────
    pt_before = len(pt)
    pt_after  = pt_before - remove_count + len(new_df)
    print(f"\n[SUMMARY]")
    print(f"  PT v29 rows:           {pt_before}")
    print(f"  - QC07 removals:      -{remove_count}")
    print(f"  + New persons:        +{len(new_df)}")
    print(f"  = PT v30 rows (est):   {pt_after}")
    print(f"  PBP v31 rows:          {len(pbp)}")
    print(f"  PBP v32 rows (same):   {len(pbp)} (row count unchanged; IDs remapped)")

    # Write report CSV
    all_rows = []
    for df in [orphan_events_df, remap_df, nonperson_df, new_df, qc07_df]:
        if len(df):
            all_rows.append(df)

    if all_rows:
        report_df = pd.concat(all_rows, ignore_index=True)
    else:
        report_df = pd.DataFrame()

    REPORT_CSV.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(REPORT_CSV, index=False)
    print(f"\nReport written: {REPORT_CSV}")
    print("\n=== DRY RUN (pass --apply to execute changes) ===")


def run_apply(pt: pd.DataFrame, pbp: pd.DataFrame, pf: pd.DataFrame, sce: pd.DataFrame) -> None:
    """Execute all referential closure operations and write output files."""
    print("=" * 70)
    print("REFERENTIAL CLOSURE — APPLY")
    print("=" * 70)

    remap_df, nonperson_df, new_df = find_orphan_persons(pt, pbp, pf)
    qc07_df = find_qc07_placeholders(pt, pf)

    # ── Reconcile QC07 removals vs orphan remaps ───────────────────────────
    # Some QC07 "remove" entries have 0 current PF rows only because the orphan
    # IDs that hold their real placements haven't been remapped yet.  After the
    # remap those entries would have placements, so they should be UPGRADED
    # instead of removed.
    #
    # Exception: QC07_EXPLICIT_REMOVE_IDS are always removed regardless.
    # Their orphan remaps are redirected to LESSARD_RENAME_ID.
    qc07_remove_ids = set(qc07_df.loc[qc07_df["action"] == "remove", "effective_person_id"])

    # Build a mapping: canonical_id → set of orphan_ids that remap to it
    remap_targets: dict[str, list[str]] = {}
    for _, row in remap_df.iterrows():
        remap_targets.setdefault(row["canonical_id"], []).append(row["orphan_id"])

    # Determine which "remove" entries need promotion to upgrade
    promoted_to_upgrade: set[str] = set()
    for cid in qc07_remove_ids:
        if cid in QC07_EXPLICIT_REMOVE_IDS:
            continue  # always remove
        if cid in remap_targets:
            promoted_to_upgrade.add(cid)
            print(f"  [FIX] QC07 remove → upgrade: {cid[:8]} has orphan remaps: "
                  f"{[o[:8] for o in remap_targets[cid]]}")

    # Apply promotions to qc07_df
    qc07_df = qc07_df.copy()
    qc07_df.loc[
        qc07_df["effective_person_id"].isin(promoted_to_upgrade), "action"
    ] = "upgrade_to_data_only"

    # Recompute remove set after promotions
    qc07_remove_ids = set(qc07_df.loc[qc07_df["action"] == "remove", "effective_person_id"])

    # Redirect orphan remaps targeting explicit-remove IDs to LESSARD_RENAME_ID
    remap_df = remap_df.copy()
    for idx, row in remap_df.iterrows():
        if row["canonical_id"] in QC07_EXPLICIT_REMOVE_IDS:
            print(f"  [FIX] Remap target {row['canonical_id'][:8]} is explicit-remove; "
                  f"redirecting {row['orphan_id'][:8]} → {LESSARD_RENAME_ID[:8]}")
            remap_df.at[idx, "canonical_id"] = LESSARD_RENAME_ID

    # ──────────────────────────────────────────────────────────────────────
    # Step 1: Remap orphan person_ids → canonical IDs in PBP
    # ──────────────────────────────────────────────────────────────────────
    pbp = pbp.copy()
    remap_count = 0
    for _, row in remap_df.iterrows():
        mask = pbp["person_id"].astype(str) == row["orphan_id"]
        if mask.any():
            pbp.loc[mask, "person_id"] = row["canonical_id"]
            remap_count += mask.sum()
            print(f"  Remapped {mask.sum():2d} PBP rows: {row['orphan_id'][:8]} → "
                  f"{row['canonical_id'][:8]}  ({row['orphan_canon']!r})")
    print(f"  Total PBP rows remapped: {remap_count}")

    # ──────────────────────────────────────────────────────────────────────
    # Step 2: Clear person_id for non-person / artifact orphans
    # ──────────────────────────────────────────────────────────────────────
    print()
    clear_count = 0
    for _, row in nonperson_df.iterrows():
        mask = pbp["person_id"].astype(str) == row["orphan_id"]
        if mask.any():
            pbp.loc[mask, "person_id"] = float("nan")
            clear_count += mask.sum()
            print(f"  Cleared person_id for {mask.sum()} PBP rows: {row['orphan_canon']!r}")
    print(f"  Total PBP rows cleared: {clear_count}")

    # ──────────────────────────────────────────────────────────────────────
    # Step 3: Lessard rename in PBP (person_canon column)
    # ──────────────────────────────────────────────────────────────────────
    print()
    lessard_pbp_mask = pbp["person_canon"] == LESSARD_OLD_CANON
    if lessard_pbp_mask.any():
        pbp.loc[lessard_pbp_mask, "person_canon"] = LESSARD_NEW_CANON
        print(f"  PBP: renamed {lessard_pbp_mask.sum()} rows "
              f"'{LESSARD_OLD_CANON}' → '{LESSARD_NEW_CANON}'")
    else:
        print(f"  PBP: no '{LESSARD_OLD_CANON}' rows found (already correct or absent)")

    # Write PBP v32
    pbp.to_csv(PBP_OUT, index=False)
    print(f"\n  Written: {PBP_OUT.name} ({len(pbp)} rows)")

    # ──────────────────────────────────────────────────────────────────────
    # Step 4: Mutate Persons_Truth
    # ──────────────────────────────────────────────────────────────────────
    pt = pt.copy()
    pt_before = len(pt)

    # 4a. QC07 removals (0 PF rows)
    remove_ids = set(qc07_df.loc[qc07_df["action"] == "remove", "effective_person_id"])
    pt = pt[~pt["effective_person_id"].isin(remove_ids)].copy()
    print(f"\n  PT: removed {pt_before - len(pt)} QC07 placeholder rows (0 PF placements)")

    # 4b. QC07 upgrades (>0 PF rows → data_only)
    upgrade_ids = set(qc07_df.loc[qc07_df["action"] == "upgrade_to_data_only", "effective_person_id"])
    upgrade_mask = pt["effective_person_id"].isin(upgrade_ids)
    if upgrade_mask.any():
        pt.loc[upgrade_mask, "source"] = "data_only"
        print(f"  PT: upgraded {upgrade_mask.sum()} QC07 rows to source='data_only'")

    # 4c. Lessard rename in PT
    lessard_pt_mask = pt["effective_person_id"] == LESSARD_RENAME_ID
    if lessard_pt_mask.any():
        pt.loc[lessard_pt_mask, "person_canon"] = LESSARD_NEW_CANON
        # Update person_canon.1 if present
        if "person_canon.1" in pt.columns:
            pt.loc[lessard_pt_mask, "person_canon.1"] = LESSARD_NEW_CANON
        # Update norm_key
        if "norm_key" in pt.columns:
            pt.loc[lessard_pt_mask, "norm_key"] = _norm(LESSARD_NEW_CANON).split()[-1]
        print(f"  PT: renamed {LESSARD_RENAME_ID[:8]} "
              f"'{LESSARD_OLD_CANON}' → '{LESSARD_NEW_CANON}'")
    else:
        print(f"  PT: WARNING — Lessard rename target {LESSARD_RENAME_ID[:8]} not found!")

    # 4d. Add new persons
    if len(new_df):
        new_pt_rows = []
        for _, row in new_df.iterrows():
            canon = str(row["orphan_canon"])
            pid   = str(row["orphan_id"])
            # Derive norm_key (last word of lowercased canon, matching existing convention)
            norm_parts = _norm(canon).split()
            norm_key_val = norm_parts[-1] if norm_parts else ""
            new_pt_rows.append({
                "effective_person_id": pid,
                "person_canon": canon,
                "player_ids_seen": pid,
                "player_names_seen": canon,
                "aliases": "",
                "alias_statuses": "",
                "notes": "added by referential_closure v30",
                "source": "placements_registry",
                "person_canon_clean": "",
                "person_canon_clean_reason": "",
                "aliases_presentable": "",
                "exclusion_reason": "",
                "last_token": norm_parts[-1] if norm_parts else "",
                "person_canon.1": canon,
                "norm_key": norm_key_val,
            })
        new_pt_df = pd.DataFrame(new_pt_rows, columns=pt.columns.tolist())
        pt = pd.concat([pt, new_pt_df], ignore_index=True)
        print(f"  PT: added {len(new_pt_rows)} new person rows")

    pt_after = len(pt)
    print(f"\n  PT: {pt_before} → {pt_after} rows (delta: {pt_after - pt_before:+d})")

    pt.to_csv(TRUTH_OUT, index=False)
    print(f"  Written: {TRUTH_OUT.name}")

    # ──────────────────────────────────────────────────────────────────────
    # Step 5: Update person_aliases.csv for Lessard
    # ──────────────────────────────────────────────────────────────────────
    print()
    aliases = pd.read_csv(ALIASES_CSV, low_memory=False)
    lessard_alias_mask = (
        (aliases["person_id"] == LESSARD_RENAME_ID) &
        (aliases["person_canon"] == LESSARD_OLD_CANON)
    )
    if lessard_alias_mask.any():
        aliases.loc[lessard_alias_mask, "person_canon"] = LESSARD_NEW_CANON
        aliases.to_csv(ALIASES_CSV, index=False)
        print(f"  person_aliases.csv: updated {lessard_alias_mask.sum()} rows "
              f"person_canon '{LESSARD_OLD_CANON}' → '{LESSARD_NEW_CANON}'")
    else:
        print(f"  person_aliases.csv: no matching rows for Lessard update (already correct?)")

    # ──────────────────────────────────────────────────────────────────────
    # Step 6: Write report CSV
    # ──────────────────────────────────────────────────────────────────────
    orphan_events_df = find_orphan_events(pbp, sce)
    all_dfs = [orphan_events_df, remap_df, nonperson_df, new_df, qc07_df]
    report_df = pd.concat([d for d in all_dfs if len(d)], ignore_index=True)
    report_df.to_csv(REPORT_CSV, index=False)
    print(f"\n  Report written: {REPORT_CSV}")

    print("\n=== APPLY COMPLETE ===")
    print(f"  Persons_Truth_Final_v30.csv : {pt_after} rows")
    print(f"  Placements_ByPerson_v32.csv : {len(pbp)} rows")
    print(f"  person_aliases.csv          : updated in-place")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--report", action="store_true",
                       help="Audit gaps and write report (dry run, no files modified)")
    group.add_argument("--apply", action="store_true",
                       help="Execute all closure operations and write v30/v32 output files")
    args = parser.parse_args()

    # Validate inputs
    missing = [p for p in [TRUTH_IN, PBP_IN, SCE_CSV, PF_CSV, ALIASES_CSV] if not p.exists()]
    if missing:
        for p in missing:
            print(f"ERROR: Missing input: {p}", file=sys.stderr)
        sys.exit(1)

    print(f"Loading data...")
    pt, pbp, pf, sce = load_data()
    print(f"  PT:  {len(pt)} rows")
    print(f"  PBP: {len(pbp)} rows")
    print(f"  PF:  {len(pf)} rows")
    print(f"  SCE: {len(sce)} rows")
    print()

    if args.report:
        run_report(pt, pbp, pf, sce)
    else:
        run_apply(pt, pbp, pf, sce)


if __name__ == "__main__":
    main()
