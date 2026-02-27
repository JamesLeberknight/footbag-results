#!/usr/bin/env python3
"""
18_migrate_identity_lock.py — Produce updated identity lock files (v22 Truth,
v20 Unresolved, v23 Placements) by processing approved migration categories.

Categories processed:

  1. NON_PERSON_SLOP (exclusion_reason == "NON_PERSON_SLOP")
     + LIKELY_NON_PERSON (unresolved_class == "LIKELY_NON_PERSON")
     → Remove from Unresolved; set person_canon = "__NON_PERSON__" in Placements

  2. COVERAGE_CLOSURE (exclusion_reason == "COVERAGE_CLOSURE")
     → Promote to Truth; set person_unresolved = "" in Placements; remove from Unresolved

  3. Fuzzy resolutions (out/backfill_resolutions.csv — optional)
     → Update Placements person_id/person_canon to resolved Truth person; remove from Unresolved

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v21.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v19.csv
  inputs/identity_lock/Placements_ByPerson_v22.csv
  out/backfill_resolutions.csv  (optional — from tool 15 --apply)

Outputs:
  inputs/identity_lock/Persons_Truth_Final_v22.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v20.csv
  inputs/identity_lock/Placements_ByPerson_v23.csv

Modes:
  default  — dry run: print summary of changes, write nothing
  --apply  — write output files to inputs/identity_lock/
  --out_dir <path>  — override output directory (for testing)

Usage:
  python tools/18_migrate_identity_lock.py                   # dry run
  python tools/18_migrate_identity_lock.py --apply           # write v19/v17 files
  python tools/18_migrate_identity_lock.py --apply --out_dir /tmp/test_out
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"
OUT = ROOT / "out"

TRUTH_IN = IDENTITY_LOCK / "Persons_Truth_Final_v22.csv"
UNRESOLVED_IN = IDENTITY_LOCK / "Persons_Unresolved_Organized_v19.csv"
PLACEMENTS_IN = IDENTITY_LOCK / "Placements_ByPerson_v23.csv"
RESOLUTIONS_CSV = OUT / "backfill_resolutions.csv"

TRUTH_OUT_NAME = "Persons_Truth_Final_v23.csv"
UNRESOLVED_OUT_NAME = "Persons_Unresolved_Organized_v20.csv"
PLACEMENTS_OUT_NAME = "Placements_ByPerson_v24.csv"

NON_PERSON_CANON = "__NON_PERSON__"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip(v: object) -> str:
    return str(v).strip() if v is not None else ""


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str).fillna("")


# ---------------------------------------------------------------------------
# Main migration logic
# ---------------------------------------------------------------------------

def run_migration(apply: bool, out_dir: Path) -> int:
    # ---- Load inputs -------------------------------------------------------
    for p in [TRUTH_IN, UNRESOLVED_IN, PLACEMENTS_IN]:
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr)
            return 2

    truth = _load_csv(TRUTH_IN)
    unresolved = _load_csv(UNRESOLVED_IN)
    placements = _load_csv(PLACEMENTS_IN)

    print(f"Loaded Truth:      {len(truth):5d} rows  ({TRUTH_IN.name})")
    print(f"Loaded Unresolved: {len(unresolved):5d} rows  ({UNRESOLVED_IN.name})")
    print(f"Loaded Placements: {len(placements):5d} rows  ({PLACEMENTS_IN.name})")
    print()

    # ---- Identify migration sets -------------------------------------------

    # NON_PERSON_SLOP: rows to remove from Unresolved, mark placements as __NON_PERSON__
    mask_nps_reason = unresolved["exclusion_reason"] == "NON_PERSON_SLOP"
    mask_lnp_class = unresolved["unresolved_class"] == "LIKELY_NON_PERSON"
    nps_rows = unresolved[mask_nps_reason | mask_lnp_class].copy()
    nps_eids = set(nps_rows["effective_person_id"].str.strip()) - {""}

    # COVERAGE_CLOSURE: rows to promote to Truth, remove from Unresolved
    cc_rows = unresolved[unresolved["exclusion_reason"] == "COVERAGE_CLOSURE"].copy()
    cc_eids = set(cc_rows["effective_person_id"].str.strip()) - {""}

    # Sanity: no overlap between nps and cc
    overlap = nps_eids & cc_eids
    if overlap:
        print(f"ERROR: {len(overlap)} effective_person_ids appear in both NON_PERSON_SLOP and "
              f"COVERAGE_CLOSURE: {list(overlap)[:5]}", file=sys.stderr)
        return 1

    # Fuzzy resolutions (optional)
    resolutions: pd.DataFrame = pd.DataFrame(
        columns=["unresolved_person_id", "resolved_to_person_id"]
    )
    if RESOLUTIONS_CSV.exists():
        resolutions = _load_csv(RESOLUTIONS_CSV)
        print(f"Loaded resolutions: {len(resolutions):5d} rows  ({RESOLUTIONS_CSV.name})")
    else:
        print(f"(no backfill_resolutions.csv found — skipping fuzzy resolution step)")
    print()

    # ---- 1. NON_PERSON_SLOP processing ------------------------------------

    print(f"[1] NON_PERSON_SLOP + LIKELY_NON_PERSON: {len(nps_rows)} rows to remove from Unresolved")

    # Find Placements rows affected by NPS (by effective_person_id)
    nps_placements_mask = placements["person_id"].isin(nps_eids)
    nps_placements_count = nps_placements_mask.sum()
    print(f"    Placements rows to mark __NON_PERSON__: {nps_placements_count}")

    if nps_placements_count > 0:
        # Show what will change
        affected = placements[nps_placements_mask][["person_id", "person_canon"]].drop_duplicates()
        for _, row in affected.iterrows():
            print(f"      {row['person_canon']!r} (pid={row['person_id'][:8]}...) → __NON_PERSON__")

    # ---- 2. COVERAGE_CLOSURE processing ------------------------------------

    print(f"\n[2] COVERAGE_CLOSURE: {len(cc_rows)} rows to promote to Truth")

    # Find Placements rows affected by CC
    cc_placements_mask = placements["person_id"].isin(cc_eids)
    cc_placements_count = cc_placements_mask.sum()
    print(f"    Placements rows to mark person_unresolved='': {cc_placements_count}")

    # Check for person_canon collisions with existing Truth
    existing_truth_canons = set(truth["person_canon"].str.strip())
    existing_truth_eids = set(truth["effective_person_id"].str.strip())
    cc_canon_collisions = [
        r["person_canon"] for _, r in cc_rows.iterrows()
        if _strip(r["person_canon"]) in existing_truth_canons
    ]
    cc_eid_collisions = [
        r["effective_person_id"] for _, r in cc_rows.iterrows()
        if _strip(r["effective_person_id"]) in existing_truth_eids
    ]
    if cc_canon_collisions:
        print(f"    WARNING: {len(cc_canon_collisions)} person_canon collisions with existing Truth:")
        for c in cc_canon_collisions[:5]:
            print(f"      {c!r}")
    if cc_eid_collisions:
        print(f"    WARNING: {len(cc_eid_collisions)} effective_person_id collisions with existing Truth:")
        for e in cc_eid_collisions[:5]:
            print(f"      {e}")

    # ---- 3. Fuzzy resolution processing ------------------------------------

    print(f"\n[3] Fuzzy resolutions: {len(resolutions)} rows")
    fuzzy_placements_count = 0
    fuzzy_unresolved_to_remove: set[str] = set()  # person_canons to remove from Unresolved

    # Build Truth lookup: effective_person_id → person_canon
    truth_eid_to_canon: dict[str, str] = {
        _strip(r["effective_person_id"]): _strip(r["person_canon"])
        for _, r in truth.iterrows()
        if _strip(r["effective_person_id"])
    }

    # Validate resolutions.
    # Two sub-cases:
    #   A) unresolved_person_id non-empty: update Placements by person_id
    #   B) unresolved_person_id empty but unresolved_canon non-empty: update Placements
    #      by person_canon (encoding-corrupted ACB entries whose person_id was blank)
    resolution_by_pid: list[dict] = []    # case A
    resolution_by_canon: list[dict] = []  # case B
    for _, row in resolutions.iterrows():
        unres_pid = _strip(row.get("unresolved_person_id", ""))
        unres_canon = _strip(row.get("unresolved_canon", ""))
        resolved_pid = _strip(row.get("resolved_to_person_id", ""))
        if not resolved_pid:
            print(f"    WARNING: skipping resolution with empty resolved_to_person_id "
                  f"(canon={unres_canon!r})")
            continue
        if resolved_pid not in truth_eid_to_canon:
            print(f"    WARNING: resolved_to_person_id {resolved_pid!r} not in Truth — skipping "
                  f"(canon={unres_canon!r})")
            continue
        entry = {
            "unresolved_person_id": unres_pid,
            "unresolved_canon": unres_canon,
            "resolved_to_person_id": resolved_pid,
            "resolved_canon": truth_eid_to_canon[resolved_pid],
        }
        if unres_pid:
            resolution_by_pid.append(entry)
        elif unres_canon:
            resolution_by_canon.append(entry)
        else:
            print(f"    WARNING: skipping resolution with both empty person_id and canon")

    resolution_valid = resolution_by_pid + resolution_by_canon
    if resolution_valid:
        # Case A: by person_id
        unres_pids_to_resolve = {r["unresolved_person_id"] for r in resolution_by_pid}
        mask_by_pid = placements["person_id"].isin(unres_pids_to_resolve)
        # Case B: by person_canon (blank-pid rows)
        unres_canons_to_resolve = {r["unresolved_canon"] for r in resolution_by_canon}
        mask_by_canon = placements["person_canon"].isin(unres_canons_to_resolve)
        fuzzy_placements_count = int(mask_by_pid.sum()) + int(mask_by_canon.sum())

        print(f"    Valid resolutions: {len(resolution_valid)} "
              f"({len(resolution_by_pid)} by-pid + {len(resolution_by_canon)} by-canon)")
        print(f"    Placements rows to remap: {fuzzy_placements_count} "
              f"({int(mask_by_pid.sum())} by-pid + {int(mask_by_canon.sum())} by-canon)")

        # Identify which Unresolved ACB rows to remove.
        # Case A: look up canon from Placements via person_id.
        pid_to_canon_in_pf: dict[str, str] = {}
        for _, pfrow in placements[mask_by_pid].iterrows():
            pid = _strip(pfrow["person_id"])
            canon = _strip(pfrow["person_canon"])
            if pid and canon:
                pid_to_canon_in_pf[pid] = canon
        for r in resolution_by_pid:
            canon = pid_to_canon_in_pf.get(r["unresolved_person_id"], "")
            if canon:
                fuzzy_unresolved_to_remove.add(canon)
        # Case B: directly use unresolved_canon from the resolution row.
        for r in resolution_by_canon:
            fuzzy_unresolved_to_remove.add(r["unresolved_canon"])

        print(f"    Unresolved ACB rows to remove: {len(fuzzy_unresolved_to_remove)}")
    else:
        print(f"    No valid resolutions to apply.")

    # ---- Summary -----------------------------------------------------------

    new_truth_count = len(truth) + len(cc_rows)
    rows_removed_from_unresolved = len(nps_rows) + len(cc_rows) + len(fuzzy_unresolved_to_remove)
    new_unresolved_count = len(unresolved) - rows_removed_from_unresolved
    print()
    print("=" * 60)
    print("Summary of changes:")
    print(f"  Truth rows:      {len(truth):5d} → {new_truth_count:5d} (+{len(cc_rows)} COVERAGE_CLOSURE)")
    print(f"  Unresolved rows: {len(unresolved):5d} → {new_unresolved_count:5d} "
          f"(-{rows_removed_from_unresolved}: {len(nps_rows)} NPS/LNP + "
          f"{len(cc_rows)} CC + {len(fuzzy_unresolved_to_remove)} fuzzy)")
    print(f"  Placements rows: {len(placements):5d} → {len(placements):5d} (count preserved, values updated)")
    print(f"    NPS marks:       {nps_placements_count}")
    print(f"    CC unresolved:   {cc_placements_count}")
    print(f"    Fuzzy remaps:    {fuzzy_placements_count}")
    print("=" * 60)

    if not apply:
        print()
        print("[DRY RUN] No files written. Re-run with --apply to write output files.")
        return 0

    # ---- Apply: build output DataFrames ------------------------------------

    print()
    print("Applying changes...")

    # --- Placements: apply all mutations in sequence ---
    pf_out = placements.copy()

    # 1. NPS: set person_canon = __NON_PERSON__
    if nps_placements_count > 0:
        pf_out.loc[pf_out["person_id"].isin(nps_eids), "person_canon"] = NON_PERSON_CANON

    # 2. CC: set person_unresolved = "" (clear the flag)
    if cc_placements_count > 0:
        pf_out.loc[pf_out["person_id"].isin(cc_eids), "person_unresolved"] = ""

    # 3. Fuzzy: remap person_id and person_canon
    for r in resolution_by_pid:
        # Case A: match by person_id
        unres_pid = r["unresolved_person_id"]
        resolved_pid = r["resolved_to_person_id"]
        resolved_canon = r["resolved_canon"]
        mask = pf_out["person_id"] == unres_pid
        pf_out.loc[mask, "person_id"] = resolved_pid
        pf_out.loc[mask, "person_canon"] = resolved_canon
        pf_out.loc[mask, "person_unresolved"] = ""
    for r in resolution_by_canon:
        # Case B: match by person_canon (blank-pid rows — encoding-corrupted names)
        unres_canon = r["unresolved_canon"]
        resolved_pid = r["resolved_to_person_id"]
        resolved_canon = r["resolved_canon"]
        mask = pf_out["person_canon"] == unres_canon
        pf_out.loc[mask, "person_id"] = resolved_pid
        pf_out.loc[mask, "person_canon"] = resolved_canon
        pf_out.loc[mask, "person_unresolved"] = ""

    assert len(pf_out) == len(placements), (
        f"Placements row count changed: {len(placements)} → {len(pf_out)}"
    )

    # --- Unresolved: remove processed rows ---
    # Rows to remove:
    #   - NPS/LNP: by effective_person_id
    #   - CC: by effective_person_id
    #   - Fuzzy: by person_canon (ACB rows have no effective_person_id)
    eids_to_remove = nps_eids | cc_eids
    ur_out = unresolved[
        ~(
            (unresolved["effective_person_id"].isin(eids_to_remove)) |
            (unresolved["person_canon"].isin(fuzzy_unresolved_to_remove))
        )
    ].copy()

    # --- Truth: add COVERAGE_CLOSURE promotions ---
    # Map Unresolved columns to Truth columns (take the intersection)
    truth_cols = list(truth.columns)
    ur_cols = set(unresolved.columns)
    cc_for_truth = cc_rows[[c for c in truth_cols if c in ur_cols]].copy()
    # Fill any Truth columns not in Unresolved with empty string
    for col in truth_cols:
        if col not in cc_for_truth.columns:
            cc_for_truth[col] = ""
    cc_for_truth = cc_for_truth[truth_cols]  # reorder to match Truth column order

    truth_out = pd.concat([truth, cc_for_truth], ignore_index=True)

    # ---- Validate ----------------------------------------------------------

    # No duplicate effective_person_id in new Truth
    truth_eids = truth_out["effective_person_id"].str.strip()
    eid_counts = truth_eids[truth_eids != ""].value_counts()
    eid_dups = eid_counts[eid_counts > 1]
    if not eid_dups.empty:
        print(f"ERROR: {len(eid_dups)} duplicate effective_person_ids in new Truth:", file=sys.stderr)
        for eid, cnt in eid_dups.head(5).items():
            print(f"  {eid} (count={cnt})", file=sys.stderr)
        return 1

    # No duplicate person_canon in new Truth
    canon_counts = truth_out["person_canon"].str.strip().value_counts()
    canon_dups = canon_counts[canon_counts > 1]
    if not canon_dups.empty:
        print(f"WARNING: {len(canon_dups)} duplicate person_canons in new Truth:")
        for canon, cnt in canon_dups.head(5).items():
            print(f"  {canon!r} (count={cnt})")

    # Placements row count preserved
    if len(pf_out) != len(placements):
        print(f"ERROR: Placements row count changed {len(placements)} → {len(pf_out)}", file=sys.stderr)
        return 1

    # ---- Write outputs -----------------------------------------------------

    out_dir.mkdir(parents=True, exist_ok=True)

    truth_out_path = out_dir / TRUTH_OUT_NAME
    ur_out_path = out_dir / UNRESOLVED_OUT_NAME
    pf_out_path = out_dir / PLACEMENTS_OUT_NAME

    truth_out.to_csv(truth_out_path, index=False)
    ur_out.to_csv(ur_out_path, index=False)
    pf_out.to_csv(pf_out_path, index=False)

    print()
    print(f"Written: {truth_out_path}  ({len(truth_out)} rows)")
    print(f"Written: {ur_out_path}  ({len(ur_out)} rows)")
    print(f"Written: {pf_out_path}  ({len(pf_out)} rows)")
    print()
    print("Final counts:")
    print(f"  Persons_Truth_Final_v20.csv:          {len(truth_out):5d} rows")
    print(f"  Persons_Unresolved_Organized_v15.csv: {len(ur_out):5d} rows")
    print(f"  Placements_ByPerson_v18.csv:          {len(pf_out):5d} rows")
    print()
    print("Next steps:")
    print("  1. Verify output files look correct")
    print("  2. Update RELEASE_CHECKLIST.md to reference v17/v15 inputs")
    print("  3. Run full pipeline and confirm QC passes")
    print("  4. Cut v2.0.0")
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate identity lock files: NON_PERSON_SLOP, COVERAGE_CLOSURE, fuzzy resolutions.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Write output files (default is dry run — print summary only)",
    )
    parser.add_argument(
        "--out_dir", type=Path, default=IDENTITY_LOCK,
        help="Override output directory (default: inputs/identity_lock/)",
    )
    args = parser.parse_args()

    return run_migration(apply=args.apply, out_dir=args.out_dir)


if __name__ == "__main__":
    sys.exit(main())
