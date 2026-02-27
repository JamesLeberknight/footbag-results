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

This satisfies the v1.0 canonical contract.
"""

import argparse
import os
import sys
import pandas as pd


def build_from_identity_lock(args):
    print("[02p5] Identity-lock mode ENABLED")
    print(f"[02p5] Loading authoritative placements: {args.identity_lock_placements_csv}")

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

    # Structural normalization only
    df_flat = df.copy()

    # division_raw is not available in locked data; derive deterministically
    if "division_raw" not in df_flat.columns:
        df_flat["division_raw"] = df_flat["division_canon"]

    out_dir = args.out_dir or "out"
    os.makedirs(out_dir, exist_ok=True)

    out_flat = os.path.join(out_dir, "Placements_Flat.csv")
    out_by_person = os.path.join(out_dir, "Placements_ByPerson.csv")

    df_flat.to_csv(out_flat, index=False)
    df.to_csv(out_by_person, index=False)

    print(f"[02p5] Wrote {out_flat}")
    print(f"[02p5] Wrote {out_by_person}")
    print(f"[02p5] Rows preserved: {len(df_flat)}")

    return 0


def main():
    parser = argparse.ArgumentParser(description="Stage 02.5 — Player token cleanup")

    parser.add_argument("--identity_lock_placements_csv")
    parser.add_argument("--out_dir", default="out")

    args, _ = parser.parse_known_args()

    if args.identity_lock_placements_csv:
        return build_from_identity_lock(args)

    print("ERROR: Non-locked (heuristic) mode disabled for v1.0 canonical release.")
    print("Use --identity_lock_placements_csv.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
