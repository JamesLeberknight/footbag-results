#!/usr/bin/env python3
"""
Export release_publication/events.csv → footbag-platform canonical_input/events.csv
in the platform schema expected by 07_build_mvfp_seed_full.py.

Source: out/release_publication/events.csv  (761 filtered events, PRE+POST1997)
Schema differences:
  event_id       → event_key  (slug-based)
  no legacy_hex_id → legacy_event_id left blank (platform doesn't join on it)
"""
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    repo_root = Path(__file__).resolve().parents[1]
    ap.add_argument(
        "--input",
        default=str(repo_root / "out" / "release_publication" / "events.csv"),
        help="Source release_publication events.csv",
    )
    ap.add_argument(
        "--output",
        default="/home/james/projects/footbag-platform/legacy_data/event_results/canonical_input/events.csv",
        help="Destination platform canonical_input events.csv",
    )
    args = ap.parse_args()

    in_path = Path(args.input).expanduser().resolve()
    out_path = Path(args.output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path, dtype=str).fillna("")

    required = ["event_id", "event_name", "year", "start_date", "end_date",
                "city", "region", "country", "host_club", "status"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"Input events.csv missing required columns: {missing}")

    out = pd.DataFrame({
        "event_key":        df["event_id"].str.strip(),
        "legacy_event_id":  "",                          # not available in release_publication
        "year":             df["year"].str.strip(),
        "event_name":       df["event_name"].str.strip(),
        "event_slug":       df["event_id"].str.strip(),  # same as event_key
        "start_date":       df["start_date"].str.strip(),
        "end_date":         df["end_date"].str.strip(),
        "city":             df["city"].str.strip(),
        "region":           df["region"].str.strip(),
        "country":          df["country"].str.strip(),
        "host_club":        df["host_club"].str.strip(),
        "status":           df["status"].str.strip(),
        "notes":            df.get("validation_status", pd.Series([""] * len(df))).str.strip(),
        "source":           df.get("data_source", pd.Series([""] * len(df))).str.strip(),
    })

    for col in out.columns:
        out[col] = out[col].fillna("").replace({"nan": ""})

    out = out.sort_values(["year", "event_name", "event_key"], kind="stable")

    out.to_csv(out_path, index=False)
    print(f"Wrote: {out_path}")
    print(f"Rows:  {len(out):,}")
    print(f"Cols:  {', '.join(out.columns)}")


if __name__ == "__main__":
    main()
