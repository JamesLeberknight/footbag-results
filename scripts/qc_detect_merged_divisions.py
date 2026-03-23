#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import pandas as pd

CSV_PATH = Path("out/Placements_Flat.csv")
OUT_PATH = Path("out/qc_merged_division_candidates.csv")


def first_existing(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def main() -> None:
    df = pd.read_csv(CSV_PATH, low_memory=False)

    event_id_col = first_existing(df, ["event_id"])
    year_col = first_existing(df, ["year"])
    event_name_col = first_existing(df, ["event_name", "event"])
    division_col = first_existing(df, ["division_canon", "division_raw", "division"])
    person_col = first_existing(df, ["person_canon", "player_name", "person_name", "name", "person"])
    place_col = first_existing(df, ["place"])

    required = [event_id_col, division_col, person_col, place_col]
    if any(c is None for c in required):
        raise ValueError(f"Missing required columns. Found columns: {df.columns.tolist()}")

    df["_place_num"] = pd.to_numeric(df[place_col], errors="coerce")

    # Per event+division summary
    grp = (
        df.groupby([event_id_col, division_col], dropna=False)
        .agg(
            n_rows=(person_col, "size"),
            n_people=(person_col, "nunique"),
            n_places=("_place_num", "nunique"),
            min_place=("_place_num", "min"),
            max_place=("_place_num", "max"),
        )
        .reset_index()
    )

    # How many rows occur at each place?
    place_counts = (
        df.groupby([event_id_col, division_col, "_place_num"], dropna=False)
        .size()
        .reset_index(name="rows_at_place")
    )

    # Repeated place runs beyond normal doubles size
    repeated = (
        place_counts.groupby([event_id_col, division_col], dropna=False)
        .agg(
            n_places_repeated_gt2=("rows_at_place", lambda s: int((s > 2).sum())),
            n_places_repeated_gt4=("rows_at_place", lambda s: int((s > 4).sum())),
            max_rows_same_place=("rows_at_place", "max"),
        )
        .reset_index()
    )

    # First-place pressure
    first_rows = place_counts[place_counts["_place_num"] == 1].copy()
    first_rows = first_rows.rename(columns={"rows_at_place": "n_first_place_rows"})
    first_rows = first_rows[[event_id_col, division_col, "n_first_place_rows"]]

    out = grp.merge(repeated, on=[event_id_col, division_col], how="left")
    out = out.merge(first_rows, on=[event_id_col, division_col], how="left")

    out["n_places_repeated_gt2"] = out["n_places_repeated_gt2"].fillna(0).astype(int)
    out["n_places_repeated_gt4"] = out["n_places_repeated_gt4"].fillna(0).astype(int)
    out["max_rows_same_place"] = out["max_rows_same_place"].fillna(0).astype(int)
    out["n_first_place_rows"] = out["n_first_place_rows"].fillna(0).astype(int)

    # Heuristics
    out["flag_many_firsts"] = out["n_first_place_rows"] > 2
    out["flag_repeated_places_gt4"] = out["n_places_repeated_gt4"] > 0
    out["flag_low_place_variety"] = (out["n_rows"] >= 6) & (out["n_places"] <= 2)
    out["flag_partial"] = out["min_place"] > 1
    out["flag_dense_repeat_pattern"] = (
        (out["n_places_repeated_gt2"] >= 2) &
        (out["n_rows"] >= 8)
    )

    # Suspicion score
    out["merged_division_score"] = (
        out["flag_many_firsts"].astype(int) * 3 +
        out["flag_repeated_places_gt4"].astype(int) * 3 +
        out["flag_low_place_variety"].astype(int) * 2 +
        out["flag_dense_repeat_pattern"].astype(int) * 2 +
        out["flag_partial"].astype(int) * 1
    )

    # Optional event name/year
    meta_cols = [c for c in [event_id_col, year_col, event_name_col] if c is not None]
    meta = df[meta_cols].drop_duplicates(subset=[event_id_col])
    out = out.merge(meta, on=[event_id_col], how="left")

    # Keep only meaningful candidates
    candidates = out[
        (out["merged_division_score"] >= 3)
    ].copy()

    # Rank highest suspicion first
    sort_cols = ["merged_division_score", "n_first_place_rows", "max_rows_same_place", "n_rows"]
    candidates = candidates.sort_values(sort_cols, ascending=[False, False, False, False])

    # Friendly column order
    preferred = [
        event_id_col,
        year_col,
        event_name_col,
        division_col,
        "merged_division_score",
        "n_rows",
        "n_people",
        "n_places",
        "min_place",
        "max_place",
        "n_first_place_rows",
        "max_rows_same_place",
        "n_places_repeated_gt2",
        "n_places_repeated_gt4",
        "flag_many_firsts",
        "flag_repeated_places_gt4",
        "flag_low_place_variety",
        "flag_dense_repeat_pattern",
        "flag_partial",
    ]
    preferred = [c for c in preferred if c in candidates.columns]
    candidates = candidates[preferred]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(OUT_PATH, index=False)

    print(f"Rows scanned: {len(df):,}")
    print(f"Candidate merged divisions: {len(candidates):,}")
    print(f"Wrote: {OUT_PATH}")
    print("\nTop 30 candidates:")
    print(candidates.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
