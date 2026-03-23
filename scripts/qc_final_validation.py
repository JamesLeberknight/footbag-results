#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import pandas as pd

CSV_PATH = Path("out/Placements_Flat.csv")
OUT_GROUPS = Path("out/qc_final_validation_groups.csv")
OUT_HITS = Path("out/qc_final_validation_hits.csv")
OUT_SUMMARY = Path("out/qc_final_validation_summary.csv")


CONTEST_TERMS = [
    "circle",
    "request",
    "sick",
    "shred",
    "battle",
    "consecutive",
    "consecutives",
]

SEEDING_TERMS = [
    "seeding",
]

ROUND_TERMS = [
    "prelim",
    "prelims",
    "preliminary",
    "pool",
    "pools",
    "semi",
    "semis",
    "semi-final",
    "semifinal",
    "quarterfinal",
    "quarter-final",
    "final",
    "finals",
]

COMPETITIVE_HINTS = [
    "open",
    "pro",
    "intermediate",
    "novice",
    "women",
    "women's",
    "womens",
    "master",
    "grand master",
    "singles",
    "doubles",
    "freestyle",
    "routine",
    "net",
]


def first_existing(df: pd.DataFrame, names: list[str]) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def contains_any(series: pd.Series, terms: list[str]) -> pd.Series:
    pattern = "|".join(terms)
    return series.fillna("").astype(str).str.contains(pattern, case=False, regex=True)


def classify_row(row: pd.Series) -> str:
    if row["is_seeding"]:
        return "SEEDING_EXPECTED"
    if row["is_contest"]:
        return "CONTEST_EXPECTED"

    # strongest true-error signals first
    if row["exact_dup_rows"] > 0:
        return "EXACT_DUPLICATE_ROWS"
    if row["same_person_multi_places"] > 0 and row["has_round_terms"]:
        return "LIKELY_POOL_FINALS_MERGE"
    if row["many_first_place_rows"] and row["competitive_like"]:
        return "LIKELY_MERGED_DIVISION"
    if row["repeated_place_gt4"] and row["competitive_like"]:
        return "LIKELY_MERGED_DIVISION"
    if row["min_place"] > 1:
        return "PARTIAL_OR_TRUNCATED_RESULTS"
    if row["same_person_multi_places"] > 0:
        return "REVIEW_MULTI_PLACE_PERSON"
    if row["too_many_rows_vs_places"] and row["competitive_like"]:
        return "REVIEW_DENSE_COMPETITIVE_DIVISION"

    return "OK"


def severity_for_class(cls: str) -> int:
    order = {
        "EXACT_DUPLICATE_ROWS": 90,
        "LIKELY_POOL_FINALS_MERGE": 80,
        "LIKELY_MERGED_DIVISION": 75,
        "PARTIAL_OR_TRUNCATED_RESULTS": 60,
        "REVIEW_MULTI_PLACE_PERSON": 50,
        "REVIEW_DENSE_COMPETITIVE_DIVISION": 45,
        "SEEDING_EXPECTED": 10,
        "CONTEST_EXPECTED": 10,
        "OK": 0,
    }
    return order.get(cls, 0)


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing file: {CSV_PATH}")

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

    df["_division"] = df[division_col].fillna("").astype(str).str.strip()
    df["_person"] = df[person_col].fillna("").astype(str).str.strip()
    df["_place_num"] = pd.to_numeric(df[place_col], errors="coerce")

    if event_name_col:
        df["_event_name"] = df[event_name_col].fillna("").astype(str).str.strip()
    else:
        df["_event_name"] = ""

    # exact duplicate rows
    exact_dup = (
        df.groupby([event_id_col, division_col, place_col, person_col], dropna=False)
        .size()
        .reset_index(name="n_exact")
    )
    exact_dup = exact_dup[exact_dup["n_exact"] > 1]
    exact_dup_div = (
        exact_dup.groupby([event_id_col, division_col], dropna=False)
        .size()
        .reset_index(name="exact_dup_rows")
    )

    # same person appears at multiple places in same event+division
    multi_place_person = (
        df.groupby([event_id_col, division_col, person_col], dropna=False)["_place_num"]
        .nunique(dropna=True)
        .reset_index(name="n_places_for_person")
    )
    multi_place_person = multi_place_person[multi_place_person["n_places_for_person"] > 1]
    multi_place_div = (
        multi_place_person.groupby([event_id_col, division_col], dropna=False)
        .size()
        .reset_index(name="same_person_multi_places")
    )

    # count rows per place
    place_counts = (
        df.groupby([event_id_col, division_col, "_place_num"], dropna=False)
        .size()
        .reset_index(name="rows_at_place")
    )

    repeated_place_stats = (
        place_counts.groupby([event_id_col, division_col], dropna=False)
        .agg(
            max_rows_same_place=("rows_at_place", "max"),
            repeated_place_gt2=("rows_at_place", lambda s: int((s > 2).sum())),
            repeated_place_gt4=("rows_at_place", lambda s: int((s > 4).sum())),
        )
        .reset_index()
    )

    first_place = place_counts[place_counts["_place_num"] == 1].copy()
    first_place = first_place.rename(columns={"rows_at_place": "n_first_place_rows"})
    first_place = first_place[[event_id_col, division_col, "n_first_place_rows"]]

    # base group stats
    grouped = (
        df.groupby([event_id_col, division_col], dropna=False)
        .agg(
            rows=(person_col, "size"),
            n_people=(person_col, "nunique"),
            min_place=("_place_num", "min"),
            max_place=("_place_num", "max"),
            unique_places=("_place_num", "nunique"),
        )
        .reset_index()
    )

    # event metadata
    meta_cols = [c for c in [event_id_col, year_col, event_name_col] if c is not None]
    meta = df[meta_cols].drop_duplicates(subset=[event_id_col])

    qc = grouped.merge(meta, on=[event_id_col], how="left")
    qc = qc.merge(exact_dup_div, on=[event_id_col, division_col], how="left")
    qc = qc.merge(multi_place_div, on=[event_id_col, division_col], how="left")
    qc = qc.merge(repeated_place_stats, on=[event_id_col, division_col], how="left")
    qc = qc.merge(first_place, on=[event_id_col, division_col], how="left")

    for col in [
        "exact_dup_rows",
        "same_person_multi_places",
        "max_rows_same_place",
        "repeated_place_gt2",
        "repeated_place_gt4",
        "n_first_place_rows",
    ]:
        qc[col] = qc[col].fillna(0).astype(int)

    qc["division_lower"] = qc[division_col].fillna("").astype(str).str.lower()
    qc["event_lower"] = qc[event_name_col].fillna("").astype(str).str.lower() if event_name_col else ""

    qc["is_contest"] = contains_any(qc["division_lower"], CONTEST_TERMS)
    qc["is_seeding"] = contains_any(qc["division_lower"], SEEDING_TERMS)
    qc["has_round_terms"] = (
        contains_any(qc["division_lower"], ROUND_TERMS) |
        contains_any(qc["event_lower"], ROUND_TERMS)
    )
    qc["competitive_like"] = contains_any(qc["division_lower"], COMPETITIVE_HINTS)

    # heuristics
    qc["many_first_place_rows"] = qc["n_first_place_rows"] > 2
    qc["too_many_rows_vs_places"] = qc["rows"] > (qc["unique_places"] * 3)
    qc["place_gap_big"] = qc["max_place"] > (qc["unique_places"] * 3)

    qc["classification"] = qc.apply(classify_row, axis=1)
    qc["severity"] = qc["classification"].map(severity_for_class)

    preferred = [
        event_id_col,
        year_col,
        event_name_col,
        division_col,
        "classification",
        "severity",
        "rows",
        "n_people",
        "min_place",
        "max_place",
        "unique_places",
        "exact_dup_rows",
        "same_person_multi_places",
        "n_first_place_rows",
        "max_rows_same_place",
        "repeated_place_gt2",
        "repeated_place_gt4",
        "is_contest",
        "is_seeding",
        "has_round_terms",
        "competitive_like",
        "many_first_place_rows",
        "too_many_rows_vs_places",
        "place_gap_big",
    ]
    preferred = [c for c in preferred if c in qc.columns]
    qc = qc[preferred]

    hits = qc[qc["classification"] != "OK"].copy()
    hits = hits.sort_values(
        ["severity", "rows", "same_person_multi_places", "exact_dup_rows"],
        ascending=[False, False, False, False]
    )

    summary = (
        hits.groupby("classification", dropna=False)
        .size()
        .reset_index(name="division_count")
        .sort_values(["division_count", "classification"], ascending=[False, True])
    )

    OUT_GROUPS.parent.mkdir(parents=True, exist_ok=True)
    qc.to_csv(OUT_GROUPS, index=False)
    hits.to_csv(OUT_HITS, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    print(f"Total event+division groups: {len(qc):,}")
    print(f"Flagged groups: {len(hits):,}")
    print(f"Wrote: {OUT_GROUPS}")
    print(f"Wrote: {OUT_HITS}")
    print(f"Wrote: {OUT_SUMMARY}")

    if not summary.empty:
        print("\nSummary by classification:")
        print(summary.to_string(index=False))

    print("\nTop 40 flagged groups:")
    print(hits.head(40).to_string(index=False))


if __name__ == "__main__":
    main()
