#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


# Terms that are better treated as style / modifier markers
STYLE_TERMS = {
    "atomic",
    "atomic set",
    "stepping",
    "ducking",
    "spinning",
    "symposium",
    "gyro",
    "pixie",
    "blazing",
    "barraging",
    "tapping",
}

# Terms that should generally count as "full tricks"
# Anything not in STYLE_TERMS will also fall into this bucket by default.
# This set exists mainly so you can refine behavior later if needed.
FULL_TRICK_EXCLUSIONS = set()


def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required input not found: {path}")
    return pd.read_csv(path)


def ensure_cols(df: pd.DataFrame, required: list[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}")


def classify_trick(trick: str) -> str:
    t = str(trick).strip().lower()
    if not t:
        return "unknown"
    if t in STYLE_TERMS:
        return "style_term"
    if t in FULL_TRICK_EXCLUSIONS:
        return "exclude"
    return "full_trick"


def first_nonnull(series: pd.Series):
    s = series.dropna()
    return s.iloc[0] if len(s) else None


def build_player_trick_profile(tricks: pd.DataFrame) -> pd.DataFrame:
    x = tricks.copy()
    x = x[x["person_id"].notna()].copy()
    x["year_num"] = pd.to_numeric(x["year"], errors="coerce")
    x["trick_type"] = x["trick_canon"].map(classify_trick)

    grouped = (
        x.groupby(["person_id", "person_canon", "trick_canon", "trick_type"], dropna=False)
        .agg(
            mentions=("trick_canon", "size"),
            first_year=("year_num", "min"),
            last_year=("year_num", "max"),
            event_count=("event_id", pd.Series.nunique),
            sample_event=("event_name", first_nonnull),
            sample_line=("line_raw", first_nonnull),
        )
        .reset_index()
        .sort_values(
            ["mentions", "event_count", "person_canon", "trick_canon"],
            ascending=[False, False, True, True],
        )
    )

    return grouped


def build_trick_leaderboard(tricks: pd.DataFrame) -> pd.DataFrame:
    x = tricks.copy()
    x["year_num"] = pd.to_numeric(x["year"], errors="coerce")
    x["trick_type"] = x["trick_canon"].map(classify_trick)

    grouped = (
        x.groupby(["trick_canon", "trick_type"], dropna=False)
        .agg(
            mentions=("trick_canon", "size"),
            resolved_mentions=("person_id", lambda s: int(s.notna().sum())),
            unique_players=("person_id", lambda s: int(s.dropna().nunique())),
            unique_events=("event_id", pd.Series.nunique),
            first_year=("year_num", "min"),
            last_year=("year_num", "max"),
            sample_event=("event_name", first_nonnull),
            sample_line=("line_raw", first_nonnull),
        )
        .reset_index()
        .sort_values(
            ["mentions", "resolved_mentions", "unique_players", "trick_canon"],
            ascending=[False, False, False, True],
        )
    )

    return grouped


def build_player_style_profile(tricks: pd.DataFrame) -> pd.DataFrame:
    x = tricks.copy()
    x = x[x["person_id"].notna()].copy()
    x["trick_type"] = x["trick_canon"].map(classify_trick)
    x = x[x["trick_type"] == "style_term"].copy()

    if x.empty:
        return pd.DataFrame(
            columns=[
                "person_id",
                "person_canon",
                "style_mentions_total",
                "distinct_style_terms",
            ]
        )

    pivot = (
        x.groupby(["person_id", "person_canon", "trick_canon"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    style_cols = [c for c in pivot.columns if c not in {"person_id", "person_canon"}]
    pivot["style_mentions_total"] = pivot[style_cols].sum(axis=1)
    pivot["distinct_style_terms"] = (pivot[style_cols] > 0).sum(axis=1)

    ordered_cols = (
        ["person_id", "person_canon", "style_mentions_total", "distinct_style_terms"]
        + sorted(style_cols)
    )
    pivot = pivot[ordered_cols].sort_values(
        ["style_mentions_total", "distinct_style_terms", "person_canon"],
        ascending=[False, False, True],
    )

    return pivot


def build_player_score_profile(scores: pd.DataFrame) -> pd.DataFrame:
    x = scores.copy()
    x = x[x["person_id"].notna()].copy()
    x["score_value_num"] = pd.to_numeric(x["score_value"], errors="coerce")
    x["year_num"] = pd.to_numeric(x["year"], errors="coerce")

    grouped = (
        x.groupby(["person_id", "person_canon"], dropna=False)
        .agg(
            score_mentions=("score_value_num", "size"),
            scored_events=("event_id", pd.Series.nunique),
            min_score=("score_value_num", "min"),
            max_score=("score_value_num", "max"),
            avg_score=("score_value_num", "mean"),
            first_year=("year_num", "min"),
            last_year=("year_num", "max"),
            sample_event=("event_name", first_nonnull),
            sample_line=("line_raw", first_nonnull),
        )
        .reset_index()
        .sort_values(
            ["score_mentions", "max_score", "person_canon"],
            ascending=[False, False, True],
        )
    )

    return grouped


def build_summary(
    tricks: pd.DataFrame,
    scores: pd.DataFrame,
    player_trick_profile: pd.DataFrame,
    trick_leaderboard: pd.DataFrame,
    player_style_profile: pd.DataFrame,
    player_score_profile: pd.DataFrame,
) -> dict:
    full_tricks = trick_leaderboard[trick_leaderboard["trick_type"] == "full_trick"]
    style_terms = trick_leaderboard[trick_leaderboard["trick_type"] == "style_term"]

    summary = {
        "trick_mentions_total": int(len(tricks)),
        "trick_mentions_resolved": int(tricks["person_id"].notna().sum()) if "person_id" in tricks.columns else 0,
        "score_mentions_total": int(len(scores)),
        "score_mentions_resolved": int(scores["person_id"].notna().sum()) if "person_id" in scores.columns else 0,
        "players_with_trick_profiles": int(player_trick_profile["person_id"].nunique()) if not player_trick_profile.empty else 0,
        "players_with_style_profiles": int(player_style_profile["person_id"].nunique()) if not player_style_profile.empty else 0,
        "players_with_score_profiles": int(player_score_profile["person_id"].nunique()) if not player_score_profile.empty else 0,
        "distinct_tricks_total": int(trick_leaderboard["trick_canon"].nunique()) if not trick_leaderboard.empty else 0,
        "distinct_full_tricks": int(full_tricks["trick_canon"].nunique()) if not full_tricks.empty else 0,
        "distinct_style_terms": int(style_terms["trick_canon"].nunique()) if not style_terms.empty else 0,
        "top_10_full_tricks": (
            full_tricks[["trick_canon", "mentions"]].head(10).to_dict(orient="records")
            if not full_tricks.empty else []
        ),
        "top_10_style_terms": (
            style_terms[["trick_canon", "mentions"]].head(10).to_dict(orient="records")
            if not style_terms.empty else []
        ),
        "top_10_players_by_trick_mentions": (
            player_trick_profile.groupby(["person_id", "person_canon"], dropna=False)["mentions"]
            .sum()
            .reset_index()
            .sort_values(["mentions", "person_canon"], ascending=[False, True])
            .head(10)
            .to_dict(orient="records")
            if not player_trick_profile.empty else []
        ),
        "top_10_players_by_score_mentions": (
            player_score_profile[["person_id", "person_canon", "score_mentions"]]
            .head(10)
            .to_dict(orient="records")
            if not player_score_profile.empty else []
        ),
    }
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Build aggregate trick/style/score profiles from mined noise features.")
    ap.add_argument("--input-dir", required=True, help="Directory containing noise_score_mentions.csv and noise_trick_mentions.csv")
    ap.add_argument("--out-dir", required=True, help="Directory for aggregate outputs")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tricks = read_csv_required(input_dir / "noise_trick_mentions.csv")
    scores = read_csv_required(input_dir / "noise_score_mentions.csv")

    ensure_cols(
        tricks,
        ["event_id", "year", "event_name", "line_raw", "trick_canon", "person_id", "person_canon"],
        "noise_trick_mentions.csv",
    )
    ensure_cols(
        scores,
        ["event_id", "year", "event_name", "line_raw", "score_value", "person_id", "person_canon"],
        "noise_score_mentions.csv",
    )

    player_trick_profile = build_player_trick_profile(tricks)
    trick_leaderboard = build_trick_leaderboard(tricks)
    player_style_profile = build_player_style_profile(tricks)
    player_score_profile = build_player_score_profile(scores)

    summary = build_summary(
        tricks=tricks,
        scores=scores,
        player_trick_profile=player_trick_profile,
        trick_leaderboard=trick_leaderboard,
        player_style_profile=player_style_profile,
        player_score_profile=player_score_profile,
    )

    player_trick_profile.to_csv(out_dir / "player_trick_profile.csv", index=False)
    trick_leaderboard.to_csv(out_dir / "trick_leaderboard.csv", index=False)
    player_style_profile.to_csv(out_dir / "player_style_profile.csv", index=False)
    player_score_profile.to_csv(out_dir / "player_score_profile.csv", index=False)

    with open(out_dir / "noise_fun_stats_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("Wrote:")
    print(f"  {out_dir / 'player_trick_profile.csv'}")
    print(f"  {out_dir / 'trick_leaderboard.csv'}")
    print(f"  {out_dir / 'player_style_profile.csv'}")
    print(f"  {out_dir / 'player_score_profile.csv'}")
    print(f"  {out_dir / 'noise_fun_stats_summary.json'}")
    print("\nSummary:")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
