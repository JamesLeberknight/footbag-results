#!/usr/bin/env python3
"""
07b_build_noise_aggregates_v2.py — Noise Aggregate Builder v2

Reads outputs from 06b_mine_noise_v2.py and produces:
  trick_leaderboard_v2.csv        — per-trick counts split by attribution confidence
  player_trick_profile_v2.csv     — per-player per-trick profile
  player_style_profile_v2.csv     — per-player style-term pivot
  player_score_profile_v2.csv     — per-player score profile (by score_type)
  trick_cooccurrence.csv          — pairwise trick co-occurrence from chain sequences
  noise_attribution_qc.csv        — rows where confidence = context_window or team_line
  noise_score_qc.csv              — rows where filter_reason is set or score_type = reject
  noise_aggregates_summary_v2.json
"""
from __future__ import annotations

import argparse
import json
from itertools import combinations
from pathlib import Path

import pandas as pd


# ─────────────────────────────────────────────
# Style / classify (unchanged from v1)
# ─────────────────────────────────────────────

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

FULL_TRICK_EXCLUSIONS: set[str] = set()


def classify_trick(trick: str) -> str:
    t = str(trick).strip().lower()
    if not t:
        return "unknown"
    if t in STYLE_TERMS:
        return "style_term"
    if t in FULL_TRICK_EXCLUSIONS:
        return "exclude"
    return "full_trick"


# ─────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────

def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required input not found: {path}")
    return pd.read_csv(path, low_memory=False)


def read_csv_optional(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path, low_memory=False)


def first_nonnull(series: pd.Series):
    s = series.dropna()
    return s.iloc[0] if len(s) else None


# ─────────────────────────────────────────────
# Trick leaderboard (v2: split attribution columns)
# ─────────────────────────────────────────────

def build_trick_leaderboard(tricks: pd.DataFrame) -> pd.DataFrame:
    x = tricks.copy()
    x["year_num"] = pd.to_numeric(x["year"], errors="coerce")
    x["trick_type"] = x["trick_canon"].map(classify_trick)

    # Total aggregation
    base = (
        x.groupby(["trick_canon", "trick_type"], dropna=False)
        .agg(
            total_mentions=("trick_canon", "size"),
            resolved_mentions=("person_id", lambda s: int(s.notna().sum())),
            unique_players=("person_id", lambda s: int(s.dropna().nunique())),
            unique_events=("event_id", pd.Series.nunique),
            first_year=("year_num", "min"),
            last_year=("year_num", "max"),
            sample_event=("event_name", first_nonnull),
            sample_line=("line_raw", first_nonnull),
        )
        .reset_index()
    )

    # Attribution-split counts
    direct_counts = (
        x[x["attribution_confidence"] == "direct"]
        .groupby("trick_canon")
        .size()
        .rename("direct_mentions")
    )
    cw_counts = (
        x[x["attribution_confidence"] == "context_window"]
        .groupby("trick_canon")
        .size()
        .rename("context_window_mentions")
    )
    team_counts = (
        x[x.get("team_flag", pd.Series(False, index=x.index)) == True]
        .groupby("trick_canon")
        .size()
        .rename("team_mentions")
    ) if "team_flag" in x.columns else pd.Series(dtype=int).rename("team_mentions")

    base = (
        base
        .join(direct_counts, on="trick_canon")
        .join(cw_counts, on="trick_canon")
        .join(team_counts, on="trick_canon")
    )
    base["direct_mentions"] = base["direct_mentions"].fillna(0).astype(int)
    base["context_window_mentions"] = base["context_window_mentions"].fillna(0).astype(int)
    base["team_mentions"] = base["team_mentions"].fillna(0).astype(int)

    # Reorder columns
    col_order = [
        "trick_canon", "trick_type",
        "total_mentions", "direct_mentions", "context_window_mentions", "team_mentions",
        "resolved_mentions", "unique_players", "unique_events",
        "first_year", "last_year", "sample_event", "sample_line",
    ]
    base = base[[c for c in col_order if c in base.columns]]

    return base.sort_values(
        ["total_mentions", "resolved_mentions", "unique_players", "trick_canon"],
        ascending=[False, False, False, True],
    )


# ─────────────────────────────────────────────
# Player trick profile
# ─────────────────────────────────────────────

def build_player_trick_profile(tricks: pd.DataFrame) -> pd.DataFrame:
    x = tricks[tricks["person_id"].notna()].copy()
    if x.empty:
        return pd.DataFrame()
    x["year_num"] = pd.to_numeric(x["year"], errors="coerce")
    x["trick_type"] = x["trick_canon"].map(classify_trick)

    base = (
        x.groupby(["person_id", "person_canon", "trick_canon", "trick_type"], dropna=False)
        .agg(
            mentions=("trick_canon", "size"),
            direct_mentions=("attribution_confidence", lambda s: int((s == "direct").sum())),
            context_window_mentions=("attribution_confidence", lambda s: int((s == "context_window").sum())),
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
    return base


# ─────────────────────────────────────────────
# Player style profile
# ─────────────────────────────────────────────

def build_player_style_profile(tricks: pd.DataFrame) -> pd.DataFrame:
    x = tricks[tricks["person_id"].notna()].copy()
    if x.empty:
        return pd.DataFrame(
            columns=["person_id", "person_canon", "style_mentions_total", "distinct_style_terms"]
        )
    x["trick_type"] = x["trick_canon"].map(classify_trick)
    x = x[x["trick_type"] == "style_term"].copy()
    if x.empty:
        return pd.DataFrame(
            columns=["person_id", "person_canon", "style_mentions_total", "distinct_style_terms"]
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
    ordered = (
        ["person_id", "person_canon", "style_mentions_total", "distinct_style_terms"]
        + sorted(style_cols)
    )
    return pivot[ordered].sort_values(
        ["style_mentions_total", "distinct_style_terms", "person_canon"],
        ascending=[False, False, True],
    )


# ─────────────────────────────────────────────
# Player score profile (v2: per score_type)
# ─────────────────────────────────────────────

def build_player_score_profile(scores: pd.DataFrame) -> pd.DataFrame:
    # Only keep non-rejected scores
    x = scores[scores["person_id"].notna()].copy()
    if "score_type" in x.columns:
        x = x[x["score_type"] != "reject"].copy()
    if x.empty:
        return pd.DataFrame()

    x["score_value_num"] = pd.to_numeric(x["score_value"], errors="coerce")
    x["year_num"] = pd.to_numeric(x["year"], errors="coerce")

    base = (
        x.groupby(
            ["person_id", "person_canon"] + (["score_type"] if "score_type" in x.columns else []),
            dropna=False,
        )
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
    return base


# ─────────────────────────────────────────────
# Trick co-occurrence from sequences
# ─────────────────────────────────────────────

def build_trick_cooccurrence(sequences: pd.DataFrame) -> pd.DataFrame:
    """
    For each chain_id, emit pairwise combinations of tricks.
    Counts how often each (trick_A, trick_B) pair appears in the same chain.
    """
    if sequences.empty or "chain_id" not in sequences.columns:
        return pd.DataFrame(columns=["trick_a", "trick_b", "cooccurrence_count"])

    required = ["chain_id", "trick_canon"]
    if not all(c in sequences.columns for c in required):
        return pd.DataFrame(columns=["trick_a", "trick_b", "cooccurrence_count"])

    pair_counter: dict[tuple[str, str], int] = {}

    for chain_id, group in sequences.groupby("chain_id"):
        tricks_in_chain = group["trick_canon"].dropna().tolist()
        if len(tricks_in_chain) < 2:
            continue
        for a, b in combinations(sorted(set(tricks_in_chain)), 2):
            key = (a, b)
            pair_counter[key] = pair_counter.get(key, 0) + 1

    rows = [{"trick_a": a, "trick_b": b, "cooccurrence_count": c} for (a, b), c in pair_counter.items()]
    df = pd.DataFrame(rows, columns=["trick_a", "trick_b", "cooccurrence_count"])
    return df.sort_values("cooccurrence_count", ascending=False)


# ─────────────────────────────────────────────
# QC tables
# ─────────────────────────────────────────────

def build_attribution_qc(tricks: pd.DataFrame) -> pd.DataFrame:
    """Rows where attribution is context_window or team_line (for human review)."""
    if tricks.empty:
        return pd.DataFrame()
    mask = (tricks["attribution_confidence"].isin(["context_window", "team_line"])) | (
        tricks.get("team_flag", pd.Series(False, index=tricks.index)) == True
    )
    return tricks[mask].copy().reset_index(drop=True)


def build_score_qc(scores: pd.DataFrame) -> pd.DataFrame:
    """Rows where filter_reason is set or score_type = 'reject'."""
    if scores.empty:
        return pd.DataFrame()
    mask = pd.Series(False, index=scores.index)
    if "filter_reason" in scores.columns:
        mask |= scores["filter_reason"].notna() & (scores["filter_reason"] != "")
    if "score_type" in scores.columns:
        mask |= scores["score_type"] == "reject"
    return scores[mask].copy().reset_index(drop=True)


# ─────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────

def build_summary(
    tricks: pd.DataFrame,
    scores: pd.DataFrame,
    leaderboard: pd.DataFrame,
    player_trick: pd.DataFrame,
    player_style: pd.DataFrame,
    player_score: pd.DataFrame,
    cooccurrence: pd.DataFrame,
) -> dict:
    full_tricks = leaderboard[leaderboard["trick_type"] == "full_trick"] if not leaderboard.empty else pd.DataFrame()
    style_terms = leaderboard[leaderboard["trick_type"] == "style_term"] if not leaderboard.empty else pd.DataFrame()

    total_tricks = len(tricks)
    direct_tricks = int((tricks["attribution_confidence"] == "direct").sum()) if not tricks.empty else 0
    cw_tricks = int((tricks["attribution_confidence"] == "context_window").sum()) if not tricks.empty else 0

    score_type_breakdown: dict[str, int] = {}
    if not scores.empty and "score_type" in scores.columns:
        score_type_breakdown = scores["score_type"].value_counts().to_dict()

    return {
        "trick_mentions_total": total_tricks,
        "trick_mentions_resolved": int(tricks["person_id"].notna().sum()) if not tricks.empty else 0,
        "direct_attribution_rate": round(direct_tricks / total_tricks, 4) if total_tricks else 0,
        "context_window_attribution_rate": round(cw_tricks / total_tricks, 4) if total_tricks else 0,
        "masked_trick_rate": None,  # populated from noise_summary_v2.json if available
        "score_mentions_total": len(scores),
        "score_mentions_resolved": int(scores["person_id"].notna().sum()) if not scores.empty else 0,
        "score_type_breakdown": score_type_breakdown,
        "players_with_trick_profiles": int(player_trick["person_id"].nunique()) if not player_trick.empty else 0,
        "players_with_style_profiles": int(player_style["person_id"].nunique()) if not player_style.empty else 0,
        "players_with_score_profiles": int(player_score["person_id"].nunique()) if not player_score.empty else 0,
        "distinct_tricks_total": int(leaderboard["trick_canon"].nunique()) if not leaderboard.empty else 0,
        "distinct_full_tricks": int(full_tricks["trick_canon"].nunique()) if not full_tricks.empty else 0,
        "distinct_style_terms": int(style_terms["trick_canon"].nunique()) if not style_terms.empty else 0,
        "cooccurrence_pairs": len(cooccurrence),
        "top_10_full_tricks": (
            full_tricks[["trick_canon", "total_mentions"]].head(10).to_dict(orient="records")
            if not full_tricks.empty else []
        ),
        "top_10_style_terms": (
            style_terms[["trick_canon", "total_mentions"]].head(10).to_dict(orient="records")
            if not style_terms.empty else []
        ),
        "top_10_players_by_trick_mentions": (
            player_trick.groupby(["person_id", "person_canon"], dropna=False)["mentions"]
            .sum()
            .reset_index()
            .sort_values(["mentions", "person_canon"], ascending=[False, True])
            .head(10)
            .to_dict(orient="records")
            if not player_trick.empty else []
        ),
    }


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build v2 aggregate trick/style/score profiles from 06b mined features."
    )
    ap.add_argument("--input-dir", required=True, help="Directory containing v2 noise CSVs")
    ap.add_argument("--out-dir", required=True, help="Directory for aggregate outputs")
    args = ap.parse_args()

    input_dir = Path(args.input_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tricks = read_csv_required(input_dir / "noise_trick_mentions_v2.csv")
    scores = read_csv_required(input_dir / "noise_score_mentions_v2.csv")
    sequences_opt = read_csv_optional(input_dir / "noise_trick_sequences.csv")
    sequences = sequences_opt if sequences_opt is not None else pd.DataFrame()

    # Ensure attribution_confidence exists (handle older files missing it)
    if "attribution_confidence" not in tricks.columns:
        tricks["attribution_confidence"] = "unknown"
    if "team_flag" not in tricks.columns:
        tricks["team_flag"] = False

    leaderboard = build_trick_leaderboard(tricks)
    player_trick = build_player_trick_profile(tricks)
    player_style = build_player_style_profile(tricks)
    player_score = build_player_score_profile(scores)
    cooccurrence = build_trick_cooccurrence(sequences)
    attribution_qc = build_attribution_qc(tricks)
    score_qc = build_score_qc(scores)

    summary = build_summary(tricks, scores, leaderboard, player_trick, player_style, player_score, cooccurrence)

    # Try to pull masked_trick_rate from the miner summary
    miner_summary_path = input_dir / "noise_summary_v2.json"
    if miner_summary_path.exists():
        miner_summary = json.loads(miner_summary_path.read_text(encoding="utf-8"))
        total = miner_summary.get("trick_mentions", 0)
        masked = miner_summary.get("tricks_masked_by_longer_match", 0)
        summary["masked_trick_rate"] = round(masked / (total + masked), 4) if (total + masked) > 0 else 0

    outputs = {
        "trick_leaderboard_v2.csv": leaderboard,
        "player_trick_profile_v2.csv": player_trick,
        "player_style_profile_v2.csv": player_style,
        "player_score_profile_v2.csv": player_score,
        "trick_cooccurrence.csv": cooccurrence,
        "noise_attribution_qc.csv": attribution_qc,
        "noise_score_qc.csv": score_qc,
    }

    print("Wrote:")
    for filename, df in outputs.items():
        path = out_dir / filename
        df.to_csv(path, index=False)
        print(f"  {path}")

    summary_path = out_dir / "noise_aggregates_summary_v2.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"  {summary_path}")

    print("\nSummary:")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
