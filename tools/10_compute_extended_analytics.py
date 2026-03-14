#!/usr/bin/env python3
"""
10_compute_extended_analytics.py

Extends the difficulty analytics layer with four community-oriented summaries.

Inputs:
  sequence_difficulty_conservative.csv  — chain-level difficulty
  sequence_tricks_conservative.csv      — trick-level, one row per trick in chain
  noise_trick_mentions_v2.csv           — all trick mentions with attribution

Outputs:
  player_diversity_profiles.csv    — per-player trick breadth and reuse ratio
  modifier_trends_by_year.csv      — standalone modifier mention counts by year
  chain_complexity_by_year.csv     — avg chain length and avg max ADD per year
  transition_statistics.csv        — top-N trick transitions with player lists
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# 1. Player trick diversity
# ─────────────────────────────────────────────────────────────────────────────

MODIFIER_TOKENS = {
    "ducking", "spinning", "symposium", "atomic", "stepping",
    "gyro", "barraging", "blazing", "tapping", "paradox",
    "pixie", "fairy", "whirling", "miraging", "weaving", "diving",
}


def build_player_diversity(mentions: pd.DataFrame) -> pd.DataFrame:
    """
    One row per player (resolved mentions only).

    unique_tricks    — distinct trick_canon values seen for this player
    total_tricks     — total trick mention rows (including repeats)
    diversity_ratio  — unique_tricks / total_tricks  (1.0 = all different)
    compound_tricks  — unique tricks that are NOT pure modifier tokens
    modifier_tokens  — unique modifier-token trick_canons observed
    top_tricks       — the 5 most-mentioned tricks for this player
    """
    res = mentions[
        (mentions["match_type"] == "exact") & mentions["person_id"].notna()
    ].copy()

    rows: list[dict] = []
    for (pid, pcanon), grp in res.groupby(["person_id", "person_canon"]):
        counts = grp["trick_canon"].value_counts()
        unique = int(counts.shape[0])
        total  = int(counts.sum())
        compounds = counts.index[~counts.index.isin(MODIFIER_TOKENS)]
        mods      = counts.index[counts.index.isin(MODIFIER_TOKENS)]

        top_5 = counts.head(5).index.tolist()

        rows.append({
            "person_id":       pid,
            "person_canon":    pcanon,
            "unique_tricks":   unique,
            "total_tricks":    total,
            "diversity_ratio": round(unique / total, 3) if total else None,
            "compound_tricks": int(len(compounds)),
            "modifier_tokens": int(len(mods)),
            "top_tricks":      " | ".join(top_5),
            "year_first":      int(grp["year"].min()),
            "year_last":       int(grp["year"].max()),
        })

    df = pd.DataFrame(rows).sort_values(
        ["unique_tricks", "total_tricks"], ascending=False
    ).reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. Modifier trends by year
# ─────────────────────────────────────────────────────────────────────────────

TRACKED_MODIFIERS = ["pixie", "ducking", "spinning", "atomic", "symposium"]


def build_modifier_trends(mentions: pd.DataFrame) -> pd.DataFrame:
    """
    Count standalone modifier-token mentions per year for each tracked modifier.

    These are trick_chain or result_trick lines where the modifier appeared
    as a standalone span (i.e. it was NOT consumed by a longer compound match).
    Normalised rate columns (per-100-mentions) allow comparison across years
    with different corpus sizes.
    """
    res = mentions[mentions["match_type"] == "exact"].copy()

    # Total resolved trick mentions per year (denominator for rates)
    totals = res.groupby("year").size().rename("total_resolved_mentions")

    mod_rows = res[res["trick_canon"].isin(TRACKED_MODIFIERS)]
    pivot = (
        mod_rows.groupby(["year", "trick_canon"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=TRACKED_MODIFIERS, fill_value=0)
    )

    df = pivot.reset_index().merge(totals.reset_index(), on="year", how="left")
    df["total_resolved_mentions"] = df["total_resolved_mentions"].fillna(0).astype(int)

    # Rate: modifier appearances per 100 total resolved mentions that year
    for mod in TRACKED_MODIFIERS:
        df[f"{mod}_rate"] = (
            df[mod] / df["total_resolved_mentions"] * 100
        ).round(2).where(df["total_resolved_mentions"] > 0, None)

    df = df.sort_values("year").reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. Chain complexity progression by year
# ─────────────────────────────────────────────────────────────────────────────

def build_chain_complexity_by_year(chains: pd.DataFrame) -> pd.DataFrame:
    """
    Per-year aggregation of chain structural and difficulty metrics.

    avg_chain_length — mean normalized_length (tricks per chain after merging)
    avg_max_add      — mean of per-chain max_add (peak trick difficulty)
    avg_avg_add      — mean of per-chain avg_add (average trick difficulty)
    pct_3plus        — % of chains with ≥3 tricks
    pct_fully_scored — % of chains where every trick was scoreable
    """
    rows: list[dict] = []
    for year, grp in chains.groupby("year"):
        scored = grp[grp["sequence_add"].notna()]
        n = len(grp)
        n_scored = len(scored)

        rows.append({
            "year":              int(year),
            "n_chains":          n,
            "n_chains_scored":   n_scored,
            "avg_chain_length":  round(grp["normalized_length"].mean(), 3),
            "median_chain_length": float(grp["normalized_length"].median()),
            "pct_3plus_tricks":  round(
                (grp["normalized_length"] >= 3).sum() / n * 100, 1
            ) if n else None,
            "pct_fully_scored":  round(
                (grp["unscored_count"] == 0).sum() / n * 100, 1
            ) if n else None,
            "avg_max_add":       round(scored["max_add"].mean(), 3) if n_scored else None,
            "avg_avg_add":       round(scored["avg_add"].mean(), 3) if n_scored else None,
            "max_chain_length":  int(grp["normalized_length"].max()),
        })

    return pd.DataFrame(rows).sort_values("year").reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Transition statistics
# ─────────────────────────────────────────────────────────────────────────────

def build_transition_statistics(
    tricks: pd.DataFrame,
    top_n: int = 25,
) -> pd.DataFrame:
    """
    Adjacent trick-pair (A→B) frequency with player annotations.

    Excludes unresolved modifier residues from both positions.
    Columns:
      trick_a, trick_b
      count           — total occurrences across all chains
      n_players       — distinct players performing this transition
      n_events        — distinct events where it appears
      players         — semicolon-separated sorted list of player names
      example_chain   — one chain_id that contains this transition
      adds_a, adds_b  — ADD values if known (joined from sequence data)
    """
    clean = tricks[tricks["merge_method"] != "unresolved_modifier"].copy()
    clean = clean.sort_values(["chain_id", "sequence_index"])

    # Load ADD values from the tricks file itself (adds column)
    add_by_trick: dict[str, float | None] = {}
    for _, row in clean.iterrows():
        t = row["normalized_trick"]
        if t not in add_by_trick:
            add_by_trick[t] = row.get("adds") if pd.notna(row.get("adds")) else None

    pairs: list[dict] = []
    for chain_id, grp in clean.groupby("chain_id"):
        grp = grp.sort_values("sequence_index")
        seq    = grp["normalized_trick"].tolist()
        pcanon = grp["person_canon"].iloc[0] if "person_canon" in grp.columns else None
        pid    = grp["person_id"].iloc[0]    if "person_id"    in grp.columns else None
        eid    = grp["event_id"].iloc[0]     if "event_id"     in grp.columns else None

        for i in range(len(seq) - 1):
            pairs.append({
                "trick_a":    seq[i],
                "trick_b":    seq[i + 1],
                "person_id":  pid,
                "person_canon": pcanon,
                "event_id":   eid,
                "chain_id":   chain_id,
            })

    if not pairs:
        return pd.DataFrame()

    pdf = pd.DataFrame(pairs)

    agg = (
        pdf.groupby(["trick_a", "trick_b"])
        .agg(
            count=("chain_id", "count"),
            n_players=("person_id", lambda x: x.dropna().nunique()),
            n_events=("event_id", "nunique"),
            players=("person_canon", lambda x: "; ".join(
                sorted(str(v) for v in x.dropna().unique())
            )),
            example_chain=("chain_id", "first"),
        )
        .reset_index()
        .sort_values(["count", "n_players"], ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    # Annotate ADD for each side
    agg["adds_a"] = agg["trick_a"].map(add_by_trick)
    agg["adds_b"] = agg["trick_b"].map(add_by_trick)

    # add_sum: combined ADD of the two-trick sequence
    def _safe_sum(a, b):
        if pd.notna(a) and pd.notna(b):
            return int(a) + int(b)
        return None
    agg["pair_add_sum"] = agg.apply(lambda r: _safe_sum(r["adds_a"], r["adds_b"]), axis=1)

    return agg


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Extended freestyle difficulty analytics.")
    ap.add_argument("--chains",   required=True, help="sequence_difficulty_conservative.csv")
    ap.add_argument("--tricks",   required=True, help="sequence_tricks_conservative.csv")
    ap.add_argument("--mentions", required=True, help="noise_trick_mentions_v2.csv")
    ap.add_argument("--out-dir",  required=True, help="Output directory")
    ap.add_argument("--top-n",    type=int, default=25,
                    help="Top-N transitions to include (default 25)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading inputs…")
    chains   = pd.read_csv(args.chains, low_memory=False)
    tricks   = pd.read_csv(args.tricks, low_memory=False)
    mentions = pd.read_csv(args.mentions, low_memory=False)

    outputs: dict[str, pd.DataFrame] = {}

    print("Building player_diversity_profiles…")
    outputs["player_diversity_profiles.csv"] = build_player_diversity(mentions)

    print("Building modifier_trends_by_year…")
    outputs["modifier_trends_by_year.csv"] = build_modifier_trends(mentions)

    print("Building chain_complexity_by_year…")
    outputs["chain_complexity_by_year.csv"] = build_chain_complexity_by_year(chains)

    print("Building transition_statistics…")
    outputs["transition_statistics.csv"] = build_transition_statistics(
        tricks, top_n=args.top_n
    )

    for fname, df in outputs.items():
        p = out_dir / fname
        df.to_csv(p, index=False)
        print(f"  → {p}  ({len(df)} rows)")

    # ── Console summaries ─────────────────────────────────────────────────────

    div = outputs["player_diversity_profiles.csv"]
    print("\n" + "═" * 65)
    print("  PLAYER TRICK DIVERSITY  (top 20 by unique_tricks)")
    print("═" * 65)
    print(f"  {'person_canon':<28}  {'uniq':>4}  {'total':>5}  {'ratio':>5}  "
          f"{'cmpd':>4}  {'mods':>4}  {'yrs':>9}")
    for _, r in div.head(20).iterrows():
        yr = f"{int(r['year_first'])}–{int(r['year_last'])}"
        print(f"  {str(r['person_canon']):<28}  {int(r['unique_tricks']):>4}  "
              f"{int(r['total_tricks']):>5}  {r['diversity_ratio']:>5.3f}  "
              f"{int(r['compound_tricks']):>4}  {int(r['modifier_tokens']):>4}  {yr:>9}")

    mod = outputs["modifier_trends_by_year.csv"]
    print("\n  MODIFIER TRENDS BY YEAR")
    print(f"  {'year':>4}  {'pixie':>5}  {'ducking':>7}  "
          f"{'spinning':>8}  {'atomic':>6}  {'symposium':>9}  {'total_res':>9}")
    for _, r in mod.iterrows():
        print(f"  {int(r['year']):>4}  {int(r['pixie']):>5}  {int(r['ducking']):>7}  "
              f"{int(r['spinning']):>8}  {int(r['atomic']):>6}  "
              f"{int(r['symposium']):>9}  {int(r['total_resolved_mentions']):>9}")

    cplx = outputs["chain_complexity_by_year.csv"]
    print("\n  CHAIN COMPLEXITY BY YEAR")
    print(f"  {'year':>4}  {'chains':>6}  {'avg_len':>7}  "
          f"{'3+%':>5}  {'avg_max':>7}  {'avg_avg':>7}")
    for _, r in cplx.iterrows():
        print(f"  {int(r['year']):>4}  {int(r['n_chains']):>6}  "
              f"{r['avg_chain_length']:>7.2f}  "
              f"{(r['pct_3plus_tricks'] or 0):>5.1f}  "
              f"{(r['avg_max_add'] or 0):>7.2f}  "
              f"{(r['avg_avg_add'] or 0):>7.3f}")

    trans = outputs["transition_statistics.csv"]
    print(f"\n  TOP {args.top_n} TRICK TRANSITIONS")
    print(f"  {'trick_a':<24}  {'trick_b':<24}  "
          f"{'cnt':>3}  {'ply':>3}  {'ev':>3}  {'A+B':>4}  players (first 3)")
    for _, r in trans.iterrows():
        ab  = int(r["pair_add_sum"]) if pd.notna(r.get("pair_add_sum")) else "?"
        plist = str(r["players"]).split("; ")
        pshow = "; ".join(plist[:3]) + ("…" if len(plist) > 3 else "")
        print(f"  {str(r['trick_a']):<24}  →  {str(r['trick_b']):<24}  "
              f"{int(r['count']):>3}  {int(r['n_players']):>3}  "
              f"{int(r['n_events']):>3}  {str(ab):>4}  {pshow}")


if __name__ == "__main__":
    main()
