#!/usr/bin/env python3
"""
09_compute_difficulty_analytics.py

Builds analytical summaries from the noise-mining difficulty pipeline.

Inputs:
  sequence_difficulty_conservative.csv  — one row per trick chain, scored
  sequence_tricks_conservative.csv      — one row per trick in each chain
  noise_trick_mentions_v2.csv           — all trick mentions with attribution
  Placements_Flat.csv                   — canonical competition placements

Outputs (all written to --out-dir):
  difficulty_by_year.csv          — difficulty stats aggregated by year
  player_difficulty_profiles.csv  — per-player difficulty + competition context
  trick_frequency.csv             — trick usage counts, player/event spread
  trick_transition_network.csv    — adjacent trick-pair (A→B) counts
  trick_innovation_timeline.csv   — per-trick first/last year seen + metrics
  innovation_by_year.csv          — new tricks introduced per year + ADD stats

Coverage note:
  The noise corpus is a sample — only events that published trick-level
  results text are included. Stats represent what was documented, not the
  full population. All outputs include n_chains or n_mentions columns so
  readers can weight accordingly.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pct(s: pd.Series, q: float) -> float:
    v = s.dropna()
    return round(float(np.percentile(v, q)), 3) if len(v) else None


def _mean(s: pd.Series) -> float | None:
    v = s.dropna()
    return round(float(v.mean()), 3) if len(v) else None


def _median(s: pd.Series) -> float | None:
    v = s.dropna()
    return round(float(v.median()), 3) if len(v) else None


# ─────────────────────────────────────────────────────────────────────────────
# 1. Difficulty by year
# ─────────────────────────────────────────────────────────────────────────────

def build_difficulty_by_year(chains: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sequence difficulty stats by year.

    Uses scored chains only (sequence_add not null).
    avg_add is the mean of per-chain average-ADD (honest difficulty: includes
    unscored tricks in denominator when the chain was partially scored).
    """
    scored = chains[chains["sequence_add"].notna()].copy()

    rows = []
    for year, grp in scored.groupby("year"):
        add = grp["sequence_add"]
        avg_add = grp["avg_add"]
        max_add = grp["max_add"]
        rows.append({
            "year":                 int(year),
            "n_chains":             len(grp),
            "n_players":            grp["person_id"].dropna().nunique(),
            "avg_sequence_add":     _mean(add),
            "median_sequence_add":  _median(add),
            "p75_sequence_add":     _pct(add, 75),
            "p90_sequence_add":     _pct(add, 90),
            "max_sequence_add":     int(add.max()),
            "avg_avg_add":          _mean(avg_add),
            "avg_max_add":          _mean(max_add),
        })

    df = pd.DataFrame(rows).sort_values("year")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. Player difficulty profiles
# ─────────────────────────────────────────────────────────────────────────────

def build_player_difficulty_profiles(
    chains: pd.DataFrame,
    tricks: pd.DataFrame,
    mentions: pd.DataFrame,
    placements_flat: pd.DataFrame,
) -> pd.DataFrame:
    """
    One row per player with at least one attributed chain.

    Difficulty columns come from chains.
    Breadth column (n_distinct_tricks) comes from mentions.
    Competition context (freestyle placements, best place) from PF.
    """
    # ── Chain-level per player ────────────────────────────────────────────────
    attr_chains = chains[chains["person_id"].notna()].copy()
    scored_chains = attr_chains[attr_chains["sequence_add"].notna()]

    chain_agg = (
        attr_chains
        .groupby(["person_id", "person_canon"])
        .agg(
            chains_total=("chain_id", "count"),
            year_first=("year", "min"),
            year_last=("year", "max"),
        )
        .reset_index()
    )

    scored_agg = (
        scored_chains
        .groupby("person_id")
        .agg(
            chains_scored=("chain_id", "count"),
            avg_sequence_add=("sequence_add", "mean"),
            median_sequence_add=("sequence_add", "median"),
            max_sequence_add=("sequence_add", "max"),
            avg_avg_add=("avg_add", "mean"),
            max_max_add=("max_add", "max"),
        )
        .reset_index()
    )
    scored_agg["avg_sequence_add"] = scored_agg["avg_sequence_add"].round(3)
    scored_agg["median_sequence_add"] = scored_agg["median_sequence_add"].round(3)
    scored_agg["avg_avg_add"] = scored_agg["avg_avg_add"].round(3)

    player_df = chain_agg.merge(scored_agg, on="person_id", how="left")
    player_df["year_span"] = player_df["year_last"] - player_df["year_first"]
    player_df["chains_scored"] = player_df["chains_scored"].fillna(0).astype(int)

    # ── Trick breadth from mentions ───────────────────────────────────────────
    resolved_mentions = mentions[
        (mentions["match_type"] == "exact") & mentions["person_id"].notna()
    ]
    breadth = (
        resolved_mentions
        .groupby("person_id")["trick_canon"]
        .nunique()
        .rename("n_distinct_tricks")
        .reset_index()
    )
    player_df = player_df.merge(breadth, on="person_id", how="left")
    player_df["n_distinct_tricks"] = player_df["n_distinct_tricks"].fillna(0).astype(int)

    # ── Competition context from Placements_Flat ──────────────────────────────
    pf_fs = placements_flat[
        (placements_flat["division_category"] == "freestyle")
        & placements_flat["person_id"].notna()
        & (placements_flat["person_id"] != "__NON_PERSON__")
    ]
    pf_agg = (
        pf_fs
        .groupby("person_id")
        .agg(
            freestyle_placements=("place", "count"),
            best_freestyle_place=("place", "min"),
        )
        .reset_index()
    )
    player_df = player_df.merge(pf_agg, on="person_id", how="left")
    player_df["freestyle_placements"] = player_df["freestyle_placements"].fillna(0).astype(int)
    player_df["best_freestyle_place"] = player_df["best_freestyle_place"].where(
        player_df["best_freestyle_place"].notna(), None
    )

    # ── Sort: most difficulty evidence first ──────────────────────────────────
    player_df = player_df.sort_values(
        ["max_sequence_add", "avg_sequence_add"], ascending=False
    ).reset_index(drop=True)

    col_order = [
        "person_id", "person_canon",
        "chains_total", "chains_scored",
        "avg_sequence_add", "median_sequence_add", "max_sequence_add",
        "avg_avg_add", "max_max_add",
        "n_distinct_tricks",
        "year_first", "year_last", "year_span",
        "freestyle_placements", "best_freestyle_place",
    ]
    return player_df[[c for c in col_order if c in player_df.columns]]


# ─────────────────────────────────────────────────────────────────────────────
# 3. Trick frequency
# ─────────────────────────────────────────────────────────────────────────────

def build_trick_frequency(
    mentions: pd.DataFrame,
    tricks: pd.DataFrame,
    trick_dict_path: Path,
) -> pd.DataFrame:
    """
    One row per trick_canon.

    Mention counts come from noise_trick_mentions_v2.csv (all resolved rows).
    In-sequence counts come from sequence_tricks_conservative.csv.
    ADD values joined from trick_dictionary.csv.
    """
    # ── Load ADD values ───────────────────────────────────────────────────────
    add_map: dict[str, int | None] = {}
    if trick_dict_path.exists():
        td = pd.read_csv(trick_dict_path)
        for _, row in td.iterrows():
            canon = str(row["trick_canon"]).strip().lower()
            try:
                add_map[canon] = int(row["adds"])
            except (ValueError, TypeError):
                add_map[canon] = None

    # ── Mention-level counts ──────────────────────────────────────────────────
    res = mentions[mentions["match_type"] == "exact"].copy()
    direct = res[res["attribution_confidence"] == "direct"]
    context = res[res["attribution_confidence"] == "context_window"]

    freq = (
        res.groupby("trick_canon")
        .agg(
            total_mentions=("line_no", "count"),
            n_players=("person_id", lambda x: x.dropna().nunique()),
            n_events=("event_id", "nunique"),
            year_first=("year", "min"),
            year_last=("year", "max"),
        )
        .reset_index()
    )

    direct_counts = (
        direct.groupby("trick_canon").size().rename("direct_mentions").reset_index()
    )
    context_counts = (
        context.groupby("trick_canon").size().rename("context_mentions").reset_index()
    )
    freq = freq.merge(direct_counts, on="trick_canon", how="left")
    freq = freq.merge(context_counts, on="trick_canon", how="left")
    freq["direct_mentions"] = freq["direct_mentions"].fillna(0).astype(int)
    freq["context_mentions"] = freq["context_mentions"].fillna(0).astype(int)

    # ── Sequence-level counts ─────────────────────────────────────────────────
    scored_tricks = tricks[tricks["adds"].notna()]
    seq_counts = (
        tricks.groupby("normalized_trick")
        .agg(
            in_sequences=("chain_id", "count"),
            in_sequences_scored=("adds", lambda x: x.notna().sum()),
            median_chain_position=("sequence_index", "median"),
        )
        .reset_index()
        .rename(columns={"normalized_trick": "trick_canon"})
    )
    seq_counts["median_chain_position"] = seq_counts["median_chain_position"].round(1)
    freq = freq.merge(seq_counts, on="trick_canon", how="left")
    freq["in_sequences"] = freq["in_sequences"].fillna(0).astype(int)
    freq["in_sequences_scored"] = freq["in_sequences_scored"].fillna(0).astype(int)

    # ── Join ADD ──────────────────────────────────────────────────────────────
    freq["adds"] = freq["trick_canon"].map(add_map)

    freq = freq.sort_values("total_mentions", ascending=False).reset_index(drop=True)

    col_order = [
        "trick_canon", "adds",
        "total_mentions", "direct_mentions", "context_mentions",
        "n_players", "n_events",
        "year_first", "year_last",
        "in_sequences", "in_sequences_scored", "median_chain_position",
    ]
    return freq[[c for c in col_order if c in freq.columns]]


# ─────────────────────────────────────────────────────────────────────────────
# 4. Trick transition network
# ─────────────────────────────────────────────────────────────────────────────


def build_trick_transition_network(tricks: pd.DataFrame) -> pd.DataFrame:
    """
    Count adjacent trick-pair (A→B) transitions across all chains.

    Only uses tricks with a scored ADD to keep the network clean.
    Unscored modifier tokens (unresolved_modifier) are excluded so that
    bare "spinning" or "ducking" residues don't pollute transitions.
    """
    # Exclude unresolved modifier residues — keep scored tricks + direct non-scored
    clean = tricks[tricks["merge_method"] != "unresolved_modifier"].copy()
    clean = clean.sort_values(["chain_id", "sequence_index"])

    pairs: list[dict] = []
    for chain_id, grp in clean.groupby("chain_id"):
        grp = grp.sort_values("sequence_index")
        trick_seq = grp["normalized_trick"].tolist()
        pid = grp["person_id"].iloc[0] if "person_id" in grp.columns else None
        eid = grp["event_id"].iloc[0] if "event_id" in grp.columns else None

        for i in range(len(trick_seq) - 1):
            pairs.append({
                "trick_a": trick_seq[i],
                "trick_b": trick_seq[i + 1],
                "person_id": pid,
                "event_id": eid,
            })

    if not pairs:
        return pd.DataFrame(columns=["trick_a", "trick_b", "count", "n_players", "n_events"])

    pairs_df = pd.DataFrame(pairs)
    network = (
        pairs_df.groupby(["trick_a", "trick_b"])
        .agg(
            count=("person_id", "count"),
            n_players=("person_id", lambda x: x.dropna().nunique()),
            n_events=("event_id", "nunique"),
        )
        .reset_index()
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    # Add ADD values for both sides
    return network



# ─────────────────────────────────────────────────────────────────────────────
# 5. Trick innovation timeline
# ─────────────────────────────────────────────────────────────────────────────

def build_trick_innovation_timeline(
    tricks: pd.DataFrame,
    chains: pd.DataFrame,
) -> pd.DataFrame:
    """
    One row per normalized_trick.

    first_year_seen — earliest year the trick appears in any scored chain
    last_year_seen  — most recent year
    mentions        — total appearances across all chains (including unscored)
    players_using   — distinct person_ids performing the trick
    events_using    — distinct event_ids where it appears
    avg_add         — mean ADD value of chains containing this trick (scored only)

    A trick is considered "first seen" in the earliest year it appears in
    sequence_tricks_conservative.csv (chain-level, not free-text mentions),
    so first_year_seen reflects structured competitive usage, not any
    incidental textual reference.
    """
    # Use year from tricks directly if present; otherwise join from chains
    if "year" in tricks.columns:
        t = tricks.copy()
    else:
        chain_years = chains[["chain_id", "year"]].drop_duplicates()
        t = tricks.merge(chain_years, on="chain_id", how="left")

    # Per-trick aggregation
    agg = (
        t.groupby("normalized_trick")
        .agg(
            first_year_seen=("year", "min"),
            last_year_seen=("year", "max"),
            mentions=("chain_id", "count"),
            players_using=("person_id", lambda x: x.dropna().nunique()),
            events_using=("event_id", "nunique"),
        )
        .reset_index()
        .rename(columns={"normalized_trick": "trick"})
    )

    # avg_add: mean ADD of scored chains that contain this trick
    # Join chain sequence_add from chains table
    chain_add = chains[chains["sequence_add"].notna()][["chain_id", "sequence_add"]]
    t_scored = t.merge(chain_add, on="chain_id", how="inner")
    avg_add = (
        t_scored.groupby("normalized_trick")["sequence_add"]
        .mean()
        .round(3)
        .rename("avg_add")
        .reset_index()
        .rename(columns={"normalized_trick": "trick"})
    )
    agg = agg.merge(avg_add, on="trick", how="left")

    agg = agg.sort_values(["first_year_seen", "trick"]).reset_index(drop=True)
    return agg


def build_innovation_by_year(timeline: pd.DataFrame) -> pd.DataFrame:
    """
    Per year: how many tricks were seen for the first time, and what was their
    average / maximum ADD value in chains recorded that year.

    new_tricks         — count of tricks whose first_year_seen == year
    avg_add_new_tricks — mean avg_add across new tricks (NaN if none scored)
    max_add_new_tricks — max avg_add across new tricks (NaN if none scored)
    cumulative_tricks  — running total of distinct tricks seen up to and including year
    """
    rows = []
    seen_so_far = 0
    all_years = sorted(timeline["first_year_seen"].dropna().unique().astype(int))

    for year in all_years:
        cohort = timeline[timeline["first_year_seen"] == year]
        n_new = len(cohort)
        seen_so_far += n_new

        scored = cohort["avg_add"].dropna()
        rows.append({
            "year":               int(year),
            "new_tricks":         n_new,
            "avg_add_new_tricks": round(float(scored.mean()), 3) if len(scored) else None,
            "max_add_new_tricks": round(float(scored.max()), 3) if len(scored) else None,
            "cumulative_tricks":  seen_so_far,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Build freestyle difficulty analytics.")
    ap.add_argument("--chains",    required=True, help="sequence_difficulty_conservative.csv")
    ap.add_argument("--tricks",    required=True, help="sequence_tricks_conservative.csv")
    ap.add_argument("--mentions",  required=True, help="noise_trick_mentions_v2.csv")
    ap.add_argument("--placements",required=True, help="Placements_Flat.csv")
    ap.add_argument("--trick-dictionary", required=True, help="trick_dictionary.csv")
    ap.add_argument("--out-dir",   required=True, help="Output directory")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading inputs…")
    chains     = pd.read_csv(args.chains, low_memory=False)
    tricks     = pd.read_csv(args.tricks, low_memory=False)
    mentions   = pd.read_csv(args.mentions, low_memory=False)
    placements = pd.read_csv(args.placements, low_memory=False)

    print("Building difficulty_by_year…")
    by_year = build_difficulty_by_year(chains)
    p = out_dir / "difficulty_by_year.csv"
    by_year.to_csv(p, index=False)
    print(f"  → {p}  ({len(by_year)} rows)")

    print("Building player_difficulty_profiles…")
    profiles = build_player_difficulty_profiles(chains, tricks, mentions, placements)
    p = out_dir / "player_difficulty_profiles.csv"
    profiles.to_csv(p, index=False)
    print(f"  → {p}  ({len(profiles)} rows)")

    print("Building trick_frequency…")
    freq = build_trick_frequency(mentions, tricks, Path(args.trick_dictionary))
    p = out_dir / "trick_frequency.csv"
    freq.to_csv(p, index=False)
    print(f"  → {p}  ({len(freq)} rows)")

    print("Building trick_transition_network…")
    network = build_trick_transition_network(tricks)
    p = out_dir / "trick_transition_network.csv"
    network.to_csv(p, index=False)
    print(f"  → {p}  ({len(network)} rows)")

    print("Building trick_innovation_timeline…")
    timeline = build_trick_innovation_timeline(tricks, chains)
    p = out_dir / "trick_innovation_timeline.csv"
    timeline.to_csv(p, index=False)
    print(f"  → {p}  ({len(timeline)} rows)")

    print("Building innovation_by_year…")
    innovation = build_innovation_by_year(timeline)
    p = out_dir / "innovation_by_year.csv"
    innovation.to_csv(p, index=False)
    print(f"  → {p}  ({len(innovation)} rows)")

    # ── Console summary ───────────────────────────────────────────────────────
    print("\n" + "═" * 62)
    print("  DIFFICULTY BY YEAR  (scored chains)")
    print("═" * 62)
    print(f"  {'year':>4}  {'chains':>6}  {'avg_seq':>7}  {'p90':>6}  {'max':>5}  {'avg/trick':>9}")
    for _, r in by_year.iterrows():
        print(f"  {int(r['year']):>4}  {int(r['n_chains']):>6}  "
              f"{r['avg_sequence_add']:>7.2f}  {r['p90_sequence_add']:>6.1f}  "
              f"{int(r['max_sequence_add']):>5}  {r['avg_avg_add']:>9.3f}")
    print("═" * 62)

    top_players = profiles.head(20)
    print("\n  TOP 20 PLAYERS BY max_sequence_add")
    print(f"  {'person_canon':<28}  {'max':>4}  {'avg':>6}  {'chains':>6}  {'tricks':>6}  {'yrs':>8}")
    for _, r in top_players.iterrows():
        yspan = f"{int(r['year_first'])}–{int(r['year_last'])}" if pd.notna(r.get('year_first')) else "?"
        print(f"  {str(r['person_canon']):<28}  "
              f"{r['max_sequence_add']:>4.0f}  "
              f"{r['avg_sequence_add']:>6.2f}  "
              f"{int(r['chains_scored']):>6}  "
              f"{int(r['n_distinct_tricks']):>6}  "
              f"{yspan:>8}")

    print("\n  TOP 20 TRICKS BY TOTAL MENTIONS")
    print(f"  {'trick_canon':<28}  {'ADD':>3}  {'total':>5}  {'direct':>6}  {'players':>7}  {'events':>6}")
    for _, r in freq.head(20).iterrows():
        adds = int(r["adds"]) if pd.notna(r.get("adds")) else "?"
        print(f"  {str(r['trick_canon']):<28}  {str(adds):>3}  "
              f"{int(r['total_mentions']):>5}  {int(r['direct_mentions']):>6}  "
              f"{int(r['n_players']):>7}  {int(r['n_events']):>6}")

    print("\n  TOP 20 TRICK TRANSITIONS (A → B)")
    print(f"  {'trick_a':<24}  {'trick_b':<24}  {'count':>5}  {'players':>7}")
    for _, r in network.head(20).iterrows():
        print(f"  {str(r['trick_a']):<24}  →  {str(r['trick_b']):<24}  "
              f"{int(r['count']):>5}  {int(r['n_players']):>7}")

    print("\n  TRICK INNOVATION TIMELINE  (first 30 tricks by year)")
    print(f"  {'trick':<28}  {'first':>5}  {'last':>5}  {'uses':>5}  {'ply':>4}  {'ev':>3}  {'avg_add':>7}")
    for _, r in timeline.head(30).iterrows():
        avg = f"{r['avg_add']:.2f}" if pd.notna(r.get("avg_add")) else "?"
        print(f"  {str(r['trick']):<28}  "
              f"{int(r['first_year_seen']):>5}  {int(r['last_year_seen']):>5}  "
              f"{int(r['mentions']):>5}  {int(r['players_using']):>4}  "
              f"{int(r['events_using']):>3}  {avg:>7}")

    print("\n  INNOVATION BY YEAR  (new tricks per year)")
    print(f"  {'year':>4}  {'new':>4}  {'cumul':>6}  {'avg_add_new':>11}  {'max_add_new':>11}")
    for _, r in innovation.iterrows():
        avg = f"{r['avg_add_new_tricks']:.2f}" if pd.notna(r.get("avg_add_new_tricks")) else "?"
        mx  = f"{r['max_add_new_tricks']:.2f}" if pd.notna(r.get("max_add_new_tricks")) else "?"
        print(f"  {int(r['year']):>4}  {int(r['new_tricks']):>4}  "
              f"{int(r['cumulative_tricks']):>6}  {avg:>11}  {mx:>11}")


if __name__ == "__main__":
    main()
