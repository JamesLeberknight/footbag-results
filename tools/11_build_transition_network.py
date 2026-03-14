#!/usr/bin/env python3
"""
11_build_transition_network.py

Build a directed trick transition network from ordered trick sequences.

Input:
  sequence_tricks_conservative.csv  — one row per trick per chain, with
                                      chain_id, sequence_index, normalized_trick

Outputs:
  trick_transition_matrix.csv  — pivot table: rows=trick_a, cols=trick_b, values=count
  trick_transition_network.csv — edge list with full metrics per (A→B) pair

Per-edge metrics:
  count         — raw transition frequency
  prob_b_given_a— P(B | A): count(A→B) / out_degree(A)
  prob_a_given_b— P(A | B): count(A→B) / in_degree(B)
  n_players     — distinct person_ids performing this transition
  n_events      — distinct event_ids where it appears
  players       — semicolon-separated player names
  adds_a, adds_b— ADD values if known

Per-node metrics (appended to network file as a second pass, also printed):
  trick         — normalized_trick
  in_degree     — sum of all incoming transition counts
  out_degree    — sum of all outgoing transition counts
  degree        — in_degree + out_degree
  n_as_source   — distinct (trick_a=trick) pairs
  n_as_target   — distinct (trick_b=trick) pairs
  pagerank      — power-iteration PageRank over the weighted graph
  hub_score     — HITS hub score
  auth_score    — HITS authority score
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_add_values(tricks_path: Path | None) -> dict[str, int | None]:
    """Load ADD values from trick_dictionary.csv if available."""
    if tricks_path is None or not tricks_path.exists():
        return {}
    df = pd.read_csv(tricks_path)
    out: dict[str, int | None] = {}
    for _, row in df.iterrows():
        t = str(row.get("trick_canon", "")).strip()
        a = row.get("add_value")
        if t:
            out[t] = int(a) if pd.notna(a) else None
    return out


def power_iteration_pagerank(
    nodes: list[str],
    edges: pd.DataFrame,
    damping: float = 0.85,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> dict[str, float]:
    """Weighted PageRank via power iteration."""
    n = len(nodes)
    if n == 0:
        return {}
    idx = {t: i for i, t in enumerate(nodes)}
    pr = np.full(n, 1.0 / n)

    # Build column-normalised adjacency (out-stochastic)
    out_totals: dict[str, float] = {}
    for _, row in edges.iterrows():
        out_totals[row["trick_a"]] = out_totals.get(row["trick_a"], 0) + row["count"]

    transitions: list[tuple[int, int, float]] = []
    for _, row in edges.iterrows():
        a, b, c = row["trick_a"], row["trick_b"], row["count"]
        if a in idx and b in idx and out_totals.get(a, 0) > 0:
            transitions.append((idx[b], idx[a], c / out_totals[a]))

    for _ in range(max_iter):
        new_pr = np.full(n, (1 - damping) / n)
        for to_i, from_i, w in transitions:
            new_pr[to_i] += damping * w * pr[from_i]
        diff = np.abs(new_pr - pr).sum()
        pr = new_pr
        if diff < tol:
            break

    return {nodes[i]: float(round(pr[i], 6)) for i in range(n)}


def hits(
    nodes: list[str],
    edges: pd.DataFrame,
    max_iter: int = 100,
    tol: float = 1e-6,
) -> tuple[dict[str, float], dict[str, float]]:
    """Weighted HITS (hub / authority) scores."""
    n = len(nodes)
    if n == 0:
        return {}, {}
    idx = {t: i for i, t in enumerate(nodes)}

    hub  = np.ones(n)
    auth = np.ones(n)

    edge_list: list[tuple[int, int, float]] = []
    for _, row in edges.iterrows():
        a, b, c = row["trick_a"], row["trick_b"], float(row["count"])
        if a in idx and b in idx:
            edge_list.append((idx[a], idx[b], c))

    for _ in range(max_iter):
        new_auth = np.zeros(n)
        for from_i, to_i, w in edge_list:
            new_auth[to_i] += hub[from_i] * w
        norm = np.linalg.norm(new_auth)
        if norm > 0:
            new_auth /= norm

        new_hub = np.zeros(n)
        for from_i, to_i, w in edge_list:
            new_hub[from_i] += new_auth[to_i] * w
        norm = np.linalg.norm(new_hub)
        if norm > 0:
            new_hub /= norm

        if np.abs(new_hub - hub).sum() + np.abs(new_auth - auth).sum() < tol:
            hub, auth = new_hub, new_auth
            break
        hub, auth = new_hub, new_auth

    return (
        {nodes[i]: float(round(hub[i],  6)) for i in range(n)},
        {nodes[i]: float(round(auth[i], 6)) for i in range(n)},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Core builders
# ─────────────────────────────────────────────────────────────────────────────

def build_edge_list(tricks: pd.DataFrame) -> pd.DataFrame:
    """
    Extract adjacent (A→B) pairs from ordered chains.
    Returns long-form DataFrame: trick_a, trick_b, person_id, person_canon, event_id, chain_id
    """
    clean = tricks.copy()
    # Filter out unresolved modifier residues if merge_method column present
    if "merge_method" in clean.columns:
        clean = clean[clean["merge_method"] != "unresolved_modifier"]

    clean = clean.sort_values(["chain_id", "sequence_index"])

    pairs: list[dict] = []
    for chain_id, grp in clean.groupby("chain_id"):
        grp = grp.sort_values("sequence_index")
        seq     = grp["normalized_trick"].tolist()
        pcanon  = grp["person_canon"].iloc[0] if "person_canon" in grp.columns else None
        pid     = grp["person_id"].iloc[0]    if "person_id"    in grp.columns else None
        eid     = grp["event_id"].iloc[0]     if "event_id"     in grp.columns else None

        for i in range(len(seq) - 1):
            pairs.append({
                "trick_a":     seq[i],
                "trick_b":     seq[i + 1],
                "person_id":   pid,
                "person_canon": pcanon,
                "event_id":    eid,
                "chain_id":    chain_id,
            })

    return pd.DataFrame(pairs) if pairs else pd.DataFrame(
        columns=["trick_a", "trick_b", "person_id", "person_canon", "event_id", "chain_id"]
    )


def build_network_edges(
    pairs: pd.DataFrame,
    add_values: dict[str, int | None],
) -> pd.DataFrame:
    """Aggregate pairs into edge list with per-edge metrics."""
    if pairs.empty:
        return pd.DataFrame()

    agg = (
        pairs.groupby(["trick_a", "trick_b"])
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
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    # Out-degree denominators for P(B|A)
    out_totals = agg.groupby("trick_a")["count"].transform("sum")
    agg["prob_b_given_a"] = (agg["count"] / out_totals).round(4)

    # In-degree denominators for P(A|B)
    in_totals = agg.groupby("trick_b")["count"].transform("sum")
    agg["prob_a_given_b"] = (agg["count"] / in_totals).round(4)

    # ADD annotations
    agg["adds_a"] = agg["trick_a"].map(add_values)
    agg["adds_b"] = agg["trick_b"].map(add_values)

    def _safe_sum(a, b):
        if pd.notna(a) and pd.notna(b):
            return int(a) + int(b)
        return None
    agg["pair_add_sum"] = agg.apply(lambda r: _safe_sum(r["adds_a"], r["adds_b"]), axis=1)

    col_order = [
        "trick_a", "trick_b",
        "count", "prob_b_given_a", "prob_a_given_b",
        "n_players", "n_events",
        "adds_a", "adds_b", "pair_add_sum",
        "players", "example_chain",
    ]
    return agg[col_order]


def build_node_metrics(
    edges: pd.DataFrame,
    add_values: dict[str, int | None],
) -> pd.DataFrame:
    """Per-trick node-level centrality metrics."""
    if edges.empty:
        return pd.DataFrame()

    all_tricks = sorted(
        set(edges["trick_a"].tolist()) | set(edges["trick_b"].tolist())
    )

    in_deg  = edges.groupby("trick_b")["count"].sum().rename("in_degree")
    out_deg = edges.groupby("trick_a")["count"].sum().rename("out_degree")
    n_src   = edges.groupby("trick_a")["trick_b"].nunique().rename("n_as_source")
    n_tgt   = edges.groupby("trick_b")["trick_a"].nunique().rename("n_as_target")

    nodes = (
        pd.DataFrame({"trick": all_tricks})
        .join(in_deg,  on="trick")
        .join(out_deg, on="trick")
        .join(n_src,   on="trick")
        .join(n_tgt,   on="trick")
        .fillna(0)
    )
    nodes["in_degree"]  = nodes["in_degree"].astype(int)
    nodes["out_degree"] = nodes["out_degree"].astype(int)
    nodes["degree"]     = nodes["in_degree"] + nodes["out_degree"]
    nodes["n_as_source"] = nodes["n_as_source"].astype(int)
    nodes["n_as_target"] = nodes["n_as_target"].astype(int)

    pr        = power_iteration_pagerank(all_tricks, edges)
    hubs, auths = hits(all_tricks, edges)

    nodes["pagerank"]   = nodes["trick"].map(pr)
    nodes["hub_score"]  = nodes["trick"].map(hubs)
    nodes["auth_score"] = nodes["trick"].map(auths)
    nodes["add_value"]  = nodes["trick"].map(add_values)

    nodes = nodes.sort_values("degree", ascending=False).reset_index(drop=True)
    return nodes


def build_pivot_matrix(edges: pd.DataFrame) -> pd.DataFrame:
    """Trick × trick count pivot (sparse, sorted by row total)."""
    if edges.empty:
        return pd.DataFrame()

    piv = (
        edges.pivot_table(
            index="trick_a",
            columns="trick_b",
            values="count",
            fill_value=0,
            aggfunc="sum",
        )
        .astype(int)
    )
    row_totals = piv.sum(axis=1).sort_values(ascending=False)
    piv = piv.loc[row_totals.index]
    col_totals = piv.sum(axis=0).sort_values(ascending=False)
    piv = piv[col_totals.index]
    piv.columns.name = None
    return piv


# ─────────────────────────────────────────────────────────────────────────────
# Console output
# ─────────────────────────────────────────────────────────────────────────────

def print_top_transitions(edges: pd.DataFrame, top_n: int = 50) -> None:
    print(f"\n{'═' * 85}")
    print(f"  TOP {top_n} TRICK TRANSITIONS  (by frequency)")
    print(f"{'═' * 85}")
    print(f"  {'trick_a':<24}  {'trick_b':<24}  "
          f"{'cnt':>4}  {'P(B|A)':>6}  {'ply':>3}  {'ev':>3}  {'A+B':>4}  players (first 3)")
    for _, r in edges.head(top_n).iterrows():
        ab    = str(int(r["pair_add_sum"])) if pd.notna(r.get("pair_add_sum")) else "?"
        plist = str(r["players"]).split("; ")
        pshow = "; ".join(plist[:3]) + ("…" if len(plist) > 3 else "")
        print(f"  {str(r['trick_a']):<24}  →  {str(r['trick_b']):<24}  "
              f"{int(r['count']):>4}  {r['prob_b_given_a']:>6.3f}  "
              f"{int(r['n_players']):>3}  {int(r['n_events']):>3}  "
              f"{ab:>4}  {pshow}")


def print_node_metrics(nodes: pd.DataFrame, top_n: int = 30) -> None:
    print(f"\n{'═' * 80}")
    print(f"  TRICK CENTRALITY  (top {top_n} by degree)")
    print(f"{'═' * 80}")
    print(f"  {'trick':<28}  {'in':>4}  {'out':>4}  {'deg':>4}  "
          f"{'PR':>7}  {'hub':>6}  {'auth':>6}  {'ADD':>3}")
    for _, r in nodes.head(top_n).iterrows():
        add = str(int(r["add_value"])) if pd.notna(r.get("add_value")) else "?"
        print(f"  {str(r['trick']):<28}  "
              f"{int(r['in_degree']):>4}  {int(r['out_degree']):>4}  "
              f"{int(r['degree']):>4}  "
              f"{r['pagerank']:>7.5f}  {r['hub_score']:>6.4f}  "
              f"{r['auth_score']:>6.4f}  {add:>3}")

    # Top hubs and authorities separately
    hubs  = nodes.nlargest(10, "hub_score")
    auths = nodes.nlargest(10, "auth_score")
    print("\n  TOP HUBS (tricks most often leading INTO multi-trick combos)")
    for _, r in hubs.iterrows():
        print(f"    {str(r['trick']):<30}  hub={r['hub_score']:.4f}  "
              f"out={int(r['out_degree'])}")
    print("\n  TOP AUTHORITIES (tricks most often following a lead-in)")
    for _, r in auths.iterrows():
        print(f"    {str(r['trick']):<30}  auth={r['auth_score']:.4f}  "
              f"in={int(r['in_degree'])}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Build directed trick transition network.")
    ap.add_argument("--tricks",      required=True,
                    help="sequence_tricks_conservative.csv")
    ap.add_argument("--dictionary",  default=None,
                    help="trick_dictionary.csv (for ADD annotations)")
    ap.add_argument("--out-dir",     required=True, help="Output directory")
    ap.add_argument("--top-n",       type=int, default=50,
                    help="Top-N transitions for console table (default 50)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading inputs…")
    tricks = pd.read_csv(args.tricks, low_memory=False)
    add_values = load_add_values(
        Path(args.dictionary) if args.dictionary else None
    )
    print(f"  {len(tricks):,} trick rows, {tricks['chain_id'].nunique():,} chains")

    print("Extracting transition pairs…")
    pairs = build_edge_list(tricks)
    print(f"  {len(pairs):,} raw pair rows")

    print("Building edge metrics…")
    edges = build_network_edges(pairs, add_values)
    print(f"  {len(edges):,} unique directed edges")

    print("Computing node centrality…")
    nodes = build_node_metrics(edges, add_values)

    print("Building pivot matrix…")
    matrix = build_pivot_matrix(edges)

    # ── Write outputs ─────────────────────────────────────────────────────────
    net_path = out_dir / "trick_transition_network.csv"
    edges.to_csv(net_path, index=False)
    print(f"\n  → {net_path}  ({len(edges)} edges)")

    node_path = out_dir / "trick_node_metrics.csv"
    nodes.to_csv(node_path, index=False)
    print(f"  → {node_path}  ({len(nodes)} nodes)")

    mat_path = out_dir / "trick_transition_matrix.csv"
    matrix.to_csv(mat_path)
    print(f"  → {mat_path}  ({matrix.shape[0]}×{matrix.shape[1]})")

    # ── Console summaries ─────────────────────────────────────────────────────
    print_top_transitions(edges, top_n=args.top_n)
    print_node_metrics(nodes, top_n=30)

    # Summary stats
    total_transitions = int(edges["count"].sum())
    n_tricks = len(nodes)
    n_edges  = len(edges)
    density  = n_edges / (n_tricks * (n_tricks - 1)) if n_tricks > 1 else 0
    print(f"\n  Network summary: {n_tricks} nodes, {n_edges} edges, "
          f"{total_transitions:,} total transitions, density={density:.4f}")

    top5_pr = nodes.nlargest(5, "pagerank")[["trick", "pagerank"]].values.tolist()
    print("  Top-5 by PageRank: " +
          ", ".join(f"{t} ({v:.5f})" for t, v in top5_pr))


if __name__ == "__main__":
    main()
