#!/usr/bin/env python3
"""
tools/12_generate_alias_suggestions.py

Generate high-precision, non-authoritative alias suggestions from out/Placements_Flat.csv.

Goal:
  - Suggest only "very safe" alias merges (diacritics/case/punctuation/spacing/encoding artifacts).
  - Provide a deterministic proposed_person_id per cluster (uuid5 from normalized key).
  - Output suggestions for human review; DO NOT modify overrides/person_aliases.csv.

Inputs:
  - out/Placements_Flat.csv

Outputs:
  - out/person_alias_suggestions.csv

Notes:
  - This is a diagnostics / review artifact.
  - It never affects truth unless you later promote accepted rows into overrides/person_aliases.csv.
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
import uuid
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd


# Stable namespace for deterministic uuid5 generation
# You can change this once if you want, but then IDs will change across runs.
SUGGESTION_NAMESPACE_UUID = uuid.UUID("2f9f3b6e-1d4a-4f8f-8b1f-7f7e2d4f2c11")


_RE_BAD_TOKENS = re.compile(r"[:$]|(\s\d+\.\s)|\bAnonymous\b", re.IGNORECASE)
_RE_MULTI_WS = re.compile(r"\s+")
_RE_PUNCT = re.compile(r"[^\w\s]", re.UNICODE)


def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr)


def info(msg: str) -> None:
    print(f"INFO: {msg}", file=sys.stderr)


def strip_diacritics(s: str) -> str:
    """
    Convert to NFKD and drop combining marks.
    Handles many diacritic/encoding variants safely.
    """
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_key(name: str) -> str:
    """
    Conservative normalization key:
      - strip diacritics
      - lowercase
      - remove punctuation
      - collapse whitespace
    """
    s = (name or "").strip()
    s = strip_diacritics(s)
    s = s.lower()
    s = _RE_PUNCT.sub(" ", s)      # remove punctuation -> spaces
    s = _RE_MULTI_WS.sub(" ", s).strip()
    return s


def is_polluted(name: str) -> bool:
    """
    Detect obviously polluted tokens (results annotations, money, numbered fragments, etc.)
    These are NOT safe to auto-suggest.
    """
    s = (name or "").strip()
    if not s:
        return True
    if _RE_BAD_TOKENS.search(s):
        return True
    return False


def proposed_person_id_from_key(key: str) -> str:
    """
    Deterministic ID stable across runs for the same normalized key.
    """
    return str(uuid.uuid5(SUGGESTION_NAMESPACE_UUID, "AUTO:" + key))


def build_name_counts(pf: pd.DataFrame) -> pd.DataFrame:
    """
    Return a table with raw name counts in player1/player2 positions.
    Columns: name, as_player1, as_player2, appearances
    """
    rows = []
    for side in ["player1", "player2"]:
        name_col = f"{side}_name"
        if name_col not in pf.columns:
            continue
        vc = pf[name_col].fillna("").astype(str).str.strip()
        vc = vc[vc != ""].value_counts()
        for nm, cnt in vc.items():
            rows.append({"name": nm, f"as_{side}": int(cnt)})

    if not rows:
        return pd.DataFrame(columns=["name", "as_player1", "as_player2", "appearances"])

    df = pd.DataFrame(rows)
    # fill missing side columns
    if "as_player1" not in df.columns:
        df["as_player1"] = 0
    if "as_player2" not in df.columns:
        df["as_player2"] = 0

    df = (
        df.groupby("name", as_index=False)
        .agg(as_player1=("as_player1", "sum"), as_player2=("as_player2", "sum"))
    )
    df["appearances"] = df["as_player1"] + df["as_player2"]
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--placements_flat", default="out/Placements_Flat.csv")
    ap.add_argument("--out_csv", default="out/person_alias_suggestions.csv")
    ap.add_argument("--min_appearances", type=int, default=5,
                    help="Ignore names that appear fewer than this many times.")
    ap.add_argument("--limit_clusters", type=int, default=500,
                    help="Max number of clusters to emit (highest impact first).")
    args = ap.parse_args()

    pf_path = Path(args.placements_flat)
    out_path = Path(args.out_csv)

    if not pf_path.exists():
        print(f"ERROR: missing {pf_path} (run 02p5 first)", file=sys.stderr)
        return 2

    pf = pd.read_csv(pf_path)
    counts = build_name_counts(pf)

    if len(counts) == 0:
        warn("No names found in Placements_Flat; nothing to do.")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_path, index=False)
        return 0

    # Filter tiny counts
    counts = counts[counts["appearances"] >= args.min_appearances].copy()

    # Compute normalization key and pollution flag
    counts["norm_key"] = counts["name"].map(normalize_key)
    counts["is_polluted"] = counts["name"].map(is_polluted)

    # Only consider non-polluted candidates for "auto-safe" suggestions
    safe = counts[~counts["is_polluted"]].copy()

    # Group by normalized key; only keys with multiple distinct raw names are interesting
    grp = safe.groupby("norm_key")

    clusters = []
    for key, g in grp:
        raw_names = sorted(set(g["name"].tolist()))
        if len(raw_names) < 2:
            continue

        total = int(g["appearances"].sum())
        # Deterministic cluster id from key
        proposed_pid = proposed_person_id_from_key(key)

        # Emit one row per alias name
        for _, row in g.sort_values("appearances", ascending=False).iterrows():
            clusters.append({
                "cluster_key": key,
                "proposed_person_id": proposed_pid,
                "alias": row["name"],
                "person_canon": raw_names[0],  # default canon choice; human can change later
                "confidence": 0.99,            # high precision bucket
                "reason": "normalized_key_match",
                "appearances": int(row["appearances"]),
                "as_player1": int(row["as_player1"]),
                "as_player2": int(row["as_player2"]),
                "human_decision": "",          # accept/reject/skip
                "notes": "",
            })

        # cluster summary row (optional later) — for now keep per-alias rows only

    if not clusters:
        info("No multi-variant safe clusters found (after normalization).")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=[
            "cluster_key","proposed_person_id","alias","person_canon","confidence","reason",
            "appearances","as_player1","as_player2","human_decision","notes"
        ]).to_csv(out_path, index=False)
        return 0

    out_df = pd.DataFrame(clusters)

    # Rank clusters by total impact (sum appearances per cluster), then alias appearances
    cluster_totals = out_df.groupby("proposed_person_id", as_index=False)["appearances"].sum()
    cluster_totals.rename(columns={"appearances": "cluster_total_appearances"}, inplace=True)
    out_df = out_df.merge(cluster_totals, on="proposed_person_id", how="left")
    out_df.sort_values(
        by=["cluster_total_appearances", "proposed_person_id", "appearances", "alias"],
        ascending=[False, True, False, True],
        inplace=True
    )

    # Limit clusters
    # Keep the top N clusters, but keep all rows within those clusters.
    top_pids = (
        out_df[["proposed_person_id", "cluster_total_appearances"]]
        .drop_duplicates()
        .head(args.limit_clusters)["proposed_person_id"]
        .tolist()
    )
    out_df = out_df[out_df["proposed_person_id"].isin(top_pids)].copy()
    out_df.drop(columns=["cluster_total_appearances"], inplace=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    info(f"Wrote {out_path} ({len(out_df)} rows; {len(top_pids)} clusters).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
