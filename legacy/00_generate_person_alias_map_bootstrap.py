#!/usr/bin/env python3
"""
00_generate_person_alias_map_bootstrap.py

Goal:
  Produce out/person_alias_map_bootstrap.csv as a HUMAN-EDITABLE bootstrap mapping.

Inputs (preferred):
  out/stage2p5_players_clean.csv

Output:
  out/person_alias_map_bootstrap.csv

Design (conservative / correctness-first):
  - We DO NOT auto-merge identities.
  - We produce "alias groups" using a stable normalization key.
  - If a group has exactly 1 player_id, we can safely suggest that player_id.
  - If a group has multiple player_ids, we suggest a "most-used" candidate but leave decision blank.

Columns:
  alias_group_key,
  alias_name,
  suggested_canonical_player_id,
  suggested_canonical_name,
  confidence,
  usage_count_total,
  player_ids_in_group,
  countries_seen,
  decision,
  notes
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


_ILLEGAL_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^a-z0-9\s]")


def _strip_diacritics(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(ch)
    )


def normalize_person_key(name: str) -> str:
    """
    Conservative, grouping-only normalization:
      - lowercase
      - remove diacritics
      - remove punctuation
      - collapse whitespace
      - drop single-letter middle initials
    """
    if not isinstance(name, str) or not name.strip():
        return ""
    t = _strip_diacritics(name).lower()
    t = _PUNCT_RE.sub(" ", t)
    t = _ILLEGAL_WS_RE.sub(" ", t).strip()
    t = re.sub(r"\b([a-z])\b", "", t)
    t = _ILLEGAL_WS_RE.sub(" ", t).strip()
    return t


def _alias_confidence(alias_names: list[str], key: str) -> str:
    """
    Same heuristic as Stage 3 presentation logic:
      - high: all normalize to same key
      - med: >1 alias and share same last token after normalization
      - low: otherwise
    """
    normed = [normalize_person_key(a) for a in alias_names if isinstance(a, str)]
    normed = [n for n in normed if n]
    if normed and all(n == key for n in normed):
        return "high"

    toks = [n.split() for n in normed if n.split()]
    lasts = [t[-1] for t in toks if t]
    if len(set(lasts)) == 1 and len(lasts) >= 2:
        return "med"
    return "low"


def main() -> None:
    repo_dir = Path(__file__).resolve().parent
    out_dir = repo_dir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    in_players = out_dir / "stage2p5_players_clean.csv"
    if not in_players.exists():
        raise SystemExit(f"ERROR: Missing input: {in_players}\nRun Stage 2.5 or provide this file.")

    df = pd.read_csv(in_players)

    # Defensive: ensure columns exist
    for col in [
        "player_id", "player_name_clean", "player_name_raw",
        "country_clean", "country_observed", "usage_count", "name_key"
    ]:
        if col not in df.columns:
            df[col] = ""

    # Only non-junk (same intent as Stage 3 sheets)
    if "name_status" in df.columns:
        df = df[df["name_status"].isin(["ok", "suspicious", "needs_review"])].copy()

    # Build grouping key:
    # Prefer Stage 2.5 name_key; else normalize clean/raw name.
    def group_key(r) -> str:
        nk = r.get("name_key", "")
        if isinstance(nk, str) and nk.strip():
            return nk.strip()
        nm = r.get("player_name_clean", "")
        if not (isinstance(nm, str) and nm.strip()):
            nm = r.get("player_name_raw", "")
        return normalize_person_key(nm)

    df["alias_group_key"] = df.apply(group_key, axis=1).fillna("").astype(str)
    df = df[df["alias_group_key"].str.len() > 0].copy()

    rows = []

    for gkey, g in df.groupby("alias_group_key"):
        # Collect aliases: prefer clean, include raw
        aliases = []
        for v in g["player_name_clean"].dropna().astype(str).tolist():
            v = v.strip()
            if v:
                aliases.append(v)
        for v in g["player_name_raw"].dropna().astype(str).tolist():
            v = v.strip()
            if v:
                aliases.append(v)

        # stable unique by casefold
        seen = set()
        aliases_u = []
        for a in aliases:
            k = a.casefold()
            if k not in seen:
                seen.add(k)
                aliases_u.append(a)

        player_ids = [str(x).strip() for x in g["player_id"].dropna().astype(str).tolist() if str(x).strip()]
        player_ids_u = sorted(set(player_ids))

        # usage rollup
        usage_total = 0
        try:
            usage_total = int(pd.to_numeric(g["usage_count"], errors="coerce").fillna(0).sum())
        except Exception:
            usage_total = 0

        # countries rollup
        countries = []
        for col in ("country_clean", "country_observed"):
            if col in g.columns:
                countries += [str(x).strip() for x in g[col].dropna().astype(str).tolist() if str(x).strip()]
        countries_u = sorted(set(countries))

        # Suggest canonical:
        # - if only 1 player_id => safe suggestion
        # - else choose the player_id with max usage_count in group (suggestion only)
        suggested_id = ""
        suggested_name = ""
        decision = ""

        if len(player_ids_u) == 1:
            suggested_id = player_ids_u[0]
            # pick best name among rows with this id
            g1 = g[g["player_id"].astype(str) == suggested_id]
            # prefer shortest clean name
            cand_names = [str(x).strip() for x in g1["player_name_clean"].dropna().astype(str).tolist() if str(x).strip()]
            if not cand_names:
                cand_names = [str(x).strip() for x in g1["player_name_raw"].dropna().astype(str).tolist() if str(x).strip()]
            cand_names.sort(key=lambda x: (len(x), x.casefold()))
            suggested_name = cand_names[0] if cand_names else ""
            decision = "AUTO_SINGLE_ID"
        else:
            # choose most-used id in this group (suggestion only)
            tmp = g.copy()
            tmp["usage_count_num"] = pd.to_numeric(tmp["usage_count"], errors="coerce").fillna(0)
            usage_by_id = tmp.groupby(tmp["player_id"].astype(str))["usage_count_num"].sum().to_dict()
            # remove empty key
            usage_by_id.pop("", None)
            if usage_by_id:
                suggested_id = sorted(usage_by_id.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
                g1 = g[g["player_id"].astype(str) == suggested_id]
                cand_names = [str(x).strip() for x in g1["player_name_clean"].dropna().astype(str).tolist() if str(x).strip()]
                if not cand_names:
                    cand_names = [str(x).strip() for x in g1["player_name_raw"].dropna().astype(str).tolist() if str(x).strip()]
                cand_names.sort(key=lambda x: (len(x), x.casefold()))
                suggested_name = cand_names[0] if cand_names else ""
            decision = ""  # force human decision

        conf = _alias_confidence(aliases_u, normalize_person_key(suggested_name) or gkey)

        for alias_name in aliases_u:
            rows.append({
                "alias_group_key": gkey,
                "alias_name": alias_name,
                "suggested_canonical_player_id": suggested_id,
                "suggested_canonical_name": suggested_name,
                "confidence": conf,
                "usage_count_total": usage_total,
                "player_ids_in_group": " | ".join(player_ids_u),
                "countries_seen": " | ".join(countries_u),
                "decision": decision if decision == "AUTO_SINGLE_ID" else "",
                "notes": "",
            })

    out_csv = out_dir / "person_alias_map_bootstrap.csv"
    out_df = pd.DataFrame(rows)
    # Helpful ordering: highest usage first, then confidence, then alias_name
    conf_rank = {"high": 0, "med": 1, "low": 2}
    if not out_df.empty:
        out_df["__conf_rank"] = out_df["confidence"].map(conf_rank).fillna(9).astype(int)
        out_df.sort_values(
            by=["usage_count_total", "__conf_rank", "alias_group_key", "alias_name"],
            ascending=[False, True, True, True],
            inplace=True,
        )
        out_df.drop(columns=["__conf_rank"], inplace=True)

    out_df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"Wrote: {out_csv} ({len(out_df)} rows)")


if __name__ == "__main__":
    main()
