#!/usr/bin/env python3
"""
tools/13_generate_alias_suggestions_questionable.py

Generate *questionable* alias suggestions (initials, nicknames-ish) for HUMAN REVIEW ONLY.

Inputs:
  - out/Placements_Flat.csv

Outputs:
  - out/person_alias_suggestions_questionable.csv

Guarantees:
  - Does NOT modify overrides/person_aliases.csv
  - Leaves human_decision blank
  - Produces deterministic proposed_person_id via uuid5 from a cluster key
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


def load_verified_aliases(path: str) -> set[str]:
    """
    Load verified aliases so we can suppress already-solved suggestions.
    """
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        return set()

    if "alias" not in df.columns:
        return set()

    return set(
        df["alias"]
        .fillna("")
        .astype(str)
        .str.strip()
    )


# Stable namespace for deterministic uuid5 generation (use same as script 12 for consistency)
SUGGESTION_NAMESPACE_UUID = uuid.UUID("2f9f3b6e-1d4a-4f8f-8b1f-7f7e2d4f2c11")

_RE_MULTI_WS = re.compile(r"\s+")
_RE_PUNCT = re.compile(r"[^\w\s]", re.UNICODE)
_RE_BAD_TOKENS = re.compile(r"[:$]|(\s\d+\.\s)|\bAnonymous\b", re.IGNORECASE)


def info(msg: str) -> None:
    print(f"INFO: {msg}", file=sys.stderr)


def warn(msg: str) -> None:
    print(f"WARN: {msg}", file=sys.stderr)


def strip_diacritics(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_spaces(s: str) -> str:
    return _RE_MULTI_WS.sub(" ", (s or "").strip())


def is_polluted(name: str) -> bool:
    s = norm_spaces(name)
    if not s:
        return True
    return _RE_BAD_TOKENS.search(s) is not None


def clean_basic(name: str) -> str:
    """
    Conservative cleaning for token parsing only (does not change raw alias output):
    - strip diacritics
    - lowercase
    - remove punctuation
    - collapse whitespace
    """
    s = norm_spaces(name)
    s = strip_diacritics(s)
    s = s.lower()
    s = _RE_PUNCT.sub(" ", s)
    s = _RE_MULTI_WS.sub(" ", s).strip()
    return s


@dataclass
class NameParts:
    raw: str
    first: str
    last: str
    first_initial: str

def split_name(name: str) -> NameParts:
    raw = norm_spaces(name)
    s = clean_basic(raw)
    toks = s.split()
    if len(toks) == 0:
        return NameParts(raw=raw, first="", last="", first_initial="")
    if len(toks) == 1:
        return NameParts(raw=raw, first=toks[0], last=toks[0], first_initial=toks[0][:1])
    first = toks[0]
    last = toks[-1]
    return NameParts(raw=raw, first=first, last=last, first_initial=first[:1])


def proposed_person_id_from_key(key: str) -> str:
    return str(uuid.uuid5(SUGGESTION_NAMESPACE_UUID, "Q:" + key))


def build_name_counts(pf: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for side in ["player1", "player2"]:
        c = f"{side}_name"
        if c not in pf.columns:
            continue
        vc = pf[c].fillna("").astype(str).str.strip()
        vc = vc[vc != ""].value_counts()
        for nm, cnt in vc.items():
            rows.append({"name": nm, f"as_{side}": int(cnt)})

    if not rows:
        return pd.DataFrame(columns=["name", "as_player1", "as_player2", "appearances"])

    df = pd.DataFrame(rows)
    if "as_player1" not in df.columns: df["as_player1"] = 0
    if "as_player2" not in df.columns: df["as_player2"] = 0
    df = df.groupby("name", as_index=False).agg(as_player1=("as_player1","sum"), as_player2=("as_player2","sum"))
    df["appearances"] = df["as_player1"] + df["as_player2"]
    return df


def build_cooccur_pairs(pf: pd.DataFrame) -> set[Tuple[str, str]]:
    """
    Build name co-occurrence within the SAME event_id + division_canon (strong evidence they are different people).
    Returns a set of (a,b) pairs (sorted) that co-occur as distinct names in same group.
    """
    needed = ["event_id", "division_canon", "player1_name", "player2_name"]
    for c in needed:
        if c not in pf.columns:
            warn(f"Placements_Flat missing {c}; co-occurrence evidence disabled.")
            return set()

    co = set()
    g = pf.groupby(["event_id", "division_canon"], dropna=False)
    for _, grp in g:
        names = set()
        p1 = grp["player1_name"].fillna("").astype(str).str.strip()
        p2 = grp["player2_name"].fillna("").astype(str).str.strip()
        for x in pd.concat([p1, p2], ignore_index=True).tolist():
            if x:
                names.add(x)
        names = sorted(names)
        for i in range(len(names)):
            for j in range(i+1, len(names)):
                a, b = names[i], names[j]
                co.add((a, b))
    return co


def is_initial_form(p: NameParts) -> bool:
    # e.g. "k shults" or "k. shults" -> first token length 1
    return len(p.first) == 1


def nickname_hint(a_first: str, b_first: str) -> bool:
    """
    Very conservative nickname-ish signal:
    one is a prefix of the other and length differs (ken < kenny, tom < thomas).
    This is NOT sufficient alone; used as small additive evidence.
    """
    if not a_first or not b_first:
        return False
    if a_first == b_first:
        return True
    short, long = (a_first, b_first) if len(a_first) < len(b_first) else (b_first, a_first)
    if len(short) <= 2:
        return False
    return long.startswith(short)


def score_pair(pa: NameParts, pb: NameParts, cooccur: bool) -> Tuple[float, List[str]]:
    reasons = []
    if pa.last != pb.last or not pa.last:
        return 0.0, reasons

    # Strong negative evidence
    if cooccur:
        return 0.0, ["cooccurs_same_event_division"]

    score = 0.0

    # Same last name is baseline
    score += 0.40
    reasons.append("same_last_name")

    # Same first initial
    if pa.first_initial and pa.first_initial == pb.first_initial:
        score += 0.25
        reasons.append("same_first_initial")

    # One is initial form (K Shults) and other is full first name
    if is_initial_form(pa) ^ is_initial_form(pb):
        score += 0.20
        reasons.append("initial_vs_full")

    # Nickname-ish prefix
    if nickname_hint(pa.first, pb.first):
        score += 0.10
        reasons.append("first_name_prefix")

    # Exact first token match (after cleaning)
    if pa.first and pa.first == pb.first:
        score += 0.10
        reasons.append("same_first_token")

    # Cap
    if score > 0.99:
        score = 0.99
    return score, reasons


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--placements_flat", default="out/Placements_Flat.csv")
    ap.add_argument("--out_csv", default="out/person_alias_suggestions_questionable.csv")
    ap.add_argument("--min_appearances", type=int, default=20,
                    help="Only consider names that appear at least this many times.")
    ap.add_argument("--min_score", type=float, default=0.75,
                    help="Minimum confidence score to emit.")
    ap.add_argument("--max_pairs_per_lastname", type=int, default=40,
                    help="Limit pair explosion per last name.")
    args = ap.parse_args()

    VERIFIED_ALIASES = load_verified_aliases("overrides/person_aliases.csv")

    pf_path = Path(args.placements_flat)
    out_path = Path(args.out_csv)
    if not pf_path.exists():
        print(f"ERROR: missing {pf_path} (run 02p5 first)", file=sys.stderr)
        return 2

    pf = pd.read_csv(pf_path)
    counts = build_name_counts(pf)
    if len(counts) == 0:
        warn("No names found; nothing to do.")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_path, index=False)
        return 0

    # Filter candidates: frequent + non-polluted
    counts["name"] = counts["name"].astype(str)
    counts["polluted"] = counts["name"].map(is_polluted)
    cand = counts[(counts["appearances"] >= args.min_appearances) & (~counts["polluted"])].copy()

    if len(cand) == 0:
        info("No candidates after filtering.")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        cand.to_csv(out_path, index=False)
        return 0

    # Build co-occurrence evidence (can be expensive; but dataset size is manageable)
    info("Building co-occurrence evidence (event_id + division_canon)...")
    co_pairs = build_cooccur_pairs(pf)

    # Index by last name
    by_last: Dict[str, List[NameParts]] = defaultdict(list)
    raw_to_counts = {r["name"]: (int(r["appearances"]), int(r["as_player1"]), int(r["as_player2"])) for _, r in cand.iterrows()}

    for nm in cand["name"].tolist():
        np = split_name(nm)
        if np.last:
            by_last[np.last].append(np)

    out_rows = []
    clusters_seen = set()

    for last, lst in by_last.items():
        if len(lst) < 2:
            continue
        # limit explosion
        lst = sorted(lst, key=lambda x: raw_to_counts.get(x.raw, (0,0,0))[0], reverse=True)
        lst = lst[: max(5, min(len(lst), args.max_pairs_per_lastname))]

        for i in range(len(lst)):
            for j in range(i+1, len(lst)):
                a, b = lst[i], lst[j]
                key_pair = tuple(sorted([a.raw, b.raw]))
                co = key_pair in co_pairs
                score, reasons = score_pair(a, b, cooccur=co)
                if score < args.min_score:
                    continue

                # Cluster key = last name + first initial (deterministic, no raw canon text)
                a_cnt = raw_to_counts.get(a.raw, (0,0,0))[0]
                b_cnt = raw_to_counts.get(b.raw, (0,0,0))[0]
                canon = a.raw if a_cnt >= b_cnt else b.raw

                cluster_key = f"{last}|{a.first_initial}"
                proposed_pid = proposed_person_id_from_key(cluster_key)

                # emit BOTH aliases as rows (so promotion can append both)
                for nm in [a.raw, b.raw]:
                    if (proposed_pid, nm) in clusters_seen:
                        continue
                    app, p1, p2 = raw_to_counts.get(nm, (0,0,0))
                    out_rows.append({
                        "cluster_key": cluster_key,
                        "proposed_person_id": proposed_pid,
                        "alias": nm,
                        "person_canon": canon,
                        "confidence": round(score, 3),
                        "reason": ";".join(reasons),
                        "appearances": app,
                        "as_player1": p1,
                        "as_player2": p2,
                        "human_decision": "",  # YOU decide
                        "notes": "",
                    })
                    clusters_seen.add((proposed_pid, nm))

    out_df = pd.DataFrame(out_rows, columns=[
        "cluster_key","proposed_person_id","alias","person_canon","confidence","reason",
        "appearances","as_player1","as_player2","human_decision","notes"
    ])

    before = len(out_df)
    out_df = out_df[
        ~out_df["alias"]
        .fillna("")
        .astype(str)
        .str.strip()
        .isin(VERIFIED_ALIASES)
    ]
    after = len(out_df)
    print(f"[alias_suggestions_questionable] "
          f"filtered {before - after} already-verified aliases; "
          f"{after} remaining")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.sort_values(by=["confidence","appearances","alias"], ascending=[False, False, True], inplace=True)
    out_df.to_csv(out_path, index=False)
    info(f"Wrote {out_path} ({len(out_df)} rows).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
