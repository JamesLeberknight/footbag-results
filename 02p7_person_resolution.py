#PERSON_ALIAS_FIXES = {
#    "Dubuis": "Christian Dubuis",
#    "Fritsch": "Theodore Fritsch",
#    "Tyrpekl": "Tomas Tyrpekl",
#    "Donnat": "Arnaud Donnat",
#    "Tellenbach": "Grischa Tellenbach",
#}
#df["person_canon"] = df["person_canon"].replace(PERSON_ALIAS_FIXES)
#!/usr/bin/env python3
"""
02p7_alias_detector.py

Detect likely alias / partial-name variants in Footbag person data.

Outputs:
- out/alias_candidates.csv
- out/alias_candidates_strong.csv

Heuristics:
1. suffix match:
   "Dubuis" -> "Christian Dubuis"
   "Rex Stoler" -> "Mike Rex Stoler"

2. same event overlap:
   if two different names appear in the same event and one is a suffix/prefix
   variant of the other, rank higher

3. rarity boost:
   short/partial names that appear rarely but match a fuller name get boosted

4. exact token containment:
   one name's tokens are contained in the other's tokens

No automatic merges are performed.
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path.home() / "projects" / "FOOTBAG_DATA"
PF_PATH = ROOT / "out" / "Placements_Flat.csv"
PT_PATH = ROOT / "out" / "Persons_Truth.csv"

OUT_ALL = ROOT / "out" / "alias_candidates.csv"
OUT_STRONG = ROOT / "out" / "alias_candidates_strong.csv"


STOPWORDS = {
    "aka", "the", "and", "of", "de", "der", "van", "von", "da", "di", "la", "le",
    "el", "og", "aus", "ka", "ma"
}

IGNORE_EXACT = {
    "__NON_PERSON__",
    "",
}


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())


def canon_name(s: str) -> str:
    s = norm_space(s)
    s = s.replace("’", "'")
    s = s.replace("`", "'")
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_tokens(s: str) -> list[str]:
    s = canon_name(s)
    toks = [t for t in re.split(r"\s+", s) if t]
    toks = [t for t in toks if t.lower() not in STOPWORDS]
    return toks


def token_set(s: str) -> set[str]:
    return {t.lower() for t in name_tokens(s)}


def suffix_match(shorter: str, longer: str) -> bool:
    a = [t.lower() for t in name_tokens(shorter)]
    b = [t.lower() for t in name_tokens(longer)]
    if not a or not b or len(a) >= len(b):
        return False
    return b[-len(a):] == a


def prefix_match(shorter: str, longer: str) -> bool:
    a = [t.lower() for t in name_tokens(shorter)]
    b = [t.lower() for t in name_tokens(longer)]
    if not a or not b or len(a) >= len(b):
        return False
    return b[:len(a)] == a


def contained_match(shorter: str, longer: str) -> bool:
    a = token_set(shorter)
    b = token_set(longer)
    if not a or not b or len(a) >= len(b):
        return False
    return a.issubset(b)


def looks_like_initial_variant(shorter: str, longer: str) -> bool:
    """
    e.g. J Walters -> John Walters
         A Noller -> Adam Noller
    Conservative: same last token, shorter has an initial token.
    """
    a = name_tokens(shorter)
    b = name_tokens(longer)
    if len(a) != len(b):
        return False
    if len(a) < 2:
        return False
    if a[-1].lower() != b[-1].lower():
        return False

    ok = True
    for x, y in zip(a[:-1], b[:-1]):
        if len(x) == 1 and y.lower().startswith(x.lower()):
            continue
        if x.lower() == y.lower():
            continue
        ok = False
    return ok


def score_pair(a: str, b: str, freq: dict[str, int], event_overlap: int) -> tuple[int, str]:
    """
    Returns (score, reason_summary)
    """
    short, long = (a, b) if len(name_tokens(a)) <= len(name_tokens(b)) else (b, a)

    reasons = []
    score = 0

    if suffix_match(short, long):
        score += 5
        reasons.append("suffix_match")

    if prefix_match(short, long):
        score += 3
        reasons.append("prefix_match")

    if contained_match(short, long):
        score += 2
        reasons.append("token_subset")

    if looks_like_initial_variant(short, long):
        score += 4
        reasons.append("initial_variant")

    if event_overlap > 0:
        score += min(3, event_overlap)
        reasons.append(f"same_event_overlap={event_overlap}")

    # boost rare short names that map to common fuller names
    short_freq = freq.get(short, 0)
    long_freq = freq.get(long, 0)
    if short_freq <= 3 and long_freq >= short_freq:
        score += 1
        reasons.append("rare_shorter_name")

    # penalize very weak matches
    if len(name_tokens(short)) == 1 and len(name_tokens(long)) == 1:
        score -= 3

    return score, ",".join(reasons)


def main():
    if not PF_PATH.exists():
        raise FileNotFoundError(PF_PATH)
    if not PT_PATH.exists():
        raise FileNotFoundError(PT_PATH)

    pf = pd.read_csv(PF_PATH)
    pt = pd.read_csv(PT_PATH)

    # Choose best available name column for PF
    pf_name_col = None
    for col in ["person_canon", "display_name", "person_name"]:
        if col in pf.columns:
            pf_name_col = col
            break
    if pf_name_col is None:
        raise ValueError("Could not find person name column in Placements_Flat.csv")

    names = (
        pf[pf_name_col]
        .dropna()
        .astype(str)
        .map(norm_space)
    )
    names = names[~names.isin(IGNORE_EXACT)]
    names = names[names != "__NON_PERSON__"]

    freq = names.value_counts().to_dict()
    unique_names = sorted(freq.keys())

    # Event overlap map: for each name, which event_ids it appears in
    if "event_id" not in pf.columns:
        raise ValueError("Placements_Flat.csv missing event_id")
    name_to_events = (
        pf[[pf_name_col, "event_id"]]
        .dropna()
        .assign(_name=lambda d: d[pf_name_col].astype(str).map(norm_space))
        .query("_name != '__NON_PERSON__'")
        .groupby("_name")["event_id"]
        .agg(lambda s: set(map(str, s)))
        .to_dict()
    )

    # Candidate generation: only compare names sharing at least one token
    token_index: dict[str, set[str]] = defaultdict(set)
    for name in unique_names:
        for tok in token_set(name):
            token_index[tok].add(name)

    seen_pairs = set()
    rows = []

    for name in unique_names:
        candidate_pool = set()
        for tok in token_set(name):
            candidate_pool.update(token_index[tok])

        for other in candidate_pool:
            if other == name:
                continue

            key = tuple(sorted([name, other]))
            if key in seen_pairs:
                continue
            seen_pairs.add(key)

            event_overlap = len(name_to_events.get(name, set()) & name_to_events.get(other, set()))
            score, reasons = score_pair(name, other, freq, event_overlap)

            if score < 4:
                continue

            short, long = (name, other) if len(name_tokens(name)) <= len(name_tokens(other)) else (other, name)

            rows.append({
                "candidate_short": short,
                "candidate_long": long,
                "short_count": freq.get(short, 0),
                "long_count": freq.get(long, 0),
                "shared_events": event_overlap,
                "score": score,
                "reasons": reasons,
            })

    out = pd.DataFrame(rows).sort_values(
        ["score", "shared_events", "short_count", "long_count", "candidate_short", "candidate_long"],
        ascending=[False, False, True, False, True, True],
    )

    if out.empty:
        print("No alias candidates found.")
        out.to_csv(OUT_ALL, index=False)
        out.to_csv(OUT_STRONG, index=False)
        return

    out.to_csv(OUT_ALL, index=False)

    strong = out[
        (out["score"] >= 6) |
        (out["reasons"].str.contains("suffix_match") & out["reasons"].str.contains("same_event_overlap"))
    ].copy()

    strong.to_csv(OUT_STRONG, index=False)

    print(f"Wrote: {OUT_ALL} ({len(out)} rows)")
    print(f"Wrote: {OUT_STRONG} ({len(strong)} rows)")
    print("\nTop candidates:")
    print(strong.head(40).to_string(index=False))


if __name__ == "__main__":
    main()
