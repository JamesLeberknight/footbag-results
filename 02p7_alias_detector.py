#!/usr/bin/env python3
"""
02p7_alias_detector.py

Robust alias detector:
- source aliases come from Placements_Flat observed names
- targets come ONLY from Persons_Truth canonical names
- optionally excludes already-approved aliases from person_aliases.csv
- adds context: active years, counts, shared events, sample events

Outputs:
- out/alias_candidates.csv
- out/alias_candidates_strong.csv
"""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path.home() / "projects" / "FOOTBAG_DATA"
PF_PATH = ROOT / "out" / "Placements_Flat.csv"
PT_PATH = ROOT / "out" / "Persons_Truth.csv"
KNOWN_ALIAS_PATH = ROOT / "out" / "person_aliases.csv"

OUT_ALL = ROOT / "out" / "alias_candidates.csv"
OUT_STRONG = ROOT / "out" / "alias_candidates_strong.csv"

IGNORE_NAMES = {
    "",
    "__NON_PERSON__",
}

STOPWORDS = {
    "aka", "the", "and", "of", "de", "der", "van", "von", "da", "di", "la", "le",
    "el", "og", "aus", "ka", "ma"
}

KNOWN_LOCATION_PREFIXES = {
    "Chicago", "Urbana", "Charleston", "Milan", "Selma",
    "Vancouver", "Malta", "Sherman", "Winfield", "Pittsburgh",
    "Wichita", "Austin", "Hebron", "McPherson",
}

JUNK_PATTERNS = [
    re.compile(r"^\s*nd=\s*", re.I),
    re.compile(r"-prizes\b", re.I),
    re.compile(r"�"),
]

# Optional exact cleanup before alias detection
ALIAS_SOURCE_FIXES = {
    "david Butcher": "David Butcher",
    "Alexis Dechenes": "Alexis Deschenes",
    "Alexis Deschene": "Alexis Deschenes",
}


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())


def clean_observed_name(name: str) -> str:
    """
    Conservative cleanup for observed source names only.
    This is NOT a canonicalizer; it only removes obvious junk
    that would pollute alias detection.
    """
    if not isinstance(name, str):
        return ""

    s = name.strip()

    # known exact source fixes
    s = ALIAS_SOURCE_FIXES.get(s, s)

    # remove obvious junk
    s = re.sub(r"^\s*nd=\s*", "", s, flags=re.I)
    s = re.sub(r"-prizes\b", "", s, flags=re.I)
    s = s.replace("�", "")
    s = re.sub(r"\s+", " ", s).strip()

    # strip known location prefix if it clearly looks like location + name
    parts = s.split()
    if len(parts) >= 3 and parts[0] in KNOWN_LOCATION_PREFIXES:
        s = " ".join(parts[1:])

    return norm_space(s)


def canon_name(s: str) -> str:
    s = norm_space(s)
    s = s.replace("’", "'").replace("`", "'")
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_tokens(s: str) -> list[str]:
    toks = [t for t in re.split(r"\s+", canon_name(s)) if t]
    return [t for t in toks if t.lower() not in STOPWORDS]


def token_set(s: str) -> set[str]:
    return {t.lower() for t in name_tokens(s)}


def suffix_match(alias: str, canonical: str) -> bool:
    a = [t.lower() for t in name_tokens(alias)]
    b = [t.lower() for t in name_tokens(canonical)]
    if not a or not b or len(a) >= len(b):
        return False
    return b[-len(a):] == a


def prefix_match(alias: str, canonical: str) -> bool:
    a = [t.lower() for t in name_tokens(alias)]
    b = [t.lower() for t in name_tokens(canonical)]
    if not a or not b or len(a) >= len(b):
        return False
    return b[:len(a)] == a


def contained_match(alias: str, canonical: str) -> bool:
    a = token_set(alias)
    b = token_set(canonical)
    if not a or not b:
        return False
    if a == b:
        return False
    return a.issubset(b)


def initial_variant(alias: str, canonical: str) -> bool:
    """
    J Walters -> John Walters
    A Noller -> Adam Noller
    """
    a = name_tokens(alias)
    b = name_tokens(canonical)
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


def alias_is_weaker(alias: str, canonical: str) -> bool:
    """
    Alias should be shorter/weaker than canonical, or an initial form.
    """
    a = name_tokens(alias)
    b = name_tokens(canonical)
    if not a or not b:
        return False

    if initial_variant(alias, canonical):
        return True
    if len(a) < len(b):
        return True
    if len(" ".join(a)) < len(" ".join(b)):
        return True
    return False


def looks_junky(name: str) -> bool:
    s = str(name).strip()
    if not s:
        return True
    if s in IGNORE_NAMES:
        return True
    if len(name_tokens(s)) == 0:
        return True
    for pat in JUNK_PATTERNS:
        if pat.search(s):
            return True
    return False


def build_context(df: pd.DataFrame, name_col: str) -> tuple[dict, dict, dict]:
    """
    Returns:
    - name_to_events
    - name_to_years
    - name_freq
    """
    tmp = df[[name_col, "event_id", "year"]].dropna().copy()
    tmp["_name"] = tmp[name_col].astype(str).map(clean_observed_name).map(norm_space)
    tmp = tmp[(tmp["_name"] != "__NON_PERSON__") & (tmp["_name"] != "")]

    name_to_events = tmp.groupby("_name")["event_id"].agg(lambda s: set(map(str, s))).to_dict()
    name_to_years = tmp.groupby("_name")["year"].agg(lambda s: sorted(set(map(str, s)))).to_dict()
    name_freq = tmp["_name"].value_counts().to_dict()

    return name_to_events, name_to_years, name_freq


def summarize_years(years: list[str], max_show: int = 6) -> str:
    years = sorted(set(y for y in years if y and y != "nan"))
    if not years:
        return ""
    if len(years) <= max_show:
        return ",".join(years)
    return f"{years[0]}..{years[-1]} ({len(years)} yrs)"


def summarize_events(events: set[str], max_show: int = 5) -> str:
    evs = sorted(events)
    if not evs:
        return ""
    return ",".join(evs[:max_show])


def load_known_aliases() -> dict[str, str]:
    if not KNOWN_ALIAS_PATH.exists():
        return {}
    df = pd.read_csv(KNOWN_ALIAS_PATH).fillna("")
    if not {"alias", "canonical"}.issubset(df.columns):
        return {}
    out = {}
    for _, row in df.iterrows():
        alias = norm_space(str(row["alias"]))
        canonical = norm_space(str(row["canonical"]))
        if alias and canonical:
            out[alias] = canonical
    return out


def score_pair(alias: str, canonical: str, alias_freq: int, canon_freq: int, overlap: int) -> tuple[int, str]:
    score = 0
    reasons = []

    if suffix_match(alias, canonical):
        score += 5
        reasons.append("suffix_match")

    if prefix_match(alias, canonical):
        score += 3
        reasons.append("prefix_match")

    if contained_match(alias, canonical):
        score += 2
        reasons.append("token_subset")

    if initial_variant(alias, canonical):
        score += 4
        reasons.append("initial_variant")

    if overlap > 0:
        score += min(4, overlap)
        reasons.append(f"shared_events={overlap}")

    if alias_freq <= 3:
        score += 1
        reasons.append("rare_alias")

    if canon_freq >= alias_freq:
        score += 1
        reasons.append("canon_more_common")

    return score, ",".join(reasons)


def main():
    if not PF_PATH.exists():
        raise FileNotFoundError(PF_PATH)
    if not PT_PATH.exists():
        raise FileNotFoundError(PT_PATH)

    pf = pd.read_csv(PF_PATH)
    pt = pd.read_csv(PT_PATH)

    pf_name_col = None
    for col in ["person_canon", "display_name", "person_name"]:
        if col in pf.columns:
            pf_name_col = col
            break
    if pf_name_col is None:
        raise ValueError("Could not find name column in Placements_Flat.csv")

    if "person_canon" not in pt.columns:
        raise ValueError("Persons_Truth.csv must contain person_canon")

    known_aliases = load_known_aliases()

    # Observed names
    observed = (
        pf[pf_name_col]
        .dropna()
        .astype(str)
        .map(clean_observed_name)
        .map(norm_space)
    )
    observed = observed[~observed.isin(IGNORE_NAMES)]
    observed = observed[observed != "__NON_PERSON__"]

    # Canonical targets only from Persons_Truth
    canonical_names = (
        pt["person_canon"]
        .dropna()
        .astype(str)
        .map(norm_space)
        .unique()
        .tolist()
    )
    canonical_set = set(canonical_names)

    # Alias sources are observed names that are not already canonical
    alias_sources = sorted(set(observed) - canonical_set)

    # Remove already approved aliases from suggestion list
    alias_sources = [a for a in alias_sources if a not in known_aliases]

    # Build context maps from observed placements
    name_to_events, name_to_years, name_freq = build_context(pf, pf_name_col)

    # Token index over canonical names only
    canon_token_index: dict[str, set[str]] = defaultdict(set)
    for cname in canonical_names:
        if looks_junky(cname):
            continue
        for tok in token_set(cname):
            canon_token_index[tok].add(cname)

    rows = []

    for alias in alias_sources:
        if looks_junky(alias):
            continue

        alias_tokens = token_set(alias)
        if not alias_tokens:
            continue

        candidate_pool = set()
        for tok in alias_tokens:
            candidate_pool.update(canon_token_index[tok])

        for canonical in sorted(candidate_pool):
            if alias == canonical:
                continue
            if looks_junky(canonical):
                continue
            if not alias_is_weaker(alias, canonical):
                continue

            overlap_events = name_to_events.get(alias, set()) & name_to_events.get(canonical, set())
            overlap = len(overlap_events)

            score, reasons = score_pair(
                alias=alias,
                canonical=canonical,
                alias_freq=name_freq.get(alias, 0),
                canon_freq=name_freq.get(canonical, 0),
                overlap=overlap,
            )

            if score < 4:
                continue

            rows.append({
                "alias": alias,
                "canonical": canonical,
                "alias_count": name_freq.get(alias, 0),
                "canonical_count_in_pf": name_freq.get(canonical, 0),
                "alias_years": summarize_years(name_to_years.get(alias, [])),
                "canonical_years": summarize_years(name_to_years.get(canonical, [])),
                "shared_events": overlap,
                "shared_event_ids_sample": summarize_events(overlap_events, max_show=5),
                "alias_event_ids_sample": summarize_events(name_to_events.get(alias, set()), max_show=5),
                "canonical_event_ids_sample": summarize_events(name_to_events.get(canonical, set()), max_show=5),
                "score": score,
                "reasons": reasons,
            })

    out = pd.DataFrame(rows)
    if out.empty:
        print("No alias candidates found.")
        out.to_csv(OUT_ALL, index=False)
        out.to_csv(OUT_STRONG, index=False)
        return

    out = out.sort_values(
        ["score", "shared_events", "alias_count", "canonical"],
        ascending=[False, False, True, True],
    )

    out.to_csv(OUT_ALL, index=False)

    # Strong suggestions only; keep best target per alias
    strong = out[
        (out["score"] >= 6)
        | (
            out["reasons"].str.contains("suffix_match", na=False)
            & out["reasons"].str.contains("shared_events=", na=False)
        )
        | out["reasons"].str.contains("initial_variant", na=False)
    ].copy()

    strong = (
        strong.sort_values(["alias", "score", "shared_events"], ascending=[True, False, False])
              .drop_duplicates(subset=["alias"], keep="first")
    )

    out.to_csv(OUT_ALL, index=False)
    strong.to_csv(OUT_STRONG, index=False)

    print(f"Wrote: {OUT_ALL} ({len(out)} rows)")
    print(f"Wrote: {OUT_STRONG} ({len(strong)} rows)")
    print("\nTop strong candidates:")
    print(
        strong[
            [
                "alias", "canonical", "alias_count", "canonical_count_in_pf",
                "alias_years", "canonical_years", "shared_events",
                "shared_event_ids_sample", "score", "reasons"
            ]
        ].head(60).to_string(index=False)
    )


if __name__ == "__main__":
    main()
