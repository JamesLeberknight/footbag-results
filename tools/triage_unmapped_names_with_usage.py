#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path
import pandas as pd


# -----------------------------
# Config
# -----------------------------
STAGE2_PLAYERS = Path("out/stage2_players.csv")
ALIASES_CSV    = Path("overrides/person_aliases.csv")
PLACEMENTS     = Path("out/Placements_ByPerson.csv")
OUT_CSV        = Path("out/unmapped_name_triage.csv")


# -----------------------------
# Heuristic dictionaries
# -----------------------------
META_TOKENS = [
    "RESULT", "RESULTS", "FINAL", "FINALS", "ROUND", "ROUNDS",
    "PARTNER", "PARTNERS", "EVENT", "EVENTS", "DIVISION", "DIVISIONS",
    "PLACE", "PLACINGS", "PLACEMENT", "SCORE", "SCORES", "RANK", "RANKING",
    "POOL", "POOLS", "SEMI", "SEMIS", "QUAL", "QUALIFIER",
    "NOTE", "NOTES", "UNKNOWN", "TBD",
    "SATURDAY", "SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY",
]
META_RE = re.compile(r"\b(" + "|".join(re.escape(t) for t in META_TOKENS) + r")\b", re.IGNORECASE)

CLUB_TOKENS = [
    "FC", "CLUB", "TEAM", "ASSOCIATION", "ASSOC",
    "FOOTBAG", "FOOTSTAR", "FREESTYLE", "NET", "CIRCLE",
]
CLUB_RE = re.compile(r"\b(" + "|".join(re.escape(t) for t in CLUB_TOKENS) + r")\b", re.IGNORECASE)

LOC_RE = re.compile(r"\b(USA|US|CANADA|UK|GER|CZE|POL|BC|AB|ON)\b", re.IGNORECASE)

MOJIBAKE_RE = re.compile(r"[�]|Ã|Â|Ð|Þ|ð|þ", re.UNICODE)
QMARK_IN_NAME_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]\?[A-Za-zÀ-ÖØ-öø-ÿ]")

MULTI_SEP_RE = re.compile(r"\s+(&|and|/)\s+|,\s*", re.IGNORECASE)
PERSON_TOKEN_RE = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ'’-]+$")


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def triage_one(name: str) -> tuple[str, str]:
    n = norm(name)

    if n == "" or n.lower() in {"?", "-", "unknown", "n/a", "na", "tbd"}:
        return ("FRAGMENT_OR_META", "EMPTY_OR_PLACEHOLDER")

    if META_RE.search(n) or len(n.split()) >= 6:
        return ("FRAGMENT_OR_META", "HEADING_OR_NARRATIVE")

    if CLUB_RE.search(n):
        return ("NON_PERSON_ENTITY", "CLUB_OR_ORG")

    if n.endswith(")") and "(" not in n:
        return ("FRAGMENT_OR_META", "DANGLING_PAREN_TAIL")

    if LOC_RE.search(n) and (")" in n or "-" in n):
        return ("FRAGMENT_OR_META", "LOCATION_FRAGMENT")

    if QMARK_IN_NAME_RE.search(n) or MOJIBAKE_RE.search(n):
        return ("ENCODING_CORRUPT_PERSON", "ENCODING_CORRUPTION")

    if MULTI_SEP_RE.search(n):
        return ("FRAGMENT_OR_META", "MULTI_PERSON_BLOB")

    toks = n.split()
    if 1 <= len(toks) <= 4 and not n.isupper():
        ok_ratio = sum(1 for t in toks if PERSON_TOKEN_RE.match(t)) / len(toks)
        if ok_ratio >= 0.75:
            return ("LIKELY_PERSON", "PERSONISH_SHAPE")

    return ("AMBIGUOUS", "NO_RULE_MATCH")


def main() -> None:
    for p in (STAGE2_PLAYERS, ALIASES_CSV, PLACEMENTS):
        if not p.exists():
            raise SystemExit(f"Missing required file: {p}")

    # --- load inputs
    players = pd.read_csv(STAGE2_PLAYERS, dtype=str).fillna("")
    aliases = pd.read_csv(ALIASES_CSV, dtype=str).fillna("")
    plc = pd.read_csv(PLACEMENTS, dtype=str).fillna("")

    # --- collect unmapped names from stage2
    stage2_names = (
        players["player_name"]
        .map(norm)
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
    )

    alias_set = set(aliases["alias"].map(norm))
    unmapped = [n for n in stage2_names.tolist() if n not in alias_set]

    # --- build usage stats from Placements_ByPerson
    # stack player1 and player2 into one column
    names_long = pd.concat(
        [
            plc[["player1_name", "year", "event_id", "division_canon"]]
                .rename(columns={"player1_name": "name"}),
            plc[["player2_name", "year", "event_id", "division_canon"]]
                .rename(columns={"player2_name": "name"}),
        ],
        ignore_index=True,
    )

    names_long["name"] = names_long["name"].map(norm)
    names_long = names_long[names_long["name"] != ""]

    usage = (
        names_long[names_long["name"].isin(unmapped)]
        .groupby("name", dropna=False)
        .agg(
            usage_count_total=("event_id", "count"),
            years_seen=("year", pd.Series.nunique),
            first_year=("year", "min"),
            last_year=("year", "max"),
        )
        .reset_index()
    )

    # sample examples for audit
    examples = (
        names_long[names_long["name"].isin(unmapped)]
        .sort_values(["name", "year"])
        .groupby("name")
        .head(3)
        .groupby("name")
        .apply(
            lambda df: "; ".join(
                f"{r.year}/{r.event_id}/{r.division_canon}"
                for r in df.itertuples(index=False)
            )
        )
        .rename("example_events")
        .reset_index()
    )

    # --- triage
    rows = []
    for n in unmapped:
        triage_class, reason = triage_one(n)
        rows.append({
            "name_unmapped": n,
            "triage_class": triage_class,
            "reason_code": reason,
        })

    triage = pd.DataFrame(rows)

    # --- merge stats
    out = (
        triage
        .merge(usage, how="left", left_on="name_unmapped", right_on="name")
        .merge(examples, how="left", left_on="name_unmapped", right_on="name")
        .drop(columns=[c for c in ["name_x", "name_y"] if c in triage.columns])
        .fillna({
            "usage_count_total": 0,
            "years_seen": 0,
        })
        .sort_values(
            ["triage_class", "usage_count_total", "years_seen"],
            ascending=[True, False, False],
            kind="stable",
        )
    )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"Wrote {len(out):,} rows → {OUT_CSV}")


if __name__ == "__main__":
    main()
