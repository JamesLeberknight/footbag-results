#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path
import pandas as pd

CSV_PATH = Path("out/Placements_Flat.csv")
OUT_HITS = Path("out/qc_20rule_hits.csv")
OUT_SUMMARY = Path("out/qc_20rule_summary.csv")


# -----------------------------
# regex vocabulary
# -----------------------------
DAYS_RE = re.compile(
    r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
    flags=re.I,
)

MONTHS_RE = re.compile(
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december|"
    r"jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\b",
    flags=re.I,
)

COUNTRIES_RE = re.compile(
    r"\b(usa|united states|canada|germany|france|finland|japan|australia|poland|czech|"
    r"czech republic|hungary|italy|switzerland|netherlands|slovakia|sweden|denmark|"
    r"norway|uk|united kingdom|england|scotland|wales)\b",
    flags=re.I,
)

DIVISION_TERMS_RE = re.compile(
    r"\b(open|intermediate|novice|women|women's|womens|mixed|singles|doubles|routine|"
    r"freestyle|net|golf|shred|sick\s*3|consecutive|consecutives|request|circle)\b",
    flags=re.I,
)

ROUND_TERMS_RE = re.compile(
    r"\b(pool|pools|prelim|prelims|preliminary|semi|semis|semi-final|semifinal|"
    r"quarterfinal|quarter-final|final|finals)\b",
    flags=re.I,
)

DATEISH_RE = re.compile(
    r"(\b\d{4}\b|\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4}\b)",
    flags=re.I,
)

HTML_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"(https?://|www\.)", flags=re.I)
EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+", flags=re.I)
MULTISPACE_RE = re.compile(r"\s{2,}")
LEADING_TRAILING_PUNCT_RE = re.compile(r"^[^\w]+|[^\w]+$")
ALL_CAPS_WORD_RE = re.compile(r"^[A-Z][A-Z\s'\-]{4,}$")
PLACE_LIKE_RE = re.compile(r"^\s*(t-?\d+|\d+)\s*$", flags=re.I)


def series_contains(s: pd.Series, pattern: re.Pattern) -> pd.Series:
    return s.fillna("").astype(str).str.contains(pattern, regex=True)


def clean_str(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str)


def add_hits(
    hits_list: list[pd.DataFrame],
    df: pd.DataFrame,
    mask: pd.Series,
    rule_name: str,
    detail: str,
    key_cols: list[str],
) -> None:
    sub = df.loc[mask].copy()
    if sub.empty:
        return
    sub.insert(0, "qc_rule", rule_name)
    sub.insert(1, "qc_detail", detail)
    keep = ["qc_rule", "qc_detail"] + [c for c in key_cols if c in sub.columns]
    for col in keep:
        if col not in sub.columns:
            sub[col] = ""
    hits_list.append(sub[keep])


def first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing file: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, low_memory=False)

    # Common columns used if present
    person_col = first_existing(df, ["person_canon", "player_name", "person_name", "name", "person"])
    division_col = first_existing(df, ["division_canon", "division_raw", "division"])
    event_name_col = first_existing(df, ["event_name", "event"])
    city_col = first_existing(df, ["city"])
    country_col = first_existing(df, ["country"])
    event_id_col = first_existing(df, ["event_id"])
    year_col = first_existing(df, ["year"])
    place_col = first_existing(df, ["place"])
    norm_col = first_existing(df, ["norm", "norm_name", "person_norm"])

    key_cols = [
        c for c in [
            event_id_col, year_col, event_name_col, division_col, place_col,
            person_col, city_col, country_col, norm_col
        ] if c is not None
    ]

    hits: list[pd.DataFrame] = []

    # -----------------------------
    # Rule 1: weekday contamination anywhere
    # -----------------------------
    mask_any_day = df.astype(str).apply(lambda col: col.str.contains(DAYS_RE, regex=True)).any(axis=1)
    add_hits(hits, df, mask_any_day, "R01_DAYNAME_ANYWHERE", "weekday appears somewhere in row", key_cols)

    # Rule 2: weekday in player field
    if person_col:
        add_hits(
            hits, df, series_contains(df[person_col], DAYS_RE),
            "R02_DAYNAME_IN_PERSON", "weekday in player/person field", key_cols
        )

    # Rule 3: weekday in division field
    if division_col:
        add_hits(
            hits, df, series_contains(df[division_col], DAYS_RE),
            "R03_DAYNAME_IN_DIVISION", "weekday in division field", key_cols
        )

    # Rule 4: month contamination in player field
    if person_col:
        add_hits(
            hits, df, series_contains(df[person_col], MONTHS_RE),
            "R04_MONTH_IN_PERSON", "month name in player/person field", key_cols
        )

    # Rule 5: date-like contamination in player field
    if person_col:
        add_hits(
            hits, df, series_contains(df[person_col], DATEISH_RE),
            "R05_DATEISH_IN_PERSON", "date-like token in player/person field", key_cols
        )

    # Rule 6: country contamination in player field
    if person_col:
        add_hits(
            hits, df, series_contains(df[person_col], COUNTRIES_RE),
            "R06_COUNTRY_IN_PERSON", "country token in player/person field", key_cols
        )

    # Rule 7: division terms inside player field
    if person_col:
        add_hits(
            hits, df, series_contains(df[person_col], DIVISION_TERMS_RE),
            "R07_DIVISION_TERM_IN_PERSON", "division keyword leaked into player/person field", key_cols
        )

    # Rule 8: round terms inside player field
    if person_col:
        add_hits(
            hits, df, series_contains(df[person_col], ROUND_TERMS_RE),
            "R08_ROUND_TERM_IN_PERSON", "round keyword leaked into player/person field", key_cols
        )

    # Rule 9: HTML in any structured field
    html_fields = [c for c in [person_col, division_col, event_name_col, city_col, country_col] if c]
    if html_fields:
        mask = pd.Series(False, index=df.index)
        for c in html_fields:
            mask |= series_contains(df[c], HTML_RE)
        add_hits(hits, df, mask, "R09_HTML_TAGS", "HTML tag text found in structured field", key_cols)

    # Rule 10: URL in any structured field
    if html_fields:
        mask = pd.Series(False, index=df.index)
        for c in html_fields:
            mask |= series_contains(df[c], URL_RE)
        add_hits(hits, df, mask, "R10_URL_IN_STRUCTURED_FIELD", "URL found in structured field", key_cols)

    # Rule 11: email in any structured field
    if html_fields:
        mask = pd.Series(False, index=df.index)
        for c in html_fields:
            mask |= series_contains(df[c], EMAIL_RE)
        add_hits(hits, df, mask, "R11_EMAIL_IN_STRUCTURED_FIELD", "email address found in structured field", key_cols)

    # Rule 12: digits in player field
    if person_col:
        mask = clean_str(df[person_col]).str.contains(r"\d", regex=True)
        add_hits(hits, df, mask, "R12_DIGITS_IN_PERSON", "digits in player/person field", key_cols)

    # Rule 13: player field equals bare place-like token
    if person_col:
        mask = clean_str(df[person_col]).str.match(PLACE_LIKE_RE)
        add_hits(hits, df, mask, "R13_PERSON_IS_PLACE_TOKEN", "player/person looks like a place token", key_cols)

    # Rule 14: empty or placeholder-like player field
    if person_col:
        mask = clean_str(df[person_col]).str.strip().str.lower().isin({
            "", "nan", "none", "unknown", "_non_person", "n/a", "na", "tbd"
        })
        add_hits(hits, df, mask, "R14_EMPTY_OR_PLACEHOLDER_PERSON", "empty or placeholder player/person", key_cols)

    # Rule 15: excessive whitespace in structured fields
    if html_fields:
        mask = pd.Series(False, index=df.index)
        for c in html_fields:
            mask |= clean_str(df[c]).str.contains(MULTISPACE_RE, regex=True)
        add_hits(hits, df, mask, "R15_EXCESSIVE_WHITESPACE", "double+ spaces in structured field", key_cols)

    # Rule 16: leading/trailing punctuation in player field
    if person_col:
        s = clean_str(df[person_col]).str.strip()
        mask = s.str.contains(LEADING_TRAILING_PUNCT_RE, regex=True) & s.ne("")
        add_hits(hits, df, mask, "R16_PUNCT_EDGE_IN_PERSON", "leading/trailing punctuation in player/person", key_cols)

    # Rule 17: suspicious all-caps person field
    if person_col:
        s = clean_str(df[person_col]).str.strip()
        mask = s.str.match(ALL_CAPS_WORD_RE) & ~s.str.contains(r"\b(MC|DJ)\b", regex=True)
        add_hits(hits, df, mask, "R17_ALLCAPS_PERSON", "suspicious all-caps player/person field", key_cols)

    # Rule 18: duplicate place within event+division+place+person
    if all(c is not None for c in [event_id_col, division_col, place_col, person_col]):
        dup_mask = df.duplicated([event_id_col, division_col, place_col, person_col], keep=False)
        add_hits(
            hits, df, dup_mask,
            "R18_EXACT_DUPLICATE_PLACEMENT_ROW",
            "duplicate event+division+place+person row",
            key_cols,
        )

    # Rule 19: multiple place=1 rows in same event+division
    if all(c is not None for c in [event_id_col, division_col, place_col]):
        p1 = df[clean_str(df[place_col]).eq("1") | (df[place_col] == 1)].copy()
        if not p1.empty:
            grp = p1.groupby([event_id_col, division_col]).size().reset_index(name="n_place1")
            bad = grp[grp["n_place1"] > 1]
            if not bad.empty:
                flagged = df.merge(
                    bad[[event_id_col, division_col]],
                    on=[event_id_col, division_col],
                    how="inner",
                )
                add_hits(
                    hits, flagged, pd.Series(True, index=flagged.index),
                    "R19_MULTIPLE_FIRST_PLACES_IN_DIVISION",
                    "multiple place=1 rows in same event+division",
                    key_cols,
                )

    # Rule 20: place sequence starts above 1 inside event+division
    if all(c is not None for c in [event_id_col, division_col, place_col]):
        tmp = df[[event_id_col, division_col, place_col]].copy()
        tmp["_place_num"] = pd.to_numeric(tmp[place_col], errors="coerce")
        mins = tmp.groupby([event_id_col, division_col], dropna=False)["_place_num"].min().reset_index(name="min_place")
        bad = mins[mins["min_place"] > 1]
        if not bad.empty:
            flagged = df.merge(
                bad[[event_id_col, division_col]],
                on=[event_id_col, division_col],
                how="inner",
            )
            add_hits(
                hits, flagged, pd.Series(True, index=flagged.index),
                "R20_MIN_PLACE_GT_1",
                "division placement run starts above 1",
                key_cols,
            )

    # -----------------------------
    # write outputs
    # -----------------------------
    if hits:
        hits_df = pd.concat(hits, ignore_index=True).drop_duplicates()
    else:
        hits_df = pd.DataFrame(columns=["qc_rule", "qc_detail"] + key_cols)

    summary_df = (
        hits_df.groupby(["qc_rule", "qc_detail"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["row_count", "qc_rule"], ascending=[False, True])
    )

    OUT_HITS.parent.mkdir(parents=True, exist_ok=True)
    hits_df.to_csv(OUT_HITS, index=False)
    summary_df.to_csv(OUT_SUMMARY, index=False)

    print(f"Rows scanned: {len(df):,}")
    print(f"Rules triggered: {summary_df.shape[0]}")
    print(f"Hit rows: {len(hits_df):,}")
    print(f"Wrote: {OUT_HITS}")
    print(f"Wrote: {OUT_SUMMARY}")

    if not summary_df.empty:
        print("\nTop triggered rules:")
        print(summary_df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
