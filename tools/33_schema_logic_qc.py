#!/usr/bin/env python3
"""
33_schema_logic_qc.py — Schema validity and logical coherence QC.

Read-only, auditing-only tool. Mirrors tool 32's architecture.
Extends QC to data-type correctness, referential integrity across all
identity artifacts, place-sequence logic, division hygiene, and
cross-artifact consistency.

Checks:
  1. Person integrity       : PF→PT referential integrity, PT uniqueness
  2. Division integrity     : soft-hyphen, encoding artifacts, unknown category
  3. Place sequence         : gaps, min-place anomalies (complete-coverage only)
  4. Same person multi-place: multiple places per person per division
  5. Division inflation     : single-use, winner-name embedding, numeric-only
  6. Longevity scan         : chronology paradoxes, future years
  7. Cardinality & density  : event_id coverage, UUID format, DI cross-check

Inputs (must all exist):
  out/Placements_Flat.csv
  out/Persons_Truth.csv
  out/Coverage_ByEventDivision.csv
  out/Coverage_GapPriority.csv
  out/Data_Integrity.csv
  out/Analytics_Safe_Surface.csv
  Footbag_Results_Canonical.xlsx  (Person_Stats sheet)
"""

from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"
XLSX = ROOT / "Footbag_Results_Canonical.xlsx"

PF_CSV  = OUT / "Placements_Flat.csv"
PT_CSV  = OUT / "Persons_Truth.csv"
COV_CSV = OUT / "Coverage_ByEventDivision.csv"
CGP_CSV = OUT / "Coverage_GapPriority.csv"
DI_CSV  = OUT / "Data_Integrity.csv"
ASS_CSV = OUT / "Analytics_Safe_Surface.csv"

SAFE_COVERAGE_FLAGS = {"complete", "mostly_complete"}

CURRENT_YEAR = date.today().year

# UUID pattern (version 1–5, RFC 4122 variant); pipeline uses UUID5 (name-based)
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# ── output helpers ─────────────────────────────────────────────────────────

def _banner(title: str) -> None:
    print(f"\n{'─'*70}")
    print(f"  {title}")
    print(f"{'─'*70}")


def _ok(msg: str)    -> None: print(f"  ✓  {msg}")
def _warn(msg: str)  -> None: print(f"  ⚠  WARN  {msg}")
def _error(msg: str) -> None: print(f"  ✗  ERROR {msg}")
def _info(msg: str)  -> None: print(f"  ·  INFO  {msg}")


def _as_int(v) -> int | None:
    try:
        return int(float(str(v)))
    except (ValueError, TypeError):
        return None


# ── check 1 · Person Integrity ─────────────────────────────────────────────

def check_person_integrity(pf: pd.DataFrame, pt: pd.DataFrame) -> int:
    """
    1a — Referential: PF → PT (non-unresolved, non-NON_PERSON rows)
    1b — PT uniqueness: effective_person_id
    1c — PT uniqueness: person_canon
    1d — NON_PERSON sentinel integrity
    """
    _banner("CHECK 1 · Person Integrity")

    issues = 0

    # Build PT lookup set
    pt_ids = set(pt["effective_person_id"].dropna().astype(str).str.strip())

    # 1a — Referential: PF → PT
    resolved = pf[
        (pf["person_unresolved"].fillna("").astype(str).str.strip() != "true") &
        (pf["person_id"].fillna("").astype(str).str.strip() != "__NON_PERSON__") &
        (pf["person_id"].notna()) &
        (pf["person_id"].astype(str).str.strip() != "")
    ].copy()

    resolved_ids = resolved["person_id"].astype(str).str.strip()
    missing_mask = ~resolved_ids.isin(pt_ids)
    missing_rows = resolved[missing_mask]

    if len(missing_rows) == 0:
        _ok(f"All {len(pf):,} PF person_ids resolve to PT (referential integrity intact)")
    else:
        # Group by person_id and report
        grouped = (
            missing_rows
            .groupby("person_id")
            .agg(
                person_canon=("person_canon", "first"),
                event_id=("event_id", "first"),
                count=("person_id", "count"),
            )
            .reset_index()
        )
        for _, row in grouped.iterrows():
            pid   = str(row["person_id"])
            canon = str(row.get("person_canon", "?"))
            eid   = str(row.get("event_id", "?"))
            _error(
                f"PF person_id not in PT: {pid[:8]}… "
                f"canon={canon!r} first_event={eid} "
                f"({int(row['count'])} row(s))"
            )
            issues += 1

    # 1b — PT uniqueness: effective_person_id
    pt_id_counts = pt["effective_person_id"].astype(str).str.strip().value_counts()
    dupes_id = pt_id_counts[pt_id_counts > 1]
    if len(dupes_id) == 0:
        _ok(f"PT effective_person_id: {len(pt):,} unique (no duplicates)")
    else:
        for pid, cnt in dupes_id.items():
            _error(f"PT duplicate effective_person_id: {pid!r} appears {cnt}×")
            issues += 1

    # 1c — PT uniqueness: person_canon
    pt_canon_counts = pt["person_canon"].astype(str).str.strip().value_counts()
    dupes_canon = pt_canon_counts[pt_canon_counts > 1]
    if len(dupes_canon) == 0:
        _ok(f"PT person_canon: {len(pt):,} unique (no duplicates)")
    else:
        for canon, cnt in dupes_canon.items():
            _error(f"PT duplicate person_canon: {canon!r} appears {cnt}×")
            issues += 1

    # 1d — NON_PERSON sentinel integrity
    non_person_rows = pf[pf["person_id"].astype(str).str.strip() == "__NON_PERSON__"]
    contradictory = non_person_rows[
        non_person_rows["person_unresolved"].fillna("").astype(str).str.strip() == "true"
    ]
    if len(contradictory) == 0:
        _ok(f"NON_PERSON sentinel: consistent ({len(non_person_rows):,} rows, none marked unresolved)")
    else:
        _warn(
            f"NON_PERSON rows also marked person_unresolved=true: "
            f"{len(contradictory)} row(s) — contradictory sentinel"
        )

    return issues


# ── check 2 · Division Integrity ──────────────────────────────────────────

def check_division_integrity(pf: pd.DataFrame) -> int:
    """
    2a — Soft-hyphen contamination (U+00AD)
    2b — Unicode substitution character / encoding failure
    2c — Unknown division_category
    2d — Excessively long division names
    """
    _banner("CHECK 2 · Division Integrity")

    issues = 0

    # Work on distinct (division_canon, division_category) pairs
    divs = (
        pf[["division_canon", "division_category", "event_id"]]
        .drop_duplicates(subset=["division_canon", "division_category"])
        .copy()
    )
    divs["division_canon"] = divs["division_canon"].fillna("").astype(str)
    divs["division_category"] = divs["division_category"].fillna("").astype(str)

    distinct_divisions = divs["division_canon"].unique()
    _info(f"Distinct division_canon values in PF: {len(distinct_divisions)}")

    # 2a — Soft-hyphen contamination
    soft_hyp = divs[divs["division_canon"].str.contains("\u00ad", regex=False)]
    if len(soft_hyp) == 0:
        _ok("No soft-hyphen (U+00AD) contamination in division names")
    else:
        for _, row in soft_hyp.iterrows():
            _error(
                f"Soft-hyphen in division_canon: {row['division_canon']!r} "
                f"(event {row['event_id']})"
            )
            issues += 1

    # 2b — Unicode substitution character / encoding failures
    # Check for U+FFFD (replacement char) or '?' in suspicious encoding context
    subst_re = re.compile(r"\ufffd|(?<=[^\x00-\x7F])\?|\?(?=[^\x00-\x7F])")
    encoding_suspects = divs[divs["division_canon"].str.contains("\ufffd", regex=False)]
    if len(encoding_suspects) > 0:
        for _, row in encoding_suspects.iterrows():
            _warn(f"Unicode replacement char (U+FFFD) in division: {row['division_canon']!r}")
    else:
        _ok("No Unicode replacement characters (U+FFFD) in division names")

    # Also check for contextual '?' (surrounded by letters, suggesting encoding fallback)
    question_re = re.compile(r"[A-Za-z]\?[A-Za-z]|\?[A-Za-z]{2}|[A-Za-z]{2}\?")
    question_hits = divs[divs["division_canon"].str.contains(question_re)]
    if len(question_hits) > 0:
        _warn(f"{len(question_hits)} division(s) with '?' in letter context (possible encoding fallback):")
        for _, row in question_hits.head(5).iterrows():
            _warn(f"  {row['division_canon']!r} (event {row['event_id']})")

    # 2c — Unknown division_category
    unknown_cats = divs[
        divs["division_category"].isin(["unknown", ""]) |
        divs["division_category"].isna()
    ]
    if len(unknown_cats) == 0:
        _ok("No unknown/empty division_category values")
    else:
        # Count affected rows in PF (not just distinct divisions)
        unknown_divs = set(unknown_cats["division_canon"])
        affected_rows = pf[pf["division_canon"].isin(unknown_divs)]
        _warn(
            f"{len(unknown_cats)} distinct division(s) with unknown/empty category "
            f"({len(affected_rows):,} PF row(s) affected):"
        )
        for _, row in unknown_cats.head(8).iterrows():
            _warn(f"  {row['division_canon']!r} (event {row['event_id']})")
        if len(unknown_cats) > 8:
            _warn(f"  … and {len(unknown_cats) - 8} more")

    # 2d — Excessively long division names (> 60 chars)
    long_divs = divs[divs["division_canon"].str.len() > 60]
    if len(long_divs) == 0:
        _ok("No excessively long division names (all ≤ 60 chars)")
    else:
        _warn(f"{len(long_divs)} division name(s) exceed 60 characters (possible unparsed trick list):")
        for _, row in long_divs.iterrows():
            display = row["division_canon"][:80]
            _warn(f"  {display!r}… (event {row['event_id']})")

    return issues


# ── check 3 · Place Sequence Integrity ────────────────────────────────────

def check_place_sequence(pf: pd.DataFrame, cov: pd.DataFrame) -> int:
    """
    Restrict to complete-coverage (event_id, division_canon) pairs.
    3a — min_place != 1 (WARN)
    3b — Sequence gaps not explained by ties (WARN)
    3c — place == 0 (ERROR)
    """
    _banner("CHECK 3 · Place Sequence Integrity (complete-coverage divisions only)")

    issues = 0

    # Get complete-coverage (event_id, division_canon) pairs from Coverage_ByEventDivision
    cov_str = cov.copy()
    cov_str["event_id"] = cov_str["event_id"].astype(str)
    cov_str["division_canon"] = cov_str["division_canon"].astype(str)
    complete_pairs = set(
        zip(
            cov_str.loc[cov_str["coverage_flag"] == "complete", "event_id"],
            cov_str.loc[cov_str["coverage_flag"] == "complete", "division_canon"],
        )
    )
    _info(f"Complete-coverage (event, division) pairs to check: {len(complete_pairs):,}")

    pf_str = pf.copy()
    pf_str["event_id"] = pf_str["event_id"].astype(str)
    pf_str["division_canon"] = pf_str["division_canon"].astype(str)
    pf_str["place_int"] = pd.to_numeric(pf_str["place"], errors="coerce")
    pf_complete = pf_str[
        pf_str[["event_id", "division_canon"]].apply(
            lambda r: (r["event_id"], r["division_canon"]) in complete_pairs, axis=1
        ) &
        pf_str["place_int"].notna()
    ]

    min_place_anomalies = 0
    gap_anomalies       = 0
    zero_place_errors   = 0

    groups = pf_complete.groupby(["event_id", "division_canon"])

    for (eid, div), grp in groups:
        places = sorted(grp["place_int"].dropna().astype(int).unique())

        if not places:
            continue

        # 3c — place == 0
        if 0 in places:
            _error(f"Place=0 in complete-coverage group: event={eid} div={div!r}")
            zero_place_errors += 1
            issues += 1

        # 3a — min_place != 1
        if places[0] != 1 and places[0] != 0:
            _warn(f"min_place={places[0]} (not 1) in event={eid} div={div!r}")
            min_place_anomalies += 1
            continue  # Skip gap check if sequence doesn't start at 1

        # 3b — Sequence gaps not explained by ties
        # Count how many persons are at each place
        place_counts = grp["place_int"].astype(int).value_counts().to_dict()

        for i, p in enumerate(places):
            if i == 0:
                continue
            prev = places[i - 1]
            expected_next = prev + 1

            if p == expected_next:
                continue  # No gap

            # Gap detected: places between prev and p-1 are missing
            # This is explained by a tie if prev appears more than once
            if place_counts.get(prev, 0) > 1:
                continue  # Tie at prev explains skip to p

            # Unexplained gap
            _warn(
                f"Place gap: event={eid} div={div!r} "
                f"has place {prev} then {p} (missing {expected_next}–{p-1})"
            )
            gap_anomalies += 1
            if gap_anomalies >= 20:
                _warn("(truncating gap warnings at 20)")
                break

    if zero_place_errors == 0:
        _ok("No place=0 entries in complete-coverage divisions")
    if min_place_anomalies == 0:
        _ok("All complete-coverage divisions start at place=1")
    else:
        _info(f"Min-place anomalies (start ≠ 1): {min_place_anomalies} groups")
    if gap_anomalies == 0:
        _ok("No unexplained place-sequence gaps in complete-coverage divisions")
    else:
        _info(f"Unexplained sequence gaps: {gap_anomalies} groups (may indicate ties or sub-divisions)")

    return issues


# ── check 4 · Same Person, Multiple Places ────────────────────────────────

def check_same_person_multi_place(pf: pd.DataFrame) -> int:
    """
    Find (event_id, division_canon, person_id) groups with > 1 distinct place.
    Restricted to competitor_type == 'player'.
    Categorize as sub_round (WARN), adjacent_podium (WARN), or duplicate (ERROR).
    """
    _banner("CHECK 4 · Same Person, Multiple Places")

    issues = 0

    pf_players = pf[
        (pf["competitor_type"].fillna("").astype(str) == "player") &
        pf["person_id"].notna() &
        (pf["person_id"].astype(str).str.strip() != "") &
        (pf["person_id"].astype(str).str.strip() != "__NON_PERSON__")
    ].copy()
    pf_players["place_int"] = pd.to_numeric(pf_players["place"], errors="coerce")
    pf_players = pf_players[pf_players["place_int"].notna()]

    collisions = (
        pf_players
        .groupby(["event_id", "division_canon", "person_id"])["place_int"]
        .apply(lambda s: sorted(s.unique().astype(int).tolist()))
        .reset_index()
    )
    collisions.columns = ["event_id", "division_canon", "person_id", "places"]
    multi = collisions[collisions["places"].apply(len) > 1]

    if len(multi) == 0:
        _ok("No same-person multi-place collisions in player rows")
        return 0

    sub_round_count  = 0
    adj_podium_count = 0
    duplicate_count  = 0

    # Lookup canon name
    pid_to_canon = (
        pf_players.drop_duplicates("person_id")
        .set_index("person_id")["person_canon"]
        .to_dict()
    )

    for _, row in multi.iterrows():
        places = row["places"]
        pid    = str(row["person_id"])
        canon  = pid_to_canon.get(pid, pid[:8])
        eid    = row["event_id"]
        div    = row["division_canon"]

        places_set = set(places)

        # Categorize
        if places_set == {1, 2} or places_set == {2, 3}:
            cat = "adjacent_podium"
            _warn(
                f"[{cat}] event={eid} div={div!r} "
                f"person={canon!r} places={places} "
                f"(dual-division entry or heat/final)"
            )
            adj_podium_count += 1

        elif len(places_set) == 1:
            # Same place twice — true duplicate row
            cat = "duplicate"
            _error(
                f"[{cat}] event={eid} div={div!r} "
                f"person={canon!r} appears {len(places)}× at place={places[0]}"
            )
            duplicate_count += 1
            issues += 1

        else:
            # Non-adjacent, non-consecutive spread → sub_round / pool-play
            cat = "sub_round"
            _warn(
                f"[{cat}] event={eid} div={div!r} "
                f"person={canon!r} places={places} "
                f"(pool/heat/round-robin)"
            )
            sub_round_count += 1

    _info(f"Collision summary: sub_round={sub_round_count}, adjacent_podium={adj_podium_count}, duplicate(ERROR)={duplicate_count}")

    if duplicate_count == 0:
        _ok("No duplicate (same person, same place) collisions")

    return issues


# ── check 5 · Division Inflation ──────────────────────────────────────────

def check_division_inflation(pf: pd.DataFrame) -> int:
    """
    5a — Single-use divisions (INFO)
    5b — Winner-name embedding in division_canon (WARN)
    5c — Numeric-only or symbol-only division names (WARN)
    """
    _banner("CHECK 5 · Division Inflation")

    issues = 0

    div_event_counts = (
        pf.groupby("division_canon")["event_id"]
        .nunique()
        .reset_index()
    )
    div_event_counts.columns = ["division_canon", "event_count"]

    # 5a — Single-use divisions
    single_use = div_event_counts[div_event_counts["event_count"] == 1]
    _info(f"Single-use divisions (appear in exactly 1 event): {len(single_use)} — normal for historical data")

    # 5b — Winner-name embedding
    # Get all (division_canon, place=1, person_canon) records
    winners = pf[
        pd.to_numeric(pf["place"], errors="coerce") == 1
    ][["division_canon", "person_canon"]].dropna()

    embedding_count = 0
    for _, row in winners.iterrows():
        div    = str(row["division_canon"])
        canon  = str(row["person_canon"]).strip()
        if not canon or canon == "__NON_PERSON__":
            continue
        # Check if winner's name (at least 5 chars, to avoid short false positives) is in division
        if len(canon) >= 5 and canon.lower() in div.lower():
            _warn(f"Winner name embedded in division: {div!r} contains winner {canon!r}")
            embedding_count += 1
            issues += 1

    if embedding_count == 0:
        _ok("No winner-name embedding detected in division names")

    # 5c — Numeric-only or symbol-only division names
    div_names = pf["division_canon"].dropna().unique()
    numeric_only_re = re.compile(r"^\d+$")
    symbol_only_re  = re.compile(r"^[^\w]+$")

    bad_divs = [
        d for d in div_names
        if numeric_only_re.match(str(d)) or symbol_only_re.match(str(d))
    ]
    if len(bad_divs) == 0:
        _ok("No numeric-only or symbol-only division names")
    else:
        for d in bad_divs:
            _warn(f"Numeric/symbol-only division name: {d!r}")
        issues += len(bad_divs)

    return issues


# ── check 6 · Longevity Scan ───────────────────────────────────────────────

def check_longevity(ps: pd.DataFrame) -> int:
    """
    6a — first_year > last_year (ERROR)
    6b — years_active > (last_year − first_year + 1) (ERROR)
    6c — Long career, very few placements (WARN)
    6d — Future year (WARN)
    """
    _banner("CHECK 6 · Longevity Scan (Person_Stats)")

    issues = 0

    required_cols = {"first_year", "last_year", "years_active", "placements_total"}
    available = set(ps.columns)
    missing_cols = required_cols - available
    if missing_cols:
        _warn(f"Person_Stats missing columns: {missing_cols} — some sub-checks skipped")

    paradox_count   = 0
    active_paradox  = 0
    sparse_career   = 0
    future_year     = 0

    for _, row in ps.iterrows():
        canon       = str(row.get("person_canon", "?"))
        first_year  = _as_int(row.get("first_year"))
        last_year   = _as_int(row.get("last_year"))
        years_active= _as_int(row.get("years_active"))
        total_pl    = _as_int(row.get("placements_total"))

        if first_year is None or last_year is None:
            continue

        # 6a — Impossible chronology
        if first_year > last_year:
            _error(f"first_year > last_year: {canon!r} ({first_year} > {last_year})")
            paradox_count += 1
            issues += 1

        # 6b — years_active exceeds span
        if years_active is not None:
            span = last_year - first_year + 1
            if years_active > span:
                _error(
                    f"years_active paradox: {canon!r} "
                    f"years_active={years_active} > span={span} ({first_year}–{last_year})"
                )
                active_paradox += 1
                issues += 1

        # 6c — Long career, very few placements
        if (last_year - first_year) > 15 and total_pl is not None and total_pl < 3:
            _warn(
                f"Long career / sparse placements: {canon!r} "
                f"({first_year}–{last_year}, {total_pl} placement(s)) — possible data artifact"
            )
            sparse_career += 1

        # 6d — Future year
        if last_year > CURRENT_YEAR:
            _warn(f"Future last_year: {canon!r} last_year={last_year} (current={CURRENT_YEAR})")
            future_year += 1

    if paradox_count == 0:
        _ok("No first_year > last_year chronology paradoxes")
    if active_paradox == 0:
        _ok("No years_active > span paradoxes")
    if sparse_career == 0:
        _ok("No long-career / sparse-placement anomalies")
    else:
        _info(f"Long-career / sparse-placement anomalies: {sparse_career} (WARNs above)")
    if future_year == 0:
        _ok(f"No future last_year values (all ≤ {CURRENT_YEAR})")
    else:
        _info(f"Future-year warnings: {future_year}")

    return issues


# ── check 7 · Cardinality & Density ───────────────────────────────────────

def check_cardinality_and_density(
    pf: pd.DataFrame,
    pt: pd.DataFrame,
    cov: pd.DataFrame,
    cgp: pd.DataFrame | None,
    di: pd.DataFrame,
    ass: pd.DataFrame,
) -> int:
    """
    7a — PF event_ids ⊆ Coverage event_ids
    7b — PT UUID format validation
    7c — Data_Integrity cross-check (staleness detector)
    7d — Persons with 0 analytics-safe placements (INFO)
    7e — Coverage gap summary (INFO, known limitation)
    """
    _banner("CHECK 7 · Cardinality & Density")

    issues = 0

    # 7a — PF event_ids ⊆ Coverage event_ids
    pf_eids  = set(pf["event_id"].astype(str))
    cov_eids = set(cov["event_id"].astype(str))
    missing_from_cov = pf_eids - cov_eids

    if len(missing_from_cov) == 0:
        _ok(f"All {len(pf_eids)} PF event_ids present in Coverage_ByEventDivision")
    else:
        for eid in sorted(missing_from_cov):
            _error(f"PF event_id {eid} has no entry in Coverage_ByEventDivision")
            issues += 1

    # 7b — PT UUID format
    uuid_col = pt["effective_person_id"].fillna("").astype(str).str.strip()
    malformed = []
    for pid in uuid_col:
        if not pid:
            continue
        if not UUID_RE.match(pid):
            malformed.append(pid)

    if len(malformed) == 0:
        _ok(f"All {len(uuid_col):,} PT effective_person_id values are well-formed UUIDs (v1–5)")
    else:
        for pid in malformed[:10]:
            _error(f"Malformed UUID in PT: {pid!r}")
        if len(malformed) > 10:
            _error(f"… and {len(malformed) - 10} more malformed UUIDs")
        issues += len(malformed)

    # 7c — Data_Integrity cross-check
    di_by_metric = {}
    for _, row in di.iterrows():
        key = (str(row.get("category", "")), str(row.get("metric", "")))
        di_by_metric[key] = str(row.get("value", "")).strip()

    stale_warnings = 0

    # Check: total PF rows vs DI "Placements / Total in source (raw)"
    di_pf_total = _as_int(di_by_metric.get(("Placements", "Total in source (raw)")))
    if di_pf_total is not None:
        actual_pf = len(pf)
        if di_pf_total != actual_pf:
            _warn(
                f"Data_Integrity.csv stale: Placements total recorded={di_pf_total} "
                f"but current PF has {actual_pf} rows (delta={actual_pf - di_pf_total:+})"
            )
            stale_warnings += 1
        else:
            _ok(f"Data_Integrity Placements total matches PF row count ({actual_pf:,})")
    else:
        _warn("Data_Integrity.csv missing 'Placements / Total in source (raw)' row")
        stale_warnings += 1

    # Check: PT Gate3 count vs DI "Persons / Total (Gate 3)"
    di_pt_total = _as_int(di_by_metric.get(("Persons", "Total (Gate 3)")))
    if di_pt_total is not None:
        # All rows in Persons_Truth.csv are Gate-3 persons (excluded persons live in
        # Persons_Truth_Excluded.csv).  exclusion_reason within PT controls analytics
        # display only (e.g. COVERAGE_CLOSURE), not Gate-3 membership.
        actual_pt = len(pt)
        if di_pt_total != actual_pt:
            _warn(
                f"Data_Integrity.csv stale: Persons Gate3 recorded={di_pt_total} "
                f"but current PT has {actual_pt} non-excluded rows (delta={actual_pt - di_pt_total:+})"
            )
            stale_warnings += 1
        else:
            _ok(f"Data_Integrity Persons Gate3 matches PT non-excluded count ({actual_pt:,})")
    else:
        _warn("Data_Integrity.csv missing 'Persons / Total (Gate 3)' row")
        stale_warnings += 1

    if stale_warnings == 0:
        _ok("Data_Integrity.csv is current (no staleness detected)")

    # 7d — Persons with 0 analytics-safe placements (INFO only)
    pt_ids = set(pt[pt["exclusion_reason"].fillna("") == ""]["effective_person_id"].astype(str))
    if "person_canon" in ass.columns:
        # Analytics_Safe_Surface has person_canon but not person_id; use what's available
        ass_canons = set(ass["person_canon"].dropna().astype(str))
        pt_canons  = set(pt[pt["exclusion_reason"].fillna("") == ""]["person_canon"].astype(str))
        zero_safe  = pt_canons - ass_canons
        _info(
            f"Presentable persons with 0 analytics-safe placements: {len(zero_safe)} "
            f"(stubs or fully-unresolved persons)"
        )
    else:
        _info("Analytics_Safe_Surface has no person_canon column — skipping density check")

    # 7e — Coverage gap summary (INFO only; no errors raised)
    # These (event, division) pairs have incomplete place sequences in the source data.
    # Known limitation: results lists were often partial (top-N only) or were never
    # fully recorded for older events. All present placements are kept; no rows dropped.
    if cgp is not None and not cgp.empty:
        n_total = len(cgp)
        missing_total = int(cgp["missing_places"].sum()) if "missing_places" in cgp.columns else 0
        _info(
            f"(event,div) pairs with incomplete place sequences (Coverage_GapPriority): "
            f"{n_total} pairs, {missing_total:,} missing place numbers total"
        )
        if "gap_class" in cgp.columns:
            for cls, cnt in cgp["gap_class"].value_counts().items():
                _info(f"    {cnt:4d}  {cls}")
        _info(
            "  Known limitation: source data for these events listed only top-N finishers. "
            "All present placements are preserved; gaps are not data errors."
        )

    return issues


# ── main ──────────────────────────────────────────────────────────────────

def main() -> None:
    required = [PF_CSV, PT_CSV, COV_CSV, DI_CSV, ASS_CSV, XLSX]
    missing  = [p for p in required if not p.exists()]
    if missing:
        for p in missing:
            print(f"ERROR: Missing input: {p}", file=sys.stderr)
        sys.exit(1)

    print("=" * 70)
    print("SCHEMA & LOGIC QC  —  7 checks")
    print("=" * 70)
    print()
    print("Loading data…")

    pf  = pd.read_csv(PF_CSV,  dtype=str, low_memory=False)
    pt  = pd.read_csv(PT_CSV,  dtype=str, low_memory=False)
    cov = pd.read_csv(COV_CSV, dtype=str, low_memory=False)
    di  = pd.read_csv(DI_CSV)
    ass = pd.read_csv(ASS_CSV, dtype=str, low_memory=False)
    ps  = pd.read_excel(XLSX, sheet_name="Person_Stats")
    cgp = pd.read_csv(CGP_CSV) if CGP_CSV.exists() else None

    # Numeric conversions for Coverage (needed for place sequence check)
    for col in ["placements_present", "min_place", "max_place", "expected_span"]:
        if col in cov.columns:
            cov[col] = pd.to_numeric(cov[col], errors="coerce")

    print(f"  Placements_Flat:          {len(pf):,} rows")
    print(f"  Persons_Truth:            {len(pt):,} rows")
    print(f"  Coverage_ByEventDivision: {len(cov):,} rows")
    print(f"  Coverage_GapPriority:     {len(cgp):,} rows" if cgp is not None else "  Coverage_GapPriority:     (not found)")
    print(f"  Analytics_Safe_Surface:   {len(ass):,} rows")
    print(f"  Person_Stats (Excel):     {len(ps):,} rows")

    total = 0
    total += check_person_integrity(pf, pt)
    total += check_division_integrity(pf)
    total += check_place_sequence(pf, cov)
    total += check_same_person_multi_place(pf)
    total += check_division_inflation(pf)
    total += check_longevity(ps)
    total += check_cardinality_and_density(pf, pt, cov, cgp, di, ass)

    _banner("SUMMARY")
    if total == 0:
        print("  ✓  All 7 checks passed — no errors found")
    else:
        print(f"  ✗  {total} error(s) across all checks")
        sys.exit(1)


if __name__ == "__main__":
    main()
