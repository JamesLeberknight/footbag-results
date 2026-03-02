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
QC_REPORTS_DIR = OUT / "qc_reports"
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

_SLOP_RE = re.compile(
    r"(discount code|if you are asked|captcha|password|login|http[s]?://|www\.)", re.I
)


def looks_like_slop_or_fragment(s: str) -> bool:
    if not s:
        return True
    s = s.strip()
    if _SLOP_RE.search(s):
        return True
    # single token fragments like "Greer"
    if len(s.split()) == 1 and len(s) <= 8:
        return True
    # very long = sentence
    if len(s) > 80:
        return True
    # low alphabetic content = junk
    alpha = sum(c.isalpha() for c in s)
    if alpha / max(1, len(s)) < 0.55:
        return True
    # "Name (Country)" style fragments
    if "(" in s and ")" in s and len(s.split()) <= 4:
        return True
    return False


# ── output helpers ─────────────────────────────────────────────────────────

CAP_EXAMPLES = 20

def _banner(title: str) -> None:
    print(f"\n{'─'*70}")
    print(f"  {title}")
    print(f"{'─'*70}")


def _ok(msg: str)    -> None: print(f"  ✓  {msg}")
def _warn(msg: str)  -> None: print(f"  ⚠  WARN  {msg}")
def _error(msg: str) -> None: print(f"  ✗  ERROR {msg}")
def _info(msg: str)  -> None: print(f"  ·  INFO  {msg}")


def _ensure_reports_dir() -> Path:
    QC_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return QC_REPORTS_DIR


def _write_report_and_cap(
    rows: list[dict],
    report_name: str,
    *,
    cap: int = CAP_EXAMPLES,
    level: str = "WARN",
) -> None:
    """Write full list to out/qc_reports/<report_name>.csv; print only first cap examples."""
    if not rows:
        return
    _ensure_reports_dir()
    path = QC_REPORTS_DIR / f"{report_name}.csv"
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    n = len(rows)
    if n <= cap:
        for r in rows:
            msg = r.get("message", str(r))
            (_warn if level == "WARN" else _info)(msg)
    else:
        for r in rows[:cap]:
            msg = r.get("message", str(r))
            (_warn if level == "WARN" else _info)(msg)
        (_warn if level == "WARN" else _info)(
            f"{report_name}: {n} items (showing first {cap}; full report: {path})"
        )


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

    # 2a — Soft-hyphen contamination (ERROR; report + cap if many)
    soft_hyp = divs[divs["division_canon"].str.contains("\u00ad", regex=False)]
    if len(soft_hyp) == 0:
        _ok("No soft-hyphen (U+00AD) contamination in division names")
    else:
        soft_rows = [{"event_id": r["event_id"], "division_canon": r["division_canon"], "message": f"Soft-hyphen in division_canon: {r['division_canon']!r} (event {r['event_id']})"} for _, r in soft_hyp.iterrows()]
        for r in soft_rows:
            issues += 1
        _ensure_reports_dir()
        path = QC_REPORTS_DIR / "division_soft_hyphen.csv"
        pd.DataFrame(soft_rows).to_csv(path, index=False)
        for r in soft_rows[:CAP_EXAMPLES]:
            _error(r["message"])
        if len(soft_rows) > CAP_EXAMPLES:
            _error(f"Soft-hyphen: {len(soft_rows)} division(s) (showing first {CAP_EXAMPLES}; full report: {path})")

    # 2b — U+FFFD (replacement char) → WARN; contextual '?' (e.g. Women?S) → INFO only
    encoding_suspects = divs[divs["division_canon"].str.contains("\ufffd", regex=False)]
    if len(encoding_suspects) > 0:
        rows_fffd = [{"event_id": r["event_id"], "division_canon": r["division_canon"], "message": f"U+FFFD in division: {r['division_canon']!r} (event {r['event_id']})"} for _, r in encoding_suspects.iterrows()]
        _warn(f"Unicode replacement char (U+FFFD) in division names: {len(rows_fffd)}")
        _write_report_and_cap(rows_fffd, "division_encoding_fffd", level="WARN")
    else:
        _ok("No Unicode replacement characters (U+FFFD) in division names")

    question_re = re.compile(r"[A-Za-z]\?[A-Za-z]|\?[A-Za-z]{2}|[A-Za-z]{2}\?")
    question_hits = divs[divs["division_canon"].str.contains(question_re)]
    if len(question_hits) > 0:
        q_rows = [{"event_id": r["event_id"], "division_canon": r["division_canon"], "message": f"Division with ? in letter context: {r['division_canon']!r} (event {r['event_id']})"} for _, r in question_hits.iterrows()]
        _info(f"Division(s) with '?' in letter context (encoding fallback, e.g. Women?S): {len(q_rows)}")
        _write_report_and_cap(q_rows, "division_question_encoding", level="INFO")

    # 2c — Unknown division_category: taxonomy coverage, not corruption.
    # WARN only if affected rows ≥ 500 or ≥ 1% of PF; else INFO + examples.
    unknown_cats = divs[
        divs["division_category"].isin(["unknown", ""]) |
        divs["division_category"].isna()
    ]
    if len(unknown_cats) == 0:
        _ok("No unknown/empty division_category values")
    else:
        unknown_divs = set(unknown_cats["division_canon"])
        affected_rows = pf[pf["division_canon"].isin(unknown_divs)]
        n_affected = len(affected_rows)
        pct = (n_affected / len(pf) * 100) if len(pf) else 0
        unknown_report = [{"event_id": r["event_id"], "division_canon": r["division_canon"], "message": f"{r['division_canon']!r} (event {r['event_id']})"} for _, r in unknown_cats.iterrows()]
        if n_affected >= 500 or pct >= 1.0:
            _warn(
                f"{len(unknown_cats)} distinct division(s) with unknown/empty category "
                f"({n_affected:,} PF row(s) affected, {pct:.2f}%)"
            )
            _write_report_and_cap(unknown_report, "division_unknown_category", level="WARN")
        else:
            _info(
                f"Unknown/empty division_category: {len(unknown_cats)} division(s), "
                f"{n_affected} PF row(s) (taxonomy limitation)"
            )
            _write_report_and_cap(unknown_report, "division_unknown_category", level="INFO")

    # 2d — Excessively long division names (> 60 chars)
    long_divs = divs[divs["division_canon"].str.len() > 60]
    if len(long_divs) == 0:
        _ok("No excessively long division names (all ≤ 60 chars)")
    else:
        long_rows = [{"event_id": r["event_id"], "division_canon": r["division_canon"][:80], "message": f"{r['division_canon'][:80]!r}… (event {r['event_id']})"} for _, r in long_divs.iterrows()]
        _warn(f"{len(long_divs)} division name(s) exceed 60 characters (possible unparsed trick list)")
        _write_report_and_cap(long_rows, "division_long_names", level="WARN")

    return issues


# ── check 3 · Place Sequence Integrity ────────────────────────────────────

def check_place_sequence(pf: pd.DataFrame, cov: pd.DataFrame) -> int:
    """
    Restrict to complete-coverage (event_id, division_canon) pairs.
    3a — min_place != 1: INFO (partial results published) unless contradictory
    3b — Gaps: WARN only for non-trivial early gaps (missing 1–3) or multiple gaps in complete div
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

    zero_place_errors = 0
    min_place_partial: list[dict] = []   # INFO: partial results (no place 1 in this group)
    gap_warn_rows: list[dict] = []       # WARN: early gaps or multiple gaps
    gap_info_rows: list[dict] = []       # INFO: other gaps

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

        # 3a — min_place != 1 → INFO (partial results published; no place 1 in this event,div)
        if places[0] != 1 and places[0] != 0:
            min_place_partial.append({
                "event_id": eid,
                "division_canon": div,
                "min_place": int(places[0]),
                "message": f"min_place={places[0]} (not 1) in event={eid} div={div!r} — partial results published",
            })
            continue  # Skip gap check if sequence doesn't start at 1

        # 3b — Sequence gaps not explained by ties
        place_counts = grp["place_int"].astype(int).value_counts().to_dict()

        for i, p in enumerate(places):
            if i == 0:
                continue
            prev = places[i - 1]
            expected_next = prev + 1

            if p == expected_next:
                continue  # No gap

            if place_counts.get(prev, 0) > 1:
                continue  # Tie at prev explains skip to p

            # Unexplained gap: missing expected_next through p-1
            early_gap = expected_next <= 3  # missing 1, 2, and/or 3
            msg = (
                f"Place gap: event={eid} div={div!r} "
                f"has place {prev} then {p} (missing {expected_next}–{p-1})"
            )
            row = {"event_id": eid, "division_canon": div, "prev": prev, "next": p, "message": msg}
            if early_gap:
                gap_warn_rows.append(row)
            else:
                gap_info_rows.append(row)

    # Multiple gaps in same (event, div) → promote to WARN
    key_counts: dict[tuple[str, str], int] = {}
    for row in gap_warn_rows + gap_info_rows:
        k = (row["event_id"], row["division_canon"])
        key_counts[k] = key_counts.get(k, 0) + 1
    multi_gap_keys = {k for k, c in key_counts.items() if c > 1}
    for row in gap_info_rows[:]:
        if (row["event_id"], row["division_canon"]) in multi_gap_keys:
            gap_info_rows.remove(row)
            row["message"] += " (multiple gaps in division)"
            gap_warn_rows.append(row)

    if zero_place_errors == 0:
        _ok("No place=0 entries in complete-coverage divisions")
    if min_place_partial:
        _info(f"Min-place ≠ 1 (partial results): {len(min_place_partial)} groups")
        _write_report_and_cap(min_place_partial, "min_place_partial", level="INFO")
    else:
        _ok("All complete-coverage divisions start at place=1 or are partial-only")
    if gap_warn_rows:
        _warn(f"Place-sequence gaps (early or multiple): {len(gap_warn_rows)} groups")
        _write_report_and_cap(gap_warn_rows, "place_gap_warn", level="WARN")
    if gap_info_rows:
        _info(f"Other place-sequence gaps: {len(gap_info_rows)} groups (ties/sub-divisions)")
        _write_report_and_cap(gap_info_rows, "place_gap_info", level="INFO")
    if not gap_warn_rows and not gap_info_rows:
        _ok("No unexplained place-sequence gaps in complete-coverage divisions")

    return issues


# ── check 4 · Same Person, Multiple Places ────────────────────────────────

def _division_hints_pool_play(div: str) -> bool:
    """True if division name suggests pool/heat/round-robin (expected multi-place)."""
    if not div:
        return False
    d = str(div).lower()
    if any(h in d for h in (
        "pool", "group", "round", "heat", "prelim", "qual", "semi", "final",
        "circle",
    )):
        return True
    # Shred 30 often has multiple rounds
    if "shred" in d and "30" in d:
        return True
    # division a / division b (subgroups)
    if "division a" in d or "division b" in d:
        return True
    return False


def _division_roundy(div: str) -> bool:
    """True if division name suggests sub-rounds (net, freestyle, shred, routines, etc.)."""
    d = (div or "").lower()
    return any(
        k in d
        for k in [
            "pool", "group", "round", "heat", "prelim", "qual", "semi", "final",
            "net", "freestyle", "shred", "sick", "routines",
        ]
    )


def _small_place_spread(places: list[int]) -> bool:
    """True if place spread (max - min) is <= 20 (plausible pool/prelim/final)."""
    if not places:
        return False
    return (max(places) - min(places)) <= 20


def _div_suggests_rounds_or_concat(div: str) -> bool:
    d = (div or "").lower()

    # Strong signals: request contests are frequently multi-round or multi-category
    if "request" in d:
        return True

    # Singles divisions often have pool-play / groups flattened into one division label
    if "singles" in d:
        return True

    # Obvious concatenation / multi-division header artifacts
    if " mens " in f" {d} " and " womens " in f" {d} ":
        return True
    if d.count("open") >= 2:
        return True

    # Explicit round words
    if any(k in d for k in ["pool", "group", "round", "heat", "qual", "prelim", "semi", "final"]):
        return True

    return False


def _place_spread(places: list[int]) -> int:
    if not places:
        return 0
    return max(places) - min(places)


def _pool_play_heavy_events(pf: pd.DataFrame, min_shared_groups: int = 10) -> set:
    """Events with ≥ min_shared_groups (event,div,place) groups that have >1 player (ties/pool-play)."""
    pf_place = pf.copy()
    pf_place["place"] = pd.to_numeric(pf_place["place"], errors="coerce")
    pf_place = pf_place[pf_place["place"].notna()]
    shared = (
        pf_place.groupby(["event_id", "division_canon", "place"])
        .size()
        .reset_index(name="n")
    )
    shared = shared[shared["n"] > 1]
    per_event = shared.groupby("event_id").size()
    heavy = set(per_event[per_event >= min_shared_groups].index.astype(str))
    return heavy


def check_same_person_multi_place(pf: pd.DataFrame) -> int:
    """
    Find (event_id, division_canon, person_id) groups with > 1 distinct place.
    sub_round / adjacent_podium → INFO (expected). WARN only for unexplained
    collisions: places far apart (diff >= 3) and no pool-play hint in division.
    Pool-play-heavy events (many shared-place groups) → multi-place is INFO.
    """
    _banner("CHECK 4 · Same Person, Multiple Places")

    issues = 0
    pool_play_heavy = _pool_play_heavy_events(pf)

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

    pid_to_canon = (
        pf_players.drop_duplicates("person_id")
        .set_index("person_id")["person_canon"]
        .to_dict()
    )

    info_rows: list[dict] = []
    warn_rows: list[dict] = []
    duplicate_count = 0

    for _, row in multi.iterrows():
        places = row["places"]
        pid = str(row["person_id"])
        canon = pid_to_canon.get(pid, pid[:8])
        eid = row["event_id"]
        div = str(row["division_canon"] or "")
        places_set = set(places)

        if len(places_set) == 1:
            cat = "duplicate"
            _error(
                f"[{cat}] event={eid} div={div!r} "
                f"person={canon!r} appears {len(places)}× at place={places[0]}"
            )
            duplicate_count += 1
            issues += 1
            continue

        # Division looks like concatenated results header (parse artifact) → INFO
        if div.lower().startswith("results "):
            cat = "parse_artifact"
            msg = f"[{cat}] event={eid} div={div!r} person={canon!r} places={places} (division looks like results header)"
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # Adjacent podium (1,2 or 2,3) → INFO
        if places_set == {1, 2} or places_set == {2, 3}:
            cat = "adjacent_podium"
            msg = f"[{cat}] event={eid} div={div!r} person={canon!r} places={places} (dual-division or heat/final)"
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # Sub-round / pool-play style → INFO
        if _division_hints_pool_play(div):
            cat = "sub_round"
            msg = f"[{cat}] event={eid} div={div!r} person={canon!r} places={places} (pool/heat/round-robin)"
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # Places close (diff < 3) → INFO (likely tie or sub-round)
        place_span = max(places) - min(places)
        if place_span < 3:
            cat = "sub_round"
            msg = f"[{cat}] event={eid} div={div!r} person={canon!r} places={places} (pool/heat/round-robin)"
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # Far apart: downgrade to INFO if event is pool-play heavy or division has round hints
        if str(eid) in pool_play_heavy:
            cat = "sub_round"
            msg = f"[{cat}] event={eid} div={div!r} person={canon!r} places={places} (pool-play-heavy event)"
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue
        if _division_hints_pool_play(div):
            cat = "sub_round"
            msg = f"[{cat}] event={eid} div={div!r} person={canon!r} places={places} (division round hint)"
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # Roundy division (net/freestyle/shred/etc.) + small place spread → INFO
        if _division_roundy(div) and _small_place_spread(places):
            cat = "sub_round"
            msg = f"[{cat}] event={eid} div={div!r} person={canon!r} places={places} (roundy-division heuristic net/freestyle/shred etc.)"
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # If division label strongly suggests multi-round or concatenation, don't WARN.
        # Treat as expected sub_round behavior for historical mirror flattening.
        if _div_suggests_rounds_or_concat(div):
            msg = (
                f"[sub_round] event={eid} div={div!r} person={canon!r} places={places} "
                "(division label suggests rounds/request/singles or concatenated header)"
            )
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # Secondary: if spread isn't huge, assume RR/pool-play style flattening in historical data
        if _place_spread(places) <= 12:
            msg = (
                f"[sub_round] event={eid} div={div!r} person={canon!r} places={places} "
                "(modest place spread; likely pool-play / multi-phase standings flattened)"
            )
            info_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})
            continue

        # Unexplained: places far apart, no pool-play hint → WARN
        msg = (
            f"Same person multiple places (far apart, no pool-play hint): "
            f"event={eid} div={div!r} person={canon!r} places={places}"
        )
        warn_rows.append({"event_id": eid, "division_canon": div, "person_canon": canon, "places": str(places), "message": msg})

    if info_rows:
        _info(f"Expected multi-place (sub_round/adjacent_podium): {len(info_rows)}")
        _write_report_and_cap(info_rows, "same_person_multi_place_info", level="INFO")
    if warn_rows:
        _warn(f"Unexplained same-person multi-place (far apart, no pool hint): {len(warn_rows)}")
        _write_report_and_cap(warn_rows, "same_person_multi_place_warn", level="WARN")
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

        # 6c — Long career, very few placements: WARN only if super extreme (span ≥ 20 and 1 placement)
        span_years = last_year - first_year
        if span_years >= 20 and total_pl is not None and total_pl == 1:
            _warn(
                f"Long career / sparse placements: {canon!r} "
                f"({first_year}–{last_year}, {total_pl} placement(s)) — possible data artifact"
            )
            sparse_career += 1
        elif span_years > 15 and total_pl is not None and total_pl < 3:
            _info(
                f"Long career / sparse placements: {canon!r} "
                f"({first_year}–{last_year}, {total_pl} placement(s)) — interesting anomaly"
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
        _info(f"Long-career / sparse-placement anomalies: {sparse_career} (WARN only for span≥20 and 1 placement)")
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
