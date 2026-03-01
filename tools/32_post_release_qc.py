#!/usr/bin/env python3
"""
32_post_release_qc.py — Six post-release data-integrity checks.

Reads the canonical pipeline outputs and reports issues.
Read-only; never modifies any file.

Checks:
  1. placements_count_reconcile   : Index stub counts vs PF; stage2↔PF discrepancy summary
  2. medal_count_consistency      : recomputed wins/podiums/events vs Person_Stats sheet
  3. duplicate_event_div_place    : same person_id at same (event, division, place)
  4. index_event_coverage         : every stage2 event in Index once; PF-only stubs present
  5. birth_year_validation        : placements before person's birth year (skipped: no birth_year)
  6. zero_participant_divisions   : events/divisions with 0 parsed placements

Inputs (must all exist):
  out/stage2_canonical_events.csv
  out/Placements_Flat.csv
  out/Coverage_ByEventDivision.csv
  Footbag_Results_Canonical.xlsx  (Index, Person_Stats sheets)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"
XLSX = ROOT / "Footbag_Results_Canonical.xlsx"

SCE_CSV = OUT / "stage2_canonical_events.csv"
PF_CSV  = OUT / "Placements_Flat.csv"
COV_CSV = OUT / "Coverage_ByEventDivision.csv"

SAFE_COVERAGE_FLAGS = {"complete", "mostly_complete"}


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


# ── check 1 ────────────────────────────────────────────────────────────────

def check_placements_count(
    sce: pd.DataFrame, pf: pd.DataFrame, index_df: pd.DataFrame
) -> int:
    """
    1a. Verify that synthetic_pre_mirror stubs in the Index have correct
        placements_count (should equal PF row count for that event_id).

    1b. Summary: overall stage2-parsed vs PF row count. Stage2 > PF is
        always expected in identity-lock mode (PF only contains resolved persons).
        Report significant per-event discrepancies (PF count < 50% of stage2 count)
        only when stage2 count is meaningful (≥5 parsed entries).

    Events with 0 PF rows but >0 stage2 entries are flagged only if
    stage2 placements look like real person names (not score/noise text).
    """
    _banner("CHECK 1 · Placements-count reconciliation (Index stubs + stage2↔PF)")

    issues = 0

    # 1a — Synthetic stub counts
    if "event_source" in index_df.columns:
        stubs = index_df[index_df["event_source"] == "synthetic_pre_mirror"]
        pf_counts = pf.groupby("event_id").size()
        for _, row in stubs.iterrows():
            eid     = str(row["event_id"])
            idx_cnt = _as_int(row.get("placements_count", None))
            pf_cnt  = int(pf_counts.get(int(eid), 0)) if eid.isdigit() else 0
            if idx_cnt is None or idx_cnt != pf_cnt:
                _error(f"stub event {eid}: Index placements_count={idx_cnt} ≠ PF rows={pf_cnt}")
                issues += 1
        if not issues:
            _ok(f"All {len(stubs)} synthetic stubs have correct placements_count in Index")
    else:
        _warn("Index sheet missing 'event_source' column — cannot check stub counts")
        issues += 1

    # 1b — stage2 vs PF summary
    total_s2  = 0
    events_with_zero_pf = []
    large_loss = []
    pf_counts  = pf.groupby("event_id").size()

    for _, row in sce.iterrows():
        eid = int(row["event_id"])
        try:
            pj = json.loads(row.get("placements_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            pj = []
        s2c = len(pj)
        total_s2 += s2c
        pfc = int(pf_counts.get(eid, 0))

        if pfc == 0 and s2c >= 5:
            # Only flag if first placement looks like a real name (not score noise)
            first_name = pj[0].get("player1_name", "") if pj else ""
            has_digit_prefix = bool(first_name) and first_name[0].isdigit()
            is_obvious_noise = any(
                kw in first_name.lower()
                for kw in ("drops", "place:", "nd", "rd", "first", "second", "years", "v1")
            )
            if not has_digit_prefix and not is_obvious_noise:
                events_with_zero_pf.append((str(eid), s2c, first_name))

        # Flag events where PF has < 40% of stage2 count and stage2 has ≥10 entries
        if s2c >= 10 and pfc < s2c * 0.4:
            large_loss.append((str(eid), s2c, pfc))

    pf_total = len(pf)
    _info(f"stage2 total parsed placements:  {total_s2}")
    _info(f"Placements_Flat total rows:       {pf_total}")
    _info(f"Resolved fraction: {pf_total/total_s2*100:.1f}% of stage2 entries in PF "
          f"(identity-lock mode; remainder = unresolved persons or noise)")

    for eid, s2c, name in events_with_zero_pf:
        _warn(f"event {eid}: {s2c} stage2 entries but 0 PF rows (first name: {name!r})")
        issues += 1

    if large_loss:
        _info(f"Events with <40% of stage2 placements in PF (≥10 stage2 entries): {len(large_loss)}")
        for eid, s2c, pfc in sorted(large_loss, key=lambda x: x[1]/max(x[2],1), reverse=True)[:5]:
            _info(f"  event {eid}: stage2={s2c}, PF={pfc} ({pfc/s2c*100:.0f}%)")
    else:
        _ok("No events with <40% resolution rate (among events with ≥10 stage2 placements)")

    return issues


# ── check 2 ────────────────────────────────────────────────────────────────

def check_medal_counts(
    pf: pd.DataFrame, person_stats_df: pd.DataFrame, cov_df: pd.DataFrame
) -> int:
    """
    Recompute wins/podiums/events_competed from Placements_Flat using the same
    gate as 04_build_analytics (coverage complete/mostly_complete per
    Coverage_ByEventDivision.csv — not the stale embedded PF flag).

    Also verifies internal consistency: wins ≤ podiums ≤ placements_with_numeric_place.
    """
    _banner("CHECK 2 · Medal-count consistency (recomputed vs Person_Stats)")

    issues = 0

    # --- Apply fresh coverage flags from Coverage_ByEventDivision (same as 04) ---
    # The embedded coverage_flag in PF/PBP can be stale from a prior run; 04 always
    # recomputes coverage fresh.  We replicate that here so our recomputed counts
    # match Person_Stats exactly.
    _fresh = (
        cov_df[["event_id", "division_canon", "coverage_flag"]]
        .drop_duplicates()
        .copy()
    )
    _fresh["event_id"] = _fresh["event_id"].astype(str)

    pf_fresh = pf.copy()
    pf_fresh["event_id"] = pf_fresh["event_id"].astype(str)
    pf_fresh = pf_fresh.drop(columns=["coverage_flag"], errors="ignore")
    pf_fresh = pf_fresh.merge(_fresh, on=["event_id", "division_canon"], how="left")
    pf_fresh["coverage_flag"] = pf_fresh["coverage_flag"].fillna("")

    # --- Recompute from PF ---
    filtered = pf_fresh[
        pf_fresh["coverage_flag"].isin(SAFE_COVERAGE_FLAGS) &
        (pf_fresh["person_unresolved"].fillna("").astype(str).str.strip() != "true")
    ].copy()
    filtered["place_int"] = pd.to_numeric(filtered["place"], errors="coerce")
    filtered["is_win"]    = filtered["place_int"] == 1
    filtered["is_podium"] = filtered["place_int"].isin([1, 2, 3])

    recomputed = (
        filtered.groupby(["person_id", "person_canon"], dropna=False)
        .agg(
            events_competed = ("event_id", "nunique"),
            placements_total= ("place", "count"),
            wins            = ("is_win",    "sum"),
            podiums         = ("is_podium", "sum"),
        )
        .reset_index()
    )
    recomputed["person_id"] = recomputed["person_id"].fillna("").astype(str)
    recomputed["wins"]      = recomputed["wins"].astype(int)
    recomputed["podiums"]   = recomputed["podiums"].astype(int)

    # --- Compare to Person_Stats ---
    ps = person_stats_df.copy()
    ps["person_id"] = ps["person_id"].fillna("").astype(str)
    ps_by_id = ps.set_index("person_id")

    mismatch_count = 0
    checked        = 0
    for _, row in recomputed.iterrows():
        pid = str(row["person_id"])
        if pid not in ps_by_id.index:
            continue  # excluded by Gate 3 or not in Person_Stats for other reasons
        ps_row = ps_by_id.loc[pid]
        checked += 1
        for col in ("wins", "podiums", "events_competed"):
            expected = int(row[col])
            actual   = _as_int(ps_row.get(col))
            if actual is None:
                continue
            if expected != actual:
                name = str(row["person_canon"])[:40]
                _error(f"{name!r} ({pid[:8]}): {col} expected={expected} actual={actual}")
                mismatch_count += 1
                if mismatch_count >= 20:
                    _warn("(truncated after 20 mismatches)")
                    issues += mismatch_count
                    return issues

    if mismatch_count == 0:
        _ok(f"All {checked} Person_Stats rows match recomputed wins/podiums/events_competed")
    else:
        issues += mismatch_count
        print(f"  → {mismatch_count} mismatch(es)")

    # --- Internal consistency ---
    internal = 0
    for _, row in ps.iterrows():
        w  = _as_int(row.get("wins"))
        p  = _as_int(row.get("podiums"))
        pt = _as_int(row.get("placements_with_numeric_place"))
        if None in (w, p, pt):
            continue
        if w > p:
            _error(f"{row.get('person_canon','?')!r}: wins({w}) > podiums({p})")
            internal += 1
        if p > pt:
            _error(f"{row.get('person_canon','?')!r}: podiums({p}) > placements_with_numeric_place({pt})")
            internal += 1
    if internal == 0:
        _ok(f"Person_Stats internal consistency: wins ≤ podiums ≤ placements_with_numeric_place (all {len(ps)} rows)")
    issues += internal

    return issues


# ── check 3 ────────────────────────────────────────────────────────────────

def check_duplicate_triples(pf: pd.DataFrame) -> int:
    """
    Find rows where the same person_id appears at the same (event_id, division_canon, place).
    These are true data errors distinct from expected pool-play / tie situations.

    Known categories:
      - Identity collision: UUID mapped to 2+ different person_canons
      - Sub-round duplicates: circle/pool events where same person appears in multiple rounds
    """
    _banner("CHECK 3 · Duplicate (event, division, place) triples for same person_id")

    pf_with_id = pf[
        pf["person_id"].notna() &
        (pf["person_id"].astype(str).str.strip() != "")
    ].copy()
    pf_num = pf_with_id[pd.to_numeric(pf_with_id["place"], errors="coerce").notna()].copy()

    true_dupes = (
        pf_num.groupby(["event_id", "division_canon", "place", "person_id"])
        .size()
    )
    true_dupes = true_dupes[true_dupes > 1]

    if len(true_dupes) == 0:
        _ok("No true duplicate placements (same person_id at same event/division/place)")
        collisions = 0
        sub_rounds = 0
    else:
        # Categorize: same person_id → multiple distinct canons = identity collision
        id_to_canons = (
            pf_num.groupby("person_id")["person_canon"]
            .apply(lambda s: set(s.dropna().astype(str)))
            .to_dict()
        )
        collisions  = 0
        sub_rounds  = 0
        for (eid, div, pl, pid), count in true_dupes.items():
            canons = id_to_canons.get(str(pid), {str(pid)})
            is_collision = len(canons) > 1
            cat = "IDENTITY_COLLISION" if is_collision else "sub_round"
            sev = _error if is_collision else _warn
            canon_str = " / ".join(sorted(canons)[:3])
            sev(f"[{cat}] event={eid} div='{div}' place={pl}: {canon_str!r} ×{count}")
            if is_collision:
                collisions += 1
            else:
                sub_rounds += 1

        _info(f"Identity collisions (UUID → >1 canon): {collisions}")
        _info(f"Sub-round duplicates (same person, multiple rounds): {sub_rounds}")
        print(f"  → {len(true_dupes)} duplicate(s) total ({collisions} errors, {sub_rounds} expected)")

    if collisions == 0 and sub_rounds > 0:
        _ok(f"No identity collisions; {sub_rounds} sub-round duplicates (expected pool/circle/round-robin data)")

    # Pool-play / tie summary (informational).
    # Multiple distinct players sharing the same (event, div, place) is expected in footbag:
    #   • Circle Contest / 2-Square: groups of 4-8 compete simultaneously; each group has
    #     its own place=1, place=2, … so the same numeric place repeats across groups.
    #   • Pool/round-robin play: separate pools each produce their own rank sequence.
    #   • True score ties: Shred 30, Golf, Consecutive — identical scores yield shared place.
    # None of these represent data errors; all placements are preserved as-is.
    pf_player = pf_num[pf_num["competitor_type"] == "player"]
    tie_groups_all = pf_player.groupby(["event_id", "division_canon", "place"])["person_id"].nunique()
    ties = tie_groups_all[tie_groups_all > 1]

    # Categorise by broad division type for transparency.
    _CIRCLE_KW  = ("circle", "circ")
    _SQUARE_KW  = ("2-square", "2 square", "2square")
    _STYLE_KW   = ("shred", "sick", "battle", "routines", "freestyle", "request",
                   "ironman", "combo", "routine")
    _GOLF_KW    = ("golf",)

    def _cat(div: str) -> str:
        d = div.lower()
        if any(k in d for k in _CIRCLE_KW):   return "circle/group"
        if any(k in d for k in _SQUARE_KW):   return "2-square/group"
        if any(k in d for k in _GOLF_KW):     return "golf (score tie)"
        if any(k in d for k in _STYLE_KW):    return "freestyle (score tie)"
        return "net/pool-play"

    tie_df = ties.reset_index()
    tie_df["cat"] = tie_df["division_canon"].apply(_cat)
    cat_counts = tie_df["cat"].value_counts()
    events_affected = tie_df["event_id"].nunique()

    _info(
        f"(event,div,place) with >1 distinct players (ties/pool-play, expected): "
        f"{len(ties)} groups across {events_affected} events"
    )
    for cat, cnt in cat_counts.items():
        _info(f"    {cnt:4d}  {cat}")
    _info(
        "  All shared-place rows are preserved; no data dropped. "
        "Known limitation: sub-group context (pool/circle ID) is not available in source."
    )

    # Only IDENTITY_COLLISION errors count toward exit code; sub_rounds are WARNs only
    return collisions


# ── check 4 ────────────────────────────────────────────────────────────────

def check_index_coverage(
    sce: pd.DataFrame, pf: pd.DataFrame, index_df: pd.DataFrame
) -> int:
    """
    Verify:
      - Every stage2 event_id appears in Index exactly once
      - Every PF-only event_id (synthetic pre-mirror) appears in Index exactly once
      - No spurious event_ids in Index (not in stage2 or PF)
      - Index total rows = stage2_count + pf_only_count
    """
    _banner("CHECK 4 · Index ↔ stage2 / PF event coverage")

    from collections import Counter

    sce_eids = set(sce["event_id"].astype(str))
    pf_eids  = set(pf["event_id"].astype(str))
    idx_list = index_df["event_id"].astype(str).tolist()
    idx_set  = set(idx_list)
    idx_counts = Counter(idx_list)

    issues = 0

    # Duplicates in Index
    dupes = {e: c for e, c in idx_counts.items() if c > 1}
    if dupes:
        for e, c in sorted(dupes.items()):
            _error(f"event_id {e} appears {c}× in Index (expected 1)")
            issues += 1
    else:
        _ok("No duplicate event_ids in Index")

    # stage2 events missing from Index
    missing = sce_eids - idx_set
    if missing:
        for e in sorted(missing):
            _error(f"stage2 event {e} missing from Index")
            issues += 1
    else:
        _ok(f"All {len(sce_eids)} stage2 events present in Index")

    # PF-only stubs missing from Index
    pf_only = pf_eids - sce_eids
    missing_stubs = pf_only - idx_set
    if missing_stubs:
        for e in sorted(missing_stubs):
            _error(f"PF-only event {e} missing from Index (should be a stub)")
            issues += 1
    else:
        _ok(f"All {len(pf_only)} PF-only (pre-mirror) events present as stubs in Index")

    # Spurious events in Index
    spurious = idx_set - sce_eids - pf_eids
    if spurious:
        for e in sorted(spurious):
            _error(f"event {e} in Index but not in stage2 or PF")
            issues += 1
    else:
        _ok("No spurious event_ids in Index")

    # Row count
    expected = len(sce_eids) + len(pf_only)
    actual   = len(idx_list)
    if actual == expected:
        _ok(f"Index total rows: {actual} = {len(sce_eids)} stage2 + {len(pf_only)} stubs")
    else:
        _warn(f"Index rows: {actual} (expected {expected})")

    if "event_source" in index_df.columns:
        stubs = index_df[index_df["event_source"] == "synthetic_pre_mirror"]
        _info(f"Synthetic pre-mirror stubs in Index: {len(stubs)}")

    return issues


# ── check 5 ────────────────────────────────────────────────────────────────

def check_birth_year(pt_path: Path) -> int:
    """Check for placements before birth year. Skipped: PT has no birth_year column."""
    _banner("CHECK 5 · Birth-year validation")

    if not pt_path.exists():
        _info("Persons_Truth not found — skipping")
        return 0

    pt = pd.read_csv(pt_path, low_memory=False)
    if "birth_year" not in pt.columns:
        _info("Persons_Truth has no birth_year column — check not applicable")
        return 0

    # Future-proof: if birth_year is ever added, this path runs
    pf = pd.read_csv(PF_CSV, low_memory=False)
    pt_by = pt[["effective_person_id", "birth_year"]].dropna()
    merged = pf.merge(
        pt_by, left_on="person_id", right_on="effective_person_id", how="inner"
    )
    bad = merged[pd.to_numeric(merged["year"], errors="coerce") <
                 pd.to_numeric(merged["birth_year"], errors="coerce")]
    if len(bad) == 0:
        _ok("No placements before birth year")
    else:
        for _, row in bad.head(10).iterrows():
            _error(f"{row.get('person_canon','?')!r}: placement year={row['year']} < birth_year={row['birth_year']}")
    return len(bad)


# ── check 6 ────────────────────────────────────────────────────────────────

def check_zero_participant_divisions(
    sce: pd.DataFrame, cov: pd.DataFrame
) -> int:
    """
    6a. Coverage_ByEventDivision rows with placements_present == 0 (should be impossible).
    6b. stage2 events with 0 parsed placements — informational (known limitations:
        events on the website without published results).
    """
    _banner("CHECK 6 · Zero-participant / empty divisions")

    issues = 0

    # 6a: Coverage entries with placements_present == 0 (structural error)
    zero_cov = cov[cov["placements_present"] == 0]
    if len(zero_cov):
        for _, row in zero_cov.iterrows():
            _error(
                f"Coverage entry with placements_present=0: "
                f"event {row['event_id']} div '{row['division_canon']}'"
            )
            issues += 1
    else:
        _ok("No Coverage entries with placements_present=0")

    # 6b: stage2 events with 0 placements (informational — known limitation)
    empty_events = []
    for _, row in sce.iterrows():
        try:
            pj = json.loads(row.get("placements_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            pj = []
        if len(pj) == 0:
            empty_events.append((str(row["event_id"]), str(row.get("event_name", "?"))))

    _info(
        f"stage2 events with 0 parsed placements (no results published on mirror): "
        f"{len(empty_events)}"
    )
    if empty_events:
        for eid, name in empty_events[:5]:
            _info(f"  event {eid}: {name!r}")
        if len(empty_events) > 5:
            _info(f"  … and {len(empty_events) - 5} more")

    # Informational: single-placement divisions
    single = cov[cov["placements_present"] == 1]
    _info(f"Divisions with exactly 1 placement (single-entry, all 'complete'): {len(single)}")

    return issues


# ── main ──────────────────────────────────────────────────────────────────

def main() -> None:
    required = [SCE_CSV, PF_CSV, COV_CSV, XLSX]
    missing  = [p for p in required if not p.exists()]
    if missing:
        for p in missing:
            print(f"ERROR: Missing input: {p}", file=sys.stderr)
        sys.exit(1)

    print("=" * 70)
    print("POST-RELEASE QC  —  6 data-integrity checks")
    print("=" * 70)
    print()
    print("Loading data…")

    sce  = pd.read_csv(SCE_CSV, low_memory=False)
    pf   = pd.read_csv(PF_CSV,  low_memory=False)
    cov  = pd.read_csv(COV_CSV, low_memory=False)
    xl   = pd.ExcelFile(XLSX)
    idx  = pd.read_excel(xl, "Index")
    ps   = pd.read_excel(xl, "Person_Stats")
    pt_p = OUT / "Persons_Truth.csv"

    print(f"  stage2 events:  {len(sce)}")
    print(f"  Placements_Flat: {len(pf)} rows")
    print(f"  Coverage:       {len(cov)} event/division pairs")
    print(f"  Index rows:     {len(idx)}")
    print(f"  Person_Stats:   {len(ps)} rows")

    total = 0
    total += check_placements_count(sce, pf, idx)
    total += check_medal_counts(pf, ps, cov)
    total += check_duplicate_triples(pf)
    total += check_index_coverage(sce, pf, idx)
    total += check_birth_year(pt_p)
    total += check_zero_participant_divisions(sce, cov)

    _banner("SUMMARY")
    if total == 0:
        print("  ✓  All 6 checks passed — no issues found")
    else:
        print(f"  ✗  {total} issue(s) across all checks")
        sys.exit(1)


if __name__ == "__main__":
    main()
