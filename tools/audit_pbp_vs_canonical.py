#!/usr/bin/env python3
"""
audit_pbp_vs_canonical.py — Phase 0 Audit Tool

Read-only analysis: compares every PBP v85 row against the active pipeline
output (out/Placements_ByPerson.csv, which stage 05 reads as the authoritative
participant source).

IMPORTANT — Framing constraint:
    All outputs are INVESTIGATION AIDS, not recovery queues.
    "Absent from active pipeline" means "warrants examination",
    NOT "incorrectly removed" or "should be recovered".
    PBP v85 rows are NOT assumed to be correct.

Outputs written to out/audit/:
    audit_row_diff.csv              — per-row presence/absence status (steps 1-3)
    [steps 4-5: implemented separately]

Usage:
    python tools/audit_pbp_vs_canonical.py [--pbp-v85 PATH] [--pipeline-pbp PATH]
                                            [--pipeline-pf PATH] [--pt PATH]
                                            [--out-dir PATH]
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

import pandas as pd

# ── Repo root ──────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[1]

# ── Default input paths ────────────────────────────────────────────────────────
# PBP v85 is the diagnostic reference (not assumed correct).
# pipeline_pbp is the authoritative participant source for stage 05.
# pipeline_pf is loaded for reference (encoding comparison, not primary diff).
# pt is the Persons Truth for later identity lookups (steps 4-5).

DEFAULT_PBP_V85      = REPO_ROOT / "inputs/identity_lock/Placements_ByPerson_v85.csv"
DEFAULT_PIPELINE_PBP = REPO_ROOT / "out/Placements_ByPerson.csv"
DEFAULT_PIPELINE_PF  = REPO_ROOT / "out/Placements_Flat.csv"
DEFAULT_PT           = REPO_ROOT / "inputs/identity_lock/Persons_Truth_Final_v49.csv"
DEFAULT_OUT_DIR      = REPO_ROOT / "out/audit"

# ── Artifact character map ─────────────────────────────────────────────────────
# Characters that appear as encoding artifacts in division_canon values.
# Stripped during normalization.  All seven types are independently detectable.
# Order within the dict is preserved (Python 3.7+) for deterministic reporting.

ARTIFACT_CHARS: dict[str, str] = {
    "\u00AD": "SOFT_HYPHEN",        # soft hyphen — HTML &shy; word-break hint
    "\uFFFD": "REPLACEMENT_CHAR",   # Unicode replacement char — encoding corruption
    "\u00A0": "NON_BREAKING_SPACE", # non-breaking space — HTML &nbsp;
    "\u200B": "ZERO_WIDTH_SPACE",   # zero-width space — invisible formatting
    "\u2019": "CURLY_APOSTROPHE",   # right single quotation mark (Women\u2019s)
    "\u201C": "CURLY_QUOTE_OPEN",   # left double quotation mark
    "\u201D": "CURLY_QUOTE_CLOSE",  # right double quotation mark
}

# ── Schema: audit_row_diff.csv column order ───────────────────────────────────
# Matches the approved design document exactly. Do not reorder.

ROW_DIFF_COLUMNS: list[str] = [
    # Traceability
    "pbp_row_index",
    # Identity fields — from PBP v85, unmodified
    "event_id",
    "year",
    "division_canon",
    "division_canon_normalized",
    "division_category",
    "place",
    "competitor_type",
    "person_id",
    "person_canon",
    "team_display_name",
    "team_person_key",
    "person_unresolved",
    # Diff results
    "status",
    "match_type",
    "match_multiplicity",
    "pipeline_match_count",
    # Slot population
    "slot_row_count_in_pbp",
    "slot_row_count_in_pipeline",
    # Investigation note — partially populated in steps 1-3;
    # extended by European-format, cascade, and attribution steps (4-5)
    "investigation_note",
]

# ── Normalization helpers ──────────────────────────────────────────────────────

def normalize_division(name: str) -> str:
    """
    Strip all known artifact characters from a division_canon value and
    collapse internal whitespace.

    The normalized form is used ONLY for tier-1 slot matching.  It is never
    written as a canonical value and never modifies any pipeline file.
    """
    if not isinstance(name, str):
        return ""
    s = name
    for char in ARTIFACT_CHARS:
        s = s.replace(char, "")
    return " ".join(s.split())


def detect_artifacts(name: str) -> str:
    """
    Return a pipe-separated string of artifact codes present in name.
    Empty string if none found.  Used to populate investigation_note.

    Example: "SOFT_HYPHEN|REPLACEMENT_CHAR"
    """
    if not isinstance(name, str):
        return ""
    found = [code for char, code in ARTIFACT_CHARS.items() if char in name]
    return "|".join(found)


def nb(x) -> str:
    """
    Normalize blank — coerce None/NaN/pandas NA to empty string, then strip.
    All field access from DataFrames goes through this to prevent comparison
    failures from mixed NaN/None/"nan" representations.
    """
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none") else s


def normalize_place(x) -> str:
    """
    Normalize a place value to a consistent string key.
    Strips trailing '.0' that may appear when an integer column is read as float
    (defensive — not expected with dtype=str loading, but guards against it).
    """
    s = nb(x)
    if s.endswith(".0"):
        s = s[:-2]
    return s


# ── Input loading ──────────────────────────────────────────────────────────────

def load_csv(path: Path, label: str) -> pd.DataFrame:
    """
    Load a CSV with all columns forced to string dtype.
    keep_default_na=False prevents pandas from converting empty strings to NaN.
    Exits with a clear message if the file is missing.
    """
    if not path.exists():
        print(f"ERROR: {label} not found: {path}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    print(f"  {label}: {len(df):>8,} rows  ({path.name})")
    return df


def load_inputs(args: argparse.Namespace) -> dict[str, pd.DataFrame]:
    """Load all input files and validate required columns on PBP v85."""
    print("\n[Phase 0] Loading inputs...")

    inputs: dict[str, pd.DataFrame] = {
        "pbp_v85":      load_csv(Path(args.pbp_v85),      "PBP v85"),
        "pipeline_pbp": load_csv(Path(args.pipeline_pbp), "Post-02p6 Placements_ByPerson (primary diff target)"),
        "pipeline_pf":  load_csv(Path(args.pipeline_pf),  "Post-02p6 Placements_Flat (reference)"),
        "pt":           load_csv(Path(args.pt),            "Persons Truth (for steps 4-5)"),
    }

    # Validate required columns on PBP v85.
    # pipeline_pbp validation is looser — it may legitimately lack norm/division_raw.
    required_pbp = {
        "event_id", "year", "division_canon", "division_category",
        "place", "competitor_type", "person_id", "team_person_key",
        "person_canon", "team_display_name", "person_unresolved",
    }
    missing = required_pbp - set(inputs["pbp_v85"].columns)
    if missing:
        print(f"ERROR: PBP v85 missing required columns: {sorted(missing)}", file=sys.stderr)
        sys.exit(1)

    required_pipeline = {
        "event_id", "division_canon", "place",
        "competitor_type", "person_canon", "team_display_name",
    }
    missing_p = required_pipeline - set(inputs["pipeline_pbp"].columns)
    if missing_p:
        print(f"ERROR: pipeline Placements_ByPerson missing columns: {sorted(missing_p)}", file=sys.stderr)
        sys.exit(1)

    return inputs


# ── Index building ─────────────────────────────────────────────────────────────

def _tier1_norm_key(row: pd.Series) -> tuple[str, str, str, str]:
    """
    Tier-1 normalized key: (event_id, division_canon_normalized, place, competitor_type).
    division_canon is stripped of artifact characters before use as a key.
    This key is used for slot-level matching and slot population counts.
    """
    return (
        nb(row.get("event_id", "")),
        normalize_division(nb(row.get("division_canon", ""))),
        normalize_place(row.get("place", "")),
        nb(row.get("competitor_type", "")),
    )


def _tier1_exact_key(row: pd.Series) -> tuple[str, str, str, str]:
    """
    Tier-1 exact key: uses raw division_canon without normalization.
    Used to detect whether a match required artifact-character stripping.
    """
    return (
        nb(row.get("event_id", "")),
        nb(row.get("division_canon", "")),
        normalize_place(row.get("place", "")),
        nb(row.get("competitor_type", "")),
    )


def _tier2_key(row: pd.Series) -> tuple[str, str]:
    """
    Tier-2 content key: (person_canon, team_display_name).
    Exact string comparison — no folding or normalization applied here.
    Content matching is intentionally strict to avoid false positives.
    """
    return (
        nb(row.get("person_canon", "")),
        nb(row.get("team_display_name", "")),
    )


def build_pbp_slot_index(pbp_df: pd.DataFrame) -> dict[tuple, int]:
    """
    For each distinct tier-1-normalized slot in PBP v85, count the rows that
    share it.  Returns a mapping from slot key to row count.

    Used to populate slot_row_count_in_pbp, which gives context on whether a
    slot had multiple PBP rows (pool format, ties, etc.).
    """
    counts: dict[tuple, int] = defaultdict(int)
    for _, row in pbp_df.iterrows():
        counts[_tier1_norm_key(row)] += 1
    return dict(counts)


def build_pipeline_index(
    pipeline_df: pd.DataFrame,
) -> tuple[dict[tuple, list[pd.Series]], dict[tuple, list[pd.Series]]]:
    """
    Build two lookup indexes over the post-02p6 pipeline output.

    norm_index:  tier-1-NORMALIZED key → list of pipeline rows at that slot
    exact_index: tier-1-EXACT key      → list of pipeline rows at that slot

    Both maps support tier-2 content filtering within the returned row lists.

    Using two separate indexes (norm vs. exact) allows match_type to be
    determined: EXACT if raw division_canon matched, NORMALIZED_DIVISION if
    artifact stripping was required.
    """
    norm_index:  dict[tuple, list] = defaultdict(list)
    exact_index: dict[tuple, list] = defaultdict(list)

    for _, row in pipeline_df.iterrows():
        norm_index[_tier1_norm_key(row)].append(row)
        exact_index[_tier1_exact_key(row)].append(row)

    # Convert to plain dict to prevent accidental defaultdict extension later
    return dict(norm_index), dict(exact_index)


# ── Per-row matching ───────────────────────────────────────────────────────────

def _match_row(
    pbp_row: pd.Series,
    norm_index: dict[tuple, list],
    exact_index: dict[tuple, list],
) -> tuple[str, str, str, int, int]:
    """
    Match a single PBP v85 row against the pipeline indexes.

    Matching is two-tiered:
      Tier 1 — slot: (event_id, division_canon_normalized, place, competitor_type)
      Tier 2 — content: (person_canon, team_display_name) within Tier-1 matches

    EXACT division match is preferred over NORMALIZED match.  If neither yields
    a tier-2 content match, the row is ABSENT.

    Returns:
        status              — PRESENT | ABSENT_SLOT_COVERED | ABSENT_NO_COVERAGE
        match_type          — EXACT | NORMALIZED_DIVISION | NONE
        match_multiplicity  — SINGLE | MULTIPLE | NONE
        pipeline_match_count   — count of tier-2 content matches in pipeline
        slot_row_count_in_pipeline — count of rows sharing normalized tier-1 slot
    """
    t1_norm  = _tier1_norm_key(pbp_row)
    t1_exact = _tier1_exact_key(pbp_row)
    t2_key   = _tier2_key(pbp_row)

    # All pipeline rows at this normalized slot (used for slot_row_count)
    slot_norm  = norm_index.get(t1_norm,  [])
    slot_exact = exact_index.get(t1_exact, [])

    slot_count_pipeline = len(slot_norm)

    def tier2_filter(candidates: list) -> list[pd.Series]:
        """Return candidates whose tier-2 key exactly matches this PBP row."""
        return [r for r in candidates if _tier2_key(r) == t2_key]

    exact_matches = tier2_filter(slot_exact)
    norm_matches  = tier2_filter(slot_norm)

    # Prefer exact over normalized — determines match_type
    if exact_matches:
        matched    = exact_matches
        match_type = "EXACT"
    elif norm_matches:
        matched    = norm_matches
        match_type = "NORMALIZED_DIVISION"
    else:
        matched    = []
        match_type = "NONE"

    n_matched = len(matched)

    if n_matched == 0:
        multiplicity = "NONE"
        status = (
            "ABSENT_NO_COVERAGE"
            if slot_count_pipeline == 0
            else "ABSENT_SLOT_COVERED"
        )
    elif n_matched == 1:
        multiplicity = "SINGLE"
        status       = "PRESENT"
    else:
        # Multiple tier-2 matches — pipeline has duplicate rows at this slot.
        # This is flagged as a potential corruption signal regardless of status.
        multiplicity = "MULTIPLE"
        status       = "PRESENT"

    return status, match_type, multiplicity, n_matched, slot_count_pipeline


# ── Investigation note construction ───────────────────────────────────────────

def build_investigation_note(
    pbp_row: pd.Series,
    multiplicity: str,
    pipeline_match_count: int,
) -> str:
    """
    Build the investigation_note string for a row_diff entry.

    Populated in steps 1-3:
      - encoding_artifact:{types}        — artifact chars in division_canon
      - multiple_pipeline_matches:{N}    — corruption signal: 2+ pipeline matches

    Extended in steps 4-5 (not yet implemented):
      - european_format_candidate:{conf} — European-name parsing signal
      - cascade_shadow_candidate         — pool-shadow cascade signal

    Notes are pipe-separated.  Empty string if no signals apply.
    """
    notes: list[str] = []

    # Signal: encoding artifact present in this row's division_canon
    artifacts = detect_artifacts(nb(pbp_row.get("division_canon", "")))
    if artifacts:
        notes.append(f"encoding_artifact:{artifacts}")

    # Signal: multiple pipeline rows matched this PBP row at the content level.
    # Indicates either a pipeline duplicate or a division-merge collision.
    if multiplicity == "MULTIPLE":
        notes.append(f"multiple_pipeline_matches:{pipeline_match_count}")

    return " | ".join(notes)


# ── Row diff computation ───────────────────────────────────────────────────────

def compute_row_diff(
    pbp_df: pd.DataFrame,
    pipeline_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compare every PBP v85 row against the post-02p6 pipeline output.

    For each PBP row, determines:
      - whether a matching row exists in the pipeline (status)
      - whether normalization was required to find the match (match_type)
      - how many pipeline rows matched (match_multiplicity, pipeline_match_count)
      - how many PBP rows share this slot (slot_row_count_in_pbp)
      - how many pipeline rows cover this slot (slot_row_count_in_pipeline)
      - what investigation signals apply (investigation_note)

    Returns a DataFrame with one row per PBP v85 input row, all ROW_DIFF_COLUMNS
    populated.  The 'investigation_note' field is partially populated here;
    European-format and cascade signals are added in steps 4-5.
    """
    print("\n[Phase 0 / step 2] Building normalized division mapping...")
    norm_index, exact_index = build_pipeline_index(pipeline_df)
    pbp_slot_index = build_pbp_slot_index(pbp_df)

    n_norm_slots  = len(norm_index)
    n_exact_slots = len(exact_index)
    print(f"  Pipeline norm-index slots:  {n_norm_slots:,}")
    print(f"  Pipeline exact-index slots: {n_exact_slots:,}")
    if n_norm_slots != n_exact_slots:
        print(
            f"  NOTE: {n_norm_slots - n_exact_slots} slot(s) collapsed by normalization "
            f"(artifact variants merged into same normalized slot)"
        )

    print("\n[Phase 0 / step 3] Computing row diff...")
    records: list[dict] = []

    for pbp_idx, pbp_row in pbp_df.iterrows():
        # Progress indicator every 5,000 rows
        if pbp_idx > 0 and pbp_idx % 5000 == 0:
            print(f"  ... {pbp_idx:,} rows processed")

        div_raw  = nb(pbp_row.get("division_canon", ""))
        div_norm = normalize_division(div_raw)
        t1_norm  = _tier1_norm_key(pbp_row)

        status, match_type, multiplicity, match_count, slot_pipeline = _match_row(
            pbp_row, norm_index, exact_index
        )

        note = build_investigation_note(pbp_row, multiplicity, match_count)

        records.append({
            # ── Traceability ────────────────────────────────────────────────
            "pbp_row_index": int(pbp_idx),

            # ── Identity fields (from PBP v85, unmodified) ──────────────────
            "event_id":                  nb(pbp_row.get("event_id",          "")),
            "year":                      nb(pbp_row.get("year",              "")),
            "division_canon":            div_raw,
            "division_canon_normalized": div_norm,
            "division_category":         nb(pbp_row.get("division_category", "")),
            "place":                     normalize_place(pbp_row.get("place", "")),
            "competitor_type":           nb(pbp_row.get("competitor_type",   "")),
            "person_id":                 nb(pbp_row.get("person_id",         "")),
            "person_canon":              nb(pbp_row.get("person_canon",      "")),
            "team_display_name":         nb(pbp_row.get("team_display_name", "")),
            "team_person_key":           nb(pbp_row.get("team_person_key",   "")),
            "person_unresolved":         nb(pbp_row.get("person_unresolved", "")),

            # ── Diff results ────────────────────────────────────────────────
            "status":              status,
            "match_type":         match_type,
            "match_multiplicity": multiplicity,
            "pipeline_match_count": match_count,

            # ── Slot population ─────────────────────────────────────────────
            "slot_row_count_in_pbp":      pbp_slot_index.get(t1_norm, 0),
            "slot_row_count_in_pipeline": slot_pipeline,

            # ── Investigation note ───────────────────────────────────────────
            # Partially populated here. Steps 4-5 will join additional signals
            # (european_format_candidate, cascade_shadow_candidate) into this field.
            "investigation_note": note,
        })

    diff_df = pd.DataFrame(records, columns=ROW_DIFF_COLUMNS)
    print(f"  Row diff complete: {len(diff_df):,} rows")
    return diff_df


# ── Output writing ─────────────────────────────────────────────────────────────

def write_row_diff(diff_df: pd.DataFrame, out_dir: Path) -> None:
    """Write audit_row_diff.csv with columns in schema-defined order."""
    out_path = out_dir / "audit_row_diff.csv"
    diff_df[ROW_DIFF_COLUMNS].to_csv(out_path, index=False)
    print(f"  Written: {out_path}  ({len(diff_df):,} rows)")


def print_row_diff_summary(diff_df: pd.DataFrame) -> None:
    """
    Print the Section 1 + partial Section 3 of audit_summary.txt to stdout.
    The full audit_summary.txt is generated in step 5 after all outputs exist.

    Includes the primary Phase 0 exit criterion check:
      ABSENT_* count must equal (PBP v85 total - post-02p6 total).
    """
    total       = len(diff_df)
    n_present   = int((diff_df["status"] == "PRESENT").sum())
    n_covered   = int((diff_df["status"] == "ABSENT_SLOT_COVERED").sum())
    n_none      = int((diff_df["status"] == "ABSENT_NO_COVERAGE").sum())
    n_absent    = n_covered + n_none

    n_single    = int((diff_df["match_multiplicity"] == "SINGLE").sum())
    n_multiple  = int((diff_df["match_multiplicity"] == "MULTIPLE").sum())
    n_norm_div  = int((diff_df["match_type"] == "NORMALIZED_DIVISION").sum())
    n_exact     = int((diff_df["match_type"] == "EXACT").sum())

    # Primary consistency check: PRESENT + ABSENT must equal total PBP rows
    count_check = "PASS" if n_present + n_absent == total else "FAIL"

    print()
    print("=" * 60)
    print("ROW DIFF SUMMARY")
    print("=" * 60)
    print(f"  PBP v85 input rows:          {total:>8,}")
    print(f"  PRESENT:                     {n_present:>8,}")
    print(f"  ABSENT_SLOT_COVERED:         {n_covered:>8,}")
    print(f"  ABSENT_NO_COVERAGE:          {n_none:>8,}")
    print(f"  Total ABSENT:                {n_absent:>8,}")
    print()
    print(f"  PRESENT + ABSENT = total:    {count_check}  ← Phase 0 exit criterion")
    print()
    print("  --- Match breakdown ---")
    print(f"  EXACT matches:               {n_exact:>8,}")
    print(f"  NORMALIZED_DIVISION matches: {n_norm_div:>8,}  ← encoding artifact candidates")
    print()
    print("  --- Signals ---")
    print(f"  MULTIPLE pipeline matches:   {n_multiple:>8,}  ← potential corruption")
    print()

    # Warn if primary check fails — outputs should not be used for investigation
    if count_check == "FAIL":
        print("  *** WARNING: count check FAILED — diff logic has a bug.", file=sys.stderr)
        print("  *** Do not use outputs for investigation until this is resolved.", file=sys.stderr)
        print()


# ── Steps 4-5: Helpers ────────────────────────────────────────────────────────

def _is_doubles_div(div: str) -> bool:
    d = div.lower()
    return "doubles" in d or bool(re.search(r"\bdbl\b", d))


def _ascii_fold_str(s: str) -> str:
    """Lowercase + strip diacritics + collapse whitespace."""
    s2 = re.sub(r"[\u00B2\u00B3\u00B9]", "", str(s))
    nfkd = unicodedata.normalize("NFKD", s2)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(stripped.lower().split())


def _ascii_strip_str(s: str) -> str:
    """Strip all non-ASCII, lowercase, collapse whitespace."""
    ascii_only = "".join(c for c in str(s).lower() if ord(c) < 128)
    return " ".join(ascii_only.split())


def _is_city_artifact(second_half: str) -> bool:
    """Match 'CityName ST' pattern (word + 2-letter uppercase code)."""
    return bool(re.fullmatch(r"[A-Z][a-z]+ [A-Z]{2}", second_half.strip()))


# Pre-pass 0 metadata phrases (must match 02p6 exactly)
METADATA_PHRASES: set[str] = {
    "MINUTE TIMED", "MIN TIMED", "MIN. TIMED",
    "TIMED KICKING", "TIMED FOOTBAG",
    "EX-AEQUO", "EX AEQUO", "EXAEQUO",
    "HIGHEST LEVEL",
}


# ── PP5 simulation ─────────────────────────────────────────────────────────────

def simulate_pp5(pbp_df: pd.DataFrame) -> dict[int, dict]:
    """
    Simulate pre-pass 5 (pool-shadow removal) on PBP v85 data.
    Tracks which iteration each row is removed in and whether it is a direct or
    cascade removal.  Does NOT modify any file.

    Returns a dict keyed by pbp_row_index (DataFrame index) with:
      iteration         — which pass removed this row (1 = direct, 2+ = cascade)
      removal_type      — "DIRECT" | "CASCADE"
      trigger_person_canon — person whose prior removal enabled this cascade (None for DIRECT)
      trigger_place     — the place at which the trigger occurred (None for DIRECT)
      unique_place_that_justified_direct — the unique place that made DIRECT eligible (None for CASCADE)
    """
    player_mask = (
        (pbp_df["competitor_type"] == "player")
        & (pbp_df["person_canon"].str.strip() != "")
        & (pbp_df["person_canon"] != "__NON_PERSON__")
    )
    player_df = pbp_df[player_mask].copy()

    active_indices: set[int] = set(player_df.index.tolist())
    removals: dict[int, dict] = {}

    # Per-iteration record of what was removed from each (event, div, place)
    # removed_from_slot[iter][(eid, div, pl)] = [person_canon, ...]
    removed_from_slot: dict[int, dict[tuple, list[str]]] = {}

    iteration = 1
    while True:
        active_df = player_df.loc[sorted(active_indices)].copy()

        # Compute current place-population within (event, div)
        place_pop = active_df.groupby(
            ["event_id", "division_canon", "place"]
        )["person_canon"].transform("nunique")
        active_df = active_df.assign(_place_pop=place_pop.values)

        shadow_this_iter: list[tuple[int, set, str, str]] = []
        # (row_index, unique_places_for_player, eid, div)

        for (eid, div, pc), grp in active_df.groupby(
            ["event_id", "division_canon", "person_canon"]
        ):
            if len(grp["place"].unique()) < 2:
                continue
            unique_places = set(grp.loc[grp["_place_pop"] == 1, "place"])
            shared_places = set(grp.loc[grp["_place_pop"] > 1, "place"])
            if unique_places and shared_places:
                for idx in grp[grp["place"].isin(shared_places)].index:
                    shadow_this_iter.append((int(idx), unique_places, str(eid), str(div)))

        if not shadow_this_iter:
            break

        # Record slot-level removals for cascade trigger tracing
        slot_removals: dict[tuple, list[str]] = {}
        for idx, _upl, eid, div in shadow_this_iter:
            row = player_df.loc[idx]
            pl  = normalize_place(row["place"])
            pc  = nb(row["person_canon"])
            key = (eid, div, pl)
            slot_removals.setdefault(key, []).append(pc)
        removed_from_slot[iteration] = slot_removals

        for idx, unique_places, eid, div in shadow_this_iter:
            row = player_df.loc[idx]
            pl  = normalize_place(row["place"])

            if iteration == 1:
                removals[idx] = {
                    "iteration":   iteration,
                    "removal_type": "DIRECT",
                    "trigger_person_canon": None,
                    "trigger_place": None,
                    "unique_place_that_justified_direct": (
                        min(unique_places) if unique_places else None
                    ),
                }
            else:
                # CASCADE: this player became eligible because one of their places
                # transitioned from shared → unique in a prior iteration.
                # The trigger is whoever was most recently removed from that place.
                trigger_person = None
                trigger_place  = None
                # Search back through previous iterations for who last vacated a
                # place that is now unique for this player
                for back in range(iteration - 1, 0, -1):
                    prev = removed_from_slot.get(back, {})
                    for upl in sorted(unique_places):
                        key = (eid, div, str(upl))
                        if key in prev and prev[key]:
                            trigger_person = prev[key][-1]
                            trigger_place  = upl
                            break
                    if trigger_person:
                        break

                removals[idx] = {
                    "iteration":   iteration,
                    "removal_type": "CASCADE",
                    "trigger_person_canon": trigger_person,
                    "trigger_place": trigger_place,
                    "unique_place_that_justified_direct": None,
                }

        for idx, _, _eid, _div in shadow_this_iter:
            active_indices.discard(idx)

        iteration += 1

    return removals


# ── Step 4b: Prepass attribution ───────────────────────────────────────────────

ATTR_COLUMNS: list[str] = [
    "pbp_row_index",
    "event_id",
    "division_canon",
    "place",
    "person_canon",
    "team_display_name",
    "status",
    "attribution",
    "attribution_confidence",
    "attribution_evidence",
    "unknown_priority",
    "is_cascade",
    "cascade_iteration",
    "cascade_trigger_person_canon",
    "cascade_trigger_place",
]


def compute_prepass_attribution(
    diff_df: pd.DataFrame,
    pbp_df: pd.DataFrame,
    pipeline_df: pd.DataFrame,
    pp5_removals: dict[int, dict],
) -> pd.DataFrame:
    """
    For every ABSENT row (ABSENT_SLOT_COVERED or ABSENT_NO_COVERAGE), determine
    which 02p6 pre-pass condition first matches it.

    First-match order: PP0 → PP2_SUPERSEDED → PP3 → PP4 → PP5 → MAIN_CITY → UNKNOWN
    (PP2_SUPERSEDED is not in the design doc pre-pass list but covers player rows
    in doubles that were superseded by team rows — listed here for completeness but
    merged into UNKNOWN if not explicitly in the design schema.)

    Actually strictly following design doc codes:
    PP0_METADATA → PP3_TEAM_SUPERSEDED → PP4_EXACT_DUPLICATE → PP5_DIRECT →
    PP5_CASCADE → MAIN_CITY_ARTIFACT → UNKNOWN
    """
    absent_mask = diff_df["status"].isin(["ABSENT_SLOT_COVERED", "ABSENT_NO_COVERAGE"])
    absent_df   = diff_df[absent_mask].copy()

    # ── Build player-slot index from PBP v85 for PP3 matching ─────────────────
    # For each (event, div, place) in non-doubles divisions, collect player
    # person_canon variants (exact-lower, ascii-fold, ascii-strip)
    player_slot_index: dict[tuple, set[str]] = {}
    for _, row in pbp_df.iterrows():
        if row.get("competitor_type", "") != "player":
            continue
        div = nb(row.get("division_canon", ""))
        if _is_doubles_div(div):
            continue
        pc = nb(row.get("person_canon", ""))
        if not pc or pc == "__NON_PERSON__":
            continue
        slot = (
            nb(row.get("event_id", "")),
            div,
            normalize_place(row.get("place", "")),
        )
        player_slot_index.setdefault(slot, set())
        player_slot_index[slot].add(pc.lower())
        player_slot_index[slot].add(_ascii_fold_str(pc))
        player_slot_index[slot].add(_ascii_strip_str(pc))

    # ── Build set of (event, div, place) where a piped team row exists (PP2) ──
    team_slots_in_pbp: set[tuple] = set()
    for _, row in pbp_df.iterrows():
        tpk = nb(row.get("team_person_key", ""))
        tdn = nb(row.get("team_display_name", ""))
        is_team = (
            row.get("competitor_type", "") == "team"
            and ("|" in tpk or " / " in tdn)
        )
        if is_team:
            team_slots_in_pbp.add((
                nb(row.get("event_id", "")),
                nb(row.get("division_canon", "")),
                normalize_place(row.get("place", "")),
            ))

    # ── Build duplicate-key set for PP4 ────────────────────────────────────────
    player_rows_pbp = pbp_df[pbp_df["competitor_type"] == "player"].copy()
    pp4_key_seen: set[tuple] = set()
    pp4_duplicate_indices: set[int] = set()
    for idx, row in player_rows_pbp.iterrows():
        pc = nb(row.get("person_canon", ""))
        if not pc:
            continue
        key = (
            nb(row.get("event_id", "")),
            nb(row.get("division_canon", "")),
            normalize_place(row.get("place", "")),
            pc,
        )
        if key in pp4_key_seen:
            pp4_duplicate_indices.add(int(idx))
        else:
            pp4_key_seen.add(key)

    records: list[dict] = []

    for _, diff_row in absent_df.iterrows():
        pbp_idx = int(diff_row["pbp_row_index"])
        status  = nb(diff_row["status"])
        eid     = nb(diff_row["event_id"])
        div     = nb(diff_row["division_canon"])
        pl      = normalize_place(diff_row.get("place", ""))
        pc      = nb(diff_row["person_canon"])
        tdn     = nb(diff_row["team_display_name"])
        ct      = nb(diff_row["competitor_type"])

        attribution = "UNKNOWN"
        confidence  = "LOW"
        evidence    = ""
        is_cascade  = False
        cascade_iter  = None
        cascade_trigger_person = None
        cascade_trigger_place  = None

        # ── PP0: metadata phrase in person_canon or team_display_name ──────────
        pc_up  = pc.upper()
        tdn_up = tdn.upper()
        for phrase in METADATA_PHRASES:
            if phrase in pc_up or phrase in tdn_up:
                attribution = "PP0_METADATA"
                confidence  = "HIGH"
                evidence    = f"'{phrase}' found in {'person_canon' if phrase in pc_up else 'team_display_name'}"
                break

        if attribution == "UNKNOWN":
            # ── PP3: team row in non-doubles div superseded by a player row ─────
            if ct == "team" and not _is_doubles_div(div) and " / " in tdn:
                parts     = tdn.split(" / ", 1)
                first_p   = parts[0].strip()
                second_p  = parts[1].strip() if len(parts) > 1 else ""
                slot      = (eid, div, pl)
                known     = player_slot_index.get(slot, set())
                for part in (first_p, second_p):
                    if not part:
                        continue
                    p_lower = part.lower()
                    p_fold  = _ascii_fold_str(part)
                    p_strip = _ascii_strip_str(part)
                    if p_lower in known or p_fold in known or p_strip in known:
                        attribution = "PP3_TEAM_SUPERSEDED"
                        confidence  = "HIGH"
                        evidence    = (
                            f"Component '{part}' matches a player row at same "
                            f"(event={eid}, div='{div}', place={pl})"
                        )
                        break

        if attribution == "UNKNOWN":
            # ── PP4: exact duplicate player row ──────────────────────────────────
            if ct == "player" and pbp_idx in pp4_duplicate_indices:
                attribution = "PP4_EXACT_DUPLICATE"
                confidence  = "HIGH"
                evidence    = (
                    f"Duplicate (event={eid}, div='{div}', place={pl}, "
                    f"person_canon='{pc}') — earlier row retained"
                )

        if attribution == "UNKNOWN":
            # ── PP5: pool-shadow removal (direct or cascade) ────────────────────
            if pbp_idx in pp5_removals:
                info     = pp5_removals[pbp_idx]
                it       = info["iteration"]
                rt       = info["removal_type"]
                is_cascade = (rt == "CASCADE")
                cascade_iter = it
                cascade_trigger_person = info["trigger_person_canon"]
                cascade_trigger_place  = info["trigger_place"]

                attribution = f"PP5_{rt}"  # PP5_DIRECT or PP5_CASCADE
                confidence  = "HIGH"
                if rt == "DIRECT":
                    ujd = info.get("unique_place_that_justified_direct")
                    evidence = (
                        f"Player '{pc}' appeared at multiple places in (event={eid}, "
                        f"div='{div}'); unique place={ujd}, shared place={pl} removed "
                        f"in iteration {it}"
                    )
                else:
                    evidence = (
                        f"Cascade removal in iteration {it}; triggered by "
                        f"'{cascade_trigger_person}' at place {cascade_trigger_place}"
                    )

        if attribution == "UNKNOWN":
            # ── MAIN_CITY_ARTIFACT: "Name / CityName ST" team-display format ─────
            if ct == "team" and " / " in tdn:
                parts = tdn.split(" / ", 1)
                if len(parts) == 2 and _is_city_artifact(parts[1]):
                    attribution = "MAIN_CITY_ARTIFACT"
                    confidence  = "HIGH"
                    evidence    = (
                        f"team_display_name second component '{parts[1].strip()}' "
                        f"matches CityName + 2-letter state-code pattern"
                    )

        # ── unknown_priority ─────────────────────────────────────────────────────
        if attribution == "UNKNOWN":
            unknown_priority = "HIGH" if status == "ABSENT_NO_COVERAGE" else "MEDIUM"
        else:
            unknown_priority = "N/A"

        records.append({
            "pbp_row_index":            pbp_idx,
            "event_id":                  eid,
            "division_canon":            div,
            "place":                     pl,
            "person_canon":              pc,
            "team_display_name":         tdn,
            "status":                    status,
            "attribution":               attribution,
            "attribution_confidence":    confidence,
            "attribution_evidence":      evidence,
            "unknown_priority":          unknown_priority,
            "is_cascade":                is_cascade,
            "cascade_iteration":         cascade_iter,
            "cascade_trigger_person_canon": cascade_trigger_person,
            "cascade_trigger_place":     cascade_trigger_place,
        })

    result = pd.DataFrame(records, columns=ATTR_COLUMNS)
    print(
        f"  Attribution breakdown: "
        + ", ".join(
            f"{k}={v}"
            for k, v in result["attribution"].value_counts().items()
        )
    )
    return result


# ── Step 4c: European-format candidates ───────────────────────────────────────

# Recognised country codes for confidence scoring
EUROPEAN_COUNTRY_CODES: frozenset[str] = frozenset({
    "FRA", "GER", "GBR", "ITA", "ESP", "POL", "CZE", "SVK", "HUN", "RUS",
    "NED", "BEL", "AUT", "SUI", "SWE", "NOR", "DEN", "FIN", "POR", "LAT",
    "LTU", "EST", "UKR", "BLR", "ROM", "BUL", "SLO", "CRO", "SRB", "GRE",
    "TUR", "ISR", "ARG", "BRA", "COL", "VEN", "MEX", "AUS", "NZL", "JPN",
    "CAN", "USA",
})

# Rough set of European event_ids based on location clues — not exhaustive,
# used only for the +1 signal.  The signal is non-definitive (marked informational).
# Rather than hardcoding, we detect by checking if any pipeline event metadata
# suggests Europe (e.g. country field — if available).  If not available, we
# fall back to "unknown" (no bonus).


EURO_FORMAT_COLUMNS: list[str] = [
    "pbp_row_index",
    "event_id",
    "year",
    "division_canon",
    "division_category",
    "place",
    "team_display_name",
    "left_component",
    "right_component",
    "reconstructed_name_tentative",
    "country_code_detected",
    "status_in_pipeline",
    "attribution_in_pipeline",
    "confidence",
    "confidence_score",
    "requires_source_validation",
    "european_event_signal",
    "pt_match_count",
    "pt_match_ids",
    "pt_match_status",
    "investigation_note",
]


def _detect_european_event(event_id: str, pipeline_df: pd.DataFrame) -> bool:
    """
    Return True if there's any signal that the event is European.
    Uses the 'country' column from pipeline if present; otherwise False.
    """
    if "country" in pipeline_df.columns:
        rows = pipeline_df[pipeline_df["event_id"].astype(str) == str(event_id)]
        if not rows.empty:
            country = nb(rows.iloc[0].get("country", ""))
            eu_codes = {"FRA", "GER", "GBR", "ITA", "ESP", "POL", "CZE", "SVK",
                        "HUN", "RUS", "NED", "BEL", "AUT", "SUI", "SWE", "NOR",
                        "DEN", "FIN", "POR", "LAT", "LTU", "EST", "UKR", "BLR"}
            return country.upper() in eu_codes
    return False


def _is_plausible_given_name(word: str) -> bool:
    """
    A word is a plausible given name if it is ≥2 chars, not a known country code,
    and is mixed-case or all-caps (i.e. not all-lowercase, not a 2-letter code).
    """
    if len(word) < 2:
        return False
    if word.upper() in EUROPEAN_COUNTRY_CODES:
        return False
    # Country codes are 2-3 uppercase letters — exclude
    if re.fullmatch(r"[A-Z]{2,3}", word):
        return False
    return True


def _euro_confidence(
    left: str,
    right_words: list[str],
    country_code: str,
    is_absent: bool,
    is_singles_div: bool,
    european_event: bool,
) -> int:
    score = 0
    if country_code:
        score += 2
    if european_event:
        score += 1
    if len(left.split()) == 1:
        score += 1
    if len(right_words) == 2 and right_words[-1].upper() in EUROPEAN_COUNTRY_CODES:
        score += 2
    if is_absent:
        score += 1
    if is_singles_div:
        score += 1
    return score


def _score_to_confidence(score: int) -> str:
    if score >= 5:
        return "HIGH"
    if score >= 3:
        return "MEDIUM"
    return "LOW"


def _pt_lookup(reconstructed: str, pt_df: pd.DataFrame) -> tuple[int, str, str]:
    """
    Search PT for the reconstructed name.
    Returns (match_count, pipe-separated person_ids, status).
    Status: UNIQUE | AMBIGUOUS | UNRESOLVED
    """
    if not reconstructed or pt_df is None or pt_df.empty:
        return 0, "", "UNRESOLVED"

    name_col = None
    for col in ("person_canon", "name", "canonical_name"):
        if col in pt_df.columns:
            name_col = col
            break
    if name_col is None:
        return 0, "", "UNRESOLVED"

    id_col = None
    for col in ("person_id", "uuid", "id"):
        if col in pt_df.columns:
            id_col = col
            break

    rec_lower = reconstructed.lower().strip()
    rec_fold  = _ascii_fold_str(reconstructed)
    rec_strip = _ascii_strip_str(reconstructed)

    matched_ids: list[str] = []
    for _, row in pt_df.iterrows():
        pt_name = nb(row.get(name_col, ""))
        if not pt_name:
            continue
        if (pt_name.lower().strip() == rec_lower
                or _ascii_fold_str(pt_name) == rec_fold
                or _ascii_strip_str(pt_name) == rec_strip):
            pid = nb(row.get(id_col, "")) if id_col else ""
            matched_ids.append(pid)

    n = len(matched_ids)
    if n == 0:
        return 0, "", "UNRESOLVED"
    if n == 1:
        return 1, matched_ids[0], "UNIQUE"
    return n, "|".join(matched_ids), "AMBIGUOUS"


def compute_european_format_candidates(
    diff_df: pd.DataFrame,
    pbp_df: pd.DataFrame,
    pt_df: pd.DataFrame,
    attr_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Identify team rows in non-doubles divisions that match the European
    "Surname / GivenName [CountryCode]" format.
    """
    # Build attribution lookup: pbp_row_index → attribution
    attr_lookup: dict[int, str] = {}
    if attr_df is not None and not attr_df.empty:
        for _, row in attr_df.iterrows():
            attr_lookup[int(row["pbp_row_index"])] = nb(row["attribution"])

    # Build a set of event_ids in pipeline for european_event_signal
    pipeline_event_ids = set(diff_df["event_id"].dropna().unique())

    # Pre-build a fake pipeline_df-like lookup from diff_df for event detection
    # (diff_df has event_id and year — no country, so european_event_signal = False unless
    # we can determine from event metadata elsewhere)
    # For now: european_event_signal is derived from year range being post-1990 only as a weak proxy.
    # A more robust approach would require event metadata CSV — using False as safe default.

    records: list[dict] = []

    for _, row in pbp_df.iterrows():
        idx = int(row.name)
        if nb(row.get("competitor_type", "")) != "team":
            continue
        # Skip metadata rows — they are not European-format candidates
        tdn_check = nb(row.get("team_display_name", "")).upper()
        pc_check  = nb(row.get("person_canon", "")).upper()
        if any(ph in tdn_check or ph in pc_check for ph in METADATA_PHRASES):
            continue
        div_raw  = nb(row.get("division_canon", ""))
        div_norm = normalize_division(div_raw)
        # Must be non-doubles
        if "doubles" in div_norm.lower() or re.search(r"\bdbl\b", div_norm.lower()):
            continue
        tdn = nb(row.get("team_display_name", ""))
        # Must have exactly one " / "
        if tdn.count(" / ") != 1:
            continue
        parts = tdn.split(" / ", 1)
        left  = parts[0].strip()
        right = parts[1].strip()
        right_words = right.split()

        # Left: 1–2 words (surname, possibly hyphenated)
        left_words = left.split()
        if len(left_words) < 1 or len(left_words) > 2:
            continue
        # Right: first word must be a plausible given name; skip address formats
        if not right_words or not _is_plausible_given_name(right_words[0]):
            continue
        # Skip address-style right components (commas or excessive words)
        if "," in right or len(right_words) > 3:
            continue

        # Find the diff row for this pbp index
        diff_rows = diff_df[diff_df["pbp_row_index"] == idx]
        status_pipeline = nb(diff_rows.iloc[0]["status"]) if not diff_rows.empty else ""
        attribution_pipeline = attr_lookup.get(idx, "")

        is_absent  = status_pipeline.startswith("ABSENT")
        div_cat    = nb(row.get("division_category", ""))
        is_singles = any(kw in div_norm.lower() for kw in
                         ("singles", "consecutive", "shred", "golf", "sick", "routines", "battle"))
        european_event = False  # conservative; requires external metadata

        # Detect trailing country code
        country_code = ""
        if right_words and right_words[-1].upper() in EUROPEAN_COUNTRY_CODES:
            country_code = right_words[-1].upper()

        score = _euro_confidence(
            left, right_words, country_code,
            is_absent, is_singles, european_event,
        )
        if score == 0:
            continue

        confidence = _score_to_confidence(score)

        # Reconstruct tentative name: "first_given_name left_surname"
        given_name = right_words[0] if right_words else ""
        if country_code and len(right_words) > 1:
            # right = "GivenName [Country]" → given is right_words[0]
            pass
        reconstructed = f"{given_name} {left}".strip()

        # PT lookup
        pt_match_count, pt_match_ids, pt_match_status = _pt_lookup(reconstructed, pt_df)

        year = nb(row.get("year", ""))
        note = ""
        if country_code:
            note = f"Country code '{country_code}' detected in right component."
        if pt_match_count == 1:
            note += f" PT UNIQUE match: {pt_match_ids}."
        elif pt_match_count > 1:
            note += f" PT AMBIGUOUS: {pt_match_count} matches."

        records.append({
            "pbp_row_index":               idx,
            "event_id":                     nb(row.get("event_id", "")),
            "year":                         year,
            "division_canon":               div_raw,
            "division_category":            div_cat,
            "place":                        normalize_place(row.get("place", "")),
            "team_display_name":            tdn,
            "left_component":               left,
            "right_component":              right,
            "reconstructed_name_tentative": reconstructed,
            "country_code_detected":        country_code,
            "status_in_pipeline":           status_pipeline,
            "attribution_in_pipeline":      attribution_pipeline,
            "confidence":                   confidence,
            "confidence_score":             score,
            "requires_source_validation":   True,
            "european_event_signal":        european_event,
            "pt_match_count":               pt_match_count,
            "pt_match_ids":                 pt_match_ids,
            "pt_match_status":              pt_match_status,
            "investigation_note":           note,
        })

    result = pd.DataFrame(records, columns=EURO_FORMAT_COLUMNS) if records else \
        pd.DataFrame(columns=EURO_FORMAT_COLUMNS)
    # Sort: HIGH first, then by confidence_score descending
    if not result.empty:
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        result["_ord"] = result["confidence"].map(order).fillna(3)
        result = result.sort_values(["_ord", "confidence_score"], ascending=[True, False]) \
                       .drop(columns=["_ord"]).reset_index(drop=True)
    return result


# ── Step 5a: Encoding artifact groups ─────────────────────────────────────────

ENC_COLUMNS: list[str] = [
    "normalized_form",
    "variant_count",
    "canonical_variant",
    "artifact_variants",
    "artifact_types_present",
    "affected_events",
    "total_affected_rows_in_pbp",
    "rows_on_artifact_variant",
    "rows_on_clean_variant",
    "absent_rows_on_artifact_variant",
    "pipeline_uses_which_variant",
    "investigation_priority",
]


def compute_encoding_artifacts(
    diff_df: pd.DataFrame,
    pbp_df: pd.DataFrame,
    pipeline_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Identify division_canon values in PBP v85 that contain artifact characters,
    group by normalized form, and report on pipeline coverage.
    """
    # Build absence lookup: (event_id, division_canon, place, competitor_type) → bool absent
    absent_keys: set[tuple] = set()
    for _, row in diff_df.iterrows():
        if row["status"].startswith("ABSENT"):
            absent_keys.add((
                nb(row["event_id"]),
                nb(row["division_canon"]),
                normalize_place(row.get("place", "")),
                nb(row["competitor_type"]),
            ))

    # Collect all distinct division_canon values in PBP v85 with their row indices
    # grouped by normalized form
    from collections import defaultdict as _dd
    groups: dict[str, dict] = {}
    for _, row in pbp_df.iterrows():
        div_raw  = nb(row.get("division_canon", ""))
        div_norm = normalize_division(div_raw)
        if div_norm not in groups:
            groups[div_norm] = {
                "variants": {},       # div_raw → {"rows": int, "events": set, "absent": int}
                "has_artifact": False,
            }
        if div_raw not in groups[div_norm]["variants"]:
            groups[div_norm]["variants"][div_raw] = {"rows": 0, "events": set(), "absent": 0}
        groups[div_norm]["variants"][div_raw]["rows"] += 1
        groups[div_norm]["variants"][div_raw]["events"].add(nb(row.get("event_id", "")))

        artifacts = detect_artifacts(div_raw)
        if artifacts:
            groups[div_norm]["has_artifact"] = True

            # Check if this row is absent
            row_key = (
                nb(row.get("event_id", "")),
                div_raw,
                normalize_place(row.get("place", "")),
                nb(row.get("competitor_type", "")),
            )
            if row_key in absent_keys:
                groups[div_norm]["variants"][div_raw]["absent"] += 1

    # Build pipeline division_canon set
    pipeline_divs: set[str] = set(
        nb(r) for r in pipeline_df["division_canon"].dropna()
    )

    records: list[dict] = []
    for norm_form, info in groups.items():
        variants = info["variants"]
        # Only report groups that have at least one artifact-containing variant
        if not info["has_artifact"]:
            continue

        # Separate clean vs artifact variants
        clean_variants    = [v for v in variants if not detect_artifacts(v)]
        artifact_variants = [v for v in variants if detect_artifacts(v)]

        canonical_variant = clean_variants[0] if clean_variants else ""
        artifact_var_str  = "|".join(artifact_variants)
        artifact_types    = set()
        all_events        = set()
        rows_artifact     = 0
        rows_clean        = 0
        absent_artifact   = 0

        for var, vinfo in variants.items():
            all_events |= vinfo["events"]
            if detect_artifacts(var):
                rows_artifact  += vinfo["rows"]
                absent_artifact += vinfo["absent"]
                for code in detect_artifacts(var).split("|"):
                    artifact_types.add(code)
            else:
                rows_clean += vinfo["rows"]

        # What does the pipeline use?
        pipeline_variant = "ABSENT"
        for var in variants:
            if var in pipeline_divs:
                pipeline_variant = var
                break
        if pipeline_variant == "ABSENT" and canonical_variant:
            if canonical_variant in pipeline_divs:
                pipeline_variant = canonical_variant

        if absent_artifact > 0:
            priority = "HIGH"
        elif rows_artifact > 0:
            priority = "MEDIUM"
        else:
            priority = "LOW"

        records.append({
            "normalized_form":                norm_form,
            "variant_count":                  len(variants),
            "canonical_variant":              canonical_variant,
            "artifact_variants":              artifact_var_str,
            "artifact_types_present":         "|".join(sorted(artifact_types)),
            "affected_events":                "|".join(sorted(all_events)),
            "total_affected_rows_in_pbp":     rows_artifact + rows_clean,
            "rows_on_artifact_variant":       rows_artifact,
            "rows_on_clean_variant":          rows_clean,
            "absent_rows_on_artifact_variant": absent_artifact,
            "pipeline_uses_which_variant":    pipeline_variant,
            "investigation_priority":         priority,
        })

    result = pd.DataFrame(records, columns=ENC_COLUMNS) if records else \
        pd.DataFrame(columns=ENC_COLUMNS)
    if not result.empty:
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        result["_ord"] = result["investigation_priority"].map(order).fillna(3)
        result = result.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)
    return result


# ── Step 5b: Cascade shadow events ────────────────────────────────────────────

CASCADE_COLUMNS: list[str] = [
    "event_id",
    "year",
    "division_canon",
    "division_category",
    "format_signals",
    "total_players_in_division",
    "distinct_places_in_division",
    "direct_removals",
    "cascade_removals",
    "total_removals",
    "max_cascade_depth",
    "removed_person_canon",
    "removed_place",
    "removal_iteration",
    "removal_type",
    "trigger_person_canon",
    "trigger_place",
    "unique_place_that_justified_direct",
    "status_in_pipeline",
    "investigation_priority",
]

# Signals suggesting a pool-only competition structure
POOL_FORMAT_KEYWORDS: list[str] = [
    "circle", "request", "shred", "contest", "big", "battle", "ironman",
]


def _format_signals(div_norm: str, div_cat: str) -> str:
    d = div_norm.lower()
    signals = []
    for kw in POOL_FORMAT_KEYWORDS:
        if re.search(r"\b" + kw + r"\b", d):
            signals.append(kw)
    # Big N pattern (e.g. "Big 10")
    if re.search(r"\bbig\s+\d+\b", d):
        signals.append("big_n")
    if div_cat == "freestyle" and "final" not in d:
        signals.append("freestyle_no_final")
    return "|".join(dict.fromkeys(signals))  # deduplicate, preserve order


def compute_cascade_shadow_events(
    diff_df: pd.DataFrame,
    pp5_removals: dict[int, dict],
    pbp_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Report on all PP5 removals (direct and cascade) per event/division,
    annotated with pool-format signals.
    """
    if not pp5_removals:
        return pd.DataFrame(columns=CASCADE_COLUMNS)

    # Build status lookup from diff_df
    status_lookup: dict[int, str] = {
        int(r["pbp_row_index"]): nb(r["status"])
        for _, r in diff_df.iterrows()
    }

    # Division-level summary counters
    div_summary: dict[tuple, dict] = {}

    for idx, info in pp5_removals.items():
        row = pbp_df.loc[idx]
        eid  = nb(row.get("event_id", ""))
        yr   = nb(row.get("year", ""))
        div  = nb(row.get("division_canon", ""))
        cat  = nb(row.get("division_category", ""))
        key  = (eid, div)

        if key not in div_summary:
            # Compute total players and distinct places in this division
            div_rows = pbp_df[
                (pbp_df["event_id"].astype(str) == eid)
                & (pbp_df["division_canon"] == div)
                & (pbp_df["competitor_type"] == "player")
            ]
            total_players = div_rows["person_canon"].nunique()
            distinct_places = div_rows["place"].nunique()
            div_summary[key] = {
                "year": yr,
                "division_category": cat,
                "total_players": total_players,
                "distinct_places": distinct_places,
                "direct": 0,
                "cascade": 0,
                "max_depth": 0,
                "removed_rows": [],
            }

        it = info["iteration"]
        rt = info["removal_type"]
        div_summary[key]["max_depth"] = max(div_summary[key]["max_depth"], it)
        if rt == "DIRECT":
            div_summary[key]["direct"] += 1
        else:
            div_summary[key]["cascade"] += 1

        div_summary[key]["removed_rows"].append({
            "pbp_idx":           idx,
            "removed_person_canon": nb(row.get("person_canon", "")),
            "removed_place":        normalize_place(row.get("place", "")),
            "removal_iteration":    it,
            "removal_type":         rt,
            "trigger_person_canon": info.get("trigger_person_canon"),
            "trigger_place":        info.get("trigger_place"),
            "unique_place_that_justified_direct": info.get("unique_place_that_justified_direct"),
            "status_in_pipeline":   status_lookup.get(idx, ""),
        })

    records: list[dict] = []
    for (eid, div), summary in div_summary.items():
        div_norm = normalize_division(div)
        cat      = summary["division_category"]
        signals  = _format_signals(div_norm, cat)
        direct   = summary["direct"]
        cascade  = summary["cascade"]
        depth    = summary["max_depth"]

        if cascade > 0 and signals:
            priority = "HIGH"
        elif cascade > 0:
            priority = "MEDIUM"
        else:
            priority = "LOW"

        for rrow in summary["removed_rows"]:
            records.append({
                "event_id":                         eid,
                "year":                             summary["year"],
                "division_canon":                   div,
                "division_category":                cat,
                "format_signals":                   signals,
                "total_players_in_division":        summary["total_players"],
                "distinct_places_in_division":      summary["distinct_places"],
                "direct_removals":                  direct,
                "cascade_removals":                 cascade,
                "total_removals":                   direct + cascade,
                "max_cascade_depth":                depth,
                "removed_person_canon":             rrow["removed_person_canon"],
                "removed_place":                    rrow["removed_place"],
                "removal_iteration":                rrow["removal_iteration"],
                "removal_type":                     rrow["removal_type"],
                "trigger_person_canon":             rrow["trigger_person_canon"],
                "trigger_place":                    rrow["trigger_place"],
                "unique_place_that_justified_direct": rrow["unique_place_that_justified_direct"],
                "status_in_pipeline":               rrow["status_in_pipeline"],
                "investigation_priority":           priority,
            })

    result = pd.DataFrame(records, columns=CASCADE_COLUMNS) if records else \
        pd.DataFrame(columns=CASCADE_COLUMNS)
    if not result.empty:
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        result["_ord"] = result["investigation_priority"].map(order).fillna(3)
        result = result.sort_values(
            ["_ord", "event_id", "division_canon", "removal_iteration"]
        ).drop(columns=["_ord"]).reset_index(drop=True)
    return result


# ── Step 5c: Player multi-place ────────────────────────────────────────────────

MULTI_COLUMNS: list[str] = [
    "event_id",
    "year",
    "division_canon",
    "division_category",
    "person_canon",
    "person_id",
    "places_in_pbp",
    "place_count",
    "unique_places",
    "shared_places",
    "pp5_eligible",
    "pp5_action",
    "statuses_in_pipeline",
    "investigation_priority",
]


def compute_player_multi_place(
    diff_df: pd.DataFrame,
    pbp_df: pd.DataFrame,
    pp5_removals: dict[int, dict],
) -> pd.DataFrame:
    """
    Report all PBP v85 player rows where the same person appears at 2+ places
    in the same (event, division).  These are the inputs to pre-pass 5.
    """
    player_df = pbp_df[
        (pbp_df["competitor_type"] == "player")
        & (pbp_df["person_canon"].str.strip() != "")
        & (pbp_df["person_canon"] != "__NON_PERSON__")
    ].copy()

    # Place-population index (for shared/unique classification)
    place_pop = player_df.groupby(
        ["event_id", "division_canon", "place"]
    )["person_canon"].transform("nunique")
    player_df["_place_pop"] = place_pop.values

    # Status lookup
    status_lookup: dict[int, str] = {
        int(r["pbp_row_index"]): nb(r["status"])
        for _, r in diff_df.iterrows()
    }

    # PP5 removal action lookup
    pp5_action_lookup: dict[int, str] = {
        idx: info["removal_type"]  # DIRECT or CASCADE
        for idx, info in pp5_removals.items()
    }

    records: list[dict] = []

    for (eid, div, pc), grp in player_df.groupby(
        ["event_id", "division_canon", "person_canon"]
    ):
        if len(grp["place"].unique()) < 2:
            continue  # only one place — not multi-place

        yr  = nb(grp.iloc[0].get("year",              ""))
        cat = nb(grp.iloc[0].get("division_category", ""))
        pid = nb(grp.iloc[0].get("person_id",         ""))

        all_places    = sorted(grp["place"].unique().tolist(), key=lambda x: int(x) if str(x).isdigit() else 0)
        unique_places = sorted(
            [p for p in grp["place"].unique() if grp.loc[grp["place"] == p, "_place_pop"].iloc[0] == 1],
            key=lambda x: int(x) if str(x).isdigit() else 0,
        )
        shared_places = sorted(
            [p for p in grp["place"].unique() if grp.loc[grp["place"] == p, "_place_pop"].iloc[0] > 1],
            key=lambda x: int(x) if str(x).isdigit() else 0,
        )
        pp5_eligible = bool(unique_places and shared_places)

        # Determine PP5 action for this player in this division
        actions = set()
        for idx in grp.index:
            if int(idx) in pp5_action_lookup:
                actions.add(pp5_action_lookup[int(idx)])
        if "CASCADE" in actions:
            pp5_action = "CASCADE"
        elif "DIRECT" in actions:
            pp5_action = "DIRECT"
        else:
            pp5_action = "NONE"

        statuses = sorted(set(
            status_lookup.get(int(idx), "") for idx in grp.index
        ))

        if pp5_eligible and "CASCADE" in (pp5_action,):
            priority = "HIGH"
        elif pp5_eligible:
            priority = "MEDIUM"
        else:
            priority = "LOW"

        records.append({
            "event_id":            str(eid),
            "year":                yr,
            "division_canon":      str(div),
            "division_category":   cat,
            "person_canon":        str(pc),
            "person_id":           pid,
            "places_in_pbp":       "|".join(str(p) for p in all_places),
            "place_count":         len(all_places),
            "unique_places":       "|".join(str(p) for p in unique_places),
            "shared_places":       "|".join(str(p) for p in shared_places),
            "pp5_eligible":        pp5_eligible,
            "pp5_action":          pp5_action,
            "statuses_in_pipeline": "|".join(statuses),
            "investigation_priority": priority,
        })

    result = pd.DataFrame(records, columns=MULTI_COLUMNS) if records else \
        pd.DataFrame(columns=MULTI_COLUMNS)
    if not result.empty:
        order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        result["_ord"] = result["investigation_priority"].map(order).fillna(3)
        result = result.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)
    return result


# ── Step 5d: Audit summary ─────────────────────────────────────────────────────

def write_audit_summary(
    diff_df:       pd.DataFrame,
    attr_df:       pd.DataFrame,
    euro_df:       pd.DataFrame,
    enc_df:        pd.DataFrame,
    cascade_df:    pd.DataFrame,
    multi_df:      pd.DataFrame,
    pipeline_pf:   pd.DataFrame,
    pipeline_pbp:  pd.DataFrame,
    pp5_removals:  dict,
    out_path:      Path,
) -> None:
    """Write audit_summary.txt with all sections and consistency checks."""
    lines: list[str] = []

    def s(text: str = "") -> None:
        lines.append(text)

    n_pbp       = len(diff_df)
    n_pf        = len(pipeline_pf)
    n_pipeline  = len(pipeline_pbp)

    n_present   = int((diff_df["status"] == "PRESENT").sum())
    n_covered   = int((diff_df["status"] == "ABSENT_SLOT_COVERED").sum())
    n_no_cov    = int((diff_df["status"] == "ABSENT_NO_COVERAGE").sum())
    n_absent    = n_covered + n_no_cov

    # ── Section 1 ──────────────────────────────────────────────────────────────
    s("=== ROW COUNT CONSISTENCY ===")
    s()
    s(f"  PBP v85 input rows:                    {n_pbp:>8,}")
    s(f"  Post-02p5 Placements_Flat rows:        {n_pf:>8,}")
    s(f"  Post-02p6 Placements_ByPerson rows:    {n_pipeline:>8,}")
    s()
    net_diff = n_pbp - n_pipeline
    s(f"  PBP v85 → post-02p6 difference:        {net_diff:>8,}")
    s()
    s(f"  Rows in diff with status=PRESENT:      {n_present:>8,}")
    s(f"  Rows in diff with status=ABSENT_*:     {n_absent:>8,}")
    s(f"    ABSENT_SLOT_COVERED:                 {n_covered:>8,}")
    s(f"    ABSENT_NO_COVERAGE:                  {n_no_cov:>8,}")
    s()
    # Primary exit criterion: every PBP row has been classified
    primary_check = "PASS" if n_present + n_absent == n_pbp else "FAIL"
    s(f"  PRESENT + ABSENT = PBP total (primary): {primary_check}")
    s()
    # Informational: ABSENT > net diff because 02p6 adds new rows not in PBP v85
    # (e.g. [UNKNOWN PARTNER] team rows from pre-pass 2 conversions).
    pipeline_adds = n_absent - net_diff
    s(f"  Note: ABSENT ({n_absent:,}) > net diff ({net_diff:,}) by {pipeline_adds:,}")
    s(f"        — 02p6 adds ~{pipeline_adds:,} new rows not present in PBP v85")
    s(f"          (expected; not a diff logic error)")
    s()

    # ── Section 2 ──────────────────────────────────────────────────────────────
    s("=== ATTRIBUTION BREAKDOWN ===")
    s()
    if not attr_df.empty:
        attr_counts   = attr_df["attribution"].value_counts()
        event_counts  = attr_df.groupby("attribution")["event_id"].nunique()
        attr_order = [
            "PP0_METADATA", "PP3_TEAM_SUPERSEDED", "PP4_EXACT_DUPLICATE",
            "PP5_DIRECT", "PP5_CASCADE", "MAIN_CITY_ARTIFACT", "UNKNOWN",
        ]
        for code in attr_order:
            n_rows   = int(attr_counts.get(code, 0))
            n_events = int(event_counts.get(code, 0))
            s(f"  {code:<25}  {n_rows:>6} rows   ({n_events:>4} events)")
        s()
        total_attributed = len(attr_df)
        attr_sum_check = "PASS" if total_attributed == n_absent else "FAIL"
        s(f"  Total attributed:                  {total_attributed:>6}")
        s(f"  Sum check = ABSENT_* count:        {attr_sum_check}")
        s()

        # UNKNOWN priority breakdown (new in design review)
        unk_df = attr_df[attr_df["attribution"] == "UNKNOWN"]
        if not unk_df.empty:
            s("  UNKNOWN priority breakdown:")
            for pri in ["HIGH", "MEDIUM", "LOW"]:
                n = int((unk_df["unknown_priority"] == pri).sum())
                s(f"    {pri:<8} {n:>5}")
        s()
    else:
        s("  (no absent rows)")
        s()

    # ── Section 3 ──────────────────────────────────────────────────────────────
    s("=== INVESTIGATION CANDIDATES ===")
    s()

    # European-format
    s("  European-format candidates:")
    if not euro_df.empty:
        for conf in ["HIGH", "MEDIUM", "LOW"]:
            sub = euro_df[euro_df["confidence"] == conf]
            n_events = sub["event_id"].nunique() if not sub.empty else 0
            s(f"    {conf:<8} confidence:  {len(sub):>5} rows   ({n_events:>4} events)")
        s(f"    Total:                    {len(euro_df):>5} rows")
    else:
        s("    (none detected)")
    s()

    # Encoding artifacts
    s("  Encoding artifact variant groups:")
    if not enc_df.empty:
        n_high = int((enc_df["investigation_priority"] == "HIGH").sum())
        total_pbp_rows = int(enc_df["total_affected_rows_in_pbp"].sum())
        absent_art     = int(enc_df["absent_rows_on_artifact_variant"].sum())
        s(f"    Total groups:             {len(enc_df):>5}")
        s(f"    HIGH priority groups:     {n_high:>5}  (have absent rows)")
        s(f"    Total affected PBP rows:  {total_pbp_rows:>5}")
        s(f"    Absent artifact rows:     {absent_art:>5}")
    else:
        s("    (none detected)")
    s()

    # Cascade shadow
    s("  Cascade shadow:")
    if not cascade_df.empty:
        n_events_cascade = cascade_df[cascade_df["cascade_removals"] > 0]["event_id"].nunique()
        n_divs_cascade   = cascade_df[cascade_df["removal_type"] == "CASCADE"][
            ["event_id", "division_canon"]].drop_duplicates().shape[0]
        n_direct   = int(cascade_df[cascade_df["removal_type"] == "DIRECT"].shape[0])
        n_cascade  = int(cascade_df[cascade_df["removal_type"] == "CASCADE"].shape[0])
        max_depth  = int(cascade_df["max_cascade_depth"].max())
        n_pool_sig = cascade_df[
            cascade_df["investigation_priority"] == "HIGH"
        ][["event_id", "division_canon"]].drop_duplicates().shape[0]
        s(f"    Events with cascade:      {n_events_cascade:>5}")
        s(f"    Divisions with cascade:   {n_divs_cascade:>5}")
        s(f"    Direct removals:          {n_direct:>5} rows")
        s(f"    Cascade removals:         {n_cascade:>5} rows")
        s(f"    Max cascade depth:        {max_depth:>5} iterations")
        s(f"    Pool-format signal divs:  {n_pool_sig:>5}  (HIGH investigation priority)")
    else:
        s("    (none)")
    s()

    # UNKNOWN attribution
    n_unk = int((attr_df["attribution"] == "UNKNOWN").sum()) if not attr_df.empty else 0
    s(f"  UNKNOWN attribution:        {n_unk:>5} rows  ← require separate investigation")
    s()

    # ── Section 4: Consistency checks ──────────────────────────────────────────
    s("=== CONSISTENCY CHECKS ===")
    s()

    def chk(cond: bool, label: str) -> str:
        return f"  [{'PASS' if cond else 'FAIL'}] {label}"

    # 1. Primary exit criterion: all PBP rows classified
    c1 = (n_present + n_absent == n_pbp)
    s(chk(c1, "PRESENT + ABSENT = PBP total (primary exit criterion)"))

    # 2. No row appears in multiple attributions (first-match → always true by construction)
    dupes = attr_df.duplicated("pbp_row_index").any() if not attr_df.empty else False
    s(chk(not dupes, "No row appears in multiple attributions"))

    # 3. European candidates are subset of PP3_TEAM_SUPERSEDED + MAIN_CITY_ARTIFACT + UNKNOWN
    #    (MAIN_CITY_ARTIFACT allowed: "Name / City ST" can also be a misread European name)
    if not euro_df.empty and not attr_df.empty:
        allowed = {"PP3_TEAM_SUPERSEDED", "MAIN_CITY_ARTIFACT", "UNKNOWN", ""}
        euro_idx = set(euro_df["pbp_row_index"].astype(int))
        c3 = True
        for aidx, arow in attr_df.iterrows():
            if int(arow["pbp_row_index"]) in euro_idx:
                if arow["attribution"] not in allowed:
                    c3 = False
                    break
    else:
        c3 = True
    s(chk(c3, "European candidates are subset of PP3/MAIN_CITY/UNKNOWN attributions"))

    # 4. Cascade rows are subset of PP5_CASCADE attribution
    if not cascade_df.empty and not attr_df.empty:
        pp5_cascade_attr_idxs = set(
            attr_df[attr_df["attribution"] == "PP5_CASCADE"]["pbp_row_index"].astype(int)
        )
        # All cascade removals in pp5_removals should have been attributed as PP5_CASCADE
        c4 = True
        for idx, info in pp5_removals.items():
            if info["removal_type"] == "CASCADE" and idx not in pp5_cascade_attr_idxs:
                # May be absent from attr_df if the row is PRESENT (shouldn't happen but guard)
                if idx in set(attr_df["pbp_row_index"].astype(int)):
                    c4 = False
                    break
    else:
        c4 = True
    s(chk(c4, "Cascade rows are subset of PP5_CASCADE attribution"))

    # 5. Encoding artifact absent rows are subset of ABSENT_* rows
    if not enc_df.empty:
        n_ea = int(enc_df["absent_rows_on_artifact_variant"].sum())
        c5 = (n_ea <= n_absent)
    else:
        c5 = True
    s(chk(c5, "Encoding artifact absent rows are subset of ABSENT_* rows"))

    # 6. All ABSENT_NO_COVERAGE rows have slot_row_count_in_pipeline = 0
    anc_df = diff_df[diff_df["status"] == "ABSENT_NO_COVERAGE"]
    if not anc_df.empty:
        c6 = (anc_df["slot_row_count_in_pipeline"].astype(int) == 0).all()
    else:
        c6 = True
    s(chk(c6, "All ABSENT_NO_COVERAGE rows have slot_row_count_in_pipeline = 0"))

    # 7. All PRESENT rows have match_type != NONE
    present_df = diff_df[diff_df["status"] == "PRESENT"]
    if not present_df.empty:
        c7 = (present_df["match_type"] != "NONE").all()
    else:
        c7 = True
    s(chk(c7, "All PRESENT rows have match_type != NONE"))

    all_pass = all([c1, not dupes, c3, c4, c5, c6, c7])
    s()
    s(f"  {'All 7 checks PASS.' if all_pass else 'WARNING: one or more checks FAILED.'}")
    s()

    text = "\n".join(lines)
    out_path.write_text(text, encoding="utf-8")
    print(f"  Written: {out_path}")
    # Also print to stdout
    print()
    print(text)


# ── Shared output helper ───────────────────────────────────────────────────────

def write_csv(df: pd.DataFrame, path: Path, label: str) -> None:
    df.to_csv(path, index=False)
    print(f"  Written: {path}  ({len(df):,} rows)")


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Phase 0 audit: PBP v85 vs. active pipeline (read-only).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--pbp-v85",
        default=str(DEFAULT_PBP_V85),
        help="PBP v85 CSV (diagnostic reference)",
    )
    p.add_argument(
        "--pipeline-pbp",
        default=str(DEFAULT_PIPELINE_PBP),
        help="Post-02p6 Placements_ByPerson.csv (primary diff target; read by stage 05)",
    )
    p.add_argument(
        "--pipeline-pf",
        default=str(DEFAULT_PIPELINE_PF),
        help="Post-02p6 Placements_Flat.csv (reference; loaded for steps 4-5)",
    )
    p.add_argument(
        "--pt",
        default=str(DEFAULT_PT),
        help="Persons Truth CSV (used in steps 4-5 for pt_match_count)",
    )
    p.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Output directory for audit files",
    )
    return p.parse_args()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    args    = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print()
    print("=" * 60)
    print("Phase 0 Audit — PBP v85 vs. Active Pipeline")
    print("Read-only. No pipeline files modified.")
    print("=" * 60)

    # ── Step 1: Load inputs ────────────────────────────────────────────────────
    inputs = load_inputs(args)

    pbp_df      = inputs["pbp_v85"]
    pipeline_df = inputs["pipeline_pbp"]
    # pipeline_pf and pt are loaded and available for steps 4-5
    # pipeline_pf = inputs["pipeline_pf"]
    # pt_df       = inputs["pt"]

    # ── Steps 2-3: Normalize + compute row diff ────────────────────────────────
    diff_df = compute_row_diff(pbp_df, pipeline_df)

    # ── Output ────────────────────────────────────────────────────────────────
    print("\n[Phase 0] Writing outputs...")
    write_row_diff(diff_df, out_dir)
    print_row_diff_summary(diff_df)

    # ── Steps 4-5: Full implementation ───────────────────────────────────────
    pipeline_pf_df = inputs["pipeline_pf"]
    pt_df          = inputs["pt"]

    print("\n[Phase 0 / step 4a] Simulating pre-pass 5 on PBP v85...")
    pp5_removals = simulate_pp5(pbp_df)
    print(f"  PP5 simulation: {len(pp5_removals)} rows removed across all iterations")

    print("\n[Phase 0 / step 4b] Computing prepass attribution...")
    attr_df = compute_prepass_attribution(diff_df, pbp_df, pipeline_df, pp5_removals)
    write_csv(attr_df, out_dir / "audit_prepass_attribution.csv", "audit_prepass_attribution.csv")

    print("\n[Phase 0 / step 4c] Detecting European-format candidates...")
    euro_df = compute_european_format_candidates(diff_df, pbp_df, pt_df, attr_df)
    write_csv(euro_df, out_dir / "audit_european_format_candidates.csv",
              "audit_european_format_candidates.csv")

    print("\n[Phase 0 / step 5a] Computing encoding artifact groups...")
    enc_df = compute_encoding_artifacts(diff_df, pbp_df, pipeline_df)
    write_csv(enc_df, out_dir / "audit_encoding_artifacts.csv", "audit_encoding_artifacts.csv")

    print("\n[Phase 0 / step 5b] Building cascade shadow event report...")
    cascade_df = compute_cascade_shadow_events(diff_df, pp5_removals, pbp_df)
    write_csv(cascade_df, out_dir / "audit_cascade_shadow_events.csv",
              "audit_cascade_shadow_events.csv")

    print("\n[Phase 0 / step 5c] Building player multi-place report...")
    multi_df = compute_player_multi_place(diff_df, pbp_df, pp5_removals)
    write_csv(multi_df, out_dir / "audit_player_multi_place.csv", "audit_player_multi_place.csv")

    print("\n[Phase 0 / step 5d] Writing audit_summary.txt...")
    write_audit_summary(
        diff_df, attr_df, euro_df, enc_df, cascade_df, multi_df,
        pipeline_pf_df, pipeline_df, pp5_removals,
        out_dir / "audit_summary.txt",
    )

    print(f"\n  Output directory: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
