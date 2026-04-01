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
import sys
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

    # ── Steps 4-5: Deferred ───────────────────────────────────────────────────
    print("  Deferred (steps 4-5):")
    print("    audit_prepass_attribution.csv")
    print("    audit_european_format_candidates.csv")
    print("    audit_encoding_artifacts.csv")
    print("    audit_cascade_shadow_events.csv")
    print("    audit_player_multi_place.csv")
    print("    audit_summary.txt")
    print()
    print(f"  Output directory: {out_dir}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
