#!/usr/bin/env python3
"""
03_build_excel.py — Stage 3: Build final Excel workbook

This script:
- Reads out/stage2_canonical_events.csv
- Generates Excel workbook with one sheet per year
- Outputs: Footbag_Results_Canonical.xlsx

Input: out/stage2_canonical_events.csv
Output: Footbag_Results_Canonical.xlsx
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Optional
from collections import defaultdict

import pandas as pd

# Import master QC orchestrator
try:
    from qc_master import run_qc_for_stage, print_qc_summary
    USE_MASTER_QC = True
except ImportError:
    print("Warning: Could not import qc_master, Stage 3 QC will not run")
    USE_MASTER_QC = False


# Excel/openpyxl rejects control chars: 0x00-0x08, 0x0B-0x0C, 0x0E-0x1F
_ILLEGAL_XLSX_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def sanitize_excel_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Required to write .xlsx safely (not semantic cleaning)."""
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_string_dtype(out[col]) or out[col].dtype == object:
            out[col] = out[col].apply(
                lambda v: _ILLEGAL_XLSX_RE.sub("", v) if isinstance(v, str) else v
            )
    return out


def sanitize_string(s: str) -> str:
    """Sanitize a single string for Excel."""
    if not isinstance(s, str):
        return s
    return _ILLEGAL_XLSX_RE.sub("", s)


# ------------------------------------------------------------
# Results formatting from placements
# ------------------------------------------------------------

# Category display order and labels
CATEGORY_ORDER = ["net", "freestyle", "golf", "sideline", "unknown"]
CATEGORY_LABELS = {
    "net": "NET",
    "freestyle": "FREESTYLE",
    "golf": "GOLF",
    "sideline": "OTHER",
    "unknown": "OTHER",
}


def format_results_from_placements(placements: list[dict]) -> Optional[str]:
    """
    Build a deterministic, consistent results blob from canonical placements.
    Groups results by category (NET, FREESTYLE, GOLF, OTHER) with clear headers.

    Format:
      === NET ===
      OPEN SINGLES NET
      1. Name
      2. Name / Name

      === FREESTYLE ===
      SHRED 30
      1. Name

    We do NOT invent missing facts. If no placements exist -> None.
    """
    if not placements:
        return None

    # Group by category, then by division
    by_category = {}
    for p in placements:
        cat = p.get("division_category", "unknown") or "unknown"
        div = p.get("division_canon") or p.get("division_raw") or "Unknown"

        if cat not in by_category:
            by_category[cat] = {}
        if div not in by_category[cat]:
            by_category[cat][div] = []
        by_category[cat][div].append(p)

    out_lines = []

    # Output categories in defined order
    for cat in CATEGORY_ORDER:
        if cat not in by_category:
            continue

        divisions = by_category[cat]
        if not divisions:
            continue

        # Add category header
        label = CATEGORY_LABELS.get(cat, cat.upper())
        out_lines.append(f"<<< {label} >>>")
        out_lines.append("")

        # Sort divisions alphabetically within category
        for div in sorted(divisions.keys(), key=str.casefold):
            entries = divisions[div]

            # Sort entries by place, then by player name
            def sort_key(p):
                place = p.get("place", 999)
                try:
                    place = int(place)
                except (ValueError, TypeError):
                    place = 999
                name = _build_name_line(p)
                return (place, name.lower() if name else "")

            entries.sort(key=sort_key)

            out_lines.append(div.upper())

            for p in entries:
                place = p.get("place")
                try:
                    place_int = int(place)
                    place_txt = f"{place_int}."
                except (ValueError, TypeError):
                    place_txt = f"{place}." if place is not None else ""

                name = _build_name_line(p)
                if place_txt:
                    out_lines.append(f"{place_txt} {name}".rstrip())
                else:
                    out_lines.append(name)

            out_lines.append("")  # blank line between divisions

    # Remove trailing blank lines
    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines) if out_lines else None


def _build_name_line(placement: dict) -> str:
    """Build display name from placement dict."""
    p1 = (placement.get("player1_name") or "").strip()
    p2 = placement.get("player2_name")
    p2 = p2.strip() if isinstance(p2, str) and p2 else ""
    if p2:
        return f"{p1} / {p2}"
    return p1


# ------------------------------------------------------------
# CSV reading
# ------------------------------------------------------------
def read_stage2_csv(csv_path: Path) -> list[dict]:
    """Read stage2 CSV and return list of event records."""
    records = []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert year to int if present
            if row.get("year"):
                try:
                    row["year"] = int(row["year"])
                except ValueError:
                    row["year"] = None
            else:
                row["year"] = None

            # Parse placements JSON
            placements_json = row.get("placements_json", "[]")
            try:
                row["placements"] = json.loads(placements_json)
            except json.JSONDecodeError:
                row["placements"] = []

            records.append(row)
    return records


# ------------------------------------------------------------
# Excel writer
# ------------------------------------------------------------
def write_excel(out_xlsx: Path, records: list[dict]) -> None:
    """
    Archive workbook writer (matches Footbag_Results_Canonical.xlsx layout):
    - One sheet per year named YYYY.0
    - Columns are event_id
    - Rows are fixed labels (Tournament Name, Date, Location, ...)
    - Results are generated from placements (canonical), not copied raw
    """
    # Build results map from placements
    results_map = {}
    for rec in records:
        eid = rec.get("event_id")
        if eid:
            placements = rec.get("placements", [])
            results_map[str(eid)] = format_results_from_placements(placements)

    # Fixed row labels (index) to match the example workbook
    row_labels = [
        "Tournament Name",
        "Date",
        "Location",
        "Event Type",
        "Host Club",
        "Results",
    ]

    # Sort key for event IDs
    def _eid_sort_key(x: str):
        try:
            return int(re.sub(r"\D+", "", x) or "0")
        except Exception:
            return 0

    # Group records by year
    by_year = {}
    unknown_year = []
    for rec in records:
        year = rec.get("year")
        if year is not None:
            if year not in by_year:
                by_year[year] = []
            by_year[year].append(rec)
        else:
            unknown_year.append(rec)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
        # Build one sheet per year
        for y in sorted(by_year.keys()):
            year_records = by_year[y]
            eids = sorted([str(r.get("event_id", "")) for r in year_records], key=_eid_sort_key)

            data = {}
            for eid in eids:
                rec = next((r for r in year_records if str(r.get("event_id")) == eid), None)
                if not rec:
                    continue

                # Use integer event_id as column header to avoid Excel apostrophe prefix
                col_key = int(eid) if eid.isdigit() else eid
                data[col_key] = [
                    sanitize_string(rec.get("event_name") or ""),
                    sanitize_string(rec.get("date") or ""),
                    sanitize_string(rec.get("location") or ""),
                    sanitize_string(rec.get("event_type") or ""),
                    sanitize_string(rec.get("host_club") or ""),
                    sanitize_string(results_map.get(eid) or ""),
                ]

            df_year = pd.DataFrame(data, index=row_labels)
            df_year.index.name = "event_id"  # puts "event_id" in A1 like the example

            sheet_name = f"{int(y)}.0"
            df_year = sanitize_excel_strings(df_year)
            df_year.to_excel(xw, sheet_name=sheet_name)

            # Apply wrap_text formatting to Results row (row 7)
            worksheet = xw.sheets[sheet_name]
            for col_idx in range(2, len(eids) + 2):  # Start from column B (2)
                cell = worksheet.cell(row=7, column=col_idx)
                cell.alignment = cell.alignment.copy(wrap_text=True)

        # Unknown-year sheet
        if unknown_year:
            eids = sorted([str(r.get("event_id", "")) for r in unknown_year], key=_eid_sort_key)
            data = {}
            for eid in eids:
                rec = next((r for r in unknown_year if str(r.get("event_id")) == eid), None)
                if not rec:
                    continue

                # Use integer event_id as column header to avoid Excel apostrophe prefix
                col_key = int(eid) if eid.isdigit() else eid
                data[col_key] = [
                    sanitize_string(rec.get("event_name") or ""),
                    sanitize_string(rec.get("date") or ""),
                    sanitize_string(rec.get("location") or ""),
                    sanitize_string(rec.get("event_type") or ""),
                    sanitize_string(rec.get("host_club") or ""),
                    sanitize_string(results_map.get(eid) or ""),
                ]

            df_unk = pd.DataFrame(data, index=row_labels)
            df_unk.index.name = "event_id"
            df_unk = sanitize_excel_strings(df_unk)
            df_unk.to_excel(xw, sheet_name="unknown_year")

            # Apply wrap_text formatting to Results row
            worksheet = xw.sheets["unknown_year"]
            for col_idx in range(2, len(eids) + 2):
                cell = worksheet.cell(row=7, column=col_idx)
                cell.alignment = cell.alignment.copy(wrap_text=True)


def run_stage3_qc(records: list[dict], results_map: dict, out_dir: Path) -> None:
    """Run Stage 3 QC checks on Excel workbook data and write outputs."""
    print("\n" + "="*60)
    print("Running Stage 3 QC: Excel Cell Scanning")
    print("="*60)

    # Run Stage 3 slop detection checks
    issues = run_slop_detection_checks_stage3_excel(records, results_map)

    # Build summary
    counts_by_check = defaultdict(lambda: {"ERROR": 0, "WARN": 0, "INFO": 0})
    for issue in issues:
        issue_dict = issue.to_dict() if hasattr(issue, 'to_dict') else issue
        counts_by_check[issue_dict["check_id"]][issue_dict["severity"]] += 1

    total_errors = sum(1 for i in issues if (i.to_dict() if hasattr(i, 'to_dict') else i)["severity"] == "ERROR")
    total_warnings = sum(1 for i in issues if (i.to_dict() if hasattr(i, 'to_dict') else i)["severity"] == "WARN")
    total_info = sum(1 for i in issues if (i.to_dict() if hasattr(i, 'to_dict') else i)["severity"] == "INFO")

    summary = {
        "stage": "stage3",
        "total_events": len(records),
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "total_info": total_info,
        "counts_by_check": dict(counts_by_check),
    }

    # Write Stage 3 QC outputs
    summary_path = out_dir / "stage3_qc_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Wrote: {summary_path}")

    issues_path = out_dir / "stage3_qc_issues.jsonl"
    with open(issues_path, "w", encoding="utf-8") as f:
        for issue in issues:
            issue_dict = issue.to_dict() if hasattr(issue, 'to_dict') else issue
            f.write(json.dumps(issue_dict, ensure_ascii=False) + "\n")
    print(f"Wrote: {issues_path} ({len(issues)} issues)")

    # Print summary
    print(f"\nStage 3 QC Results:")
    print(f"  Total issues: {len(issues)}")
    print(f"  Errors: {total_errors}")
    print(f"  Warnings: {total_warnings}")
    print(f"  Info: {total_info}")

    if counts_by_check:
        print(f"\nIssues by check:")
        for check_id in sorted(counts_by_check.keys()):
            counts = counts_by_check[check_id]
            err = counts.get("ERROR", 0)
            warn = counts.get("WARN", 0)
            info = counts.get("INFO", 0)
            print(f"  {check_id}: {err} errors, {warn} warnings, {info} info")

    print("="*60)


def print_verification_stats(records: list[dict], out_xlsx: Path) -> None:
    """Print verification gate statistics."""
    total = len(records)
    print(f"\n{'='*60}")
    print("VERIFICATION GATE: Stage 3 (Excel Output)")
    print(f"{'='*60}")
    print(f"Total events in output: {total}")

    if total == 0:
        return

    # Count by year
    by_year = {}
    unknown = 0
    for rec in records:
        year = rec.get("year")
        if year is not None:
            by_year[year] = by_year.get(year, 0) + 1
        else:
            unknown += 1

    years = sorted(by_year.keys())
    print(f"\nSheet count: {len(years)} year sheets" + (", 1 unknown_year sheet" if unknown else ""))

    if years:
        print(f"Year range: {min(years)} - {max(years)}")

    print("\nEvents per sheet (first 10):")
    for y in years[:10]:
        print(f"  {int(y)}.0: {by_year[y]} events")
    if len(years) > 10:
        print(f"  ... and {len(years) - 10} more year sheets")
    if unknown:
        print(f"  unknown_year: {unknown} events")

    # Spot check 10 events
    print("\nSpot check (10 sample events):")
    import random
    sample = random.sample(records, min(10, len(records)))
    for rec in sample:
        eid = rec.get("event_id")
        year = rec.get("year")
        name = str(rec.get("event_name", ""))[:30]
        placements = len(rec.get("placements", []))
        print(f"  {eid:6s} | {year or '????'} | {name:30s} | {placements} placements")

    print(f"\nOutput file: {out_xlsx}")
    print(f"{'='*60}\n")


def main():
    """
    Read stage2 CSV and output final Excel workbook.
    """
    repo_dir = Path(__file__).resolve().parent
    out_dir = repo_dir / "out"
    in_csv = out_dir / "stage2_canonical_events.csv"
    out_xlsx = repo_dir / "Footbag_Results_Canonical.xlsx"

    if not in_csv.exists():
        print(f"ERROR: Input file not found: {in_csv}")
        print("Run 02_canonicalize_results.py first.")
        return

    print(f"Reading: {in_csv}")
    records = read_stage2_csv(in_csv)

    print(f"Writing Excel with {len(records)} events...")
    write_excel(out_xlsx, records)

    # Build results_map for Stage 3 QC
    results_map = {}
    for rec in records:
        eid = rec.get("event_id")
        if eid:
            placements = rec.get("placements", [])
            results_map[str(eid)] = format_results_from_placements(placements)

    # Run Stage 3 QC on Excel workbook data
    if USE_MASTER_QC:
        qc_summary, qc_issues = run_qc_for_stage("stage3", records, results_map=results_map, out_dir=out_dir)
        print_qc_summary(qc_summary, "stage3")
    else:
        print("Skipping Stage 3 QC (qc_master not available)")

    print_verification_stats(records, out_xlsx)
    print(f"Wrote: {out_xlsx}")


if __name__ == "__main__":
    main()
