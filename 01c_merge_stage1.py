#!/usr/bin/env python3
"""
01c_merge_stage1.py

Deterministic, validated merge of Stage 1 raw events:
- out/stage1_raw_events.csv        (from 01_parse_mirror.py)
- out/stage1_raw_events_old.csv    (from 01b_import_old_results.py)

Writes:
- out/stage1_raw_events.csv        (replaced with merged content)
- out/stage1_merge_summary.json    (counts + schema + collision info)

Policy:
- No guessing. If anything looks unsafe/ambiguous, FAIL (exit non-zero),
  except for optional "allow_*" flags.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional


@dataclass
class MergeSummary:
    timestamp_utc: str
    mirror_path: str
    old_path: str
    output_path: str
    summary_path: str

    mirror_rows: int
    old_rows: int
    merged_rows: int

    header_mirror: List[str]
    header_old: List[str]
    header_equal: bool

    event_id_column: str
    mirror_event_id_dupes: int
    old_event_id_dupes: int
    cross_source_collisions: int
    cross_collision_examples: List[str]

    mirror_year_minmax: Optional[Tuple[str, str]]
    old_year_minmax: Optional[Tuple[str, str]]

    notes: List[str]


def _read_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            raise ValueError(f"Empty CSV (no header): {path}")


def _count_rows_and_ids(
    path: Path,
    event_id_col: str,
    year_col: Optional[str],
    header: List[str],
) -> Tuple[int, Counter, Optional[Tuple[str, str]]]:
    """
    Returns:
      (data_rows_count, event_id_counter, (year_min, year_max) or None)
    """
    if event_id_col not in header:
        raise ValueError(f"Required column '{event_id_col}' not found in {path.name} header")

    idx_event = header.index(event_id_col)
    idx_year = header.index(year_col) if (year_col and year_col in header) else None

    cnt = Counter()
    years: List[str] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        _ = next(reader, None)  # header
        rows = 0
        for row in reader:
            if not row:
                continue
            rows += 1
            if idx_event < len(row):
                eid = row[idx_event].strip()
                if eid:
                    cnt[eid] += 1
            if idx_year is not None and idx_year < len(row):
                y = row[idx_year].strip()
                if y:
                    years.append(y)

    year_minmax = None
    if years:
        # Year strings may be numeric; keep as strings but sort by int when possible.
        def _key(v: str):
            try:
                return int(v)
            except Exception:
                return v

        years_sorted = sorted(years, key=_key)
        year_minmax = (years_sorted[0], years_sorted[-1])

    return rows, cnt, year_minmax


def _write_merged(
    mirror_path: Path,
    old_path: Path,
    output_path: Path,
    header: List[str],
) -> int:
    """
    Writes merged output with a single header, then mirror rows, then old rows.
    Returns merged data row count.
    """
    tmp_path = output_path.with_suffix(".csv.tmp")

    def _copy_rows(src: Path, writer: csv.writer) -> int:
        with src.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # skip header
            n = 0
            for row in reader:
                if not row:
                    continue
                writer.writerow(row)
                n += 1
            return n

    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    with tmp_path.open("w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(header)
        n_m = _copy_rows(mirror_path, writer)
        n_o = _copy_rows(old_path, writer)

    os.replace(tmp_path, output_path)
    return n_m + n_o


def main() -> int:
    ap = argparse.ArgumentParser(description="Validated Stage 1 merge for raw events CSVs")
    ap.add_argument("--out-dir", default="out", help="Output directory (default: out)")
    ap.add_argument("--mirror", default=None, help="Path to stage1_raw_events.csv (default: <out-dir>/stage1_raw_events.csv)")
    ap.add_argument("--old", default=None, help="Path to stage1_raw_events_old.csv (default: <out-dir>/stage1_raw_events_old.csv)")
    ap.add_argument("--output", default=None, help="Output path (default: <out-dir>/stage1_raw_events.csv)")
    ap.add_argument("--summary", default=None, help="Summary JSON path (default: <out-dir>/stage1_merge_summary.json)")

    ap.add_argument("--event-id-col", default="event_id", help="Event id column name (default: event_id)")
    ap.add_argument("--year-col", default="year", help="Year column name, for reporting only (default: year)")

    ap.add_argument("--allow-header-mismatch", action="store_true",
                    help="Allow header mismatch (NOT recommended). Still writes using mirror header.")
    ap.add_argument("--allow-cross-collisions", action="store_true",
                    help="Allow event_id collisions between sources (NOT recommended).")
    ap.add_argument("--fail-on-internal-dupes", action="store_true",
                    help="Fail if duplicates exist within either source by event_id (default: warn only).")

    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    mirror_path = Path(args.mirror) if args.mirror else (out_dir / "stage1_raw_events.csv")
    old_path = Path(args.old) if args.old else (out_dir / "stage1_raw_events_old.csv")
    output_path = Path(args.output) if args.output else (out_dir / "stage1_raw_events.csv")
    summary_path = Path(args.summary) if args.summary else (out_dir / "stage1_merge_summary.json")

    notes: List[str] = []

    if not mirror_path.exists():
        print(f"ERROR: missing mirror CSV: {mirror_path}", file=sys.stderr)
        return 2
    if not old_path.exists():
        print(f"ERROR: missing old CSV: {old_path}", file=sys.stderr)
        return 2

    header_m = _read_header(mirror_path)
    header_o = _read_header(old_path)
    header_equal = header_m == header_o

    if not header_equal:
        msg = (
            "Header mismatch between mirror and old CSV.\n"
            f"  mirror: {header_m}\n"
            f"  old:    {header_o}\n"
        )
        if args.allow_header_mismatch:
            notes.append("WARNING: header mismatch allowed; using mirror header for output; old rows copied as-is.")
            print("WARN:", msg, file=sys.stderr)
        else:
            print("ERROR:", msg, file=sys.stderr)
            return 3

    # Count rows and ids
    mirror_rows, mirror_ids, mirror_year_minmax = _count_rows_and_ids(
        mirror_path, args.event_id_col, args.year_col, header_m
    )
    old_rows, old_ids, old_year_minmax = _count_rows_and_ids(
        old_path, args.event_id_col, args.year_col, header_o
    )

    mirror_internal_dupes = sum(1 for k, v in mirror_ids.items() if v > 1)
    old_internal_dupes = sum(1 for k, v in old_ids.items() if v > 1)

    if (mirror_internal_dupes or old_internal_dupes) and args.fail_on_internal_dupes:
        print(
            f"ERROR: internal event_id duplicates detected "
            f"(mirror unique-dupe-ids={mirror_internal_dupes}, old unique-dupe-ids={old_internal_dupes}).",
            file=sys.stderr,
        )
        return 4
    elif mirror_internal_dupes or old_internal_dupes:
        notes.append(
            f"WARNING: internal event_id duplicates present "
            f"(mirror unique-dupe-ids={mirror_internal_dupes}, old unique-dupe-ids={old_internal_dupes})."
        )

    # Cross-source collisions
    collisions = sorted(set(mirror_ids.keys()) & set(old_ids.keys()))
    collision_examples = collisions[:20]
    if collisions and not args.allow_cross_collisions:
        print(
            f"ERROR: event_id collisions between mirror and old: count={len(collisions)} "
            f"(examples={collision_examples})",
            file=sys.stderr,
        )
        return 5
    elif collisions:
        notes.append(f"WARNING: cross-source event_id collisions allowed: count={len(collisions)}")

    # Write merged
    merged_rows = _write_merged(mirror_path, old_path, output_path, header_m)

    # Final sanity: merged rows should match sum
    expected = mirror_rows + old_rows
    if merged_rows != expected:
        # This should basically never happen unless weird blank-row handling differs.
        notes.append(f"WARNING: merged row count {merged_rows} != expected {expected}")

    summary = MergeSummary(
        timestamp_utc=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        mirror_path=str(mirror_path),
        old_path=str(old_path),
        output_path=str(output_path),
        summary_path=str(summary_path),

        mirror_rows=mirror_rows,
        old_rows=old_rows,
        merged_rows=merged_rows,

        header_mirror=header_m,
        header_old=header_o,
        header_equal=header_equal,

        event_id_column=args.event_id_col,
        mirror_event_id_dupes=mirror_internal_dupes,
        old_event_id_dupes=old_internal_dupes,
        cross_source_collisions=len(collisions),
        cross_collision_examples=collision_examples,

        mirror_year_minmax=mirror_year_minmax,
        old_year_minmax=old_year_minmax,

        notes=notes,
    )

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary.__dict__, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(
        f"OK: merged {mirror_rows} (mirror) + {old_rows} (old) -> {merged_rows} rows\n"
        f"  output:  {output_path}\n"
        f"  summary: {summary_path}"
    )
    if notes:
        for n in notes:
            print(n)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

