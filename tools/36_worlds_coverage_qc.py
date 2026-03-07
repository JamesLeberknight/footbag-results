#!/usr/bin/env python3
"""
36_worlds_coverage_qc.py — IFPA Worlds coverage completeness audit.

For every event whose name contains "world" (case-insensitive) and whose
year is >= 1997, checks for presence of the basic disciplines that a Worlds
event is expected to contain, and classifies each gap by root cause.

Expected discipline families (checked by keyword against division_canon):
  NET_OPEN_SINGLES   — Open/Pro Singles Net
  NET_WOMEN_SINGLES  — Women's Singles Net
  NET_OPEN_DOUBLES   — Open/Pro Doubles Net (excl. Mixed/Women/Intermediate)
  NET_GENDER_DOUBLES — Women's Doubles Net OR Mixed Doubles Net
  FS_OPEN_SINGLES    — Open Singles Freestyle / Routines / Battles (2003+)
  FS_WOMEN_SINGLES   — Women's Freestyle singles event (2006+)
  FS_OPEN_DOUBLES    — Open Doubles Freestyle / Routines (2004+)
  FS_WOMEN_DOUBLES   — Women's Doubles Freestyle / Routines (2006+)

Root-cause tags:
  EXTERNAL_LINK      — full results were linked to a URL not in the mirror
  INCOMPLETE_POST    — source page was partially populated by organiser
  NOT_IN_MIRROR      — event page exists but results directory missing
  PARTIAL            — some divisions present, others silently absent
  OK                 — all expected disciplines present

Usage:
  python tools/36_worlds_coverage_qc.py [--csv out/worlds_coverage_qc.csv]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"
STAGE2_CSV = OUT / "stage2_canonical_events.csv"

# ---------------------------------------------------------------------------
# Known root-cause overrides for specific events
# (event_id → (root_cause_tag, explanation))
# ---------------------------------------------------------------------------
KNOWN_CAUSES: dict[str, tuple[str, str]] = {
    "915561090":  ("EXTERNAL_LINK",
                   "Full results at /worlds99/results/results.html not mirrored; "
                   "freestyle at http://www.wam.umd.edu/~rvbpaco/1999WorldsFreestyle.html (off-site)"),
    "1706036811": ("INCOMPLETE_POST",
                   "Women's freestyle divisions (Routines, Battles, Shred 30, Sick 3) "
                   "were not posted to footbag.org by the 2024 organiser"),
    "1002342588": ("NOT_IN_MIRROR",   "Mirror missing results for this event"),
    "1449259560": ("INCOMPLETE_POST",
                   "2016 Prague Worlds — source page says 'Results added as they become available'; "
                   "only Women's Singles Net + 5 freestyle disciplines posted; Open Singles Net never added"),
    "1194517448": ("INCOMPLETE_POST",
                   "2008 Bordeaux Worlds — Open Singles Net + freestyle-only posted; "
                   "net doubles and Women's Singles Net absent from source"),
    "1471686537": ("INCOMPLETE_POST", "2017 Worlds results sparsely posted; missing many freestyle divisions"),
    "2001980001": ("PARTIAL", "Pre-mirror stub — only partial net results available"),
    "2001981001": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001982001": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001982002": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001983001": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001983002": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001983003": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001983004": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001984001": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001984002": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001985001": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001985002": ("PARTIAL", "Pre-mirror stub — partial coverage"),
    "2001986001": ("PARTIAL", "Pre-mirror stub — partial coverage"),
}

# ---------------------------------------------------------------------------
# Discipline detection helpers
# ---------------------------------------------------------------------------

def _divs(placements: list[dict]) -> set[str]:
    """Return lowercased division_canon set."""
    return {(p.get("division_canon") or "").lower().strip() for p in placements}


def _has(divs: set[str], *keywords: str) -> bool:
    """True if any division contains ALL of the given keywords."""
    for d in divs:
        if all(kw in d for kw in keywords):
            return True
    return False


def check_net_open_singles(divs: set[str]) -> bool:
    # "open singles net", "pro open singles net", "pro singles net" etc.
    for d in divs:
        if "singles" in d and "net" in d:
            if not any(x in d for x in ("women", "intermediate", "master")):
                return True
    return False


def check_net_women_singles(divs: set[str]) -> bool:
    for d in divs:
        if "women" in d and "singles" in d and "net" in d and "doubles" not in d:
            return True
    return False


def check_net_open_doubles(divs: set[str]) -> bool:
    for d in divs:
        if "doubles" in d and "net" in d:
            if not any(x in d for x in ("women", "intermediate", "master", "mixed")):
                return True
    return False


def check_net_gender_doubles(divs: set[str]) -> bool:
    """Women's Doubles Net OR Mixed Doubles Net."""
    for d in divs:
        if "doubles" in d and "net" in d:
            if "women" in d or "mixed" in d:
                return True
    return False


def check_fs_open_singles(divs: set[str]) -> bool:
    """Open singles freestyle event: routines / battles / shred / sick / freestyle."""
    fs_kws = ("routines", "battles", "shred", "sick", "freestyle", "request", "circle")
    for d in divs:
        if any(k in d for k in fs_kws):
            if not any(x in d for x in ("women", "intermediate", "doubles", "mixed")):
                return True
    return False


def check_fs_women_singles(divs: set[str]) -> bool:
    fs_kws = ("routines", "battles", "shred", "sick", "freestyle", "request", "circle")
    for d in divs:
        if "women" in d and any(k in d for k in fs_kws) and "doubles" not in d:
            return True
    return False


def check_fs_open_doubles(divs: set[str]) -> bool:
    fs_kws = ("routines", "battles", "freestyle")
    for d in divs:
        if "doubles" in d and any(k in d for k in fs_kws):
            if not any(x in d for x in ("women", "intermediate", "mixed")):
                return True
    return False


def check_fs_women_doubles(divs: set[str]) -> bool:
    fs_kws = ("routines", "battles", "freestyle")
    for d in divs:
        if "women" in d and "doubles" in d and any(k in d for k in fs_kws):
            return True
    return False


# ---------------------------------------------------------------------------
# Per-event audit
# ---------------------------------------------------------------------------

DISCIPLINE_CHECKS = [
    # (key, label, check_fn, applies_from_year, severity)
    ("NET_OPEN_SINGLES",   "Open Singles Net",          check_net_open_singles,   1997, "ERROR"),
    ("NET_WOMEN_SINGLES",  "Women's Singles Net",        check_net_women_singles,  1997, "WARN"),
    ("NET_OPEN_DOUBLES",   "Open Doubles Net",           check_net_open_doubles,   1997, "WARN"),
    ("NET_GENDER_DOUBLES", "Women's/Mixed Doubles Net",  check_net_gender_doubles, 1997, "WARN"),
    ("FS_OPEN_SINGLES",    "Open Freestyle Singles",     check_fs_open_singles,    2003, "WARN"),
    ("FS_WOMEN_SINGLES",   "Women's Freestyle Singles",  check_fs_women_singles,   2006, "WARN"),
    ("FS_OPEN_DOUBLES",    "Open Freestyle Doubles",     check_fs_open_doubles,    2004, "WARN"),
    ("FS_WOMEN_DOUBLES",   "Women's Freestyle Doubles",  check_fs_women_doubles,   2006, "WARN"),
]

# Events that are satellite / warmup / online — relax expectations
RELAXED_EVENTS = {
    "987361779",   # Cornerstone Festival circle record
    "1623054449",  # 2021 Online Worlds (different format)
    "857881519",   # Vancouver Open (worlds warm-up, not the actual Worlds)
    "2001980001", "2001981001", "2001982001", "2001982002",
    "2001983001", "2001983002", "2001983003", "2001983004",
    "2001984001", "2001984002", "2001985001", "2001985002",
    "2001986001",
}


def audit_event(event_id: str, year: int, name: str,
                placements: list[dict]) -> dict:
    divs = _divs(placements)
    n_placements = len(placements)
    div_list = sorted({p.get("division_canon","") for p in placements})
    n_divs = len(div_list)

    missing = []
    present = []
    relaxed = event_id in RELAXED_EVENTS

    for key, label, fn, from_year, sev in DISCIPLINE_CHECKS:
        if year < from_year:
            continue
        if fn(divs):
            present.append(label)
        else:
            if not relaxed:
                missing.append((key, label, sev))

    # Derive root cause
    cause, cause_note = KNOWN_CAUSES.get(event_id, (None, ""))
    if cause is None:
        if not missing:
            cause = "OK"
        elif n_placements < 20:
            cause = "PARTIAL"
            cause_note = "Very few placements; likely limited source posting"
        elif n_divs <= 2:
            cause = "PARTIAL"
            cause_note = f"Only {n_divs} division(s) found; most results likely on external/unmirrored page"
        else:
            cause = "PARTIAL"
            cause_note = "Some disciplines present, others absent from source"

    errors   = [m for m in missing if m[2] == "ERROR"]
    warnings = [m for m in missing if m[2] == "WARN"]

    return {
        "event_id":    event_id,
        "year":        year,
        "event_name":  name,
        "n_placements": n_placements,
        "n_divs":      n_divs,
        "divisions":   div_list,
        "present":     present,
        "missing_errors":   [m[1] for m in errors],
        "missing_warnings": [m[1] for m in warnings],
        "root_cause":  cause,
        "cause_note":  cause_note,
        "relaxed":     relaxed,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", help="Write results to CSV file")
    args = parser.parse_args()

    if not STAGE2_CSV.exists():
        print(f"ERROR: {STAGE2_CSV} not found — run 'make rebuild' first", file=sys.stderr)
        sys.exit(1)

    worlds_events = []
    with open(STAGE2_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("event_name", "")
            if "world" not in name.lower():
                continue
            try:
                year = int(row.get("year") or 0)
            except ValueError:
                year = 0
            if year < 1997:
                continue
            placements = json.loads(row.get("placements_json", "[]"))
            worlds_events.append(audit_event(
                event_id=row["event_id"],
                year=year,
                name=name,
                placements=placements,
            ))

    worlds_events.sort(key=lambda r: (r["year"], r["event_name"]))

    # ── Print report ──────────────────────────────────────────────────────────
    sep = "─" * 76
    print()
    print("=" * 76)
    print("  WORLDS DIVISION COVERAGE AUDIT")
    print("=" * 76)

    total_errors   = 0
    total_warnings = 0
    problem_events = []

    for r in worlds_events:
        if r["relaxed"]:
            continue
        errs  = r["missing_errors"]
        warns = r["missing_warnings"]
        if not errs and not warns:
            continue

        total_errors   += len(errs)
        total_warnings += len(warns)
        problem_events.append(r)

        tag = "ERROR" if errs else "WARN"
        print(f"\n[{tag}] {r['year']} — {r['event_name']}")
        print(f"       event_id={r['event_id']}  placements={r['n_placements']}  divisions={r['n_divs']}")
        print(f"       root_cause: {r['root_cause']}")
        if r["cause_note"]:
            print(f"       note: {r['cause_note']}")
        if errs:
            print(f"       MISSING (ERROR): {', '.join(errs)}")
        if warns:
            print(f"       MISSING (WARN):  {', '.join(warns)}")
        print(f"       present: {', '.join(r['present']) or '(none)'}")
        print(f"       divisions in data: {', '.join(r['divisions'][:8])}" +
              (f"  +{len(r['divisions'])-8} more" if len(r['divisions']) > 8 else ""))

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print(sep)
    n_ok = len([r for r in worlds_events if not r["relaxed"]
                and not r["missing_errors"] and not r["missing_warnings"]])
    n_relaxed = len([r for r in worlds_events if r["relaxed"]])
    print(f"  Worlds events audited:  {len(worlds_events) - n_relaxed}")
    print(f"  Fully covered:          {n_ok}")
    print(f"  With ERROR gaps:        {len([r for r in problem_events if r['missing_errors']])}")
    print(f"  With WARN gaps:         {len([r for r in problem_events if not r['missing_errors']])}")
    print(f"  Total missing (ERROR):  {total_errors}")
    print(f"  Total missing (WARN):   {total_warnings}")
    print(f"  Skipped (satellite/relaxed): {n_relaxed}")
    print(sep)

    # ── Gap priority table ────────────────────────────────────────────────────
    print()
    print("  RECOVERY PRIORITY (events with ERROR or WARN gaps, sorted by severity):")
    print()
    print(f"  {'Year':<6} {'Placements':>10}  {'Root cause':<18}  {'Missing disciplines'}")
    print(f"  {'----':<6} {'-----------':>10}  {'----------':<18}  {'-------------------'}")
    for r in sorted(problem_events,
                    key=lambda x: (0 if x["missing_errors"] else 1, x["year"])):
        missing_all = r["missing_errors"] + r["missing_warnings"]
        print(f"  {r['year']:<6} {r['n_placements']:>10}  {r['root_cause']:<18}  {', '.join(missing_all)}")

    # ── CSV output ────────────────────────────────────────────────────────────
    if args.csv:
        out_path = Path(args.csv)
        fieldnames = ["year", "event_id", "event_name", "n_placements", "n_divs",
                      "root_cause", "cause_note",
                      "missing_errors", "missing_warnings", "present", "divisions"]
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in worlds_events:
                row = dict(r)
                row["missing_errors"]   = "; ".join(r["missing_errors"])
                row["missing_warnings"] = "; ".join(r["missing_warnings"])
                row["present"]          = "; ".join(r["present"])
                row["divisions"]        = "; ".join(r["divisions"])
                w.writerow(row)
        print(f"\n  CSV written → {out_path}")

    print()
    if total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
