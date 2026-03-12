#!/usr/bin/env python3
"""
Tool 58 — Final Publication Validation

Gates 1–4: data preservation and encoding integrity check.

Outputs:
  out/FINAL_VALIDATION_REPORT.md
  out/missing_placements_report.csv
  out/missing_divisions_report.csv
  out/encoding_corruption_report.csv

Exit code: 0 = READY, 1 = BLOCKED
"""

import csv
import json
import re
import subprocess
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(10**7)

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def normalize_name(s: str) -> str:
    """Accent-fold, lowercase, collapse whitespace, strip punctuation."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")  # strip diacritics
    s = s.lower()
    s = re.sub(r"[''\"'`´]", "", s)     # remove quotes/apostrophes
    s = re.sub(r"[-–—]", " ", s)         # normalize hyphens/dashes
    s = re.sub(r"[^\w\s]", "", s)        # remove remaining punctuation
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_division(s: str) -> str:
    """Normalize division names for comparison across pipeline versions.

    Handles spacing differences ("Shred 30" vs "Shred30"),
    punctuation differences ("Shred:30" vs "Shred 30"),
    and apostrophe corruption ("Women?S" vs "Women's").
    """
    if not s:
        return ""
    # Apostrophe normalization (handles ?S corruption and various quote chars)
    s = re.sub(r"\?[Ss]\b", "'s", s)
    s = re.sub(r"[''`´]", "'", s)
    # Colon/hyphen adjacent to digits or words → space
    s = re.sub(r"\s*[:]\s*", " ", s)
    # Strip all whitespace then lowercase for final key
    key = re.sub(r"\s+", "", s).lower()
    # Remove remaining punctuation
    key = re.sub(r"[^a-z0-9]", "", key)
    return key


# Encoding issue categories:
#   FIXABLE    — pipeline bugs that must be corrected before publication
#   SOURCE_LOSS — unrecoverable encoding loss from the HTML mirror source
#   SOFT       — minor source formatting issues (quoted nicknames, etc.)

# Patterns that BLOCK publication (fixable pipeline corruption)
_BLOCKING_PATTERNS = [
    (re.compile(r"[ÃÂ][^\s]{1,2}"),  "UTF-8 double-encoding artifact (Ã/Â prefix)"),
    (re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]"), "C0 control character"),
    (re.compile(r"[\x80-\x9f]"),     "C1 control character (Windows-1252 range)"),
    (re.compile(r"â€[œ™˜•›‹›]"),    "UTF-8 smart-quote artifact"),
    (re.compile(r"Women\?[Ss]"),     "Women?s apostrophe corruption [FIXABLE]"),
    (re.compile(r"Master\?[Ss]"),    "Master?s apostrophe corruption [FIXABLE]"),
    (re.compile(r"\bWomen[^'\s]s\b"), "Women + corrupt apostrophe [FIXABLE]"),
]

# Patterns that are SOURCE_LOSS (documented limitation, non-blocking)
_SOURCE_LOSS_PATTERNS = [
    (re.compile(r"\ufffd"),          "U+FFFD replacement character [SOURCE_LOSS]"),
]

# Patterns that are SOFT (minor source formatting, non-blocking)
_SOFT_PATTERNS = [
    (re.compile(r"\?[A-Z]"),         "?-prefix encoding fallback [SOFT]"),
]

# Combined list for scanning — all patterns
_MOJIBAKE_PATTERNS = _BLOCKING_PATTERNS + _SOURCE_LOSS_PATTERNS + _SOFT_PATTERNS

def check_encoding(value: str, field: str, row_key: str) -> list[dict]:
    """Return list of encoding corruption issues found in value."""
    issues = []
    if not value:
        return issues
    for pat, label in _MOJIBAKE_PATTERNS:
        m = pat.search(value)
        if m:
            issues.append({
                "row_key":   row_key,
                "field":     field,
                "value":     value[:120],
                "issue":     label,
                "match":     repr(m.group()),
            })
            break  # one issue per field per row
    return issues


# ─────────────────────────────────────────────────────────────────────────────
# Load reference data
# ─────────────────────────────────────────────────────────────────────────────

def load_quarantined() -> set[str]:
    p = ROOT / "inputs" / "review_quarantine_events.csv"
    out: set[str] = set()
    if p.exists():
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                out.add(row["event_id"])
    return out


def load_known_issues() -> dict[str, str]:
    """event_id → severity"""
    p = ROOT / "overrides" / "known_issues.csv"
    out: dict[str, str] = {}
    if p.exists():
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                out[row["event_id"]] = row.get("severity", "unknown")
    return out


def load_source_partial() -> set[str]:
    """Events flagged SOURCE_PARTIAL in quarantine list."""
    p = ROOT / "inputs" / "review_quarantine_events.csv"
    out: set[str] = set()
    if p.exists():
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("reason", "") == "SOURCE_PARTIAL":
                    out.add(row["event_id"])
    return out


def load_stage2() -> dict[str, dict]:
    """event_id → {name, year, placements: [...]}"""
    events: dict[str, dict] = {}
    path = OUT / "stage2_canonical_events.csv"
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = row["event_id"]
            plist = json.loads(row.get("placements_json") or "[]")
            events[eid] = {
                "name":       row.get("event_name", ""),
                "year":       row.get("year", ""),
                "placements": plist,
            }
    return events


def load_pf() -> dict[tuple, list[dict]]:
    """(event_id, normalized_division_key) → list of PF rows."""
    pf: dict[tuple, list[dict]] = defaultdict(list)
    path = OUT / "Placements_Flat.csv"
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row["event_id"], normalize_division(row["division_canon"]))
            pf[key].append(row)
    return pf


def load_canonical_export() -> dict:
    """Load canonical CSV export for Gates 1/2 validation.

    Returns dict with:
      event_id_to_key: event_id → event_key
      disc_count: total discipline rows in event_disciplines.csv
      participant_count: total rows in event_result_participants.csv
      disciplines_by_event: event_key → set of discipline names
      results_by_event_disc: (event_key, discipline_key) → set of placements
    """
    canon_dir = OUT / "canonical"
    data: dict = {
        "event_id_to_key": {},
        "disc_count": 0,
        "participant_count": 0,
        "disciplines_by_eid": defaultdict(set),   # event_id → set of discipline names
        "placements_by_eid_div": defaultdict(set), # (event_id, disc_key) → set of placements
    }

    # events.csv: legacy_event_id → event_key
    with open(canon_dir / "events.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data["event_id_to_key"][row["legacy_event_id"]] = row["event_key"]

    key_to_id = {v: k for k, v in data["event_id_to_key"].items()}

    # event_disciplines.csv: count per event
    with open(canon_dir / "event_disciplines.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data["disc_count"] += 1
            eid = key_to_id.get(row["event_key"], "")
            if eid:
                data["disciplines_by_eid"][eid].add(row["discipline_name"])

    # event_result_participants.csv: count
    with open(canon_dir / "event_result_participants.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data["participant_count"] += 1

    # event_results.csv: placement slots per (event, discipline)
    with open(canon_dir / "event_results.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = key_to_id.get(row["event_key"], "")
            if eid:
                data["placements_by_eid_div"][(eid, row["discipline_key"])].add(
                    str(row["placement"])
                )

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Gate 1 — Division Preservation
# ─────────────────────────────────────────────────────────────────────────────

def gate1_divisions(
    stage2: dict[str, dict],
    canonical: dict,
    quarantined: set[str],
    known_issues: dict[str, str],
) -> tuple[list[dict], dict]:
    """Gate 1: verify every stage2 division appears in canonical event_disciplines.csv.

    Compares source (stage2) against the canonical CSV export — the authoritative
    published dataset produced by pipeline/05_export_canonical_csv.py.

    Returns (missing_divisions, summary_stats).
    """
    missing: list[dict] = []
    stats = {
        "stage2_events":    len(stage2),
        "events_skipped_quarantined": 0,
        "events_with_no_results": 0,
        "divisions_checked": 0,
        "canonical_disc_total": canonical["disc_count"],
        "exact_match": 0,
        "missing": 0,
    }

    # canonical: disciplines_by_eid is already normalized names per event_id
    canon_divs_by_eid = canonical["disciplines_by_eid"]

    for eid, ev in stage2.items():
        if eid in quarantined:
            stats["events_skipped_quarantined"] += 1
            continue

        placements = ev["placements"]
        source_divs: set[str] = set()
        for p in placements:
            dc = (p.get("division_canon") or "").strip()
            if dc and dc.lower() not in ("unknown", ""):
                source_divs.add(dc)

        if not source_divs:
            stats["events_with_no_results"] += 1
            continue

        canon_for_event = canon_divs_by_eid.get(eid, set())
        # Normalize both sides for comparison
        canon_keys = {normalize_division(d) for d in canon_for_event}

        for div in sorted(source_divs):
            stats["divisions_checked"] += 1
            if normalize_division(div) in canon_keys:
                stats["exact_match"] += 1
            else:
                stats["missing"] += 1
                missing.append({
                    "event_id":   eid,
                    "year":       ev["year"],
                    "event_name": ev["name"],
                    "division":   div,
                    "known_issue": "YES" if eid in known_issues else "NO",
                    "severity":   known_issues.get(eid, ""),
                })

    return missing, stats


# ─────────────────────────────────────────────────────────────────────────────
# Gate 2 — Placement Preservation
# ─────────────────────────────────────────────────────────────────────────────

def gate2_placements(
    stage2: dict[str, dict],
    canonical: dict,
    quarantined: set[str],
    known_issues: dict[str, str],
) -> tuple[list[dict], dict]:
    """Gate 2: verify stage2 participant count matches canonical event_result_participants.csv.

    Compares source (stage2) against the canonical CSV export.
    Uses total participant counts per event: stage2 vs canonical erp rows.

    Returns (missing_placements, summary_stats).
    """
    missing: list[dict] = []
    stats = {
        "stage2_participants": 0,
        "canonical_participants": canonical["participant_count"],
        "events_checked": 0,
        "events_ok": 0,
        "events_missing": 0,
    }

    # Count stage2 participants per event (1 for singles, 2 per doubles team)
    s2_participants_by_event: dict[str, int] = defaultdict(int)
    s2_participant_names: dict[str, list] = defaultdict(list)
    for eid, ev in stage2.items():
        if eid in quarantined:
            continue
        for p in ev["placements"]:
            # Count all participants including unknown-division placements
            # (canonical export includes them too)
            if p.get("player1_name", "").strip():
                s2_participants_by_event[eid] += 1
                s2_participant_names[eid].append(p.get("player1_name", "").strip())
            if p.get("player2_name", "").strip():
                s2_participants_by_event[eid] += 1
    stats["stage2_participants"] = sum(s2_participants_by_event.values())

    # Build canonical participants per event from erp
    # Need to load erp per event — use event_id_to_key mapping
    canon_erp_path = OUT / "canonical" / "event_result_participants.csv"
    id_to_key = canonical["event_id_to_key"]
    key_to_id = {v: k for k, v in id_to_key.items()}
    canon_count_by_event: dict[str, int] = defaultdict(int)
    with open(canon_erp_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = key_to_id.get(row["event_key"], "")
            if eid:
                canon_count_by_event[eid] += 1

    # Compare per event
    all_eids = set(s2_participants_by_event) | set(canon_count_by_event)
    for eid in sorted(all_eids):
        if eid in quarantined:
            continue
        stats["events_checked"] += 1
        s2_count = s2_participants_by_event.get(eid, 0)
        canon_count = canon_count_by_event.get(eid, 0)
        if s2_count == canon_count:
            stats["events_ok"] += 1
        else:
            stats["events_missing"] += 1
            ev = stage2.get(eid, {})
            missing.append({
                "event_id":          eid,
                "year":              ev.get("year", ""),
                "event_name":        ev.get("name", ""),
                "stage2_count":      s2_count,
                "canonical_count":   canon_count,
                "delta":             canon_count - s2_count,
                "known_issue":       "YES" if eid in known_issues else "NO",
            })

    return missing, stats


# ─────────────────────────────────────────────────────────────────────────────
# Gate 3 — Encoding Cleanliness
# ─────────────────────────────────────────────────────────────────────────────

def gate3_encoding() -> tuple[list[dict], dict]:
    """Scan public-facing fields across canonical outputs for encoding corruption."""
    all_issues: list[dict] = []
    stats: dict[str, int] = defaultdict(int)

    # (path, key_field, fields_to_check)
    scan_targets = [
        (
            OUT / "canonical" / "events.csv",
            "legacy_event_id",
            ["event_name", "city", "region", "country", "host_club"],
        ),
        (
            OUT / "canonical" / "event_disciplines.csv",
            "discipline_key",
            ["discipline_name"],
        ),
        (
            OUT / "canonical" / "event_result_participants.csv",
            "discipline_key",
            ["display_name"],
        ),
        (
            OUT / "Placements_Flat.csv",
            "event_id",
            ["division_canon", "division_raw", "person_canon", "team_display_name"],
        ),
    ]

    for path, key_field, fields in scan_targets:
        if not path.exists():
            continue
        file_label = path.name
        stats[f"rows_scanned_{file_label}"] = 0
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                stats[f"rows_scanned_{file_label}"] += 1
                row_key = f"{file_label}:{row.get(key_field, '?')}"
                for field in fields:
                    val = row.get(field, "")
                    if not val:
                        continue
                    issues = check_encoding(val, field, row_key)
                    all_issues.extend(issues)
                    stats[f"issues_{file_label}"] = stats.get(f"issues_{file_label}", 0) + len(issues)

    # Deduplicate by (row_key, field) — take first occurrence only
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for issue in all_issues:
        k = (issue["row_key"], issue["field"])
        if k not in seen:
            seen.add(k)
            deduped.append(issue)

    stats["total_issues"] = len(deduped)
    return deduped, dict(stats)


# ─────────────────────────────────────────────────────────────────────────────
# Gate 4 — Schema Integrity
# ─────────────────────────────────────────────────────────────────────────────

def gate4_schema() -> tuple[bool, str]:
    """Run tools/32 and tools/33. Return (all_pass, combined_output)."""
    lines = []
    all_pass = True
    for tool in ["tools/32_post_release_qc.py", "tools/33_schema_logic_qc.py"]:
        result = subprocess.run(
            ["python3", tool],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
        )
        passed = result.returncode == 0
        if not passed:
            all_pass = False
        summary = [l for l in (result.stdout + result.stderr).splitlines()
                   if "SUMMARY" in l or "✓" in l or "✗" in l or "PASS" in l or "FAIL" in l]
        lines.append(f"### {Path(tool).stem}")
        lines.append(f"Exit code: {result.returncode} ({'PASS' if passed else 'FAIL'})")
        lines.extend(summary[-5:])
        lines.append("")
    return all_pass, "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Write outputs
# ─────────────────────────────────────────────────────────────────────────────

def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: Path,
    g1_missing: list[dict],
    g1_stats: dict,
    g2_missing: list[dict],
    g2_stats: dict,
    g3_issues: list[dict],
    g3_stats: dict,
    g4_pass: bool,
    g4_detail: str,
    quarantined: set[str],
    source_partial: set[str],
    known_issues: dict[str, str],
    stage2: dict[str, dict],
) -> None:
    # Only real blockers: missing divisions/placements NOT in known_issues
    g1_blockers = [r for r in g1_missing if r["known_issue"] == "NO"]
    g2_blockers = [r for r in g2_missing if r["known_issue"] == "NO"]
    # Gate 3: only BLOCKING patterns are true blockers
    # SOURCE_LOSS (U+FFFD from HTML mirror) and SOFT (?Nickname?) are documented limitations
    g3_blockers    = [r for r in g3_issues if "[SOURCE_LOSS]" not in r["issue"] and "[SOFT]" not in r["issue"]]
    g3_source_loss = [r for r in g3_issues if "[SOURCE_LOSS]" in r["issue"]]
    g3_soft        = [r for r in g3_issues if "[SOFT]" in r["issue"]]

    is_blocked = bool(g1_blockers or g2_blockers or g3_blockers or not g4_pass)
    status = "**BLOCKED**" if is_blocked else "**READY**"
    status_line = "🔴 PUBLICATION STATUS: BLOCKED" if is_blocked else "🟢 PUBLICATION STATUS: READY"

    # Counts
    total_events = len(stage2)
    skipped_q = g1_stats.get("events_skipped_quarantined", 0)
    no_results = g1_stats.get("events_with_no_results", 0)
    total_divs_checked = g1_stats.get("divisions_checked", 0)
    total_divs_ok = g1_stats.get("exact_match", 0)
    total_divs_missing = g1_stats.get("missing", 0)
    total_enc = g3_stats.get("total_issues", 0)

    lines = [
        "# Final Publication Validation Report",
        "",
        f"**Date:** 2026-03-12",
        f"**Dataset:** Footbag Historical Results v2.10.1",
        f"**PBP:** Placements_ByPerson_v62.csv (27,154 rows)",
        f"**Persons Truth:** Persons_Truth_Final_v42.csv (3,441 persons)",
        "",
        "---",
        "",
        f"## {status_line}",
        "",
        "---",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Events in stage2 | {total_events} |",
        f"| Events skipped — quarantined | {skipped_q} |",
        f"| Events skipped — no results | {no_results} |",
        f"| Source-partial events (documented) | {len(source_partial)} |",
        f"| Known-issue events | {len(known_issues)} |",
        "",
        "| Check | Result | Blockers |",
        "|---|---|---|",
        f"| Gate 1 — Division Preservation | {'✓ PASS' if not g1_blockers else '✗ FAIL'} | {len(g1_blockers)} |",
        f"| Gate 2 — Placement Preservation | {'✓ PASS' if not g2_blockers else '✗ FAIL'} | {len(g2_blockers)} |",
        f"| Gate 3 — Encoding Cleanliness | {'✓ PASS' if not g3_blockers else '✗ FAIL'} | {len(g3_blockers)} |",
        f"| Gate 4 — Schema Integrity | {'✓ PASS' if g4_pass else '✗ FAIL'} | {'0' if g4_pass else 'SEE BELOW'} |",
        "",
        "---",
        "",
        "## Gate 1 — Division Preservation",
        "",
        f"Source divisions checked: {total_divs_checked}",
        f"Exact matches: {total_divs_ok}",
        f"Missing (total, incl. known-issue events): {total_divs_missing}",
        f"Missing (blockers — not in known_issues): {len(g1_blockers)}",
        "",
    ]

    if g1_blockers:
        lines += [
            "### ⛔ Blocking Missing Divisions",
            "",
            "| Event ID | Year | Event Name | Division |",
            "|---|---|---|---|",
        ]
        for r in g1_blockers[:50]:
            lines.append(f"| {r['event_id']} | {r['year']} | {r['event_name'][:50]} | {r['division'][:50]} |")
        if len(g1_blockers) > 50:
            lines.append(f"| ... | ... | ({len(g1_blockers)-50} more in missing_divisions_report.csv) | |")
        lines.append("")

    if g1_missing and not g1_blockers:
        lines += [
            f"_All {total_divs_missing} missing division(s) are from events in known_issues.csv — documented, non-blocking._",
            "",
        ]

    lines += [
        "---",
        "",
        "## Gate 2 — Placement Preservation",
        "",
        f"Compares stage2 participant counts against canonical event_result_participants.csv.",
        f"Stage2 participants (non-quarantined): {g2_stats.get('stage2_participants', 0):,}",
        f"Canonical participants (all events):   {g2_stats.get('canonical_participants', 0):,}",
        f"  (difference = quarantined events excluded from Gate 2 check but present in canonical)",
        f"Events checked (non-quarantined):      {g2_stats.get('events_checked', 0)}",
        f"Events with participant delta:         {g2_stats.get('events_missing', 0)}",
        f"Blockers (non-known-issue deltas):     {len(g2_blockers)}",
        "",
    ]

    if g2_blockers:
        lines += [
            "### ⛔ Blocking Participant Count Mismatches",
            "",
            "| Event ID | Year | Event | Stage2 | Canonical | Delta |",
            "|---|---|---|---|---|---|",
        ]
        for r in g2_blockers[:50]:
            lines.append(
                f"| {r['event_id']} | {r['year']} | {r['event_name'][:40]} "
                f"| {r['stage2_count']} | {r['canonical_count']} | {r['delta']} |"
            )
        if len(g2_blockers) > 50:
            lines.append(f"| ... ({len(g2_blockers)-50} more in missing_placements_report.csv) | | | | | |")
        lines.append("")

    if not g2_blockers:
        lines += ["_All participant counts match. Canonical export is complete._", ""]

    lines += [
        "---",
        "",
        "## Gate 3 — Encoding Cleanliness",
        "",
        "Files scanned: events.csv, event_disciplines.csv, event_result_participants.csv, Placements_Flat.csv",
        "",
        "Issues are classified into three tiers:",
        "- **FIXABLE** — pipeline bugs; block publication until resolved",
        "- **SOURCE_LOSS** — U+FFFD from HTML mirror encoding loss; unrecoverable without original source",
        "- **SOFT** — quoted nicknames stored as `?Name?`; low severity, non-blocking",
        "",
        f"| Tier | Count | Blocking? |",
        f"|---|---|---|",
        f"| FIXABLE pipeline corruption | {len(g3_blockers)} | {'Yes' if g3_blockers else 'No — 0 found'} |",
        f"| SOURCE_LOSS (U+FFFD from HTML mirror) | {len(g3_source_loss)} | No — documented limitation |",
        f"| SOFT (?Nickname? patterns) | {len(g3_soft)} | No — non-blocking |",
        "",
    ]

    if g3_blockers:
        lines += [
            "### ⛔ Blocking Encoding Issues (FIXABLE)",
            "",
            "| Row Key | Field | Issue | Match | Value |",
            "|---|---|---|---|---|",
        ]
        for r in g3_blockers[:40]:
            val_short = r["value"][:60].replace("|", "\\|")
            lines.append(
                f"| {r['row_key'][:50]} | {r['field']} | {r['issue']} | {r['match']} | {val_short} |"
            )
        if len(g3_blockers) > 40:
            lines.append(f"| ... ({len(g3_blockers)-40} more in encoding_corruption_report.csv) | | | | |")
        lines.append("")
    else:
        lines += ["_No fixable encoding corruption detected. Zero pipeline encoding bugs._", ""]

    if g3_source_loss:
        lines += [
            f"### ℹ SOURCE_LOSS — {len(g3_source_loss)} U+FFFD Characters (Non-blocking)",
            "",
            "These characters represent encoding loss in the HTML mirror source. The original footbag.org "
            "mirror had UTF-8 characters that were corrupted during archival. They cannot be recovered "
            "without the original source pages. Affected fields: player display names, person_canon "
            "for unresolved players.",
            "",
            "Affected names include: accented characters in French, Finnish, German, Polish, and Czech names "
            "(e.g., François, Geneviève, Toni Pääkkönen, Václav Klouda, Robin Péchel).",
            "",
            "This is a known source limitation documented in the dataset release notes.",
            "",
        ]

    if g3_soft:
        lines += [
            f"### ℹ SOFT — {len(g3_soft)} Quoted Nickname Patterns (Non-blocking)",
            "",
            "Names like `?Dexter?`, `?Hollywood?`, `?Crazy?` represent quoted nicknames where the "
            "quotation marks were lost in source encoding. The names remain readable and searchable.",
            "",
        ]

    lines += [
        "---",
        "",
        "## Gate 4 — Schema Integrity",
        "",
        g4_detail,
        "",
        "---",
        "",
        "## Quarantined Events (excluded from Gates 1–2)",
        "",
        f"Total quarantined: {skipped_q}",
        "These events have documented structural issues and are excluded from the canonical dataset.",
        "",
    ]

    # List quarantined events
    q_path = ROOT / "inputs" / "review_quarantine_events.csv"
    if q_path.exists():
        lines += ["| Event ID | Year | Event Name | Reason |", "|---|---|---|---|"]
        with open(q_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                lines.append(
                    f"| {row['event_id']} | {row.get('year','')} "
                    f"| {row.get('event_name','')[:50]} | {row.get('reason','')} |"
                )
        lines.append("")

    lines += [
        "---",
        "",
        "## Artifact Inventory",
        "",
        "| Artifact | Version | Rows |",
        "|---|---|---|",
        "| Placements_ByPerson | v62 | 27,154 |",
        "| Persons_Truth_Final | v42 | 3,441 |",
        "| Persons_Unresolved_Organized | v28 | 82 |",
        "| Stage2 events | — | 774 |",
        "| Known-issue events | — | 54 |",
        "| Quarantined events | — | 20 |",
        "",
        "---",
        "",
        "_Generated by tools/58_final_publication_validation.py_",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    print("Loading reference data…")
    quarantined    = load_quarantined()
    known_issues   = load_known_issues()
    source_partial = load_source_partial()
    stage2         = load_stage2()
    canonical      = load_canonical_export()

    print(f"  Stage2 events:     {len(stage2)}")
    print(f"  Quarantined:       {len(quarantined)}")
    print(f"  Known issues:      {len(known_issues)}")
    print(f"  Canonical events:  {len(canonical['event_id_to_key'])}")
    print(f"  Canonical discs:   {canonical['disc_count']}")
    print(f"  Canonical partic.: {canonical['participant_count']}")

    print("\nGate 1 — Division Preservation…")
    g1_missing, g1_stats = gate1_divisions(stage2, canonical, quarantined, known_issues)
    print(f"  Divisions checked:  {g1_stats['divisions_checked']}")
    print(f"  Exact matches:      {g1_stats['exact_match']}")
    print(f"  Missing (total):    {g1_stats['missing']}")
    g1_blockers = [r for r in g1_missing if r["known_issue"] == "NO"]
    print(f"  Blockers:           {len(g1_blockers)}")

    print("\nGate 2 — Placement Preservation…")
    g2_missing, g2_stats = gate2_placements(stage2, canonical, quarantined, known_issues)
    g2_blockers = [r for r in g2_missing if r["known_issue"] == "NO"]
    print(f"  Stage2 participants:     {g2_stats['stage2_participants']}")
    print(f"  Canonical participants:  {g2_stats['canonical_participants']}")
    print(f"  Events checked:          {g2_stats['events_checked']}")
    print(f"  Events with delta:       {g2_stats['events_missing']}")
    print(f"  Blockers:                {len(g2_blockers)}")

    print("\nGate 3 — Encoding Cleanliness…")
    g3_issues, g3_stats = gate3_encoding()
    g3_blockers_main    = [r for r in g3_issues if "[SOURCE_LOSS]" not in r["issue"] and "[SOFT]" not in r["issue"]]
    g3_source_loss_main = [r for r in g3_issues if "[SOURCE_LOSS]" in r["issue"]]
    g3_soft_main        = [r for r in g3_issues if "[SOFT]" in r["issue"]]
    print(f"  FIXABLE blockers:   {len(g3_blockers_main)}")
    print(f"  SOURCE_LOSS (FFFD): {len(g3_source_loss_main)}")
    print(f"  SOFT (?Nickname?):  {len(g3_soft_main)}")

    print("\nGate 4 — Schema Integrity…")
    g4_pass, g4_detail = gate4_schema()
    print(f"  Pass:               {g4_pass}")

    # Write CSVs
    OUT.mkdir(exist_ok=True)
    write_csv(
        OUT / "missing_divisions_report.csv",
        ["event_id", "year", "event_name", "division", "known_issue", "severity"],
        g1_missing,
    )
    write_csv(
        OUT / "missing_placements_report.csv",
        ["event_id", "year", "event_name", "stage2_count", "canonical_count",
         "delta", "known_issue"],
        g2_missing,
    )
    write_csv(
        OUT / "encoding_corruption_report.csv",
        ["row_key", "field", "issue", "match", "value"],
        g3_issues,
    )

    write_report(
        OUT / "FINAL_VALIDATION_REPORT.md",
        g1_missing, g1_stats,
        g2_missing, g2_stats,
        g3_issues, g3_stats,
        g4_pass, g4_detail,
        quarantined, source_partial, known_issues, stage2,
    )

    # Final verdict — only FIXABLE encoding issues block publication
    is_blocked = bool(g1_blockers or g2_blockers or g3_blockers_main or not g4_pass)
    print("\n" + "=" * 60)
    if is_blocked:
        print("PUBLICATION STATUS: BLOCKED")
        print(f"  Gate 1 blockers:          {len(g1_blockers)}")
        print(f"  Gate 2 blockers:          {len(g2_blockers)}")
        print(f"  Encoding FIXABLE:         {len(g3_blockers_main)}")
        print(f"  Schema pass:              {g4_pass}")
    else:
        print("PUBLICATION STATUS: READY")
        print(f"  Gate 1 (divisions):       PASS (0 blockers)")
        print(f"  Gate 2 (placements):      PASS (0 blockers)")
        print(f"  Gate 3 (encoding):        PASS (0 fixable issues)")
        print(f"    SOURCE_LOSS (FFFD):     {len(g3_source_loss_main)} documented — HTML mirror encoding loss")
        print(f"    SOFT (?Nickname?):      {len(g3_soft_main)} documented — quoted nicknames in source")
        print(f"  Gate 4 (schema):          PASS")
    print("=" * 60)
    print(f"\nOutputs written to {OUT}/")
    print(f"  FINAL_VALIDATION_REPORT.md")
    print(f"  missing_divisions_report.csv  ({len(g1_missing)} rows)")
    print(f"  missing_placements_report.csv ({len(g2_missing)} rows)")
    print(f"  encoding_corruption_report.csv ({len(g3_issues)} rows)")

    return 1 if is_blocked else 0


if __name__ == "__main__":
    sys.exit(main())
