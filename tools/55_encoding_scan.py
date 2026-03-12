#!/usr/bin/env python3
"""
55_encoding_scan.py

Scans all display-facing fields across pipeline files for encoding corruption:
  - U+FFFD replacement character
  - ISO-8859-2 bytes misread as Latin-1 (¹→š, è→č, ¦→Ś, ¸→ž, ¿→ż, etc.)
  - CP1252 mojibake quote pattern: Ï...Ó around nicknames
  - ?S apostrophe corruption (Women?S, Master?S)
  - C1 control characters (U+0080–U+009F)

Outputs:
  out/final_validation/encoding_corruption_report.csv
  out/final_validation/encoding_fix_summary.md
"""

import csv
import re
import sys
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO = Path(__file__).resolve().parent.parent
OUT  = REPO / "out"
VAL  = OUT / "final_validation"
VAL.mkdir(parents=True, exist_ok=True)

REPORT_CSV = VAL / "encoding_corruption_report.csv"
SUMMARY_MD = VAL / "encoding_fix_summary.md"

# ── Corruption detectors ──────────────────────────────────────────────────────

RE_FFFD        = re.compile(r"\ufffd")
RE_QS          = re.compile(r"\b\w+\?[Ss]\b")          # Women?S, Master?S
RE_MOJI_QUOTE  = re.compile(r"Ï(.+?)Ó")               # ÏGatorÓ pattern
RE_CTRL        = re.compile(r"[\x80-\x9f]")             # C1 control chars
# ISO-8859-2 bytes as Latin-1: ¹(B9→š), è(E8→č), ¦(A6→Ś), ¸(B8→ž), ¿(BF→ż), ¼(BC→ź)
RE_ISO88592    = re.compile(r"[¹\xb9](?=[a-zA-Z])|[¦\xa6](?=[a-zA-Z])|¸(?=[a-zA-Z])|¿(?=[a-zA-Z])|¼(?=[a-zA-Z])")
# Superscript digits used as letters (¹² etc. inside names)
RE_SUPER_DIGIT = re.compile(r"[¹²³](?=[a-zA-Z])")

# Known correctable patterns
_MOJI_QUOTE_FIX   = lambda m: f'"{m.group(1)}"'
_QS_FIX           = lambda m: m.group(0).replace("?S", "'s").replace("?s", "'s")

# ISO-8859-2 → proper Unicode mapping (bytes misread as Latin-1)
_ISO2_MAP = {
    "\u00b9": "\u0161",   # ¹ → š
    "\u00b8": "\u017e",   # ¸ → ž   (note: Robin Püchel likely has different source byte)
    "\u00a6": "\u015a",   # ¦ → Ś
    "\u00bf": "\u017c",   # ¿ → ż
    "\u00bc": "\u017a",   # ¼ → ź
    "\u00e8": "\u010d",   # è → č  (ISO-8859-2 0xE8)
    "\u00f2": "\u0142",   # ò → ł  (ISO-8859-2 0xF2)
    "\u00b6": "\u015b",   # ¶ → ś  (ISO-8859-2 0xB6)
    "\u00ba": "\u015f",   # º → ş  (context-dependent)
}

def _suggested_fix(value: str, corruption_type: str) -> str:
    """Return best-effort corrected value for known patterns."""
    if corruption_type == "MOJI_QUOTE":
        return RE_MOJI_QUOTE.sub(_MOJI_QUOTE_FIX, value)
    if corruption_type == "QS_APOS":
        return RE_QS.sub(_QS_FIX, value)
    if corruption_type == "FFFD":
        # \ufffd + uppercase → lowercase, then strip remaining
        fixed = re.sub(r"\ufffd([A-Z])", lambda m: m.group(1).lower(), value)
        return fixed.replace("\ufffd", "")
    if corruption_type == "ISO88592":
        result = value
        for bad, good in _ISO2_MAP.items():
            result = result.replace(bad, good)
        return result
    return value


def classify(value: str) -> list[tuple[str, str]]:
    """Return list of (corruption_type, description) for a string."""
    found = []
    if RE_FFFD.search(value):
        found.append(("FFFD", "U+FFFD replacement char (encoding fallback)"))
    if RE_QS.search(value):
        found.append(("QS_APOS", "?S apostrophe corruption (Women?S etc.)"))
    if RE_MOJI_QUOTE.search(value):
        found.append(("MOJI_QUOTE", "ÏxyzÓ mojibake around nickname"))
    if RE_CTRL.search(value):
        found.append(("CTRL_CHAR", "C1 control character U+0080-U+009F"))
    if RE_ISO88592.search(value):
        found.append(("ISO88592", "ISO-8859-2 byte misread as Latin-1"))
    return found


# ── File scan configuration ───────────────────────────────────────────────────

SCAN_TARGETS = [
    {
        "file": OUT / "Placements_Flat.csv",
        "columns": ["division_canon", "team_display_name"],
        "event_id_col": "event_id",
        "label": "Placements_Flat",
        "fix_layer": "04B presentation",
    },
    {
        "file": OUT / "Placements_ByPerson.csv",
        "columns": ["person_canon", "division_canon"],
        "event_id_col": "event_id",
        "label": "Placements_ByPerson",
        "fix_layer": "04B presentation",
    },
    {
        "file": OUT / "Persons_Truth.csv",
        "columns": ["person_canon"],
        "event_id_col": None,
        "label": "Persons_Truth",
        "fix_layer": "04B presentation (display only — do not alter identity key)",
    },
    {
        "file": OUT / "stage2_canonical_events.csv",
        "columns": ["event_name", "location", "host_club"],
        "event_id_col": "event_id",
        "label": "stage2_canonical_events",
        "fix_layer": "04B presentation (event name display)",
    },
    {
        "file": OUT / "canonical" / "events.csv",
        "columns": ["event_name", "location", "host_club"],
        "event_id_col": "event_id",
        "label": "canonical/events",
        "fix_layer": "stage 05 re-run after pipeline fix",
    },
    {
        "file": OUT / "canonical" / "event_results.csv",
        "columns": ["division_canon", "team_display_name"],
        "event_id_col": "event_id",
        "label": "canonical/event_results",
        "fix_layer": "stage 05 re-run after pipeline fix",
    },
    {
        "file": OUT / "canonical" / "persons.csv",
        "columns": ["person_canon"],
        "event_id_col": None,
        "label": "canonical/persons",
        "fix_layer": "stage 05 re-run after pipeline fix",
    },
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    report_rows = []
    type_counts: dict[str, int] = {}
    file_counts: dict[str, int] = {}

    for target in SCAN_TARGETS:
        path = target["file"]
        if not path.exists():
            print(f"  SKIP (not found): {path.name}")
            continue

        target_issues = 0
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                eid = row.get(target["event_id_col"] or "", "") if target["event_id_col"] else ""
                for col in target["columns"]:
                    val = (row.get(col) or "").strip()
                    if not val:
                        continue
                    issues = classify(val)
                    for ctype, desc in issues:
                        suggested = _suggested_fix(val, ctype)
                        is_blocker = "YES" if ctype in ("MOJI_QUOTE", "QS_APOS", "FFFD", "ISO88592") else "NO"
                        report_rows.append({
                            "event_id":        eid,
                            "source_file":     target["label"],
                            "field_name":      col,
                            "current_value":   val,
                            "corrected_value": suggested,
                            "corruption_type": ctype,
                            "description":     desc,
                            "blocker":         is_blocker,
                            "fix_applied_where": target["fix_layer"],
                        })
                        type_counts[ctype] = type_counts.get(ctype, 0) + 1
                        target_issues += 1

        file_counts[target["label"]] = target_issues
        print(f"  {target['label']}: {target_issues} issues")

    # Deduplicate: same (source_file, field_name, current_value) → keep first
    seen: set = set()
    deduped = []
    for r in report_rows:
        key = (r["source_file"], r["field_name"], r["current_value"], r["corruption_type"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    # Write CSV
    fields = ["event_id","source_file","field_name","current_value","corrected_value",
              "corruption_type","description","blocker","fix_applied_where"]
    with open(REPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(deduped)

    # Write summary
    total_raw     = len(report_rows)
    total_deduped = len(deduped)
    blockers      = [r for r in deduped if r["blocker"] == "YES"]

    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write("# Encoding Corruption Fix Summary\n\n")
        f.write(f"Total raw findings: {total_raw}  \n")
        f.write(f"Unique (file, field, value, type) entries: {total_deduped}  \n")
        f.write(f"Publication blockers (visible corruption): {len(blockers)}\n\n")

        f.write("## By file\n\n")
        f.write("| Source file | Raw issues |\n")
        f.write("|---|---|\n")
        for lbl, cnt in file_counts.items():
            f.write(f"| {lbl} | {cnt} |\n")
        f.write("\n")

        f.write("## By corruption type\n\n")
        f.write("| Type | Count | Description |\n")
        f.write("|---|---|---|\n")
        type_desc = {
            "FFFD":       "U+FFFD replacement char — encoding fallback in source HTML",
            "QS_APOS":    "?S apostrophe — Women?S, Master?S (encoding artifact)",
            "MOJI_QUOTE": "ÏxyzÓ mojibake — corrupted smart quotes around nickname",
            "ISO88592":   "ISO-8859-2 byte misread as Latin-1 (¹→š, ¿→ż, etc.)",
            "CTRL_CHAR":  "C1 control character in string value",
        }
        for ctype, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
            f.write(f"| {ctype} | {cnt} | {type_desc.get(ctype, '')} |\n")
        f.write("\n")

        f.write("## Where fixes are applied\n\n")
        f.write("All corruption listed here originates in the HTML mirror or legacy data files,\n")
        f.write("was propagated through stages 1-2 into the identity lock artifacts, and appears\n")
        f.write("in canonical outputs. Canonical CSVs are **not altered** — fixes are applied\n")
        f.write("in the presentation layer (`pipeline/04B_create_community_excel.py`) only.\n\n")
        f.write("The functions `_clean_div()`, `_fix_name_encoding()`, and `_fix_display_str()`\n")
        f.write("in 04B handle the presentation-layer corrections.\n\n")

        f.write("## Publication rule\n\n")
        f.write("Any visible encoding corruption in public-facing workbook cells is a **BLOCKER**.\n\n")
        f.write("After applying presentation-layer fixes, no visible corruption should remain.\n\n")

        f.write("---\n\n")
        if len(blockers) == 0:
            f.write("**ENCODING_PASS** — no visible corruption detected in scan targets.\n")
        else:
            f.write(f"**ENCODING_FAIL** — {len(blockers)} unique corrupted values require fixes.\n")
            f.write("See `encoding_corruption_report.csv` for full details.\n")

    print()
    print(f"  Report: {REPORT_CSV}")
    print(f"  Summary: {SUMMARY_MD}")
    print()
    print(f"Unique corrupted values: {total_deduped}")
    print("By type:")
    for ctype, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {ctype}: {cnt}")
    print()
    if blockers:
        print(f"ENCODING_FAIL — {len(blockers)} blockers")
        sys.exit(1)
    else:
        print("ENCODING_PASS")


if __name__ == "__main__":
    main()
