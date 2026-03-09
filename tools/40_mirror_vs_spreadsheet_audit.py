#!/usr/bin/env python3
"""
tools/40_mirror_vs_spreadsheet_audit.py

Systematic audit: mirror HTML ground truth vs pipeline outputs.

Outputs (all written to out/audit/):
  mirror_results.csv          — structured ground truth extracted from mirror HTML
  sheet_results.csv           — normalized pipeline output (stage2 placements_json)
  event_coverage_audit.csv    — event-level comparison
  division_coverage_audit.csv — division-level comparison
  placement_diff_audit.csv    — events/divisions with placement count discrepancies
  name_quality_audit.csv      — name quality issues in pipeline output

Usage:
  python tools/40_mirror_vs_spreadsheet_audit.py [--worlds-only] [--event-id ID]

Flags:
  --worlds-only    Restrict to Worlds events only (fastest for initial review)
  --event-id ID    Restrict to a single event_id
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent.parent
MIRROR_DIR    = ROOT / "mirror"
STAGE2_CSV    = ROOT / "out" / "stage2_canonical_events.csv"
PF_CSV        = ROOT / "out" / "Placements_Flat.csv"
OUT_DIR       = ROOT / "out" / "audit"

csv.field_size_limit(10 ** 7)

# ── Mirror HTML helpers (mirrors logic from 01_parse_mirror.py) ────────────────

_ILLEGAL_CSV_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

_PLACEMENT_LINE_RE = re.compile(
    r"^\s*[1-9]\d{0,2}\s*"
    r"(?:[.)\-:]\s*\S|(?:st|nd|rd|th)\b|[°º])",
    re.MULTILINE,
)

_PLACE_NUM_RE = re.compile(r"^\s*([1-9]\d{0,2})")

# Division header lines: heuristic detection in pre-block text
_DIV_HEADER_RE = re.compile(
    r"^(?:[A-Z][A-Za-z\s&'\-/,()]{3,60}):\s*$", re.MULTILINE
)


def _read_html(p: Path) -> str:
    for enc in ("utf-8", "latin-1"):
        try:
            return p.read_text(encoding=enc)
        except UnicodeDecodeError:
            pass
    return p.read_text(encoding="utf-8", errors="replace")


def _fix_encoding(s: str) -> str:
    fixes = {"©": "Š", "£": "Ł", "\x92": "'", "\x93": '"', "\x94": '"', "\x9a": "š"}
    for bad, good in fixes.items():
        s = s.replace(bad, good)
    s = re.sub(r"(\w)\ufffd" + r"s\b", r"\1's", s)
    return s


def _norm_div(s: str) -> str:
    """Normalize a division name for fuzzy matching."""
    s = s.lower().strip().rstrip(":")
    s = s.replace("\u00ad", "")   # strip soft hyphens (U+00AD) common in 2012 Worlds
    s = s.replace("\u2011", "-")  # non-breaking hyphen → regular hyphen
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _has_question_mark_name(s: str) -> bool:
    """Return True if string has embedded '?' that looks like encoding loss."""
    # Allow '?' at end of team names like 'Smith / ?', but flag mid-word '?'
    return bool(re.search(r"\w\?\w", s))


def _unicode_damage_score(s: str) -> int:
    """Count characters that look like encoding damage."""
    count = 0
    for ch in s:
        if ch == "\ufffd":
            count += 1
        cat = unicodedata.category(ch)
        if cat == "Cc" and ch not in ("\n", "\r", "\t"):  # control chars
            count += 1
    return count


# ── Mirror extraction ──────────────────────────────────────────────────────────

def extract_mirror_ground_truth(event_id: str) -> dict | None:
    """
    Parse mirror HTML for one event.  Returns dict with:
      event_id, event_name, year,
      divisions: list of {div_raw, placement_lines: [{num, text}]}
      raw_results_text: str
      has_results: bool
      h2_div_names: list of str (from <h2> tags)
      total_placement_lines: int
    """
    event_dir = MIRROR_DIR / "www.footbag.org" / "events" / "show" / event_id
    html_path = event_dir / "index.html"
    if not html_path.exists():
        alt = event_dir / f"{event_id}.html"
        if alt.exists():
            html_path = alt
        else:
            return None

    html = _fix_encoding(_read_html(html_path))
    soup = BeautifulSoup(html, "html.parser")

    # Event name from title
    event_name = ""
    if soup.title and soup.title.string:
        event_name = soup.title.string.strip()

    # Year
    year = None
    for src in (event_name, html[:2000]):
        m = re.search(r"\b(19\d{2}|20\d{2})\b", src)
        if m:
            y = int(m.group(1))
            if 1970 <= y <= 2030:
                year = y
                break

    # Extract results div text
    results_div = soup.select_one("div.eventsResults")
    raw_results_text = ""
    h2_div_names = []

    if results_div:
        # Collect h2 division headers (excluding "Manually Entered Results" sentinel)
        for h2 in results_div.find_all("h2"):
            txt = h2.get_text(strip=True).replace("\u00a0", " ").strip()
            if txt and "manually" not in txt.lower():
                h2_div_names.append(txt)

        # Find best pre tag with numbered placement lines (any pre, not just .eventsPre)
        best_pre = None
        for pre in results_div.find_all("pre"):
            pre_text = pre.get_text("\n", strip=False).replace("\u00a0", " ")
            if _PLACEMENT_LINE_RE.search(pre_text):
                if best_pre is None or len(pre_text) > len(best_pre.get_text()):
                    best_pre = pre

        h2_text = results_div.get_text("\n", strip=False).replace("\u00a0", " ")

        if h2_div_names:
            # Use full div text (captures h2-structured content)
            # But if pre is much larger and has freestyle content not in h2 text, use pre
            if best_pre:
                pre_text = best_pre.get_text("\n", strip=False).replace("\u00a0", " ")
                if (len(pre_text) > len(h2_text) * 1.5 or
                        (re.search(r"\bfreestyle\b", pre_text, re.I) and
                         not re.search(r"\bfreestyle\b", h2_text, re.I))):
                    raw_results_text = pre_text
                else:
                    raw_results_text = h2_text
            else:
                raw_results_text = h2_text
        elif best_pre:
            # No h2 structure — fall back to pre block (e.g. "Manually Entered Results")
            raw_results_text = best_pre.get_text("\n", strip=False).replace("\u00a0", " ")
        else:
            raw_results_text = h2_text
    else:
        # Fallback: any pre not inside eventsEvents
        for pre in soup.find_all("pre"):
            pre_text = pre.get_text("\n", strip=False).replace("\u00a0", " ")
            if not _PLACEMENT_LINE_RE.search(pre_text):
                continue
            ev_div = soup.select_one("div.eventsEvents")
            if ev_div and pre in ev_div.find_all("pre"):
                continue
            raw_results_text = pre_text
            break

    # Parse divisions and placement lines from raw text
    divisions = _parse_divisions_from_text(raw_results_text, h2_div_names)
    total_pl = sum(len(d["placement_lines"]) for d in divisions)

    return {
        "event_id":              event_id,
        "event_name":            event_name,
        "year":                  year,
        "h2_div_names":          h2_div_names,
        "divisions":             divisions,
        "raw_results_text":      raw_results_text,
        "has_results":           total_pl > 0 or bool(h2_div_names),
        "total_placement_lines": total_pl,
    }


def _parse_divisions_from_text(text: str, h2_names: list[str]) -> list[dict]:
    """
    Parse division → placement lines from the results text.
    Uses h2 names as anchors, falls back to heuristic div-header detection.
    Returns list of {div_raw, placement_lines: [{num, text}]}
    """
    lines = [ln.rstrip() for ln in text.splitlines()]

    # Build set of normalised h2 names for quick lookup
    h2_norm = {_norm_div(n) for n in h2_names}

    divisions = []
    current_div = "__PREAMBLE__"
    current_pls: list[dict] = []

    def flush():
        nonlocal current_div, current_pls
        if current_pls:
            divisions.append({"div_raw": current_div, "placement_lines": current_pls})
        current_pls = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Stop at "Manually Entered Results" — often duplicate of h2 section
        if re.search(r"manually entered results", stripped, re.I):
            break
        if "Related Photos" in stripped:
            break

        # Check if this line is an h2 division header
        is_h2_header = _norm_div(stripped) in h2_norm

        # Check if this looks like a standalone division header
        # (title-cased, ends with ':', no leading number)
        is_heuristic_header = (
            not is_h2_header
            and not _PLACEMENT_LINE_RE.match(line)
            and re.match(r"^[A-Z][A-Za-z0-9\s&'\-/,()]{3,70}:\s*$", stripped)
            and not re.search(r"\bvs\b|\bvs\.\b", stripped, re.I)
        )

        if is_h2_header or is_heuristic_header:
            flush()
            current_div = stripped.rstrip(":")
            continue

        # Placement line
        m = _PLACE_NUM_RE.match(stripped)
        if m:
            place_num = int(m.group(1))
            # Strip leading number+separator to get player text
            player_text = re.sub(
                r"^\s*\d+\s*[.)\-:]?\s*(?:st|nd|rd|th)?\s*", "", stripped
            ).strip()
            current_pls.append({"num": place_num, "text": player_text, "raw": stripped})

    flush()

    # If nothing was parsed (no numbered lines, no h2 headers), create one
    # synthetic division from h2 names so event shows up in event-level audit
    if not divisions and h2_names:
        for name in h2_names:
            divisions.append({"div_raw": name, "placement_lines": []})

    return divisions


# ── Stage-2 + Placements_Flat loading ─────────────────────────────────────────

def load_stage2() -> dict:
    """Returns dict event_id → {event_name, year, event_type, divisions: {div_raw→[placements]}}"""
    events = {}
    with open(STAGE2_CSV, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.DictReader(fh):
            eid = row["event_id"].strip()
            try:
                pj = json.loads(row.get("placements_json") or "[]")
            except (json.JSONDecodeError, TypeError):
                pj = []

            # Group placements by division_raw
            by_div: dict[str, list] = defaultdict(list)
            for p in pj:
                dr = (p.get("division_raw") or p.get("division_canon") or "").strip()
                by_div[dr].append(p)

            events[eid] = {
                "event_name":  (row.get("event_name") or "").strip(),
                "year":        int(row.get("year") or 0) or None,
                "event_type":  (row.get("event_type") or "").strip(),
                "results_raw": (row.get("results_raw") or "").strip(),
                "divisions":   dict(by_div),   # div_raw → [placement dicts]
                "total_placements": len(pj),
            }
    return events


def load_placements_flat() -> dict:
    """Returns dict event_id → list of placement rows."""
    pf: dict[str, list] = defaultdict(list)
    with open(PF_CSV, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            pf[row["event_id"].strip()].append(row)
    return dict(pf)


# ── Comparison helpers ─────────────────────────────────────────────────────────

def _match_divisions(mirror_divs: list[dict], stage2_divs: dict) -> list[dict]:
    """
    For each mirror division, find the best matching stage2 division.
    Returns list of match records.
    """
    s2_norm = {_norm_div(k): k for k in stage2_divs.keys()}
    results = []

    for md in mirror_divs:
        if md["div_raw"] == "__PREAMBLE__":
            continue
        mn = _norm_div(md["div_raw"])
        mirror_count = len(md["placement_lines"])

        # Exact normalised match
        if mn in s2_norm:
            s2_raw = s2_norm[mn]
            s2_count = len(stage2_divs[s2_raw])
            results.append({
                "div_mirror":   md["div_raw"],
                "div_stage2":   s2_raw,
                "match_type":   "exact",
                "mirror_count": mirror_count,
                "stage2_count": s2_count,
            })
            continue

        # Partial / substring match
        best_s2 = None
        best_score = 0
        for s2n, s2k in s2_norm.items():
            # Overlap: how many words from mirror div appear in s2 div
            mw = set(mn.split())
            s2w = set(s2n.split())
            overlap = len(mw & s2w) / max(len(mw | s2w), 1)
            if overlap > best_score:
                best_score = overlap
                best_s2 = s2k

        if best_score >= 0.5 and best_s2:
            s2_count = len(stage2_divs[best_s2])
            results.append({
                "div_mirror":   md["div_raw"],
                "div_stage2":   best_s2,
                "match_type":   f"partial({best_score:.2f})",
                "mirror_count": mirror_count,
                "stage2_count": s2_count,
            })
        else:
            results.append({
                "div_mirror":   md["div_raw"],
                "div_stage2":   "",
                "match_type":   "no_match",
                "mirror_count": mirror_count,
                "stage2_count": 0,
            })

    # Also find stage2 divisions with no mirror match
    matched_s2 = {r["div_stage2"] for r in results if r["div_stage2"]}
    for s2k, s2pls in stage2_divs.items():
        if s2k not in matched_s2:
            results.append({
                "div_mirror":   "",
                "div_stage2":   s2k,
                "match_type":   "stage2_only",
                "mirror_count": 0,
                "stage2_count": len(s2pls),
            })

    return results


# ── Name quality checks ────────────────────────────────────────────────────────

def check_name_quality(pf_rows: list[dict], event_name: str) -> list[dict]:
    issues = []
    for row in pf_rows:
        # Check person_canon and team_display_name
        for field in ("person_canon", "team_display_name"):
            val = row.get(field, "")
            if not val or val in ("__NON_PERSON__",):
                continue

            if _has_question_mark_name(val):
                issues.append({
                    "event_id":    row["event_id"],
                    "year":        row.get("year", ""),
                    "event_name":  event_name,
                    "division":    row.get("division_canon", ""),
                    "place":       row.get("place", ""),
                    "field":       field,
                    "issue_type":  "embedded_question_mark",
                    "value":       val,
                })

            dmg = _unicode_damage_score(val)
            if dmg > 0:
                issues.append({
                    "event_id":    row["event_id"],
                    "year":        row.get("year", ""),
                    "event_name":  event_name,
                    "division":    row.get("division_canon", ""),
                    "place":       row.get("place", ""),
                    "field":       field,
                    "issue_type":  "unicode_damage",
                    "value":       val,
                })

        # Flag '?' in team display (solo-in-doubles)
        tdname = row.get("team_display_name", "")
        if re.search(r"/ \?", tdname):
            issues.append({
                "event_id":    row["event_id"],
                "year":        row.get("year", ""),
                "event_name":  event_name,
                "division":    row.get("division_canon", ""),
                "place":       row.get("place", ""),
                "field":       "team_display_name",
                "issue_type":  "solo_in_doubles",
                "value":       tdname,
            })

    return issues


# ── Writers ────────────────────────────────────────────────────────────────────

def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {len(rows):,} rows → {path.relative_to(ROOT)}")


# ── Main audit ─────────────────────────────────────────────────────────────────

def run_audit(event_ids: list[str] | None, worlds_only: bool) -> None:
    print("Loading stage2…")
    stage2 = load_stage2()
    print(f"  {len(stage2):,} events in stage2")

    print("Loading Placements_Flat…")
    pf_by_event = load_placements_flat()
    print(f"  {sum(len(v) for v in pf_by_event.values()):,} placements in Placements_Flat")

    # Determine which events to audit
    if event_ids:
        audit_eids = event_ids
    elif worlds_only:
        audit_eids = [eid for eid, ev in stage2.items() if ev["event_type"] == "worlds"]
    else:
        audit_eids = sorted(stage2.keys())

    print(f"Auditing {len(audit_eids)} events…")

    mirror_rows: list[dict] = []
    sheet_rows:  list[dict] = []
    event_audit: list[dict] = []
    div_audit:   list[dict] = []
    name_audit:  list[dict] = []

    # Events in stage2 but not in mirror
    mirror_event_ids = set()
    events_show = MIRROR_DIR / "www.footbag.org" / "events" / "show"
    if events_show.exists():
        mirror_event_ids = {d.name for d in events_show.iterdir() if d.is_dir() and d.name.isdigit()}

    total = len(audit_eids)
    for i, eid in enumerate(sorted(audit_eids, key=lambda x: int(stage2.get(x, {}).get("year") or 0))):
        if i % 50 == 0:
            print(f"  [{i}/{total}] processing…")

        s2 = stage2.get(eid, {})
        event_name = s2.get("event_name", "")
        year       = s2.get("year") or 0
        event_type = s2.get("event_type", "")

        # Sheet side
        s2_total = s2.get("total_placements", 0)
        s2_divs  = s2.get("divisions", {})
        pf_rows  = pf_by_event.get(eid, [])

        # Build sheet_results rows
        for div_raw, pls in s2_divs.items():
            for p in pls:
                sheet_rows.append({
                    "event_id":       eid,
                    "year":           year,
                    "event_name":     event_name,
                    "event_type":     event_type,
                    "division_raw":   div_raw,
                    "division_canon": p.get("division_canon", ""),
                    "place":          p.get("place", ""),
                    "competitor_type": p.get("competitor_type", ""),
                    "player1_name":   p.get("player1_name", ""),
                    "player2_name":   p.get("player2_name", ""),
                })

        # Name quality from Placements_Flat
        name_audit.extend(check_name_quality(pf_rows, event_name))

        # Mirror side
        mirror = None
        if eid in mirror_event_ids:
            mirror = extract_mirror_ground_truth(eid)

        if mirror is None:
            # Event in stage2 but mirror HTML not found
            event_audit.append({
                "event_id":               eid,
                "year":                   year,
                "event_type":             event_type,
                "event_name":             event_name,
                "mirror_found":           False,
                "mirror_has_results":     False,
                "mirror_h2_div_count":    0,
                "mirror_parsed_div_count":0,
                "mirror_placement_lines": 0,
                "stage2_div_count":       len(s2_divs),
                "stage2_placement_count": s2_total,
                "pf_placement_count":     len(pf_rows),
                "status":                 "NO_MIRROR",
                "notes":                  "mirror HTML not found",
            })
            continue

        # Build mirror_results rows
        for div in mirror["divisions"]:
            if div["div_raw"] == "__PREAMBLE__":
                continue
            for pl in div["placement_lines"]:
                mirror_rows.append({
                    "event_id":      eid,
                    "year":          year,
                    "event_name":    event_name,
                    "event_type":    event_type,
                    "division_raw":  div["div_raw"],
                    "placement_num": pl["num"],
                    "player_text":   pl["text"],
                    "source_line":   pl["raw"],
                })

        # Division matching
        div_matches = _match_divisions(mirror["divisions"], s2_divs)

        for dm in div_matches:
            diff = dm["stage2_count"] - dm["mirror_count"]
            pct  = diff / dm["mirror_count"] if dm["mirror_count"] else None

            status = "OK"
            if dm["match_type"] == "no_match":
                status = "DIV_MISSING_IN_STAGE2"
            elif dm["match_type"] == "stage2_only":
                status = "DIV_ONLY_IN_STAGE2"
            elif dm["mirror_count"] > 0 and dm["stage2_count"] == 0:
                status = "PLACEMENTS_LOST"
            elif dm["mirror_count"] > 0 and pct is not None and pct < -0.3:
                status = "PLACEMENTS_REDUCED"

            div_audit.append({
                "event_id":       eid,
                "year":           year,
                "event_type":     event_type,
                "event_name":     event_name,
                "div_mirror":     dm["div_mirror"],
                "div_stage2":     dm["div_stage2"],
                "match_type":     dm["match_type"],
                "mirror_count":   dm["mirror_count"],
                "stage2_count":   dm["stage2_count"],
                "diff":           diff,
                "pct_change":     f"{pct:+.1%}" if pct is not None else "",
                "status":         status,
            })

        # Event-level summary
        mirror_div_count  = sum(1 for d in mirror["divisions"] if d["div_raw"] != "__PREAMBLE__")
        mirror_pl_lines   = mirror["total_placement_lines"]

        if not mirror["has_results"]:
            ev_status = "MIRROR_NO_RESULTS"
        elif s2_total == 0:
            ev_status = "STAGE2_EMPTY"
        elif mirror_pl_lines > 0 and s2_total < mirror_pl_lines * 0.5:
            ev_status = "LARGE_LOSS"
        elif mirror_pl_lines > 0 and s2_total < mirror_pl_lines * 0.8:
            ev_status = "MODERATE_LOSS"
        elif mirror_div_count > len(s2_divs) + 1:
            ev_status = "DIVS_MISSING"
        else:
            ev_status = "OK"

        notes_parts = []
        if mirror["h2_div_names"] and len(mirror["h2_div_names"]) != mirror_div_count:
            notes_parts.append(f"h2_headers={len(mirror['h2_div_names'])}")
        no_match_divs = [dm["div_mirror"] for dm in div_matches if dm["match_type"] == "no_match"]
        if no_match_divs:
            notes_parts.append(f"unmatched_mirror_divs=[{'; '.join(no_match_divs[:3])}]")

        event_audit.append({
            "event_id":                eid,
            "year":                    year,
            "event_type":              event_type,
            "event_name":              event_name,
            "mirror_found":            True,
            "mirror_has_results":      mirror["has_results"],
            "mirror_h2_div_count":     len(mirror["h2_div_names"]),
            "mirror_parsed_div_count": mirror_div_count,
            "mirror_placement_lines":  mirror_pl_lines,
            "stage2_div_count":        len(s2_divs),
            "stage2_placement_count":  s2_total,
            "pf_placement_count":      len(pf_rows),
            "status":                  ev_status,
            "notes":                   "; ".join(notes_parts),
        })

    # ── Write outputs ──────────────────────────────────────────────────────────
    print("\nWriting outputs…")

    write_csv(OUT_DIR / "mirror_results.csv", mirror_rows, [
        "event_id", "year", "event_name", "event_type",
        "division_raw", "placement_num", "player_text", "source_line",
    ])

    write_csv(OUT_DIR / "sheet_results.csv", sheet_rows, [
        "event_id", "year", "event_name", "event_type",
        "division_raw", "division_canon", "place",
        "competitor_type", "player1_name", "player2_name",
    ])

    write_csv(OUT_DIR / "event_coverage_audit.csv", event_audit, [
        "event_id", "year", "event_type", "event_name",
        "mirror_found", "mirror_has_results",
        "mirror_h2_div_count", "mirror_parsed_div_count", "mirror_placement_lines",
        "stage2_div_count", "stage2_placement_count", "pf_placement_count",
        "status", "notes",
    ])

    write_csv(OUT_DIR / "division_coverage_audit.csv", div_audit, [
        "event_id", "year", "event_type", "event_name",
        "div_mirror", "div_stage2", "match_type",
        "mirror_count", "stage2_count", "diff", "pct_change", "status",
    ])

    write_csv(OUT_DIR / "name_quality_audit.csv", name_audit, [
        "event_id", "year", "event_name", "division", "place",
        "field", "issue_type", "value",
    ])

    # ── Summary ────────────────────────────────────────────────────────────────
    print_summary(event_audit, div_audit, name_audit, worlds_only)


def print_summary(event_audit, div_audit, name_audit, worlds_only):
    scope = "Worlds events" if worlds_only else "all events"
    print(f"\n{'='*70}")
    print(f"MIRROR vs SPREADSHEET AUDIT SUMMARY  ({scope})")
    print(f"{'='*70}")

    # Event-level breakdown
    status_counts: dict[str, int] = defaultdict(int)
    for row in event_audit:
        status_counts[row["status"]] += 1

    print("\n── Event-level status ──")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {status:<30} {count:>4}")

    # Largest losses
    large_loss = [
        r for r in event_audit
        if r["status"] in ("LARGE_LOSS", "MODERATE_LOSS", "STAGE2_EMPTY", "DIVS_MISSING")
        and r["mirror_has_results"]
    ]
    large_loss.sort(key=lambda r: (
        -(r["mirror_placement_lines"] - r["stage2_placement_count"])
    ))

    if large_loss:
        print("\n── Events with largest placement loss (mirror_lines → stage2) ──")
        for r in large_loss[:20]:
            ml  = r["mirror_placement_lines"]
            s2  = r["stage2_placement_count"]
            pct = f"{(s2-ml)/ml:+.0%}" if ml else "n/a"
            print(f"  {r['year']}  {r['event_id']}  [{r['status']:<18}]  "
                  f"mirror={ml:4d}  stage2={s2:4d} ({pct})  {r['event_name'][:50]}")

    # Division gaps
    missing_divs = [r for r in div_audit if r["status"] in ("DIV_MISSING_IN_STAGE2", "PLACEMENTS_LOST")]
    print(f"\n── Divisions missing/lost in stage2 ({len(missing_divs)} total) ──")
    # Top repeated missing division patterns
    div_name_freq: dict[str, int] = defaultdict(int)
    for r in missing_divs:
        key = _norm_div(r["div_mirror"])
        div_name_freq[key] += 1
    for name, cnt in sorted(div_name_freq.items(), key=lambda x: -x[1])[:15]:
        print(f"  {cnt:>3}×  {name}")

    # Name quality
    iq_types: dict[str, int] = defaultdict(int)
    for r in name_audit:
        iq_types[r["issue_type"]] += 1

    print("\n── Name quality issues ──")
    if iq_types:
        for issue, cnt in sorted(iq_types.items(), key=lambda x: -x[1]):
            print(f"  {issue:<35} {cnt:>4}")
    else:
        print("  (none found)")

    print(f"\nOutputs written to out/audit/")
    print("="*70)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--worlds-only", action="store_true",
                   help="Restrict to World Footbag Championship events")
    p.add_argument("--event-id", metavar="ID",
                   help="Restrict to a single event_id")
    args = p.parse_args()

    if not MIRROR_DIR.exists():
        print(f"ERROR: mirror/ not found at {MIRROR_DIR}", file=sys.stderr)
        print("       Extract mirror.tar.gz or: ln -s mirror_full mirror", file=sys.stderr)
        sys.exit(1)
    if not STAGE2_CSV.exists():
        print(f"ERROR: {STAGE2_CSV} not found — run rebuild first", file=sys.stderr)
        sys.exit(1)
    if not PF_CSV.exists():
        print(f"ERROR: {PF_CSV} not found — run release first", file=sys.stderr)
        sys.exit(1)

    event_ids = [args.event_id.strip()] if args.event_id else None
    run_audit(event_ids, args.worlds_only)


if __name__ == "__main__":
    main()
