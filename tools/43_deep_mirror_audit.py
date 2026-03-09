#!/usr/bin/env python3
"""
tools/43_deep_mirror_audit.py

Deep mirror audit — extends tool 40 with sport-hierarchy tracking,
completeness scoring, and failure-mode classification.

Outputs (written to out/audit/):
  mirror_results_v2.csv         — mirror ground truth with sport column
  division_completeness_audit.csv — per (event, sport, division) completeness
  events_with_major_loss.csv    — events with completeness_ratio < 0.9
  placement_diff_audit.csv      — place-range gaps per division
  summary_report.md             — narrative summary

Does NOT modify stage2, identity lock, or any canonical artifacts.

Usage:
  python tools/43_deep_mirror_audit.py [--worlds-only] [--event-id ID] [--threshold FLOAT]

Flags:
  --worlds-only       Restrict to Worlds events only
  --event-id ID       Restrict to a single event_id
  --threshold FLOAT   Completeness ratio below which an event is "major loss" (default 0.9)
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT       = Path(__file__).parent.parent
MIRROR_DIR = ROOT / "mirror"
STAGE2_CSV = ROOT / "out" / "stage2_canonical_events.csv"
OUT_DIR    = ROOT / "out" / "audit"

csv.field_size_limit(10 ** 7)

# ── Known false-positive / explained event sets ────────────────────────────────
#
# Events in these sets are excluded from events_with_major_loss.csv and labelled
# with their known cause in division_completeness_audit.csv.  The raw mirror data
# is still written to mirror_results_v2.csv unchanged.
#
# KNOWN_SEEDING_DOUBLE_COUNT
#   The audit tool's simple line-by-line parser counts BOTH the "Initial Seeding"
#   section AND the "Final Results" section, giving ~2× the real placement count.
#   Stage2 correctly captures only the final standings.
KNOWN_SEEDING_DOUBLE_COUNT: dict[str, str] = {
    "1320232300": "RNH 2011 — Initial Seeding + Finals double-counted by audit tool",
    "1301675662": "Copa Venezuela 2011 — INITIAL SEEDING + FINAL RESULTS for 5 divs",
    "1297909685": "Copa Ciencias 2011 — same INITIAL SEEDING + FINAL RESULTS pattern",
    "1311699287": "Copa X-PRO 3ra 2011 — same pattern",
    "1516978874": "RNH 2018 — seeding + finals double-count",
    "1270631935": "RNH 2010 — Initial seeding + Results + Pool sub-sections",
    "1568289502": "Bulgarian Open Vol.2 2019 — seeding + final pattern",
    "1557144269": "Bembel Cup 2019 — seeding + final pattern",
    "1181021804": "ShrEdmonton 2007 — seeding + final double-count",
    "1389730147": "Todexon 15 2014 — seeding + final (audit flagged seeding_plus_final)",
    "1378666423": "Danish Footbag Open 2013 — Pool A/B/C/D + Finals all counted",
    "1487797845": "19th IFPA Euro Champs 2017 — Initial Seeding + Semifinal + Finals",
    "959353403":  "SE Idaho 2000 — seeding + final (audit flagged seeding_plus_final)",
    "1200325415": "Rochester Shred Symposium 2008 — HOTEL/DATES noise + Sick 3 seeding",
    "1235149093": "RNH Contest 2009 — finals (43) at top of pre block, then Initial Seeding + pool brackets; audit counts all 135 numbered lines, stage2 gets 97; correct count is 43 finals only",
    "1131659634": "Green Cup 2006 — Round Robin + Finals double-count",
    "1203604560": "CommALaMaison 2008 — seeding sub-sections",
    "1148241486": "Steel City Shred Off 2006 — seeding + partial finals",
    "1172931308": "Montreal Spring Jam 2 2007 — 'Best Trick' and 'Best Link' are sub-categories "
                  "of 'Overall Best Tricks' ranking; audit counts both as separate groups (6 extra "
                  "rows). Stage2 correctly captures 16 non-redundant placements.",
    "1092073845": "Czech Footbag Championships 2004 — 'Open Freestyle' listed twice in mirror "
                  "(abbreviated qualifying round + full finals listing); audit counts 8 duplicate "
                  "rows from the qualifying round. Legacy file uses finals-only (44 placements). "
                  "Big 1 (15 entries) recovered as 'Open Sick Trick'.",
    "1369141018": "Burgas Summer Footbag Jam V 2013 — 'open singles initial seeding' + "
                  "'open singles results' both parsed; stage2 deduplicates, audit double-counts; "
                  "stage2's 11 correct finals placements are accurate",
    "1466942562": "18th German Open 2016 — 'Seeding' section (46 rows) inflates audit count; "
                  "stage2 correctly captures 48 finals placements from structured HTML h2 headers; "
                  "Open Singles Net 22-vs-11 gap is seeding vs finals bracket",
    "1036298726": "Colorado Shred Symposium 4 2003 — mirror has qualifying-round data mixed with "
                  "finals; audit counts 55 numbered lines but legacy file correctly captures "
                  "41 finals-only placements via RESULTS_FILE_OVERRIDES",
}

# KNOWN_NO_RESULTS
#   Mirror HTML exists but contains only event announcements, driving directions,
#   or jam descriptions — no competitive standings were posted.  The audit tool's
#   simple numeric regex matched incidental numbers (times, counts) as placements.
KNOWN_NO_RESULTS: dict[str, str] = {
    "1063109533": "2003 Philly Oktoberfest Freestyle Jam — social jam, no standings posted",
    "1070400528": "West Coast Xmas Shred 2003 — only driving directions in <pre> block",
    "1268338685": "II Spanish OPEN 2010 — announcement only, no competitive results",
    "1163624128": "Moonin' Beaver Open Post Turkey Day 2006 — verify: likely announcement only",
    "1737312020": "Canadian Closed 2025 — verify: likely announcement only",
    "1057773472": "23rd Annual Moonin' & Noonin' Beaver Open 2003 — audit detected narrative "
                  "text ('Also, much honor to the 2003 Beaver Hall of Famer', 'I believe the "
                  "only undefeated team was') as pseudo-division headers with numbered lines; "
                  "stage2 correctly captures 2 real placements",
    "1032472601": "Chilly Philly Jam 2003 — audit counts noise entries at place 6 "
                  "('the rest of the pack'); stage2 correctly captures 13 real placements",
    "1081766954": "Russian Open Stage 2 2004 — group-range entries ('5-8') with team names "
                  "not parseable as individual players; stage2 captures reachable entries only",
    "1079024287": "Shercle Session #1 2004 — audit counts trick-combo lines with their own '1.' "
                  "prefix (per-player best-trick entries) as placements; stage2 correctly "
                  "captures only 6 real ranked placements (Shred30: 2, Sick3: 2, Ironman: 2)",
    "1093955766": "Russian Open Stage 5 2004 — audit sees '3-4' group-range entry as 2 rows "
                  "but stage2 captures only the individually named entries; gap ≤ 1",
    "1250478677": "Montreal End-of-Summer Jam 1 2009 — '2-Square' division header starts with "
                  "'2' and is matched by audit placement regex (place=2, player='Square'); "
                  "stage2 correctly identifies it as a division header; real count = 8",
    "1711181388": "Bulgarian Footbag Open 2024 — match score '25/21; 25/22; 25/20' on its own "
                  "line starts with '25', matched as placement by audit; real count = 8",
    "1756448770": "Bulgarian Footbag Championships 2025 — match score '21:17 / 21:15 / 22:20' "
                  "on its own line starts with '21', matched as 2 placements by audit; "
                  "real count = 16",
    "979089216":  "First Annual Eugene Freestyle Freekout 2001 — '45 second shred' division "
                  "header appears twice (pro + intermediate), each starting with '45', matched "
                  "as place=45 by audit; stage2 correctly identifies as division headers; "
                  "real count = 17 (1 tied-4th pair merged into single row)",
    "1745686591": "Footbag Finnish Open 2024 — audit counts '5-6' bracket header (starts with "
                  "'5') and '1st round, Round Robin...' (starts with '1') as 2 extra placements; "
                  "stage2 fixed via legacy file to 6 real placements (correct)",
    # Basque net tournaments — audit overcounts due to numbered pool-match lines,
    # date/score lines, or continuation-line player names in doubles format.
    # Stage2 correctly captures the 'Last Classification' or 'Final Classification'
    # section for each event. Real counts confirmed by manual inspection.
    "1634938934": "II.Basque Open Footbag Net Tournament (Doubles) 2021 — audit sees 6 rows "
                  "(numbered pool lines); real classification has 4 doubles pairs; stage2 correct",
    "1721923932": "V.Basque Tournament Footbag Net (Doubles) 2024 — audit sees 4 rows; "
                  "p2 entry lists 3 players (2 + reserve), stage2 correctly captures 3 teams",
    "1598616506": "II.Basque Tournament Footbag Net (Individual) 2020 — audit sees 10 rows; "
                  "numbered pool/match lines inflate count; real final classification has 8; "
                  "stage2 correct",
    "1564005204": "I.Ereaga Footbag Net Tournament (Doubles) 2019 — audit sees 5 rows; "
                  "numbered pool/match lines inflate count; real final classification has 4; "
                  "stage2 correct",
    "1574365921": "Basque Open Footbag Net Tournament (Doubles) 2019 — audit sees 5 rows; "
                  "numbered pool/match lines inflate count; real final classification has 4; "
                  "stage2 correct (p4 name has source typo '7' for separator)",
    "1653647467": "I.Elorrieta Tournament Footbag Net (Basque Country) 2022 — audit sees 5 rows; "
                  "numbered pool/match lines inflate count; real final classification has 4; "
                  "stage2 correct",
    "1657486956": "II.Ereaga Tournament Footbag Net 2022 — audit sees 5 rows; "
                  "numbered pool/match lines inflate count; real final classification has 4; "
                  "stage2 correct (p4 is a mixed doubles pair in individual event)",
    "1669556651": "III.Basque Tournament Footbag Net (Doubles) 2022 — audit sees 5 rows; "
                  "numbered pool/match lines inflate count; real final classification has 4; "
                  "stage2 correct",
    "1617902706": "III.Basque Tournament Footba Net (Individual) 2021 — audit sees 6 rows; "
                  "numbered pool/match lines inflate count; real final classification has 5; "
                  "stage2 correct",
    "1645621833": "IV.Basque Tournament Footbag Net (Individual) 2022 — audit sees 13 rows "
                  "(seeding + partial + final sections all parsed); stage2 fixed via legacy file "
                  "to 6 correct final classification placements",
    "1742511366": "VII.Basque Tournament Footbag Net (Individual) 2025 — audit sees 11 rows "
                  "(date/location lines + numbered pool match results); stage2 fixed via legacy "
                  "file to 7 correct final classification placements",
    "1566500647": "I.Basque Tournament Footbag Net (Individual) 2019 — audit sees 7 rows "
                  "(date/registration line parsed as p23); stage2 fixed via legacy file "
                  "to 5 correct final classification placements",
    "1677285621": "V.Basque Tournament Footbag Net (Individual) 2023 — audit sees 8 rows "
                  "('1. round' + '3rd and 4th position match' match headers counted); "
                  "stage2 fixed via legacy file to 6 correct placements",
}

# KNOWN_SOURCE_CORRUPT
#   Mirror HTML contains database error placeholders (ERROR 42109) or other
#   source corruption that makes some placements unrecoverable.
KNOWN_SOURCE_CORRUPT: dict[str, str] = {
    "857852604": "Southern CA Champs 1997 — ERROR 42109 DB corruption; blank player slots unrecoverable",
}

# Combined lookup for any known-explained event
def _known_cause(eid: str) -> str | None:
    if eid in KNOWN_SEEDING_DOUBLE_COUNT:
        return f"seeding_double_count: {KNOWN_SEEDING_DOUBLE_COUNT[eid]}"
    if eid in KNOWN_NO_RESULTS:
        return f"no_results_posted: {KNOWN_NO_RESULTS[eid]}"
    if eid in KNOWN_SOURCE_CORRUPT:
        return f"source_corrupt: {KNOWN_SOURCE_CORRUPT[eid]}"
    return None

# ── Encoding helpers (shared with tool 40) ─────────────────────────────────────

_ILLEGAL_CSV_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

_PLACEMENT_LINE_RE = re.compile(
    r"^\s*[1-9]\d{0,2}\s*"
    r"(?:[.)\-:]\s*\S|(?:st|nd|rd|th)\b|[°º])",
    re.MULTILINE,
)

_PLACE_NUM_RE = re.compile(r"^\s*([1-9]\d{0,2})")

# Sport header: lines like "*** Singles Net" or "*** Golf"
_SPORT_HEADER_RE = re.compile(r"^\s*\*+\s*(.+?)\s*\**\s*$")

# Division header: title-case line ending with colon (or bare)
_DIV_HEADER_BARE_RE = re.compile(
    r"^[A-Z][A-Za-z0-9\s&'\-/,()]{3,70}:\s*$"
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
    s = s.replace("\u00ad", "")   # soft hyphen
    s = s.replace("\u2011", "-")  # non-breaking hyphen
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _has_question_mark_name(s: str) -> bool:
    return bool(re.search(r"\w\?\w", s))


def _unicode_damage_score(s: str) -> int:
    count = 0
    for ch in s:
        if ch == "\ufffd":
            count += 1
        if unicodedata.category(ch) == "Cc" and ch not in ("\n", "\r", "\t"):
            count += 1
    return count


# ── Mirror HTML extraction (Phase 1) ──────────────────────────────────────────

def _get_results_text(event_id: str) -> tuple[str, list[str]]:
    """
    Extract raw results text and h2 division names from mirror HTML.
    Returns (raw_results_text, h2_div_names).
    """
    event_dir = MIRROR_DIR / "www.footbag.org" / "events" / "show" / event_id
    html_path = event_dir / "index.html"
    if not html_path.exists():
        alt = event_dir / f"{event_id}.html"
        if alt.exists():
            html_path = alt
        else:
            return "", []

    html = _fix_encoding(_read_html(html_path))
    soup = BeautifulSoup(html, "html.parser")

    h2_div_names: list[str] = []
    raw_results_text = ""

    results_div = soup.select_one("div.eventsResults")
    if results_div:
        for h2 in results_div.find_all("h2"):
            txt = h2.get_text(strip=True).replace("\u00a0", " ").strip()
            if txt and "manually" not in txt.lower():
                h2_div_names.append(txt)

        best_pre = None
        for pre in results_div.find_all("pre"):
            pre_text = pre.get_text("\n", strip=False).replace("\u00a0", " ")
            if _PLACEMENT_LINE_RE.search(pre_text):
                if best_pre is None or len(pre_text) > len(best_pre.get_text()):
                    best_pre = pre

        h2_text = results_div.get_text("\n", strip=False).replace("\u00a0", " ")

        if h2_div_names:
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
            raw_results_text = best_pre.get_text("\n", strip=False).replace("\u00a0", " ")
        else:
            raw_results_text = h2_text
    else:
        for pre in soup.find_all("pre"):
            pre_text = pre.get_text("\n", strip=False).replace("\u00a0", " ")
            if not _PLACEMENT_LINE_RE.search(pre_text):
                continue
            ev_div = soup.select_one("div.eventsEvents")
            if ev_div and pre in ev_div.find_all("pre"):
                continue
            raw_results_text = pre_text
            break

    return raw_results_text, h2_div_names


def _extract_score(player_text: str) -> tuple[str, str]:
    """
    Split player_text into (player_part, score_part).
    Trailing score patterns: " - 123", " (3yrs) - 45.6", etc.
    """
    m = re.search(r"\s+-\s+[\d.]+\s*$", player_text)
    if m:
        return player_text[:m.start()].strip(), player_text[m.start():].strip()
    m = re.search(r"\s+\(\d+\s*yrs?\)\s*-\s*[\d.]+\s*$", player_text, re.I)
    if m:
        return player_text[:m.start()].strip(), player_text[m.start():].strip()
    return player_text.strip(), ""


def parse_mirror_with_sports(
    event_id: str,
    raw_text: str,
    h2_names: list[str],
) -> list[dict]:
    """
    Walk lines tracking: current_sport, current_division, current_placement.

    Returns list of placement rows:
      {sport, division, placement, player_text_raw, player_names_split,
       team_flag, score_text, source_line}
    """
    lines = [ln.rstrip() for ln in raw_text.splitlines()]
    h2_norm = {_norm_div(n) for n in h2_names}

    rows: list[dict] = []
    current_sport = ""
    current_division = ""
    has_star_headers = False

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if re.search(r"manually entered results", stripped, re.I):
            break
        if "Related Photos" in stripped:
            break

        # ── Sport header (*** Freestyle, *** Golf, etc.) ──────────────────────
        sm = _SPORT_HEADER_RE.match(stripped)
        if sm:
            candidate = sm.group(1).strip().rstrip("*").strip()
            # Only treat as sport header if it doesn't also look like a division
            # (sport headers typically single or two words, no colon)
            if candidate and len(candidate.split()) <= 4:
                current_sport = candidate
                current_division = ""
                has_star_headers = True
                continue

        # ── Division header ────────────────────────────────────────────────────
        is_h2_header = _norm_div(stripped) in h2_norm
        is_heuristic_header = (
            not is_h2_header
            and not _PLACE_NUM_RE.match(stripped)
            and _DIV_HEADER_BARE_RE.match(stripped)
            and not re.search(r"\bvs\b|\bvs\.\b", stripped, re.I)
        )

        if is_h2_header or is_heuristic_header:
            current_division = stripped.rstrip(":").strip()
            continue

        # ── Placement line ─────────────────────────────────────────────────────
        m = _PLACE_NUM_RE.match(stripped)
        if m:
            place_num = int(m.group(1))
            if place_num > 500:
                continue
            player_text_raw = re.sub(
                r"^\s*\d+\s*[.)\-:]?\s*(?:st|nd|rd|th)?\s*", "", stripped
            ).strip()
            player_text_raw, score_text = _extract_score(player_text_raw)

            team_flag = "/" in player_text_raw
            if team_flag:
                parts = [p.strip() for p in player_text_raw.split("/", 1)]
            else:
                parts = [player_text_raw]
            player_names_split = "|".join(parts)

            rows.append({
                "sport":             current_sport,
                "division":          current_division,
                "placement":         place_num,
                "player_text_raw":   player_text_raw,
                "player_names_split": player_names_split,
                "team_flag":         "1" if team_flag else "",
                "score_text":        score_text,
                "source_line":       stripped,
                "_has_star_headers": has_star_headers,
            })

    return rows


def extract_mirror_v2(event_id: str) -> dict | None:
    """
    Full extraction for one event.
    Returns dict with keys:
      event_id, event_name, year, h2_div_names,
      rows (list of placement row dicts),
      has_results, has_star_headers
    """
    event_dir = MIRROR_DIR / "www.footbag.org" / "events" / "show" / event_id
    html_path = event_dir / "index.html"
    if not html_path.exists():
        alt = event_dir / f"{event_id}.html"
        if not alt.exists():
            return None
        html_path = alt

    # Get event name + year from HTML
    html_raw = _read_html(html_path)
    soup = BeautifulSoup(_fix_encoding(html_raw), "html.parser")
    event_name = ""
    if soup.title and soup.title.string:
        event_name = soup.title.string.strip()
    year = None
    for src in (event_name, html_raw[:2000]):
        m = re.search(r"\b(19\d{2}|20\d{2})\b", src)
        if m:
            y = int(m.group(1))
            if 1970 <= y <= 2030:
                year = y
                break

    raw_text, h2_names = _get_results_text(event_id)
    rows = parse_mirror_with_sports(event_id, raw_text, h2_names)
    has_star_headers = any(r.get("_has_star_headers") for r in rows)
    # Strip internal helper key
    for r in rows:
        r.pop("_has_star_headers", None)

    return {
        "event_id":        event_id,
        "event_name":      event_name,
        "year":            year,
        "h2_div_names":    h2_names,
        "rows":            rows,
        "has_results":     len(rows) > 0 or bool(h2_names),
        "has_star_headers": has_star_headers,
    }


# ── Stage-2 loading (Phase 2) ──────────────────────────────────────────────────

def load_stage2() -> dict:
    """Returns dict event_id → {event_name, year, event_type, by_div_raw, by_div_canon, total}"""
    events: dict[str, dict] = {}
    with open(STAGE2_CSV, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.DictReader(fh):
            eid = row["event_id"].strip()
            try:
                pj = json.loads(row.get("placements_json") or "[]")
            except (json.JSONDecodeError, TypeError):
                pj = []

            by_div_raw: dict[str, list] = defaultdict(list)
            by_div_canon: dict[str, list] = defaultdict(list)
            for p in pj:
                dr = (p.get("division_raw") or "").strip()
                dc = (p.get("division_canon") or dr).strip()
                by_div_raw[dr].append(p)
                by_div_canon[dc].append(p)

            events[eid] = {
                "event_name":  (row.get("event_name") or "").strip(),
                "year":        int(row.get("year") or 0) or None,
                "event_type":  (row.get("event_type") or "").strip(),
                "by_div_raw":  dict(by_div_raw),
                "by_div_canon": dict(by_div_canon),
                "total":       len(pj),
            }
    return events


# ── Comparison and completeness (Phase 3) ─────────────────────────────────────

def _word_overlap(a: str, b: str) -> float:
    aw = set(a.split())
    bw = set(b.split())
    if not aw and not bw:
        return 1.0
    return len(aw & bw) / max(len(aw | bw), 1)


def _match_sport_div_to_stage2(
    sport: str, division: str, s2_div_canon: dict[str, list]
) -> tuple[str, str, float]:
    """
    Return (matched_div_canon, match_type, score).
    match_type: 'exact', 'partial(N.NN)', 'no_match'
    """
    # Build composite key: "sport division" (e.g. "Golf Open") or just "division"
    composite = f"{sport} {division}".strip() if sport else division
    composite_n = _norm_div(composite)
    div_n = _norm_div(division)
    sport_n = _norm_div(sport)

    s2_norm = {_norm_div(k): k for k in s2_div_canon.keys()}

    # Try exact on composite
    if composite_n in s2_norm:
        return s2_norm[composite_n], "exact", 1.0

    # Try exact on division alone
    if div_n in s2_norm:
        return s2_norm[div_n], "exact", 1.0

    # Partial: best word overlap across composite and div_n
    best_key = ""
    best_score = 0.0
    for s2n, s2k in s2_norm.items():
        score = max(
            _word_overlap(composite_n, s2n),
            _word_overlap(div_n, s2n),
        )
        if score > best_score:
            best_score = score
            best_key = s2k

    if best_score >= 0.5 and best_key:
        return best_key, f"partial({best_score:.2f})", best_score
    return "", "no_match", 0.0


def _classify_failure_mode(
    ratio: float,
    gap: int,
    has_star_headers: bool,
    mirror_row_count: int,
    question_marks_in_raw: int,
) -> str:
    """Label likely failure mode — not a fix."""
    if ratio == 0.0:
        if has_star_headers:
            return "sport_header_not_detected"
        return "division_header_not_detected"
    if 0.45 <= ratio <= 0.55:
        return "seeding_plus_final_double_count"
    if abs(ratio - 0.5) < 0.01:
        return "seeding_plus_final_double_count"
    if gap > 10 and ratio < 0.9:
        return "placement_regex_too_strict"
    if question_marks_in_raw > 3 and ratio < 0.9:
        return "unicode_corruption"
    if ratio < 0.5:
        return "large_data_loss"
    if ratio < 0.9:
        return "partial_data_loss"
    return ""


def _compute_place_ranges(placements: list[int]) -> tuple[str, list[int]]:
    """
    Return (range_str, sorted_list).
    range_str: "1–N" or "1,3,5–8" compact notation
    """
    if not placements:
        return "", []
    s = sorted(set(placements))
    return f"{s[0]}-{s[-1]}" if len(s) > 1 else str(s[0]), s


def _missing_and_extra(mirror_places: list[int], sheet_places: list[int]) -> tuple[str, str, str]:
    """
    Returns (missing_places_str, extra_places_str, issue_type).
    """
    m_set = set(mirror_places)
    s_set = set(sheet_places)
    missing = sorted(m_set - s_set)
    extra = sorted(s_set - m_set)

    def compress(lst: list[int]) -> str:
        if not lst:
            return ""
        if len(lst) <= 10:
            return ",".join(str(x) for x in lst)
        return f"{lst[0]}-{lst[-1]} ({len(lst)} places)"

    # Classify issue
    if missing and not extra:
        if missing == list(range(missing[0], missing[-1] + 1)):
            if missing[0] > min(m_set):
                issue = "truncated_tail"
            else:
                issue = "missing_middle"
        else:
            issue = "truncated_tail"
    elif extra and not missing:
        issue = "extra_in_sheet"
    elif missing and extra:
        issue = "tie_lost"
    else:
        issue = ""

    return compress(missing), compress(extra), issue


# ── Core audit loop ────────────────────────────────────────────────────────────

def run_audit(filter_eids: list[str] | None, worlds_only: bool, threshold: float) -> None:
    print("Loading stage2…")
    stage2 = load_stage2()
    print(f"  {len(stage2):,} events")

    events_show = MIRROR_DIR / "www.footbag.org" / "events" / "show"
    mirror_event_ids: set[str] = set()
    if events_show.exists():
        mirror_event_ids = {d.name for d in events_show.iterdir()
                            if d.is_dir() and d.name.isdigit()}

    if filter_eids:
        audit_eids = filter_eids
    elif worlds_only:
        audit_eids = [eid for eid, ev in stage2.items() if ev["event_type"] == "worlds"]
    else:
        audit_eids = sorted(stage2.keys())

    print(f"Auditing {len(audit_eids)} events…")

    # Output accumulators
    mirror_v2_rows:    list[dict] = []
    completeness_rows: list[dict] = []
    diff_rows:         list[dict] = []
    event_summaries:   list[dict] = []

    total = len(audit_eids)
    for i, eid in enumerate(
        sorted(audit_eids, key=lambda x: int(stage2.get(x, {}).get("year") or 0))
    ):
        if i % 50 == 0:
            print(f"  [{i}/{total}]…")

        s2 = stage2.get(eid, {})
        event_name = s2.get("event_name", "")
        year       = s2.get("year") or 0
        event_type = s2.get("event_type", "")
        s2_total   = s2.get("total", 0)
        s2_div_canon = s2.get("by_div_canon", {})

        # Mirror extraction
        if eid not in mirror_event_ids:
            event_summaries.append({
                "event_id":              eid,
                "event_name":            event_name,
                "year":                  year,
                "mirror_divisions":      0,
                "sheet_divisions":       len(s2_div_canon),
                "mirror_total_rows":     0,
                "sheet_total_rows":      s2_total,
                "completeness_ratio":    "",
                "suspected_failure_mode": "no_mirror_html",
            })
            continue

        mirror = extract_mirror_v2(eid)
        if mirror is None:
            continue

        m_rows = mirror["rows"]
        has_star_headers = mirror["has_star_headers"]

        # Question-mark count in all player text
        qm_count = sum(
            1 for r in m_rows if _has_question_mark_name(r.get("player_text_raw", ""))
        )

        # Write mirror_results_v2 rows
        for r in m_rows:
            mirror_v2_rows.append({
                "event_id":          eid,
                "year":              year,
                "event_name":        event_name,
                "event_type":        event_type,
                "sport":             r["sport"],
                "division":          r["division"],
                "placement":         r["placement"],
                "player_text_raw":   r["player_text_raw"],
                "player_names_split": r["player_names_split"],
                "team_flag":         r["team_flag"],
                "score_text":        r["score_text"],
                "source_line":       r["source_line"],
            })

        # Group mirror rows by (sport, division)
        mirror_groups: dict[tuple[str, str], list[int]] = defaultdict(list)
        for r in m_rows:
            k = (r["sport"], r["division"])
            mirror_groups[k].append(r["placement"])

        # No divisions found — synthetic missing entry if event has h2 names
        if not mirror_groups and mirror["h2_div_names"]:
            for name in mirror["h2_div_names"]:
                mirror_groups[("", name)] = []

        # Match each mirror (sport, division) → stage2
        matched_s2_divs: set[str] = set()
        comp_rows_for_event: list[dict] = []

        for (sport, division), m_places in mirror_groups.items():
            if division == "__PREAMBLE__":
                continue

            mirror_row_count = len(m_places)
            mirror_max_place = max(m_places) if m_places else 0
            mirror_place_range, m_sorted = _compute_place_ranges(m_places)

            matched_dc, match_type, _ = _match_sport_div_to_stage2(
                sport, division, s2_div_canon
            )

            sheet_places: list[int] = []
            sheet_row_count = 0
            sheet_max_place = 0
            sheet_place_range = ""

            if matched_dc:
                matched_s2_divs.add(matched_dc)
                s2_placements = s2_div_canon.get(matched_dc, [])
                sheet_places = sorted(
                    int(p.get("place", 0)) for p in s2_placements if p.get("place")
                )
                sheet_row_count = len(sheet_places)
                sheet_max_place = max(sheet_places) if sheet_places else 0
                sheet_place_range, _ = _compute_place_ranges(sheet_places)

            ratio = (
                round(sheet_row_count / mirror_row_count, 4)
                if mirror_row_count > 0 else 0.0
            )
            gap = max(0, mirror_max_place - sheet_max_place)

            if mirror_row_count == 0:
                status = "MIRROR_ONLY_EMPTY"
            elif ratio == 0.0:
                status = "MISSING"
            elif ratio < 0.5:
                status = "MAJOR_LOSS"
            elif ratio < 0.9:
                status = "REDUCED"
            else:
                status = "OK" if gap <= 3 else "OK_PLACE_GAP"

            failure_mode = _classify_failure_mode(
                ratio, gap, has_star_headers, mirror_row_count, qm_count
            )

            # Place-diff
            missing_str, extra_str, issue_type = _missing_and_extra(m_sorted, sheet_places)

            comp_row = {
                "event_id":                  eid,
                "event_name":                event_name,
                "year":                      year,
                "event_type":                event_type,
                "sport":                     sport,
                "division":                  division,
                "matched_stage2_division":   matched_dc,
                "match_type":                match_type,
                "mirror_row_count":          mirror_row_count,
                "sheet_row_count":           sheet_row_count,
                "mirror_max_place":          mirror_max_place,
                "sheet_max_place":           sheet_max_place,
                "placement_completeness_ratio": ratio,
                "max_place_gap":             gap,
                "status":                    status,
                "failure_mode":              failure_mode,
                "notes":                     "",
            }
            completeness_rows.append(comp_row)
            comp_rows_for_event.append(comp_row)

            # Placement diff row
            if issue_type or missing_str or extra_str:
                diff_rows.append({
                    "event_id":         eid,
                    "event_name":       event_name,
                    "year":             year,
                    "sport":            sport,
                    "division":         division,
                    "mirror_place_range": mirror_place_range,
                    "sheet_place_range":  sheet_place_range,
                    "missing_places":   missing_str,
                    "extra_places":     extra_str,
                    "issue_type":       issue_type,
                })

        # Stage2 divisions with no mirror match
        for dc, s2pls in s2_div_canon.items():
            if dc in matched_s2_divs:
                continue
            sheet_row_count = len(s2pls)
            completeness_rows.append({
                "event_id":                  eid,
                "event_name":                event_name,
                "year":                      year,
                "event_type":                event_type,
                "sport":                     "",
                "division":                  "",
                "matched_stage2_division":   dc,
                "match_type":                "stage2_only",
                "mirror_row_count":          0,
                "sheet_row_count":           sheet_row_count,
                "mirror_max_place":          0,
                "sheet_max_place":           0,
                "placement_completeness_ratio": "",
                "max_place_gap":             0,
                "status":                    "STAGE2_ONLY",
                "failure_mode":              "",
                "notes":                     "",
            })

        # Event-level summary
        mirror_total = len(m_rows)
        sheet_total  = s2_total
        ev_ratio = round(sheet_total / mirror_total, 4) if mirror_total > 0 else 0.0

        # Aggregate failure modes for this event
        failure_modes_event = list({
            r["failure_mode"] for r in comp_rows_for_event if r["failure_mode"]
        })

        known_cause = _known_cause(eid)

        # Annotate completeness rows with known cause in notes field
        if known_cause:
            for r in comp_rows_for_event:
                r["notes"] = known_cause

        event_summaries.append({
            "event_id":              eid,
            "event_name":            event_name,
            "year":                  year,
            "mirror_divisions":      len(mirror_groups),
            "sheet_divisions":       len(s2_div_canon),
            "mirror_total_rows":     mirror_total,
            "sheet_total_rows":      sheet_total,
            "completeness_ratio":    ev_ratio,
            "suspected_failure_mode": "; ".join(failure_modes_event),
            "known_cause":           known_cause or "",
        })

    # ── Write outputs ──────────────────────────────────────────────────────────
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("\nWriting outputs…")

    _write_csv(OUT_DIR / "mirror_results_v2.csv", mirror_v2_rows, [
        "event_id", "year", "event_name", "event_type",
        "sport", "division", "placement",
        "player_text_raw", "player_names_split", "team_flag", "score_text",
        "source_line",
    ])

    _write_csv(OUT_DIR / "division_completeness_audit.csv", completeness_rows, [
        "event_id", "event_name", "year", "event_type",
        "sport", "division", "matched_stage2_division", "match_type",
        "mirror_row_count", "sheet_row_count",
        "mirror_max_place", "sheet_max_place",
        "placement_completeness_ratio", "max_place_gap",
        "status", "failure_mode", "notes",
    ])

    # Events with major loss (ratio < threshold) — exclude known false positives
    major_loss_all = [
        ev for ev in event_summaries
        if isinstance(ev["completeness_ratio"], float)
        and ev["completeness_ratio"] < threshold
        and ev["mirror_total_rows"] > 0
    ]
    major_loss_all.sort(key=lambda r: (r["completeness_ratio"], -(r["mirror_total_rows"])))

    # Split into unexplained (actionable) and explained (known cause)
    major_loss          = [ev for ev in major_loss_all if not ev["known_cause"]]
    major_loss_explained = [ev for ev in major_loss_all if ev["known_cause"]]

    _write_csv(OUT_DIR / "events_with_major_loss.csv", major_loss, [
        "event_id", "event_name", "year",
        "mirror_divisions", "sheet_divisions",
        "mirror_total_rows", "sheet_total_rows",
        "completeness_ratio", "suspected_failure_mode",
    ])

    _write_csv(OUT_DIR / "events_with_known_cause.csv", major_loss_explained, [
        "event_id", "event_name", "year",
        "mirror_divisions", "sheet_divisions",
        "mirror_total_rows", "sheet_total_rows",
        "completeness_ratio", "known_cause",
    ])

    _write_csv(OUT_DIR / "placement_diff_audit.csv", diff_rows, [
        "event_id", "event_name", "year", "sport", "division",
        "mirror_place_range", "sheet_place_range",
        "missing_places", "extra_places", "issue_type",
    ])

    # Summary report
    _write_summary_report(
        event_summaries, completeness_rows, diff_rows,
        major_loss, major_loss_explained, threshold
    )


# ── Output helpers ─────────────────────────────────────────────────────────────

def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            clean = {k: _illegal_csv(str(v) if v is not None else "") for k, v in row.items()}
            w.writerow(clean)
    print(f"  wrote {len(rows):,} rows → {path.relative_to(ROOT)}")


def _illegal_csv(s: str) -> str:
    return _ILLEGAL_CSV_RE.sub("", s)


_ILLEGAL_CSV_RE2 = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _write_summary_report(
    event_summaries: list[dict],
    completeness_rows: list[dict],
    diff_rows: list[dict],
    major_loss: list[dict],
    major_loss_explained: list[dict],
    threshold: float,
) -> None:
    path = OUT_DIR / "summary_report.md"

    total_mirror = sum(
        ev["mirror_total_rows"] for ev in event_summaries
        if isinstance(ev["mirror_total_rows"], int)
    )
    total_sheet = sum(
        ev["sheet_total_rows"] for ev in event_summaries
        if isinstance(ev["sheet_total_rows"], int)
    )
    overall_ratio = total_sheet / total_mirror if total_mirror else 0.0

    events_ok = sum(
        1 for ev in event_summaries
        if isinstance(ev["completeness_ratio"], float) and ev["completeness_ratio"] >= threshold
    )
    events_loss = len(major_loss)
    events_explained = len(major_loss_explained)
    events_no_mirror = sum(
        1 for ev in event_summaries
        if ev["completeness_ratio"] == ""
    )

    # Failure mode counts
    fm_counts: dict[str, int] = defaultdict(int)
    for row in completeness_rows:
        if row.get("failure_mode"):
            fm_counts[row["failure_mode"]] += 1

    # Worst divisions by truncation
    worst_divs = [
        r for r in completeness_rows
        if isinstance(r.get("placement_completeness_ratio"), float)
        and r["placement_completeness_ratio"] < threshold
        and r["mirror_row_count"] > 0
    ]
    worst_divs.sort(key=lambda r: (r["placement_completeness_ratio"], -r["mirror_row_count"]))

    # Issue type counts
    issue_counts: dict[str, int] = defaultdict(int)
    for row in diff_rows:
        if row.get("issue_type"):
            issue_counts[row["issue_type"]] += 1

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"# Deep Mirror Audit — Summary Report",
        f"",
        f"Generated: {ts}",
        f"",
        f"## 1. Overall Archive Completeness",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total mirror placements | {total_mirror:,} |",
        f"| Total sheet placements (stage2) | {total_sheet:,} |",
        f"| Overall completeness ratio | {overall_ratio:.1%} |",
        f"| Events ≥ {threshold:.0%} complete | {events_ok} |",
        f"| Events < {threshold:.0%} — unexplained (actionable) | {events_loss} |",
        f"| Events < {threshold:.0%} — explained (false pos / known) | {events_explained} |",
        f"| Events without mirror HTML | {events_no_mirror} |",
        f"",
        f"## 2. Top 20 Worst Events by Data Loss",
        f"",
        f"| Year | Event ID | Event Name | Mirror | Sheet | Ratio | Failure Mode |",
        f"|------|----------|------------|--------|-------|-------|--------------|",
    ]
    for ev in major_loss[:20]:
        lines.append(
            f"| {ev['year']} | {ev['event_id']} | {ev['event_name'][:45]} "
            f"| {ev['mirror_total_rows']} | {ev['sheet_total_rows']} "
            f"| {ev['completeness_ratio']:.1%} | {ev['suspected_failure_mode']} |"
        )

    lines += [
        f"",
        f"## 3. Top 20 Worst Divisions by Truncation",
        f"",
        f"| Year | Event | Sport | Division | Mirror | Sheet | Ratio | Failure Mode |",
        f"|------|-------|-------|----------|--------|-------|-------|--------------|",
    ]
    for r in worst_divs[:20]:
        sport_str = r["sport"] or "–"
        lines.append(
            f"| {r['year']} | {r['event_id']} | {sport_str} | {r['division'][:30]} "
            f"| {r['mirror_row_count']} | {r['sheet_row_count']} "
            f"| {r['placement_completeness_ratio']:.1%} | {r['failure_mode']} |"
        )

    lines += [
        f"",
        f"## 4. Parser Failure Mode Counts",
        f"",
        f"| Failure Mode | Count |",
        f"|-------------|-------|",
    ]
    for fm, cnt in sorted(fm_counts.items(), key=lambda x: -x[1]):
        lines.append(f"| `{fm}` | {cnt} |")
    if not fm_counts:
        lines.append("| (none) | – |")

    lines += [
        f"",
        f"## 5. Place-Gap Issue Types",
        f"",
        f"| Issue Type | Count |",
        f"|-----------|-------|",
    ]
    for it, cnt in sorted(issue_counts.items(), key=lambda x: -x[1]):
        lines.append(f"| `{it}` | {cnt} |")
    if not issue_counts:
        lines.append("| (none) | – |")

    lines += [
        f"",
        f"## 6. Explained Events (False Positives / Known Causes)",
        f"",
        f"These {events_explained} events appear below the {threshold:.0%} threshold but are",
        f"not genuine stage2 failures. They are excluded from `events_with_major_loss.csv`",
        f"and written separately to `events_with_known_cause.csv`.",
        f"",
        f"| Year | Event ID | Event Name | Mirror | Sheet | Ratio | Known Cause |",
        f"|------|----------|------------|--------|-------|-------|-------------|",
    ]
    for ev in sorted(major_loss_explained, key=lambda r: r["year"]):
        cause_short = ev["known_cause"].split(":")[0] if ev["known_cause"] else ""
        lines.append(
            f"| {ev['year']} | {ev['event_id']} | {ev['event_name'][:40]} "
            f"| {ev['mirror_total_rows']} | {ev['sheet_total_rows']} "
            f"| {ev['completeness_ratio']:.1%} | {cause_short} |"
        )

    lines += [
        f"",
        f"---",
        f"*Generated by tools/43_deep_mirror_audit.py*",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  wrote summary → {path.relative_to(ROOT)}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--worlds-only", action="store_true",
                   help="Restrict to World Footbag Championship events")
    p.add_argument("--event-id", metavar="ID",
                   help="Restrict to a single event_id")
    p.add_argument("--threshold", type=float, default=0.9,
                   help="Completeness ratio below which event is 'major loss' (default 0.9)")
    args = p.parse_args()

    if not MIRROR_DIR.exists():
        print(f"ERROR: mirror/ not found at {MIRROR_DIR}", file=sys.stderr)
        sys.exit(1)
    if not STAGE2_CSV.exists():
        print(f"ERROR: {STAGE2_CSV} not found — run rebuild first", file=sys.stderr)
        sys.exit(1)

    filter_eids = [args.event_id.strip()] if args.event_id else None
    run_audit(filter_eids, args.worlds_only, args.threshold)


if __name__ == "__main__":
    main()
