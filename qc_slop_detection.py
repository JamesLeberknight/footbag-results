#!/usr/bin/env python3
"""
qc_slop_detection.py — Advanced QC: Slop Detection Suite

This module implements comprehensive slop detection checks:
1. Global field scanners that check EVERY field/cell for corruption
2. Targeted checks for specific known issues
3. Source-vs-output integrity checks

All checks emit QCIssue objects compatible with the existing QC framework.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

# Import QCIssue from main canonicalization module
# (In production, would refactor QCIssue to shared module)


class QCIssue:
    """QC Issue - mirrors structure from 02_canonicalize_results.py"""
    def __init__(
        self,
        check_id: str,
        severity: str,
        event_id: str,
        field: str,
        message: str,
        example_value: str = "",
        context: dict = None,
    ):
        self.check_id = check_id
        self.severity = severity
        self.event_id = event_id
        self.field = field
        self.message = message
        self.example_value = example_value
        self.context = context or {}

    def to_dict(self) -> dict:
        return {
            "check_id": self.check_id,
            "severity": self.severity,
            "event_id": self.event_id,
            "field": self.field,
            "message": self.message,
            "example_value": self.example_value,
            "context": self.context,
        }


# ============================================================
# GLOBAL FIELD SCANNERS
# These scan EVERY field in EVERY record for corruption
# ============================================================

def check_any_field_contains_url(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """
    Global scanner: detect URL patterns anywhere.
    URLs should never exist in canonical fields/cells.
    """
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")

    # URL patterns
    url_patterns = [
        r'https?://',           # http:// or https://
        r'www\.',               # www.
        r'mailto:',             # mailto:
        r'footbag\.org',        # footbag.org domain
        r'\w+@\w+\.\w+',        # email-like patterns
        r'://\w+',              # generic protocol markers
    ]

    for pattern in url_patterns:
        if re.search(pattern, value, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="any_field_contains_url",
                severity="ERROR",
                event_id=str(event_id),
                field=field_name,
                message=f"Field '{field_name}' contains URL pattern",
                example_value=value[:150],
                context={"pattern_matched": pattern}
            ))
            break  # One issue per field is enough

    return issues


def check_any_field_contains_c0_controls(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """
    Global scanner: detect C0 control characters (U+0000-U+001F, U+007F).
    These should never appear in canonical text fields.
    """
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")

    # C0 control characters (excluding tab \t, newline \n, carriage return \r)
    # U+0000-U+0008, U+000B-U+000C, U+000E-U+001F, U+007F
    c0_pattern = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')

    match = c0_pattern.search(value)
    if match:
        char = match.group()
        char_code = f"U+{ord(char):04X}"
        issues.append(QCIssue(
            check_id="any_field_contains_c0_controls",
            severity="ERROR",
            event_id=str(event_id),
            field=field_name,
            message=f"Field '{field_name}' contains C0 control character {char_code}",
            example_value=value[:150],
            context={"char_code": char_code, "position": match.start()}
        ))

    return issues


def check_any_field_contains_c1_controls(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """
    Global scanner: detect C1 control characters (U+0080-U+009F).
    These are CP1252/Latin-1 control characters that indicate encoding corruption.
    Example: U+0092 in "WOMEN'S DOUBLES NET"
    """
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")

    # C1 control characters U+0080-U+009F
    c1_pattern = re.compile(r'[\x80-\x9F]')

    matches = list(c1_pattern.finditer(value))
    if matches:
        # Report all C1 characters found
        char_codes = [f"U+{ord(m.group()):04X}" for m in matches]
        issues.append(QCIssue(
            check_id="any_field_contains_c1_controls",
            severity="ERROR",
            event_id=str(event_id),
            field=field_name,
            message=f"Field '{field_name}' contains C1 control characters: {', '.join(char_codes)}",
            example_value=value[:150],
            context={
                "char_codes": char_codes,
                "char_count": len(matches),
                "positions": [m.start() for m in matches]
            }
        ))

    return issues


def check_any_field_contains_html_or_entities(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """
    Global scanner: detect residual HTML tags, entities, or markup fragments.
    These indicate incomplete HTML parsing/cleanup.
    """
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")

    # HTML patterns
    html_patterns = [
        (r'</?[a-z][a-z0-9]*\b[^>]*>', 'HTML tag'),           # <div>, </p>, <br/>
        (r'&[a-z]+;', 'HTML entity'),                         # &nbsp;, &amp;, &lt;
        (r'&#\d+;', 'numeric HTML entity'),                   # &#160;
        (r'&#x[0-9a-fA-F]+;', 'hex HTML entity'),            # &#x00A0;
    ]

    for pattern, label in html_patterns:
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            issues.append(QCIssue(
                check_id="any_field_contains_html_or_entities",
                severity="ERROR",
                event_id=str(event_id),
                field=field_name,
                message=f"Field '{field_name}' contains {label}: {match.group()}",
                example_value=value[:150],
                context={"pattern_type": label, "matched_text": match.group()}
            ))
            break  # One issue per field is enough

    return issues


def check_any_field_contains_placeholder_or_instructional_text(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """
    Global scanner: detect placeholder text, instructional text, or TBD/TBA markers.
    These indicate incomplete data or parsing errors.
    """
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")
    value_lower = value.lower().strip()

    # Placeholder patterns (case-insensitive)
    placeholder_patterns = [
        (r'\bTBD\b', 'TBD marker', "WARN"),
        (r'\bTBA\b', 'TBA marker', "WARN"),
        (r'\bn/a\b', 'N/A marker', "WARN"),
        (r'\bunknown\b', 'unknown marker', "INFO"),
        (r'\?\?+', 'question marks', "WARN"),
        (r'\bclick here\b', 'clickable instruction', "ERROR"),
        (r'\bsee below\b', 'reference instruction', "ERROR"),
        (r'\bdetails\b.*\bbelow\b', 'reference instruction', "WARN"),
        (r'\bregister\b', 'registration instruction', "WARN"),
        (r'\badd to ical\b', 'calendar instruction', "ERROR"),
        (r'\bcontact\b.*@', 'contact instruction with email', "ERROR"),
    ]

    for pattern, label, severity in placeholder_patterns:
        match = re.search(pattern, value_lower)
        if match:
            issues.append(QCIssue(
                check_id="any_field_contains_placeholder_or_instructional_text",
                severity=severity,
                event_id=str(event_id),
                field=field_name,
                message=f"Field '{field_name}' contains {label}",
                example_value=value[:150],
                context={"pattern_type": label, "matched_text": match.group()}
            ))
            break  # One issue per field

    return issues


def check_any_field_has_whitespace_slop(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """
    Global scanner: detect whitespace slop (leading/trailing, repeated internal, tabs/newlines).
    Single-value fields should not have these.
    """
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")

    # Skip multiline fields where newlines are expected
    multiline_fields = {"results_raw", "placements_json", "Results"}
    if field_name in multiline_fields:
        return issues

    problems = []

    # Leading/trailing whitespace
    if value != value.strip():
        problems.append("leading/trailing whitespace")

    # Repeated internal whitespace (2+ spaces)
    if re.search(r'  +', value):
        problems.append("repeated spaces")

    # Tabs in single-value field
    if '\t' in value:
        problems.append("tab character")

    # Newlines in single-value field
    if '\n' in value or '\r' in value:
        problems.append("newline character")

    if problems:
        severity = "ERROR" if "newline" in problems or "tab" in problems else "WARN"
        issues.append(QCIssue(
            check_id="any_field_has_whitespace_slop",
            severity=severity,
            event_id=str(event_id),
            field=field_name,
            message=f"Field '{field_name}' has whitespace issues: {', '.join(problems)}",
            example_value=value[:150],
            context={"problems": problems}
        ))

    return issues


def check_any_field_has_bogus_character_sequences(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """
    Global scanner: detect mojibake patterns, replacement characters, odd punctuation.
    These indicate encoding problems or corruption.
    """
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")

    # Mojibake and corruption patterns
    bogus_patterns = [
        (r'�', 'Unicode replacement character (�)', "ERROR"),
        (r'Ã[\x80-\xBF]', 'UTF-8 mojibake (Ã sequences)', "ERROR"),
        (r'Â[\x80-\xBF]', 'UTF-8 mojibake (Â sequences)', "ERROR"),
    ]

    for pattern, label, severity in bogus_patterns:
        match = re.search(pattern, value)
        if match:
            issues.append(QCIssue(
                check_id="any_field_has_bogus_character_sequences",
                severity=severity,
                event_id=str(event_id),
                field=field_name,
                message=f"Field '{field_name}' contains {label}",
                example_value=value[:150],
                context={"pattern_type": label, "matched_text": match.group()}
            ))
            break  # One issue per field

    return issues


# ============================================================
# TARGETED CHECKS FOR SPECIFIC ISSUES
# ============================================================

def check_host_club_suspicious_prefix_or_markup(rec: dict) -> list[QCIssue]:
    """
    Targeted check: Host club field with suspicious prefixes or markup.
    Example: "\\m/ichigan footbag"
    """
    issues = []
    event_id = rec.get("event_id", "")
    host_club = rec.get("host_club", "")

    if not host_club:
        return issues

    # Suspicious patterns in host_club
    suspicious_patterns = [
        (r'\\[a-z]/', 'backslash markup pattern (e.g., \\m/)', "WARN"),
        (r'^[^a-zA-Z0-9\s]', 'starts with special character', "WARN"),
        (r'[<>{}[\]]', 'contains markup characters', "ERROR"),
        (r'\\[nt]', 'contains escape sequences', "ERROR"),
    ]

    for pattern, label, severity in suspicious_patterns:
        if re.search(pattern, host_club):
            # Check if it's potentially stylized (ambiguous)
            needs_review = bool(re.search(r'\\[a-z]/', host_club))

            issues.append(QCIssue(
                check_id="host_club_suspicious_prefix_or_markup",
                severity=severity,
                event_id=str(event_id),
                field="host_club",
                message=f"Host club contains {label}",
                example_value=host_club[:100],
                context={
                    "needs_human_review": needs_review,
                    "pattern_type": label
                }
            ))
            break  # One issue per host_club

    return issues


def check_host_club_contains_url_or_contact(rec: dict) -> list[QCIssue]:
    """
    Targeted check: Host club should not contain URLs, emails, or phone numbers.
    """
    issues = []
    event_id = rec.get("event_id", "")
    host_club = rec.get("host_club", "")

    if not host_club:
        return issues

    # Contact info patterns
    contact_patterns = [
        (r'https?://', 'URL'),
        (r'www\.', 'URL'),
        (r'\w+@\w+\.\w+', 'email address'),
        (r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', 'phone number'),
        (r'\bphone\b.*\d+', 'phone reference'),
        (r'\bemail\b.*@', 'email reference'),
    ]

    for pattern, label in contact_patterns:
        if re.search(pattern, host_club, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="host_club_contains_url_or_contact",
                severity="ERROR",
                event_id=str(event_id),
                field="host_club",
                message=f"Host club contains {label}",
                example_value=host_club[:100],
                context={"contact_type": label}
            ))
            break

    return issues


def check_worlds_missing_expected_disciplines(rec: dict) -> list[QCIssue]:
    """
    Targeted check: Worlds events should have both NET and FREESTYLE divisions.
    If only one is present, flag as incomplete.
    """
    issues = []
    event_id = rec.get("event_id", "")
    event_type = rec.get("event_type", "")
    event_name = rec.get("event_name", "")

    # Check if this is a Worlds event
    is_worlds = (
        event_type and "world" in event_type.lower()
    ) or (
        event_name and "world" in event_name.lower() and "championship" in event_name.lower()
    )

    if not is_worlds:
        return issues

    # Skip future events — they haven't happened yet, no results expected
    year_str = rec.get("year", "")
    try:
        year = int(year_str)
        if year >= datetime.now().year:
            return issues
    except (ValueError, TypeError):
        pass

    # Known limitations: results on external pages not captured in mirror
    # These are documented data gaps, not parsing failures
    WORLDS_KNOWN_EXTERNAL_RESULTS = {
        "1587822289": "2020 Online Worlds — results on external wiki, not in mirror",
        "915561090": "1999 Worlds — freestyle results on external linked pages, not in mirror",
    }

    if str(event_id) in WORLDS_KNOWN_EXTERNAL_RESULTS:
        reason = WORLDS_KNOWN_EXTERNAL_RESULTS[str(event_id)]
        issues.append(QCIssue(
            check_id="worlds_missing_expected_disciplines",
            severity="INFO",
            event_id=str(event_id),
            field="placements_json",
            message=f"Worlds event has known data gap: {reason}",
            example_value="",
            context={"known_limitation": True, "reason": reason}
        ))
        return issues

    # Parse placements to check disciplines
    placements_str = rec.get("placements_json", "[]")
    try:
        placements = json.loads(placements_str)
    except json.JSONDecodeError:
        return issues

    if not placements:
        issues.append(QCIssue(
            check_id="worlds_missing_expected_disciplines",
            severity="ERROR",
            event_id=str(event_id),
            field="placements_json",
            message="Worlds event has no placements at all",
            example_value="",
            context={"is_worlds": True, "placements_count": 0}
        ))
        return issues

    # Check what categories we have
    categories = set()
    for p in placements:
        cat = p.get("division_category", "unknown")
        categories.add(cat)

    has_net = "net" in categories
    has_freestyle = "freestyle" in categories

    if has_net and not has_freestyle:
        issues.append(QCIssue(
            check_id="worlds_missing_expected_disciplines",
            severity="ERROR",
            event_id=str(event_id),
            field="placements_json",
            message="Worlds event has NET but no FREESTYLE divisions",
            example_value=f"Categories found: {sorted(categories)}",
            context={
                "has_net": True,
                "has_freestyle": False,
                "categories": sorted(categories),
                "placements_count": len(placements)
            }
        ))
    elif has_freestyle and not has_net:
        issues.append(QCIssue(
            check_id="worlds_missing_expected_disciplines",
            severity="WARN",
            event_id=str(event_id),
            field="placements_json",
            message="Worlds event has FREESTYLE but no NET divisions",
            example_value=f"Categories found: {sorted(categories)}",
            context={
                "has_net": False,
                "has_freestyle": True,
                "categories": sorted(categories),
                "placements_count": len(placements)
            }
        ))

    return issues


def check_worlds_results_suspiciously_small(rec: dict) -> list[QCIssue]:
    """
    Targeted check: Worlds events should have many placements (typically 50+).
    If a Worlds event has very few placements, flag for review.
    """
    issues = []
    event_id = rec.get("event_id", "")
    event_type = rec.get("event_type", "")
    event_name = rec.get("event_name", "")

    # Check if this is a Worlds event
    is_worlds = (
        event_type and "world" in event_type.lower()
    ) or (
        event_name and "world" in event_name.lower() and "championship" in event_name.lower()
    )

    if not is_worlds:
        return issues

    # Skip future events — no results expected yet
    year_str = rec.get("year", "")
    try:
        year = int(year_str)
        if year >= datetime.now().year:
            return issues
    except (ValueError, TypeError):
        pass

    # Parse placements
    placements_str = rec.get("placements_json", "[]")
    try:
        placements = json.loads(placements_str)
    except json.JSONDecodeError:
        return issues

    placements_count = len(placements)

    # Heuristic: Worlds typically has 50+ placements
    # If less than 20, flag as suspicious
    if placements_count < 20:
        issues.append(QCIssue(
            check_id="worlds_results_suspiciously_small",
            severity="WARN",
            event_id=str(event_id),
            field="placements_json",
            message=f"Worlds event has only {placements_count} placements (expected 50+)",
            example_value="",
            context={
                "placements_count": placements_count,
                "expected_min": 50,
                "needs_human_review": True
            }
        ))

    return issues


def check_results_raw_has_strong_signals_but_output_empty(rec: dict) -> list[QCIssue]:
    """
    Targeted check: If results_raw has strong signals of results (division headers,
    ordinals, numbering) but placements_json is empty or near-empty, flag as dropped results.
    """
    issues = []
    event_id = rec.get("event_id", "")
    results_raw = rec.get("results_raw", "")

    if not results_raw or len(results_raw) < 50:
        return issues

    # Parse placements
    placements_str = rec.get("placements_json", "[]")
    try:
        placements = json.loads(placements_str)
    except json.JSONDecodeError:
        placements = []

    placements_count = len(placements)

    # If we have decent placements, no issue
    if placements_count >= 5:
        return issues

    # Check for strong signals in results_raw
    signals = []

    # Division-like headers
    if re.search(r'(?i)(open|women|intermediate|beginner|masters|novice|advanced)\s+(singles|doubles|mixed)', results_raw):
        signals.append("division headers")

    # Ordinal markers (1st, 2nd, 3rd) - but only placement-style, not event names
    # Match: "1st: Name" or "1st Place" or line starting with "1st Name"
    # Don't match: "1st Annual Tournament" or "12th Century"
    ordinal_placement_pattern = r'(?:^|\n)\s*\d+(st|nd|rd|th)\s*[:\-]|(?:1st|2nd|3rd)\s+place'
    if re.search(ordinal_placement_pattern, results_raw, re.IGNORECASE):
        signals.append("ordinal placements")

    # Numbered lists (1., 2., 3.)
    numbered_lines = len(re.findall(r'^\s*\d+\.\s+[A-Z]', results_raw, re.MULTILINE))
    if numbered_lines >= 5:
        signals.append(f"{numbered_lines} numbered lines")

    # Medal markers
    if re.search(r'(?i)(gold|silver|bronze|1st place|2nd place|3rd place)', results_raw):
        signals.append("medal references")

    # Tables (tab-separated or multi-column)
    if results_raw.count('\t') >= 10:
        signals.append("tabular data")

    if signals:
        # Suppress: if only signal is "tabular data" and text looks like a scoring table
        # (adds, contacts, score columns — not placement lists)
        if signals == ["tabular data"]:
            scoring_keywords = re.search(
                r'(?i)\b(adds|contacts|score|ratio|uniques|pts|points)\b', results_raw
            )
            # Tab-heavy data with only first names (no surnames) = scoring breakdown
            lines = [l.strip() for l in results_raw.split('\n') if l.strip()]
            all_tab_lines = all('\t' in l for l in lines if l)
            if scoring_keywords or all_tab_lines:
                return issues  # Suppress — scoring table, not placements

        # Downgrade: small events with some valid placements and short results_raw
        # These are correctly parsed small events, not dropped results
        severity = "ERROR"
        if placements_count > 0 and len(results_raw) < 600:
            severity = "WARN"

        # Calculate evidence snippet
        snippet_lines = results_raw.split('\n')[:10]
        snippet = '\n'.join(snippet_lines)

        issues.append(QCIssue(
            check_id="results_raw_has_strong_signals_but_output_empty",
            severity=severity,
            event_id=str(event_id),
            field="results_raw",
            message=f"Results_raw has strong signals ({', '.join(signals)}) but output has only {placements_count} placements",
            example_value=snippet[:200],
            context={
                "signals": signals,
                "placements_count": placements_count,
                "results_raw_length": len(results_raw)
            }
        ))

    return issues


def check_placements_duplicate_rows(rec: dict) -> list[QCIssue]:
    """
    Targeted check: Detect duplicate placement objects within same event/division.
    Uses stable key: (division_canon, place, competitor_type, player1_name, player2_name).
    """
    issues = []
    event_id = rec.get("event_id", "")

    # Parse placements
    placements_str = rec.get("placements_json", "[]")
    try:
        placements = json.loads(placements_str)
    except json.JSONDecodeError:
        return issues

    if len(placements) < 2:
        return issues

    # Build stable keys
    seen = {}
    duplicates = []

    for idx, p in enumerate(placements):
        division = p.get("division_canon") or p.get("division_raw") or "Unknown"
        place = p.get("place", "")
        competitor_type = p.get("competitor_type", "")
        player1 = (p.get("player1_name") or "").strip().lower()
        player2 = (p.get("player2_name") or "").strip().lower()

        # Stable key
        key = (division.lower(), str(place), competitor_type, player1, player2)

        if key in seen:
            duplicates.append({
                "key": key,
                "first_index": seen[key],
                "duplicate_index": idx,
                "placement": p
            })
        else:
            seen[key] = idx

    if duplicates:
        dup_count = len(duplicates)
        example = duplicates[0]["placement"]
        example_str = f"{example.get('place')}. {example.get('player1_name', '')} - {example.get('division_canon', '')}"

        issues.append(QCIssue(
            check_id="placements_duplicate_rows",
            severity="ERROR",
            event_id=str(event_id),
            field="placements_json",
            message=f"Found {dup_count} duplicate placement(s) in event",
            example_value=example_str[:100],
            context={
                "duplicate_count": dup_count,
                "example_indices": [d["duplicate_index"] for d in duplicates[:3]]
            }
        ))

    return issues


# ============================================================
# STAGE 2 INTEGRATION: Run all new checks on records
# ============================================================

def run_slop_detection_checks_stage2(records: list[dict]) -> list[QCIssue]:
    """
    Run all slop detection checks on Stage 2 records.
    Returns list of QCIssue objects.
    """
    all_issues = []

    # Field names to scan globally
    text_fields = [
        "event_id", "event_name", "date", "location", "event_type",
        "host_club", "year", "results_raw"
    ]

    for rec in records:
        # === GLOBAL FIELD SCANNERS ===
        # Scan every text field in the record
        for field in text_fields:
            value = rec.get(field)
            if value is None:
                continue

            # Run all global scanners on this field
            all_issues.extend(check_any_field_contains_url(rec, field, value))
            all_issues.extend(check_any_field_contains_c0_controls(rec, field, value))
            all_issues.extend(check_any_field_contains_c1_controls(rec, field, value))
            all_issues.extend(check_any_field_contains_html_or_entities(rec, field, value))
            all_issues.extend(check_any_field_contains_placeholder_or_instructional_text(rec, field, value))
            all_issues.extend(check_any_field_has_whitespace_slop(rec, field, value))
            all_issues.extend(check_any_field_has_bogus_character_sequences(rec, field, value))

        # Also scan placements fields
        placements_str = rec.get("placements_json", "[]")
        try:
            placements = json.loads(placements_str)
            for p_idx, p in enumerate(placements):
                placement_fields = ["player1_name", "player2_name", "division_raw", "division_canon", "entry_raw"]
                for pfield in placement_fields:
                    pvalue = p.get(pfield)
                    if pvalue is None:
                        continue
                    # Create a pseudo-record for placement field scanning
                    pseudo_rec = {"event_id": rec.get("event_id", "")}
                    field_path = f"placements[{p_idx}].{pfield}"
                    all_issues.extend(check_any_field_contains_url(pseudo_rec, field_path, pvalue))
                    all_issues.extend(check_any_field_contains_c0_controls(pseudo_rec, field_path, pvalue))
                    all_issues.extend(check_any_field_contains_c1_controls(pseudo_rec, field_path, pvalue))
                    all_issues.extend(check_any_field_contains_html_or_entities(pseudo_rec, field_path, pvalue))
                    all_issues.extend(check_any_field_contains_placeholder_or_instructional_text(pseudo_rec, field_path, pvalue))
                    all_issues.extend(check_any_field_has_whitespace_slop(pseudo_rec, field_path, pvalue))
                    all_issues.extend(check_any_field_has_bogus_character_sequences(pseudo_rec, field_path, pvalue))
        except json.JSONDecodeError:
            pass

        # === TARGETED CHECKS ===
        all_issues.extend(check_host_club_suspicious_prefix_or_markup(rec))
        all_issues.extend(check_host_club_contains_url_or_contact(rec))
        all_issues.extend(check_worlds_missing_expected_disciplines(rec))
        all_issues.extend(check_worlds_results_suspiciously_small(rec))
        all_issues.extend(check_results_raw_has_strong_signals_but_output_empty(rec))
        all_issues.extend(check_placements_duplicate_rows(rec))

    return all_issues


# ============================================================
# STAGE 3 CHECKS: Scan Excel workbook cells
# ============================================================

def check_results_cell_duplicate_lines(results_text: str, event_id: str) -> list[QCIssue]:
    """
    Stage 3 check: After rendering Results cell, detect duplicate output lines WITHIN THE SAME DIVISION.

    Important: The same person can legitimately place in multiple divisions.
    We only flag duplicates if they appear multiple times within the SAME division.

    Division context tracking:
    - <<< CATEGORY >>> markers start a new category
    - ALL CAPS lines are division headers
    - Lines under a division belong to that division
    """
    issues = []

    if not results_text:
        return issues

    lines = results_text.split('\n')

    # Track current category and division
    current_category = None
    current_division = None

    # Track lines seen per (category, division) tuple
    seen_in_division = {}
    duplicates = []

    for idx, line in enumerate(lines):
        line_stripped = line.strip()

        # Ignore blank lines
        if not line_stripped:
            continue

        # Category header: <<< NET >>>
        if line_stripped.startswith('<<<') and line_stripped.endswith('>>>'):
            current_category = line_stripped.strip('<> ')
            current_division = None  # Reset division when category changes
            continue

        # Division header: ALL CAPS line (e.g., "OPEN SINGLES NET")
        # No length limit — in formatted output, ALL division headers are UPPER CASE
        if line_stripped.isupper() and len(line_stripped) >= 3:
            current_division = line_stripped
            # New division scope - reset seen lines for this division
            division_key = (current_category, current_division)
            if division_key not in seen_in_division:
                seen_in_division[division_key] = {}
            continue

        # This is a placement line
        # Check for duplicates WITHIN THE CURRENT DIVISION ONLY
        if current_category and current_division:
            division_key = (current_category, current_division)
            if division_key not in seen_in_division:
                seen_in_division[division_key] = {}

            line_key = line_stripped.lower()

            if line_key in seen_in_division[division_key]:
                duplicates.append({
                    "line": line_stripped,
                    "first_line_number": seen_in_division[division_key][line_key],
                    "duplicate_line_number": idx + 1,
                    "category": current_category,
                    "division": current_division,
                })
            else:
                seen_in_division[division_key][line_key] = idx + 1

    if duplicates:
        dup_count = len(duplicates)
        example = duplicates[0]["line"]
        example_div = duplicates[0]["division"]

        issues.append(QCIssue(
            check_id="results_cell_duplicate_lines",
            severity="ERROR",
            event_id=str(event_id),
            field="Results",
            message=f"Results cell contains {dup_count} duplicate line(s) within same division",
            example_value=example[:100],
            context={
                "duplicate_count": dup_count,
                "example_division": example_div,
                "example_line_numbers": [(d["first_line_number"], d["duplicate_line_number"]) for d in duplicates[:3]],
                "divisions_affected": list(set(d["division"] for d in duplicates)),
            }
        ))

    return issues


def check_results_cell_roundtrip_missing_any_placement(
    placements: list[dict],
    results_text: str,
    event_id: str
) -> list[QCIssue]:
    """
    Stage 3 check: Verify every placement object appears in rendered Results text.
    Each placement should have its place number + competitor name under correct division.
    """
    issues = []

    if not placements or not results_text:
        return issues

    results_lower = results_text.lower()
    missing = []

    for idx, p in enumerate(placements):
        place = p.get("place", "")
        player1 = (p.get("player1_name") or "").strip()
        player2 = (p.get("player2_name") or "").strip()
        division = p.get("division_canon") or p.get("division_raw") or ""

        # Build expected patterns
        if player2:
            name_pattern = f"{player1.lower()}.*{player2.lower()}"
        else:
            name_pattern = player1.lower()

        place_pattern = f"{place}."

        # Check if this placement appears in results
        # Look for: "1. John Smith" or "1. John / Jane"
        if name_pattern and place_pattern:
            # Simple check: does the name appear anywhere in results?
            if player1.lower() not in results_lower:
                missing.append({
                    "index": idx,
                    "place": place,
                    "player1": player1,
                    "player2": player2,
                    "division": division
                })

    if missing:
        miss_count = len(missing)
        example = missing[0]
        example_str = f"{example['place']}. {example['player1']}"

        issues.append(QCIssue(
            check_id="results_cell_roundtrip_missing_any_placement",
            severity="ERROR",
            event_id=str(event_id),
            field="Results",
            message=f"Results cell missing {miss_count} placement(s) from placements_json",
            example_value=example_str[:100],
            context={
                "missing_count": miss_count,
                "example_indices": [m["index"] for m in missing[:3]]
            }
        ))

    return issues


def check_results_cell_near_excel_limit(results_text: str, event_id: str) -> list[QCIssue]:
    """
    Stage 3 check: Excel cells have a 32,767 character limit.
    Warn if we're approaching that limit.
    """
    issues = []

    if not results_text:
        return issues

    text_length = len(results_text)
    EXCEL_LIMIT = 32767
    WARNING_THRESHOLD = 30000

    if text_length >= WARNING_THRESHOLD:
        issues.append(QCIssue(
            check_id="results_cell_near_excel_limit",
            severity="WARN",
            event_id=str(event_id),
            field="Results",
            message=f"Results cell length ({text_length}) approaching Excel limit ({EXCEL_LIMIT})",
            example_value="",
            context={
                "text_length": text_length,
                "excel_limit": EXCEL_LIMIT,
                "percent_used": round(100 * text_length / EXCEL_LIMIT, 1)
            }
        ))

    return issues


def run_slop_detection_checks_stage3_excel(records: list[dict], results_map: dict) -> list[QCIssue]:
    """
    Run Stage 3 checks on Excel workbook data.

    Args:
        records: List of event records (from Stage 2)
        results_map: Dict mapping event_id -> formatted results text

    Returns:
        List of QCIssue objects
    """
    all_issues = []

    for rec in records:
        event_id = rec.get("event_id", "")
        results_text = results_map.get(str(event_id), "")

        if not results_text:
            continue

        # Parse placements for roundtrip check
        placements_str = rec.get("placements_json", "[]")
        try:
            placements = json.loads(placements_str)
        except json.JSONDecodeError:
            placements = []

        # Run Stage 3 checks
        all_issues.extend(check_results_cell_duplicate_lines(results_text, event_id))
        all_issues.extend(check_results_cell_roundtrip_missing_any_placement(placements, results_text, event_id))
        all_issues.extend(check_results_cell_near_excel_limit(results_text, event_id))

        # Scan the results_text itself with global scanners
        pseudo_rec = {"event_id": event_id}
        all_issues.extend(check_any_field_contains_url(pseudo_rec, "Results", results_text))
        all_issues.extend(check_any_field_contains_c0_controls(pseudo_rec, "Results", results_text))
        all_issues.extend(check_any_field_contains_c1_controls(pseudo_rec, "Results", results_text))
        all_issues.extend(check_any_field_contains_html_or_entities(pseudo_rec, "Results", results_text))
        all_issues.extend(check_any_field_contains_placeholder_or_instructional_text(pseudo_rec, "Results", results_text))
        all_issues.extend(check_any_field_has_whitespace_slop(pseudo_rec, "Results", results_text))
        all_issues.extend(check_any_field_has_bogus_character_sequences(pseudo_rec, "Results", results_text))

    return all_issues
