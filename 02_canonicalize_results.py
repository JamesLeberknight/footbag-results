#!/usr/bin/env python3
"""
02_canonicalize_results.py — Stage 2: Canonicalize raw event data

This script:
- Reads out/stage1_raw_events.csv
- Parses results text into structured placements
- Outputs: out/stage2_canonical_events.csv

Input: out/stage1_raw_events.csv
Output: out/stage2_canonical_events.csv
"""

from __future__ import annotations

import csv
import json
import re
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional


# ------------------------------------------------------------
# QC Constants
# ------------------------------------------------------------
VALID_EVENT_TYPES = {"freestyle", "net", "worlds", "mixed", "social", "golf", ""}
YEAR_MIN = 1970
YEAR_MAX = 2030

# Expected divisions by event type for cross-validation
EXPECTED_DIVISIONS = {
    "worlds": {
        "required": ["net"],        # ERROR if missing
        "expected": ["freestyle"],  # WARN if missing
    },
    "net": {
        "required": ["net"],
        "expected": [],
    },
    "freestyle": {
        "required": ["freestyle"],
        "expected": [],
    },
    "golf": {
        "required": ["golf"],
        "expected": [],
    },
    "mixed": {
        "required": [],
        "expected": [],  # Can have net, freestyle, or both
    },
    "social": {
        "required": [],
        "expected": [],
    },
}

# Known broken source events (SQL errors in original HTML mirror)
# Decision: 2026-02 - These 9 events exist in the mirror but have SQL errors.
# The original footbag.org site had unescaped apostrophes that broke queries.
# We keep the event name (from <title>) and use location/year overrides.
# Note: 11 other broken events were removed - they don't exist in the mirror.
KNOWN_BROKEN_SOURCE_EVENTS = {
    "1023993464",  # Funtastik Summer Classic Footbag Tournament
    "1030642331",  # Seattle Juggling and Footbag Festival
    "1099545007",  # Seapa NZ Footbag Nationals 2005
    "1151949245",  # ShrEdmonton 2006
    "1278991986",  # 23rd Annual Vancouver Open Footbag Championships
    "1299244521",  # Warsaw Footbag Open 2011
    "860082052",   # Texas State Footbag Championships
    "941066992",   # WESTERN REGIONAL FOOTBAG CHAMPIONSHIPS
    "959094047",   # Battle of the Year Switzerland
}
BROKEN_SOURCE_MESSAGE = "[SOURCE ERROR: Database error in original HTML]"

# Junk events to exclude from final output
# Decision: 2026-02 - These events have no useful data (no year, no location, no results)
# Only the event name exists, which isn't useful without context
# Note: 1146524016 and 879559482 were removed - they don't exist in the mirror
JUNK_EVENTS_TO_EXCLUDE = {
    "1118129163",  # "Event Listing" placeholder - June TBA 2005, Site TBA, no results
    "1031232420",  # FootJam03 - stats not results: "91 players registered...1000+spectators"
    "1043428338",  # Spring Footbag Jam - description not results: "31 players said SFJ was their first"
}

# Event name overrides for placeholder/template names
# Decision: 2026-02 - Some events have template names that need human correction
EVENT_NAME_OVERRIDES = {
    "1068164371": "Portland Footbag Jam 2003 (Oct 23)",  # Was: "Footbag WorldWide Event Listing: Event Listing"
    "1068164424": "Portland Footbag Jam 2003 (Oct 31)",  # Was: "Footbag WorldWide Event Listing: Event Listing"
}

# Year overrides for broken source events
# Decision: 2026-02-06 - Years extracted from results_year_YYYY directory references in mirror
# The source mirror ALWAYS has year data (events are sorted by year on the website)
YEAR_OVERRIDES = {
    "1023993464": 2002,  # Funtastik Summer Classic Footbag Tournament
    "1030642331": 2002,  # Seattle Juggling and Footbag Festival
    "1278991986": 2010,  # 23rd Annual Vancouver Open Footbag Championships
    "860082052": 1997,   # Texas State Footbag Championships
    "941066992": 2000,   # WESTERN REGIONAL FOOTBAG CHAMPIONSHIPS
    "959094047": 2000,   # Battle of the Year Switzerland
}

# Location overrides for broken source events (inferred from event names)
# Decision: 2026-02 - These locations were inferred from event names for events
# where the original HTML had SQL errors and no location data was available.
LOCATION_OVERRIDES = {
    # === Broken source events (SQL errors, 9 total) ===
    "1023993464": "Hershey, Pennsylvania, USA",         # Funtastik Summer Classic (always Hershey)
    "1030642331": "Seattle, Washington, USA",           # Seattle Juggling and Footbag Festival
    "1099545007": "New Zealand",                        # Seapa NZ Footbag Nationals 2005
    "1151949245": "Edmonton, Alberta, Canada",          # ShrEdmonton 2006
    "1278991986": "Vancouver, British Columbia, Canada",# 23rd Annual Vancouver Open
    "1299244521": "Warsaw, Poland",                     # Warsaw Footbag Open 2011
    "860082052": "Texas, USA",                          # Texas State Footbag Championships
    "941066992": "California, USA",                     # Western Regional (always California)
    "959094047": "Switzerland",                         # Battle of the Year Switzerland
    # Decision: 2026-02 - Verbose/multi-sentence locations simplified
    "1076748214": "St. Louis, Missouri, USA",           # Was: St. Matthias Church St. Louis...
    "937366766": "Mt. Prospect, IL, USA",               # Was: Meadows Park, Mt. Prospect...
    "1008128589": "Boulder, Colorado, USA",             # Was: Day 1... Day 2... (multi-venue)
    "1326725476": "Mérida, Venezuela",                  # Was: Cancha Techada de la FCU... (verbose Spanish)
    "1458170459": "Paris, France",                      # Was: Gymnase Caillaux. Paris 75013...
    # Decision: 2026-02 - Long locations (>100 chars) simplified
    "1072202155": "Oakland, California, USA",           # The Green Cup - multi-venue
    "1102227996": "Oakland, California, USA",           # The Green Cup - multi-venue Oakland/SF
    "1109356644": "Harrisburg, Pennsylvania, USA",      # Funtastik - Morrison Park alias
    "1143172220": "Somerville, New Jersey, USA",        # Jersey Spike & Shred
    "1149881200": "Harrisburg, Pennsylvania, USA",      # Funtastik - multi-day venues
    "1180708622": "Silver Spring, Maryland, USA",       # East Coast Championships
    "1252451527": "Harrisburg, Pennsylvania, USA",      # Funtastik - Morrison Park alias
    "1295120951": "Oakland, California, USA",           # Green Cup - multi-venue
    "1297909685": "Caracas, Venezuela",                 # 2da Copa Ciencias
    "1301675662": "Caracas, Venezuela",                 # 2da Copa Venezuela - UCV
    "1330833781": "Caracas, Venezuela",                 # 3ra Copa Ciencias-UCV
    "1361239920": "San Cristóbal, Táchira, Venezuela",  # FOOTCAMP 2013
    "1362598500": "Montréal, Québec, Canada",           # Akisphere - multi-park
    "1378928859": "San Cristóbal, Táchira, Venezuela",  # 5º Copa Táchira
}

# Event type overrides for events that can't be auto-classified
# Decision: 2026-02 - Manual classification for edge cases
EVENT_TYPE_OVERRIDES = {
    # Real doubles net results but no division header detected
    "1733755410": "net",      # Segunda 'Copa Perpetual Flame' - 8 doubles teams
    # Footbag Golf events (sideline activity)
    "1718928783": "golf",     # 6th Annual Birken Open Footbag Golf Tournament
    "1745203963": "golf",     # 7th Annual FROpen Footbag Golf Tournament
    "967123362": "golf",      # Danish Open Footbag Golf
    # Social events with noise parsed as placements
    "1079664495": "social",   # May Day '04 - "4 Square money games" noise
    "1093115479": "social",   # 2nd Annual SoCal Labor Day Jam - address parsed as place
    "1200725314": "social",   # SoCali Jam 08 - activity description
    # Sideline-only events misclassified as freestyle
    "1250478677": "mixed",    # Montreal End-of-Summer Jam 1 - only 2-Square (sideline) results
    "955642039": "social",    # Zion Footbag Tour - "5 WEEKS OF FUN" noise
    "961029998": "social",    # Club Hackedout T.V Appearance - media event
    # Decision: 2026-02 - Events with unusual formats that can't be auto-classified
    "1044952105": "net",      # L'Hivernal, The Windchill - doubles with "-" separator
    "1269111845": "net",      # King of the Hill 2010 - singles knockout format
    "1347475695": "net",      # Carnabal Footbag Contest - doubles with "/" pairs
}

# ------------------------------------------------------------
# Event-Specific Parsing Rules
# ------------------------------------------------------------
# Per-event rules for handling unusual data formats.
# Each event_id maps to a dict of rule names and their config.
# Available rules:
#   - "split_merged_teams": Split "Player1 [seed] COUNTRY Player2 COUNTRY" format
#   - "division_ranges": List of (start_idx, end_idx, division_name, category) tuples
#
# Decision: 2026-02 - This structure allows adding event-specific parsing
# without polluting the general parsing logic.
EVENT_PARSING_RULES = {
    # 2011 World Championships - doubles results have merged team format
    # Format: "Emmanuel Bouchard [1] CAN Florian Goetze GER"
    "1293877677": {
        "split_merged_teams": True,
    },
    # 2003 East Coast Championships - complex HTML structure with multiple divisions
    # parsed as single block. Map placement indices to divisions.
    "1049457912": {
        "division_ranges": [
            (0, 17, "Intermediate Freestyle", "freestyle"),
            (18, 24, "Open Routines", "freestyle"),
            (25, 26, "Open Sick 3", "freestyle"),
            (27, 31, "Novice Freestyle", "freestyle"),
            (32, 43, "Intermediate Singles Net", "net"),
            (44, 59, "Open Singles Net", "net"),
            (60, 65, "Master's Division Singles Net", "net"),
            (66, 71, "Intermediate Doubles Net", "net"),
            (72, 80, "Open Doubles Net", "net"),
        ],
    },
}

# Valid 3-letter country codes for merged team detection
VALID_COUNTRY_CODES = {
    "ARG", "AUS", "AUT", "BEL", "BRA", "CAN", "CHI", "COL", "CZE", "DEN",
    "ESP", "FIN", "FRA", "GBR", "GER", "HUN", "ITA", "JPN", "MEX", "NED",
    "NOR", "NZL", "PER", "POL", "RUS", "SUI", "SWE", "URU", "USA", "VEN",
}


# ------------------------------------------------------------
# QC Issue tracking
# ------------------------------------------------------------
class QCIssue:
    """Represents a single QC issue."""
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


# Stable UUID namespace for players
NAMESPACE_PLAYERS = uuid.UUID("12345678-1234-5678-1234-567812345678")


def stable_uuid(ns: uuid.UUID, s: str) -> str:
    """Generate stable UUID from namespace and string."""
    return str(uuid.uuid5(ns, s))


# ------------------------------------------------------------
# Division detection and categorization
# ------------------------------------------------------------
# Keywords that DEFINITIVELY indicate a category
# Note: "doubles", "singles", "mixed" are AMBIGUOUS - they exist in both net and freestyle
CATEGORY_KEYWORDS = {
    # NET-specific keywords (if present, division is definitely net)
    "net": {
        "net",           # "Open Singles Net", "Doubles Net"
        "volley",        # "Kick Volley"
        "side-out",      # Net scoring format: "Open Doubles (Side-Out)"
        "side out",      # Variant spacing
        "rallye",        # Net scoring format: "Open Singles (Rallye)"
    },
    # FREESTYLE-specific keywords (if present, division is definitely freestyle)
    "freestyle": {
        "freestyle",     # "Open Freestyle", "Singles Freestyle"
        "routine",       # "Open Routines", "Routine"
        "routines",
        "shred",         # "Shred 30", "Open Shred"
        "circle",        # "Circle Contest", "Open Circle"
        "sick",          # "Sick 3", "Sick3"
        "request",       # "Request Contest"
        "battle",        # "Freestyle Battle"
        "ironman",       # Freestyle endurance event
        "combo",         # "Big Combo", "Huge Combo"
        "trick",         # "Big Trick", "Sick 3-Trick"
        # French keywords
        "homme",         # French men's freestyle
        "femme",         # French women's freestyle
        "feminin",       # French feminine
        # NOTE: "consecutive" is NOT freestyle - it's OTHER (sideline)
    },
    # GOLF keywords
    "golf": {
        "golf",
        "golfer",
        "golfers",
    },
    # SIDELINE/OTHER keywords
    "sideline": {
        "2-square",
        "2 square",      # Without hyphen
        "two square",
        "four square",
        "4-square",
        "4 square",      # Without hyphen
        "consecutive",   # Timed consecutives, one-pass consecutives
        "consec",        # Abbreviation
        "one pass",      # Distance one pass
        "one-pass",      # Hyphenated variant
        "distance",      # Distance events
    },
}

# Keywords for detecting division headers in raw text (used by looks_like_division_header)
# This is a FLAT set - we just need to know if it's a division, not which category
DIVISION_KEYWORDS = {
    # Modifiers (category-neutral)
    "open", "pro", "women", "womens", "men", "mens", "woman", "ladies",
    "intermediate", "advanced", "beginner", "novice", "amateur", "masters",
    # Structure words (category-neutral)
    "double", "doubles", "single", "singles", "mixed",
    # Net-specific
    "net", "volley",
    # Freestyle-specific
    "freestyle", "circle", "shred", "routine", "routines",
    "battle", "sick3", "sick 3", "sick", "request", "last standing",
    "ironman", "combo", "trick",
    # Sideline/other
    "consecutive", "consec", "one pass", "distance",
    # Golf
    "golf",
    # 2-square/4-square
    "2-square", "2 square", "two square", "four square", "4-square",
    # Non-English terms
    "simple",  # French for singles
    "doble",   # Spanish for doubles
    "sencillo", # Spanish for singles
    "homme",   # French for men's
    "femme",   # French for women's
    "feminin", # French for feminine
}

# Common abbreviated division headers and their expansions
ABBREVIATED_DIVISIONS = {
    # Net abbreviations
    "osn": "Open Singles Net",
    "odn": "Open Doubles Net",
    "isn": "Intermediate Singles Net",
    "idn": "Intermediate Doubles Net",
    "wsn": "Women's Singles Net",
    "wdn": "Women's Doubles Net",
    "mdn": "Mixed Doubles Net",
    "msn": "Masters Singles Net",
    # Freestyle abbreviations
    "osf": "Open Singles Freestyle",
    "odf": "Open Doubles Freestyle",
    "osr": "Open Singles Routines",
    "odr": "Open Doubles Routines",
    "wsr": "Women's Singles Routines",
    # Other common abbreviations
    "os": "Open Singles",
    "od": "Open Doubles",
    "is": "Intermediate Singles",
    "id": "Intermediate Doubles",
    "ws": "Women's Singles",
    "wd": "Women's Doubles",
    "md": "Mixed Doubles",
}


def categorize_division(division_name: str, event_type: str = None) -> str:
    """
    Categorize a division name into: net, freestyle, golf, or unknown.

    Priority:
    1. If contains NET keyword (e.g., "net") → "net"
    2. If contains FREESTYLE keyword (e.g., "shred", "routine") → "freestyle"
    3. If contains GOLF keyword → "golf"
    4. If ambiguous but event_type is known → use event_type
    5. Otherwise → "unknown"

    Note: "Singles", "Doubles", "Mixed", "Open", "Intermediate" alone are AMBIGUOUS
    but can be inferred from event_type context.
    """
    if not division_name:
        return "unknown"

    low = division_name.lower()

    # Check for net keywords first (most specific)
    for keyword in CATEGORY_KEYWORDS["net"]:
        if keyword in low:
            return "net"

    # Check for freestyle keywords
    for keyword in CATEGORY_KEYWORDS["freestyle"]:
        if keyword in low:
            return "freestyle"

    # Check for golf keywords
    for keyword in CATEGORY_KEYWORDS["golf"]:
        if keyword in low:
            return "golf"

    # Check for other sideline keywords
    for keyword in CATEGORY_KEYWORDS["sideline"]:
        if keyword in low:
            return "sideline"

    # Ambiguous division name - use event context if available
    # e.g., "Open Singles", "Doubles", "Intermediate" could be net or freestyle
    if event_type:
        event_type_lower = event_type.lower()
        # If event is clearly net or freestyle, use that
        if event_type_lower == "net":
            return "net"
        elif event_type_lower == "freestyle":
            return "freestyle"
        elif event_type_lower == "golf":
            return "golf"
        elif event_type_lower == "worlds":
            # For worlds, we can't infer - divisions must be explicit
            return "unknown"
        elif event_type_lower == "mixed":
            # In footbag, freestyle divisions always self-identify via keywords
            # (routines, shred, circle, sick, battle, etc.).  If we reached here,
            # no freestyle keyword matched, so a named division is net by elimination.
            # Only truly unidentified divisions ("Unknown") stay unknown.
            if division_name and division_name != "Unknown":
                return "net"
        # For "social", stay unknown

    return "unknown"


def looks_like_division_header(line: str) -> bool:
    """
    Check if line looks like a division header.

    Good division headers:
      - "Open Singles Net", "Intermediate Shred", "DOUBLE:", "Sick 3"
      - Abbreviated: "OSN", "ODN", "ODF"
      - Non-English: "Simple:", "Doble:"
      - Short, typically under 50 chars
      - Contains division keywords
      - May end with colon

    NOT division headers (noise):
      - Narrative sentences with "the", "was", "will be", etc.
      - Lines containing scores like "15-5" or "12-11"
      - Lines with result data embedded: "Singles Net: 1. Player Name"
      - Long descriptive text
    """
    low = line.lower().strip()

    # Check for abbreviated divisions first (e.g., "OSN", "ODN:")
    abbrev = low.rstrip(':')
    if abbrev in ABBREVIATED_DIVISIONS:
        return True

    # Length check - real division headers are short
    if len(line) > 50:
        return False

    # Reject empty or very short lines
    if len(line) < 3:
        return False

    # Reject lines that look like results (start with number + separator + name)
    # e.g., "1. John Smith", "2) Jane Doe"
    if re.match(r"^\d+\s*[.):\-]\s+[A-Z]", line):
        return False

    # Reject lines that contain embedded results (colon followed by number)
    # e.g., "Singles Net: 1. The Enforcer Kenny Schultz"
    if re.search(r":\s*\d+[.)]", line):
        return False

    # Reject lines with scores (number-number patterns)
    # e.g., "15-5", "12-11", "21 - 16"
    if re.search(r"\d{2,}\s*[-–]\s*\d{2,}", line):
        return False

    # Reject lines that start with times (schedule noise)
    if re.match(r"^\d{1,2}:\d{2}", line):
        return False

    # Reject lines starting with ordinals followed by a name
    # e.g., "1ST Kenneth Godfrey" - this is a result, not a division
    if re.match(r"^\d+(st|nd|rd|th)\s+[A-Z][a-z]", line, re.IGNORECASE):
        return False

    # Reject admin/instruction text
    if re.match(r"^(important|registration|when:|where:|click|email)", low):
        return False

    # Reject lines containing "place" (result context)
    if "place" in low:
        return False

    # Reject lines starting with "&" or containing contact info
    if line.startswith("&") or "contact" in low:
        return False

    # Reject narrative sentences - these contain common narrative markers
    narrative_patterns = [
        r'\bthe\s+\w+\s+\w+\s+\w+',  # "the summer opening has" (4+ words after "the")
        r'\bwas\b', r'\bwere\b',     # past tense verbs
        r'\bwill\s+be\b',            # future tense
        r'\bgoing\s+to\b',           # "going to celebrate"
        r'\bhere\b.*\bresults?\b',   # "here the results"
        r'\bplayed\b',               # past tense
        r'\bspectators\b',           # audience mention
        r'\bunbeatable\b',           # narrative adjective
        r'\bcelebrate\b',            # narrative verb
    ]
    for pattern in narrative_patterns:
        if re.search(pattern, low):
            return False

    # Reject lines that look like player entries with locations
    # e.g., "Klemens Längauer (AT - 4.)"
    if re.search(r'\([A-Z]{2,3}\s*[-–]\s*\d', line):
        return False

    # Reject comma-separated lists that look like descriptions
    # e.g., "10 golfers, great weather, crazy course!"
    # Valid headers rarely have multiple commas
    if line.count(',') >= 2:
        return False

    # Reject lines with exclamation marks (typically narrative/excitement)
    if '!' in line and not line.rstrip().endswith(':'):
        return False

    # Must contain at least one division keyword
    if not any(k in low for k in DIVISION_KEYWORDS):
        return False

    # Accept if line is reasonably structured:
    # 1. Starts with a division-related word, OR
    # 2. Is a short all-caps header, OR
    # 3. Ends with colon (header style), OR
    # 4. Is short enough (<=35 chars) and contains keyword

    valid_starts = [
        'open', 'pro', 'intermediate', 'int', 'amateur', 'novice', 'beginner',
        'advanced', 'masters', 'women', "women's", 'woman', 'men', "men's",
        'ladies', 'girls', 'junior', 'mixed', 'single', 'double', 'net',
        'freestyle', 'shred', 'sick', 'circle', 'routine', 'golf', 'battle',
        'request', 'consecutive', 'timed', 'big', 'last',
        # Non-English variants
        'simple', 'doble', 'feminin', 'homme', 'dívky', 'dvojice', 'mixte',
        # Numbers followed by keywords (e.g., "30 Sec. Shred", "5 Minute Timed")
    ]

    # Check if starts with valid word (case-insensitive)
    first_word = low.split()[0].rstrip(':,.-') if low.split() else ''
    starts_valid = first_word in valid_starts

    # Numbers at start are OK if followed by division keyword
    # e.g., "30 Sec. Shred", "5 Minute Timed Consecutives"
    if first_word.isdigit():
        rest = ' '.join(low.split()[1:])
        starts_valid = any(k in rest for k in DIVISION_KEYWORDS)

    # Check if it's a short all-caps header (e.g., "DOUBLE:", "SINGLE:")
    is_caps_header = line.isupper() and len(line) <= 30

    # Check if it ends with colon and is short (likely a header)
    is_colon_header = line.rstrip().endswith(':') and len(line) <= 40

    # Short lines with keywords are likely headers
    is_short_with_keyword = len(line) <= 35

    return starts_valid or is_caps_header or is_colon_header or is_short_with_keyword


def smart_title(s: str) -> str:
    """
    Title case that handles apostrophes correctly.
    Fixes: "women's" -> "Women's" (not "Women'S")
    """
    words = s.split()
    result = []
    for word in words:
        titled = word.title()
        # Fix 'S after apostrophe -> 's
        titled = re.sub(r"'S\b", "'s", titled)
        result.append(titled)
    return " ".join(result)


def canonicalize_division(division_raw: str) -> str:
    """
    Produce canonical division name.
    Normalize whitespace and apply smart title casing.
    """
    if not division_raw:
        return "Unknown"
    return smart_title(" ".join(division_raw.split()))


# ------------------------------------------------------------
# Results parsing
# ------------------------------------------------------------
def strip_trailing_score(name: str) -> str:
    """
    Remove trailing numeric scores from player names.
    Examples: "Matt Strong 526" -> "Matt Strong"
              "John Doe 1234" -> "John Doe"
    """
    # Remove trailing 2-4 digit numbers
    cleaned = re.sub(r'\s+\d{2,4}\s*$', '', name).strip()
    # Remove trailing score patterns like "123 -" or "456 ="
    cleaned = re.sub(r'\s+\d+\s*[-=].*$', '', cleaned).strip()
    return cleaned


def split_entry(entry: str) -> tuple[str, Optional[str], str]:
    """
    Detect doubles teams separated by '/', ' and ', ' & ', etc.
    Returns (player1, player2, competitor_type).
    Canonical output format uses '/' separator (handled in _build_name_line).

    Priority:
    1. " & " between names (alternative separator, checked first to handle city notation)
    2. "/" outside parentheses (most common team separator)
    3. " and " between names (word separator, includes "und" German, "plus")
    4. " et " between names (French separator)
    """
    entry = " ".join(entry.split()).strip()

    # Strip common prefixes that shouldn't affect team detection
    # e.g., "tie : Name1 & Name2", "(tie) Name1 / Name2", "3rd place - Name1 / Name2"
    entry_clean = re.sub(
        r'^(\(\s*tie\s*\)[.\-:\s]*|tie\s*[.:\-]?\s*|\d+\s*[.)\-:]?\s*(st|nd|rd|th)?\s*place\s*[-:]?)\s*',
        '', entry, flags=re.IGNORECASE
    ).strip()

    # Strip "d " or "d\t" prefix from ordinal parsing corruption
    entry_clean = re.sub(r'^[dD]\s+', '', entry_clean).strip()

    if not entry_clean:
        entry_clean = entry

    # Helper to strip surrounding quotes from a name
    def strip_quotes(s: str) -> str:
        """Strip surrounding single or double quotes from a string."""
        s = s.strip()
        if len(s) >= 2 and s[0] in ('"', "'") and s[-1] in ('"', "'"):
            return s[1:-1].strip()
        # Also strip just leading quotes (e.g., "Elliott")
        if s.startswith('"') or s.startswith("'"):
            s = s[1:].strip()
        if s.endswith('"') or s.endswith("'"):
            s = s[:-1].strip()
        return s

    # Helper to check if a "/" is inside parentheses
    def slash_outside_parens(s):
        """Find first "/" that is NOT inside parentheses."""
        depth = 0
        for i, c in enumerate(s):
            if c == '(':
                depth += 1
            elif c == ')':
                depth = max(0, depth - 1)
            elif c == '/' and depth == 0:
                return i
        return -1

    # Helper to validate team member name
    def looks_like_name(s: str) -> bool:
        """Check if a string looks like a valid player name."""
        s = strip_quotes(s)
        if len(s) < 2:
            return False
        # Accept if it has at least one uppercase letter (for nicknames like "his Watercarrier")
        return bool(re.search(r'[A-Z]', s))

    # Try " & " first - it's often used when "/" appears in city/country notation
    if " & " in entry_clean:
        a, b = entry_clean.split(" & ", 1)
        a = a.strip()
        b = b.strip()
        if looks_like_name(a) and looks_like_name(b):
            return strip_trailing_score(strip_quotes(a)), strip_trailing_score(strip_quotes(b)), "team"

    # Try "/" outside parentheses
    slash_idx = slash_outside_parens(entry_clean)
    if slash_idx > 0:
        a = entry_clean[:slash_idx].strip()
        b = entry_clean[slash_idx + 1:].strip()
        if len(a) >= 2 and len(b) >= 2:
            return strip_trailing_score(a), strip_trailing_score(b), "team"

    # " and ", "und" (German), or "plus" between two names (case insensitive)
    and_match = re.search(r'\s+(and|und|plus)\s+', entry_clean, re.IGNORECASE)
    if and_match:
        a = entry_clean[:and_match.start()].strip()
        b = entry_clean[and_match.end():].strip()
        if looks_like_name(a) and looks_like_name(b):
            return strip_trailing_score(strip_quotes(a)), strip_trailing_score(strip_quotes(b)), "team"

    # French "et" separator (case insensitive)
    et_match = re.search(r'\s+et\s+', entry_clean, re.IGNORECASE)
    if et_match:
        a = entry_clean[:et_match.start()].strip()
        b = entry_clean[et_match.end():].strip()
        if looks_like_name(a) and looks_like_name(b):
            return strip_trailing_score(strip_quotes(a)), strip_trailing_score(strip_quotes(b)), "team"

    return strip_trailing_score(entry), None, "player"


def split_merged_team(entry: str) -> tuple[str, Optional[str], str]:
    """
    Split merged team entry format: "Player1 [seed] COUNTRY Player2 COUNTRY"

    Examples:
      "Emmanuel Bouchard [1] CAN Florian Goetze GER" -> ("Emmanuel Bouchard", "Florian Goetze", "team")
      "Matti Pohjola [6] FIN Janne Uusitalo FIN" -> ("Matti Pohjola", "Janne Uusitalo", "team")

    Returns (player1, player2, competitor_type) or (entry, None, "player") if no match.
    """
    # Pattern: Name1 [optional seed] COUNTRY Name2 COUNTRY
    # The seed is optional, country codes are 3 uppercase letters
    pattern = re.compile(
        r'^(.+?)\s*'              # Player 1 name (non-greedy)
        r'(?:\[\d+\])?\s*'        # Optional seed in brackets
        r'([A-Z]{3})\s+'          # Country code 1
        r'(.+?)\s+'               # Player 2 name (non-greedy)
        r'([A-Z]{3})$'            # Country code 2
    )

    match = pattern.match(entry.strip())
    if match:
        p1_name, p1_country, p2_name, p2_country = match.groups()
        # Validate both country codes are known
        if p1_country in VALID_COUNTRY_CODES and p2_country in VALID_COUNTRY_CODES:
            return p1_name.strip(), p2_name.strip(), "team"

    # No match - return original as single player
    return entry, None, "player"


def infer_division_from_event_name(event_name: str, placements: list = None, event_type: str = None) -> Optional[str]:
    """
    Infer division from event name, placement patterns, and event type when no division headers are present.

    Examples:
      "Finnish Singles Net Footbag Championships" -> "Open Singles Net"
      "Basque Tournament of Footbag Net (Individual)" -> "Open Singles Net"
      "Colorado Shred Symposium" -> "Open Shred"
      Event with team entries (Name & Name) -> doubles
    """
    name_lower = event_name.lower()
    placements = placements or []
    event_type = (event_type or "").lower()

    # Check for singles/doubles in event name
    has_singles = "singles" in name_lower or "individual" in name_lower or "single" in name_lower
    has_doubles = "doubles" in name_lower or "double" in name_lower

    # If we have placements, check if they look like teams (doubles) or individuals (singles)
    if placements and not has_singles and not has_doubles:
        team_count = sum(1 for p in placements if p.get("competitor_type") == "team")
        player_count = sum(1 for p in placements if p.get("competitor_type") == "player")
        if team_count > player_count:
            has_doubles = True
        elif player_count > team_count:
            has_singles = True

    # Check for net (in name or event_type)
    is_net = "net" in name_lower or event_type == "net"
    if is_net:
        if has_singles and not has_doubles:
            return "Open Singles Net"
        elif has_doubles and not has_singles:
            return "Open Doubles Net"
        # Just "net" without clear singles/doubles
        # Default to singles if all entries are individual players
        if placements and all(p.get("competitor_type") == "player" for p in placements):
            return "Open Singles Net"
        elif placements and all(p.get("competitor_type") == "team" for p in placements):
            return "Open Doubles Net"
        return None

    # Check for freestyle disciplines
    is_freestyle = "freestyle" in name_lower or event_type == "freestyle"
    if "shred" in name_lower:
        return "Open Shred"
    if "routine" in name_lower:
        return "Open Routines"
    if "circle" in name_lower:
        return "Open Circle"
    if is_freestyle:
        if has_singles:
            return "Open Singles Freestyle"
        elif has_doubles:
            return "Open Doubles Freestyle"
        return "Open Freestyle"

    # Check for known tournament name patterns
    if "king of the hill" in name_lower:
        return "Open Singles Net"  # Always singles knockout format
    if "bembel cup" in name_lower:
        return "Open Doubles Net"  # Always doubles tournament

    # For mixed events with placements but no clear keywords, infer from competitor type
    # This handles events like "IFPA Turku Open", "Bedford Championships", etc.
    if event_type == "mixed" and placements:
        # Check if all placements are teams or all are players
        team_count = sum(1 for p in placements if p.get("competitor_type") == "team")
        player_count = sum(1 for p in placements if p.get("competitor_type") == "player")

        # If predominantly one type, infer division
        if team_count > 0 and player_count == 0:
            # All teams - likely doubles net (default for mixed events)
            return "Open Doubles Net"
        elif player_count > 0 and team_count == 0:
            # All players - likely singles net (default for mixed events)
            return "Open Singles Net"

    return None


def parse_results_text(results_text: str, event_id: str, event_type: str = None) -> list[dict]:
    """
    Parse results text into structured placements.
    Returns list of placement dicts with confidence scoring.

    Args:
        results_text: Raw results text to parse
        event_id: Event identifier
        event_type: Event type for context (net, freestyle, etc.) - used to disambiguate divisions
    """
    placements = []
    division_raw = "Unknown"

    # Get event-specific parsing rules
    event_rules = EVENT_PARSING_RULES.get(str(event_id), {})
    use_merged_team_split = event_rules.get("split_merged_teams", False)

    # Track whether we're in a seeding section (should skip these entries)
    in_seeding_section = False

    place_re = re.compile(r"^\s*(\d{1,3})\s*[.)\-:]?\s*(.+)$")
    # Pattern for ordinal placements like "1ST Name", "2ND Name", "3RD Name", "4TH Name"
    ordinal_re = re.compile(r"^\s*(\d{1,2})(ST|ND|RD|TH)\s+(.+)$", re.IGNORECASE)
    # Pattern for tied placements like "23/24 Name" - captures the tie suffix
    tied_place_re = re.compile(r"^/\d+\s+(.+)$")

    for raw_line in (results_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Detect seeding vs results sections (skip seeding data)
        line_lower = line.lower()

        # Check for division headers that include "- Initial Seeding" or "- Final Results"
        # e.g., "Open Routines - Initial Seeding", "Open Battles - Complete Results"
        if " - " in line and looks_like_division_header(line.split(" - ")[0].strip()):
            suffix = line.split(" - ", 1)[1].lower() if " - " in line else ""
            if "seeding" in suffix:
                in_seeding_section = True
                division_raw = line.split(" - ")[0].strip()  # Use just the division name
                continue
            elif "result" in suffix or "final" in suffix or "complete" in suffix:
                in_seeding_section = False
                division_raw = line.split(" - ")[0].strip()
                continue

        # Standalone seeding section markers
        if line_lower == "initial seeding" or line_lower == "seeding":
            in_seeding_section = True
            continue

        # "Results" or "Results Pool X" or "Final Results" etc. indicate actual results
        if (line_lower.startswith("results") or
            line_lower == "final standings" or
            line_lower.startswith("final results") or
            "final" in line_lower and "standing" in line_lower):
            in_seeding_section = False
            # Don't continue - this might be a division header like "Results Pool A"
            if looks_like_division_header(line):
                division_raw = line.rstrip(":")
                continue

        # Skip entries in seeding sections (these are pre-tournament rankings, not results)
        if in_seeding_section:
            continue

        # Check for bold-style division headers (common in manually entered results)
        # e.g., "**Intermediate Singles**" or text that was in <b> tags
        if looks_like_division_header(line):
            div_text = line.rstrip(":")
            # Expand abbreviated divisions (e.g., "OSN" -> "Open Singles Net")
            abbrev = div_text.lower()
            if abbrev in ABBREVIATED_DIVISIONS:
                division_raw = ABBREVIATED_DIVISIONS[abbrev]
            else:
                division_raw = div_text
            # Reset seeding flag when we hit a new division
            in_seeding_section = False
            continue

        # Try ordinal format first (1ST, 2ND, 3RD, 4TH, etc.)
        ordinal_match = ordinal_re.match(line)
        if ordinal_match:
            place = int(ordinal_match.group(1))
            entry_raw = ordinal_match.group(3).strip()
        else:
            m = place_re.match(line)
            if not m:
                continue
            place = int(m.group(1))
            entry_raw = m.group(2).strip()

            # Strip ordinal suffix if entry starts with it (from "1ST" parsed as "1" + "ST Name")
            # Handle both "ST Name" (space) and "st. Name" (dot) formats
            # Also handle Spanish ordinals: 1er, 2do, 3er, 4to, 5to
            entry_raw = re.sub(r'^(ST|ND|RD|TH|ER|DO|TO|TA)[.\s]+', '', entry_raw, flags=re.IGNORECASE)

        # Strip "place"/"puesto"/"lugar" prefix (from "1st place - Name", "1er PUESTO Name", "1er LUGAR")
        entry_raw = re.sub(r'^(place|puesto|lugar)\s*[-:]?\s*', '', entry_raw, flags=re.IGNORECASE).strip()

        # Strip bare dash prefix (from "1st - Name" parsed as "- Name")
        entry_raw = re.sub(r'^-\s+', '', entry_raw).strip()

        # Handle tied placements like "23/24 Name" -> entry starts with "/24 Name"
        # Convert to just "Name" and keep place as 23 (the first/lower number)
        # Must happen before noise filters so "1/2 Finals..." resolves to "Finals..."
        tied_match = tied_place_re.match(entry_raw)
        if tied_match:
            entry_raw = tied_match.group(1).strip()

        # Skip lines that look like years (e.g., "2007 US Open..." parsed as place=200)
        if place >= 100:
            continue  # No event has 100+ placements in a single division

        # Skip schedule/time noise: lines like "9:30 Open Doubles Meeting"
        # get parsed as place=9, entry="30 Open Doubles..."
        # Also skip entries that are clearly times or admin text
        # Pattern 1: entry starts with "00 am/pm" (from "6:00 pm" parsed as place=6)
        if re.match(r'^\d{1,2}\s*(am|pm|a\.m|p\.m)', entry_raw, re.IGNORECASE):
            continue  # Skip - this is a time, not a placement
        # Pattern 2: entry starts with ":30" (from "9:30" parsed as place=9)
        if re.match(r'^:\d{2}', entry_raw):
            continue  # Skip - this is the minutes part of a time
        # Pattern 3: entry IS a time like "6:30pm" or "12:00 noon"
        if re.match(r'^\d{1,2}:\d{2}\s*(am|pm|noon)?', entry_raw, re.IGNORECASE):
            continue  # Skip - this is a time
        # Pattern 4: entry starts with "End of" or similar admin phrases
        if re.match(r'^(end of|registration|reservations)', entry_raw, re.IGNORECASE):
            continue  # Skip - this is admin text
        # Pattern 5: entry starts with "00 " + admin word (from "10:00 End of..." parsed as place=10)
        if re.match(r'^00\s+(end|registration|check)', entry_raw, re.IGNORECASE):
            continue  # Skip - minutes part of time + admin text
        # Pattern 6: entry contains phone number patterns
        if re.search(r'\d{3}[-.]\d{3}[-.]\d{4}|\d{3}[-.]\d{4}|1-800-', entry_raw):
            continue  # Skip - contains phone number
        # Pattern 7: entry is a rule/instruction sentence (contains "is allowed", "contact", "reservations")
        if re.search(r'\b(is allowed|contact is|by phone|make reservations)\b', entry_raw, re.IGNORECASE):
            continue  # Skip - rule or instruction text
        # Pattern 8: entry starts with degree sign (Spanish ordinal remnant, e.g., "º and 4º position match")
        if entry_raw.startswith('º'):
            continue  # Skip - degree-sign ordinal noise
        # Pattern 9: narrative/commentary text (section headers or match descriptions)
        if re.match(r'^(Finals|Finas|points|position)', entry_raw, re.IGNORECASE):
            continue  # Skip - narrative text, not a placement
        # Pattern 10: hotel/hostel names (French and English)
        if re.search(r'\b(hostel|auberge|hotel|hôtel|gîte|manoir)\b', entry_raw, re.IGNORECASE):
            continue  # Skip - accommodation information
        # Pattern 11: schedule/meeting keywords
        if re.search(r'\b(registration|check-in|check in|meet at)\b', entry_raw, re.IGNORECASE):
            continue  # Skip - schedule information
        # Pattern 12: narrative text about tournament format/pools
        if re.search(r'\b(competed|players competed|games played|pools)\b', entry_raw, re.IGNORECASE):
            # Only skip if it's clearly narrative (contains "and" or long text)
            if ' and ' in entry_raw.lower() or len(entry_raw) > 50:
                continue  # Skip - narrative text

        # Apply event-specific parsing rules
        if use_merged_team_split:
            player1, player2, competitor_type = split_merged_team(entry_raw)
        else:
            player1, player2, competitor_type = split_entry(entry_raw)

        # Skip placements with invalid player names (noise)
        # A valid player name should have at least 2 alphanumeric characters
        if not player1 or len(player1) < 2 or not re.search(r"[a-zA-Z]{2,}", player1):
            continue  # Skip this as parsing noise

        # Confidence scoring
        confidence = "high"
        notes = []

        if division_raw == "Unknown":
            confidence = "medium"
            notes.append("no division header found")

        if not player1:
            confidence = "low"
            notes.append("empty player name")

        # Check for suspicious patterns in entry
        if re.search(r"[<>{}|\\]", entry_raw):
            confidence = "low"
            notes.append("suspicious characters in entry")

        division_canon = canonicalize_division(division_raw)
        division_category = categorize_division(division_canon, event_type)

        placements.append({
            "division_raw": division_raw,
            "division_canon": division_canon,
            "division_category": division_category,  # net, freestyle, golf, or unknown
            "place": place,
            "competitor_type": competitor_type,
            "player1_name": player1,
            "player2_name": player2,
            "entry_raw": entry_raw,
            "parse_confidence": confidence,
            "notes": "; ".join(notes) if notes else "",
        })

    return placements


# ------------------------------------------------------------
# CSV processing
# ------------------------------------------------------------
def read_stage1_csv(csv_path: Path) -> list[dict]:
    """Read stage1 CSV and return list of event records."""
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
            records.append(row)
    return records


def clean_date(date_raw: str) -> str:
    """Clean date field by removing iCal remnant text."""
    if not date_raw:
        return ""
    # Remove iCal UI text suffix
    cleaned = re.sub(r"\s*add this event to iCal.*$", "", date_raw, flags=re.IGNORECASE)
    return cleaned.strip()


def infer_event_type(event_name: str, results_raw: str, placements: list = None) -> str:
    """
    Infer event_type from event name and placement division categories.

    Priority:
    1. "World Footbag Championships" in name → "worlds"
    2. If placements exist, use their division_category counts:
       - Only net divisions → "net"
       - Only freestyle divisions → "freestyle"
       - Both net and freestyle → "mixed"
       - Only golf → "golf"
       - Only unknown/ambiguous → fall back to text analysis
    3. Fall back to text analysis if no placements or all ambiguous

    Returns: worlds, net, freestyle, mixed, golf, or social
    """
    placements = placements or []
    name_lower = (event_name or "").lower()
    results_lower = (results_raw or "").lower()

    # Check for World Footbag Championships first (strict match)
    if "world footbag championship" in name_lower:
        return "worlds"

    # If we have placements, use their division categories
    if placements:
        categories = set()
        for p in placements:
            cat = p.get("division_category", "unknown")
            if cat and cat != "unknown":
                categories.add(cat)

        # Determine event type from categories present
        has_net = "net" in categories
        has_freestyle = "freestyle" in categories
        has_golf = "golf" in categories
        has_sideline = "sideline" in categories

        # If only golf, it's a golf event
        if has_golf and not has_net and not has_freestyle:
            return "golf"

        # If both net and freestyle, it's mixed
        if has_net and has_freestyle:
            return "mixed"

        # If only net
        if has_net:
            return "net"

        # If only freestyle
        if has_freestyle:
            return "freestyle"

        # If we have placements but all are "unknown" category,
        # fall through to text analysis below

    # --- Text-based fallback (when no placements or all unknown) ---
    combined = name_lower + " " + results_lower

    # Check for golf in text
    if re.search(r'\bgolf\b|\bgolfers?\b', combined):
        return "golf"

    # Check for sideline events (4-square, 2-square)
    if re.search(r'\b(4-square|four.?square|2-square|two.?square)\b', name_lower):
        return "social"

    # Keywords that definitively indicate category
    net_keywords = ["net", "footbag net", "kick volley"]
    freestyle_keywords = ["routine", "shred", "circle", "freestyle", "sick", "request", "consecutive"]

    has_net = any(kw in combined for kw in net_keywords)
    has_freestyle = any(kw in combined for kw in freestyle_keywords)

    # "Jam" in event name indicates freestyle gathering
    if re.search(r'\bjam\b', name_lower):
        has_freestyle = True

    # Net scoring patterns (rally scores like "21-16, 21-11")
    if re.search(r'\b\d{1,2}-\d{1,2},?\s*\d{1,2}-\d{1,2}\b', results_lower):
        has_net = True

    if has_net and has_freestyle:
        return "mixed"
    elif has_net:
        return "net"
    elif has_freestyle:
        return "freestyle"

    # Events with "open", "tournament", "championship", or "cup" in name
    # that have placements are likely mixed competitions
    if placements:
        if re.search(r'\b(open|tournament|championship|cup)\b', name_lower):
            return "mixed"

    # No competition indicators found
    if not placements:
        return "social"

    return "mixed"  # Has placements but couldn't classify - assume mixed


def canonicalize_records(records: list[dict]) -> list[dict]:
    """
    Process stage1 records into canonical format with placements.
    """
    canonical = []

    for rec in records:
        event_id = rec.get("event_id", "")

        # Get basic event info
        results_raw = rec.get("results_block_raw", "")
        event_name = rec.get("event_name_raw", "")

        # Try to get event_type hint before parsing (from raw field or event name)
        event_type_hint = rec.get("event_type_raw", "")
        if not event_type_hint:
            # Quick check of event name for obvious net/freestyle/golf keywords
            name_lower = (event_name or "").lower()
            if "world footbag championship" in name_lower:
                event_type_hint = "worlds"
            elif " net" in name_lower or "footbag net" in name_lower:
                event_type_hint = "net"
            elif "freestyle" in name_lower or "shred" in name_lower or "routine" in name_lower:
                event_type_hint = "freestyle"
            elif "golf" in name_lower:
                event_type_hint = "golf"

        # Parse placements WITH event_type context for better division categorization
        placements = parse_results_text(results_raw, event_id, event_type_hint)

        # Infer final event_type (now that we have placements)
        event_type_for_div = event_type_hint or infer_event_type(event_name, results_raw, placements)

        # Re-categorize divisions if event_type changed after inference
        if event_type_hint != event_type_for_div and event_type_for_div:
            for p in placements:
                # Re-categorize using the final event_type
                p["division_category"] = categorize_division(p["division_canon"], event_type_for_div)

        # Apply event-specific division range mapping if configured
        event_rules = EVENT_PARSING_RULES.get(str(event_id), {})
        division_ranges = event_rules.get("division_ranges")
        if division_ranges:
            for idx, p in enumerate(placements):
                for start_idx, end_idx, div_name, div_cat in division_ranges:
                    if start_idx <= idx <= end_idx:
                        p["division_raw"] = f"[Event-specific mapping]"
                        p["division_canon"] = canonicalize_division(div_name)
                        p["division_category"] = div_cat
                        if p["parse_confidence"] == "high":
                            # Keep high confidence for entry parsing
                            pass
                        else:
                            p["parse_confidence"] = "medium"
                        if p["notes"]:
                            p["notes"] += f"; division mapped via event-specific rule to {div_name}"
                        else:
                            p["notes"] = f"division mapped via event-specific rule to {div_name}"
                        break

        # If all placements have Unknown division, try to infer from event name, placements, and event type
        if placements and all(p.get("division_canon") == "Unknown" for p in placements):
            inferred_div = infer_division_from_event_name(event_name, placements, event_type_for_div)
            if inferred_div:
                for p in placements:
                    p["division_raw"] = f"[Inferred from event name: {event_name[:30]}]"
                    p["division_canon"] = inferred_div
                    p["division_category"] = categorize_division(inferred_div, event_type_for_div)
                    if p["parse_confidence"] == "medium":
                        # Keep medium if it was already medium for other reasons
                        pass
                    else:
                        p["parse_confidence"] = "medium"
                    if p["notes"]:
                        p["notes"] += "; division inferred from event name"
                    else:
                        p["notes"] = "division inferred from event name"

        # Handle known broken source events
        location = rec.get("location_raw", "")
        date = clean_date(rec.get("date_raw", ""))
        if str(event_id) in KNOWN_BROKEN_SOURCE_EVENTS:
            if not location:
                location = BROKEN_SOURCE_MESSAGE
            if not date:
                date = BROKEN_SOURCE_MESSAGE

        # Apply location override if available
        if str(event_id) in LOCATION_OVERRIDES:
            location = LOCATION_OVERRIDES[str(event_id)]

        # Get event name and apply override if available
        event_name = rec.get("event_name_raw", "")
        if str(event_id) in EVENT_NAME_OVERRIDES:
            event_name = EVENT_NAME_OVERRIDES[str(event_id)]

        # Infer event_type from name and placement categories
        event_type = rec.get("event_type_raw", "")
        if not event_type:
            event_type = infer_event_type(event_name, results_raw, placements)

        # Apply event_type override if available
        if str(event_id) in EVENT_TYPE_OVERRIDES:
            event_type = EVENT_TYPE_OVERRIDES[str(event_id)]

        # Apply year override if available (for broken source events)
        year = rec.get("year")
        if str(event_id) in YEAR_OVERRIDES:
            year = YEAR_OVERRIDES[str(event_id)]

        canonical.append({
            "event_id": event_id,
            "year": year,
            "event_name": event_name,
            "date": date,
            "location": location,
            "host_club": rec.get("host_club_raw", ""),
            "event_type": event_type,
            "results_raw": results_raw,
            "placements_json": json.dumps(placements, ensure_ascii=False),
        })

    return canonical


def deduplicate_events(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Remove duplicate events based on (year, event_name, location).

    When duplicates are found, keep the "better" record:
    1. Prefer actual dates over TBA/placeholder dates
    2. Prefer records with more placements
    3. If still tied, keep the lower event_id (first entered)

    Returns: (deduplicated_records, removed_duplicates)
    """
    from collections import defaultdict

    # Group by (year, event_name, location)
    groups = defaultdict(list)
    for rec in records:
        key = (rec.get("year", ""), rec.get("event_name", ""), rec.get("location", ""))
        groups[key].append(rec)

    deduplicated = []
    removed = []

    for key, group in groups.items():
        if len(group) == 1:
            deduplicated.append(group[0])
        else:
            # Sort to pick the best record
            def score(rec):
                date = rec.get("date", "").lower()
                placements = json.loads(rec.get("placements_json", "[]"))

                # Higher score = better record
                date_score = 0 if "tba" in date or date == "" else 1
                placement_score = len(placements)
                # Lower event_id = tiebreaker (negative so lower is better)
                id_score = -int(rec.get("event_id", "0") or "0")

                return (date_score, placement_score, id_score)

            group.sort(key=score, reverse=True)
            deduplicated.append(group[0])  # Keep best
            removed.extend(group[1:])      # Remove rest

    # Sort output by event_id for stable ordering
    deduplicated.sort(key=lambda r: r.get("event_id", ""))

    return deduplicated, removed


def write_stage2_csv(records: list[dict], out_path: Path) -> None:
    """Write canonical records to stage2 CSV file."""
    if not records:
        print("No records to write!")
        return

    fieldnames = [
        "event_id",
        "year",
        "event_name",
        "date",
        "location",
        "host_club",
        "event_type",
        "results_raw",
        "placements_json",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


# ------------------------------------------------------------
# QC Field-Level Checks
# ------------------------------------------------------------
def check_event_id(rec: dict) -> list[QCIssue]:
    """Check event_id field: required, non-empty, pattern."""
    issues = []
    event_id = rec.get("event_id", "")

    if not event_id:
        issues.append(QCIssue(
            check_id="event_id_missing",
            severity="ERROR",
            event_id=event_id,
            field="event_id",
            message="event_id is missing or empty",
        ))
    elif not re.match(r"^\d+$", str(event_id)):
        issues.append(QCIssue(
            check_id="event_id_pattern",
            severity="WARN",
            event_id=str(event_id),
            field="event_id",
            message="event_id should be digits only",
            example_value=str(event_id)[:50],
        ))
    return issues


def check_event_name(rec: dict) -> list[QCIssue]:
    """Check event_name field: required, non-empty, no HTML/URLs."""
    issues = []
    event_id = rec.get("event_id", "")
    event_name = rec.get("event_name", "")

    if not event_name or not event_name.strip():
        issues.append(QCIssue(
            check_id="event_name_missing",
            severity="ERROR",
            event_id=str(event_id),
            field="event_name",
            message="event_name is missing or empty",
        ))
    else:
        # Check for HTML remnants
        if re.search(r"<[^>]+>|&[a-z]+;|&amp;", event_name, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="event_name_html",
                severity="WARN",
                event_id=str(event_id),
                field="event_name",
                message="event_name contains HTML remnants",
                example_value=event_name[:100],
            ))
        # Check for URLs
        if re.search(r"https?://|www\.", event_name, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="event_name_url",
                severity="WARN",
                event_id=str(event_id),
                field="event_name",
                message="event_name contains URL",
                example_value=event_name[:100],
            ))
        # Check for placeholder/template names
        if "event listing" in event_name.lower():
            issues.append(QCIssue(
                check_id="event_name_placeholder",
                severity="WARN",
                event_id=str(event_id),
                field="event_name",
                message="event_name appears to be a placeholder/template",
                example_value=event_name[:100],
            ))
    return issues


def check_event_type(rec: dict) -> list[QCIssue]:
    """Check event_type: must be in valid set or empty."""
    issues = []
    event_id = rec.get("event_id", "")
    event_type = rec.get("event_type", "")

    if event_type and event_type.lower() not in VALID_EVENT_TYPES:
        issues.append(QCIssue(
            check_id="event_type_invalid",
            severity="ERROR",
            event_id=str(event_id),
            field="event_type",
            message=f"event_type must be in {VALID_EVENT_TYPES}",
            example_value=event_type[:50],
        ))
    return issues


def check_location(rec: dict) -> list[QCIssue]:
    """Check location: required, no URLs/emails, not multi-sentence."""
    issues = []
    event_id = rec.get("event_id", "")
    location = rec.get("location", "")

    # Check for broken source or missing location
    is_known_broken = str(event_id) in KNOWN_BROKEN_SOURCE_EVENTS
    is_broken_or_unknown = (
        not location or
        not location.strip() or
        location == BROKEN_SOURCE_MESSAGE or
        location == "Unknown"
    )

    if is_broken_or_unknown:
        issues.append(QCIssue(
            check_id="location_broken_source" if is_known_broken else "location_missing",
            severity="WARN" if is_known_broken else "ERROR",
            event_id=str(event_id),
            field="location",
            message="known broken source (SQL error in HTML)" if is_known_broken else "location is missing or empty",
        ))
        return issues  # Skip other checks for broken/missing
    else:
        # Check for URLs
        if re.search(r"https?://|www\.", location, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="location_url",
                severity="WARN",
                event_id=str(event_id),
                field="location",
                message="location contains URL",
                example_value=location[:100],
            ))
        # Check for email
        if re.search(r"\S+@\S+\.\S+", location):
            issues.append(QCIssue(
                check_id="location_email",
                severity="WARN",
                event_id=str(event_id),
                field="location",
                message="location contains email address",
                example_value=location[:100],
            ))
        # Check for "Hosted by"
        if re.search(r"hosted\s+by", location, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="location_hosted_by",
                severity="WARN",
                event_id=str(event_id),
                field="location",
                message="location contains 'Hosted by' (should be in host_club)",
                example_value=location[:100],
            ))
        # Multi-sentence detection (multiple periods followed by capital)
        sentences = re.split(r"\.\s+[A-Z]", location)
        if len(sentences) > 2:
            issues.append(QCIssue(
                check_id="location_multi_sentence",
                severity="WARN",
                event_id=str(event_id),
                field="location",
                message="location appears to contain multiple sentences",
                example_value=location[:100],
            ))
        # Check for overly long locations (>100 chars)
        if len(location) > 100:
            issues.append(QCIssue(
                check_id="location_too_long",
                severity="WARN",
                event_id=str(event_id),
                field="location",
                message=f"location too long ({len(location)} chars), should be simplified",
                example_value=location[:100] + "...",
            ))
    return issues


def check_date(rec: dict) -> list[QCIssue]:
    """Check date field: parseable, required if worlds."""
    issues = []
    event_id = rec.get("event_id", "")
    date_str = rec.get("date", "")
    event_type = rec.get("event_type", "")

    # Required if worlds
    if event_type and event_type.lower() == "worlds":
        if not date_str or not date_str.strip():
            issues.append(QCIssue(
                check_id="date_missing_worlds",
                severity="ERROR",
                event_id=str(event_id),
                field="date",
                message="date is required for worlds events",
            ))

    if date_str and date_str.strip():
        # Check for iCal remnants
        if "ical" in date_str.lower():
            issues.append(QCIssue(
                check_id="date_ical_remnant",
                severity="WARN",
                event_id=str(event_id),
                field="date",
                message="date contains iCal remnants",
                example_value=date_str[:100],
            ))
        # Try to parse year from date for consistency check
        year_match = re.search(r"\b(19|20)\d{2}\b", date_str)
        if year_match:
            date_year = int(year_match.group(0))
            rec_year = rec.get("year")
            if rec_year and date_year != rec_year:
                issues.append(QCIssue(
                    check_id="date_year_mismatch",
                    severity="WARN",
                    event_id=str(event_id),
                    field="date",
                    message=f"date year ({date_year}) doesn't match record year ({rec_year})",
                    example_value=date_str[:50],
                    context={"date_year": date_year, "record_year": rec_year},
                ))
    return issues


def check_year(rec: dict) -> list[QCIssue]:
    """Check year field: plausible range, required if worlds."""
    issues = []
    event_id = rec.get("event_id", "")
    year = rec.get("year")
    event_type = rec.get("event_type", "")

    # Required if worlds
    if event_type and event_type.lower() == "worlds":
        if year is None:
            issues.append(QCIssue(
                check_id="year_missing_worlds",
                severity="ERROR",
                event_id=str(event_id),
                field="year",
                message="year is required for worlds events",
            ))

    if year is not None:
        if not (YEAR_MIN <= year <= YEAR_MAX):
            issues.append(QCIssue(
                check_id="year_out_of_range",
                severity="WARN",
                event_id=str(event_id),
                field="year",
                message=f"year {year} outside plausible range ({YEAR_MIN}-{YEAR_MAX})",
                example_value=str(year),
            ))
    return issues


def check_host_club(rec: dict) -> list[QCIssue]:
    """Check host_club: coverage tracking, club-like validation."""
    issues = []
    # We track coverage but don't error on missing - it's optional
    # Just warn on suspicious patterns
    event_id = rec.get("event_id", "")
    host_club = rec.get("host_club", "")

    if host_club and host_club.strip():
        # Check for URLs
        if re.search(r"https?://|www\.", host_club, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="host_club_url",
                severity="WARN",
                event_id=str(event_id),
                field="host_club",
                message="host_club contains URL",
                example_value=host_club[:100],
            ))
    return issues


def check_placements_json(rec: dict) -> list[QCIssue]:
    """Check placements_json: valid JSON, schema validation."""
    issues = []
    event_id = rec.get("event_id", "")
    placements_str = rec.get("placements_json", "[]")

    try:
        placements = json.loads(placements_str)
    except json.JSONDecodeError as e:
        issues.append(QCIssue(
            check_id="placements_json_invalid",
            severity="ERROR",
            event_id=str(event_id),
            field="placements_json",
            message=f"Invalid JSON: {str(e)[:50]}",
            example_value=placements_str[:100],
        ))
        return issues

    # Schema checks on each placement
    for i, p in enumerate(placements):
        place = p.get("place")
        if place is None or place <= 0:
            issues.append(QCIssue(
                check_id="placements_place_invalid",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Placement {i}: place must be > 0",
                example_value=str(place),
                context={"placement_index": i},
            ))

        competitor_type = p.get("competitor_type", "")
        if competitor_type and competitor_type not in {"player", "team"}:
            issues.append(QCIssue(
                check_id="placements_competitor_type_invalid",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Placement {i}: competitor_type must be 'player' or 'team'",
                example_value=competitor_type,
                context={"placement_index": i},
            ))

        player1 = p.get("player1_name", "")
        if not player1 or not player1.strip():
            issues.append(QCIssue(
                check_id="placements_name_empty",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Placement {i}: player1_name is empty",
                context={"placement_index": i},
            ))
        elif len(player1) < 2:
            issues.append(QCIssue(
                check_id="placements_name_short",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Placement {i}: player1_name too short",
                example_value=player1,
                context={"placement_index": i},
            ))

        # Check for noise in player names (phone numbers, schedules, instructions)
        if player1:
            if re.search(r"\d{3}[-.]\d{3}[-.]\d{4}", player1):
                issues.append(QCIssue(
                    check_id="placements_name_noise",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Placement {i}: player name contains phone number",
                    example_value=player1[:60],
                    context={"placement_index": i, "noise_type": "phone"},
                ))
            elif re.search(r"\d{1,2}:\d{2}\s*(am|pm)", player1, re.IGNORECASE):
                issues.append(QCIssue(
                    check_id="placements_name_noise",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Placement {i}: player name contains schedule time",
                    example_value=player1[:60],
                    context={"placement_index": i, "noise_type": "schedule"},
                ))
            # Match admin text but NOT freestyle scoring (e.g., "31 contacts" is valid)
            elif re.search(r"registration|reservations|contact\s+(us|is|me|info)|please\s+contact", player1, re.IGNORECASE):
                issues.append(QCIssue(
                    check_id="placements_name_noise",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Placement {i}: player name contains admin text",
                    example_value=player1[:60],
                    context={"placement_index": i, "noise_type": "admin"},
                ))
            # Check for merged team entries (Player1 [seed] COUNTRY Player2 COUNTRY)
            if re.search(r"\[\d+\]\s+[A-Z]{3}\s+\w+", player1):
                issues.append(QCIssue(
                    check_id="placements_merged_team",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Placement {i}: team entry not properly split (tab-delimited format?)",
                    example_value=player1[:60],
                    context={"placement_index": i},
                ))
            # Check for unsplit team entries (contains " and " or " & " that should have been split)
            # This is a canonical format violation - teams should be split into player1/player2
            # Only flag if it looks like "Name1 & Name2" pattern (both parts start with capital)
            unsplit_match = re.search(r'\s+&\s+', player1)
            if unsplit_match:
                a = player1[:unsplit_match.start()].strip()
                b = player1[unsplit_match.end():].strip()
                # Both parts should look like names (start with capital, no special prefixes)
                a_clean = re.sub(r'^(tie\s*:|\(\s*tie\s*\)|\d+\s*[.)\-:]?\s*place\s*[-:]?)\s*', '', a, flags=re.IGNORECASE).strip()
                if (len(a_clean) >= 2 and len(b) >= 2 and
                    a_clean[0].isupper() and b[0].isupper() and
                    not re.search(r'\$|prize|place|pool|seed', player1, re.IGNORECASE)):
                    issues.append(QCIssue(
                        check_id="placements_unsplit_team",
                        severity="WARN",
                        event_id=str(event_id),
                        field="placements_json",
                        message=f"Placement {i}: team entry may not be properly split (contains '&')",
                        example_value=player1[:60],
                        context={"placement_index": i},
                    ))

        # Check for noise in division names
        div_canon = p.get("division_canon", "")
        if div_canon:
            if re.search(r"\d{1,2}:\d{2}", div_canon):
                issues.append(QCIssue(
                    check_id="placements_division_noise",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Placement {i}: division contains schedule time",
                    example_value=div_canon[:60],
                    context={"placement_index": i},
                ))
            elif re.search(r"registration|contact|email|click", div_canon, re.IGNORECASE):
                issues.append(QCIssue(
                    check_id="placements_division_noise",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Placement {i}: division contains instructions/links",
                    example_value=div_canon[:60],
                    context={"placement_index": i},
                ))

        # Check for unknown division category when division_raw has keywords
        div_category = p.get("division_category", "")
        div_raw = p.get("division_raw", "")
        if div_category == "unknown" and div_raw:
            div_raw_lower = div_raw.lower()
            # Check if division_raw contains any known keywords
            found_keywords = []
            for kw in ["singles", "doubles", "net", "shred", "freestyle", "routine",
                      "homme", "femme", "feminin", "simple", "doble", "circle"]:
                if kw in div_raw_lower:
                    found_keywords.append(kw)
            if found_keywords:
                issues.append(QCIssue(
                    check_id="placements_unknown_with_keywords",
                    severity="WARN",
                    event_id=str(event_id),
                    field="division_category",
                    message=f"Placement {i}: division '{div_raw}' has keywords {found_keywords} but category=unknown",
                    example_value=div_raw[:60],
                    context={"placement_index": i, "keywords_found": found_keywords},
                ))

    return issues


def check_results_extraction(rec: dict) -> list[QCIssue]:
    """Warn if results_raw has content but no placements extracted."""
    issues = []
    event_id = rec.get("event_id", "")
    results_raw = rec.get("results_raw", "") or ""
    placements = json.loads(rec.get("placements_json", "[]"))

    # Check if results_raw looks like it has results data
    if len(results_raw) > 100:  # Non-trivial content
        # Look for strict placement patterns: "1. Name", "1) Name", "1: Name", "1 - Name"
        # Require explicit separator to avoid matching event format descriptions
        has_placements_pattern = bool(re.search(
            r'^\s*[1-9]\d?\s*[.):\-]\s+[A-Z][a-z]+(?:\s+[A-Z])?',
            results_raw,
            re.MULTILINE
        ))
        # Exclude false positives: event format descriptions
        # These contain patterns like "2 minute Routine", "Shred 30", "Sick 3"
        is_event_format = bool(re.search(
            r'\b\d+\s+minute|\bminute\s+routine|Open:\s+\d|\bShred\s+\d|Sick\s+\d',
            results_raw,
            re.IGNORECASE
        ))
        if has_placements_pattern and not placements and not is_event_format:
            issues.append(QCIssue(
                check_id="results_not_extracted",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message="Results raw has placement patterns but no placements extracted",
                example_value=results_raw[:200],
                context={"results_raw_length": len(results_raw)}
            ))
    return issues


# ------------------------------------------------------------
# QC Cross-Validation Checks (Stage 2 Specific)
# ------------------------------------------------------------
def check_expected_divisions(rec: dict) -> list[QCIssue]:
    """Check if event has expected divisions based on event type."""
    issues = []
    event_id = rec.get("event_id", "")
    event_type = (rec.get("event_type") or "").lower()
    placements = json.loads(rec.get("placements_json", "[]"))

    if not placements or event_type not in EXPECTED_DIVISIONS:
        return issues

    # Get division categories present in placements
    categories_present = set()
    for p in placements:
        cat = p.get("division_category", "unknown")
        if cat and cat != "unknown":
            categories_present.add(cat)

    # Check required divisions
    expected = EXPECTED_DIVISIONS[event_type]
    for required_cat in expected.get("required", []):
        if required_cat not in categories_present:
            if event_type == "worlds" and required_cat == "net":
                issues.append(QCIssue(
                    check_id="cv_worlds_missing_net",
                    severity="ERROR",
                    event_id=str(event_id),
                    field="placements_json",
                    message="Worlds event has no net divisions",
                    context={"categories_present": list(categories_present)}
                ))
            elif event_type == "net" and required_cat == "net":
                issues.append(QCIssue(
                    check_id="cv_net_event_no_net_divs",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message="event_type=net but no net divisions found",
                    context={"categories_present": list(categories_present)}
                ))
            elif event_type == "freestyle" and required_cat == "freestyle":
                issues.append(QCIssue(
                    check_id="cv_freestyle_event_no_freestyle_divs",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message="event_type=freestyle but no freestyle divisions found",
                    context={"categories_present": list(categories_present)}
                ))

    # Check expected (warn if missing)
    for expected_cat in expected.get("expected", []):
        if expected_cat not in categories_present:
            if event_type == "worlds" and expected_cat == "freestyle":
                issues.append(QCIssue(
                    check_id="cv_worlds_missing_freestyle",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message="Worlds event has no freestyle divisions",
                    context={"categories_present": list(categories_present)}
                ))

    # cv_all_unknown_divisions: All placements have division_category=unknown
    if placements:
        all_unknown = all(p.get("division_category") == "unknown" for p in placements)
        if all_unknown:
            issues.append(QCIssue(
                check_id="cv_all_unknown_divisions",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message="All placements have division_category=unknown",
                context={"placement_count": len(placements)}
            ))

    return issues


def check_division_quality(rec: dict) -> list[QCIssue]:
    """Check for division name quality issues."""
    issues = []
    # Note: cv_division_looks_like_player check was removed as it had too many false positives
    # (e.g., "Single Homme" = French for "Men's Singles")
    return issues


def check_team_splitting(rec: dict) -> list[QCIssue]:
    """Check for doubles teams that weren't properly split."""
    issues = []
    event_id = rec.get("event_id", "")
    placements = json.loads(rec.get("placements_json", "[]"))

    for i, p in enumerate(placements):
        competitor_type = p.get("competitor_type", "")
        player1 = p.get("player1_name", "")
        player2 = p.get("player2_name", "")
        div_canon = p.get("division_canon", "")

        # cv_doubles_unsplit_team: Doubles division with single player (missed separator)
        is_doubles_div = "doubles" in div_canon.lower() or "double" in div_canon.lower()
        if is_doubles_div and competitor_type == "player" and player1 and not player2:
            # Check if player1 looks like it might contain two names
            if " & " in player1 or " and " in player1.lower():
                issues.append(QCIssue(
                    check_id="cv_doubles_unsplit_team",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Doubles division with unsplit team: {player1[:60]}",
                    example_value=player1[:60],
                    context={"placement_index": i, "division": div_canon}
                ))

    return issues


def check_year_date_consistency(rec: dict) -> list[QCIssue]:
    """Check if year field matches year in date field."""
    issues = []
    event_id = rec.get("event_id", "")
    year = rec.get("year")
    date_str = rec.get("date", "")

    if year and date_str:
        # Extract year from date
        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if year_match:
            date_year = int(year_match.group(0))
            if date_year != year:
                issues.append(QCIssue(
                    check_id="cv_year_date_mismatch",
                    severity="ERROR",
                    event_id=str(event_id),
                    field="year",
                    message=f"Year field ({year}) doesn't match year in date ({date_year})",
                    example_value=date_str,
                    context={"year_field": year, "date_year": date_year}
                ))

    return issues


# ------------------------------------------------------------
# QC Cross-Record Checks
# ------------------------------------------------------------
def check_event_id_uniqueness(records: list[dict]) -> list[QCIssue]:
    """Check that event_id values are unique."""
    issues = []
    seen = {}
    for rec in records:
        event_id = str(rec.get("event_id", ""))
        if event_id in seen:
            issues.append(QCIssue(
                check_id="event_id_duplicate",
                severity="ERROR",
                event_id=event_id,
                field="event_id",
                message=f"Duplicate event_id (first seen at index {seen[event_id]})",
                context={"first_index": seen[event_id]},
            ))
        else:
            seen[event_id] = len(seen)
    return issues


def check_worlds_per_year(records: list[dict]) -> list[QCIssue]:
    """Check exactly one worlds event per year."""
    issues = []
    worlds_by_year = defaultdict(list)

    for rec in records:
        event_type = rec.get("event_type", "")
        if event_type and event_type.lower() == "worlds":
            year = rec.get("year")
            if year:
                worlds_by_year[year].append(rec.get("event_id"))

    for year, event_ids in worlds_by_year.items():
        if len(event_ids) > 1:
            issues.append(QCIssue(
                check_id="worlds_multiple_per_year",
                severity="ERROR",
                event_id=str(event_ids[0]),
                field="event_type",
                message=f"Multiple worlds events in {year}: {event_ids}",
                context={"year": year, "event_ids": event_ids},
            ))

    return issues


def check_duplicates(records: list[dict]) -> list[QCIssue]:
    """Check for duplicate (year, event_name, location) combinations."""
    issues = []
    seen = {}

    for rec in records:
        year = rec.get("year")
        event_name = (rec.get("event_name") or "").strip().lower()
        location = (rec.get("location") or "").strip().lower()

        if year and event_name:
            key = (year, event_name, location)
            if key in seen:
                issues.append(QCIssue(
                    check_id="duplicate_event",
                    severity="WARN",
                    event_id=str(rec.get("event_id")),
                    field="event_name",
                    message=f"Possible duplicate: same (year, event_name, location) as event {seen[key]}",
                    context={"duplicate_of": seen[key], "year": year},
                ))
            else:
                seen[key] = rec.get("event_id")

    return issues


# ------------------------------------------------------------
# QC Orchestration
# ------------------------------------------------------------
def run_qc(records: list[dict]) -> tuple[dict, list[dict]]:
    """
    Run all QC checks on records.
    Returns (summary_dict, issues_list).
    """
    all_issues = []

    # Field-level checks
    for rec in records:
        all_issues.extend(check_event_id(rec))
        all_issues.extend(check_event_name(rec))
        all_issues.extend(check_event_type(rec))
        all_issues.extend(check_location(rec))
        all_issues.extend(check_date(rec))
        all_issues.extend(check_year(rec))
        all_issues.extend(check_host_club(rec))
        all_issues.extend(check_placements_json(rec))
        all_issues.extend(check_results_extraction(rec))
        # Cross-validation checks (Stage 2 specific)
        all_issues.extend(check_expected_divisions(rec))
        all_issues.extend(check_division_quality(rec))
        all_issues.extend(check_team_splitting(rec))
        all_issues.extend(check_year_date_consistency(rec))

    # Cross-record checks
    all_issues.extend(check_event_id_uniqueness(records))
    all_issues.extend(check_worlds_per_year(records))
    all_issues.extend(check_duplicates(records))

    # Build summary
    counts_by_check = defaultdict(lambda: {"ERROR": 0, "WARN": 0})
    for issue in all_issues:
        counts_by_check[issue.check_id][issue.severity] += 1

    total_errors = sum(1 for i in all_issues if i.severity == "ERROR")
    total_warnings = sum(1 for i in all_issues if i.severity == "WARN")

    # Field coverage stats
    field_coverage = {}
    for field in ["event_id", "event_name", "date", "location", "host_club", "event_type", "year"]:
        non_empty = sum(1 for r in records if r.get(field) not in [None, ""])
        field_coverage[field] = {
            "present": non_empty,
            "total": len(records),
            "percent": round(100 * non_empty / len(records), 1) if records else 0,
        }

    summary = {
        "total_records": len(records),
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "counts_by_check": dict(counts_by_check),
        "field_coverage": field_coverage,
    }

    return summary, [i.to_dict() for i in all_issues]


def write_qc_outputs(summary: dict, issues: list[dict], out_dir: Path) -> None:
    """Write QC summary and issues to output files."""
    # Write summary JSON
    summary_path = out_dir / "stage2_qc_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Wrote: {summary_path}")

    # Write issues JSONL
    issues_path = out_dir / "stage2_qc_issues.jsonl"
    with open(issues_path, "w", encoding="utf-8") as f:
        for issue in issues:
            f.write(json.dumps(issue, ensure_ascii=False) + "\n")
    print(f"Wrote: {issues_path} ({len(issues)} issues)")


def load_baseline(data_dir: Path) -> Optional[dict]:
    """Load QC baseline if it exists."""
    baseline_path = data_dir / "qc_baseline_stage2.json"
    if baseline_path.exists():
        with open(baseline_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_baseline(summary: dict, data_dir: Path) -> None:
    """Save QC summary as baseline."""
    data_dir.mkdir(parents=True, exist_ok=True)
    baseline_path = data_dir / "qc_baseline_stage2.json"
    with open(baseline_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"Saved baseline: {baseline_path}")


def print_qc_delta(current: dict, baseline: dict) -> bool:
    """
    Print delta between current and baseline QC results.
    Returns True if no regressions (ERROR increases), False otherwise.
    """
    print(f"\n{'='*60}")
    print("QC DELTA REPORT (vs baseline)")
    print(f"{'='*60}")

    baseline_checks = baseline.get("counts_by_check", {})
    current_checks = current.get("counts_by_check", {})

    all_checks = set(baseline_checks.keys()) | set(current_checks.keys())
    regressions = []

    for check_id in sorted(all_checks):
        b = baseline_checks.get(check_id, {"ERROR": 0, "WARN": 0})
        c = current_checks.get(check_id, {"ERROR": 0, "WARN": 0})

        b_err, b_warn = b.get("ERROR", 0), b.get("WARN", 0)
        c_err, c_warn = c.get("ERROR", 0), c.get("WARN", 0)

        err_delta = c_err - b_err
        warn_delta = c_warn - b_warn

        if err_delta != 0 or warn_delta != 0:
            err_sign = "+" if err_delta > 0 else ""
            warn_sign = "+" if warn_delta > 0 else ""
            print(f"  {check_id}:")
            if err_delta != 0:
                print(f"    ERROR: {b_err} -> {c_err} ({err_sign}{err_delta})")
            if warn_delta != 0:
                print(f"    WARN:  {b_warn} -> {c_warn} ({warn_sign}{warn_delta})")

            if err_delta > 0:
                regressions.append(check_id)

    if not regressions and all_checks:
        # Check for any changes
        has_changes = any(
            baseline_checks.get(c, {}) != current_checks.get(c, {})
            for c in all_checks
        )
        if not has_changes:
            print("  No changes from baseline.")

    print(f"\nTotal: {baseline.get('total_errors', 0)} -> {current.get('total_errors', 0)} errors, "
          f"{baseline.get('total_warnings', 0)} -> {current.get('total_warnings', 0)} warnings")

    if regressions:
        print(f"\n⚠️  REGRESSIONS DETECTED in: {regressions}")
        print(f"{'='*60}\n")
        return False

    print(f"{'='*60}\n")
    return True


def print_qc_summary(summary: dict) -> None:
    """Print QC summary to console."""
    print(f"\n{'='*60}")
    print("QC SUMMARY")
    print(f"{'='*60}")
    print(f"Total records: {summary['total_records']}")
    print(f"Total errors:  {summary['total_errors']}")
    print(f"Total warnings: {summary['total_warnings']}")

    print("\nField coverage:")
    for field, stats in summary.get("field_coverage", {}).items():
        print(f"  {field:15s}: {stats['present']:4d}/{stats['total']:4d} ({stats['percent']:5.1f}%)")

    print("\nIssues by check:")
    for check_id, counts in sorted(summary.get("counts_by_check", {}).items()):
        err = counts.get("ERROR", 0)
        warn = counts.get("WARN", 0)
        if err > 0:
            print(f"  {check_id}: {err} ERROR, {warn} WARN")
        elif warn > 0:
            print(f"  {check_id}: {warn} WARN")

    print(f"{'='*60}\n")


def print_verification_stats(records: list[dict]) -> None:
    """Print verification gate statistics."""
    total = len(records)
    print(f"\n{'='*60}")
    print("VERIFICATION GATE: Stage 2 (Canonicalization)")
    print(f"{'='*60}")
    print(f"Total events processed: {total}")

    if total == 0:
        return

    # Count placements
    total_placements = 0
    division_counts = {}
    confidence_counts = {"high": 0, "medium": 0, "low": 0}

    for rec in records:
        placements = json.loads(rec.get("placements_json", "[]"))
        total_placements += len(placements)

        for p in placements:
            div = p.get("division_canon", "Unknown")
            division_counts[div] = division_counts.get(div, 0) + 1
            conf = p.get("parse_confidence", "unknown")
            if conf in confidence_counts:
                confidence_counts[conf] += 1

    print(f"Total placements parsed: {total_placements}")
    print(f"Average placements per event: {total_placements / total:.1f}")

    # Division frequency (top 10)
    print("\nTop 10 divisions by frequency:")
    sorted_divs = sorted(division_counts.items(), key=lambda x: -x[1])[:10]
    for div, count in sorted_divs:
        print(f"  {div:30s}: {count:5d}")

    # Confidence distribution
    print("\nParse confidence distribution:")
    for conf, count in sorted(confidence_counts.items()):
        pct = (count / total_placements * 100) if total_placements > 0 else 0
        print(f"  {conf:10s}: {count:5d} ({pct:5.1f}%)")

    # Low confidence detail
    low_conf_events = []
    for rec in records:
        placements = json.loads(rec.get("placements_json", "[]"))
        low_count = sum(1 for p in placements if p.get("parse_confidence") == "low")
        if low_count > 0:
            low_conf_events.append((rec.get("event_id"), low_count))

    if low_conf_events:
        print(f"\nEvents with low-confidence parses: {len(low_conf_events)}")
        print("Sample low-confidence events (first 5):")
        for eid, count in low_conf_events[:5]:
            print(f"  event_id={eid}: {count} low-confidence placements")

    # Sample output
    print("\nSample events (first 3):")
    for i, rec in enumerate(records[:3]):
        placements = json.loads(rec.get("placements_json", "[]"))
        print(f"  [{i+1}] event_id={rec.get('event_id')}, "
              f"year={rec.get('year')}, "
              f"placements={len(placements)}")

    print(f"{'='*60}\n")


def main():
    """
    Read stage1 CSV, canonicalize, run QC, and output stage2 CSV.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Stage 2: Canonicalize raw event data")
    parser.add_argument("--save-baseline", action="store_true",
                        help="Save current QC results as the new baseline")
    args = parser.parse_args()

    repo_dir = Path(__file__).resolve().parent
    out_dir = repo_dir / "out"
    data_dir = repo_dir / "data"
    in_csv = out_dir / "stage1_raw_events.csv"
    out_csv = out_dir / "stage2_canonical_events.csv"

    if not in_csv.exists():
        print(f"ERROR: Input file not found: {in_csv}")
        print("Run 01_parse_mirror.py first.")
        return

    print(f"Reading: {in_csv}")
    records = read_stage1_csv(in_csv)

    print(f"Canonicalizing {len(records)} events...")
    canonical = canonicalize_records(records)

    # Deduplicate events with same (year, event_name, location)
    canonical, removed_duplicates = deduplicate_events(canonical)
    if removed_duplicates:
        print(f"Removed {len(removed_duplicates)} duplicate events:")
        for dup in removed_duplicates:
            print(f"  - {dup['event_id']}: {dup['event_name'][:50]} ({dup['year']})")

    # Remove junk events with no useful data
    junk_removed = [r for r in canonical if r["event_id"] in JUNK_EVENTS_TO_EXCLUDE]
    canonical = [r for r in canonical if r["event_id"] not in JUNK_EVENTS_TO_EXCLUDE]
    if junk_removed:
        print(f"Removed {len(junk_removed)} junk events (no useful data):")
        for junk in junk_removed:
            print(f"  - {junk['event_id']}: {junk['event_name'][:50]}")

    print(f"Writing to: {out_csv}")
    write_stage2_csv(canonical, out_csv)

    print_verification_stats(canonical)
    print(f"Wrote: {out_csv}")

    # Run QC checks
    print("\nRunning QC checks...")
    qc_summary, qc_issues = run_qc(canonical)

    # Write QC outputs
    write_qc_outputs(qc_summary, qc_issues, out_dir)

    # Print QC summary
    print_qc_summary(qc_summary)

    # Delta reporting against baseline
    baseline = load_baseline(data_dir)
    if baseline:
        no_regressions = print_qc_delta(qc_summary, baseline)
        if not no_regressions:
            print("WARNING: QC regressions detected!")
    else:
        print("No baseline found. Run with --save-baseline to create one.")

    # Save baseline if requested
    if args.save_baseline:
        save_baseline(qc_summary, data_dir)


if __name__ == "__main__":
    main()
