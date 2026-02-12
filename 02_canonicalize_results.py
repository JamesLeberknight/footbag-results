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

# Import master QC orchestrator
# Note: qc_master will import slop detection checks automatically
try:
    from qc_master import (
        run_qc_for_stage,
        load_baseline as load_baseline_master,
        save_baseline as save_baseline_master,
        print_qc_delta as print_qc_delta_master,
        print_qc_summary as print_qc_summary_master,
    )
    USE_MASTER_QC = True
except ImportError:
    # Fallback: keep old QC if master not available
    print("Warning: Could not import qc_master, using embedded QC")
    USE_MASTER_QC = False


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
#
# Decision: 2026-02 - This structure allows adding event-specific parsing
# without polluting the general parsing logic.
EVENT_PARSING_RULES = {
    # 2011 World Championships - doubles results have merged team format
    # Format: "Emmanuel Bouchard [1] CAN Florian Goetze GER"
    "1293877677": {
        "split_merged_teams": True,
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
    "battle", "sick3", "sick 3", "sick", "request", "last standing", "last",
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

# Division name normalization for non-English languages
# Maps division headers to English equivalents
DIVISION_LANGUAGE_MAP = {
    # Spanish divisions
    # Pattern: normalize by removing RESULTADO/RESULTADOS prefix, PUESTOS suffix
    # INDIVIDUAL = Singles, DOBLES = Doubles
    "resultados open individual": "Open Singles",
    "resultado open individual": "Open Singles",
    "resultados open individual puestos": "Open Singles",
    "resultado open individual puestos": "Open Singles",
    "resultados open singles": "Open Singles",
    "resultado open singles": "Open Singles",
    "resultados open dobles": "Open Doubles",
    "resultado open dobles": "Open Doubles",
    "resultado open dobles puestos": "Open Doubles",
    "resultados open dobles puestos": "Open Doubles",
    "resultado footbag net open dobles": "Open Doubles Net",
    "open net dobles": "Open Doubles Net",
    "open dobles": "Open Doubles",
    "resultados sick three": "Sick 3",
    "sick 3 resultados": "Sick 3",

    # French divisions
    # SIMPLE/SINGLE = Singles, Homme = Men's, Féminine/Féminin = Women's
    "single homme": "Men's Singles",
    "single féminine": "Women's Singles",
    "simple net féminin": "Women's Singles Net",
}


def normalize_language_division(division_raw: str) -> str:
    """Normalize non-English division names to English equivalents."""
    if not division_raw:
        return division_raw
    key = division_raw.lower().strip().rstrip('.:')
    return DIVISION_LANGUAGE_MAP.get(key, division_raw)


def truncate_long_division(division_raw: str, max_length: int = 80) -> str:
    """
    Truncate excessively long division names.

    Long divisions are usually misidentified placements or event descriptions.
    Keeps meaningful part and truncates at word boundary.
    Also strips explanatory parenthetical content (e.g., "Shred 30 (Total Adds...)").
    """
    if not division_raw:
        return division_raw

    # First, strip explanatory parenthetical content from end
    # E.g., "Shred 30 (Total Adds Compared To Total Contacts)" -> "Shred 30"
    cleaned = re.sub(r'\s*\([^)]*\)\s*$', '', division_raw).strip()

    # If already short after cleaning parentheses, return it
    if len(cleaned) <= max_length:
        return cleaned

    # Truncate at max_length and try to break at word boundary
    truncated = cleaned[:max_length]
    last_space = truncated.rfind(' ')
    if last_space > max_length // 2:  # Only break at word if we still have meaningful content
        truncated = truncated[:last_space].strip()
    return truncated


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


def _has_division_keyword(text: str) -> bool:
    """Check if text contains any division keyword as a whole word (not substring)."""
    text_lower = text.lower()
    for kw in DIVISION_KEYWORDS:
        # Use word boundary matching to avoid "pro" matching "Prokoph"
        if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
            return True
    return False


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

    # Strip explanatory text in parentheses for length check
    # E.g., "Intermediate Shred 30 (total adds only.. uniques etc. not counted)"
    #    -> "Intermediate Shred 30"
    line_without_parens = re.sub(r'\s*\([^)]+\)\s*', ' ', line).strip()

    # Length check - real division headers are short (after removing explanations)
    if len(line_without_parens) > 50:
        return False

    # Reject empty or very short lines
    if len(line) < 3:
        return False

    # Reject lines that look like results (start with number + name)
    # BUT: Don't reject if it contains division keywords (e.g., "30 Second Shred", "1 Minute Freestyle")
    # Covers both:
    #   - Separator format: "1. John Smith", "2) Jane Doe", "3: Player"
    #   - Tab-separated format: "1\tJohn Smith" (common in tabular results)
    if re.match(r"^\d+\s*[.):\-]?\s+[A-Z]", line):
        # Check if it has division keywords - if so, might be a time-based division
        if not _has_division_keyword(low):
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
    # Use word boundary for "contact" to avoid matching "Consecutive"
    if line.startswith("&") or re.search(r'\bcontact\b', low):
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

    # Must contain at least one division keyword (word-boundary match)
    if not _has_division_keyword(low):
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
        starts_valid = _has_division_keyword(rest)

    # Check if it's a short all-caps header (e.g., "DOUBLE:", "SINGLE:")
    is_caps_header = line.isupper() and len(line_without_parens) <= 30

    # Check if it ends with colon and is short (likely a header)
    # Use line_without_parens for length: "Open Circle (3 rounds: variety, etc.):" is a valid header
    is_colon_header = line.rstrip().endswith(':') and len(line_without_parens) <= 40

    # Short lines with keywords are likely headers
    is_short_with_keyword = len(line_without_parens) <= 35

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
    Remove ONLY trailing scores and obvious non-name data from player names.
    Conservative approach to avoid creating duplicates.

    Examples:
      "Matt Strong 526" -> "Matt Strong"
      "Ricky Moran: Bothell, WA - 121.35" -> "Ricky Moran: Bothell, WA"

    NOTE: Parenthesized data is kept (may be club names OR tricks - can't reliably distinguish)
    """
    # Remove trailing 2-4 digit numbers (scores)
    cleaned = re.sub(r'\s+\d{2,4}\s*$', '', name).strip()

    # Remove trailing decimal scores like "- 121.35" or "= 95.2"
    cleaned = re.sub(r'\s+[-=]\s*\d+(\.\d+)?\s*$', '', cleaned).strip()

    # Remove trailing score patterns like "123 -" or "456 ="
    cleaned = re.sub(r'\s+\d+\s*[-=]\s*$', '', cleaned).strip()

    return cleaned


def clean_host_club(name: str) -> str:
    """
    Clean host club names by removing common formatting artifacts.

    Removes:
    - Numbered prefixes: "1. Club Name", "2. Club Name"
    - Ordinal prefixes: "1st Club", "2nd Club" (converted from numbered)
    """
    if not name:
        return name

    # Remove numbered prefix: "1. Name", "2. Name", etc.
    name = re.sub(r'^\d+\.\s+', '', name).strip()

    # Remove ordinal prefix: "1st Name", "2nd Name", "3rd Name", "4th Name", etc.
    name = re.sub(r'^\d+(?:st|nd|rd|th)\s+', '', name).strip()

    return name


def clean_player_name(name: str) -> str:
    """
    Remove scores, trick lists, and narrative commentary from player names.
    Applied after split_entry() to clean individual player names.

    Preserves: country/club codes in parentheses like (CZE), (Paris Zion)
    Removes: scores, stats breakdowns, trick lists, narrative text
    """
    if not name:
        return name

    original = name

    # Rule 1: Strip "Name (CZE) - 242.79 (127 adds, 31 uniques, ...)"
    # Score + stats after dash/equals following a parenthetical
    name = re.sub(r'(\))\s*[-=]\s*\d+\.?\d*\s*\([\d\s,a-zA-Z]+\).*$', r'\1', name).strip()

    # Rule 2: Strip "Name 146,66 (36 contacts, 12 uniques, 110 adds...)"
    # European comma-decimal score followed by stats parenthetical
    name = re.sub(r'\s+\d+[,.]\d+\s*\([\d\s,a-zA-Z]+\).*$', '', name).strip()

    # Rule 3: Strip "Name ---------(<score>)" or "Name --------- (<score>)"
    # Dashed-out scores (player didn't make finals)
    name = re.sub(r'\s+-{3,}\s*\(\d+\.?\d*\)\s*$', '', name).strip()

    # Rule 4: Strip "Name <score> (<score>)" — double score pattern
    # e.g. "Vasek Klouda 259.35 (281.53)"
    name = re.sub(r'\s+\d+\.?\d+\s*\(\d+\.?\d+\)\s*$', '', name).strip()

    # Rule 5: Strip parenthetical scores: "Name (194.47)"
    # Only numeric content with decimal point, 2+ digits before decimal
    name = re.sub(r'\s*\(\d{2,}\.?\d*\)\s*$', '', name).strip()

    # Rule 6: Strip "Name (Country) - score (stats...)" where stats has non-paren format
    # e.g. "David Clavens (USA) - whirlwalk > blurriest > ..."
    name = re.sub(r'(\([A-Z]{2,}(?:\s+\w+)*\))\s*[-=]\s+\S.{15,}$', r'\1', name).strip()

    # Rule 7: Strip score + trick parenthetical: "Name 42.6 (Alpine Food Processor > ...)"
    # Score followed by tricks in parentheses (contains > or uppercase trick names)
    name = re.sub(r'\s+\d+\.?\d*\s*\([A-Z][\w\s>.,]+\).*$', '', name).strip()

    # Rule 8: Strip colon-separated trick lists: "Name: trick, trick, trick"
    # Colon followed by 10+ chars containing trick indicators (, > ; ( ")
    m = re.search(r':\s+.{10,}$', name)
    if m and re.search(r'[,>;("]', m.group()):
        name = name[:m.start()].strip()

    # Rule 9: Strip trick lists with "--" separator: "Name--trick, trick"
    name = re.sub(r'\s*--\s*.{10,}$', '', name).strip()

    # Rule 10: Strip trick lists with ">" after country/club parenthetical
    # e.g. "Felix Zenger (FIN) Double Blender > Superfly > ..."
    m = re.match(r'^(.+?\([^)]+\))\s+\S.*>.*$', name)
    if m:
        candidate = m.group(1)
        # Make sure what follows the paren looks like tricks (has >)
        rest = name[len(candidate):]
        if '>' in rest:
            name = candidate.strip()

    # Rule 11: Strip trick lists with ">" after bare name (no parenthetical)
    # e.g. "Damian Gielnicki Spinning Eggbeater > Paradon Swirl > ..."
    # Only if the name doesn't already contain parentheses
    if '(' not in name and '>' in name:
        # Find the first > and look for a name before the trick
        idx = name.index('>')
        before = name[:idx].strip()
        # Try to find where the name ends and tricks begin
        # Look for a transition from capitalized name words to trick content
        words = before.split()
        # Find the last word that looks like a name start (before trick words)
        # Trick words tend to come after 2+ name words
        if len(words) >= 3:
            # Check if word 3+ look like trick content (Spinning, Ducking, etc.)
            # Heuristic: first 2 words are the name, rest is tricks
            candidate_name = ' '.join(words[:2])
            # Verify the candidate looks like a name (both words start uppercase)
            if all(w[0].isupper() for w in words[:2] if w):
                name = candidate_name.strip()

    # Rule 12: Strip narrative text after club parenthetical
    # e.g. "Claire Beltran (Paris Zion) Elle reste championne..."
    # Match: close paren, then 10+ chars of non-paren text
    m = re.match(r'^(.+?\([^)]+\))\s+(.{10,})$', name)
    if m:
        after_paren = m.group(2)
        # Only strip if text after paren looks like narrative (starts with lowercase
        # or contains sentence-like content), NOT like a name suffix
        if (after_paren[0].islower() or
            re.search(r'[.!,;].*\s', after_paren) or
            len(after_paren) > 30):
            # But preserve if it looks like it could be team members
            # e.g. "Name (Club), Name2, Name3"
            if not re.match(r'^[A-Z][a-z]+\s+[A-Z]', after_paren):
                name = m.group(1).strip()

    # Rule 13: Strip "? " trick lists (? used as separator in some events)
    # e.g. "Serge Kaldany ? Quantum Ducking Mirage > Pixie..."
    if ' ? ' in name and '>' in name:
        idx = name.index(' ? ')
        candidate = name[:idx].strip()
        if candidate and candidate[0].isupper():
            name = candidate

    # Rule 14: Strip "Name (Country)(tricks...)" — double parenthetical
    # e.g. "Filip Wojciuk (Poland)(fairy ducking butterfly-bedwetter-...)"
    # Must run before general parenthetical stripping to preserve country code
    m = re.match(r'^(.+?\([^)]{2,15}\))\((.{10,})\)(.*)$', name)
    if m:
        paren2 = m.group(2)
        # Only strip if second paren content looks like tricks (lowercase, has separators)
        if paren2[0].islower() or '>' in paren2 or paren2.count('-') >= 2:
            name = m.group(1).strip()

    # Rule 15: Strip parenthetical trick lists (contains > or | or many commas or = separator)
    # e.g. "Vasek Klouda (Janiwalker>Blurriest, Bedwetter>...)"
    # e.g. "Jakub Mo¶ciszewski (phoenix>bedwetter>pixie paradon | phasing>...)"
    # e.g. "Ale? Zelinka (Backside Symposium Atomic Eggbeater = Symposium...)"
    # e.g. "Jon Schneider (Hopover-Swirl-dragon-rake, Infinity-swirl-...)"
    # But preserve country codes like (CZE) and club names like (Paris Zion)
    m = re.match(r'^([^(]+)\((.+)\)(.*)$', name)
    if m:
        before_paren = m.group(1).strip()
        paren_content = m.group(2)
        after_paren = m.group(3).strip()
        # Strip if paren content contains trick indicators and is long
        has_trick_indicators = ('>' in paren_content or '|' in paren_content or
                                '=' in paren_content or paren_content.count(',') >= 2 or
                                paren_content.count('-') >= 2)
        # Also strip if it's long narrative text (contains ... or starts with common words)
        is_narrative = ('...' in paren_content or
                        re.match(r'^(I |the |a |an |we |he |she |it |this )', paren_content, re.IGNORECASE))
        if len(paren_content) > 15 and (has_trick_indicators or is_narrative):
            # But not if it's clearly a country/club code (short, all letters)
            if not re.match(r'^[A-Za-z\s]{2,10}$', paren_content):
                name = before_paren
                # If there was text after the paren that looks like a country code, keep it
                if after_paren and re.match(r'^\([A-Z]{2,5}\)', after_paren):
                    name = name + ' ' + after_paren

    # Rule 16: Strip "Name (Country) - narrative" where narrative isn't a score
    # e.g. "Dan Greer (USA)- 3 way tie; did not advance past semi-final"
    m = re.match(r'^(.+?\([^)]{2,15}\))\s*-\s*(.{10,})$', name)
    if m:
        after = m.group(2)
        # Only strip if it doesn't start with a digit (which would be a score, handled above)
        if not re.match(r'^\d', after):
            name = m.group(1).strip()

    # Rule 17: Strip square bracket annotations
    # e.g. "Florian Goetze [Final: Emmanuel withdrew due to injury]..."
    m_bracket = re.search(r'\s*\[.{10,}$', name)
    if m_bracket and len(name[:m_bracket.start()].strip()) >= 3:
        name = name[:m_bracket.start()].strip()

    # Rule 18: Strip narrative after club parenthetical with comma separator
    # e.g. "Christopher Reyer (Paris Rien n'est Hacky), désolé pour l'oubli..."
    m = re.match(r'^(.+?\([^)]+\)),\s+(.{10,})$', name)
    if m:
        after = m.group(2)
        # Strip if the text after comma is narrative (starts lowercase)
        if after[0].islower():
            name = m.group(1).strip()

    # Rule 19: Strip colon trick lists where content has parenthetical explanation
    # e.g. "Jeremy Benton: Stepping P.S. Blender ("S" means 'simple' = 7...)"
    m = re.search(r':\s+\S.{10,}$', name)
    if m and ('(' in m.group() or '"' in m.group()):
        name = name[:m.start()].strip()

    # Rule 20: Strip bare name + all-lowercase trick content with >
    # e.g. "DamianPiechocki stepping ps whirl >spinning pdxwhirl >..."
    # e.g. "Maciek Niczyporuk janiwalk>bedwetter>pixie whirling swirl (5,2)"
    if '>' in name:
        # Find where lowercase trick text starts after an uppercase-starting name
        m = re.match(r'^([A-Z]\S+(?:\s+[A-Z]\S+)*)\s+([a-z].+)$', name)
        if m and '>' in m.group(2):
            name = m.group(1).strip()

    # Rule 21: Strip unclosed second parenthetical after country code
    # e.g. "Alex Trener (Austria)(matador-blury whirl-ps whirl, janiwalker-..."
    # Must run before unclosed-paren rule to preserve the country code
    m = re.match(r'^(.+?\([^)]{2,15}\))\((.{10,})$', name)
    if m:
        paren2 = m.group(2)
        if paren2[0].islower() or '>' in paren2 or paren2.count('-') >= 2:
            name = m.group(1).strip()

    # Rule 23: Strip leading slash (parsing artifact from some events)
    # e.g. "/ Serge Kaldany" → "Serge Kaldany"
    name = re.sub(r'^/\s*', '', name).strip()

    # Rule 24: Strip trailing slash
    # e.g. "Forest Schrodt /" → "Forest Schrodt", "scratch/" → "scratch"
    name = re.sub(r'\s*/\s*$', '', name).strip()

    # Rule 25: Strip "- n/a" suffix (player didn't compete)
    # e.g. "Sergio Garcia (Spain) - n/a" → "Sergio Garcia (Spain)"
    name = re.sub(r'\s*-\s*n/a\s*$', '', name, flags=re.IGNORECASE).strip()

    # Rule 26: Strip N/A placeholders in parentheses or with dash prefix
    # e.g. "Jeff Mudd (N/A)" → "Jeff Mudd", "SERVICE POACHING-n/a" → "SERVICE POACHING"
    # Strip trailing "(n/a)" or "(N/A)" or similar placeholders
    name = re.sub(r'\s*\(n/a\)\s*$', '', name, flags=re.IGNORECASE).strip()
    # Also handle "TRICK-n/a" pattern (missing trick data)
    name = re.sub(r'(-n/a)\s*$', '', name, flags=re.IGNORECASE).strip()

    # Rule 22: Strip unclosed parenthetical trick/narrative content
    # e.g. "Vasek Klouda (Janiwalker>Blurriest, Bedwetter>Frantic Butterfly, Pixie"
    # e.g. "Nick Landes 42.2 (Nuclear Osis > Spinning Ducking Butterfly > ..."
    # These are truncated trick lists with ( but no closing )
    if name.count('(') > name.count(')'):
        idx = name.index('(')
        before = name[:idx].strip()
        after_open = name[idx+1:]
        # Strip if content after ( is long and has trick/narrative indicators
        if len(after_open) > 15 and (
            '>' in after_open or '|' in after_open or '=' in after_open or
            '...' in after_open or
            after_open.count(',') >= 2 or after_open.count('-') >= 2 or
            re.match(r'^[a-z]', after_open) or
            re.match(r'^(I |the |a |an |we |he |she |it |this )', after_open, re.IGNORECASE)
        ):
            # Strip the trailing score before ( if present
            before = re.sub(r'\s+\d+\.?\d*\s*$', '', before).strip()
            if len(before) >= 3:
                name = before

    # Rule 27: Strip trick name in middle parenthetical for "Big One" division
    # e.g., "Paweł Ścierski (Symp. Whirling SS. Rev. Symp. Whirl) (Poland)"
    # Format: Name (TrickDescription) (Country)
    # Detect: two parenthetical groups at the end, middle one is longer
    paren_pairs = []
    i = 0
    while i < len(name):
        if name[i] == '(':
            close = name.find(')', i)
            if close > i:
                paren_pairs.append((i, close, name[i+1:close]))
                i = close + 1
            else:
                break
        else:
            i += 1

    # If we have 2 parenthetical groups at the end, check if it's Name (Trick) (Country)
    if len(paren_pairs) >= 2:
        second_last = paren_pairs[-2]
        last = paren_pairs[-1]
        trick_content = second_last[2]
        country_content = last[2]

        # Check if this looks like trick + country pattern
        # Trick: 3+ words, contains trick keywords
        # Country: short (2-15 chars), typically all uppercase or normal country name
        if (trick_content.count(' ') >= 2 and  # 3+ words
            2 <= len(country_content) <= 15 and
            any(word in trick_content for word in ['Symp', 'Whirl', 'Rev', 'Bedwetter', 'Fusion',
                                                    'Paradox', 'Eggbeater', 'Legbeater', 'Swirl',
                                                    'Mirage', 'Osis', 'Marius', 'Nemesis', 'Gauntlet',
                                                    'Atomic', 'Merlin', 'Mulet', 'Drifter', 'Clown'])):
            # Strip the trick parenthetical, keep name + country
            # Extract everything before the trick paren + the country paren
            before_trick = name[:second_last[0]].strip()
            name = (before_trick + ' (' + country_content + ')').strip()

    return name.strip()


def split_entry(entry: str) -> tuple[str, Optional[str], str]:
    """
    Detect teams/multiple players separated by '/', ' and ', ' & ', commas, or dash separators.
    Returns (player1, player2, competitor_type).
    For multi-player entries (3+ comma-separated names), returns first 2 as team.
    Canonical output format uses '/' separator (handled in _build_name_line).

    Priority:
    1. " & " between names (alternative separator, checked first to handle city notation)
    2. "/" outside parentheses (most common team separator)
    3. " and " between names (word separator)
    4. "et" between names (French "and")
    5. " - ", " – ", " — " between names (dash separators: hyphen, en-dash, em-dash)
    6. ", " between multiple names (comma separator for groups - returns first 2)
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

    # Check for comma-separated names FIRST (before "and" split)
    # This handles cases like "Name1, Name2 and Name3" where commas are primary separator
    # We check early but not before " & " which handles different cases
    comma_count = entry_clean.count(',')

    # Try " & " first - it's often used when "/" appears in city/country notation
    if " & " in entry_clean:
        a, b = entry_clean.split(" & ", 1)
        # Validate: both parts should start with capital letter (name-like)
        a_first = a.strip()[:1] if a.strip() else ''
        b_first = b.strip()[:1] if b.strip() else ''
        if (len(a.strip()) >= 2 and len(b.strip()) >= 2 and
            a_first.isupper() and b_first.isupper()):
            return strip_trailing_score(a.strip()), strip_trailing_score(b.strip()), "team"

    # Try "/" outside parentheses
    slash_idx = slash_outside_parens(entry_clean)
    if slash_idx > 0:
        a = entry_clean[:slash_idx].strip()
        b = entry_clean[slash_idx + 1:].strip()
        if len(a) >= 2 and len(b) >= 2:
            return strip_trailing_score(a), strip_trailing_score(b), "team"

    # " and " between two names (case insensitive)
    # Be careful not to match "and" within names like "Alexandra"
    # Special handling: if entry has commas AND "and", check if left side of "and" has commas
    # This handles "Name1, Name2 and Name3" format
    and_match = re.search(r'\s+and\s+', entry_clean, re.IGNORECASE)
    if and_match:
        a = entry_clean[:and_match.start()].strip()
        b = entry_clean[and_match.end():].strip()
        a_first = a[:1] if a else ''
        b_first = b[:1] if b else ''
        # Validate both parts look like names (at least 2 chars each, start with capital)
        if (len(a) >= 2 and len(b) >= 2 and
            a_first.isupper() and b_first.isupper()):
            # If 'a' has commas (multiple names), try to split on comma first
            if ',' in a and a.count(',') >= 1:
                a_parts = [p.strip() for p in a.split(',')]
                # Use first comma-separated part as player1, rest + b as player2
                if len(a_parts) >= 2 and len(a_parts[0]) >= 2:
                    p1 = strip_trailing_score(a_parts[0])
                    # Reconstruct player2: remaining commas + "and" part
                    remaining = ', '.join(a_parts[1:]) + ' and ' + b
                    p2_clean = strip_trailing_score(remaining)
                    if len(p2_clean) >= 2:
                        return p1, p2_clean, "team"
            return strip_trailing_score(a), strip_trailing_score(b), "team"

    # French "et" separator (case insensitive)
    # Check for " et " between two names
    et_match = re.search(r'\s+et\s+', entry_clean, re.IGNORECASE)
    if et_match:
        a = entry_clean[:et_match.start()].strip()
        b = entry_clean[et_match.end():].strip()
        a_first = a[:1] if a else ''
        b_first = b[:1] if b else ''
        # Validate both parts look like names (at least 2 chars each, start with capital)
        if (len(a) >= 2 and len(b) >= 2 and
            a_first.isupper() and b_first.isupper()):
            return strip_trailing_score(a), strip_trailing_score(b), "team"

    # Dash separator (common in Spanish/Portuguese events)
    # Check for " - ", " – " (en-dash), or " — " (em-dash) between two names
    # Be careful not to match dashes in prefixes like "3rd place - Name"
    # or in hyphenated names like "Jean-Pierre"
    # Matches: hyphen-minus (U+002D), en-dash (U+2013), em-dash (U+2014)
    dash_match = re.search(r'\s+[-–—]\s+', entry_clean)
    if dash_match:
        a = entry_clean[:dash_match.start()].strip()
        b = entry_clean[dash_match.end():].strip()
        a_first = a[:1] if a else ''
        b_first = b[:1] if b else ''
        # Validate both parts look like names (at least 2 chars each, start with capital)
        # Also ensure 'a' doesn't look like an ordinal (e.g., "1st", "2nd", "3rd")
        ordinal_pattern = r'^\d+(st|nd|rd|th)?$'
        if (len(a) >= 2 and len(b) >= 2 and
            a_first.isupper() and b_first.isupper() and
            not re.match(ordinal_pattern, a, re.IGNORECASE)):
            return strip_trailing_score(a), strip_trailing_score(b), "team"

    # Comma-separated names (for multi-player entries like Circle Contest)
    # e.g., "Paweł Nowak, Paweł Ścierski, Krzysztof Sobótka, Sylwia Kocyk (Poland)"
    # Split on comma, but exclude commas that look like location info (e.g., "City, Country")
    # Heuristic: If entry has 3+ comma-separated parts and most look like names, split them
    if ',' in entry_clean:
        # First remove trailing location info like "(Poland)" before splitting
        entry_no_location = re.sub(r'\s*\([^)]*\)\s*$', '', entry_clean).strip()

        if ',' in entry_no_location:
            parts = [p.strip() for p in entry_no_location.split(',')]

            # Check if this looks like a multi-player entry vs "City, Country" format
            # Multi-player: most parts start with capital letter (names)
            # City,Country: 2 parts, second is usually short (country code or country name)
            capital_count = sum(1 for p in parts if p and p[0].isupper())

            # If we have 3+ parts that look like names (start with capital), treat as multi-player
            if len(parts) >= 3 and capital_count >= 3:
                # Multi-player entry: return first two as "team"
                p1 = strip_trailing_score(parts[0])
                p2 = strip_trailing_score(parts[1])
                if len(p1) >= 2 and len(p2) >= 2:
                    return p1, p2, "team"
            elif len(parts) == 2 and capital_count >= 2:
                # Two-part comma entry (less common, might be "Name, Country" or "Name, Name")
                # Only treat as team if both parts are reasonable name lengths (3+ chars)
                p1 = strip_trailing_score(parts[0])
                p2 = strip_trailing_score(parts[1])
                # Check if both parts look like names (not location info)
                # Location format: short country code (2-3 chars) or country names
                # Names are typically 3+ chars, contain letters, may have accents
                if (len(p1) >= 3 and len(p2) >= 3 and
                    p1[0].isupper() and p2[0].isupper() and
                    not re.match(r'^[A-Z]{2,3}$', p2)):  # Not a country code like "POL"
                    return p1, p2, "team"

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

    # Normalize results_text: replace non-breaking spaces with regular spaces
    # Non-breaking spaces (\xa0, \u00a0) can break pattern matching
    if results_text:
        results_text = results_text.replace('\xa0', ' ').replace('\u00a0', ' ')

    # Get event-specific parsing rules
    event_rules = EVENT_PARSING_RULES.get(str(event_id), {})
    use_merged_team_split = event_rules.get("split_merged_teams", False)

    # Track whether we're in a seeding section (should skip these entries)
    in_seeding_section = False

    place_re = re.compile(r"^\s*(\d{1,3})\s*[.)\-:]?\s*(.+)$")
    # Pattern for ordinal placements like "1ST Name", "2ND Name", "1st: Name", "2nd: Name"
    ordinal_re = re.compile(r"^\s*(\d{1,2})(ST|ND|RD|TH):?\s+(.+)$", re.IGNORECASE)
    # Pattern for tied placements like "23/24 Name" - captures the tie suffix
    tied_place_re = re.compile(r"^/\d+\s+(.+)$")
    # Pattern for multi-line ordinal: place indicator on its own line, name on next line
    # English: "1st Place", "2nd Place"
    # Spanish: "1° LUGAR", "2°", "1º", "1er LUGAR", "2do LUGAR"
    multiline_ordinal_re = re.compile(
        r"^\s*(\d{1,2})\s*"
        r"(?:"
        r"(?:st|nd|rd|th)\s+place"                          # English: "1st Place"
        r"|"
        r"[°º]\s*(?:lugar|puesto|place)?"                   # Spanish: "1° LUGAR", "1°", "1º"
        r"|"
        r"(?:er|do|ro|to|ta)\s*(?:lugar|puesto|place)?"     # Spanish text: "1er LUGAR", "2do"
        r")\s*$", re.IGNORECASE)

    # Pending place from multi-line ordinal format ("1st Place\nName")
    pending_place = None

    # Pending division: when we see "Division Header" with no inline name,
    # we expect the next line might be a bare player name (e.g., "Lee Van Sickle")
    pending_division = None

    # Flag to indicate that place/entry_raw have been set by bare name or inline detection
    # (skip ordinal/place regex parsing in this case)
    placement_already_parsed = False

    for raw_line in (results_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Reset placement parsing flag at start of loop
        placement_already_parsed = False

        # Normalize range-style placements (e.g., "9.-12. Player" -> "9. Player")
        # Handles tied placements shown as ranges where each player gets the same line
        # Examples: "9.-12. Wiktor Debski", "13.-16. Jindrich Smola", "17.-20. Alexander Trenner"
        # Pattern: <start>.-<end>. <player> where start is the lower place number
        line = re.sub(r'^(\s*\d{1,3})\.-\d{1,3}\.', r'\1.', line)

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
        # Includes common misspellings: "seddings" (seen in Colombian events)
        if line_lower.rstrip(':') in ("initial seeding", "seeding", "seedings",
                                       "seddings", "seeds"):
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

        # Multi-line ordinal: "1st Place" on its own line, name on next line
        multiline_match = multiline_ordinal_re.match(line)
        if multiline_match:
            pending_place = int(multiline_match.group(1))
            continue

        # If we have a pending place from "Xth Place" line, this line is the name
        if pending_place is not None:
            # This line should be the player/team name
            entry_raw = line
            place = pending_place
            pending_place = None
            # Skip if it looks like a division header (not a player name)
            if looks_like_division_header(line):
                div_text = line.rstrip(":")
                abbrev = div_text.lower()
                if abbrev in ABBREVIATED_DIVISIONS:
                    division_raw = ABBREVIATED_DIVISIONS[abbrev]
                else:
                    division_raw = div_text
                in_seeding_section = False
                continue
            # Fall through to player name processing below
            # (skip the normal place/ordinal parsing)
        else:
            # Check if we're waiting for a bare player name after a division header
            if pending_division is not None and not looks_like_division_header(line):
                # Line doesn't look like a division header
                # Check if it looks like a bare player name
                # Conservative criteria to minimize false positives:
                # 1. Starts with uppercase letter (after stripping leading dash/whitespace)
                # 2. Doesn't start with digit (not a score/time)
                # 3. Reasonable length (not a long URL or narrative)
                # 4. Must have at least one space (First Last format, not single acronym)
                # 5. Must have balanced parentheses (names can have (CZE) but not unmatched parens)
                # 6. No URL-like patterns (://, www., @)
                # 7. No problematic punctuation at line level (;, or multiple commas)

                is_potential_name = False
                # Strip leading dash and whitespace for name validation
                # (e.g., "-Scott Bevier" -> "Scott Bevier")
                line_stripped = re.sub(r'^-\s*', '', line).strip()

                if (line_stripped and line_stripped[0].isupper() and
                    not re.match(r'^\d', line_stripped) and
                    3 <= len(line_stripped) < 70 and  # Tighter length: 3-70 chars (was 100)
                    ' ' in line_stripped and  # Must have space (First Last pattern)
                    not re.search(r'://|www\.|@', line_stripped) and  # No URL patterns
                    line_stripped.count('(') == line_stripped.count(')') and  # Balanced parentheses
                    ';' not in line_stripped and  # No semicolons
                    line_stripped.count(',') <= 1):  # At most one comma (e.g., "Name, Country")

                    # Additional check: must have at least 2 words starting with uppercase
                    words = line_stripped.split()
                    uppercase_words = [w for w in words if w and w[0].isupper()]
                    if len(uppercase_words) >= 2:
                        # Verify it looks like a name, not narrative
                        # Real names: mostly letters, maybe punctuation in parens
                        # Narrative: will have articles, verbs, lowercase-starting words
                        lower_words = [w for w in words if w and w[0].islower()]
                        # Allow up to 1 lowercase word (like "van", "de", "von" in names)
                        if len(lower_words) <= 1:
                            is_potential_name = True

                if is_potential_name:
                    # This looks like a bare player name
                    place = 1  # Implied first place
                    entry_raw = line_stripped  # Use stripped version to remove leading dash
                    division_raw = pending_division
                    pending_division = None
                    placement_already_parsed = True
                    # Fall through to player name processing below
                else:
                    # Not a bare name - reset pending_division and continue with normal parsing
                    pending_division = None
                    # Continue below with normal division header and placement checks

            # Check for bold-style division headers (common in manually entered results)
            # e.g., "**Intermediate Singles**" or text that was in <b> tags
            if looks_like_division_header(line):
                pending_place = None  # Reset pending place on division change

                # Handle "Division: Name" inline format
                # e.g., "4-Square: Lee Van Sickle", "Open Doubles Net: Matthew Johns & Emily Johns"
                # But NOT "Open Singles:" (trailing colon with no name after)
                inline_name = None
                if ':' in line:
                    div_part, _, name_part = line.partition(':')
                    name_part = name_part.strip()
                    # Only treat as inline if name_part looks like a person name
                    # (has Firstname Lastname pattern) and isn't a sub-header
                    if (name_part and re.search(r'[A-Z][a-z]+\s+[A-Z]', name_part)
                            and not looks_like_division_header(name_part)):
                        inline_name = name_part
                        line_for_div = div_part.strip()
                    else:
                        line_for_div = line.rstrip(":")
                else:
                    line_for_div = line.rstrip(":")

                # Expand abbreviated divisions (e.g., "OSN" -> "Open Singles Net")
                abbrev = line_for_div.lower()
                if abbrev in ABBREVIATED_DIVISIONS:
                    division_raw = ABBREVIATED_DIVISIONS[abbrev]
                else:
                    division_raw = line_for_div
                # Reset seeding flag when we hit a new division
                in_seeding_section = False

                if inline_name:
                    # Treat inline name as implied 1st place
                    place = 1
                    entry_raw = inline_name
                    placement_already_parsed = True
                    # Fall through to player name processing below
                else:
                    # No inline name - next line might be a bare player name
                    # Set pending_division flag so next line is checked for bare name
                    pending_division = division_raw
                    continue

            else:
                # Try ordinal format first (1ST, 2ND, 3RD, 4TH, etc.)
                # Skip this if we already parsed place/entry_raw from bare name or inline format
                if not placement_already_parsed:
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
            # Handle: "ST Name" (space), "st. Name" (dot), "st) Name" (paren)
            # Also handle Spanish ordinals: 1er, 2do, 3er, 4to, 5to
            # Also handle degree/ordinal signs: °, º (from "1º Name" parsed as "1" + "º Name")
            entry_raw = re.sub(r'^(ST|ND|RD|TH|ER|DO|TO|TA|[°º])[.\s)\t]+', '', entry_raw, flags=re.IGNORECASE)

        # Strip "place"/"puesto"/"lugar" prefix (from "1st place - Name", "1er PUESTO Name", "1er LUGAR")
        entry_raw = re.sub(r'^(place|puesto|lugar)\s*[-:]?\s*', '', entry_raw, flags=re.IGNORECASE).strip()

        # Strip bare dash prefix (from "1st - Name" or "1.-Name" parsed as "- Name" or "-Name")
        entry_raw = re.sub(r'^-\s*', '', entry_raw).strip()

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
        # Pattern 8: entry starts with degree/ordinal sign noise (after stripping, only noise remains)
        # e.g., "º and 4º position match" — but NOT valid names (which had º stripped above)
        if entry_raw.startswith(('°', 'º')):
            continue  # Skip - degree-sign ordinal noise that wasn't stripped
        # Pattern 9: narrative/commentary text (section headers or match descriptions)
        if re.match(r'^(Finals|Finas|points|position)', entry_raw, re.IGNORECASE):
            continue  # Skip - narrative text, not a placement
        # Pattern 10: hotel/hostel names (French and English)
        if re.search(r'\b(hostel|auberge|hotel|hôtel|gîte|manoir)\b', entry_raw, re.IGNORECASE):
            continue  # Skip - accommodation information
        # Pattern 11: schedule/meeting keywords
        if re.search(r'\b(registration|check-in|check in|meet at)\b', entry_raw, re.IGNORECASE):
            continue  # Skip - schedule information
        # Pattern 12: narrative/descriptive text with exclamation marks
        # e.g., "golfers, great weather, crazy course!" from "10 golfers, great..."
        # But NOT legitimate entries with locations like "Name, City, Country"
        if '!' in entry_raw and not entry_raw.rstrip().endswith(':'):
            continue  # Skip - exclamatory text is not a placement

        # Pattern 13: event narrative keywords
        # e.g., "annual Summer Classic next year", "net players from 5 countries", "different states"
        # These are tournament descriptions, not placement entries
        narrative_patterns = [
            r'\b(annual|classic|championship|tournament)\b.*\b(next year|this year|coming soon|was|hosted|held)\b',  # Event narrative
            r'\bnet players\b.*\b(countries|states)\b',  # Attendee description
            r'\bdifferent states\b',  # Location description
            r'\breceived.*tournament',  # Event recap
            r'\bhighest.*ratio.*games\b',  # Tournament rules/tiebreaker
            r'\bin.*finals.*seed\b.*\bbeat\b',  # Tournament scoring description
        ]
        if any(re.search(pattern, entry_raw, re.IGNORECASE) for pattern in narrative_patterns):
            continue  # Skip - this is tournament narrative, not a placement

        # Apply event-specific parsing rules
        if use_merged_team_split:
            player1, player2, competitor_type = split_merged_team(entry_raw)
        else:
            player1, player2, competitor_type = split_entry(entry_raw)

        # Skip trick sequences (identified by " > " separator)
        # e.g., "Diving Clipper > Spinning Clipper > Spinning Paradox Dragonfly"
        # These appear in freestyle routines but should not be treated as player names
        if player1 and ' > ' in player1:
            # This looks like a trick sequence, not a name
            continue  # Skip - this is a trick list, not a placement
        if player2 and ' > ' in player2:
            # If player2 is a trick list, just treat entry as single player
            player2 = None
            competitor_type = "player"

        # Skip placements with invalid player names (noise)
        # A valid player name should have at least 2 alphanumeric characters
        if not player1 or len(player1) < 2 or not re.search(r"[a-zA-Z]{2,}", player1):
            continue  # Skip this as parsing noise

        # Skip entries that are narrative prose (not player names)
        # E.g., "square in my very first game" (from "4-square in my...")
        player1_lower = player1.lower()
        prose_indicators = [' in my ', ' i ', ' the ', ' was ', ' were ', ' said ', ' say ',
                           ' overall ', ' about ', ' would ', ' could ', ' should ']
        if any(indicator in player1_lower for indicator in prose_indicators):
            continue  # Skip as narrative text

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
        # Note: '>' and tabs are allowed in trick competitions (Sick 3, Request, Battles)
        # Format: "Player\tTrick1>Trick2>Trick3" or "Player\tScore"
        # Heuristic: If entry has tabs AND '>', it's likely a trick combo (not suspicious)
        is_trick_combo_format = '\t' in entry_raw and '>' in entry_raw

        if is_trick_combo_format:
            # Trick combo format - tabs and '>' are expected, only flag other chars
            if re.search(r"[<{}|\\]", entry_raw):
                confidence = "low"
                notes.append("suspicious characters in entry")
        else:
            # Standard format - flag unusual characters including '>'
            if re.search(r"[<>{}|\\]", entry_raw):
                confidence = "low"
                notes.append("suspicious characters in entry")

        # Normalize non-English division names (Spanish, French, etc.) to English
        division_raw = normalize_language_division(division_raw)
        # Truncate excessively long divisions (usually misidentified placements)
        # Use 55 chars to ensure canonicalized version stays under 60 char QC threshold
        division_raw = truncate_long_division(division_raw, max_length=55)
        division_canon = canonicalize_division(division_raw)
        division_category = categorize_division(division_canon, event_type)

        placements.append({
            "division_raw": normalize_whitespace(division_raw),
            "division_canon": division_canon,
            "division_category": division_category,  # net, freestyle, golf, or unknown
            "place": place,
            "competitor_type": competitor_type,
            "player1_name": normalize_whitespace(clean_player_name(player1)),
            "player2_name": normalize_whitespace(clean_player_name(player2)) if player2 else "",
            "entry_raw": normalize_whitespace(entry_raw),
            "parse_confidence": confidence,
            "notes": normalize_whitespace("; ".join(notes)) if notes else "",
        })

    # Deduplicate: same (division, place, type, player1, player2) is always an
    # extraction artifact (e.g., h2-structured + pre block both parsed, or
    # pool/overall standings repeating final results).  Keep first occurrence.
    seen_keys = set()
    deduped = []
    for p in placements:
        key = (
            p["division_canon"].lower(),
            str(p["place"]),
            p["competitor_type"],
            p["player1_name"].strip().lower(),
            (p["player2_name"] or "").strip().lower(),
        )
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(p)

    return deduped


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


def normalize_whitespace(text: str) -> str:
    """
    Normalize all whitespace in text.

    - Replaces tabs with spaces
    - Collapses multiple consecutive spaces into single space
    - Strips leading/trailing whitespace

    This is a mechanically deterministic operation that preserves
    actual content while cleaning presentation.
    """
    if not text:
        return ""
    # Replace tabs with spaces
    cleaned = text.replace('\t', ' ')
    # Collapse multiple spaces into single space
    cleaned = re.sub(r' {2,}', ' ', cleaned)
    # Strip leading/trailing whitespace
    return cleaned.strip()


def clean_date(date_raw: str) -> str:
    """Clean date field by removing iCal remnant text."""
    if not date_raw:
        return ""
    # Remove iCal UI text suffix
    cleaned = re.sub(r"\s*add this event to iCal.*$", "", date_raw, flags=re.IGNORECASE)
    return normalize_whitespace(cleaned)


def canonicalize_location(location_raw: str) -> str:
    """
    Canonicalize location by removing noise and keeping only place names.

    Removes:
    - "Site(s) TBA" prefix (very common - appears in 22% of events)
    - "TBD" prefix
    - Narrative text ("see below", "click here", etc.)
    - Venue names before location (e.g., "Golden Gate Park - San Francisco" → "San Francisco")

    Preserves:
    - City, State/Province, Country format
    - Special characters in place names (e.g., Czech: Nový Jičín)
    """
    if not location_raw:
        return ""

    cleaned = location_raw.strip()

    # Remove "Site(s) TBA" - multiple patterns
    # Prefix: "Site(s) TBA Sofia, Bulgaria" → "Sofia, Bulgaria"
    cleaned = re.sub(r'^Site\s*\(?\s*s?\s*\)?\s*TBA\s*', '', cleaned, flags=re.IGNORECASE)
    # Parenthetical: "University of Oregon (site TBA) Eugene..." → "Eugene..."
    cleaned = re.sub(r'\([^)]*\bsite\s+tba[^)]*\)\s*', '', cleaned, flags=re.IGNORECASE)
    # General TBA in parentheses
    cleaned = re.sub(r'\(\s*tba\s*\)\s*', '', cleaned, flags=re.IGNORECASE)

    # Remove "TBD" and "Location TBD" - multiple patterns
    # Prefix: "TBD Chandler, Arizona" → "Chandler, Arizona"
    cleaned = re.sub(r'^(Location\s+)?TBD\s+', '', cleaned, flags=re.IGNORECASE)
    # Inline: "Sat: TBD; Sun: Levy Pavilion..." → "Sun: Levy Pavilion..." (keep useful part)
    # Complex pattern - remove time-specific TBD parts
    cleaned = re.sub(r'\b(sat|sun|mon|tue|wed|thu|fri):\s*tbd\s*;?\s*', '', cleaned, flags=re.IGNORECASE)
    # General TBD in parentheses
    cleaned = re.sub(r'\(\s*tbd\s*\)\s*', '', cleaned, flags=re.IGNORECASE)

    # Remove narrative text - multiple patterns
    # Prefix: "See details. Salem..." → "Salem..."
    cleaned = re.sub(r'^See\s+details\.?\s*', '', cleaned, flags=re.IGNORECASE)
    # Prefix: "Check the home page for details..." → rest
    cleaned = re.sub(r'^Check\s+the\s+home\s+page\s+for\s+details\.?\s*', '', cleaned, flags=re.IGNORECASE)
    # Parenthetical: "(See details for locations) Oakland..." → "Oakland..."
    cleaned = re.sub(r'\([^)]*\bsee\s+details[^)]*\)\s*', '', cleaned, flags=re.IGNORECASE)
    # Suffix with dash: "Dallas - see below" → "Dallas"
    cleaned = re.sub(r'\s*[-–]\s*see\s+below.*$', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s*[-–]\s*click\s+here.*$', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s*[-–]\s*details?.*$', '', cleaned, flags=re.IGNORECASE)

    # Remove "to be announced" variations
    cleaned = re.sub(r'\s*\(?\s*to\s+be\s+announced\s*\)?', '', cleaned, flags=re.IGNORECASE)

    # Normalize all whitespace (tabs, multiple spaces, leading/trailing)
    return normalize_whitespace(cleaned)


def clean_results_raw(results_raw: str) -> str:
    """
    Remove ALL noise from results_raw field.

    Removes:
    - URLs (http://, https://, www., mailto:, domain references)
    - Email addresses
    - Narrative/promotional paragraphs
    - Commentary and descriptive text
    - Instructional text (click here, see below, etc.)
    - Acknowledgments and thank-you messages
    - Event descriptions and announcements

    Preserves:
    - Division headers
    - Placement entries (number + player/team names)
    - Actual results data

    Philosophy: If it's not a division header or a placement entry, it's noise.
    """
    if not results_raw:
        return ""

    lines = results_raw.split('\n')
    cleaned_lines = []

    # URL patterns to detect and remove
    url_patterns = [
        r'https?://',           # http:// or https://
        r'www\.',               # www.
        r'mailto:',             # mailto:
        r'\w+@\w+\.\w+',        # email addresses
        r'footbag\.org',        # footbag.org references
        r'\.(com|org|net|de|ch|fr|ca|uk|au|ru)\b',  # domain extensions
    ]

    # Noise phrase patterns (case-insensitive)
    # NOTE: Single-word patterns MUST use word boundaries (\b) to avoid false positives
    # e.g., r'\bcontact\b' not r'contact' (to avoid matching "Consecutive")
    noise_patterns = [
        # Promotional/descriptive
        r'check out',
        r'visit',
        r'see.*website',
        r'for more info',
        r'click here',
        r'see below',
        r'see.*full results',
        r'see.*highlights',
        r'full results.*here',

        # Acknowledgments
        r'thanks to',
        r'thank you',
        r'cheers to',
        r'congrats',
        r'congratulations',
        r'special thanks',
        r'shout.*out',

        # Sponsor/donation text
        r'\bsponsor\b',  # Word boundary to avoid matching "sponsorship", etc.
        r'\bdonate\b',   # Word boundary to avoid false matches
        r'\bprize\b',    # Word boundary to match only prize, not "prize money"
        r'without.*help',
        r'would not have',

        # Event descriptions/announcements
        r'people from far and wide',
        r'great success',
        r'biggest event',
        r'biggest party',
        r'hot news',
        r'you don.*t want to miss',
        r'inaugural',
        r'for the.*time we organise',
        r'this year.*s.*will be',

        # Instructional
        r'see.*details',
        r'check.*details',
        r'more information',
        r'\bcontact\b',   # Word boundary: avoid matching "Consecutive" which contains "contact"
        r'\bregister\b',  # Word boundary: avoid matching "Registered Competitors"
    ]

    for line in lines:
        line_stripped = line.strip()

        # Skip empty lines
        if not line_stripped:
            continue

        # Remove lines containing URLs
        has_url = any(re.search(pattern, line_stripped, re.IGNORECASE) for pattern in url_patterns)
        if has_url:
            continue

        # Remove lines that are purely narrative/noise
        # A line is noise if it:
        # 1. Contains noise phrases AND
        # 2. Doesn't look like a result entry (no leading number)
        has_noise_phrase = any(re.search(pattern, line_stripped, re.IGNORECASE) for pattern in noise_patterns)
        looks_like_result = re.match(r'^\s*\d{1,3}[.)\-:\s]', line_stripped) or re.match(r'^\s*\d{1,2}(ST|ND|RD|TH)\s', line_stripped, re.IGNORECASE)

        if has_noise_phrase and not looks_like_result:
            continue

        # Filter fake result entries: lines that START with a number (look like "1. Name")
        # but are actually narrative text (contain narrative keywords after the number)
        # Examples: "20th annual Summer Classic", "23 net players from 5 countries", "4 different states"
        if looks_like_result:
            # Extract the part after the leading number+punctuation
            text_after_number = re.sub(r'^\s*\d+(?:st|nd|rd|th|[.)\-:\s])*\s*', '', line_stripped, flags=re.IGNORECASE)

            # Check if the rest contains narrative keywords
            narrative_keywords = {
                'annual', 'classic', 'annual', 'summer', 'celebration',  # Event names
                'ratio', 'ratio of', 'games won', 'games lost',  # Stats descriptions
                'straight', 'games in',  # Tournament play descriptions
                'net players', 'freestyle players', 'countries',  # Attendee descriptions
                'different states', 'received', 'tournament t', 'sandbag',  # Event recap
                'great success', 'wonderful weather', 'great food',  # Event narrative
            }

            has_narrative_keyword = any(
                keyword in text_after_number.lower()
                for keyword in narrative_keywords
            )

            if has_narrative_keyword:
                # This is a fake result entry - skip it
                continue

        # Remove standalone HTML/markdown artifacts
        if line_stripped in ['---', '***', '===', '___', '...']:
            continue

        # Remove common section headers that are noise (not division headers)
        noise_headers = [
            'results',
            'tournament results',
            'final results',
            'event results',
            'competition results',
            'notes',
            'comments',
            'summary',
        ]
        if line_stripped.lower().strip(':').strip() in noise_headers and len(line_stripped) < 30:
            # Keep it - these might be legitimate section markers
            # But remove overly long narrative-style headers
            pass

        # Remove lines that are complete sentences (narrative paragraphs)
        # Heuristic: If a line is long (>80 chars) and contains multiple sentences, it's likely narrative
        sentence_count = line_stripped.count('. ') + line_stripped.count('! ') + line_stripped.count('? ')
        if len(line_stripped) > 80 and sentence_count >= 2:
            continue

        # Remove lines with lots of prose
        # Multiple heuristics to detect narrative text vs. results data
        words = line_stripped.split()
        if len(words) > 5:  # Only check lines with enough words
            common_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from',
                          'is', 'was', 'are', 'were', 'be', 'been', 'being', 'will', 'would', 'should', 'could', 'may',
                          'this', 'that', 'these', 'those', 'it', 'we', 'you', 'they', 'who', 'what', 'when', 'where',
                          'why', 'how', 'all', 'some', 'any', 'each', 'every', 'both', 'more', 'most', 'such', 'so', 'than',
                          'about', 'information', 'detailed', 'videos', 'results', 'event', 'please', 'here', 'out',
                          'came', 'make', 'great', 'people', 'like', 'would', 'have', 'had', 'not', 'his', 'her'}

            # Count common words
            common_count = sum(1 for w in words if w.lower().strip('.,!?;:') in common_words)

            # Also count words with multiple capital letters (likely place names: "NY", "PA", "MI")
            # These are often in event descriptions listing locations
            # Strip punctuation before checking length and case
            multi_cap_count = sum(1 for w in words if len(w.strip('.,!?;:')) == 2 and w.strip('.,!?;:').isupper())

            # If high common word density OR multiple state abbreviations, it's likely prose
            common_ratio = common_count / len(words)
            has_many_states = multi_cap_count >= 3  # 3+ state abbreviations = location list

            if (common_ratio > 0.35 or has_many_states) and not looks_like_result:
                continue

        # Remove sentence fragments that look like incomplete prose
        # E.g., "For detailed information about the" or "and some videos"
        fragment_starters = ['for', 'and', 'or', 'but', 'with', 'about', 'regarding', 'concerning', 'to', 'who', 'which']
        if len(words) > 2 and len(words) < 15 and words[0].lower() in fragment_starters:
            # This is likely a sentence fragment left over from URL removal
            continue

        # Remove lines that start with lowercase (likely continuation of previous sentence)
        # Exception: don't remove if it looks like a player name or result entry
        if words and words[0][0].islower() and not looks_like_result:
            # This is a sentence continuation fragment
            continue

        # Remove lines ending with comma (incomplete list/sentence)
        if line_stripped.endswith(',') and not looks_like_result:
            continue

        # Remove venue/sponsor description lines
        venue_words = {'provided', 'chairs', 'tables', 'carpet', 'site', 'venue', 'location',
                      'authority', 'exhibition', 'direct', 'communications', 'elements'}
        if len(words) > 4 and sum(1 for w in words if w.lower().strip('.,!?;:') in venue_words) >= 2:
            # Has 2+ venue-related words - likely venue/sponsor description
            if not looks_like_result:
                continue

        # Remove lines that are just punctuation
        if re.match(r'^[.,!?;:\-\s]+$', line_stripped):
            continue

        # Remove very short lines that are just common words (noise fragments)
        # E.g., "results", "videos", "and some", etc.
        if len(words) <= 3:
            # Check if all words are very common (not player names)
            noise_words = {'results', 'videos', 'video', 'photos', 'photo', 'images', 'image',
                          'information', 'info', 'details', 'detail', 'event', 'tournament',
                          'and', 'or', 'the', 'a', 'an', 'some', 'more', '.', '...'}
            if all(w.lower().strip('.,!?;:') in noise_words for w in words):
                continue

        # If we got here, keep the line
        cleaned_lines.append(line_stripped)

    return '\n'.join(cleaned_lines)


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

        # If all placements have Unknown division, try to infer from event name, placements, and event type
        if placements and all(p.get("division_canon") == "Unknown" for p in placements):
            inferred_div = infer_division_from_event_name(event_name, placements, event_type_for_div)
            if inferred_div:
                for p in placements:
                    p["division_raw"] = normalize_whitespace(f"[Inferred from event name: {event_name[:30]}]")
                    p["division_canon"] = inferred_div
                    p["division_category"] = categorize_division(inferred_div, event_type_for_div)
                    if p["parse_confidence"] == "medium":
                        # Keep medium if it was already medium for other reasons
                        pass
                    else:
                        p["parse_confidence"] = "medium"
                    if p["notes"]:
                        p["notes"] = normalize_whitespace(p["notes"] + "; division inferred from event name")
                    else:
                        p["notes"] = "division inferred from event name"

        # Handle known broken source events
        location = canonicalize_location(rec.get("location_raw", ""))
        date = clean_date(rec.get("date_raw", ""))
        if str(event_id) in KNOWN_BROKEN_SOURCE_EVENTS:
            if not location:
                location = BROKEN_SOURCE_MESSAGE
            if not date:
                date = BROKEN_SOURCE_MESSAGE

        # Apply location override if available (overrides take precedence over canonicalization)
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
            "event_name": normalize_whitespace(event_name),
            "date": date,
            "location": location,
            "host_club": normalize_whitespace(clean_host_club(rec.get("host_club_raw", ""))),
            "event_type": event_type,
            "results_raw": clean_results_raw(results_raw),
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

        # Check for "Site(s) TBA" noise (should be cleaned by canonicalization)
        if re.search(r'\bsite\s*\(?\s*s?\s*\)?\s*tba\b', location, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="location_has_tba",
                severity="WARN",
                event_id=str(event_id),
                field="location",
                message="location contains 'Site(s) TBA' noise (canonicalization bug)",
                example_value=location[:100],
            ))

        # Check for "TBD" noise
        if re.search(r'\btbd\b', location, re.IGNORECASE):
            issues.append(QCIssue(
                check_id="location_has_tbd",
                severity="WARN",
                event_id=str(event_id),
                field="location",
                message="location contains 'TBD' noise (canonicalization bug)",
                example_value=location[:100],
            ))

        # Check for narrative text ("see below", "click here", etc.)
        if re.search(r'\b(see\s+below|click\s+here|details?)\b', location, re.IGNORECASE):
            # Exception: "Neusiedlersee" is a German lake name, not narrative
            if 'neusiedlersee' not in location.lower():
                issues.append(QCIssue(
                    check_id="location_has_narrative",
                    severity="WARN",
                    event_id=str(event_id),
                    field="location",
                    message="location contains narrative text (should be cleaned)",
                    example_value=location[:100],
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

        # Check for player names with leading dashes or other corrupt prefixes
        player1 = p.get("player1_name", "")
        player2 = p.get("player2_name", "")
        for player_name in [player1, player2]:
            if player_name and player_name.startswith(('-', '–', '—')):
                issues.append(QCIssue(
                    check_id="cv_player_name_leading_dash",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Player name starts with dash (parsing error): {player_name[:60]}",
                    example_value=player_name[:60],
                    context={"placement_index": i}
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

    event_id = rec.get("event_id", "")
    placements = json.loads(rec.get("placements_json", "[]"))

    # Check for non-English division headers (Spanish, Portuguese, etc.)
    spanish_keywords = {
        'resultados', 'dobles', 'individuales', 'mixto', 'mixta',
        'abierto', 'abierta', 'masculino', 'femenino', 'simples'
    }
    portuguese_keywords = {
        'resultados', 'duplas', 'individuais', 'misto', 'mista',
        'aberto', 'aberta', 'masculino', 'feminino'
    }
    french_keywords = {
        'résultats', 'doubles', 'simples', 'mixte', 'ouvert', 'ouverte',
        'homme', 'femme', 'masculin', 'féminin'
    }

    for i, p in enumerate(placements):
        div_raw = p.get("division_raw", "").lower()
        if not div_raw:
            continue

        # Check for Spanish keywords
        if any(keyword in div_raw for keyword in spanish_keywords):
            issues.append(QCIssue(
                check_id="cv_division_spanish",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Division header contains Spanish text: {p.get('division_raw', '')[:60]}",
                example_value=p.get('division_raw', '')[:60],
                context={"placement_index": i, "division_raw": p.get('division_raw', '')}
            ))
        # Check for Portuguese keywords (excluding overlap with Spanish)
        elif any(keyword in div_raw for keyword in portuguese_keywords - spanish_keywords):
            issues.append(QCIssue(
                check_id="cv_division_portuguese",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Division header contains Portuguese text: {p.get('division_raw', '')[:60]}",
                example_value=p.get('division_raw', '')[:60],
                context={"placement_index": i, "division_raw": p.get('division_raw', '')}
            ))
        # Check for French keywords (excluding overlap with English)
        elif any(keyword in div_raw for keyword in french_keywords - {'doubles', 'simples', 'mixte'}):
            issues.append(QCIssue(
                check_id="cv_division_french",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Division header contains French text: {p.get('division_raw', '')[:60]}",
                example_value=p.get('division_raw', '')[:60],
                context={"placement_index": i, "division_raw": p.get('division_raw', '')}
            ))

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
            # Check if player1 looks like it might contain two names with dash separator
            # Pattern: "Name1 - Name2" where both parts look like names
            # Matches: hyphen-minus (U+002D), en-dash (U+2013), em-dash (U+2014)
            dash_pattern = re.match(r'^(.+?)\s+[-–—]\s+(.+)$', player1)
            if dash_pattern:
                part1, part2 = dash_pattern.groups()
                # Validate both parts look like names (at least 2 chars, start with capital)
                if (len(part1.strip()) >= 2 and len(part2.strip()) >= 2 and
                    part1.strip()[0].isupper() and part2.strip()[0].isupper()):
                    issues.append(QCIssue(
                        check_id="cv_doubles_dash_separator",
                        severity="WARN",
                        event_id=str(event_id),
                        field="placements_json",
                        message=f"Doubles team using dash separator instead of '/': {player1[:60]}",
                        example_value=player1[:60],
                        context={"placement_index": i, "division": div_canon}
                    ))
                    continue

            # Check if player1 looks like it might contain two names with other separators
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
# Universal String Hygiene Checks
# ------------------------------------------------------------
def check_string_hygiene(rec: dict) -> list[QCIssue]:
    """Check for string hygiene issues across all text fields."""
    issues = []
    event_id = rec.get("event_id", "")

    # Fields to check
    fields_to_check = {
        'event_name': rec.get('event_name', ''),
        'date': rec.get('date', ''),
        'location': rec.get('location', ''),
        'host_club': rec.get('host_club', '')
    }

    for field_name, value in fields_to_check.items():
        if not value:
            continue

        # Leading/trailing whitespace
        if value != value.strip():
            issues.append(QCIssue(
                check_id="string_whitespace",
                severity="WARN",
                event_id=str(event_id),
                field=field_name,
                message=f"{field_name} has leading/trailing whitespace",
                example_value=repr(value[:60]),
                context={"field": field_name}
            ))

        # Multiple consecutive spaces
        if '  ' in value:
            issues.append(QCIssue(
                check_id="string_double_space",
                severity="INFO",
                event_id=str(event_id),
                field=field_name,
                message=f"{field_name} has multiple consecutive spaces",
                example_value=value[:60],
                context={"field": field_name}
            ))

        # Control characters
        if re.search(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', value):
            issues.append(QCIssue(
                check_id="string_control_chars",
                severity="ERROR",
                event_id=str(event_id),
                field=field_name,
                message=f"{field_name} contains control characters",
                example_value=repr(value[:60]),
                context={"field": field_name}
            ))

        # Unicode replacement character or mojibake patterns
        if '\ufffd' in value or re.search(r'â€|Ã[^\s]{1,2}\s', value):
            issues.append(QCIssue(
                check_id="string_mojibake",
                severity="WARN",
                event_id=str(event_id),
                field=field_name,
                message=f"{field_name} may contain mojibake/encoding issues",
                example_value=value[:60],
                context={"field": field_name}
            ))

        # HTML remnants
        if re.search(r'<[^>]+>|&nbsp;|&amp;|&lt;|&gt;|&quot;', value):
            issues.append(QCIssue(
                check_id="string_html_remnants",
                severity="WARN",
                event_id=str(event_id),
                field=field_name,
                message=f"{field_name} contains HTML tags or entities",
                example_value=value[:60],
                context={"field": field_name}
            ))

        # URL or email leakage
        if re.search(r'https?://|www\.|[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', value):
            issues.append(QCIssue(
                check_id="string_url_email",
                severity="WARN",
                event_id=str(event_id),
                field=field_name,
                message=f"{field_name} contains URL or email address",
                example_value=value[:60],
                context={"field": field_name}
            ))

    return issues


def check_location_semantics(rec: dict) -> list[QCIssue]:
    """Check location field for semantic issues."""
    issues = []
    event_id = rec.get("event_id", "")
    location = rec.get("location", "")

    if not location:
        return issues

    # Street addresses (3+ consecutive digits)
    if re.search(r'\d{3,}', location):
        issues.append(QCIssue(
            check_id="location_has_street_address",
            severity="WARN",
            event_id=str(event_id),
            field="location",
            message="Location appears to contain street address/ZIP code",
            example_value=location[:80],
            context={"pattern": "digits"}
        ))

    # Multiple venues (semicolons)
    if ';' in location:
        issues.append(QCIssue(
            check_id="location_multiple_venues",
            severity="WARN",
            event_id=str(event_id),
            field="location",
            message="Location contains semicolon (multiple venues?)",
            example_value=location[:80],
            context={"semicolon_count": location.count(';')}
        ))

    # Parenthetical notes (often venue details that should be elsewhere)
    if '(' in location or ')' in location:
        issues.append(QCIssue(
            check_id="location_parenthetical",
            severity="INFO",
            event_id=str(event_id),
            field="location",
            message="Location contains parenthetical note",
            example_value=location[:80],
            context={}
        ))

    # TBA/TBD placeholders
    if re.search(r'\bTBA\b|\bTBD\b', location, re.IGNORECASE):
        issues.append(QCIssue(
            check_id="location_tba",
            severity="WARN",
            event_id=str(event_id),
            field="location",
            message="Location contains TBA/TBD placeholder",
            example_value=location[:80],
            context={}
        ))

    # Narrative/instruction tokens
    if re.search(r'\b(contact|details|see below|hosted by|venue|site|registration)\b', location, re.IGNORECASE):
        issues.append(QCIssue(
            check_id="location_narrative",
            severity="WARN",
            event_id=str(event_id),
            field="location",
            message="Location contains narrative/instruction text",
            example_value=location[:80],
            context={}
        ))

    # Very long location (likely narrative)
    if len(location) > 100:
        issues.append(QCIssue(
            check_id="location_too_long",
            severity="WARN",
            event_id=str(event_id),
            field="location",
            message=f"Location is very long ({len(location)} chars), may contain narrative",
            example_value=location[:80],
            context={"length": len(location)}
        ))

    return issues


def check_date_semantics(rec: dict) -> list[QCIssue]:
    """Check date field for semantic issues beyond basic parsing."""
    issues = []
    event_id = rec.get("event_id", "")
    date_str = rec.get("date", "")

    if not date_str:
        return issues

    # iCal leakage
    if re.search(r'\bical\b|\bsubscribe\b', date_str, re.IGNORECASE):
        issues.append(QCIssue(
            check_id="date_ical_leakage",
            severity="WARN",
            event_id=str(event_id),
            field="date",
            message="Date field contains iCal UI text",
            example_value=date_str[:80],
            context={}
        ))

    # Very long date (narrative schedule)
    if len(date_str) > 100:
        issues.append(QCIssue(
            check_id="date_too_long",
            severity="WARN",
            event_id=str(event_id),
            field="date",
            message=f"Date is very long ({len(date_str)} chars), may contain schedule narrative",
            example_value=date_str[:80],
            context={"length": len(date_str)}
        ))

    # Multiple semicolons (complex multi-date narrative)
    if date_str.count(';') > 2:
        issues.append(QCIssue(
            check_id="date_many_semicolons",
            severity="INFO",
            event_id=str(event_id),
            field="date",
            message="Date contains many semicolons (complex schedule?)",
            example_value=date_str[:80],
            context={"semicolon_count": date_str.count(';')}
        ))

    return issues


def check_host_club_semantics(rec: dict) -> list[QCIssue]:
    """Check host_club field for semantic issues."""
    issues = []
    event_id = rec.get("event_id", "")
    host_club = rec.get("host_club", "")

    if not host_club:
        return issues

    # Numbered list prefix (parsing artifact)
    if re.match(r'^\d+\.', host_club):
        issues.append(QCIssue(
            check_id="host_club_numbered_prefix",
            severity="WARN",
            event_id=str(event_id),
            field="host_club",
            message="Host club starts with number prefix (parsing artifact)",
            example_value=host_club[:80],
            context={}
        ))

    # Very long (narrative or location leakage)
    if len(host_club) > 80:
        issues.append(QCIssue(
            check_id="host_club_too_long",
            severity="INFO",
            event_id=str(event_id),
            field="host_club",
            message=f"Host club is very long ({len(host_club)} chars)",
            example_value=host_club[:80],
            context={"length": len(host_club)}
        ))

    return issues


def check_player_name_quality(rec: dict) -> list[QCIssue]:
    """Check player names within placements for quality issues."""
    issues = []
    event_id = rec.get("event_id", "")
    placements = json.loads(rec.get("placements_json", "[]"))

    for i, p in enumerate(placements):
        player1 = p.get("player1_name", "")
        player2 = p.get("player2_name", "")

        # Check for duplicate same player in team
        if player1 and player2 and player1 == player2:
            issues.append(QCIssue(
                check_id="player_duplicate_in_team",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Team has same player twice: {player1}",
                example_value=f"{player1} / {player2}",
                context={"placement_index": i}
            ))

        for player_name in [player1, player2]:
            if not player_name:
                continue

            # Slash in player name (should be split into team)
            # Skip if all slashes are inside parentheses (country/club info)
            # Skip if it's country codes (e.g., "Name GER/USA" or "Name DE/CH" or "Name (SUI)/(GER)")
            if '/' in player_name:
                # Check for country code pattern: 2-3 uppercase letters separated by slash
                # Matches: "GER/USA", "USA/(GER)", "(SUI)/(GER)", etc.
                is_country_code = bool(re.search(r'[A-Z]{2,3}\s*/\s*[A-Z]{2,3}|\([A-Z]{2,3}\)\s*/\s*\([A-Z]{2,3}\)', player_name))

                # Check for clear team separator with spaces: "Name / Name" or "Name and Name"
                name_no_parens = re.sub(r'\([^)]*\)', '', player_name)
                is_team_separator = ' / ' in name_no_parens or ' and ' in name_no_parens

                # Check if slash only appears inside parentheses
                is_parens_only = '/' not in name_no_parens

                if not is_country_code and not is_team_separator and not is_parens_only:
                    issues.append(QCIssue(
                        check_id="player_has_slash",
                        severity="WARN",
                        event_id=str(event_id),
                        field="placements_json",
                        message=f"Player name contains slash: {player_name[:60]}",
                        example_value=player_name[:60],
                        context={"placement_index": i}
                    ))

            # Score/numeric patterns in name (scores should be in notes)
            if re.search(r'\(\d{2,}\.\d{2}\)|\(\d{3,}\s+add', player_name, re.IGNORECASE):
                issues.append(QCIssue(
                    check_id="player_has_score",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Player name contains score: {player_name[:60]}",
                    example_value=player_name[:60],
                    context={"placement_index": i}
                ))

            # Admin commentary tokens
            if re.search(r'\b(tie|pool|seed|record|commentary|disqualif|dnf|dns)\b', player_name, re.IGNORECASE):
                # But "tie" at the start might be legitimate for ties
                if not player_name.lower().startswith('tie '):
                    issues.append(QCIssue(
                        check_id="player_has_admin_text",
                        severity="INFO",
                        event_id=str(event_id),
                        field="placements_json",
                        message=f"Player name contains admin text: {player_name[:60]}",
                        example_value=player_name[:60],
                        context={"placement_index": i}
                    ))

            # Semicolons (multiple entries or move descriptions)
            if ';' in player_name:
                issues.append(QCIssue(
                    check_id="player_has_semicolon",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Player name contains semicolon: {player_name[:60]}",
                    example_value=player_name[:60],
                    context={"placement_index": i}
                ))

            # Very long name (narrative commentary)
            if len(player_name) > 60:
                issues.append(QCIssue(
                    check_id="player_name_too_long",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Player name is very long ({len(player_name)} chars): {player_name[:60]}",
                    example_value=player_name[:60],
                    context={"placement_index": i, "length": len(player_name)}
                ))

            # Leading/trailing whitespace
            if player_name != player_name.strip():
                issues.append(QCIssue(
                    check_id="player_name_whitespace",
                    severity="WARN",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Player name has whitespace issues: {repr(player_name[:60])}",
                    example_value=repr(player_name[:60]),
                    context={"placement_index": i}
                ))

    return issues


def check_division_name_quality(rec: dict) -> list[QCIssue]:
    """Check division names for quality issues beyond language detection."""
    issues = []
    event_id = rec.get("event_id", "")
    placements = json.loads(rec.get("placements_json", "[]"))

    seen_divisions = set()
    for i, p in enumerate(placements):
        div_canon = p.get("division_canon", "")

        if not div_canon:
            continue

        seen_divisions.add(div_canon)

        # Very long division name (narrative)
        if len(div_canon) > 60:
            issues.append(QCIssue(
                check_id="division_too_long",
                severity="WARN",
                event_id=str(event_id),
                field="placements_json",
                message=f"Division name is very long ({len(div_canon)} chars): {div_canon[:60]}",
                example_value=div_canon[:60],
                context={"placement_index": i, "length": len(div_canon)}
            ))

        # Schedule time in division name (already checked elsewhere, but be comprehensive)
        if re.search(r'\d{1,2}:\d{2}\s*(am|pm)?', div_canon, re.IGNORECASE):
            # Already handled in check_placements_json, skip to avoid duplicate
            pass

        # Registration/admin text in division name
        if re.search(r'\b(registration|contact|email|click here|register|sign.?up)\b', div_canon, re.IGNORECASE):
            # Already handled in check_placements_json, skip
            pass

    return issues


def check_event_name_quality(rec: dict) -> list[QCIssue]:
    """Check event name for quality issues."""
    issues = []
    event_id = rec.get("event_id", "")
    event_name = rec.get("event_name", "")

    if not event_name:
        return issues

    # Very long event name
    if len(event_name) > 100:
        issues.append(QCIssue(
            check_id="event_name_too_long",
            severity="INFO",
            event_id=str(event_id),
            field="event_name",
            message=f"Event name is very long ({len(event_name)} chars)",
            example_value=event_name[:80],
            context={"length": len(event_name)}
        ))

    return issues


def check_year_range(rec: dict) -> list[QCIssue]:
    """Check if year is in reasonable range."""
    issues = []
    event_id = rec.get("event_id", "")
    year = rec.get("year")

    if not year:
        return issues

    # Year should be between 1980 and 2030 (footbag sport started in late 1970s)
    if year < 1980 or year > 2030:
        issues.append(QCIssue(
            check_id="year_out_of_range",
            severity="ERROR",
            event_id=str(event_id),
            field="year",
            message=f"Year {year} is outside reasonable range (1980-2030)",
            example_value=str(year),
            context={"year": year}
        ))

    return issues


def check_field_leakage(rec: dict) -> list[QCIssue]:
    """Check for field content leaking into wrong fields."""
    issues = []
    event_id = rec.get("event_id", "")
    event_name = rec.get("event_name", "")
    location = rec.get("location", "")
    host_club = rec.get("host_club", "")

    # Check if location contains event name fragments (significant overlap)
    if event_name and location:
        # Check for significant word overlap
        event_words = set(event_name.lower().split())
        location_words = set(location.lower().split())
        # Ignore common words
        common_words = {'the', 'of', 'and', 'in', 'at', 'to', 'a', 'for', 'on', 'with'}
        event_words -= common_words
        location_words -= common_words

        overlap = event_words & location_words
        # If >50% of event name words appear in location, flag it
        if event_words and len(overlap) / len(event_words) > 0.5 and len(overlap) >= 3:
            issues.append(QCIssue(
                check_id="location_contains_event_name",
                severity="INFO",
                event_id=str(event_id),
                field="location",
                message="Location may contain event name fragments",
                example_value=f"Event: {event_name[:40]} | Location: {location[:40]}",
                context={"overlap_words": list(overlap)[:5]}
            ))

    # Check if host_club contains location fragments
    if host_club and location:
        # Simple check: if location city appears in host_club
        if ',' in location:
            city = location.split(',')[0].strip()
            if len(city) > 3 and city.lower() in host_club.lower():
                issues.append(QCIssue(
                    check_id="host_club_contains_location",
                    severity="INFO",
                    event_id=str(event_id),
                    field="host_club",
                    message=f"Host club may contain location: '{city}' found in club name",
                    example_value=host_club[:60],
                    context={"city": city}
                ))

    return issues


def check_place_values(rec: dict) -> list[QCIssue]:
    """Check place values for semantic issues."""
    issues = []
    event_id = rec.get("event_id", "")
    placements = json.loads(rec.get("placements_json", "[]"))

    # Group by division to check place sequences
    by_division = defaultdict(list)
    for i, p in enumerate(placements):
        div_canon = p.get("division_canon", "Unknown")
        place = p.get("place", "")
        by_division[div_canon].append((i, place))

    for div_canon, place_list in by_division.items():
        for i, place in place_list:
            if not place:
                continue

            try:
                # Try to parse place as integer
                if isinstance(place, str):
                    # Handle "1st", "2nd", etc.
                    place_num = int(re.match(r'(\d+)', place).group(1))
                else:
                    place_num = int(place)

                # Zero or negative
                if place_num <= 0:
                    issues.append(QCIssue(
                        check_id="place_zero_or_negative",
                        severity="ERROR",
                        event_id=str(event_id),
                        field="placements_json",
                        message=f"Place is zero or negative: {place}",
                        example_value=str(place),
                        context={"placement_index": i, "division": div_canon}
                    ))

                # Huge outlier (>200 is suspicious)
                if place_num > 200:
                    issues.append(QCIssue(
                        check_id="place_huge_outlier",
                        severity="WARN",
                        event_id=str(event_id),
                        field="placements_json",
                        message=f"Place is unusually large: {place}",
                        example_value=str(place),
                        context={"placement_index": i, "division": div_canon, "place": place_num}
                    ))

            except (ValueError, AttributeError):
                # Non-numeric place
                issues.append(QCIssue(
                    check_id="place_non_numeric",
                    severity="ERROR",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Place is not numeric: {place}",
                    example_value=str(place),
                    context={"placement_index": i, "division": div_canon}
                ))

    return issues


def check_place_sequences(rec: dict) -> list[QCIssue]:
    """Check for issues in place sequences within divisions."""
    issues = []
    event_id = rec.get("event_id", "")
    placements = json.loads(rec.get("placements_json", "[]"))

    # Group by division
    by_division = defaultdict(list)
    for i, p in enumerate(placements):
        div_canon = p.get("division_canon", "Unknown")
        place = p.get("place", "")
        by_division[div_canon].append((i, place, p))

    for div_canon, place_list in by_division.items():
        if len(place_list) < 2:
            continue

        # Extract numeric places
        places_numeric = []
        for i, place, p in place_list:
            try:
                if isinstance(place, str):
                    place_num = int(re.match(r'(\d+)', place).group(1))
                else:
                    place_num = int(place)
                places_numeric.append((i, place_num, p))
            except (ValueError, AttributeError):
                pass

        if not places_numeric:
            continue

        # Sort by place
        places_numeric.sort(key=lambda x: x[1])

        # Check if first place is not 1
        if places_numeric[0][1] != 1:
            issues.append(QCIssue(
                check_id="place_does_not_start_at_1",
                severity="INFO",
                event_id=str(event_id),
                field="placements_json",
                message=f"Division '{div_canon}' places start at {places_numeric[0][1]}, not 1",
                example_value=f"{div_canon}: first place = {places_numeric[0][1]}",
                context={"division": div_canon, "first_place": places_numeric[0][1]}
            ))

        # Check for large gaps (>5) in sequence
        for j in range(1, len(places_numeric)):
            prev_place = places_numeric[j-1][1]
            curr_place = places_numeric[j][1]
            gap = curr_place - prev_place

            if gap > 5:
                issues.append(QCIssue(
                    check_id="place_large_gap",
                    severity="INFO",
                    event_id=str(event_id),
                    field="placements_json",
                    message=f"Large gap in places: {prev_place} -> {curr_place} (gap={gap})",
                    example_value=f"{div_canon}: {prev_place} -> {curr_place}",
                    context={"division": div_canon, "gap": gap, "from": prev_place, "to": curr_place}
                ))

    return issues


def check_missing_required_fields(rec: dict) -> list[QCIssue]:
    """Check for missing values in required fields that weren't caught elsewhere."""
    issues = []
    event_id = rec.get("event_id", "")

    # Date is missing (not already checked by check_date)
    if not rec.get("date"):
        issues.append(QCIssue(
            check_id="date_missing",
            severity="WARN",
            event_id=str(event_id),
            field="date",
            message="Date is missing",
            example_value="",
            context={}
        ))

    # Year is missing
    if not rec.get("year"):
        issues.append(QCIssue(
            check_id="year_missing",
            severity="WARN",
            event_id=str(event_id),
            field="year",
            message="Year is missing",
            example_value="",
            context={}
        ))

    return issues


def check_country_names(rec: dict) -> list[QCIssue]:
    """Check for non-English country names or inconsistent variants."""
    issues = []
    event_id = rec.get("event_id", "")
    location = rec.get("location", "")

    if not location:
        return issues

    # Extract last comma-separated segment (likely country)
    if ',' in location:
        country = location.split(',')[-1].strip()

        # Check for non-English country names (common ones)
        non_english_countries = {
            'Deutschland': 'Germany',
            'Österreich': 'Austria',
            'Schweiz': 'Switzerland',
            'España': 'Spain',
            'México': 'Mexico',
            'Brasil': 'Brazil',
            'Česká republika': 'Czech Republic',
            'Česko': 'Czech Republic',
            'Polska': 'Poland',
            'Italia': 'Italy'
        }

        for non_eng, eng in non_english_countries.items():
            if non_eng.lower() in country.lower():
                issues.append(QCIssue(
                    check_id="location_non_english_country",
                    severity="INFO",
                    event_id=str(event_id),
                    field="location",
                    message=f"Country name may be non-English: '{country}' (expected '{eng}'?)",
                    example_value=location[:80],
                    context={"country_segment": country, "expected": eng}
                ))

    return issues


# ------------------------------------------------------------
# Cross-Record Consistency Checks
# ------------------------------------------------------------
def check_host_club_location_consistency(records: list[dict]) -> list[QCIssue]:
    """Check if same host club appears with different locations."""
    issues = []

    # Map host_club -> set of locations
    club_to_locations = defaultdict(set)
    club_to_event_ids = defaultdict(list)

    for rec in records:
        host_club = rec.get("host_club", "")
        location = rec.get("location", "")
        event_id = rec.get("event_id", "")

        if host_club and location:
            # Normalize host club for comparison
            club_normalized = host_club.strip().lower()
            club_to_locations[club_normalized].add(location)
            club_to_event_ids[club_normalized].append((event_id, location))

    # Check for clubs with multiple different locations
    for club_norm, locations in club_to_locations.items():
        if len(locations) > 3:  # More than 3 different locations is suspicious
            # Get original club name from first event
            first_event_id, _ = club_to_event_ids[club_norm][0]
            first_rec = next((r for r in records if r.get("event_id") == first_event_id), None)
            if first_rec:
                club_name = first_rec.get("host_club", "")
                issues.append(QCIssue(
                    check_id="host_club_multiple_locations",
                    severity="INFO",
                    event_id=str(first_event_id),
                    field="host_club",
                    message=f"Host club '{club_name}' appears with {len(locations)} different locations",
                    example_value=club_name[:60],
                    context={
                        "location_count": len(locations),
                        "locations": list(locations)[:5]
                    }
                ))

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
        # Basic field validation
        all_issues.extend(check_event_id(rec))
        all_issues.extend(check_event_name(rec))
        all_issues.extend(check_event_type(rec))
        all_issues.extend(check_location(rec))
        all_issues.extend(check_date(rec))
        all_issues.extend(check_year(rec))
        all_issues.extend(check_host_club(rec))
        all_issues.extend(check_placements_json(rec))
        all_issues.extend(check_results_extraction(rec))

        # Universal string hygiene
        all_issues.extend(check_string_hygiene(rec))

        # Enhanced field quality checks
        all_issues.extend(check_event_name_quality(rec))
        all_issues.extend(check_year_range(rec))
        all_issues.extend(check_missing_required_fields(rec))

        # Semantic field checks
        all_issues.extend(check_location_semantics(rec))
        all_issues.extend(check_date_semantics(rec))
        all_issues.extend(check_host_club_semantics(rec))
        all_issues.extend(check_country_names(rec))

        # Field leakage checks
        all_issues.extend(check_field_leakage(rec))

        # Placements quality checks
        all_issues.extend(check_player_name_quality(rec))
        all_issues.extend(check_division_name_quality(rec))
        all_issues.extend(check_place_values(rec))
        all_issues.extend(check_place_sequences(rec))

        # Cross-validation checks (Stage 2 specific)
        all_issues.extend(check_expected_divisions(rec))
        all_issues.extend(check_division_quality(rec))
        all_issues.extend(check_team_splitting(rec))
        all_issues.extend(check_year_date_consistency(rec))

    # Cross-record checks
    all_issues.extend(check_event_id_uniqueness(records))
    all_issues.extend(check_worlds_per_year(records))
    all_issues.extend(check_duplicates(records))
    all_issues.extend(check_host_club_location_consistency(records))

    # Slop detection checks (comprehensive field scanning + targeted checks)
    slop_issues = run_slop_detection_checks_stage2(records)
    all_issues.extend(slop_issues)

    # Build summary
    counts_by_check = defaultdict(lambda: {"ERROR": 0, "WARN": 0, "INFO": 0})
    for issue in all_issues:
        counts_by_check[issue.check_id][issue.severity] += 1

    total_errors = sum(1 for i in all_issues if i.severity == "ERROR")
    total_warnings = sum(1 for i in all_issues if i.severity == "WARN")
    total_info = sum(1 for i in all_issues if i.severity == "INFO")

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
        "total_info": total_info,
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
        b = baseline_checks.get(check_id, {"ERROR": 0, "WARN": 0, "INFO": 0})
        c = current_checks.get(check_id, {"ERROR": 0, "WARN": 0, "INFO": 0})

        b_err, b_warn, b_info = b.get("ERROR", 0), b.get("WARN", 0), b.get("INFO", 0)
        c_err, c_warn, c_info = c.get("ERROR", 0), c.get("WARN", 0), c.get("INFO", 0)

        err_delta = c_err - b_err
        warn_delta = c_warn - b_warn
        info_delta = c_info - b_info

        if err_delta != 0 or warn_delta != 0 or info_delta != 0:
            err_sign = "+" if err_delta > 0 else ""
            warn_sign = "+" if warn_delta > 0 else ""
            info_sign = "+" if info_delta > 0 else ""
            print(f"  {check_id}:")
            if err_delta != 0:
                print(f"    ERROR: {b_err} -> {c_err} ({err_sign}{err_delta})")
            if warn_delta != 0:
                print(f"    WARN:  {b_warn} -> {c_warn} ({warn_sign}{warn_delta})")
            if info_delta != 0:
                print(f"    INFO:  {b_info} -> {c_info} ({info_sign}{info_delta})")

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
    print(f"Total info:     {summary.get('total_info', 0)}")

    print("\nField coverage:")
    for field, stats in summary.get("field_coverage", {}).items():
        print(f"  {field:15s}: {stats['present']:4d}/{stats['total']:4d} ({stats['percent']:5.1f}%)")

    print("\nIssues by check:")
    for check_id, counts in sorted(summary.get("counts_by_check", {}).items()):
        err = counts.get("ERROR", 0)
        warn = counts.get("WARN", 0)
        info = counts.get("INFO", 0)
        if err > 0:
            print(f"  {check_id}: {err} ERROR, {warn} WARN")
        elif warn > 0:
            print(f"  {check_id}: {warn} WARN")
        elif info > 0:
            print(f"  {check_id}: {info} INFO")

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

    if USE_MASTER_QC:
        # Use consolidated master QC orchestrator
        qc_summary, qc_issues = run_qc_for_stage("stage2", canonical, out_dir=out_dir)
        print_qc_summary_master(qc_summary, "stage2")

        # Delta reporting against baseline
        baseline = load_baseline_master(data_dir, "stage2")
        if baseline:
            no_regressions = print_qc_delta_master(qc_summary, baseline, "stage2")
            if not no_regressions:
                print("WARNING: QC regressions detected!")
        else:
            print("No baseline found. Run with --save-baseline to create one.")

        # Save baseline if requested
        if args.save_baseline:
            save_baseline_master(qc_summary, data_dir, "stage2")
    else:
        # Fallback to embedded QC (old behavior)
        qc_summary, qc_issues = run_qc(canonical)
        write_qc_outputs(qc_summary, qc_issues, out_dir)
        print_qc_summary(qc_summary)

        baseline = load_baseline(data_dir)
        if baseline:
            no_regressions = print_qc_delta(qc_summary, baseline)
            if not no_regressions:
                print("WARNING: QC regressions detected!")
        else:
            print("No baseline found. Run with --save-baseline to create one.")

        if args.save_baseline:
            save_baseline(qc_summary, data_dir)


if __name__ == "__main__":
    main()
