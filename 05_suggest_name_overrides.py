#!/usr/bin/env python3
"""
05_suggest_name_overrides.py

Generate *suggested* name-spelling overrides using OLD_RESULTS
as SECONDARY evidence only.

NO changes to canonical data are made.
"""

from __future__ import annotations
import csv
import json
import re
import difflib
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# ---- CONFIG ----
OLD_PLACEMENTS = Path("out/secondary_evidence/old_results__placements_raw.csv")
CANONICAL_EVENTS = Path("out/stage2_canonical_events.csv")
OUTFILE = Path("out/secondary_evidence/old_results__name_suggestions.csv")

# Minimum similarity ratio (0.0-1.0) for suggesting a name match.
# Lower values = more suggestions (but more false positives).
# 0.60 chosen to catch more potential matches while filtering obvious false positives.
SIMILARITY_THRESHOLD = 0.60
# ----------------


def norm(s: str) -> str:
    """Normalize string for comparison: lowercase, alphanumeric + spaces only."""
    normalized = "".join(c.lower() for c in s if c.isalnum() or c == " ").strip()
    # Collapse multiple spaces to single space for consistency
    return " ".join(normalized.split())


def load_primary_players(path: Path) -> List[Dict]:
    """Extract unique player names from canonical events CSV (from placements_json)."""
    players_seen = set()  # Track normalized names to avoid duplicates
    players = []
    
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            placements_json = row.get("placements_json", "")
            if not placements_json:
                continue
            
            try:
                placements = json.loads(placements_json)
            except json.JSONDecodeError:
                continue
            
            for placement in placements:
                # Extract player1_name and player2_name
                for player_field in ["player1_name", "player2_name"]:
                    name = placement.get(player_field, "").strip()
                    if not name:
                        continue
                    
                    normalized = norm(name)
                    if normalized and normalized not in players_seen:
                        players_seen.add(normalized)
                        players.append({
                            "name": name,
                            "_norm": normalized,
                        })
    
    return players


def is_parsing_artifact(name: str) -> bool:
    """Detect if a name looks like a parsing artifact (multiple players, control chars, etc.)."""
    # Check for control characters (replacement characters, etc.)
    if re.search(r'[\x00-\x08\x0B\x0C\x0E-\x1F\uFFFD]', name):
        return True
    
    # Check for place indicators (2nd, 3rd, etc.) - suggests multiple players
    if re.search(r'\b\d+(st|nd|rd|th)\b', name, re.IGNORECASE):
        return True
    
    # Check for multiple names separated by commas with place indicators
    # e.g., "Ken Shults, 2nd - Jim Caveney, 3rd - Bruce Guettich"
    if ',' in name and re.search(r'\d+(st|nd|rd|th)', name, re.IGNORECASE):
        return True
    
    # Check for multiple names separated by dashes with place indicators
    # e.g., "Name1, 2nd - Name2, 3rd - Name3"
    if re.search(r',\s*\d+(st|nd|rd|th)\s*[-–—]', name, re.IGNORECASE):
        return True
    
    # Check for multiple capitalized words that look like multiple names
    # (more than 4 words suggests it might be multiple people)
    words = name.split()
    if len(words) > 4:
        # Count capitalized words (likely names)
        capitalized = sum(1 for w in words if w and w[0].isupper())
        if capitalized > 3:
            return True
    
    return False


def best_match(name: str, players: List[Dict]) -> Tuple[Optional[Dict], float]:
    """Find the best matching player name using fuzzy string matching."""
    n = norm(name)
    if not n:
        return None, 0.0
    
    best = None
    best_score = 0.0
    for p in players:
        score = difflib.SequenceMatcher(None, n, p["_norm"]).ratio()
        if score > best_score:
            best_score = score
            best = p
    return best, best_score


def main():
    if not OLD_PLACEMENTS.exists():
        raise FileNotFoundError(OLD_PLACEMENTS)
    if not CANONICAL_EVENTS.exists():
        raise FileNotFoundError(CANONICAL_EVENTS)

    print(f"Loading primary players from {CANONICAL_EVENTS}...")
    primary_players = load_primary_players(CANONICAL_EVENTS)
    print(f"Found {len(primary_players)} unique primary players")

    suggestions = []
    # Track seen suggestions to avoid duplicates (same old_name -> primary_name)
    seen = set()

    with open(OLD_PLACEMENTS, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            old_name_raw = row.get("competitor_raw", "").strip()
            if not old_name_raw:
                continue
            
            if "/" in old_name_raw:
                continue  # skip teams; individuals only
            
            # Skip parsing artifacts
            if is_parsing_artifact(old_name_raw):
                continue

            best, score = best_match(old_name_raw, primary_players)
            if best and score >= SIMILARITY_THRESHOLD:
                primary_name = best.get("name", "")
                if not primary_name:
                    continue
                
                # Skip if primary name is also a parsing artifact
                if is_parsing_artifact(primary_name):
                    continue
                
                # Only suggest if normalized names differ
                if norm(primary_name) != norm(old_name_raw):
                    # Filter out likely false positives:
                    # - Single-letter initials (e.g., "A Smith" vs "Max Smith")
                    old_parts = old_name_raw.split()
                    primary_parts = primary_name.split()
                    if old_parts and primary_parts:
                        old_first = old_parts[0].strip()
                        primary_first = primary_parts[0].strip()
                        # Skip if one is a single letter and the other isn't
                        if (len(old_first) == 1 and len(primary_first) > 1) or \
                           (len(primary_first) == 1 and len(old_first) > 1):
                            continue
                    
                    # Deduplicate: same old_name -> primary_name pair
                    suggestion_key = (old_name_raw.lower(), primary_name.lower())
                    if suggestion_key not in seen:
                        seen.add(suggestion_key)
                        suggestions.append({
                            "old_name": old_name_raw,
                            "primary_name": primary_name,
                            "confidence": round(score, 3),
                            "reason": "fuzzy-name-match",
                            "secondary_event_key": row.get("sec_event_key", ""),
                        })

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTFILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "old_name",
                "primary_name",
                "confidence",
                "reason",
                "secondary_event_key",
            ],
        )
        w.writeheader()
        for s in sorted(suggestions, key=lambda x: (-x["confidence"], x["old_name"])):
            w.writerow(s)

    print(f"Wrote {OUTFILE} ({len(suggestions)} unique suggestions)")


if __name__ == "__main__":
    main()
