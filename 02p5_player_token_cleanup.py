#!/usr/bin/env python3
"""
Stage 2.5 (player token cleanup)

Reads:
  out/stage2_players.csv

Optionally reads (for usage_count / source_hint):
  out/stage2_canonical_events.csv   (or any CSV with player_id columns)

Writes:
  out/stage2p5_players_clean.csv
  out/stage2p5_player_alias_edges.csv
  out/stage2p5_qc_summary.json

Design goals:
- Do NOT change player_id values.
- Keep raw truth (player_name_raw, country_observed) and add derived fields.
- Be conservative: flag > delete. "junk" rows remain present for audit.
- Deterministic output: sorted, stable keys.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import ftfy
import pandas as pd

_RE_LEADING_ORDINAL = re.compile(r"^\s*(?:\d+|1st|2nd|3rd|[4-9]th)\b", re.IGNORECASE)

_WS = re.compile(r"\s+")

# Drop-in helper (02p5): junk person name detection for Placements_ByPerson filtering
_RE_JUNK_PERSON = re.compile(
    r"""(
        [\\/]|            # slash or backslash (two names / team-ish)
        ,|                # comma (often location fragments)
        \)|               # stray closing paren
        \b(results?|result|partners|seed|place)\b  # headings / non-people
    )""",
    re.IGNORECASE | re.VERBOSE,
)

def looks_like_junk_person_name(s: str) -> bool:
    if not isinstance(s, str):
        return False
    t = s.strip()
    if not t:
        return False
    return bool(_RE_JUNK_PERSON.search(t))

_RE_TRAILING_PAREN = re.compile(r"\s*\([^)]*$")         # matches " (Phoenix" or " (Austria)"
_RE_TRAILING_STAR = re.compile(r"\s*\*+$")
_RE_ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")  # ZWSP/ZWNJ/ZWJ/WJ/BOM

# Display canon sanitizer: strip trailing contamination from alias-assigned person names only
_RE_CANON_CUT_DASH = re.compile(r"\s-\s")                 # "Name - stuff"
_RE_CANON_PARENS = re.compile(r"\s*\(.*?\)\s*$")          # trailing "(...)" notes
_RE_TRAIL_NUMS = re.compile(r"(?:\s+\d+(?:,\s*\d+)+,?\s*$)")  # " 24, 57, 12," at end
_RE_QUOTED_NICK = re.compile(r'\s+"[^"]+"\s*')            # strip "Kenny" from Kenneth "Kenny" Shults


def clean_person_canon_for_output(s: str) -> str:
    """Correctness-first display canon: strip obvious trailing contamination from an already-assigned person."""
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""

    # strip quoted nicknames (per your decision)
    s = _RE_QUOTED_NICK.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # remove trailing numeric payloads like "24, 57, 12,"
    s = _RE_TRAIL_NUMS.sub("", s).strip()

    # remove trailing parens notes
    s = _RE_CANON_PARENS.sub("", s).strip()

    # cut at first " - " (common: tricks / club / location notes)
    m = _RE_CANON_CUT_DASH.search(s)
    if m:
        s = s[:m.start()].strip()

    return re.sub(r"\s+", " ", s).strip()


# Safe trailing annotation stripping (for A-name fallback only: nicknames, " - metadata")
_RE_TRAIL_PARENS = re.compile(r"\s*\([^)]*\)\s*")       # remove (...) tokens
_RE_TRAIL_DASH_META = re.compile(r"\s-\s[^-]+$")        # strip one trailing " - metadata" chunk


def strip_safe_trailing_annotations(s: str) -> str:
    s = "" if s is None else str(s).strip()
    if not s:
        return s
    # remove nicknames like (Tree), (PT)
    s2 = _RE_TRAIL_PARENS.sub(" ", s)
    s2 = re.sub(r"\s+", " ", s2).strip()
    # strip trailing dash metadata like " - Team84" / " - FC Footstar Berlin"
    s2 = _RE_TRAIL_DASH_META.sub("", s2).strip()
    return re.sub(r"\s+", " ", s2).strip()


def strip_invisible(s: str) -> str:
    s = (s or "")
    s = _RE_ZERO_WIDTH.sub("", s)
    # also remove soft hyphen (often invisible)
    s = s.replace("\u00ad", "")
    return s


# QC03 minimal deterministic guardrail: blank non-presentable metadata, safe '+' split, final separator check
_RE_BAD_SEP = re.compile(r"[+=\\|]")


def _is_presentable_person_name(s: str) -> bool:
    """
    Strict 'presentable human name' gate:
    - at least 2 tokens (first+last)
    - only letters, spaces, hyphen, apostrophe, period
    - no digits
    """
    if not s:
        return False
    t = " ".join(s.split())
    if any(ch.isdigit() for ch in t):
        return False
    if _RE_BAD_SEP.search(t):
        return False
    # allow unicode letters
    if not re.fullmatch(r"[^\W\d_]+(?:[ \-'.][^\W\d_]+)+", t, flags=re.UNICODE):
        return False
    return True


def _qc03_guardrail(p1: str, p2: str, team: str) -> tuple[str, str, str]:
    # 1) '=' is always non-presentable metadata → blank
    if p1 and "=" in p1:
        p1 = ""
    if p2 and "=" in p2:
        p2 = ""
    if team and "=" in team:
        team = ""

    # 2) Safe '+' split only when p2 empty and BOTH sides presentable
    if (not p2) and p1 and ("+" in p1):
        parts = [x.strip() for x in p1.split("+")]
        if len(parts) == 2 and _is_presentable_person_name(parts[0]) and _is_presentable_person_name(parts[1]):
            p1, p2 = parts[0], parts[1]
            team = f"{p1} / {p2}"
        else:
            p1 = ""
            p2 = ""
            team = ""

    # 3) Final separator guardrail for QC03
    if p1 and _RE_BAD_SEP.search(p1):
        p1 = ""
    if p2 and _RE_BAD_SEP.search(p2):
        p2 = ""
    if team and _RE_BAD_SEP.search(team):
        team = ""

    return p1, p2, team


def split_trailing_paren_noise(s: str) -> tuple[str, str]:
    # If there is an unmatched "(" at end, treat it as noise
    m = _RE_TRAILING_PAREN.search(s)
    if not m:
        return s, ""
    clean = s[:m.start()].strip()
    noise = s[m.start():].strip()
    return clean, noise


def split_trailing_star_noise(s: str) -> tuple[str, str]:
    m = _RE_TRAILING_STAR.search(s)
    if not m:
        return s, ""
    clean = s[:m.start()].strip()
    noise = s[m.start():].strip()
    return clean, noise


_RE_NEEDS_CP1252_UTF8_REPAIR = re.compile(r"[¶¦±]")


def repair_cp1252_utf8_mojibake(s: str) -> str:
    """
    Repairs text where UTF-8 bytes were mis-decoded as cp1252/latin1,
    producing characters like ¶ ¦ ±.
    Deterministic. Only apply when markers are present.
    """
    if not isinstance(s, str):
        return s
    if not _RE_NEEDS_CP1252_UTF8_REPAIR.search(s):
        return s
    try:
        return s.encode("cp1252", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        # correctness-first: if repair fails, keep original
        return s


def fix_cp1250_mojibake_if_detected(s: str) -> tuple[str, bool]:
    """
    Repair cp1250/latin2 bytes that were mis-decoded as latin1.
    Example: "Pawe³" -> "Paweł", "Mo¶ciszewski" -> "Mościszewski".
    Returns (fixed_string, was_fixed).
    """
    if not s:
        return s, False

    # Typical latin1 glyphs that often indicate cp1250 bytes interpreted as latin1
    # (seen in your Top unmapped: ³ ¶ ¿ ± ê ¦ etc.)
    markers = ("³", "¶", "¿", "±", "ê", "Ê", "Ð", "ð", "Ñ", "ñ", "­", "¦")  # include soft hyphen, broken bar (Ś)
    if not any(m in s for m in markers):
        return s, False

    # Soft hyphen frequently appears as invisible junk inside names; drop it early
    s2 = s.replace("\u00ad", "")

    try:
        repaired = s2.encode("latin1").decode("cp1250")
    except Exception:
        return s, False

    # Accept only if we actually improved (changed) and result is printable-ish
    if repaired == s:
        return s, False
    if not repaired.isprintable():
        return s, False
    return repaired, True


def fix_mojibake_if_detected(s: str) -> tuple[str, bool]:
    """
    Attempt a latin1→utf8 repair ONLY if mojibake markers are present.
    Returns (fixed_string, was_fixed).
    """
    if not s:
        return s, False

    # Common mojibake markers from UTF-8 mis-decoding
    markers = ("Ã", "Â", "Å", "Ð", "Ñ", "Ä", "Ã¤", "Ã©", "Ã³")
    if not any(m in s for m in markers):
        return s, False

    try:
        repaired = s.encode("latin1").decode("utf-8")
    except Exception:
        return s, False

    # Accept only if markers are gone and result is printable
    if any(m in repaired for m in markers):
        return s, False

    if not repaired.isprintable():
        return s, False

    return repaired, True


_RE_NAME_POLLUTION = re.compile(
    r"""
    (?:
        \s*:\s*.*$              # colon annotations  "Name: blah"
      | \s*\$\s*\d+.*$          # money             "Name $275"
      | \s+\d+(?:\.\d+)?\s*$    # trailing number   "Name 56.9"
    )
    """,
    re.VERBOSE,
)

_RE_LEADING_NUMBERED_FRAGMENT = re.compile(r"^\s*\d+\.\s+")  # "2. Kenny Shults"
_RE_EMBEDDED_RANK = re.compile(r"\s+\d+\.\s+")  # "Chris Gator Routh 2. Kenny Shults"

# Name decontamination (Tier-1): safe stripping before alias lookup. No guessing; does not merge people.
_RE_LEADING_RANK_DIGITS = re.compile(r"^\s*\d+\b\s*")  # Rule A: "794 Scott Davidson" -> "Scott Davidson"
_RE_TRAILING_DASH_LOCATION = re.compile(
    r"\s*[-–]\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*$"
)  # Rule B: "Scott Davidson- Chicago" / "Scott Davidson-Chicago"
# Rule C: "Scott Davidson (IL) - ??" → "Scott Davidson". Requires First Last + (XX/XXX) + dash + junk.
_RE_NAME_TWO_TOKENS_THEN_STATE_DASH_JUNK = re.compile(
    r"^([A-Za-z][^\W\d_]+)\s+([A-Za-z][^\W\d_]+)\s*\([A-Z]{2,3}\)\s*[-–—]\s*.*$"
)
_JUNK_TAIL_TOKENS = frozenset({"prize", "prizes"})  # Rule C: obvious non-name trailing tokens

# "Scott Davidson (US) 8 9 6 9 10 9 7 8 7 7 9" → "Scott Davidson" (First Last + (XX/XXX) + score digits)
_RE_COUNTRY_SCORE_TAIL = re.compile(
    r"""^
    ([A-Za-z][^\W\d_]+)\s+([A-Za-z][^\W\d_]+)      # First Last
    \s*\([A-Z]{2,3}\)                              # (US) / (GER) / etc
    \s+[0-9][0-9\s,.\-]*$                          # numbers / spaces / punctuation to end
    """,
    re.VERBOSE,
)


def strip_country_score_tail(name: str) -> str:
    s = (name or "").strip()
    m = _RE_COUNTRY_SCORE_TAIL.match(s)
    if not m:
        return s
    return f"{m.group(1)} {m.group(2)}"


def decontaminate_name_for_alias_lookup(s: str) -> str:
    """
    Safe decontamination before alias/person-id mapping.
    Rules A/B/C only; does not touch scorelines like "Name (US) 8 9 6".
    Returns decontaminated string; does not merge different people.
    """
    if not s:
        return s
    t = str(s).strip()
    if not t:
        return t

    # Rule A: strip leading numeric rank tokens
    t = _RE_LEADING_RANK_DIGITS.sub("", t).strip()

    # Rule B: strip trailing "- City/Location" (apply until stable)
    while True:
        m = _RE_TRAILING_DASH_LOCATION.search(t)
        if not m:
            break
        t = t[: m.start()].strip()

    # Rule C: "Scott Davidson (IL) - ??" → "Scott Davidson" (only when First Last + (XX/XXX) + dash + junk)
    m = _RE_NAME_TWO_TOKENS_THEN_STATE_DASH_JUNK.match(t)
    if m:
        t = f"{m.group(1)} {m.group(2)}"

    # Rule C: strip known junk tail tokens
    while t:
        parts = t.rsplit(maxsplit=1)
        if len(parts) < 2:
            break
        last = parts[-1].lower()
        if last in _JUNK_TAIL_TOKENS:
            t = parts[0].rstrip()
        else:
            break

    # Rule D: "Scott Davidson (US) 8 9 6 9 10 9 7 8 7 7 9" → "Scott Davidson"
    t = strip_country_score_tail(t)

    return _WS.sub(" ", t).strip()


def split_name_pollution(raw: str) -> tuple[str, str, bool]:
    """
    Returns (clean_name, noise, changed).
    Only strips very high-precision junk patterns.
    """
    if not raw:
        return raw, "", False

    s = raw.strip()
    if not s:
        return s, "", False

    noise_parts = []

    # If line starts with "2. " etc, capture it as noise (don't guess)
    m_lead = _RE_LEADING_NUMBERED_FRAGMENT.match(s)
    if m_lead:
        noise_parts.append(m_lead.group(0).strip())
        s = _RE_LEADING_NUMBERED_FRAGMENT.sub("", s).strip()

    # NEW: embedded rank list fragments: "Chris Gator Routh 2. Kenny Shults"
    # Split at the first occurrence of " <digits>. "
    m_embedded = _RE_EMBEDDED_RANK.search(s)
    if m_embedded:
        left = s[:m_embedded.start()].strip()
        right = s[m_embedded.start():].strip()
        # If left is non-empty, treat right as pollution/noise.
        if left:
            # Normalize common junk prefixes before returning
            left = re.sub(r'^(and|=)\s+', '', left, flags=re.IGNORECASE).strip()
            if left:  # Only split if left is still non-empty after normalization
                noise_parts.append(right)
                s = left

    m = _RE_NAME_POLLUTION.search(s)
    if m:
        noise_parts.append(s[m.start():].strip())
        s = s[:m.start()].strip()

    noise = " | ".join([p for p in noise_parts if p])
    changed = (s != raw.strip()) or bool(noise)

    return s, noise, changed


def _one_line(s: str) -> str:
    return _WS.sub(" ", (s or "").replace("\r", " ").replace("\n", " ").replace("\t", " ")).strip()


def team_display_name(p1: str, p2: str) -> str:
    p1 = (p1 or "").strip()
    p2 = (p2 or "").strip()
    return f"{p1} / {p2}".strip(" /") if p2 else p1


def load_stage2_canonical_events_records(events_csv_path: Path) -> list[dict]:
    df = pd.read_csv(events_csv_path)
    records: list[dict] = []
    for _, row in df.iterrows():
        r = row.to_dict()

        # normalize year (some files have floats)
        y = r.get("year", None)
        if pd.isna(y):
            r["year"] = None
        else:
            try:
                r["year"] = int(float(y))
            except Exception:
                r["year"] = None

        placements_json = r.get("placements_json", "[]")
        try:
            r["placements"] = json.loads(placements_json) if isinstance(placements_json, str) else []
        except Exception:
            r["placements"] = []

        records.append(r)
    return records


def build_players_by_id(players_clean_df: pd.DataFrame) -> dict:
    players_by_id = {}
    if players_clean_df is None or players_clean_df.empty:
        return players_by_id
    for _, r in players_clean_df.iterrows():
        pid = str(r.get("player_id") or "").strip()
        if pid:
            players_by_id[pid] = dict(r)
    return players_by_id


def build_placements_flat_df(records: list[dict], players_by_id: dict, out_dir: Path) -> tuple[pd.DataFrame, list[dict]]:
    rows = []
    audit_rows = []
    for rec in records:
        eid = str(rec.get("event_id") or "").strip()
        year = rec.get("year")
        placements = rec.get("placements", []) or []
        for p in placements:
            div_canon = (p.get("division_canon") or "").strip()
            div_raw = (p.get("division_raw") or "").strip()
            div_cat = (p.get("division_category") or "unknown") or "unknown"
            competitor_type = (p.get("competitor_type") or "").strip()
            place = p.get("place", "")

            def _raw_clean_noise(pid: str, raw: str) -> tuple[str, str, str]:
                """Returns (name_clean, name_noise). Uses canonical player_name_clean when pid in players_by_id."""
                raw = repair_name(raw)
                raw = strip_invisible(raw)
                raw_cp, cp_fixed = fix_cp1250_mojibake_if_detected(raw)
                if cp_fixed:
                    raw = raw_cp
                raw_utf8, utf8_fixed = fix_mojibake_if_detected(raw)
                if utf8_fixed:
                    raw = raw_utf8
                raw, noise1 = split_trailing_star_noise(raw)
                raw, noise2 = split_trailing_paren_noise(raw)
                suffix_noise = " ".join(x for x in [noise1, noise2] if x).strip()
                clean_part, noise, _ = split_name_pollution(raw)
                clean_part = decontaminate_name_for_alias_lookup(clean_part)
                if suffix_noise:
                    noise = " ".join(x for x in [noise, suffix_noise] if x).strip()
                if pid and pid in players_by_id:
                    canonical = str(players_by_id[pid].get("player_name_clean") or "").strip()
                    canonical = repair_name(canonical)  # IMPORTANT: repair mojibake in canonical too
                    canonical = strip_invisible(canonical)
                    canonical_cp, cp_fixed = fix_cp1250_mojibake_if_detected(canonical)
                    if cp_fixed:
                        canonical = canonical_cp
                    canonical_utf8, utf8_fixed = fix_mojibake_if_detected(canonical)
                    if utf8_fixed:
                        canonical = canonical_utf8
                    canonical = decontaminate_name_for_alias_lookup(canonical)  # Scott Davidson- Chicago → Scott Davidson
                    name_clean = canonical or clean_part
                else:
                    name_clean = clean_part
                return name_clean, noise

            player1_id = str(p.get("player1_id") or p.get("player_id") or p.get("player1_player_id") or "").strip()
            player2_id = str(p.get("player2_id") or p.get("player2_player_id") or "").strip()

            player1_raw = repair_name(str(p.get("player1_name") or p.get("player_name") or "")).strip()
            player1_name_clean, player1_name_noise = _raw_clean_noise(player1_id, player1_raw)
            player1_name = player1_name_clean

            player2_raw = repair_name(str(p.get("player2_name") or "")).strip()
            player2_name_clean, player2_name_noise = _raw_clean_noise(player2_id, player2_raw)
            player2_name = player2_name_clean

            # Patch 1 — 02p5: split plus-packed doubles where safe
            team_disp = None  # Will be set below
            if player2_name == "" and player1_name and "+" in player1_name:
                parts = [part.strip() for part in player1_name.split("+", 1)]
                if len(parts) == 2:
                    part1, part2 = parts[0], parts[1]
                    # Check if both parts are presentable (look like person names and not junk)
                    part1_presentable = (
                        part1 and 
                        looks_like_person_name(part1) and 
                        not looks_like_junk_person_name(part1)
                    )
                    part2_presentable = (
                        part2 and 
                        looks_like_person_name(part2) and 
                        not looks_like_junk_person_name(part2)
                    )
                    
                    if part1_presentable and part2_presentable:
                        # Both parts presentable → assign into player1/player2 and blank team_display_name
                        player1_name = part1
                        player2_name = part2
                        team_disp = ""  # Blank team_display_name when split succeeds
                    else:
                        # Not presentable → blank player1_name and team_display_name (and emit audit)
                        audit_rows.append({
                            "event_id": eid,
                            "year": year if year is not None else "",
                            "division_canon": _one_line(div_canon),
                            "division_raw": _one_line(div_raw),
                            "player1_name_original": _one_line(player1_name),
                            "player1_name_part1": _one_line(part1),
                            "player1_name_part2": _one_line(part2),
                            "part1_presentable": part1_presentable,
                            "part2_presentable": part2_presentable,
                            "reason": "plus_split_failed_presentability_check",
                        })
                        player1_name = ""
                        team_disp = ""  # Blank team_display_name when split fails

            if team_disp is None:
                team_disp = team_display_name(player1_name, player2_name) if player2_name else player1_name

            rows.append({
                "event_id": eid,
                "year": year if year is not None else "",
                "division_canon": _one_line(div_canon),
                "division_raw": _one_line(div_raw),
                "division_category": div_cat,
                "competitor_type": competitor_type,
                "place": place,
                "player1_id": _one_line(player1_id),
                "player1_name_raw": _one_line(player1_raw),
                "player1_name_clean": _one_line(player1_name_clean),
                "player1_name_noise": _one_line(player1_name_noise),
                "player1_name": _one_line(player1_name),
                "player2_id": _one_line(player2_id),
                "player2_name_raw": _one_line(player2_raw),
                "player2_name_clean": _one_line(player2_name_clean),
                "player2_name_noise": _one_line(player2_name_noise),
                "player2_name": _one_line(player2_name),
                "team_display_name": _one_line(team_disp),
            })

    df = pd.DataFrame(rows)

    # ------------------------------------------------------------
    # PATCH: Singles parse contamination handling (NO GUESSING)
    # ------------------------------------------------------------
    # Problem addressed:
    # - Singles rows incorrectly classified as teams because:
    #   * player2_name contains location/noise, OR
    #   * rank tokens embedded in player1_name ("2. Kenny Shults")
    #
    # Strategy:
    # - Safe fix ONLY when player2_name is clearly location/noise
    # - Otherwise quarantine for human review

    import re

    _RE_EMBEDDED_RANK = re.compile(r"\b\d+\.\s+[A-Z]")

    US_CA_REGIONS = {
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
        "MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK",
        "OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY",
        "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT",
    }

    def _is_singles_division(row) -> bool:
        for col in ("division_category", "division_canon", "division_raw"):
            v = str(row.get(col, "") or "")
            if "single" in v.lower():
                return True
        return False

    def _looks_like_location_or_noise(s: str) -> bool:
        if not s:
            return False
        t = s.strip()
        if "," in t:
            return True
        if any(ch.isdigit() for ch in t):
            return True
        toks = re.findall(r"\b[A-Z]{2}\b", t.upper())
        if any(tok in US_CA_REGIONS for tok in toks):
            return True
        return False

    def _has_embedded_rank(row) -> bool:
        return bool(
            _RE_EMBEDDED_RANK.search(str(row.get("player1_name", ""))) or
            _RE_EMBEDDED_RANK.search(str(row.get("player2_name", "")))
        )

    if not df.empty:
        is_singles = df.apply(_is_singles_division, axis=1)
        has_p2 = df["player2_name"].astype(str).str.strip() != ""
        p2_is_noise = df["player2_name"].astype(str).map(_looks_like_location_or_noise)
        embedded_rank = df.apply(_has_embedded_rank, axis=1)

        # HARD GATE 1: ALL Singles with player2 filled → clear player2, force competitor_type='player'
        # Singles divisions should never have a player2. Any player2 data is noise (location, club, etc.)
        singles_with_p2 = is_singles & has_p2

        if "player2_name_noise" not in df.columns:
            df["player2_name_noise"] = ""

        df.loc[singles_with_p2, "player2_name_noise"] = df.loc[singles_with_p2, "player2_name"]
        df.loc[singles_with_p2, "player2_name"] = ""
        df.loc[singles_with_p2, "player2_id"] = ""
        df.loc[singles_with_p2, "competitor_type"] = "player"
        df.loc[singles_with_p2, "team_display_name"] = ""

        # HARD GATE 2: Non-singles with competitor_type='player' but player2 filled → force team
        # If it's not a Singles division and player2 is present, this is a team entry.
        non_singles_player_with_p2 = (~is_singles) & (df["competitor_type"] == "player") & has_p2
        df.loc[non_singles_player_with_p2, "competitor_type"] = "team"

        # QUARANTINE: only embedded rank contamination (different issue)
        quarantine_mask = embedded_rank

        quarantine_df = df.loc[quarantine_mask, [
            "event_id","year","division_category","division_canon","division_raw","place",
            "competitor_type","player1_name","player2_name","team_display_name",
            "player1_name_noise","player2_name_noise"
        ]].copy()

        quarantine_df["quarantine_reason"] = ""
        quarantine_df.loc[embedded_rank, "quarantine_reason"] += "embedded_rank_token;"

        quarantine_path = out_dir / "Placements_ByPerson_SinglesQuarantine.csv"
        quarantine_df.to_csv(quarantine_path, index=False)

        # Remove quarantined rows from analytics surface
        df = df.loc[~quarantine_mask].copy()

    if not df.empty:
        def _place_sort(x):
            try:
                return int(x)
            except Exception:
                return 999999
        df["_place_sort"] = df["place"].apply(_place_sort)
        df.sort_values(
            by=["year", "event_id", "division_canon", "division_raw", "_place_sort", "team_display_name"],
            ascending=[True, True, True, True, True, True],
            inplace=True,
        )
        df.drop(columns=["_place_sort"], inplace=True)
    return df, audit_rows


def _norm_alias(s: str) -> str:
    return " ".join((s or "").strip().split()).casefold()


# Deterministic namespace for auto person_id creation (A-names only).
# Do NOT change once published, or IDs will change.
_PERSON_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "footbag-results.person.v1")

def _is_name_char_ok(ch: str) -> bool:
    # allow any Unicode letters + space + hyphen + apostrophe
    if ch.isalpha():
        return True
    if ch in {" ", "-", "'"}:
        return True
    return False


def _norm_person_key(s: str) -> str:
    # Stable key for UUID derivation (case/whitespace insensitive)
    return " ".join((s or "").strip().split()).casefold()


def is_clean_canonical_person_name(name: str) -> bool:
    """
    Safe A-name heuristic:
      - 2+ tokens
      - only letters (Unicode) + space + apostrophe + hyphen
      - no digits / weird punctuation
    """
    n = " ".join((name or "").strip().split())
    if not n:
        return False
    if len(n.split()) < 2:
        return False
    # correctness-first: " - " is a common contamination separator ("Name - tricks/notes/club/location")
    if _RE_CANON_CUT_DASH.search(n):
        return False
    return all(_is_name_char_ok(ch) for ch in n)


def repair_name(name: str) -> str:
    """Unicode/mojibake repair before person mapping (encoding fix, not guessing)."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    return ftfy.fix_text(str(name))


def repair_mojibake_for_column(s) -> str:
    """Full mojibake pipeline for name columns (ftfy + cp1250 + utf8). Run twice for cascaded damage."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s)
    for _ in range(2):  # two passes for cascaded/double-encoded mojibake
        s = repair_name(s)
        fixed, ok = fix_cp1250_mojibake_if_detected(s)
        if ok:
            s = fixed
        fixed, ok = fix_mojibake_if_detected(s)
        if ok:
            s = fixed
    return s


def _repair_name_cols_for_person_resolution(df: pd.DataFrame) -> None:
    """Repair mojibake in columns that feed person resolution (encoding fix only, no merge/invention)."""
    for col in ["player1_name_clean", "player2_name_clean"]:
        if col in df.columns:
            df[col] = df[col].map(repair_cp1252_utf8_mojibake)
    fix_mojibake = repair_mojibake_for_column
    TARGET_COLS = [
        "player1_name_clean", "player2_name_clean",
        "player1_name", "player2_name",
        "player1_name_raw", "player2_name_raw",
    ]
    for col in TARGET_COLS:
        if col in df.columns:
            # Always run repair (handles str/None/NaN); skip only breaks on pandas/numpy edge types
            df[col] = df[col].map(fix_mojibake)
            df[col] = df[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()


def is_junk_name(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return True
    # Common non-person placeholders / ordinals that show up as "names"
    # Keep this list tiny + explicit to avoid false positives.
    if n in {"na", "n/a", "nd", "rd", "st", "st.", "th", "dnf", "()"}:
        return True
    if any(x in n for x in [
        "canada", "usa", "poland", "california", "arizona",
        "club", "footbag", "position", "match"
    ]):
        return True
    return False


def load_verified_person_aliases(csv_path: Path) -> dict[str, dict]:
    """Load verified alias → person_id mappings. Raises if file missing or invalid."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing required file: {csv_path}")

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        req = {"alias", "person_id", "person_canon", "status"}
        if not req.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"{csv_path} must have columns: {sorted(req)}")

        out: dict[str, dict] = {}
        for row in reader:
            status = (row.get("status") or "").strip().lower()
            if status != "verified":
                continue
            alias = row.get("alias") or ""
            key = _norm_alias(alias)
            if not key:
                continue
            out[key] = {
                "person_id": (row.get("person_id") or "").strip(),
                "person_canon": (row.get("person_canon") or "").strip(),
            }
        return out


def load_verified_person_aliases_optional(csv_path: Path) -> dict[str, dict]:
    """Load verified aliases if file exists; return {} otherwise. Caller should WARN when empty."""
    if not csv_path.exists():
        return {}
    try:
        return load_verified_person_aliases(csv_path)
    except Exception:
        return {}


def apply_person_alias(raw_name, alias_map: dict[str, dict]) -> tuple[Optional[str], Optional[str]]:
    """Look up raw_name in alias_map; returns (person_id, person_canon) or (None, None)."""
    if raw_name is None or (isinstance(raw_name, float) and pd.isna(raw_name)):
        return None, None
    key = str(raw_name).strip()
    if not key:
        return None, None
    norm_key = _norm_alias(key)
    if norm_key in alias_map:
        rec = alias_map[norm_key]
        return rec["person_id"], rec["person_canon"]
    return None, None


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I
)


def _is_uuid(s) -> bool:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return False
    s = (s if isinstance(s, str) else str(s)).strip()
    return bool(_UUID_RE.match(s))


def apply_person_ids_to_placements_flat(df: pd.DataFrame, alias_map: dict[str, dict]) -> pd.DataFrame:
    """Apply person alias lookup to every player slot. Runs even when player_id is NaN."""
    if df is None or df.empty:
        return df

    # Player slots to process (extensible for team members later)
    player_slots = ["player1", "player2"]

    def map_one(name: str):
        # IMPORTANT: use the same conservative cleanup pipeline as stage2p5 players.
        raw = repair_name(name)
        raw = strip_invisible(raw)

        res = clean_one(raw)

        # If it's junk or empty after cleanup, leave unmapped
        clean = (res.player_name_clean or "").strip()
        if not clean or res.name_status == "junk":
            return ("", "")

        # 1) Verified aliases always win (human truth) — unless name has embedded org (e.g. "Brendan Erskine Melb Footbag Club")
        if _has_embedded_org_tokens(clean):
            pass  # fall through to blank
        else:
            key = _norm_alias(clean)
            hit = alias_map.get(key)
            if hit:
                canon = clean_person_canon_for_output(hit["person_canon"])
                return (hit["person_id"], canon)

        # 2) A-names: auto-create a stable person_id (NOT an alias merge)
        if not _has_embedded_org_tokens(clean) and is_clean_canonical_person_name(clean):
            pid = str(uuid.uuid5(_PERSON_NAMESPACE, _norm_person_key(clean)))
            return (pid, clean)

        # 3) Otherwise leave blank (needs aliasing / cleanup / human review)
        return ("", "")

    for slot in player_slots:
        name_col = f"{slot}_name"
        name_clean_col = f"{slot}_name_clean"
        id_col = f"{slot}_person_id"
        canon_col = f"{slot}_person_canon"
        if name_col not in df.columns:
            continue
        # Process every row (including when player_id is NaN) — use raw name, not player_id
        # Use name_clean for alias lookup (decontaminated); fallback to name for compatibility
        lookup_col = name_clean_col if name_clean_col in df.columns else name_col
        pairs = df[lookup_col].apply(map_one)
        df[id_col] = pairs.map(lambda t: t[0])
        df[canon_col] = pairs.map(lambda t: t[1])
        # Guard against accidental swap: id should look like UUID; canon should NOT.
        swap = df.apply(
            lambda r: (not _is_uuid(r.get(id_col, ""))) and _is_uuid(r.get(canon_col, "")),
            axis=1,
        )
        if swap.any():
            tmp = df.loc[swap, id_col].copy()
            df.loc[swap, id_col] = df.loc[swap, canon_col]
            df.loc[swap, canon_col] = tmp
            print(f"[swap-guard] {slot}: swapped id/canon on {int(swap.sum())} rows (fixing inversion)")
        # When resolved, force both name and name_clean to canon (consistent display)
        resolved = df[id_col].fillna("").astype(str).str.strip() != ""
        if name_clean_col in df.columns:
            df.loc[resolved, name_clean_col] = df.loc[resolved, canon_col]
        df.loc[resolved, name_col] = df.loc[resolved, canon_col]

    return df


# -----------------------------
# Fallback A-name person_id assignment (last-mile fill for clean names)
# -----------------------------
PERSON_NS = uuid.UUID("00000000-0000-0000-0000-000000000001")

# Evidence-based blockers for fallback person_id assignment (correctness-first)
_RE_FALLBACK_BLOCK = re.compile(r"""
    [\d()/]            |  # digits, parens, slash
    \+                 |  # multi-person packed
    \\                 |  # backslash separator
    &                  |  # ampersand separator
    \s-\s              |  # "name - tricks"
    >                  |  # trick chain
    "                  |  # quoted tricks
    \$                 |  # $$$ noise
    [#]                |  # trailing markers
    _                  |  # underscores
    [¶¦±¼¨¹]            |  # mojibake artifacts seen in data
    \?                    # ambiguous placeholder
""", re.VERBOSE)

_STOPWORDS = {
    "RESULT", "RESULTS", "FINAL", "FINALS", "PARTNER", "PARTNERS",
    "ANNUAL", "OPEN", "TOURNAMENT", "CHAMPIONSHIP", "CHAMPIONSHIPS",
    "WORLD", "WORLDS", "EUROPEAN", "NATIONAL", "INTERNATIONAL",
    "FOOTBAG", "NET", "FREESTYLE", "IFPA",
}

_CONNECTORS = {"de", "da", "del", "van", "von", "der", "di", "la", "le", "du", "st", "st."}

_RE_PARENS_NICK = re.compile(r"\(([^)]*)\)")
_RE_DASH_META = re.compile(r"^(.*?)\s-\s(.*)$")

_META_TOKENS = {"team", "club", "fc", "association"}


def _has_embedded_org_tokens(s: str) -> bool:
    """True when name contains org tokens (club/association/fc) as whole words and no safe ' - ' form."""
    if not s or " - " in s:
        return False
    tokens = set(s.casefold().split())
    return bool(("club" in tokens or "association" in tokens or "fc" in tokens))


def normalize_for_fallback_aname(raw: str) -> str | None:
    """
    Conservative: remove ONLY safe nickname parens and safe dash-metadata suffixes
    so strict A-name matching can proceed. Never touches digits/comma-heavy notes.
    """
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if not s:
        return None

    # 1) Remove nickname-style parentheses: (Tree), (PT)
    # Only if inside contains: no digits, no commas, and short length.
    def _parens_repl(m):
        inner = m.group(1).strip()
        inner_low = inner.casefold()

        # Explicit "not an ID / not a name" markers → block fallback
        if ("ifpa" in inner_low) or ("no" in inner_low) or ("#" in inner):
            raise ValueError("BLOCK_FALLBACK_PARENS")

        if (not inner) or any(ch.isdigit() for ch in inner) or ("," in inner) or (len(inner) > 15):
            return m.group(0)  # keep as-is (too risky)
        return " "  # drop nickname

    try:
        s = _RE_PARENS_NICK.sub(_parens_repl, s)
    except ValueError:
        return None
    s = re.sub(r"\s+", " ", s).strip()

    # Block org/affiliation tokens embedded in the name (e.g., "Brendan Erskine Melb Footbag Club")
    # We only allow these tokens when they appear in the SAFE dash-metadata form ("Name - FC ...", "Name - Team ...").
    low = s.casefold()
    tokens = set(low.split())
    if ("club" in tokens or "association" in tokens or "fc" in tokens) and (" - " not in s):
        return None

    # 2) Strip " - metadata" ONLY when RHS looks like team/club metadata (no digits/scores)
    m = _RE_DASH_META.match(s)
    if m:
        left = m.group(1).strip()
        right = m.group(2).strip()
        right_low = right.casefold()
        # strip only when RHS is team/club-like and has no digits (do not strip " - 56", " - 16 kicks", " - 240 pkt")
        if any(t in right_low for t in _META_TOKENS) and not any(ch.isdigit() for ch in right):
            s = left

    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def normalize_person_name(s: str) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u2019", "'").replace("`", "'")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    # Trim common trailing punctuation that breaks matching/validation
    s = s.strip(" ,;")
    return s or None


def is_strict_aname(s: str) -> bool:
    """
    Strict A-name gate:
    - >=2 tokens
    - letters (Unicode) + [' . -] only — allows E.J., P.T., etc.
    - no digits, no parentheses, no slashes
    - blocks obvious event/result words
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return False
    s = str(s)
    if not s.strip():
        return False
    low = s.casefold()
    if low in {"na", "n/a", "dnf", "dq"}:
        return False
    if re.search(r"\d", s):
        return False
    if re.search(r"[()]", s):
        return False
    if "/" in s:
        return False

    parts = s.split()
    if len(parts) < 2:
        return False

    # stopword filter (prevents tournaments/events being promoted)
    for p in parts:
        token = p.strip(".,'").upper()
        if token in _STOPWORDS:
            return False

    # token validation: Unicode letters + . ' - (allows E.J., P.T., François)
    for p in parts:
        base = p.strip(".,'").casefold()
        if base in _CONNECTORS:
            continue
        if not all(c.isalpha() or c in ".'-" for c in p):
            return False

    # last name must be >=2 letters
    last = "".join(c for c in parts[-1] if c.isalpha())
    return len(last) >= 2


def person_id_from_aname(display_name: str) -> str:
    # ID based on casefolded normalized form (prevents case-only splits)
    key = normalize_person_name(display_name).casefold()
    return str(uuid.uuid5(PERSON_NS, "person:" + key))


def assign_person_ids_fallback(df: pd.DataFrame, name_col: str, person_id_col: str, person_canon_col: str) -> pd.DataFrame:
    """
    After alias merge, fill remaining null person_ids for strict A-names.
    """
    # treat empty string as null
    empty_mask = (df[person_id_col].fillna("").astype(str).str.strip() == "")
    df.loc[empty_mask, person_id_col] = pd.NA
    n0 = df[person_id_col].isna().sum()

    name_raw = df[name_col].fillna("").astype(str).str.strip()

    # Safe normalization for fallback gating only
    name_pre = df[name_col].map(normalize_for_fallback_aname)

    blocked = name_pre.fillna("").map(lambda s: bool(_RE_FALLBACK_BLOCK.search(str(s))))
    normed = name_pre.map(normalize_person_name)

    missing = df[person_id_col].isna()
    ok = (~blocked) & normed.notna() & normed.map(is_strict_aname)
    mask = missing & ok

    df.loc[mask, person_id_col] = normed[mask].map(person_id_from_aname)
    df.loc[mask, person_canon_col] = normed[mask]

    n1 = df[person_id_col].isna().sum()
    print(f"[fallback A-name] {person_id_col}: filled {n0 - n1} rows (remaining null: {n1})")
    return df


# -----------------------------
# Country helpers (conservative)
# -----------------------------
def _load_country_sets() -> Tuple[set, Dict[str, str]]:
    """
    Returns:
      valid_codes: set of acceptable country-ish tokens (alpha2 + alpha3 + a few extras)
      alpha2_to_alpha3: mapping for conversion when needed
    """
    valid = set()
    a2_to_a3: Dict[str, str] = {}

    # Try pycountry if present (best)
    try:
        import pycountry  # type: ignore

        for c in pycountry.countries:
            if getattr(c, "alpha_2", None):
                valid.add(c.alpha_2.upper())
            if getattr(c, "alpha_3", None):
                valid.add(c.alpha_3.upper())
            if getattr(c, "alpha_2", None) and getattr(c, "alpha_3", None):
                a2_to_a3[c.alpha_2.upper()] = c.alpha_3.upper()

        # Common non-standard tokens
        valid.update({"UK", "UAE", "KSA"})
        a2_to_a3.setdefault("UK", "GBR")
    except Exception:
        # Minimal fallback: common countries seen in footbag results
        common_a3 = {
            "USA",
            "CAN",
            "MEX",
            "BRA",
            "ARG",
            "CHL",
            "COL",
            "PER",
            "URY",
            "GBR",
            "IRL",
            "FRA",
            "DEU",
            "NLD",
            "BEL",
            "ESP",
            "PRT",
            "ITA",
            "CHE",
            "AUT",
            "SWE",
            "NOR",
            "DNK",
            "FIN",
            "ISL",
            "POL",
            "CZE",
            "SVK",
            "HUN",
            "ROU",
            "BGR",
            "SRB",
            "HRV",
            "SVN",
            "BIH",
            "UKR",
            "RUS",
            "BLR",
            "EST",
            "LVA",
            "LTU",
            "ISR",
            "TUR",
            "GRC",
            "CHN",
            "JPN",
            "KOR",
            "TWN",
            "HKG",
            "THA",
            "MYS",
            "SGP",
            "IDN",
            "PHL",
            "VNM",
            "KHM",
            "LAO",
            "MMR",
            "AUS",
            "NZL",
            "ZAF",
            "EGY",
            "MAR",
            "TUN",
        }
        common_a2 = {
            "US",
            "CA",
            "MX",
            "GB",
            "IE",
            "FR",
            "DE",
            "NL",
            "BE",
            "ES",
            "PT",
            "IT",
            "CH",
            "AT",
            "SE",
            "NO",
            "DK",
            "FI",
            "IS",
            "PL",
            "CZ",
            "SK",
            "HU",
            "RO",
            "BG",
            "UA",
            "RU",
            "BY",
            "EE",
            "LV",
            "LT",
            "TR",
            "GR",
            "CN",
            "JP",
            "KR",
            "TW",
            "HK",
            "TH",
            "MY",
            "SG",
            "ID",
            "PH",
            "VN",
            "AU",
            "NZ",
            "ZA",
            "EG",
            "MA",
            "TN",
        }
        valid.update(common_a3)
        valid.update(common_a2)
        a2_to_a3.update(
            {
                "US": "USA",
                "CA": "CAN",
                "GB": "GBR",
                "AU": "AUS",
                "NZ": "NZL",
                "CZ": "CZE",
                "SK": "SVK",
                "KR": "KOR",
                "CN": "CHN",
                "JP": "JPN",
            }
        )
        a2_to_a3.setdefault("UK", "GBR")
        valid.add("UK")

    return valid, a2_to_a3


VALID_COUNTRY_CODES, A2_TO_A3 = _load_country_sets()

# Strict allowlist: only these ISO3 (and common aliases) are accepted as country_clean
STRICT_COUNTRY_ALLOWLIST = frozenset({
    "USA", "CAN", "MEX", "BRA", "ARG", "CHL", "COL", "PER", "VEN", "URY", "GBR", "IRL", "FRA", "DEU", "NLD", "BEL",
    "ESP", "PRT", "ITA", "CHE", "AUT", "SWE", "NOR", "DNK", "FIN", "ISL", "POL", "CZE", "SVK", "HUN", "SVN", "HRV",
    "SRB", "ROU", "BGR", "UKR", "RUS", "BLR", "EST", "LVA", "LTU", "TUR", "GRC", "ISR", "JPN", "KOR", "CHN", "TWN",
    "HKG", "THA", "MYS", "SGP", "IDN", "PHL", "VNM", "KHM", "LAO", "MMR", "AUS", "NZL", "ZAF", "EGY", "MAR", "TUN",
})

# US state/DC abbreviations: do not treat as country when seen in raw (IL→ISR, CO→COL mistakes)
US_STATES = frozenset(
    "AL AK AZ AR CA CO CT DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY DC".split()
)

# Canadian province/territory abbreviations (do not treat as person names)
CANADIAN_PROVINCES = frozenset(
    "BC AB MB SK ON QC NB NS NL PE YT NT NU".split()
)

# Single-token country/region names (lowercase) that are pure location, not person names
COUNTRY_AND_REGION_NAMES = frozenset({
    "usa", "us", "u.s.a.", "canada", "uk", "germany", "france", "finland", "poland", "spain", "italy",
    "netherlands", "sweden", "norway", "denmark", "austria", "switzerland", "belgium", "ireland",
    "portugal", "greece", "czech", "czechia", "hungary", "romania", "russia", "ukraine", "japan",
    "china", "australia", "brazil", "mexico", "argentina", "chile", "colombia", "basque", "europe",
})

# Full US state names (lowercase) that sometimes appear as junk "names"
US_STATE_FULL_NAMES = frozenset({
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut",
    "delaware", "florida", "georgia", "hawaii", "iowa", "idaho", "illinois", "indiana",
    "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan",
    "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire",
    "new jersey", "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington", "west virginia",
    "wisconsin", "wyoming",
})

# Pattern: comma followed by geography term (e.g. ", USA", ", Canada")
_RE_COMMA_GEO = re.compile(
    r",\s*(?:USA|U\.S\.A\.|US|Canada|UK|Germany|France|Finland|Poland|Spain|Italy|Netherlands)\b",
    re.IGNORECASE,
)


def is_location_like(name: str) -> bool:
    """
    True if the string is a US state, province, country, pure location, or contains
    comma + geography terms. Used to mark as junk and skip person resolution.
    """
    if not name or not isinstance(name, str):
        return False
    t = name.strip()
    if not t:
        return False
    t_lower = t.lower()
    t_upper = t.upper()
    # Single alpha token: state/province abbreviation (2-letter)
    alpha_tokens = re.findall(r"[A-Za-z]+", t)
    if len(alpha_tokens) == 1:
        tok = alpha_tokens[0]
        if len(tok) == 2 and tok.upper() in (US_STATES | CANADIAN_PROVINCES):
            return True
        if t_lower in COUNTRY_AND_REGION_NAMES:
            return True
        if t_lower in US_STATE_FULL_NAMES:
            return True
    # Multi-word: entire string is a known region (e.g. "New York", "Rhode Island")
    if t_lower in US_STATE_FULL_NAMES:
        return True
    # Comma + geography: "Something, USA" or "AZ, Canada"
    if _RE_COMMA_GEO.search(t):
        return True
    # ALL CAPS geographic: single token that is state/province/country code
    if t.isupper() and len(t) >= 2 and len(t) <= 4:
        clean_upper = re.sub(r"[^A-Z]", "", t)
        if clean_upper in US_STATES or clean_upper in CANADIAN_PROVINCES:
            return True
        if len(clean_upper) == 3 and clean_upper in STRICT_COUNTRY_ALLOWLIST:
            return True
    return False


# Aliases mapped to canonical alpha-3 before allowlist check
_COUNTRY_ALIAS_TO_CANONICAL = {"GER": "DEU", "SUI": "CHE", "CH": "CHE"}


def _reject_country_token_for_us_state(token: str, raw: Optional[str]) -> bool:
    """If True, do not use this token as country (US state guardrail)."""
    if not token:
        return True
    t = token.strip().upper()
    t = re.sub(r"[^A-Z]", "", t)
    if t in US_STATES:
        return True
    if raw and ", USA" in raw.upper() and len(t) == 2:
        return True
    return False


# Flag emoji decoder: 🇺🇸 -> "US" -> "USA" when possible.
# Regional Indicator Symbol Letters range: U+1F1E6..U+1F1FF
def _flag_emoji_to_alpha2(s: str) -> Optional[str]:
    if not s:
        return None
    m = re.search(r"([\U0001F1E6-\U0001F1FF]{2})", s)
    if not m:
        return None
    pair = m.group(1)

    a2_chars = []
    for ch in pair:
        code = ord(ch) - 0x1F1E6 + ord("A")
        if code < ord("A") or code > ord("Z"):
            return None
        a2_chars.append(chr(code))
    return "".join(a2_chars)


def _normalize_country_token(tok: str) -> Optional[str]:
    if not tok:
        return None
    tok = tok.strip().upper()
    tok = re.sub(r"[^A-Z]", "", tok)
    if len(tok) not in (2, 3):
        return None
    # Never treat "IS" as Iceland (Pender Is, etc.); only ISL or explicit context
    if tok == "IS":
        return None
    if tok not in VALID_COUNTRY_CODES:
        return None
    # Prefer alpha-3 when we can safely map
    cand = A2_TO_A3[tok] if len(tok) == 2 and tok in A2_TO_A3 else tok
    cand = _COUNTRY_ALIAS_TO_CANONICAL.get(cand, cand)
    if cand not in STRICT_COUNTRY_ALLOWLIST:
        return None
    return cand


# -----------------------------
# Cleaning / classification rules
# -----------------------------
RE_HAS_LETTER = re.compile(r"[A-Za-z]")
RE_PLACE_FRAGMENT = re.compile(r"^\s*\d+(\.|)\s+")
RE_STARTS_JUNK = re.compile(r"^\s*(&|\*-\s*|\*|\(|\[)")
RE_PHRASE_JUNK = re.compile(
    r"\b(POOL|RANKING|FINAL\s+RESULTS|RESULTS|SCORES|DID\s+NOT|ACCORDING\s+TO|SEEDING)\b",
    re.IGNORECASE,
)

# trailing ".. 33 adds" or similar
RE_TRAILING_ADDS = re.compile(r"\.\.\s*\d+\s*adds\b", re.IGNORECASE)

# trailing bracket/paren blocks: "... (scratch)" or "... [RUS]"
RE_TRAILING_PAREN = re.compile(r"\s*\(([^)]{1,40})\)\s*$")
RE_TRAILING_BRACK = re.compile(r"\s*\[([^\]]{1,40})\]\s*$")

_RE_TRAILING_MONEY_OR_DELTA = re.compile(
    r"""
    (?:\s*\$+\s*\d+(?:\.\d+)?\s*$) |          # $40, $ 40, $$50
    (?:\s*[+-]\s*\d+(?:\.\d+)?\s*$) |         # +2, -44
    (?:\s*-\s*\d+\s*$)                        # name-44 (no space)
    """,
    re.VERBOSE,
)

# leading ordinal fragments: "1." "17.&18." "3)" "2 -" etc.
RE_LEADING_ORDINAL = re.compile(r"^\s*\d+\s*(\.\s*|\)\s*|-\s*|&\s*\d+\s*\.\s*)")

# stray leading punctuation
RE_LEADING_PUNCT = re.compile(r"^\s*[,.)\]]+")

# "team/club" indicators
RE_TEAMISH = re.compile(r"\b(TEAM|CLUB|FC|SC)\b", re.IGNORECASE)

# colon followed by lowercase words (trick descriptions)
RE_COLON_LOWERCASE = re.compile(r":\s*[a-z]+(?:\s+[a-z]+)*")

# Pattern for detecting colon in cleaned names
RE_HAS_COLON = re.compile(r":")

# trick keywords (footbag trick names)
TRICK_KEYWORDS = {
    "pixie", "ducking", "whirl", "mirage", "symp", "symposium", "butterfly",
    "osis", "clipper", "set", "delay", "spinning", "stepping",
    "flying", "atomic", "quantum", "phasing", "diving", "blur", "torque",
    "ripwalk", "dod", "paradox", "gyro", "flapper", "flip", "butter",
    "pickup", "toe", "inside", "outside", "same", "op", "sailing",
    "legover", "parradon", "race", "legbeater", "blender", "swirl"
}

# tokens that look like country codes in parentheses or tail
RE_UPPER_CODE = re.compile(r"\b([A-Z]{2,3})\b")

# common suffixes where comma is part of name
NAME_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}


@dataclass
class CleanResult:
    player_name_clean: str
    name_status: str  # ok|suspicious|junk|needs_review
    junk_reason: str
    country_clean: str
    country_evidence: str = ""   # emoji|paren|suffix_token (blank => no evidence)


def hard_reject_reason(raw: str) -> Optional[str]:
    t = (raw or "").strip()
    if not t:
        return "empty"
    
    # Fix 2: Handle leading *- correctly (must check before other * patterns)
    # If string starts with *- → junk (unchanged)
    if re.match(r"^\s*\*-\s*", t):
        return "starts_with_junk_punct"
    
    # Note: (tie) and * (but not *-) are stripped in clean_one before this function is called
    if RE_STARTS_JUNK.search(t):
        return "starts_with_junk_punct"
    if RE_PHRASE_JUNK.search(t):
        return "contains_heading_phrase"
    
    # Convert to lowercase once for multiple checks
    t_lower = t.lower()
    
    # Pattern P3: Event/tournament titles leaking in
    # If string starts with "Annual" and contains "Open|Championship|Tournament"
    if t_lower.startswith("annual"):
        if any(keyword in t_lower for keyword in ["open", "championship", "tournament"]):
            return "event_tournament_title"
    
    # Rule 1: Colon + trick words → junk
    # Check if colon is present and RHS contains trick keywords
    if ":" in t:
        colon_pos = t.find(":")
        rhs = t[colon_pos + 1 :].strip().lower()
        if rhs:
            # Check if RHS contains trick keywords
            if any(keyword in rhs for keyword in TRICK_KEYWORDS):
                return "contains_trick_description"
            # Also check for common trick-like patterns (single word that looks like a trick)
            # Examples: "Mariuz", "Montage", "Fog", "Blurriest" - single word tricks
            rhs_words = rhs.split()
            if len(rhs_words) == 1 and len(rhs_words[0]) >= 3:
                # Single word after colon - likely a trick name (conservative heuristic)
                return "contains_trick_description"
            # Multi-word trick patterns like "Scorpion Tail", "Whirling Swirl"
            if len(rhs_words) >= 2:
                # Check if any word is a trick keyword
                if any(word in TRICK_KEYWORDS for word in rhs_words):
                    return "contains_trick_description"
    
    # New junk rule: contains > character (trick notation)
    if ">" in t:
        return "contains_trick_notation"
    
    # New junk rule: multiple trick keywords (2+ occurrences)
    trick_count = sum(1 for keyword in TRICK_KEYWORDS if keyword in t_lower)
    if trick_count >= 2:
        return "contains_multiple_trick_keywords"
    
    if not RE_HAS_LETTER.search(t):
        return "no_letters"
    # place-only fragment heuristic
    if RE_PLACE_FRAGMENT.search(t):
        t2 = RE_PLACE_FRAGMENT.sub("", t).strip()
        name_tokens = re.findall(r"[A-Za-z]{2,}", t2)
        if len(name_tokens) < 1:
            return "place_fragment_only"
    return None


def strip_trailing_notes(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s

    s = RE_TRAILING_ADDS.sub("", s).strip()

    # Iteratively strip one trailing (...) or [...] note (max 2 times)
    for _ in range(2):
        if RE_TRAILING_BRACK.search(s):
            s = RE_TRAILING_BRACK.sub("", s).strip()
            continue
        if RE_TRAILING_PAREN.search(s):
            s = RE_TRAILING_PAREN.sub("", s).strip()
            continue
        break

    s = RE_LEADING_ORDINAL.sub("", s).strip()
    return s


def normalize_punctuation_ws(s: str) -> str:
    s = (s or "")
    # Replace curly quotes with straight quotes
    s = s.replace("“", '"').replace("”", '"').replace("’", "'")
    # Remove triple quotes around nicknames: """Flash""" -> "Flash"
    s = re.sub(r'"""\s*([^"]+?)\s*"""', r'"\1"', s)

    # Policy: keep nickname content but remove quote chars
    s = s.replace('"', "")

    s = re.sub(r"\s+", " ", s).strip()
    s = RE_LEADING_PUNCT.sub("", s).strip()
    return s


def split_name_and_country(s: str, raw: Optional[str] = None) -> Tuple[str, str, Optional[str]]:
    """
    Conservative extraction of country codes from:
      - flag emoji
      - trailing standalone code
      - parenthetical codes e.g. (PA CAN) -> CAN
    Uses US state guardrails: reject token if in US_STATES or raw contains ", USA" with 2-letter token.
    Returns (name, country_clean, country_evidence).
    country_evidence is "emoji" | "paren" | "suffix_token" or None (no country set).
    """
    if not s:
        return s, "", None

    # Emoji flag
    a2 = _flag_emoji_to_alpha2(s)
    if a2 and not _reject_country_token_for_us_state(a2, raw):
        c = _normalize_country_token(a2)
        if c:
            s2 = re.sub(r"[\U0001F1E6-\U0001F1FF]{2}", "", s).strip()
            return s2, c, "emoji"

    # Parenthetical like (PA CAN) or (CAN)
    m = re.search(r"\(([^)]{1,20})\)\s*$", s)
    if m:
        inside = m.group(1)
        toks = RE_UPPER_CODE.findall(inside.upper())
        if toks:
            token = toks[-1]
            if not _reject_country_token_for_us_state(token, raw):
                c = _normalize_country_token(token)
                if c:
                    s2 = s[: m.start()].strip()
                    return s2, c, "paren"

    # Trailing code token: "... FIN" — require standalone 2–3 letter uppercase token (reject Bel, Hák)
    parts = s.rsplit(" ", 1)
    if len(parts) == 2:
        maybe = parts[1].strip()
        if (
            len(maybe) in (2, 3)
            and maybe.isascii()
            and maybe.isalpha()
            and maybe.isupper()
            and not _reject_country_token_for_us_state(maybe, raw)
        ):
            c = _normalize_country_token(maybe)
            if c:
                return parts[0].strip(), c, "suffix_token"

    return s, "", None


def strip_location_after_comma(name: str) -> str:
    """
    For clean display, drop obvious location fragments after commas, but keep suffixes like "Jr."
    Examples:
      '"Big" Ben Alston, Memphis, TN' -> 'Big Ben Alston'
      'John Smith, Jr.' -> 'John Smith, Jr.' (kept)
    """
    if "," not in name:
        return name.strip()

    head, tail = name.split(",", 1)
    tail_stripped = tail.strip().upper().replace(".", "")
    if tail_stripped in NAME_SUFFIXES:
        return name.strip()

    rest = tail.strip()
    first_after_comma = rest.split(",")[0].strip().upper()
    # Guardrail: if "Head, <geo>" and head is single-token, it's almost certainly not a person.
    if " " not in head and (
        re.search(r"\b(?:USA|U\.S\.A\.|CANADA|FINLAND|GERMANY|FRANCE|UK)\b", rest, flags=re.I)
        or first_after_comma in US_STATES
    ):
        return ""
    if re.search(r"\b(USA|Canada|Finland|Germany|France|UK)\b", rest, flags=re.I):
        return head.strip()
    return head.strip()


def has_multiple_players(s: str) -> bool:
    """
    Pattern P2: Detect multiple players combined in one string.
    Examples:
      ALEX LOPEZ Y GABRIEL BOHORQUEZ
      ANDRES ARCE Y BERNARDO PALACIOS
      Andy Götze und Flo Wolff
      Chard Cook and Steve Dusablon and PT Lovern
      Dave Bernard (OR) Chris Routh (AZ)
    
    Returns True if pattern suggests multiple players.
    """
    if not s:
        return False
    
    s_upper = s.upper()
    
    # Check for (OR) separator
    if "(OR)" in s_upper:
        return True
    
    # Check for multiple "and" connectors (2+ occurrences)
    # This catches: "Chard Cook and Steve Dusablon and PT Lovern"
    connectors = [" Y ", " UND ", " AND "]
    connector_count = sum(s_upper.count(conn) for conn in connectors)
    if connector_count >= 2:
        return True
    
    # Check for single connector between name patterns
    # Pattern: [Name] [connector] [Name] where both sides look like names
    for conn in connectors:
        if conn in s_upper:
            # Split by connector and check if both sides look like names
            parts = s_upper.split(conn, 1)
            if len(parts) == 2:
                left, right = parts[0].strip(), parts[1].strip()
                # Both sides should have words starting with capital letters
                # Match words that start with capital letter (handles both ALL CAPS and Mixed Case)
                left_words = re.findall(r"\b[A-Z][A-Za-z]{1,}\b", left)
                right_words = re.findall(r"\b[A-Z][A-Za-z]{1,}\b", right)
                # Need at least one capitalized word on each side
                if len(left_words) >= 1 and len(right_words) >= 1:
                    return True
    
    return False


def has_encoding_ocr_garbage(s: str) -> bool:
    """
    Pattern P4: Detect encoding/OCR garbage artifacts.
    Examples:
      28? (I'm not positively sure about Tom's
      ? Chris Young Aaron de Glanville
      Chis Löw ? Hannes Daniel
      Budás "SiGi" Bálint   (partially broken)
      Chris­top­her Schil­lem
    
    Returns True if pattern suggests encoding/OCR issues.
    """
    if not s:
        return False
    
    # Check for question marks (especially at start or in middle, not just trailing)
    if "?" in s:
        # If starts with ?, or has ? in middle (not just at end)
        if s.strip().startswith("?") or s.count("?") > 1:
            return True
        # Single ? in middle (not at very end)
        q_pos = s.find("?")
        if q_pos >= 0 and q_pos < len(s) - 2:
            return True
    
    # Check for encoding artifacts: soft hyphens (U+00AD), zero-width characters
    # Soft hyphen: \u00AD
    # Zero-width space: \u200B
    # Zero-width non-joiner: \u200C
    # Zero-width joiner: \u200D
    encoding_chars = ["\u00AD", "\u200B", "\u200C", "\u200D"]
    if any(char in s for char in encoding_chars):
        return True
    
    # Check for annotations indicating broken/partial text
    s_lower = s.lower()
    broken_indicators = [
        "(partially broken)",
        "(broken)",
        "(incomplete)",
        "(partial)",
        "not positively sure",
        "i'm not sure",
    ]
    if any(indicator in s_lower for indicator in broken_indicators):
        return True
    
    # Check for incomplete parentheses (opening but no closing, suggesting truncated text)
    # But allow normal parentheticals, so check if there's text after opening paren that looks incomplete
    if "(" in s and ")" not in s:
        # If there's an opening paren but no closing, and it's not at the very end
        paren_pos = s.rfind("(")
        if paren_pos >= 0 and paren_pos < len(s) - 1:
            # Check if there's substantial text after the opening paren
            text_after = s[paren_pos + 1 :].strip()
            if len(text_after) > 5:  # Substantial text suggests incomplete
                return True
    
    return False


def has_name_with_stats_rankings(s: str) -> bool:
    """
    Pattern P5: Detect names mixed with stats/rankings.
    Examples:
      Andi Erromo (Basque Country) 1 victory 9 points
      Benjamin Kanske 18. Ulrike Häßler
      Dave Bernard III 1. Dave Bernard
    
    Returns True if pattern suggests name + stats/rankings mixed in.
    """
    if not s:
        return False
    
    s_lower = s.lower()
    
    # Check for stats keywords after numbers
    # Pattern: number followed by stats words (victory, victories, point, points, etc.)
    stats_patterns = [
        r"\d+\s+(victory|victories|point|points|win|wins|loss|losses|match|matches)",
        r"\d+\s+\d+\s+(victory|victories|point|points)",  # "1 victory 9 points"
    ]
    for pattern in stats_patterns:
        if re.search(pattern, s_lower):
            return True
    
    # Check for ranking pattern: number followed by period and then another name
    # Pattern: "Name Number. Name" or "Name Number Name"
    # Examples: "Benjamin Kanske 18. Ulrike Häßler" or "Dave Bernard III 1. Dave Bernard"
    ranking_pattern = r"\b\d+\s*\.\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*"
    if re.search(ranking_pattern, s):
        # Check if there's a name-like pattern before the number
        # Split by the ranking pattern and check if left side looks like a name
        match = re.search(ranking_pattern, s)
        if match:
            before = s[: match.start()].strip()
            # If there's substantial text before the ranking pattern, it's likely a name
            if len(before) > 3:
                # Check if before part has capitalized words (name-like)
                before_words = re.findall(r"\b[A-Z][a-z]+\b", before)
                if len(before_words) >= 1:
                    return True
    
    # Check for duplicate name pattern: "Name Number. Name" where both names are similar
    # This catches cases like "Dave Bernard III 1. Dave Bernard"
    duplicate_pattern = r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+\d+\s*\.\s*\1"
    if re.search(duplicate_pattern, s, re.IGNORECASE):
        return True
    
    return False


def looks_like_person_name(name: str) -> bool:
    """
    Simple heuristic to check if a string looks like a person name.
    Returns True if:
      - Contains ≥2 capitalized tokens (e.g., "First Last")
      - AND does not start with lowercase
    """
    if not name:
        return False
    
    # Check if starts with lowercase
    if name.strip() and name.strip()[0].islower():
        return False
    
    # Find capitalized tokens (words that start with capital letter)
    capitalized_tokens = re.findall(r"\b[A-Z][a-z]+\b", name)
    
    # Need at least 2 capitalized tokens to look like a person name
    return len(capitalized_tokens) >= 2


def looks_like_person(name: str) -> str:
    """
    Returns: ok | suspicious | junk | needs_review
    """
    if not name:
        return "junk"

    if RE_PHRASE_JUNK.search(name):
        return "junk"

    if RE_TEAMISH.search(name):
        return "suspicious"

    letters = len(re.findall(r"[A-Za-z]", name))
    nonspace = len(re.sub(r"\s+", "", name))
    if nonspace > 0 and letters / nonspace < 0.4:
        return "junk"

    tokens = re.findall(r"[A-Za-z][A-Za-z'\-]*\.?", name)
    tokens = [t for t in tokens if t]
    if len(tokens) == 0:
        return "junk"

    if len(tokens) == 1:
        return "suspicious"

    if 2 <= len(tokens) <= 4:
        return "ok"

    return "needs_review"


def _is_acronym_name(s: str) -> bool:
    """True if cleaned name is exactly 2–4 uppercase letters (skip for alias edges)."""
    s = (s or "").strip()
    return bool(re.fullmatch(r"[A-Z]{2,4}", s))


def compute_name_key(name: str) -> str:
    """
    Canonical key for suggested merges. Conservative:
      - lowercase
      - remove punctuation
      - drop trailing location fragments after commas
    """
    if not name:
        return ""
    raw2 = re.sub(r'^\s*=\-?\s*', '', name).strip()
    base = strip_location_after_comma(raw2).lower()
    base = re.sub(r"[^a-z0-9\s]", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    noise = {"the", "and"}
    toks = [t for t in base.split() if t not in noise]
    return " ".join(toks)


def clean_one(raw: str, country_observed: str = "") -> CleanResult:
    raw = "" if raw is None else str(raw)
    country_observed = "" if country_observed is None else str(country_observed)

    s = raw.strip()
    # --- Mojibake repair (INFO-level, no guessing) ---
    s_fixed2, was_fixed2 = fix_cp1250_mojibake_if_detected(s)
    if was_fixed2:
        s = s_fixed2

    s_fixed, was_fixed = fix_mojibake_if_detected(s)
    if was_fixed:
        s = s_fixed

    s = strip_invisible(s)
    # peel off obvious suffix noise first
    s, noise1 = split_trailing_star_noise(s)
    s, noise2 = split_trailing_paren_noise(s)
    suffix_noise = " ".join(x for x in [noise1, noise2] if x).strip()

    # --- Pollution splitter (clean vs noise for provenance) ---
    name_raw = s
    name_clean, name_noise, changed = split_name_pollution(name_raw)
    name_clean = decontaminate_name_for_alias_lookup(name_clean)
    if suffix_noise:
        name_noise = " ".join(x for x in [name_noise, suffix_noise] if x).strip()
    if changed:
        pass  # INFO-level: we are not asserting identity, just splitting junk
    s = name_clean
    # strip spreadsheet/formula-ish prefix markers
    s = re.sub(r'^\s*=\-?\s*', '', s).strip()

    # If input looks like "Last, First", swap it (but never for locations / note-parens).
    if "," in s:
        left, right = [p.strip() for p in s.split(",", 1)]
        right_up = right.replace(".", "").upper()
        bad_geo = {"BC","AB","MB","SK","ON","QC","NB","NS","NL","PE","YT","NT","NU"} | US_STATES
        if ("(" not in left) and (" " not in left) and not (left.isupper() and len(left) <= 3):
            if right_up and right_up not in NAME_SUFFIXES and right_up not in bad_geo:
                s = f"{right} {left}".strip()

    # Fix 3: Strip trailing parentheses before phrase-junk check
    # Step 1: strip trailing bracket/parenthetical junk + ordinals (before hard reject)
    s = strip_trailing_notes(s)
    
    # Fix 1: Handle (tie) before junking on (
    # Strip (tie) case-insensitively if present
    s = re.sub(r"^\s*\(tie\)\s+", "", s, flags=re.IGNORECASE).strip()
    
    # Fix 2: Handle leading * correctly
    # Check for *- first (must be before stripping *)
    if re.match(r"^\s*\*-\s*", s):
        # Let hard_reject_reason handle this case
        pass
    # Else if string starts with * (but not *-), strip it
    elif re.match(r"^\s*\*", s):
        s = re.sub(r"^\s*\*", "", s).strip()

    reason = hard_reject_reason(s)
    if reason:
        return CleanResult(
            player_name_clean="",
            name_status="junk",
            junk_reason=reason,
            country_clean="",
        )

    # Step 3: normalize punctuation / whitespace
    s = normalize_punctuation_ws(s)

    # Optional: drop location fragments after comma for display
    s = strip_location_after_comma(s)

    # Step 4: extract country codes (conservative), with US state guardrails
    s2, c, country_evidence = split_name_and_country(s, raw=raw)

    # No inference: only keep country_clean when we have in-string evidence (emoji/paren/suffix_token)
    c_final = c if country_evidence is not None else ""

    s2 = normalize_punctuation_ws(s2)
    s2 = re.sub(r"\s+\d+(?:\.\d+)?\s*$", "", s2).strip()

    # Pattern P2: Check for multiple players combined in one string
    # This should be checked on the cleaned string but before looks_like_person
    if has_multiple_players(s2):
        return CleanResult(
            player_name_clean=s2,
            name_status="needs_review",
            junk_reason="multiple_players_combined",
            country_clean=c_final or "",
        )

    # Pattern P4: Check for encoding/OCR garbage artifacts
    # This should be checked on the cleaned string but before looks_like_person
    if has_encoding_ocr_garbage(s2):
        return CleanResult(
            player_name_clean=s2,
            name_status="needs_review",
            junk_reason="encoding_ocr_garbage",
            country_clean=c_final or "",
        )

    # Pattern P5: Check for names mixed with stats/rankings
    # This should be checked on the cleaned string but before looks_like_person
    if has_name_with_stats_rankings(s2):
        return CleanResult(
            player_name_clean=s2,
            name_status="needs_review",
            junk_reason="name_with_stats_rankings",
            country_clean=c_final or "",
        )

    # Location-only: state/province/country or comma+geo → junk, skip person resolution
    if is_location_like(s2):
        return CleanResult(
            player_name_clean="",
            name_status="junk",
            junk_reason="location_only",
            country_clean=c_final or "",
            country_evidence=country_evidence or "",
        )

    # Step 5: looks-like-person gate
    status = looks_like_person(s2)
    junk_reason = ""
    if status == "junk":
        junk_reason = "not_person_like"

    # Rule 2: Colon cases should never be "ok"
    # If name_status is "ok" and colon is present, downgrade based on RHS content
    if status == "ok" and ":" in s2:
        colon_pos = s2.find(":")
        rhs = s2[colon_pos + 1 :].strip()
        rhs_lower = rhs.lower()
        
        # If RHS contains trick keywords → junk
        if rhs and any(keyword in rhs_lower for keyword in TRICK_KEYWORDS):
            status = "junk"
            junk_reason = "colon_trick_description"
        # Otherwise → needs_review
        else:
            status = "needs_review"
            junk_reason = "contains_colon_uncertain"

    # Final downgrade rule: If name_status is "ok" but contains trick keywords
    # and does NOT look like a person name → downgrade to junk
    if status == "ok":
        s2_lower = s2.lower()
        if any(keyword in s2_lower for keyword in TRICK_KEYWORDS):
            # Downgrade to junk if ANY of these are true:
            # 1. No space-separated capitalized name tokens (no First Last)
            # 2. Starts with a lowercase word (ducking x-body sole)
            # 3. Matches known trick abbreviations (P., PS, Pdx)
            # 4. Contains trick keywords and no person structure
            should_downgrade = False
            
            # Check 1: No space-separated capitalized name tokens
            if not looks_like_person_name(s2):
                should_downgrade = True
            
            # Check 2: Starts with lowercase word
            if s2.strip() and s2.strip()[0].islower():
                should_downgrade = True
            
            # Check 3: Matches known trick abbreviations (P., PS, Pdx)
            if re.search(r"\b(P\.|PS|Pdx)\b", s2, re.IGNORECASE):
                should_downgrade = True
            
            if should_downgrade:
                status = "junk"
                junk_reason = "trick_name_only"

    # Final fix: "OK leak guard" - catch trick-only entries that passed earlier checks
    # Apply this only when name_status == "ok"
    if status == "ok":
        s2_stripped = s2.strip()
        
        # A) Standalone trick phrases → junk
        # Check if begins with a trick keyword
        words_list = s2_stripped.split()
        if words_list:
            first_word = words_list[0].lower()
            if first_word in TRICK_KEYWORDS:
                status = "junk"
                junk_reason = "trick_phrase_only"
            # OR is exactly 2 words and both are in trick vocabulary
            elif len(words_list) == 2:
                word1, word2 = words_list[0].lower(), words_list[1].lower()
                if word1 in TRICK_KEYWORDS and word2 in TRICK_KEYWORDS:
                    status = "junk"
                    junk_reason = "trick_phrase_only"
        
        # B) Dash-separated "name - trick" → junk
        # Pattern: ^[A-Z][a-z].+\s-\s.+ and RHS contains trick keywords
        if status == "ok" and " - " in s2_stripped:
            dash_pattern = re.match(r"^[A-Z][a-z].+\s-\s.+", s2_stripped)
            if dash_pattern:
                parts = s2_stripped.split(" - ", 1)
                if len(parts) == 2:
                    rhs = parts[1].strip().lower()
                    if any(keyword in rhs for keyword in TRICK_KEYWORDS):
                        status = "junk"
                        junk_reason = "name_dash_trick"
        
        # C) "trick label - Name Name" glued with hyphen → junk
        # Contains a hyphen, left side contains trick keywords, right side looks like person name
        if status == "ok" and "-" in s2_stripped:
            # Split by hyphen (could be " - " or just "-")
            if " - " in s2_stripped:
                parts = s2_stripped.split(" - ", 1)
            else:
                parts = s2_stripped.split("-", 1)
            
            if len(parts) == 2:
                left, right = parts[0].strip(), parts[1].strip()
                left_lower = left.lower()
                # Left side contains trick keywords
                if any(keyword in left_lower for keyword in TRICK_KEYWORDS):
                    # Right side looks like a person name (2 capitalized tokens)
                    if looks_like_person_name(right):
                        status = "junk"
                        junk_reason = "trick_label_dash_name"

    # Hard reject location fragments that slipped through (e.g., "AZ, USA) and ...")
    if s2 and s2.strip() in {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","BC","QC"}:
        return CleanResult("", "junk", "location_fragment", "", "")
    if raw and (" and " in raw.lower()) and (s2.strip() in {"AZ","CA","MD","BC","QC"}):
        return CleanResult("", "junk", "location_fragment", "", "")

    return CleanResult(
        player_name_clean=s2 if status != "junk" else "",
        name_status=status or "needs_review",
        junk_reason=junk_reason,
        country_clean=c_final or "",
        country_evidence=country_evidence or "",
    )


# -----------------------------
# Optional usage counts helpers
# -----------------------------
def compute_usage_counts(events_csv: Path) -> Dict[str, int]:
    """
    Heuristic scan for player_id usage counts from a canonical events CSV.
    We count occurrences in common columns:
      player_id, player1_id, player2_id, player3_id, etc.
    """
    counts: Dict[str, int] = defaultdict(int)
    if not events_csv.exists():
        return counts

    try:
        df = pd.read_csv(events_csv)
    except Exception:
        return counts

    cols = [c for c in df.columns if re.search(r"player\d*_id$|player_id$", c)]
    if not cols:
        cols = [c for c in df.columns if ("player" in c.lower() and c.lower().endswith("_id"))]

    for c in cols:
        series = df[c].dropna().astype(str)
        for v in series:
            v = v.strip()
            if v:
                counts[v] += 1

    return counts

def main() -> int:
    ap = argparse.ArgumentParser(description="Stage 2.5 player token cleanup (conservative, reversible).")
    ap.add_argument("--players_csv", default="out/stage2_players.csv", help="Input Stage 2 players CSV.")
    ap.add_argument("--events_csv", default="out/stage2_canonical_events.csv", help="Optional canonical events CSV (usage counts).")
    ap.add_argument("--out_dir", default="out", help="Output directory.")
    ap.add_argument("--person_aliases_csv", default="overrides/person_aliases.csv",
                    help="Human-verified alias → person_id mappings (required for person_id assignment).")
    ap.add_argument("--emit_placements_flat", action="store_true", default=True,
                    help="Emit out/Placements_Flat.csv (default: on).")
    args = ap.parse_args()

    players_csv = Path(args.players_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not players_csv.exists():
        print(f"ERROR: missing input {players_csv}", file=sys.stderr)
        return 2

    df = pd.read_csv(players_csv)

    # Defensive column mapping
    col_id = "player_id"
    col_name = "player_name"
    col_country = "country_observed"
    for required in (col_id, col_name):
        if required not in df.columns:
            print(f"ERROR: expected column '{required}' in {players_csv}. Found: {list(df.columns)}", file=sys.stderr)
            return 2
    if col_country not in df.columns:
        df[col_country] = ""

    usage_counts = compute_usage_counts(Path(args.events_csv))

    out_rows = []
    status_counts = Counter()
    junk_reasons = Counter()
    country_extracted = 0

    for _, r in df.iterrows():
        pid = str(r.get(col_id, "")).strip()
        raw = "" if pd.isna(r.get(col_name)) else str(r.get(col_name))
        c_obs = "" if pd.isna(r.get(col_country)) else str(r.get(col_country))

        res = clean_one(raw, c_obs)

        status_counts[res.name_status] += 1
        if res.name_status == "junk":
            junk_reasons[res.junk_reason or "unknown"] += 1

        # Count cases where we derived a country differing from observed (or observed empty)
        if res.country_clean and (not c_obs or _normalize_country_token(c_obs) != res.country_clean):
            country_extracted += 1

        row = {
            "player_id": pid,
            "player_name_raw": raw,
            "country_observed": c_obs,
            "player_name_clean": res.player_name_clean,
            "name_status": res.name_status,
            "junk_reason": res.junk_reason,
            "country_clean": res.country_clean,
            "country_evidence": res.country_evidence,
            "usage_count": usage_counts.get(pid, 0),
            "source_hint": "",  # reserved (optional)
        }
        # --- Stage 2.5 late demotion (display/id-safe): dead tokens only ---
        # Only demote when this token is unused (won't affect placements/results).
        if int(row.get("usage_count") or 0) == 0 and (row.get("name_status") in ("ok", "suspicious", "needs_review")):
            nm = (row.get("player_name_clean") or row.get("player_name_raw") or "").strip()
            if (not nm) or (nm.upper() == "#NAME?") or _RE_LEADING_ORDINAL.search(nm):
                row["name_status"] = "junk"
                row["junk_reason"] = row.get("junk_reason") or "dead_token_placeholder_or_rank"
            if _RE_TRAILING_MONEY_OR_DELTA.search(row.get("player_name_clean") or ""):
                row["name_status"] = "junk"
                row["junk_reason"] = "trailing_money_or_delta"
            s = (row.get("player_name_clean") or "").strip()
            if "$" in s:
                row["name_status"] = "junk"
                row["junk_reason"] = row.get("junk_reason") or "trailing_money"
            elif "scratch" in s.lower():
                row["name_status"] = "junk"
                row["junk_reason"] = row.get("junk_reason") or "scratch_note"
            elif s.endswith("&"):
                row["name_status"] = "junk"
                row["junk_reason"] = row.get("junk_reason") or "dangling_ampersand"
        out_rows.append(row)

    out_df = pd.DataFrame(out_rows)

    # Compute name_key for alias suggestions (only for non-junk; skip acronym-like names)
    out_df["name_key"] = out_df.apply(
        lambda x: compute_name_key(x["player_name_clean"])
        if x["name_status"] != "junk" and not _is_acronym_name(x["player_name_clean"])
        else "",
        axis=1,
    )

    # Suggested merges: same name_key across multiple player_ids
    edges = []
    groups = out_df[(out_df["name_status"] != "junk") & (out_df["name_key"] != "")].groupby("name_key")
    for key, g in groups:
        uniq = g[["player_id", "player_name_raw", "player_name_clean"]].drop_duplicates()
        if len(uniq) <= 1:
            continue
        items = uniq.to_dict("records")

        # Emit pairwise edges; cap per group to avoid blowups
        max_pairs = 2000
        pairs = 0
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                if pairs >= max_pairs:
                    break
                a, b = items[i], items[j]
                edges.append(
                    {
                        "name_key": key,
                        "player_id_a": a["player_id"],
                        "player_name_a": a["player_name_raw"],
                        "player_id_b": b["player_id"],
                        "player_name_b": b["player_name_raw"],
                        "reason": "same_name_key",
                        "confidence": "low",
                    }
                )
                pairs += 1
            if pairs >= max_pairs:
                break

    edges_df = pd.DataFrame(edges)

    # Deterministic sort and write
    out_path = out_dir / "stage2p5_players_clean.csv"
    edges_path = out_dir / "stage2p5_player_alias_edges.csv"
    qc_path = out_dir / "stage2p5_qc_summary.json"

    out_df = out_df.sort_values(by=["name_status", "player_name_clean", "player_id"], kind="mergesort")
    mask = (
        out_df.player_name_clean.fillna("").str.strip().eq("") |
        out_df.player_name_clean.str.upper().eq("#NAME?")
    )
    out_df.loc[mask, "name_status"] = "junk"
    out_df.loc[mask, "junk_reason"] = "missing_clean_name"
    out_df.to_csv(out_path, index=False)

    # --- Emit Placements_Flat (Stage 2.5 contract) ---
    # Load stage2 canonical events, flatten (same logic as 03), apply verified person_id only, write CSV.
    # If person_aliases.csv missing: WARN and emit with empty person_id columns so runs complete.
    if args.emit_placements_flat:
        events_csv = Path(args.events_csv)
        if not events_csv.exists():
            raise FileNotFoundError(f"Required for Placements_Flat: {events_csv}")

        person_aliases_csv = Path(args.person_aliases_csv)
        alias_map = load_verified_person_aliases_optional(person_aliases_csv)
        if not alias_map:
            print(f"WARN: {person_aliases_csv} missing or empty; emitting Placements_Flat with empty person_id columns", file=sys.stderr)

        records = load_stage2_canonical_events_records(events_csv)
        players_by_id = build_players_by_id(out_df)
        flat_df, plus_split_audit_rows = build_placements_flat_df(records, players_by_id, out_dir)

        # Write audit CSV for plus-split failures
        if plus_split_audit_rows:
            audit_df = pd.DataFrame(plus_split_audit_rows)
            audit_path = out_dir / "02p5_plus_split_audit.csv"
            audit_df.to_csv(audit_path, index=False, na_rep="")
            print(f"Wrote plus-split audit: {audit_path} ({len(plus_split_audit_rows)} rows)")

        # RIGHT BEFORE alias merge / person-id assignment: repair mojibake in name columns
        _repair_name_cols_for_person_resolution(flat_df)

        def _looks_like_rank_slop(s: str) -> bool:
            """Detects ranking/junk fragments like '2.' embedded in a name."""
            if not isinstance(s, str) or not s.strip():
                return False
            return bool(re.search(r"\b\d+\.\s*", s))  # rank number with a dot

        # apply alias merge
        flat_df = apply_person_ids_to_placements_flat(flat_df, alias_map)

        # block slop-like names from fallback
        flat_df.loc[
            flat_df["player1_name_clean"].map(_looks_like_rank_slop),
            ["player1_person_id", "player1_person_canon"],
        ] = ["", ""]

        flat_df.loc[
            flat_df["player2_name_clean"].map(_looks_like_rank_slop),
            ["player2_person_id", "player2_person_canon"],
        ] = ["", ""]

        # now do fallback assignment for everything else
        flat_df = assign_person_ids_fallback(
            flat_df, "player1_name_clean", "player1_person_id", "player1_person_canon"
        )
        flat_df = assign_person_ids_fallback(
            flat_df, "player2_name_clean", "player2_person_id", "player2_person_canon"
        )

        # DO NOT overwrite canon for resolved rows: only fill canon from name_clean when both canon and person_id are missing
        for slot in ("player1", "player2"):
            name_col = f"{slot}_name_clean"
            pid_col = f"{slot}_person_id"
            canon_col = f"{slot}_person_canon"

            missing_canon = flat_df[canon_col].isna() | (flat_df[canon_col].astype(str).str.strip() == "")
            missing_pid   = flat_df[pid_col].isna()   | (flat_df[pid_col].astype(str).str.strip() == "")

            mask = missing_canon & missing_pid
            flat_df.loc[mask, canon_col] = flat_df.loc[mask, name_col].map(clean_person_canon_for_output)

        out_path_flat = out_dir / "Placements_Flat.csv"
        flat_df.to_csv(out_path_flat, index=False, na_rep="")
        print(f"Wrote {out_path_flat} ({len(flat_df)} rows)")

        def _coalesce_nonblank(primary: pd.Series, fallback: pd.Series) -> pd.Series:
            """Return primary where it is non-empty (after strip), else fallback."""
            p = primary.fillna("").astype(str).str.strip()
            f = fallback.fillna("").astype(str).str.strip()
            return p.where(p != "", f)

        # --- NEW: Placements keyed by person_id (identity), not player_id (variant) ---
        placements_by_person = flat_df.copy()

        _RE_SLOP = re.compile(r"\b\d+\.\b", re.IGNORECASE)  # catches "2." etc.

        def _is_slop_name(s: str) -> bool:
            if not isinstance(s, str):
                return False
            t = s.strip()
            if not t:
                return False
            return bool(_RE_SLOP.search(t))

        # Check for slop and junk BEFORE blanking (check name_clean, person_canon, AND name)
        # Final name uses person_canon first, then name_clean, but name might already have slop
        slop_p1_clean = placements_by_person["player1_name_clean"].fillna("").map(_is_slop_name)
        slop_p1_canon = placements_by_person["player1_person_canon"].fillna("").map(_is_slop_name)
        slop_p1_name = placements_by_person["player1_name"].fillna("").map(_is_slop_name)
        slop_p2_clean = placements_by_person["player2_name_clean"].fillna("").map(_is_slop_name)
        slop_p2_canon = placements_by_person["player2_person_canon"].fillna("").map(_is_slop_name)
        slop_p2_name = placements_by_person["player2_name"].fillna("").map(_is_slop_name)
        
        junk_p1_clean = placements_by_person["player1_name_clean"].map(looks_like_junk_person_name)
        junk_p1_canon = placements_by_person["player1_person_canon"].map(looks_like_junk_person_name)
        junk_p1_name = placements_by_person["player1_name"].map(looks_like_junk_person_name)
        junk_p2_clean = placements_by_person["player2_name_clean"].map(looks_like_junk_person_name)
        junk_p2_canon = placements_by_person["player2_person_canon"].map(looks_like_junk_person_name)
        junk_p2_name = placements_by_person["player2_name"].map(looks_like_junk_person_name)
        
        slop_p1 = slop_p1_clean | slop_p1_canon | slop_p1_name
        slop_p2 = slop_p2_clean | slop_p2_canon | slop_p2_name
        junk_p1 = junk_p1_clean | junk_p1_canon | junk_p1_name
        junk_p2 = junk_p2_clean | junk_p2_canon | junk_p2_name

        # Blank IDs and names when slop or junk detected (forces rejection from ByPerson)
        bad_p1 = slop_p1 | junk_p1
        bad_p2 = slop_p2 | junk_p2

        placements_by_person.loc[bad_p1, "player1_id"] = ""
        placements_by_person.loc[bad_p1, "player1_person_id"] = ""
        placements_by_person.loc[bad_p1, "player1_person_canon"] = ""
        placements_by_person.loc[bad_p1, "player1_name_clean"] = ""
        placements_by_person.loc[bad_p1, "player1_name"] = ""

        placements_by_person.loc[bad_p2, "player2_id"] = ""
        placements_by_person.loc[bad_p2, "player2_person_id"] = ""
        placements_by_person.loc[bad_p2, "player2_person_canon"] = ""
        placements_by_person.loc[bad_p2, "player2_name_clean"] = ""
        placements_by_person.loc[bad_p2, "player2_name"] = ""

        # replace player_id with person_id when available; otherwise keep original player_id
        placements_by_person["player1_id"] = _coalesce_nonblank(
            placements_by_person.get("player1_person_id", ""),
            placements_by_person["player1_id"],
        )
        placements_by_person["player2_id"] = _coalesce_nonblank(
            placements_by_person.get("player2_person_id", ""),
            placements_by_person["player2_id"],
        )

        # Re-check slop/junk after coalescing (check name_clean, person_canon, AND name)
        slop_p1_clean_after = placements_by_person["player1_name_clean"].fillna("").map(_is_slop_name)
        slop_p1_canon_after = placements_by_person["player1_person_canon"].fillna("").map(_is_slop_name)
        slop_p1_name_after = placements_by_person["player1_name"].fillna("").map(_is_slop_name)
        slop_p2_clean_after = placements_by_person["player2_name_clean"].fillna("").map(_is_slop_name)
        slop_p2_canon_after = placements_by_person["player2_person_canon"].fillna("").map(_is_slop_name)
        slop_p2_name_after = placements_by_person["player2_name"].fillna("").map(_is_slop_name)
        
        junk_p1_clean_after = placements_by_person["player1_name_clean"].map(looks_like_junk_person_name)
        junk_p1_canon_after = placements_by_person["player1_person_canon"].map(looks_like_junk_person_name)
        junk_p1_name_after = placements_by_person["player1_name"].map(looks_like_junk_person_name)
        junk_p2_clean_after = placements_by_person["player2_name_clean"].map(looks_like_junk_person_name)
        junk_p2_canon_after = placements_by_person["player2_person_canon"].map(looks_like_junk_person_name)
        junk_p2_name_after = placements_by_person["player2_name"].map(looks_like_junk_person_name)
        
        slop_p1_after = slop_p1_clean_after | slop_p1_canon_after | slop_p1_name_after
        slop_p2_after = slop_p2_clean_after | slop_p2_canon_after | slop_p2_name_after
        junk_p1_after = junk_p1_clean_after | junk_p1_canon_after | junk_p1_name_after
        junk_p2_after = junk_p2_clean_after | junk_p2_canon_after | junk_p2_name_after

        # Blank IDs again if slop/junk detected (ensures they stay blank after coalescing)
        placements_by_person.loc[slop_p1_after | junk_p1_after, "player1_id"] = ""
        placements_by_person.loc[slop_p2_after | junk_p2_after, "player2_id"] = ""

        # Also blank name columns when slop/junk detected (prevents slop from appearing in final name)
        placements_by_person.loc[slop_p1_after | junk_p1_after, "player1_name_clean"] = ""
        placements_by_person.loc[slop_p1_after | junk_p1_after, "player1_person_canon"] = ""
        placements_by_person.loc[slop_p1_after | junk_p1_after, "player1_name"] = ""
        placements_by_person.loc[slop_p2_after | junk_p2_after, "player2_name_clean"] = ""
        placements_by_person.loc[slop_p2_after | junk_p2_after, "player2_person_canon"] = ""
        placements_by_person.loc[slop_p2_after | junk_p2_after, "player2_name"] = ""

        # One authoritative ID per column: player1_id / player2_id are person IDs; drop redundant person_id columns
        placements_by_person = placements_by_person.drop(columns=["player1_person_id", "player2_person_id"], errors="ignore")

        # standardize display names to canonical person names when available
        # Use coalesce_nonblank to handle empty strings (not just NaN)
        placements_by_person["player1_name"] = _coalesce_nonblank(
            placements_by_person["player1_person_canon"],
            placements_by_person["player1_name_clean"]
        )
        placements_by_person["player2_name"] = _coalesce_nonblank(
            placements_by_person["player2_person_canon"],
            placements_by_person["player2_name_clean"]
        )

        # Enforce definitive structural rules for ByPerson
        ct = placements_by_person["competitor_type"].fillna("").astype(str).str.lower()

        # 1) player1_id must always exist
        bad_p1 = placements_by_person["player1_id"].fillna("").astype(str).str.strip() == ""
        if bad_p1.any():
            # Optional: write rejects for audit
            rejects = placements_by_person.loc[bad_p1].copy()
            rejects["reject_reason"] = "missing_player1_id"
            (out_dir / "Placements_ByPerson_Rejected.csv").write_text("")  # ensure dir exists in some envs
            rejects_path = out_dir / "Placements_ByPerson_Rejected.csv"
            rejects.to_csv(rejects_path, index=False, na_rep="")
            print(f"WARN: Dropping {int(bad_p1.sum())} rows from ByPerson (missing player1_id). Wrote rejects to {rejects_path}", file=sys.stderr)
            placements_by_person = placements_by_person.loc[~bad_p1].copy()

        # 2) team rows must have player2_id; non-team rows must have blank player2 fields
        is_team = ct.eq("team")

        p2_blank = placements_by_person["player2_id"].fillna("").astype(str).str.strip() == ""
        bad_team = is_team & p2_blank
        if bad_team.any():
            rejects = placements_by_person.loc[bad_team].copy()
            rejects["reject_reason"] = "team_missing_player2_id"
            rejects_path = out_dir / "Placements_ByPerson_Rejected.csv"
            # append if already exists
            mode = "a" if rejects_path.exists() else "w"
            header = not rejects_path.exists()
            rejects.to_csv(rejects_path, index=False, na_rep="", mode=mode, header=header)
            print(f"WARN: Dropping {int(bad_team.sum())} team rows missing player2_id. Appended rejects to {rejects_path}", file=sys.stderr)
            placements_by_person = placements_by_person.loc[~bad_team].copy()

        # Force blanks on non-team
        non_team = ~is_team
        placements_by_person.loc[non_team, "player2_id"] = ""
        placements_by_person.loc[non_team, "player2_name"] = ""

        # QC03 minimal deterministic guardrail (right before writing placements output for 03/04)
        def _apply_qc03(row: pd.Series) -> tuple[str, str, str]:
            return _qc03_guardrail(
                str(row.get("player1_name") or ""),
                str(row.get("player2_name") or ""),
                str(row.get("team_display_name") or ""),
            )
        qc03_results = placements_by_person.apply(_apply_qc03, axis=1)
        placements_by_person["player1_name"] = [r[0] for r in qc03_results]
        placements_by_person["player2_name"] = [r[1] for r in qc03_results]
        placements_by_person["team_display_name"] = [r[2] for r in qc03_results]

        # POST-QC03 HARD GATE: qc03_guardrail may have re-split "+" names,
        # creating player2_name on rows that were previously blank.
        # Force competitor_type='team' for any non-singles row that now has player2.
        _post_p2 = placements_by_person["player2_name"].fillna("").astype(str).str.strip()
        _post_has_p2 = _post_p2 != ""
        _post_is_player = placements_by_person["competitor_type"].fillna("").astype(str).str.strip() == "player"
        _post_is_singles = (
            placements_by_person["division_category"].fillna("").str.lower().str.contains("single", na=False) |
            placements_by_person["division_canon"].fillna("").str.lower().str.contains("single", na=False) |
            placements_by_person["division_raw"].fillna("").str.lower().str.contains("single", na=False)
        )
        _post_needs_team = _post_has_p2 & _post_is_player & (~_post_is_singles)
        if _post_needs_team.any():
            print(f"  Post-QC03 hard gate: forcing {int(_post_needs_team.sum())} rows to competitor_type='team'")
            placements_by_person.loc[_post_needs_team, "competitor_type"] = "team"

        # Clear team_display_name for non-team rows (it's a display-name artifact, not a team indicator)
        _ct_final = placements_by_person["competitor_type"].fillna("").astype(str).str.strip()
        placements_by_person.loc[_ct_final != "team", "team_display_name"] = ""

        # Unpresentable filter: drop rows with no presentable name (solo blank or team with both blank)
        ct = placements_by_person["competitor_type"].fillna("").astype(str).str.strip()
        is_team = ct == "team"
        p1_name = placements_by_person["player1_name"].fillna("").astype(str).str.strip()
        p2_name = placements_by_person["player2_name"].fillna("").astype(str).str.strip()
        drop_mask = (p1_name == "") | (is_team & (p2_name == ""))
        excluded = placements_by_person.loc[drop_mask].copy()
        kept = placements_by_person.loc[~drop_mask].copy()

        (out_dir / "qc").mkdir(parents=True, exist_ok=True)
        excluded.to_csv(out_dir / "qc" / "excluded_results_rows_unpresentable.csv", index=False, na_rep="")
        out_by_person = out_dir / "Placements_ByPerson.csv"
        kept.to_csv(out_by_person, index=False, na_rep="")
        print(f"Wrote: {out_by_person} ({len(kept)} rows)")
        print(f"Wrote: {out_dir / 'qc' / 'excluded_results_rows_unpresentable.csv'} ({len(excluded)} excluded)")

    if len(edges_df) > 0:
        edges_df = edges_df.sort_values(by=["name_key", "player_id_a", "player_id_b"], kind="mergesort")
        edges_df.to_csv(edges_path, index=False)
        alias_groups = int(edges_df["name_key"].nunique())
    else:
        edges_df = pd.DataFrame(
            columns=["name_key", "player_id_a", "player_name_a", "player_id_b", "player_name_b", "reason", "confidence"]
        )
        edges_df.to_csv(edges_path, index=False)
        alias_groups = 0

    # QC1: country_clean not in allowlist (should drop dramatically with expanded allowlist)
    with_cc = out_df[out_df["country_clean"].notna() & (out_df["country_clean"].astype(str).str.strip() != "")]
    qc1_count = int((~with_cc["country_clean"].astype(str).str.upper().isin(STRICT_COUNTRY_ALLOWLIST)).sum())

    # QC2: looks like US state got mapped to country (IL→ISR, CO→COL in US context)
    # Only treat as state token when in state-like context: (XX USA or XX, USA (not DE, PA CAN)
    raw_fill = out_df["player_name_raw"].fillna("")
    state = raw_fill.str.extract(r"\(\s*([A-Z]{2})\s*USA\b", expand=False)
    state = state.fillna(raw_fill.str.extract(r"\b([A-Z]{2}),\s*USA\b", expand=False))
    bad_state = out_df[
        state.notna()
        & state.isin(US_STATES)
        & out_df["country_clean"].notna()
        & (out_df["country_clean"].astype(str).str.strip() != "")
    ]
    qc2_count = int(len(bad_state))

    # QC3: name_status == "ok" must not have player_name_clean exactly ^[A-Z]{2,4}$
    ok_mask = out_df["name_status"] == "ok"
    clean_stripped = out_df["player_name_clean"].fillna("").astype(str).str.strip()
    qc3_ok_acronym_violation = int((ok_mask & clean_stripped.str.fullmatch(r"[A-Z]{2,4}")).sum())

    qc = {
        "players_total": int(len(out_df)),
        "status_counts": dict(status_counts),
        "junk_reasons": dict(junk_reasons),
        "country_extracted_or_normalized": int(country_extracted),
        "alias_edges": int(len(edges_df)),
        "alias_groups": int(alias_groups),
        "qc_country_not_in_allowlist": qc1_count,
        "qc_state_mapped_as_country": qc2_count,
        "qc_ok_acronym_violation": qc3_ok_acronym_violation,
        "inputs": {
            "players_csv": str(players_csv),
            "events_csv": str(Path(args.events_csv)),
        },
        "outputs": {
            "players_clean_csv": str(out_path),
            "alias_edges_csv": str(edges_path),
            "qc_summary_json": str(qc_path),
        },
    }
    qc_path.write_text(json.dumps(qc, indent=2, sort_keys=True), encoding="utf-8")

    print(f"Wrote: {out_path}")
    print(f"Wrote: {edges_path}")
    print(f"Wrote: {qc_path}")
    print(f"Status counts: {dict(status_counts)}")
    print(f"QC: country_not_in_allowlist={qc1_count}, state_mapped_as_country={qc2_count}, ok_acronym_violation={qc3_ok_acronym_violation}")
    if junk_reasons:
        print(f"Top junk reasons: {junk_reasons.most_common(10)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
