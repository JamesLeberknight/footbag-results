#!/usr/bin/env python3
"""
12_build_player_metadata.py

Produces out/player_metadata.csv — enriched player profile for every canonical
person in Persons_Truth who has at least one placement in Placements_Flat.

Output schema
─────────────
person_id            UUID from Persons_Truth (effective_person_id)
person_canon         Canonical display name
country_primary      Modal competition country (proxy for nationality)
first_event_year     Earliest year in Placements_Flat
last_event_year      Most recent year in Placements_Flat
events_count         Distinct event_ids in Placements_Flat
wins_count           Placements where place = 1
podiums_count        Placements where place ≤ 3
bap_nickname         BAP nickname if member, empty otherwise
fbhof_member         True / False (plain boolean)
aliases_presentable  Presentable alias list from Persons_Truth
name_variants_count  Distinct raw spellings seen in source data
person_quality_flag  clean | needs_alias_review | multi_source_name_variation
                     | manual_override
legacyid             Legacy integer ID from Persons_Truth (nullable)

Derivation notes
────────────────
country_primary
  Placements_Flat.event_id → events_normalized.legacy_event_id → .country
  Most-frequent non-Global country across all of the person's placements.
  "Global" (online / multi-country events) is excluded from the tally.
  Ties broken alphabetically for reproducibility.
  If no events link to a country, field is empty.

  Interpretation: "primary competition country", a reliable proxy for
  nationality for players who mostly compete domestically.  International
  travellers will show their most-visited country, which may differ from
  their home country.  Label accordingly in any downstream display.

person_quality_flag
  manual_override         source field contains "manual" keyword, OR
                          notes field contains "manual"
  multi_source_name_variation
                          ≥ 5 distinct normalised raw name spellings in
                          player_names_seen AND no override in source
                          (many spellings without a manual resolution suggest
                          a merge that may need verification)
  needs_alias_review      has unresolved aliases (aliases field non-empty)
                          AND source does not contain "override"
  clean                   all other cases

Inputs (all under BASE_DIR)
───────────────────────────
  out/Persons_Truth.csv
  out/Placements_Flat.csv
  out/canonical/events_normalized.csv
  inputs/bap_data_updated.csv
  inputs/fbhof_data_updated.csv

Output
──────
  out/player_metadata.csv
"""

from __future__ import annotations

import csv
import os
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)

BASE_DIR   = Path(__file__).resolve().parent.parent
PT_CSV     = BASE_DIR / "out" / "Persons_Truth.csv"
PF_CSV     = BASE_DIR / "out" / "Placements_Flat.csv"
EVENTS_CSV = BASE_DIR / "out" / "canonical" / "events_normalized.csv"
BAP_CSV    = BASE_DIR / "inputs" / "bap_data_updated.csv"
FBHOF_CSV  = BASE_DIR / "inputs" / "fbhof_data_updated.csv"
OUT_CSV    = BASE_DIR / "out" / "player_metadata.csv"

OUTPUT_COLUMNS = [
    "person_id", "person_canon",
    "country_primary",
    "first_event_year", "last_event_year", "events_count",
    "wins_count", "podiums_count",
    "bap_nickname", "fbhof_member",
    "aliases_presentable", "name_variants_count",
    "person_quality_flag",
    "legacyid",
]


# ─────────────────────────────────────────────────────────────────────────────
# Normalisation
# ─────────────────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode()
    return " ".join(s.lower().split())


# Honour-CSV name → Persons_Truth person_canon  (manual overrides)
_HONOR_OVERRIDES: dict[str, str] = {
    "ken shults":               "Kenneth Shults",
    "kenny shults":             "Kenneth Shults",
    "vasek klouda":             "Václav Klouda",
    "vaclav (vasek) klouda":    "Václav Klouda",
    "tina aberli":              "Tina Aeberli",
    "eli piltz":                "Eliott Piltz Galán",
    "eliott piltz galan":       "Eliott Piltz Galán",
    "evanne lamarch":           "Evanne Lemarche",
    "evanne lamarche":          "Evanne Lemarche",
    "arek dzudzinski":          "Arkadiusz Dudzinski",
    "martin cote":              "Martin Côté",
    "sebastien duchesne":       "Sébastien Duchesne",
    "sebastien duschesne":      "Sébastien Duchesne",
    "lon smith":                "Lon Skyler Smith",
    "lon skyler smith":         "Lon Skyler Smith",
    "ales zelinka":             "Aleš Zelinka",
    "jere vainikka":            "Jere Väinikkä",
    "tuomas karki":             "Tuomas Kärki",
    "rafal kaleta":             "Rafał Kaleta",
    "pawel nowak":              "Paweł Nowak",
    "jakub mosciszewski":       "Jakub Mościszewski",
    "dominik simku":            "Dominik Šimků",
    "honza weber":              "Jan Weber",
    "becca english":            "Becca English",
    "becca english-ross":       "Becca English",
    "pt lovern":                "P.T. Lovern",
    "p.t. lovern":              "P.T. Lovern",
    "kendall kic":              "Kendall KIC",
    "wiktor debski":            "Wiktor Dębski",
    "florian gotze":            "Florian Götze",
    "genevieve bousquet":       "Geneviève Bousquet",
    "lori jean conover":        "Lori Jean Conover",
    "jody badger welch":        "Jody Badger Welch",
    "heather squires thomas":   "Heather Squires Thomas",
    "lisa mcdaniel jones":      "Lisa McDaniel Jones",
    "scott-mag hughes":         "Scott-Mag Hughes",
    "carol wedemeyer":          "Carol Wedemeyer",
    "cheryl aubin hughes":      "Cheryl Aubin Hughes",
}


def _resolve_honor_name(raw: str, canon_by_norm: dict[str, str]) -> str | None:
    key = _norm(raw)
    if key in _HONOR_OVERRIDES:
        pc = _HONOR_OVERRIDES[key]
        return pc
    return canon_by_norm.get(key)


# ─────────────────────────────────────────────────────────────────────────────
# Persons Truth
# ─────────────────────────────────────────────────────────────────────────────

def load_persons_truth() -> tuple[list[dict], dict[str, str]]:
    """
    Returns (rows, norm_key→person_canon mapping).
    Filters out __NON_PERSON__ rows.
    """
    rows: list[dict] = []
    canon_by_norm: dict[str, str] = {}
    with open(PT_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            pc  = r.get("person_canon", "").strip()
            pid = r.get("effective_person_id", "").strip()
            if not pc or pid == "__NON_PERSON__" or pc == "__NON_PERSON__":
                continue
            excl = r.get("exclusion_reason", "") or ""
            if "non_person" in excl.lower():
                continue
            rows.append(r)
            canon_by_norm[_norm(pc)] = pc
            nk = r.get("norm_key", "").strip()
            if nk:
                canon_by_norm[nk] = pc
    return rows, canon_by_norm


# ─────────────────────────────────────────────────────────────────────────────
# Event country index
# ─────────────────────────────────────────────────────────────────────────────

def build_event_country_index() -> dict[str, str]:
    """Returns {legacy_event_id (str): country}. Global/blank entries omitted."""
    idx: dict[str, str] = {}
    with open(EVENTS_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid     = r.get("legacy_event_id", "").strip()
            country = r.get("country", "").strip()
            if eid and country and country.lower() != "global":
                idx[eid] = country
    return idx


# ─────────────────────────────────────────────────────────────────────────────
# Placement aggregation
# ─────────────────────────────────────────────────────────────────────────────

def aggregate_placements(
    event_country: dict[str, str],
) -> dict[str, dict]:
    """
    Returns {person_id: stats_dict} where stats_dict has:
      events, wins, podiums, years, country_votes (Counter)
    """
    agg: dict[str, dict] = defaultdict(lambda: {
        "events":        set(),
        "wins":          0,
        "podiums":       0,
        "years":         set(),
        "country_votes": Counter(),
    })

    with open(PF_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = row.get("person_id", "").strip()
            if not pid or pid == "__NON_PERSON__":
                continue
            eid = row.get("event_id", "").strip()
            yr  = row.get("year",     "").strip()
            try:
                place = int(row.get("place", 0) or 0)
            except ValueError:
                place = 0

            s = agg[pid]
            if eid:
                s["events"].add(eid)
                country = event_country.get(eid)
                if country:
                    s["country_votes"][country] += 1
            if yr:
                try:
                    s["years"].add(int(yr))
                except ValueError:
                    pass
            if place == 1:
                s["wins"] += 1
            if 1 <= place <= 3:
                s["podiums"] += 1

    # Flatten
    result: dict[str, dict] = {}
    for pid, s in agg.items():
        years = sorted(s["years"])
        # country_primary: most common, ties broken alphabetically
        votes = s["country_votes"]
        if votes:
            max_count = max(votes.values())
            candidates = sorted(c for c, n in votes.items() if n == max_count)
            country_primary = candidates[0]
        else:
            country_primary = ""
        result[pid] = {
            "events_count":     len(s["events"]),
            "wins_count":       s["wins"],
            "podiums_count":    s["podiums"],
            "first_event_year": years[0]  if years else None,
            "last_event_year":  years[-1] if years else None,
            "country_primary":  country_primary,
        }
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Honours
# ─────────────────────────────────────────────────────────────────────────────

def load_bap_nicknames(canon_by_norm: dict[str, str]) -> dict[str, str]:
    """Returns {person_canon: nickname}."""
    result: dict[str, str] = {}
    unmatched: list[str]   = []
    with open(BAP_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            raw  = r.get("name",     "").strip()
            nick = r.get("nickname", "").strip()
            pc   = _resolve_honor_name(raw, canon_by_norm)
            if pc:
                result[pc] = nick
            else:
                unmatched.append(raw)
    if unmatched:
        print(f"  [WARN] BAP names unmatched ({len(unmatched)}): "
              + ", ".join(unmatched[:5])
              + (" …" if len(unmatched) > 5 else ""))
    return result


def load_fbhof_members(canon_by_norm: dict[str, str]) -> set[str]:
    """Returns set of person_canon strings who are FBHOF members."""
    members: list[str] = []
    unmatched: list[str] = []
    with open(FBHOF_CSV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            raw = r.get("name", "").strip()
            pc  = _resolve_honor_name(raw, canon_by_norm)
            if pc:
                members.append(pc)
            else:
                unmatched.append(raw)
    if unmatched:
        print(f"  [WARN] FBHOF names unmatched ({len(unmatched)}): "
              + ", ".join(unmatched[:5])
              + (" …" if len(unmatched) > 5 else ""))
    return set(members)


# ─────────────────────────────────────────────────────────────────────────────
# Person quality flag
# ─────────────────────────────────────────────────────────────────────────────

def compute_quality_flag(
    names_seen:       str,
    aliases:          str,
    alias_statuses:   str,
    source:           str,
    notes:            str,
) -> str:
    """
    Returns one of: clean | needs_alias_review | multi_source_name_variation
                    | manual_override

    Rules (first match wins):
      manual_override         'manual' appears in source or notes
      multi_source_name_variation
                              ≥ 5 distinct normalised raw name spellings AND
                              source does not contain 'override'
                              (many unresolved spellings = merge risk)
      needs_alias_review      aliases non-empty AND source has no 'override'
                              (aliases present but not yet in override file)
      clean                   all other cases
    """
    source_l = (source or "").lower()
    notes_l  = (notes  or "").lower()

    # 1. Manual override
    if "manual" in source_l or "manual" in notes_l:
        return "manual_override"

    # 2. Count distinct normalised name variants
    raw_names     = [n.strip() for n in names_seen.split("|") if n.strip()]
    norm_variants = len(set(_norm(n) for n in raw_names))

    has_override = "override" in source_l

    if norm_variants >= 5 and not has_override:
        return "multi_source_name_variation"

    # 3. Has aliases but they aren't in the override file
    alias_list = [a.strip() for a in aliases.split("|") if a.strip()] if aliases else []
    if alias_list and not has_override:
        return "needs_alias_review"

    return "clean"


def count_name_variants(names_seen: str) -> int:
    parts = [n.strip() for n in names_seen.split("|") if n.strip()]
    return len(set(_norm(n) for n in parts))


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Loading Persons_Truth…")
    pt_rows, canon_by_norm = load_persons_truth()
    print(f"  {len(pt_rows)} canonical persons (non-NON_PERSON)")

    print("Building event→country index…")
    event_country = build_event_country_index()
    print(f"  {len(event_country)} events with country")

    print("Aggregating Placements_Flat…")
    placement_stats = aggregate_placements(event_country)
    print(f"  {len(placement_stats)} persons with placements")

    print("Loading BAP data…")
    bap_nicknames = load_bap_nicknames(canon_by_norm)
    print(f"  {len(bap_nicknames)} BAP members matched")

    print("Loading FBHOF data…")
    fbhof_members = load_fbhof_members(canon_by_norm)
    print(f"  {len(fbhof_members)} FBHOF members matched")

    print("Building player_metadata.csv…")
    rows_written = 0
    flag_counts: Counter = Counter()

    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for r in pt_rows:
            pid = r.get("effective_person_id", "").strip()
            pc  = r.get("person_canon", "").strip()

            stats = placement_stats.get(pid)
            if not stats:
                continue  # skip persons with no placements in PF

            names_seen     = r.get("player_names_seen",  "") or ""
            aliases        = r.get("aliases",            "") or ""
            alias_statuses = r.get("alias_statuses",     "") or ""
            source         = r.get("source",             "") or ""
            notes          = r.get("notes",              "") or ""

            n_variants = count_name_variants(names_seen)
            flag       = compute_quality_flag(
                names_seen, aliases, alias_statuses, source, notes
            )
            flag_counts[flag] += 1

            row = {
                "person_id":           pid,
                "person_canon":        pc,
                "country_primary":     stats["country_primary"],
                "first_event_year":    stats["first_event_year"] or "",
                "last_event_year":     stats["last_event_year"]  or "",
                "events_count":        stats["events_count"],
                "wins_count":          stats["wins_count"],
                "podiums_count":       stats["podiums_count"],
                "bap_nickname":        bap_nicknames.get(pc, ""),
                "fbhof_member":        "True" if pc in fbhof_members else "False",
                "aliases_presentable": r.get("aliases_presentable", "") or "",
                "name_variants_count": n_variants,
                "person_quality_flag": flag,
                "legacyid":            r.get("legacyid", "") or "",
            }
            writer.writerow(row)
            rows_written += 1

    print(f"\n  → {OUT_CSV}")
    print(f"  {rows_written} rows written")
    print("\n  Quality flag distribution:")
    for flag, count in sorted(flag_counts.items(), key=lambda x: -x[1]):
        print(f"    {flag:<32}  {count}")

    # ── Sanity checks ─────────────────────────────────────────────────────────
    print("\n  Sanity checks:")
    bap_in_output   = sum(1 for r in pt_rows
                          if r.get("person_canon") in bap_nicknames
                          and r.get("effective_person_id") in placement_stats)
    fbhof_in_output = sum(1 for r in pt_rows
                          if r.get("person_canon") in fbhof_members
                          and r.get("effective_person_id") in placement_stats)
    print(f"    BAP members in output:   {bap_in_output} / {len(bap_nicknames)}")
    print(f"    FBHOF members in output: {fbhof_in_output} / {len(fbhof_members)}")
    print(f"    Persons skipped (no PF): "
          f"{len(pt_rows) - rows_written}")


if __name__ == "__main__":
    main()
