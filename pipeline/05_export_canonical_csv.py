#!/usr/bin/env python3
"""
pipeline/05_export_canonical_csv.py

Export canonical relational CSVs from pipeline outputs.

Inputs (all in out/):
  stage2_canonical_events.csv   — event metadata + team structure in placements_json
  Placements_ByPerson.csv       — identity-locked placements (not directly used;
                                   person_id is resolved via PT token lookup instead)
  Coverage_ByEventDivision.csv  — coverage flags per (event, division)
  Persons_Truth.csv             — canonical person records

Outputs (in out/canonical/):
  events.csv                    — one row per event
  event_disciplines.csv         — one row per discipline within an event
  event_results.csv             — one row per placement slot (deduped across ties)
  event_result_participants.csv — one row per participant in a placement slot
  persons.csv                   — canonical person export

Natural keys:
  events:              event_key
  event_disciplines:   (event_key, discipline_key)
  event_results:       (event_key, discipline_key, placement)
  event_result_participants: (event_key, discipline_key, placement, participant_order)

Notes on ties:
  When multiple players/teams share a placement number, they all map to the same
  event_results row. participant_order increments sequentially across all participants
  at that placement slot (e.g., two tied singles players → orders 1,2; two tied
  doubles teams → orders 1,2,3,4). The DB loader can reconstruct team membership
  using the team_type field from event_disciplines.csv.

Notes on unresolved persons:
  Participants without a person_id in Persons_Truth appear with person_id="" and
  display_name set to the raw name from stage2.
"""
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "out"
CANONICAL = OUT / "canonical"

csv.field_size_limit(10_000_000)


# ── Helpers ───────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    """Lowercase, ASCII-safe slug. Collapses non-alphanumeric runs to underscores."""
    s = text.lower().strip()
    # Replace common separators and punctuation with space first
    s = re.sub(r"['\u2019\u2018\u201c\u201d]", "", s)        # strip apostrophes/quotes
    s = re.sub(r"[^a-z0-9]+", "_", s)                         # non-alphanum → _
    s = re.sub(r"_+", "_", s).strip("_")                      # collapse & trim
    return s[:80]


def infer_team_type(division_canon: str) -> str:
    dl = division_canon.lower()
    if re.search(r"\bdoubles?\b|\bpairs?\b|\bdoble\b|\bdobles\b|\bdouble\b", dl):
        return "doubles"
    if re.search(r"\bteam\b", dl):
        return "team"
    return "singles"


def parse_location(location: str) -> tuple[str, str, str]:
    """
    Best-effort parse of a raw location string into (city, region, country).

    Handles:
      "City, Region, Country"      → ("City", "Region", "Country")
      "Region, Country"            → ("", "Region", "Country")
      "City, Country"              → ("City", "", "Country")   (when last part is known country-like)
      "Country"                    → ("", "", "Country")
    """
    if not location:
        return "", "", ""
    # Some locations have multi-part oddities like "Salem, OR / Harrisburg, PA, USA"
    # Just take the first segment before "/" if present
    location = location.split("/")[0].strip()
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[-1]
    if len(parts) == 2:
        return parts[0], "", parts[1]
    if len(parts) == 1:
        return "", "", parts[0]
    return "", "", ""


def derive_status(placements_count: int, coverage_flags: list[str]) -> str:
    """
    "no_results"  — event has no placements in our dataset
    "completed"   — event ran; we have results (coverage may vary)
    """
    if placements_count == 0:
        return "no_results"
    return "completed"


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {path.name} ({len(rows):,} rows)")


# ── Load Persons_Truth — build resolution indexes ─────────────────────────────

print("Loading Persons_Truth.csv...")
token_to_person: dict[str, str] = {}   # player_token_uuid → effective_person_id
names_to_person: dict[str, str] = {}   # norm(player_name_seen) → effective_person_id
pt_rows: list[dict] = []
with open(OUT / "Persons_Truth.csv", newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        pt_rows.append(row)
        pid = row["effective_person_id"]
        for tok in row["player_ids_seen"].split(" | "):
            tok = tok.strip()
            if tok:
                token_to_person[tok] = pid
        for name in row["player_names_seen"].split(" | "):
            name = name.strip()
            if name:
                n = name.lower().strip()
                n = re.sub(r"\s+", " ", n)
                names_to_person[n] = pid
print(f"  {len(pt_rows):,} persons, {len(token_to_person):,} tokens, {len(names_to_person):,} names indexed")


def resolve_person_id(player_id: str | None, player_name: str) -> str:
    """
    Three-level resolution:
      1. player_id (UUID5 token) → PT player_ids_seen  (exact, fast)
      2. norm(player_name)       → PT player_names_seen  (catches alias variants)
      3. "" — genuinely unresolved (noise, handles, city names, etc.)
    """
    if player_id and player_id in token_to_person:
        return token_to_person[player_id]
    if player_name:
        n = re.sub(r"\s+", " ", player_name.lower().strip())
        if n in names_to_person:
            return names_to_person[n]
    return ""


# ── Load Coverage ─────────────────────────────────────────────────────────────

print("Loading Coverage_ByEventDivision.csv...")
coverage: dict[tuple[str, str], str] = {}  # (event_id, division_canon) → coverage_flag
with open(OUT / "Coverage_ByEventDivision.csv", newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        coverage[(row["event_id"], row["division_canon"])] = row["coverage_flag"]
print(f"  {len(coverage):,} (event, division) coverage flags")


# ── Load Stage2 ───────────────────────────────────────────────────────────────

print("Loading stage2_canonical_events.csv...")
with open(OUT / "stage2_canonical_events.csv", newline="", encoding="utf-8") as f:
    stage2_rows = list(csv.DictReader(f))
print(f"  {len(stage2_rows):,} events")


# ── Generate event_key slugs (with collision detection) ───────────────────────

slug_to_eids: dict[str, list[str]] = defaultdict(list)
for row in stage2_rows:
    year  = row["year"] or "unknown"
    slug  = slugify(row["event_name"]) if row["event_name"] else row["event_id"]
    candidate = f"event_{year}_{slug}"
    slug_to_eids[candidate].append(row["event_id"])

event_key_map: dict[str, str] = {}  # event_id → event_key
for candidate, eids in slug_to_eids.items():
    if len(eids) == 1:
        event_key_map[eids[0]] = candidate
    else:
        # Disambiguate: append last 4 digits of event_id
        for eid in eids:
            event_key_map[eid] = f"{candidate}_{eid[-4:]}"

collisions = sum(1 for eids in slug_to_eids.values() if len(eids) > 1)
if collisions:
    print(f"  NOTE: {collisions} slug collision group(s) disambiguated with event_id suffix")


# ── Build output rows ─────────────────────────────────────────────────────────

events_out:       list[dict] = []
disciplines_out:  list[dict] = []
results_out:      list[dict] = []
participants_out: list[dict] = []

# Sort events by year, then event_name for stable output
sorted_rows = sorted(
    stage2_rows,
    key=lambda r: (r["year"] or "0000", r["event_name"] or "")
)

for row in sorted_rows:
    eid        = row["event_id"]
    event_key  = event_key_map[eid]
    year       = row["year"] or ""
    event_name = row["event_name"] or ""
    location   = row.get("location", "") or ""
    city, region, country = parse_location(location)
    pj = json.loads(row.get("placements_json", "[]"))

    # Ordered unique divisions (preserving first-appearance order from parsed results)
    seen_divs: list[str] = []
    for p in pj:
        d = p["division_canon"]
        if d not in seen_divs:
            seen_divs.append(d)

    # Discipline-level coverage flags
    cov_flags = [coverage.get((eid, d), "") for d in seen_divs]

    # ── events.csv ────────────────────────────────────────────────────────────
    events_out.append({
        "event_key":       event_key,
        "legacy_event_id": eid,
        "year":            year,
        "event_name":      event_name,
        "event_slug":      slugify(event_name) if event_name else "",
        "start_date":      "",   # not available in stage2
        "end_date":        "",   # not available in stage2
        "city":            city,
        "region":          region,
        "country":         country,
        "host_club":       row.get("host_club", "") or "",
        "status":          derive_status(len(pj), cov_flags),
        "notes":           "",
        "source":          "mirror",
    })

    # Discipline-key collision detection within this event
    disc_slug_seen: dict[str, int] = {}  # slug → count (for suffix disambiguation)

    # participant_order counter per (event_key, discipline_key, placement)
    part_counter: dict[tuple[str, str, str], int] = defaultdict(int)

    # Result dedup: emit one event_results row per (event_key, disc_key, place)
    emitted_results: set[tuple[str, str, str]] = set()

    for sort_order, div in enumerate(seen_divs, start=1):
        # ── Discipline key (event-scoped) ────────────────────────────────────
        base_slug = slugify(div)
        if base_slug in disc_slug_seen:
            disc_slug_seen[base_slug] += 1
            discipline_key = f"{base_slug}_{disc_slug_seen[base_slug]}"
        else:
            disc_slug_seen[base_slug] = 1
            discipline_key = base_slug

        div_placements = [p for p in pj if p["division_canon"] == div]
        if not div_placements:
            continue

        div_cat   = div_placements[0].get("division_category", "") or ""
        team_type = infer_team_type(div)
        cov_flag  = coverage.get((eid, div), "")

        # ── event_disciplines.csv ─────────────────────────────────────────────
        disciplines_out.append({
            "event_key":           event_key,
            "discipline_key":      discipline_key,
            "discipline_name":     div,
            "discipline_category": div_cat,
            "team_type":           team_type,
            "sort_order":          sort_order,
            "coverage_flag":       cov_flag,
            "notes":               "",
        })

        for p in div_placements:
            place = str(p.get("place", ""))
            if not place:
                continue

            result_key = (event_key, discipline_key, place)

            # ── event_results.csv (one row per placement slot) ────────────────
            if result_key not in emitted_results:
                results_out.append({
                    "event_key":      event_key,
                    "discipline_key": discipline_key,
                    "placement":      place,
                    "score_text":     "",
                    "notes":          "",
                    "source":         "",
                })
                emitted_results.add(result_key)

            # ── event_result_participants.csv ─────────────────────────────────
            p1_name = p.get("player1_name", "") or ""
            p1_id   = p.get("player1_id") or None
            p2_name = p.get("player2_name", "") or ""
            p2_id   = p.get("player2_id") or None

            if p1_name:
                part_counter[result_key] += 1
                participants_out.append({
                    "event_key":         event_key,
                    "discipline_key":    discipline_key,
                    "placement":         place,
                    "participant_order": part_counter[result_key],
                    "display_name":      p1_name,
                    "person_id":         resolve_person_id(p1_id, p1_name),
                    "notes":             "",
                })

            if p2_name:
                part_counter[result_key] += 1
                participants_out.append({
                    "event_key":         event_key,
                    "discipline_key":    discipline_key,
                    "placement":         place,
                    "participant_order": part_counter[result_key],
                    "display_name":      p2_name,
                    "person_id":         resolve_person_id(p2_id, p2_name),
                    "notes":             "",
                })


# ── persons.csv ───────────────────────────────────────────────────────────────

persons_out: list[dict] = []
for row in sorted(pt_rows, key=lambda r: r["person_canon"]):
    persons_out.append({
        "person_id":        row["effective_person_id"],
        "person_canon":     row["person_canon"],
        "aliases":          row.get("aliases_presentable", "") or "",
        "legacy_member_id": row.get("legacyid", "") or "",
        "notes":            row.get("notes", "") or "",
        "source":           row.get("source", "") or "",
    })


# ── Write outputs ─────────────────────────────────────────────────────────────

print(f"\nWriting canonical CSVs to {CANONICAL} ...")
CANONICAL.mkdir(exist_ok=True)

write_csv(
    CANONICAL / "events.csv",
    ["event_key", "legacy_event_id", "year", "event_name", "event_slug",
     "start_date", "end_date", "city", "region", "country",
     "host_club", "status", "notes", "source"],
    events_out,
)
write_csv(
    CANONICAL / "event_disciplines.csv",
    ["event_key", "discipline_key", "discipline_name", "discipline_category",
     "team_type", "sort_order", "coverage_flag", "notes"],
    disciplines_out,
)
write_csv(
    CANONICAL / "event_results.csv",
    ["event_key", "discipline_key", "placement", "score_text", "notes", "source"],
    results_out,
)
write_csv(
    CANONICAL / "event_result_participants.csv",
    ["event_key", "discipline_key", "placement", "participant_order",
     "display_name", "person_id", "notes"],
    participants_out,
)
write_csv(
    CANONICAL / "persons.csv",
    ["person_id", "person_canon", "aliases", "legacy_member_id", "notes", "source"],
    persons_out,
)

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"""
Done.
  events:               {len(events_out):>7,}
  event_disciplines:    {len(disciplines_out):>7,}
  event_results:        {len(results_out):>7,}
  event_result_participants: {len(participants_out):>7,}
  persons:              {len(persons_out):>7,}
""")

# ── Integrity checks ──────────────────────────────────────────────────────────

# Unique keys
result_keys    = [(r["event_key"], r["discipline_key"], r["placement"]) for r in results_out]
part_keys      = [(r["event_key"], r["discipline_key"], r["placement"], r["participant_order"]) for r in participants_out]
disc_keys      = [(r["event_key"], r["discipline_key"]) for r in disciplines_out]

errors = 0
if len(result_keys) != len(set(result_keys)):
    dups = len(result_keys) - len(set(result_keys))
    print(f"ERROR: {dups} duplicate (event, discipline, placement) keys in event_results.csv")
    errors += 1
else:
    print("✓  event_results:    all (event_key, discipline_key, placement) keys unique")

if len(part_keys) != len(set(part_keys)):
    dups = len(part_keys) - len(set(part_keys))
    print(f"ERROR: {dups} duplicate participant keys in event_result_participants.csv")
    errors += 1
else:
    print("✓  event_result_participants: all (event_key, discipline_key, placement, participant_order) keys unique")

if len(disc_keys) != len(set(disc_keys)):
    dups = len(disc_keys) - len(set(disc_keys))
    print(f"ERROR: {dups} duplicate (event_key, discipline_key) keys in event_disciplines.csv")
    errors += 1
else:
    print("✓  event_disciplines: all (event_key, discipline_key) keys unique")

event_keys_set = {r["event_key"] for r in events_out}
orphan_discs   = [r for r in disciplines_out if r["event_key"] not in event_keys_set]
if orphan_discs:
    print(f"ERROR: {len(orphan_discs)} discipline rows with no matching event")
    errors += 1
else:
    print("✓  referential integrity: all discipline event_keys present in events")

if errors:
    import sys
    sys.exit(1)
