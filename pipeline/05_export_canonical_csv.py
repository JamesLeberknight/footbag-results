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

Outputs (in ~/projects/footbag-platform/legacy_data/event_results/canonical_input/):
  events.csv                    — one row per event
  event_disciplines.csv         — one row per discipline within an event
  event_results.csv             — one row per placement slot (deduped across ties)
  event_result_participants.csv — one row per participant in a placement slot
  persons.csv                   — canonical person export (extended: stats, honors, freestyle)

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
import re
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT  = ROOT / "out"
CANONICAL = OUT / "canonical"

csv.field_size_limit(10_000_000)


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_display_str(s: str) -> str:
    """Strip invisible/garbage Unicode from display strings.

    - U+00AD (soft hyphen): zero-width formatting char from HTML &shy; — strip silently.
    - U+FFFD (replacement char): appears when source had &shy; that was mis-decoded.
      When followed by an uppercase letter (e.g. Rou\ufffdTines), lowercase that letter
      so "RouTines" → "Routines".
    """
    # U+FFFD followed by uppercase → lowercase that letter
    s = re.sub(r"\ufffd([A-Z])", lambda m: m.group(1).lower(), s)
    # Strip any remaining U+FFFD or U+00AD
    s = s.replace("\ufffd", "").replace("\u00ad", "")
    return s


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


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def parse_date_range(s: str) -> tuple[str, str]:
    """
    Parse stage2 date strings into ISO start_date and end_date.

    Handles:
      "July 31-August 6, 2011"   → ("2011-07-31", "2011-08-06")
      "August 3-9, 2019"         → ("2019-08-03", "2019-08-09")
      "March 15, 2008"           → ("2008-03-15", "2008-03-15")
    Returns ("", "") if unparseable.
    """
    s = s.strip()
    # Month D-Month D, YYYY
    m = re.match(r"(\w+)\s+(\d+)\s*-\s*(\w+)\s+(\d+),\s*(\d{4})", s)
    if m:
        m1, d1, m2, d2, y = m.groups()
        mo1 = _MONTHS.get(m1.lower())
        mo2 = _MONTHS.get(m2.lower())
        if mo1 and mo2:
            return f"{y}-{mo1:02d}-{int(d1):02d}", f"{y}-{mo2:02d}-{int(d2):02d}"
    # Month D-D, YYYY
    m = re.match(r"(\w+)\s+(\d+)\s*-\s*(\d+),\s*(\d{4})", s)
    if m:
        mn, d1, d2, y = m.groups()
        mo = _MONTHS.get(mn.lower())
        if mo:
            return f"{y}-{mo:02d}-{int(d1):02d}", f"{y}-{mo:02d}-{int(d2):02d}"
    # Month D, YYYY  (single day)
    m = re.match(r"(\w+)\s+(\d+),\s*(\d{4})", s)
    if m:
        mn, d, y = m.groups()
        mo = _MONTHS.get(mn.lower())
        if mo:
            iso = f"{y}-{mo:02d}-{int(d):02d}"
            return iso, iso
    return "", ""


# Regions that are NOT countries — map to (canonical_country, canonical_region)
_REGION_NOT_COUNTRY: dict[str, tuple[str, str]] = {
    "basque country": ("Spain", "Basque Country"),
    "euskadi":        ("Spain", "Basque Country"),
    "pais vasco":     ("Spain", "Basque Country"),
    "catalonia":      ("Spain", "Catalonia"),
    "cataluña":       ("Spain", "Catalonia"),
    "scotland":       ("United Kingdom", "Scotland"),
    "wales":          ("United Kingdom", "Wales"),
    "england":        ("United Kingdom", "England"),
    "northern ireland": ("United Kingdom", "Northern Ireland"),
}


def parse_location(location: str) -> tuple[str, str, str]:
    """
    Best-effort parse of a raw location string into (city, region, country).

    Handles:
      "City, Region, Country"      → ("City", "Region", "Country")
      "Region, Country"            → ("", "Region", "Country")
      "City, Country"              → ("City", "", "Country")   (when last part is known country-like)
      "Country"                    → ("", "", "Country")

    Post-processing:
      Any part that matches _REGION_NOT_COUNTRY is replaced with the canonical
      country, and the region is set to the canonical region name.
      e.g. "Bizkaia, Basque Country" → city="Bizkaia", region="Basque Country", country="Spain"
    """
    if not location:
        return "", "", ""
    # Some locations have multi-part oddities like "Salem, OR / Harrisburg, PA, USA"
    # Just take the first segment before "/" if present
    location = location.split("/")[0].strip()
    parts = [p.strip() for p in location.split(",") if p.strip()]
    if len(parts) >= 3:
        city, region, country = parts[0], parts[1], parts[-1]
    elif len(parts) == 2:
        city, region, country = parts[0], "", parts[1]
    elif len(parts) == 1:
        city, region, country = "", "", parts[0]
    else:
        return "", "", ""

    # Normalise: if country is actually a sub-national region, fix it
    country_lc = country.lower().strip()
    if country_lc in _REGION_NOT_COUNTRY:
        canonical_country, canonical_region = _REGION_NOT_COUNTRY[country_lc]
        # preserve any existing region from the string; fall back to canonical region
        region = region or canonical_region
        if not region:
            region = canonical_region
        country = canonical_country

    # Also check if city itself is a known region-not-country (e.g. "Country, Spain")
    city_lc = city.lower().strip()
    if city_lc in _REGION_NOT_COUNTRY and not region:
        canonical_country, canonical_region = _REGION_NOT_COUNTRY[city_lc]
        region = canonical_region
        city = ""

    return city, region, country


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

# player_ids_seen lookup (for persons.csv export)
pt_player_ids: dict[str, str] = {}    # effective_person_id → pipe-sep player_ids_seen
for row in pt_rows:
    pt_player_ids[row["effective_person_id"]] = row["player_ids_seen"]

# ── Load member_id assignments ─────────────────────────────────────────────────

print("Loading member_id_assignments.csv...")
_member_id_csv = ROOT / "out" / "member_id_enrichment" / "member_id_assignments.csv"
member_id_map: dict[str, str] = {}   # effective_person_id → footbag.org member_id
if _member_id_csv.exists():
    with open(_member_id_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pid = row.get("effective_person_id", "").strip()
            mid = row.get("member_id", "").strip()
            if pid and mid:
                member_id_map[pid] = mid
    print(f"  {len(member_id_map):,} persons with member_id")
else:
    print(f"  (member_id_assignments.csv not found — skipping)")


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


# ── Load events_normalized for curated location / metadata overrides ──────────
# Keyed by legacy_event_id. New events not yet in this file fall back to
# parse_location() automatically — no manual step required for new data.

print("Loading events_normalized.csv...")
_norm_csv = ROOT / "inputs" / "events_normalized.csv"
events_normalized: dict[str, dict] = {}   # legacy_event_id → row
if _norm_csv.exists():
    with open(_norm_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            eid = row.get("legacy_event_id", "").strip()
            if eid:
                events_normalized[eid] = row
    print(f"  {len(events_normalized):,} events with normalized location/metadata")
else:
    print(f"  (events_normalized.csv not found — all locations will be auto-parsed)")


# ── Load Stage2 ───────────────────────────────────────────────────────────────

print("Loading stage2_canonical_events.csv...")
with open(OUT / "stage2_canonical_events.csv", newline="", encoding="utf-8") as f:
    stage2_rows = list(csv.DictReader(f))
print(f"  {len(stage2_rows):,} events")


# ── Generate event_key slugs (human-readable, collision-safe) ─────────────────

def normalize_country(country: str) -> str:
    c = slugify(country).replace("_", "-")
    mapping = {
        "united-states": "usa",
        "usa":            "usa",
        "us":             "usa",
        "u-s-a":          "usa",
        "u-s":            "usa",
        "united-kingdom": "uk",
        "great-britain":  "uk",
    }
    return mapping.get(c, c or "unknown-country")


def normalize_descriptor(event_name: str) -> str:
    n = (event_name or "").lower().strip()
    rules = [
        (r"\b(world|worlds|world championship|world championships|ifpa world)\b", "worlds"),
        (r"\b(us open|u\.s\. open|u s open)\b",                                  "us-open"),
        (r"\beuropean championship|\beuropean championships|\beuros?\b",           "euros"),
        (r"\basian championship|\basian championships|\basia\b",                   "asia"),
        (r"\bcanadian championship|\bcanadian championships|\bcanada\b",           "canada"),
        (r"\bnational championship|\bnational championships|\bnationals?\b",       "nationals"),
        (r"\bfootbag open\b|\bopen\b",                                             "open"),
        (r"\bchampionship\b|\bchampionships\b",                                    "championships"),
    ]
    for pattern, label in rules:
        if re.search(pattern, n):
            return label
    stop = {"the", "and", "of", "footbag", "freestyle", "net",
            "championship", "championships"}
    words = [w for w in slugify(event_name).replace("_", "-").split("-") if w]
    useful = [w for w in words if w not in stop]
    return "-".join(useful[:3]) if useful else "event"


def make_candidate_event_key(year: str, event_name: str, city: str, country: str) -> str:
    year_slug    = (year or "unknown-year").strip()
    descriptor   = normalize_descriptor(event_name)
    city_slug    = slugify(city).replace("_", "-") or "unknown-city"
    country_slug = normalize_country(country)
    return f"{year_slug}-{descriptor}-{city_slug}-{country_slug}"


candidate_to_eids: dict[str, list[str]] = defaultdict(list)
candidate_map:     dict[str, str]       = {}

for row in stage2_rows:
    eid  = row["event_id"]
    year = row["year"] or "unknown-year"
    city, _region, country = parse_location(row.get("location", "") or "")
    candidate = make_candidate_event_key(year, row.get("event_name", ""), city, country)
    candidate_map[eid] = candidate
    candidate_to_eids[candidate].append(eid)

event_key_map: dict[str, str] = {}
for candidate, eids in candidate_to_eids.items():
    if len(eids) == 1:
        event_key_map[eids[0]] = candidate
    else:
        for eid in eids:
            event_key_map[eid] = f"{candidate}-{eid}"

collisions = sum(1 for eids in candidate_to_eids.values() if len(eids) > 1)
if collisions:
    print(f"  NOTE: {collisions} event-key collision group(s) disambiguated with legacy_event_id suffix")


# ── Load PBP — authoritative source for participant/discipline data ────────────
# Replaces reading placements_json from stage2. PBP is the identity-locked gold
# standard: canonical person names, __NON_PERSON__ markers, all manual patches.
# stage2 is used only for event metadata (name, date) and event_key generation.

print("Loading Placements_ByPerson.csv (authoritative participant source)...")

# event_id → country for person stats; prefer curated events_normalized location
_eid_country: dict[str, str] = {}
for _r in stage2_rows:
    _n     = events_normalized.get(_r["event_id"])
    _cntry = (_n.get("country", "") if _n else "") or ""
    if not _cntry:
        _, _, _cntry = parse_location(_r.get("location", "") or "")
    if _cntry:
        _eid_country[_r["event_id"]] = _cntry

pbp_by_event: dict[str, list[dict]] = defaultdict(list)
_pbp_stats:   dict[str, dict]       = {}

with open(OUT / "Placements_ByPerson.csv", newline="", encoding="utf-8") as _f:
    for _row in csv.DictReader(_f):
        _eid = _row["event_id"]
        pbp_by_event[_eid].append(_row)
        _pid = (_row.get("person_id") or "").strip()
        if _pid and _pid != "__NON_PERSON__":
            _yr    = _row.get("year", "")
            _cntry = _eid_country.get(_eid, "")
            if _pid not in _pbp_stats:
                _pbp_stats[_pid] = {
                    "years": set(), "event_ids": set(),
                    "placement_count": 0, "countries": Counter(),
                }
            _s = _pbp_stats[_pid]
            _s["placement_count"] += 1
            _s["event_ids"].add(_eid)
            if _yr:
                try:   _s["years"].add(int(_yr))
                except ValueError: pass
            if _cntry:
                _s["countries"][_cntry] += 1

_pbp_total = sum(len(v) for v in pbp_by_event.values())
print(f"  {_pbp_total:,} placement rows, {len(pbp_by_event):,} events covered")


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
    start_date, end_date = parse_date_range(row.get("date", "") or "")

    # Location: curated events_normalized → fall back to stage2 parse
    _norm = events_normalized.get(eid)
    if _norm:
        city       = _norm.get("city", "")    or ""
        region     = _norm.get("region", "")  or ""
        country    = _norm.get("country", "") or ""
        host_club  = _norm.get("host_club", "") or row.get("host_club", "") or ""
        event_type = _norm.get("event_type", "") or row.get("event_type", "") or ""
        if not start_date:
            start_date = _norm.get("start_date", "") or ""
            end_date   = _norm.get("end_date",   "") or ""
    else:
        location   = row.get("location", "") or ""
        city, region, country = parse_location(location)
        host_club  = row.get("host_club", "") or ""
        event_type = row.get("event_type", "") or ""

    # ── PBP rows for this event (authoritative: names, IDs, structure) ────────
    event_pbp = pbp_by_event.get(eid, [])

    # Ordered unique divisions from PBP; derive team_type from competitor_type
    seen_divs: list[str]       = []
    div_meta:  dict[str, dict] = {}
    for pbp_row in event_pbp:
        div = pbp_row.get("division_canon") or ""
        if not div:
            continue
        if div not in seen_divs:
            seen_divs.append(div)
            div_meta[div] = {
                "div_cat":  pbp_row.get("division_category", "") or "",
                "team_type": "singles",
                "cov_flag":  pbp_row.get("coverage_flag", "") or "",
            }
        if pbp_row.get("competitor_type", "player") == "team":
            div_meta[div]["team_type"] = "doubles"

    # ── events.csv ────────────────────────────────────────────────────────────
    events_out.append({
        "event_key":       event_key,
        "legacy_event_id": eid,
        "year":            year,
        "event_name":      event_name,
        "event_slug":      slugify(event_name) if event_name else "",
        "start_date":      start_date,
        "end_date":        end_date,
        "city":            city,
        "region":          region,
        "country":         country,
        "host_club":       host_club,
        "event_type":      event_type,
        "status":          derive_status(len(event_pbp), []),
        "notes":           _norm.get("notes", "") if _norm else "",
        "source":          "mirror",
    })

    # Discipline-key slugs — collision-safe within each event
    disc_slug_seen:  dict[str, int] = {}
    div_to_disc_key: dict[str, str] = {}
    for div in seen_divs:
        base_slug = slugify(div)
        if base_slug in disc_slug_seen:
            disc_slug_seen[base_slug] += 1
            disc_key = f"{base_slug}_{disc_slug_seen[base_slug]}"
        else:
            disc_slug_seen[base_slug] = 1
            disc_key = base_slug
        div_to_disc_key[div] = disc_key

    # ── event_disciplines.csv ─────────────────────────────────────────────────
    for sort_order, div in enumerate(seen_divs, start=1):
        meta = div_meta[div]
        disciplines_out.append({
            "event_key":           event_key,
            "discipline_key":      div_to_disc_key[div],
            "discipline_name":     clean_display_str(div),
            "discipline_category": meta["div_cat"],
            "team_type":           meta["team_type"],
            "sort_order":          sort_order,
            "coverage_flag":       meta["cov_flag"],
            "notes":               "",
        })

    # ── event_results + event_result_participants ─────────────────────────────
    # participant_order:
    #   Singles  → always "1"  (tied singles = multiple rows all at order=1)
    #   Doubles  → sequential within each placement slot (1, 2, 3, 4…)
    #              team_person_key included so consumers can reconstruct teams
    # Dedup: skip rows where the same (disc_key, place, person_name) has already
    #        been emitted — PBP occasionally has resolved+unresolved duplicates.
    emitted_results:    set[tuple[str, str, str]]       = set()
    placement_counter:  dict[tuple[str, str, str], int] = defaultdict(int)
    seen_participants:  set[tuple[str, str, str, str]]   = set()

    _UUID_RE = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
    )

    def _clean_pid(pid: str, name: str) -> str:
        """Return a valid UUID person_id, or "" if malformed/unresolvable."""
        if not pid or pid == "__NON_PERSON__":
            return ""
        if _UUID_RE.match(pid):
            return pid
        # Malformed (pipe-separated composite key, truncated |? token, etc.)
        # Try to re-resolve from the canonical name via PT.
        return resolve_person_id(None, name) or ""

    for pbp_row in event_pbp:
        div = pbp_row.get("division_canon") or ""
        if not div:
            continue
        disc_key = div_to_disc_key.get(div, "")
        if not disc_key:
            continue
        place = str(pbp_row.get("place", "")).strip()
        if not place:
            continue

        person_id   = pbp_row.get("person_id", "") or ""
        person_name = pbp_row.get("person_canon", "") or ""
        tpk         = pbp_row.get("team_person_key", "") or ""
        tdm         = pbp_row.get("team_display_name", "") or ""
        is_doubles  = div_meta.get(div, {}).get("team_type") == "doubles"
        result_key  = (event_key, disc_key, place)

        # Expand __NON_PERSON__ team aggregate rows.
        # PBP stores unresolved doubles teams as one row:
        #   person_canon="__NON_PERSON__", team_display_name="Name1 / Name2"
        # Expand into individual participant entries by splitting on " / ".
        if person_name == "__NON_PERSON__":
            if tdm:
                members = [m.strip() for m in tdm.split(" / ") if m.strip()]
            else:
                members = [person_name]   # preserve as __NON_PERSON__ placeholder
            entries = [(m, resolve_person_id(None, m) or "", "") for m in members]
        else:
            entries = [(person_name, _clean_pid(person_id, person_name), tpk)]

        # event_results.csv — one row per placement slot
        if result_key not in emitted_results:
            results_out.append({
                "event_key":      event_key,
                "discipline_key": disc_key,
                "placement":      place,
                "score_text":     "",
                "notes":          "",
                "source":         "",
            })
            emitted_results.add(result_key)

        # event_result_participants.csv — one row per resolved individual
        for (m_name, m_pid, m_tpk) in entries:
            dedup_key = (event_key, disc_key, place, m_name)
            if dedup_key in seen_participants:
                continue
            seen_participants.add(dedup_key)

            if is_doubles:
                placement_counter[result_key] += 1
                participant_order = str(placement_counter[result_key])
            else:
                participant_order = "1"

            participants_out.append({
                "event_key":         event_key,
                "discipline_key":    disc_key,
                "placement":         place,
                "participant_order": participant_order,
                "display_name":      clean_display_str(m_name),
                "person_id":         m_pid,
                "team_person_key":   m_tpk,
                "notes":             "",
            })


# ── persons.csv — extended ────────────────────────────────────────────────────
# _pbp_stats and _eid_country already built during PBP load above.
print(f"  {len(_pbp_stats):,} persons with placements")

# BAP / FBHOF matching helpers (mirrors build_final_workbook_v12 logic)
def _honor_norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())

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
    "genevieve bousquet":       "Geneviève Bousquet",
    "becca english-ross":       "Becca English",
    "pt lovern":                "P.T. Lovern",
    "p.t. lovern":              "P.T. Lovern",
    "kendall kic":              "Kendall KIC",
    "wiktor debski":            "Wiktor Dębski",
    "florian gotze":            "Florian Götze",
    "chantelle laurent":        "Chantelle Laurent",
}

# Build norm → person_id lookup from PT
_norm_to_pid: dict[str, str] = {}
_norm_to_canon: dict[str, str] = {}
for _r in pt_rows:
    _pc  = _r["person_canon"]
    _pid = _r["effective_person_id"]
    _k   = _honor_norm(_pc)
    _norm_to_pid[_k]   = _pid
    _norm_to_canon[_k] = _pc
    _nk = _r.get("norm_key", "").strip()
    if _nk:
        _norm_to_pid[_nk]   = _pid
        _norm_to_canon[_nk] = _pc

def _resolve_honor(raw_name: str) -> str | None:
    """Returns person_id or None."""
    key = _honor_norm(raw_name)
    canon = _HONOR_OVERRIDES.get(key.replace("", "").strip()) or _HONOR_OVERRIDES.get(raw_name.lower().strip())
    if not canon:
        # Try direct key lookup
        if key in _norm_to_canon:
            canon = _norm_to_canon[key]
    if canon:
        return _norm_to_pid.get(_honor_norm(canon))
    return None

# Rebuild using override-first approach
def _match_honor(raw_name: str) -> str | None:
    key = _honor_norm(raw_name)
    override_canon = _HONOR_OVERRIDES.get(raw_name.lower().strip())
    if override_canon:
        return _norm_to_pid.get(_honor_norm(override_canon))
    if key in _norm_to_pid:
        return _norm_to_pid[key]
    return None

# Load BAP
print("Loading BAP data...")
_bap_by_pid: dict[str, dict] = {}
_bap_csv = ROOT / "inputs" / "bap_data_updated.csv"
if _bap_csv.exists():
    with open(_bap_csv, newline="", encoding="utf-8") as _f:
        for _i, _row in enumerate(csv.DictReader(_f), start=1):
            _raw = _row.get("name", "").strip()
            _yr  = _row.get("year_inducted", "").strip()
            _nick = _row.get("nickname", "").strip()
            _pid = _match_honor(_raw)
            if _pid:
                _bap_by_pid[_pid] = {
                    "bap_member": 1,
                    "bap_nickname": _nick,
                    "bap_induction_year": _yr,
                }
print(f"  {len(_bap_by_pid):,} BAP members matched")

# Load FBHOF
print("Loading FBHOF data...")
_fbhof_by_pid: dict[str, dict] = {}
_fbhof_csv = ROOT / "inputs" / "fbhof_data_updated.csv"
if _fbhof_csv.exists():
    with open(_fbhof_csv, newline="", encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            _raw = _row.get("name", "").strip()
            _yr  = _row.get("year_inducted", "").strip()
            _pid = _match_honor(_raw)
            if _pid:
                _fbhof_by_pid[_pid] = {
                    "fbhof_member": 1,
                    "fbhof_induction_year": _yr if _yr.lower() != "unknown" else "",
                }
print(f"  {len(_fbhof_by_pid):,} FBHOF members matched")

# Load freestyle difficulty profiles
print("Loading freestyle analytics...")
_difficulty_by_pid: dict[str, dict] = {}
_diff_csv = OUT / "noise_aggregates" / "player_difficulty_profiles.csv"
if _diff_csv.exists():
    with open(_diff_csv, newline="", encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            _pid = _row.get("person_id", "").strip()
            if _pid:
                _difficulty_by_pid[_pid] = _row

_diversity_by_pid: dict[str, dict] = {}
_div_csv = OUT / "noise_aggregates" / "player_diversity_profiles.csv"
if _div_csv.exists():
    with open(_div_csv, newline="", encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            _pid = _row.get("person_id", "").strip()
            if _pid:
                _diversity_by_pid[_pid] = _row
print(f"  {len(_difficulty_by_pid):,} difficulty profiles, {len(_diversity_by_pid):,} diversity profiles")

# Build persons_out
persons_out: list[dict] = []
for row in sorted(pt_rows, key=lambda r: r["person_canon"]):
    pid   = row["effective_person_id"]
    stats = _pbp_stats.get(pid, {})
    years = sorted(stats.get("years", []))
    countries = stats.get("countries", Counter())
    top_country = countries.most_common(1)[0][0] if countries else ""

    diff  = _difficulty_by_pid.get(pid, {})
    divrs = _diversity_by_pid.get(pid, {})
    bap   = _bap_by_pid.get(pid, {})
    fbhof = _fbhof_by_pid.get(pid, {})

    top_tricks = [t.strip() for t in divrs.get("top_tricks", "").split("|") if t.strip()]

    persons_out.append({
        "person_id":                  pid,
        "person_name":                row["person_canon"],
        "member_id":                  member_id_map.get(pid, ""),
        "player_ids":                 pt_player_ids.get(pid, ""),
        "country":                    top_country,
        "first_year":                 years[0]  if years else "",
        "last_year":                  years[-1] if years else "",
        "event_count":                len(stats.get("event_ids", set())),
        "placement_count":            stats.get("placement_count", 0),
        "bap_member":                 bap.get("bap_member", 0),
        "bap_nickname":               bap.get("bap_nickname", ""),
        "bap_induction_year":         bap.get("bap_induction_year", ""),
        "fbhof_member":               fbhof.get("fbhof_member", 0),
        "fbhof_induction_year":       fbhof.get("fbhof_induction_year", ""),
        "freestyle_sequences":        diff.get("chains_total", ""),
        "freestyle_max_add":          diff.get("max_sequence_add", ""),
        "freestyle_unique_tricks":    divrs.get("unique_tricks", ""),
        "freestyle_diversity_ratio":  divrs.get("diversity_ratio", ""),
        "signature_trick_1":          top_tricks[0] if len(top_tricks) > 0 else "",
        "signature_trick_2":          top_tricks[1] if len(top_tricks) > 1 else "",
        "signature_trick_3":          top_tricks[2] if len(top_tricks) > 2 else "",
    })


# ── Write outputs ─────────────────────────────────────────────────────────────

print(f"\nWriting canonical CSVs to {CANONICAL} ...")
CANONICAL.mkdir(parents=True, exist_ok=True)

write_csv(
    CANONICAL / "events.csv",
    ["event_key", "legacy_event_id", "year", "event_name", "event_slug",
     "start_date", "end_date", "city", "region", "country",
     "host_club", "event_type", "status", "notes", "source"],
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
     "display_name", "person_id", "team_person_key", "notes"],
    participants_out,
)
write_csv(
    CANONICAL / "persons.csv",
    [
        "person_id", "person_name", "member_id", "player_ids",
        "country", "first_year", "last_year", "event_count", "placement_count",
        "bap_member", "bap_nickname", "bap_induction_year",
        "fbhof_member", "fbhof_induction_year",
        "freestyle_sequences", "freestyle_max_add",
        "freestyle_unique_tricks", "freestyle_diversity_ratio",
        "signature_trick_1", "signature_trick_2", "signature_trick_3",
    ],
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

# Participant key uniqueness: singles ties intentionally share participant_order=1.
# Only flag duplicates in doubles disciplines (those are always structural errors).
singles_disc_keys = {
    (r["event_key"], r["discipline_key"])
    for r in disciplines_out if r["team_type"] == "singles"
}
doubles_part_keys = [
    k for k in part_keys
    if (k[0], k[1]) not in singles_disc_keys
]
singles_ties = len(part_keys) - len(set(part_keys))
doubles_dups = len(doubles_part_keys) - len(set(doubles_part_keys))
if doubles_dups:
    print(f"ERROR: {doubles_dups} duplicate participant keys in doubles disciplines")
    errors += 1
else:
    print(f"✓  event_result_participants: doubles keys unique; "
          f"{singles_ties:,} singles-tie duplicates (expected, order=1 for all tied)")

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
