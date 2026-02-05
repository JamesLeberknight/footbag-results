CLAUDE.md — Footbag Events Data Pipeline

## Purpose

This repo converts an offline HTML mirror of footbag event pages into clean, canonical CSV and Excel output suitable for analysis.

## Project Map

- `01_parse_mirror.py` (Stage 1): HTML mirror → `out/stage1_raw_events.csv`
- `02_canonicalize_results.py` (Stage 2): Stage 1 CSV → `out/stage2_canonical_events.csv`
- `03_build_excel.py` (Stage 3): Stage 2 CSV → `Footbag_Results_Canonical.xlsx`
- `out/`: Generated artifacts (CSV + QC reports)
- `data/`: QC baseline files
- Mirror location: `./mirror/www.footbag.org/events/show/*/index.html`

## Quickstart Commands

```bash
# Run full pipeline
python3 01_parse_mirror.py
python3 02_canonicalize_results.py
python3 03_build_excel.py

# Fast loop (only Stage 2 after edits)
python3 02_canonicalize_results.py

# Save new QC baseline after improvements
python3 02_canonicalize_results.py --save-baseline
```

---

## Current Pipeline Stats (2026-02-04)

| Metric | Value |
|--------|-------|
| Total events | 777 |
| Total placements | 22,962 |
| QC errors | 0 |
| QC warnings | 0 |
| Parse confidence (high) | 96.1% |
| Unknown divisions | 279 (1.2%) |
| Year coverage | 99.2% (771/777) |
| Location coverage | 100% |

---

## Stage 2 Output Contract

### Required Fields in `out/stage2_canonical_events.csv`

| Field | Requirements |
|-------|-------------|
| `event_id` | Required; stable; unique |
| `year` | Integer or empty; plausible range; consistent with date/name |
| `event_name` | Required; no HTML remnants; not a URL |
| `date` | Prefer parseable; no garbage; no extra commentary |
| `location` | Required; place name only |
| `host_club` | Optional but preferred; club-like text only |
| `event_type` | Required enum; canonicalized |
| `results_raw` | Raw text; may be empty |
| `placements_json` | Valid JSON list conforming to placement schema |

### Placement Object Schema

Each entry in `placements_json` must have:
- `division_raw`, `division_canon`, `division_category`
- `place` (integer)
- `competitor_type` ∈ {player, team}
- `player1_name`, `player2_name` (team requires both unless flagged)
- `entry_raw`, `parse_confidence`, `notes`

---

## Known Data Quality Issues

### Broken Source Events (9 total)

These events have SQL errors in the original HTML mirror (unescaped apostrophes broke database queries). We keep them with inferred location from event names:

| event_id | Event Name | Inferred Location |
|----------|------------|-------------------|
| 1023993464 | Funtastik Summer Classic | Hershey, Pennsylvania, USA |
| 1030642331 | Seattle Juggling and Footbag Festival | Seattle, Washington, USA |
| 1099545007 | Seapa NZ Footbag Nationals 2005 | New Zealand |
| 1151949245 | ShrEdmonton 2006 | Edmonton, Alberta, Canada |
| 1278991986 | 23rd Annual Vancouver Open | Vancouver, British Columbia, Canada |
| 1299244521 | Warsaw Footbag Open 2011 | Warsaw, Poland |
| 860082052 | Texas State Footbag Championships | Texas, USA |
| 941066992 | WESTERN REGIONAL FOOTBAG CHAMPIONSHIPS | California, USA |
| 959094047 | Battle of the Year Switzerland | Switzerland |

### Events Missing Year (6 total)

Broken source events without year in their names. Years could be researched from external sources.

### Remaining Unknown Divisions (279 placements, 1.2%)

42 events with complex HTML structures or truly ambiguous data. Largest contributor: East Coast Championships 2003 (81 placements with nested table structure and `<b>` tag divisions).

---

## Division Categorization

### Category Keywords

| Category | Keywords |
|----------|----------|
| **net** | net, volley |
| **freestyle** | freestyle, routine, shred, circle, sick, request, battle, ironman, combo, trick |
| **golf** | golf, golfer |
| **sideline** | 2-square, 4-square, consecutive, distance |

### Abbreviated Division Headers

The parser recognizes these abbreviations:
- OSN/ODN/ISN/IDN/WSN/WDN/MDN/MSN (Net divisions)
- OSF/ODF/OSR/ODR/WSR (Freestyle divisions)
- OS/OD/IS/ID/WS/WD/MD (Generic)

### Division Inference

When no division header is found and all placements are Unknown:
1. Check event name for keywords (singles, doubles, net, shred, etc.)
2. Check competitor types (teams → doubles, players → singles)
3. Check event type (net/freestyle)
4. Apply known tournament patterns (King of the Hill → Singles Net, Bembel Cup → Doubles Net)

---

## Parsing Rules

### Seeding Section Detection

The parser skips seeding data using these patterns:
- "Initial Seeding" or "Seeding" headers
- "Division Name - Initial Seeding" format (skips entries, keeps division)
- Resumes parsing at "Results", "Final Results", "Complete Results"

### Results Extraction Priority (Stage 1)

1. **Structured results** from `<h2>` division headers (preferred)
2. **`<pre>` blocks** with numbered placements (fallback)
3. **`pre.eventsPre`** (final fallback)

### Team Detection

Teams are detected by separators:
- `/` outside parentheses (most common)
- ` & ` between names
- ` and ` between names

---

## Event Type Inference

Priority order:
1. "World Footbag Championships" in name → `worlds`
2. Division categories present in placements
3. Text analysis (net keywords, freestyle keywords, scoring patterns)
4. "Jam" in name → `freestyle`
5. Net scoring patterns (21-16, 21-11) → `net`

Valid event types: `freestyle`, `net`, `worlds`, `mixed`, `social`, `golf`

---

## QC System

### QC Artifacts (generated every run)

- `out/stage2_qc_summary.json`: Aggregated counts, field coverage, parse stats
- `out/stage2_qc_issues.jsonl`: One issue per line with check_id, severity, event_id

### Baseline and Gating

Baseline stored in: `data/qc_baseline_stage2.json`

Gate rules:
- ERROR counts must never increase vs baseline
- WARN counts should not increase unless justified

### Field-Level Checks

| Field | Checks |
|-------|--------|
| `event_id` | Required; unique; digits only |
| `year` | Plausible range (1970-2030); required for worlds |
| `event_name` | Required; no HTML/URLs; no placeholders |
| `date` | No iCal remnants; consistent with year |
| `location` | Required; no URLs/emails; reasonable length |
| `event_type` | Valid enum value |
| `placements_json` | Valid JSON; schema validation |

---

## Override Dictionaries

### Location Overrides (`LOCATION_OVERRIDES`)

Event-specific location fixes for broken source events and verbose locations.

### Event Type Overrides (`EVENT_TYPE_OVERRIDES`)

Manual classification for edge cases (golf events, social events, unusual formats).

### Event Name Overrides (`EVENT_NAME_OVERRIDES`)

Fixes for placeholder/template names.

### Event Parsing Rules (`EVENT_PARSING_RULES`)

Per-event parsing configuration (e.g., merged team splitting for specific events).

---

## Iteration Protocol

### Loop (Follow Exactly)

1. **Run Stage 2** to produce CSV + QC artifacts
2. **Identify one highest-impact issue** (single check_id)
3. **Show evidence**: Counts + 3-10 concrete examples with event_id
4. **Investigate source HTML** if issue concentrated in few events
5. **Ask exactly one human question** (if needed for rule/mapping/threshold)
6. **Implement smallest safe change**
7. **Re-run Stage 2 and compute delta**
8. **Persist decision in overrides** (if any)
9. **Write iteration report** (proof of improvement + no regressions)
10. **Only after passing gate**, move to next issue

### When to Ask vs. Proceed Independently

**Always ask**:
- New division category unclear (net vs. freestyle vs. sideline)
- Event-specific override needed
- Ambiguous data with multiple interpretations
- Change affects >10 events

**Can proceed independently**:
- Adding obvious division keyword
- Fixing clear parsing bug
- Noise cleanup that doesn't change legitimate data
- Change affects ≤3 events with clear correct answer

---

## Recent Improvements (2026-02-04)

1. **Broken source events** - Reduced from 20 to 9 (11 don't exist in mirror)
2. **Stage 1 extraction** - Prefer structured `<h2>` results over `<pre>` blocks
3. **Seeding detection** - Handle "Division - Initial Seeding" format
4. **Abbreviated divisions** - Recognize OSN, ODN, ODF, etc.
5. **Division inference** - Infer from event name, competitor type, and event type
6. **Known patterns** - King of the Hill → Singles Net, Bembel Cup → Doubles Net
7. **Unknown reduction** - 832 → 279 placements (66% improvement)

---

## Files to Track in Git

Essential files:
- `01_parse_mirror.py`
- `02_canonicalize_results.py`
- `03_build_excel.py`
- `claude.md`
- `requirements.txt`
- `data/qc_baseline_stage2.json`

Generated (can exclude):
- `out/` directory
- `Footbag_Results_Canonical.xlsx`
- `*.zip` files
- `*:Zone.Identifier` files (Windows artifacts)
