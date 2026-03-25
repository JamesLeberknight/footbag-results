# Footbag Results — Official Merged Canonical Dataset

This is the **official merged canonical dataset** combining historical footbag
competition results from both the PRE1997 reconstruction and the POST1997 mirror.

Suitable for application ingestion, database loading, and downstream tooling.

---

## Coverage

| Source | Years | Events | Disciplines | Result rows |
|--------|-------|--------|-------------|-------------|
| PRE-1997 reconstruction | 1980–1996 | 29 | 304 | 741 |
| POST-1997 mirror | 1997–present | 781 | 3,832 | 26,299 |
| **Combined** | **1980–present** | **810** | **4,136** | **27,040** |

---

## Files

| File | Rows | Description |
|------|------|-------------|
| `events.csv` | 810 | All official events |
| `event_disciplines.csv` | 4,136 | All disciplines |
| `event_results.csv` | 27,040 | All result rows |
| `event_result_participants.csv` | 35,189 | All participants |
| `persons.csv` | 3,482 | All persons |
| `validation_summary.txt` | — | Build validation report |

---

## Schema

### events.csv
`event_id, event_name, year, event_type, location, start_date, end_date, city, region, country, host_club, status, validation_status, num_placements, source_types, data_source`

### event_disciplines.csv
`event_id, discipline, discipline_name, discipline_category, team_type, sort_order, coverage_flag, total_placements, notes, data_source`

### event_results.csv
`event_id, discipline, discipline_name, placement, player_raw, team_raw, score_text, source_type, data_source, result_row_id`

### event_result_participants.csv
`event_id, discipline, placement, participant_order, display_name, player_name_raw, person_id, team_person_key, resolution_status, data_source`

### persons.csv
`person_id, person_canon, source_scope, ifpa_member_id, bap_member, bap_nickname, bap_induction_year, fbhof_member, fbhof_induction_year, first_year, last_year, country, data_source`

---

## Event ID System

All events use a single slug-based identifier:

```
YYYY_event_city      # e.g. 2003_worlds_prague, 1986_worlds_golden
YYYY_event           # fallback when city unknown, e.g. 1993_worlds
```

No legacy numeric IDs appear in this dataset.

---

## Worlds Classification

All 49 world championship events have `event_type = "worlds"`:

| Era | Events | Notes |
|-----|--------|-------|
| 1980–1982 | 3 × NHSA (authoritative worlds) + 3 displaced generics | NHSA = `YYYY_worlds`; displaced = `YYYY_worlds_<city>` |
| 1983 | NHSA + WFA + displaced generic | Dual-worlds year |
| 1984–1992 | WFA worlds | Golden, CO (1986–1989); unknown city otherwise |
| 1993–1996 | IFAB worlds | Palo Alto, CA (1994); unknown otherwise |
| 1997–2025 | Modern worlds | Full location data |

---

## Key Concepts

### `data_source` field
- `PRE1997` — evidence-based reconstruction from FBW magazine scans, IFAB summaries, OLD_RESULTS.txt
- `POST1997` — footbag.org mirror dataset (authoritative for 1997+)

### PRE1997 takes precedence for year < 1997

For events before 1997, the PRE1997 reconstructed records are authoritative.
POST1997 legacy stubs for the same real-world championships have been suppressed
from this official view to avoid duplicate early-year records.

Suppressed events are retained in `out/canonical_all_union/` for audit purposes.

### `source_scope` in persons.csv
| Value | Meaning |
|---|---|
| `POST1997` | Person appears only in modern dataset |
| `PRE1997_AND_POST1997` | Person appears in both datasets |
| `PRE1997_ONLY` | Historical player with no modern records |

---

## Provenance-preserving union

The full union (including suppressed POST1997 early stubs) is in:

`out/canonical_all_union/`

Do not use the union for downstream applications — it contains intentional overlaps.

---

## Built by

`tools/build_appsafe_merged.py` — reads from `out/canonical_all_union/` and
applies the early-overlap filter rule to produce this official dataset.

Rebuild command:
```bash
python3 tools/build_appsafe_merged.py
python3 tools/build_merged_feeds.py
```
