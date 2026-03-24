# Footbag Results — Merged Canonical Dataset

Unified view of historical footbag competition results combining:

- **POST-1997** — primary, production dataset (footbag.org mirror)
- **PRE-1997** — historical reconstruction from FBW magazine scans, IFAB summaries, OLD_RESULTS.txt

Datasets are preserved side-by-side for provenance and auditability.

---

## Files

| File | Rows | Description |
|------|------|-------------|
| `events_all.csv` | 846 | 32 pre-1997 + 814 post-1997 canonical events |
| `event_disciplines_all.csv` | 4,344 | 308 pre-1997 + 4,036 post-1997 disciplines |
| `event_results_all.csv` | 27,416 | 755 pre-1997 + 26,661 post-1997 result rows |
| `event_result_participants_all.csv` | 35,720 | 1,141 pre-1997 + 34,579 post-1997 participants |
| `persons_all.csv` | 3,482 | 3,468 post-1997 + 14 PRE1997_ONLY persons |

All files share a `data_source` column: `PRE1997` or `POST1997`.

---

## Schema

### events_all.csv
`event_id, event_name, year, event_type, location, start_date, end_date, city, region, country, host_club, status, validation_status, num_placements, source_types, data_source`

### event_disciplines_all.csv
`event_id, discipline, discipline_name, discipline_category, team_type, sort_order, coverage_flag, total_placements, notes, data_source`

### event_results_all.csv
`event_id, discipline, discipline_name, placement, player_raw, team_raw, score_text, source_type, data_source, result_row_id`

### event_result_participants_all.csv
`event_id, discipline, placement, participant_order, display_name, player_name_raw, person_id, team_person_key, resolution_status, data_source`

### persons_all.csv
`person_id, person_canon, source_scope, ifpa_member_id, bap_member, bap_nickname, bap_induction_year, fbhof_member, fbhof_induction_year, first_year, last_year, country, data_source`

---

## Key Concepts

### `data_source` filter
```sql
-- Modern dataset only
WHERE data_source = 'POST1997'

-- Historical reconstruction only
WHERE data_source = 'PRE1997'

-- Full combined dataset (no filter)
```

### Persons by scope
| `source_scope` | Description |
|---|---|
| `POST1997` | Appears only in modern dataset |
| `PRE1997_AND_POST1997` | Appears in both datasets |
| `PRE1997_ONLY` | Historical player, no modern records |

### Dual representation of early events
For years before 1997, two versions of the same real-world event may exist:
- **POST1997 legacy stub** — minimal placeholder (e.g., `1980_worlds_oregon_city`)
- **PRE1997 reconstruction** — rich evidence-based record (e.g., `WORLD_CHAMPIONSHIPS_1980`)

These are **not duplicates to merge** — they represent different fidelity levels.

---

## Data Integrity

Built by `early_data/scripts/12_build_enrichment_and_merged.py`.
Validation: 0 errors on last run (referential integrity PASS).

---

## Coverage

| Dataset | Years | Events | Disciplines | Placements |
|---------|-------|--------|-------------|------------|
| PRE-1997 | 1980–1996 | 32 | 308 | 755 |
| POST-1997 | 1997–present | 814 | 4,036 | 26,661 |
| Combined | 1980–present | 846 | 4,344 | 27,416 |
