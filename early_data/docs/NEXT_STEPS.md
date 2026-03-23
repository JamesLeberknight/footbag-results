# Pre-1997 Recovery — Next Steps

**Last updated:** 2026-03-23
**Current phase:** Human Review
**Gate to next phase:** All review decisions recorded in review files

---

## Immediate Review Tasks (Human)

### 1. Person Alias Review
**File:** `early_data/review/review_aliases.xlsx` (sheet: Alias Review)
**CSV:** `early_data/review/person_alias_resolution.csv`

Review all 12 REVIEW_NEEDED entries. For each, record a decision:

| Decision | Meaning |
|----------|---------|
| `ACCEPT` | Map raw_name to the candidate PT person (add as alias) |
| `CREATE_NEW` | Treat as a new early player separate from the candidate |
| `REJECT` | The candidate is definitely wrong; leave unresolved |
| `DEFER` | Cannot decide yet; needs source image check |

High-priority cases:
- **Jim Caveney ↔ Jimmy Caveney** — likely same person; check if PT rename was applied
- **Tim Fitzgerald ↔ Jim Fitzgerald** — source error likely; verify source image
- **Ken Shults / Kenny Shults ↔ Kenneth Shults** — Ken is an unregistered nickname
- **Tobin/Torben Wigger ↔ Torbin Wigger** — spelling variant of Scandinavian name
- **Karin Atogpian ↔ Karen Atgopian** — both currently unresolved; decide if same person

### 2. Event Group Review
**File:** `early_data/review/review_event_groups.xlsx` (sheet: Event Groups)
**CSV:** `early_data/review/event_group_resolution.csv`

Key questions to answer:

- Are any SAME-YEAR events from different sources actually the same real-world event
  that were grouped separately due to different normalized types?

  Notable cases:
  - 1994: `IFAB_WORLD_CHAMPIONSHIPS` (FBW source) vs `WORLD_CHAMPIONSHIPS` (IFAB source)
    → Almost certainly the same event. Should these share a canonical ID?
  - 1986–1992: Each year has both `WFA_WORLD_CHAMPIONSHIPS` and `WORLD_CHAMPIONSHIPS`
    → The WFA Championships IS the World Championship in those years. Same event?
  - 1984/1985: `WORLD_CHAMPIONSHIPS` (OLD_RESULTS "1984"/"1985") alongside
    `WFA_NATIONALS` (FBW + OLD_RESULTS "1984 WFA" / "1985 WFA")
    → These are distinct events (World Footbag Championships ≠ WFA Nationals)

- Are any SINGLE_SOURCE events known to have more complete results available elsewhere?

---

## After Review Is Complete

### Step A — Apply alias decisions
Run `early_data/scripts/09_apply_alias_decisions.py` (to be written) which reads
`person_alias_resolution.csv` decisions and:
- Adds ACCEPT cases to `person_aliases_pre1997.csv`
- Promotes CREATE_NEW cases to `new_early_players.csv`
- Rebuilds `persons_pre1997.csv` and `event_result_participants_pre1997.csv`

### Step B — Apply event group decisions
Run `early_data/scripts/10_apply_event_decisions.py` (to be written) which reads
`event_group_resolution.csv` decisions and:
- Merges groups where the decision is SAME_EVENT
- Flags groups where sources disagree (CONFLICT)
- Rebuilds `events_pre1997.csv` and `canonical_events.csv`

### Step C — Rebuild early-data spreadsheet
Re-run `early_data/scripts/07_build_early_release.py` to regenerate the Excel workbook
with updated identity and event resolutions.

### Step D — Add additional sources (future)
When new pre-1997 source material becomes available:
1. Add Gemini batch JSON to `early_data/review/`
2. Re-run `early_data/scripts/04_json_to_csv.py`
3. Re-run `early_data/scripts/05_build_historical_dataset.py`
4. Re-run `early_data/scripts/06_identity_resolution.py`
5. Review new unresolved names
6. Re-run `early_data/scripts/07_build_early_release.py`

### Step E — Promote to main dataset (deferred)
Only after:
- All review decisions recorded
- QC passes on early-data canonical CSVs
- Explicit decision to merge pre-1997 into main pipeline

Do NOT merge yet. Pre-1997 is a separate deliverable.

---

## Known Gaps in Current Coverage

| Year range | Gap | Notes |
|------------|-----|-------|
| 1980 | Only 1 placement for World Footbag Championships | IFAB history page limited |
| 1981 | Only 2 placements for World Footbag Championships | Same |
| 1982–1985 | Regional / state / club events missing | Not in scanned sources |
| 1986–1992 | Non-WFA events missing | Only WFA Worlds in FBW scans |
| 1987–1996 | Regional and national events (other than top-tier) largely absent | |
| 1988 | US Nationals has only 1 placement | Scan quality or page missing |
| 1984 | European Championships has only 1 placement | Same |

---

## Decisions NOT to Make Automatically

The following will ALWAYS require human sign-off:
- Merging two PT persons (even with strong evidence)
- Merging two event groups into one canonical event
- Promoting a pre-1997 new player into the main PT
- Accepting a name change (married → maiden name, etc.)
