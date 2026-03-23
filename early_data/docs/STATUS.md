# Pre-1997 Historical Recovery — Pipeline Status

**Last updated:** 2026-03-23
**Phase:** LOCKED — Human Review
**Next automation gate:** None until review decisions are recorded

---

## Overview

The first full reconstruction pass of pre-1997 footbag competition results is complete.
All automated extraction, parsing, grouping, and identity resolution steps have run.
The dataset is now frozen for human review before any further changes.

---

## Source Material

| Source | Type | Pages/Files | Events Extracted |
|--------|------|-------------|-----------------|
| FBW Magazine Vols 2–14 (scans) | AI extraction (Gemini) | 53 slides | 35 in-scope |
| IFAB Worlds History page | AI extraction (Gemini) | 3 slides | 3 in-scope |
| OLD_RESULTS.txt | Contributed text file | — | 10 in-scope |
| **Total** | | | **45 source events → 37 canonical groups** |

Out-of-scope (year ≥ 1997): 13 source events routed to separate files.

---

## Canonical Dataset Counts

| Metric | Count |
|--------|-------|
| Canonical event groups (year < 1997) | 37 |
| Years covered | 17 (1980–1996) |
| Unique normalized event types | 8 |
| Source-level event rows | 45 |
| Placement rows (all sources) | 755 |
| Participant rows (teams expanded) | 1,141 |
| Unique raw player names | 115 |
| Unique disciplines (division × event) | 308 |

---

## Cross-Source Validation

| Status | Count | Notes |
|--------|-------|-------|
| CONFIRMED_MULTI_SOURCE | 8 | FBW magazine + OLD_RESULTS.txt agree |
| SINGLE_SOURCE | 29 | Only one source for this event |
| CONFLICT | 0 | No location/name conflicts detected |

Confirmed years: 1982–1986 (NHSA, WFA, and World Championships).

---

## Identity Resolution

| Category | Count | Notes |
|----------|-------|-------|
| EXACT match to PT | 83 | Matched via person_canon, aliases, or player_names_seen |
| Auto-accepted aliases | 5 | Safe spelling/nickname variants, explicitly approved |
| Review-needed aliases | 12 | NOT auto-resolved — requires human decision |
| New early players | 14 | Absent from post-1997 PT; assigned stable PRE1997_ONLY IDs |
| Noise | 1 | Literal `unknown` — preserved, not added to PT |
| **Total unique raw names** | **115** | |

Auto-accepted aliases:
- `Billy Hayne` → Bill Hayne (Billy/Bill nickname)
- `Fred Kipley` → Fred Kippley (missing `p`)
- `Misty Helme` → Misty Helms (missing `s`)
- `Max Smith Jr.` → Max Smith (suffix only)
- `Gary Laut` → Gary Lautt (missing `t`)

---

## Participant Resolution Breakdown

| Status | Count | Meaning |
|--------|-------|---------|
| MATCHED | 933 | Exact PT match |
| AUTOACCEPTED | 21 | Safe alias → existing PT person |
| NEW_PLAYER | 29 | Pre-1997-only player with new stable ID |
| REVIEW_NEEDED | 154 | Unresolved — 12 distinct names, pending human decision |
| UNRESOLVED | 3 | No plausible PT match found, not yet classified |
| NOISE | 1 | `unknown` literal |

---

## Year Coverage Summary

| Year | Events | Participant rows | Notes |
|------|--------|-----------------|-------|
| 1980 | 4 | 27 | Sparse — 3 events are 1-placement stubs |
| 1981 | 3 | 44 | Sparse — 1 event is stub |
| 1982 | 3 | 80 | NHSA confirmed multi-source (77 participant rows) |
| 1983 | 4 | 179 | NHSA + WFA both confirmed multi-source |
| 1984 | 3 | 150 | Well-covered (WFA Nationals + World + Euro stub) |
| 1985 | 2 | 152 | Well-covered |
| 1986 | 2 | 60 | WFA Worlds confirmed multi-source |
| 1987 | 3 | 48 | WFA Worlds + World + Euro stubs |
| 1988–1996 | 1–3/yr | 44–47/yr | Primarily WFA/IFAB World Championships only |

---

## Files Frozen at This Lock

All files under `early_data/canonical/`, `early_data/identity/`, and `early_data/out/`
reflect the state as of this lock. **Do not modify canonical outputs** until review
decisions from `early_data/review/` have been incorporated via a new pipeline run.

---

## What Is NOT Done Yet

- Manual review of 12 person alias candidates
- Manual review of event group groupings (are all 37 groups correct?)
- Possible merge of `1994 IFAB World Championships` (FBW) with `1994 World Footbag Championships` (IFAB) — same event, different source names
- Source image verification for review-needed aliases
- Promotion of any pre-1997 identities into the main Persons_Truth.csv
- Integration with post-1997 pipeline
