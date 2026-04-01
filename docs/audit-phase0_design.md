Phase 0 Audit Design — Output Schemas and Detection Logic

  ---
  Framing Constraint (Governing All Outputs)

  Every output file is an investigation aid. Presence of a row in any output means "this warrants examination," not
  "this is a confirmed loss" or "this should be recovered." No output field should imply automatic recovery eligibility.
   The word "lost" is avoided throughout; the outputs use "absent from active pipeline" instead.

  ---
  1. Output File Inventory

  ┌──────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────┐
  │                 File                 │                                  Purpose                                  │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_row_diff.csv                   │ Per-row presence/absence status for every PBP v85 row                     │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_prepass_attribution.csv        │ Which 02p6 pre-pass check first matches each absent row                   │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_european_format_candidates.csv │ Team rows in singles divisions with structural European-name signals      │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_encoding_artifacts.csv         │ Division_canon values that differ from a sibling value only by artifact   │
  │                                      │ characters                                                                │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_cascade_shadow_events.csv      │ Events/divisions where iterative pool-shadow removal compounded across    │
  │                                      │ iterations                                                                │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_summary.txt                    │ Consistency checks and aggregate counts                                   │
  └──────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────┘

  All files are written to out/audit/. They are read-only artifacts. No pipeline file is modified.

  ---
  2. Row Identity and Diff Structure

  2.1 The Identity Problem

  No single field or small key uniquely identifies a PBP v85 row in all cases. Reasons:

  - Pool-format events have multiple rows per player per division (same player, different places)
  - Doubles team rows share a place with no per-person identity in the row itself (person_canon = __NON_PERSON__)
  - Exact duplicate rows exist (pre-pass 4 removes them — so PBP v85 may contain duplicates)

  The diff must therefore use a two-tier matching strategy:

  Tier 1 — Structural slot match: (event_id, division_canon_normalized, place, competitor_type)
  Identifies the slot. Multiple PBP rows may share a slot (pool format, multiple team members at same rank).

  Tier 2 — Content match within slot: (person_canon, team_display_name)
  Within a slot, distinguishes individual rows.

  division_canon_normalized is the division_canon with all known artifact characters stripped (U+00AD, U+FFFD, U+00A0,
  U+200B, U+2019 — see Section 4.2). This ensures a soft-hyphen-containing PBP row can match its clean post-02p5
  counterpart.

  2.2 Match Outcomes

  For each PBP v85 row, the diff produces one of three status values:

  ┌─────────────────────┬─────────────────────────────────────────────────────────────────────────────────────┐
  │       Status        │                                       Meaning                                       │
  ├─────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ PRESENT             │ A row matching on both tiers is found in the post-02p6 output                       │
  ├─────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ ABSENT_SLOT_COVERED │ No Tier-2 match, but at least one other row exists at this slot in post-02p6 output │
  ├─────────────────────┼─────────────────────────────────────────────────────────────────────────────────────┤
  │ ABSENT_NO_COVERAGE  │ No match at either tier — the slot is entirely absent from post-02p6 output         │
  └─────────────────────┴─────────────────────────────────────────────────────────────────────────────────────┘

  ABSENT_SLOT_COVERED covers two scenarios that look identical from the row alone: (a) the row was removed and a
  different row legitimately covers the slot, (b) the row was transformed into a different form at the same slot. The
  audit does not distinguish these — that is investigation work for later phases.

  ABSENT_NO_COVERAGE is the higher-urgency signal: the competitive placement has no representation in the active
  pipeline at all.

  2.3 Match Type

  Because division_canon_normalized is used for matching, the audit records how the match was made:

  ┌─────────────────────┬──────────────────────────────────────────────────────────────────┐
  │     match_type      │                             Meaning                              │
  ├─────────────────────┼──────────────────────────────────────────────────────────────────┤
  │ EXACT               │ Division_canon matched without normalization                     │
  ├─────────────────────┼──────────────────────────────────────────────────────────────────┤
  │ NORMALIZED_DIVISION │ Match required stripping artifact characters from division_canon │
  ├─────────────────────┼──────────────────────────────────────────────────────────────────┤
  │ NONE                │ No match found (row is absent)                                   │
  └─────────────────────┴──────────────────────────────────────────────────────────────────┘

  NORMALIZED_DIVISION matches are flagged for Phase 1 review — they confirm that encoding artifacts are causing division
   misidentification.

  ---
  3. audit_row_diff.csv — Schema

  ┌────────────────────────────┬──────┬──────────────────────────────────────────────────────────────────────────────┐
  │           Field            │ Type │                                 Description                                  │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ pbp_row_index              │ int  │ 0-based row index in PBP v85 (for traceability back to source)               │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ event_id                   │ str  │ Event identifier                                                             │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ year                       │ int  │ Event year                                                                   │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ division_canon             │ str  │ Division name as it appears in PBP v85 (unmodified)                          │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ division_canon_normalized  │ str  │ Division name after artifact-character stripping                             │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ division_category          │ str  │ net / freestyle / golf / unknown                                             │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ place                      │ int  │ Placement position                                                           │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ competitor_type            │ str  │ player / team                                                                │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ person_id                  │ str  │ Person UUID or blank                                                         │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ person_canon               │ str  │ Canonical name or __NON_PERSON__                                             │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ team_display_name          │ str  │ Display string for team rows                                                 │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ team_person_key            │ str  │ Piped UUID string for team rows                                              │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ person_unresolved          │ str  │ Unresolved flag from PBP                                                     │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ status                     │ enum │ PRESENT / ABSENT_SLOT_COVERED / ABSENT_NO_COVERAGE                           │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ match_type                 │ enum │ EXACT / NORMALIZED_DIVISION / NONE                                           │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ slot_row_count_in_pbp      │ int  │ How many PBP rows share this (event, div_normalized, place, competitor_type) │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ slot_row_count_in_pipeline │ int  │ How many post-02p6 rows exist at this slot                                   │
  ├────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ investigation_note         │ str  │ Free-text flag for known patterns (populated by detection logic, not human)  │
  └────────────────────────────┴──────┴──────────────────────────────────────────────────────────────────────────────┘

  Example rows

  pbp_row_index: 4421
  event_id: 1035277529
  year: 2003
  division_canon: "Open Singles Freestyle"
  division_canon_normalized: "Open Singles Freestyle"
  division_category: freestyle
  place: 12
  competitor_type: player
  person_id: a1b2c3d4-...
  person_canon: "Jean Dupont"
  team_display_name: ""
  team_person_key: ""
  person_unresolved: ""
  status: PRESENT
  match_type: EXACT
  slot_row_count_in_pbp: 1
  slot_row_count_in_pipeline: 1
  investigation_note: ""

  pbp_row_index: 8833
  event_id: 941418343
  year: 1999
  division_canon: "Open Circle Contest"          ← contains U+00AD
  division_canon_normalized: "Open Circle Contest"
  division_category: freestyle
  place: 3
  competitor_type: player
  person_id: e9f1a2b3-...
  person_canon: "Ville Haapasalo"
  team_display_name: ""
  team_person_key: ""
  person_unresolved: ""
  status: PRESENT
  match_type: NORMALIZED_DIVISION
  slot_row_count_in_pbp: 1
  slot_row_count_in_pipeline: 1
  investigation_note: "encoding_artifact:SOFT_HYPHEN in division_canon"

  pbp_row_index: 11204
  event_id: 1001076203
  year: 2001
  division_canon: "Open Singles Net"
  division_canon_normalized: "Open Singles Net"
  division_category: net
  place: 7
  competitor_type: team
  person_id: ""
  person_canon: "__NON_PERSON__"
  team_display_name: "Lefebvre / Pierre FRA"
  team_person_key: "uuid1|uuid2"
  person_unresolved: ""
  status: ABSENT_SLOT_COVERED
  match_type: NONE
  slot_row_count_in_pbp: 1
  slot_row_count_in_pipeline: 1
  investigation_note: "european_format_candidate:HIGH"

  pbp_row_index: 14872
  event_id: 941418343
  year: 1999
  division_canon: "Open Circle Contest"
  division_canon_normalized: "Open Circle Contest"
  division_category: freestyle
  place: 5
  competitor_type: player
  person_id: f3a9c1e2-...
  person_canon: "Mikael Sundberg"
  team_display_name: ""
  team_person_key: ""
  person_unresolved: ""
  status: ABSENT_NO_COVERAGE
  match_type: NONE
  slot_row_count_in_pbp: 3
  slot_row_count_in_pipeline: 0
  investigation_note: "cascade_shadow_candidate"

  ---
  4. audit_prepass_attribution.csv — Schema

  For every row with status = ABSENT_SLOT_COVERED or ABSENT_NO_COVERAGE, this file records which 02p6 pre-pass check
  first matches the row.

  4.1 Attribution Logic

  Attribution is first-match in pre-pass order. Each absent row is tested against each pre-pass removal condition
  independently. The first condition that matches is the attribution. This is not "which pre-pass actually removed it"
  (which cannot be known without re-running the pipeline) — it is "which pre-pass removal condition applies to this
  row."

  Pre-pass order and codes:

  ┌─────────────────────┬───────────────┬───────────────────────────────────────────────────────────────────────────┐
  │        Code         │   Pre-pass    │                             Removal Condition                             │
  ├─────────────────────┼───────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ PP0_METADATA        │ Pre-pass 0    │ person_canon or team_display_name contains a METADATA_NON_PLAYER_PHRASES  │
  │                     │               │ member                                                                    │
  ├─────────────────────┼───────────────┼───────────────────────────────────────────────────────────────────────────┤
  │                     │               │ competitor_type=team, division is non-doubles, team_display_name contains │
  │ PP3_TEAM_SUPERSEDED │ Pre-pass 3    │  " / ", and first or second component of " / " split matches a player row │
  │                     │               │  at same (event, div, place) by exact, ascii-folded, or ascii-stripped    │
  │                     │               │ comparison                                                                │
  ├─────────────────────┼───────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ PP4_EXACT_DUPLICATE │ Pre-pass 4    │ competitor_type=player, and another PBP row with identical (event_id,     │
  │                     │               │ division_canon, place, person_canon) appears earlier in PBP v85           │
  ├─────────────────────┼───────────────┼───────────────────────────────────────────────────────────────────────────┤
  │                     │ Pre-pass 5    │ player with this person_canon appears at 2+ distinct places in same       │
  │ PP5_DIRECT          │ (iteration 1) │ (event, div); has at least one unique place AND at least one shared       │
  │                     │               │ place; this row is at a shared place                                      │
  ├─────────────────────┼───────────────┼───────────────────────────────────────────────────────────────────────────┤
  │                     │ Pre-pass 5    │ The place was shared in PBP v85 (multiple players) but became unique only │
  │ PP5_CASCADE         │ (iteration    │  after a PP5_DIRECT removal reduced the population to 1                   │
  │                     │ 2+)           │                                                                           │
  ├─────────────────────┼───────────────┼───────────────────────────────────────────────────────────────────────────┤
  │                     │ Main loop     │ competitor_type=team, team_display_name matches "Name / CityName ST"      │
  │ MAIN_CITY_ARTIFACT  │ section D     │ format (two-word second component where second word is 2-letter uppercase │
  │                     │               │  state code)                                                              │
  ├─────────────────────┼───────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ UNKNOWN             │ —             │ No pre-pass condition applies                                             │
  └─────────────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────┘

  4.2 Schema

  ┌──────────────────────────────┬──────┬────────────────────────────────────────────────────────────────────────────┐
  │            Field             │ Type │                                Description                                 │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ pbp_row_index                │ int  │ Links to audit_row_diff.csv                                                │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ event_id                     │ str  │                                                                            │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ division_canon               │ str  │ From PBP                                                                   │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ place                        │ int  │                                                                            │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ person_canon                 │ str  │                                                                            │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ team_display_name            │ str  │                                                                            │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ status                       │ enum │ From audit_row_diff.csv — always ABSENT_*                                  │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ attribution                  │ enum │ PP0 / PP3 / PP4 / PP5_DIRECT / PP5_CASCADE / MAIN_CITY_ARTIFACT / UNKNOWN  │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ attribution_confidence       │ enum │ HIGH / MEDIUM / LOW                                                        │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ attribution_evidence         │ str  │ Why this attribution was made (specific matching criterion)                │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ is_cascade                   │ bool │ True only for PP5_CASCADE                                                  │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ cascade_iteration            │ int  │ Which pre-pass-5 iteration removed the row (1 = direct, 2+ = cascade; null │
  │                              │      │  if not PP5)                                                               │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ cascade_trigger_person_canon │ str  │ The person whose removal in iteration N-1 caused this cascade (null if not │
  │                              │      │  PP5_CASCADE)                                                              │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────────────────┤
  │ cascade_trigger_place        │ int  │ The place involved in the trigger removal (null if not PP5_CASCADE)        │
  └──────────────────────────────┴──────┴────────────────────────────────────────────────────────────────────────────┘

  Example rows

  pbp_row_index: 11204
  event_id: 1001076203
  division_canon: "Open Singles Net"
  place: 7
  person_canon: "__NON_PERSON__"
  team_display_name: "Lefebvre / Pierre FRA"
  status: ABSENT_SLOT_COVERED
  attribution: PP3_TEAM_SUPERSEDED
  attribution_confidence: HIGH
  attribution_evidence: "First component 'Lefebvre' matches person_canon 'Lefebvre' of player row
                         at same (event, div, place) by exact comparison"
  is_cascade: false
  cascade_iteration: null
  cascade_trigger_person_canon: null
  cascade_trigger_place: null

  pbp_row_index: 14872
  event_id: 941418343
  division_canon: "Open Circle Contest"
  place: 5
  person_canon: "Mikael Sundberg"
  team_display_name: ""
  status: ABSENT_NO_COVERAGE
  attribution: PP5_CASCADE
  attribution_confidence: HIGH
  attribution_evidence: "Place 5 had population=3 in PBP. After iteration 1 removed
                         'Anders Karlsson' (who had unique place=2), population became 2.
                         After iteration 2 removed 'Ville Haapasalo' (who had unique place=1),
                         population became 1 — making Sundberg's place 'unique', then removed
                         in iteration 3 despite having no other placement."
  is_cascade: true
  cascade_iteration: 3
  cascade_trigger_person_canon: "Ville Haapasalo"
  cascade_trigger_place: 5

  pbp_row_index: 7291
  event_id: 955745735
  division_canon: "Open Freestyle Routines"
  place: 4
  person_canon: "Jason Buster"
  team_display_name: "Jason Buster / Wichita KS"
  status: ABSENT_SLOT_COVERED
  attribution: MAIN_CITY_ARTIFACT
  attribution_confidence: HIGH
  attribution_evidence: "team_display_name second component 'Wichita KS' matches
                         CityName + 2-letter state code pattern"
  is_cascade: false
  cascade_iteration: null
  cascade_trigger_person_canon: null
  cascade_trigger_place: null

  ---
  5. audit_european_format_candidates.csv — Schema and Detection

  5.1 Detection Logic

  A row is a European-format candidate if it satisfies ALL of the following:

  1. competitor_type = team
  2. division_canon_normalized does NOT contain "doubles" or "dbl" (case-insensitive)
  3. team_display_name contains exactly one / separator
  4. Left component of the split is 1–2 words (a surname, possibly hyphenated)
  5. Right component of the split begins with a word that is a plausible given name (≥2 characters, mixed case or
  all-caps, not a country code)

  Confidence is then assessed by additional signals:

  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┬────────┐
  │                                                 Signal                                                  │ Weight │
  ├─────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────┤
  │ Right component ends with 2–3 uppercase letters that are a recognized country code (FRA, GER, CZE, POL, │ +2     │
  │  etc.)                                                                                                  │        │
  ├─────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────┤
  │ Event location is in Europe (event_id maps to a European event by year/location metadata)               │ +1     │
  ├─────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────┤
  │ Left component is a single word (pure surname — more European)                                          │ +1     │
  ├─────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────┤
  │ Right component has exactly 2 words: given_name + country_code                                          │ +2     │
  ├─────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────┤
  │ Row is absent from active pipeline (status = ABSENT_*)                                                  │ +1     │
  ├─────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────┤
  │ Division is a singles format (Singles, Consecutive, Golf, Shred)                                        │ +1     │
  └─────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┘

  Score → Confidence:
  - 5+: HIGH
  - 3–4: MEDIUM
  - 1–2: LOW

  Rows with confidence=LOW are still included but are listed last in the file and are not intended as investigation
  priorities.

  Important caveat recorded in every row: requires_source_validation = TRUE. The detection logic identifies structural
  candidates only. Whether the source mirror actually shows a single player at this placement must be verified by
  reading the original HTML. The reconstructed name field is tentative and must not be used as identity evidence.

  5.2 Schema

  ┌──────────────────────────────┬──────┬────────────────────────────────────────────────────────────────┐
  │            Field             │ Type │                          Description                           │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ pbp_row_index                │ int  │ Links to row_diff                                              │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ event_id                     │ str  │                                                                │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ year                         │ int  │                                                                │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ division_canon               │ str  │ From PBP                                                       │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ division_category            │ str  │                                                                │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ place                        │ int  │                                                                │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ team_display_name            │ str  │ The full malformed string                                      │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ left_component               │ str  │ Part before /                                                  │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ right_component              │ str  │ Part after /                                                   │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ reconstructed_name_tentative │ str  │ "{right_first} {left_surname}" — tentative only, not validated │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ country_code_detected        │ str  │ Trailing country code if present, else blank                   │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ status_in_pipeline           │ enum │ From row_diff                                                  │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ attribution_in_pipeline      │ enum │ From prepass_attribution                                       │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ confidence                   │ enum │ HIGH / MEDIUM / LOW                                            │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ confidence_score             │ int  │ Raw signal score                                               │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ requires_source_validation   │ bool │ Always TRUE                                                    │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ european_event_signal        │ bool │ True if event location metadata suggests Europe                │
  ├──────────────────────────────┼──────┼────────────────────────────────────────────────────────────────┤
  │ investigation_note           │ str  │                                                                │
  └──────────────────────────────┴──────┴────────────────────────────────────────────────────────────────┘

  Example rows

  pbp_row_index: 11204
  event_id: 1001076203
  year: 2001
  division_canon: "Open Singles Net"
  division_category: net
  place: 7
  team_display_name: "Lefebvre / Pierre FRA"
  left_component: "Lefebvre"
  right_component: "Pierre FRA"
  reconstructed_name_tentative: "Pierre Lefebvre"    ← NOT validated
  country_code_detected: "FRA"
  status_in_pipeline: ABSENT_SLOT_COVERED
  attribution_in_pipeline: PP3_TEAM_SUPERSEDED
  confidence: HIGH
  confidence_score: 7
  requires_source_validation: TRUE
  european_event_signal: TRUE
  investigation_note: "Classic European format. Player row at same slot has person_canon='Lefebvre'
                       — confirms parser captured surname only."

  pbp_row_index: 19872
  event_id: 1035277529
  year: 2003
  division_canon: "Open Singles Freestyle"
  division_category: freestyle
  place: 18
  team_display_name: "Schmidt / Andreas"
  left_component: "Schmidt"
  right_component: "Andreas"
  reconstructed_name_tentative: "Andreas Schmidt"    ← NOT validated
  country_code_detected: ""
  status_in_pipeline: ABSENT_SLOT_COVERED
  attribution_in_pipeline: PP3_TEAM_SUPERSEDED
  confidence: MEDIUM
  confidence_score: 4
  requires_source_validation: TRUE
  european_event_signal: TRUE
  investigation_note: "No country code. Right component is a plausible German given name.
                       Cannot confirm without mirror source."

  ---
  6. audit_encoding_artifacts.csv — Schema and Detection

  6.1 Artifact Character Set

  The following codepoints are stripped during normalization. Each has a code used in the artifact_types field:

  ┌────────────────────┬───────────┬───────────────────────────────┬──────────────────────────────────────────┐
  │        Code        │ Codepoint │             Name              │               Known Source               │
  ├────────────────────┼───────────┼───────────────────────────────┼──────────────────────────────────────────┤
  │ SOFT_HYPHEN        │ U+00AD    │ Soft hyphen                   │ Source HTML word-break hints             │
  ├────────────────────┼───────────┼───────────────────────────────┼──────────────────────────────────────────┤
  │ REPLACEMENT_CHAR   │ U+FFFD    │ Unicode replacement character │ Encoding corruption during mirror scrape │
  ├────────────────────┼───────────┼───────────────────────────────┼──────────────────────────────────────────┤
  │ NON_BREAKING_SPACE │ U+00A0    │ Non-breaking space            │ HTML &nbsp; entities                     │
  ├────────────────────┼───────────┼───────────────────────────────┼──────────────────────────────────────────┤
  │ ZERO_WIDTH_SPACE   │ U+200B    │ Zero-width space              │ Source formatting artifacts              │
  ├────────────────────┼───────────┼───────────────────────────────┼──────────────────────────────────────────┤
  │ CURLY_APOSTROPHE   │ U+2019    │ Right single quotation mark   │ "Women's" variants                       │
  ├────────────────────┼───────────┼───────────────────────────────┼──────────────────────────────────────────┤
  │ CURLY_QUOTE_OPEN   │ U+201C    │ Left double quotation mark    │ Quoted division names                    │
  ├────────────────────┼───────────┼───────────────────────────────┼──────────────────────────────────────────┤
  │ CURLY_QUOTE_CLOSE  │ U+201D    │ Right double quotation mark   │ Quoted division names                    │
  └────────────────────┴───────────┴───────────────────────────────┴──────────────────────────────────────────┘

  Normalization produces a division_canon_normalized by stripping all seven types and collapsing whitespace.

  6.2 Detection Approach

  1. Collect all distinct division_canon values from PBP v85
  2. For each, compute division_canon_normalized
  3. Group by normalized form — groups with 2+ members are variant families
  4. Also flag any single-member division_canon that contains one or more artifact codepoints (it may be the only
  occurrence of that variant but still needs normalization)

  One row per variant group in the output.

  6.3 Schema

  ┌─────────────────────────────────┬──────┬─────────────────────────────────────────────────────────────────────────┐
  │              Field              │ Type │                               Description                               │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ normalized_form                 │ str  │ The artifact-stripped canonical form                                    │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ variant_count                   │ int  │ Number of distinct division_canon values with this normalized form      │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ canonical_variant               │ str  │ The "clean" variant (no artifact characters) — may be blank if all      │
  │                                 │      │ variants have artifacts                                                 │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ artifact_variants               │ str  │ Pipe-separated list of division_canon values that contain artifacts     │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ artifact_types_present          │ str  │ Pipe-separated list of artifact codes found across all variants         │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ affected_events                 │ str  │ Pipe-separated event_ids where artifact variants appear                 │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ total_affected_rows_in_pbp      │ int  │ Total PBP v85 rows across all variants                                  │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ rows_on_artifact_variant        │ int  │ Rows on a non-canonical (artifact-containing) variant                   │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ rows_on_clean_variant           │ int  │ Rows on the clean variant (may be 0 if all are artifacts)               │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ absent_rows_on_artifact_variant │ int  │ Of the artifact-variant rows, how many are absent from post-02p6        │
  │                                 │      │ pipeline                                                                │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ pipeline_uses_which_variant     │ str  │ The division_canon form used in post-02p6 output, or "ABSENT" if none   │
  ├─────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────────┤
  │ investigation_priority          │ enum │ HIGH (absent rows > 0) / MEDIUM (all rows present but mismatched) / LOW │
  │                                 │      │  (informational)                                                        │
  └─────────────────────────────────┴──────┴─────────────────────────────────────────────────────────────────────────┘

  Example rows

  normalized_form: "Open Circle Contest"
  variant_count: 2
  canonical_variant: "Open Circle Contest"
  artifact_variants: "Open Circle Contest"       ← U+00AD between 'n' and 't'
  artifact_types_present: "SOFT_HYPHEN"
  affected_events: "941418343|1035277529"
  total_affected_rows_in_pbp: 47
  rows_on_artifact_variant: 12
  rows_on_clean_variant: 35
  absent_rows_on_artifact_variant: 0
  pipeline_uses_which_variant: "Open Circle Contest"
  investigation_priority: MEDIUM

  normalized_form: "Mixed Doubles Routines"
  variant_count: 2
  canonical_variant: "Mixed Doubles Routines"
  artifact_variants: "Mixed DouBles RouTines"
  artifact_types_present: "SOFT_HYPHEN"
  affected_events: "1323272493"
  total_affected_rows_in_pbp: 14
  rows_on_artifact_variant: 14
  rows_on_clean_variant: 0
  absent_rows_on_artifact_variant: 14
  pipeline_uses_which_variant: "ABSENT"
  investigation_priority: HIGH

  normalized_form: "Women's Singles Net"
  variant_count: 2
  canonical_variant: "Women's Singles Net"
  artifact_variants: "Women\u2019s Singles Net"
  artifact_types_present: "CURLY_APOSTROPHE"
  affected_events: "1035277529|915561090"
  total_affected_rows_in_pbp: 23
  rows_on_artifact_variant: 8
  rows_on_clean_variant: 15
  absent_rows_on_artifact_variant: 0
  pipeline_uses_which_variant: "Women's Singles Net"
  investigation_priority: LOW

  ---
  7. audit_cascade_shadow_events.csv — Schema and Detection

  7.1 Detection Approach

  Simulate pre-pass 5 on PBP v85 data in audit mode — iterating as the live code does, but recording which iteration
  removes each row and why. For each row removed in iteration ≥ 2, trace back to find the trigger: which row removed in
  iteration N-1 caused the place-population for this row's slot to decrease to 1, making this row eligible for removal.

  Additionally, scan for format signals in the division_canon that suggest a pool-only competition structure (where
  pre-pass 5 should not fire at all). These are not definitive — they are signals for investigation.

  Pool-only format signals (recorded in format_signals field):
  - division_canon contains: circle, request, shred, contest, big n (n = any digit), battle, ironman
  - division_category = freestyle AND division has no "final" keyword

  These signals do NOT mean the event is pool-only — they mean it warrants investigation before any pre-pass 5 exclusion
   is applied.

  7.2 Schema

  ┌────────────────────────────────────┬──────┬──────────────────────────────────────────────────────────────────────┐
  │               Field                │ Type │                             Description                              │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ event_id                           │ str  │                                                                      │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ year                               │ int  │                                                                      │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ division_canon                     │ str  │ From PBP                                                             │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ division_category                  │ str  │                                                                      │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ format_signals                     │ str  │ Pipe-separated pool-only format signals detected in division name    │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ total_players_in_division          │ int  │ Distinct person_canon values in this division in PBP                 │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ distinct_places_in_division        │ int  │ Distinct place values in this division in PBP                        │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ direct_removals                    │ int  │ Rows removed in pre-pass 5 iteration 1                               │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ cascade_removals                   │ int  │ Rows removed in pre-pass 5 iteration 2+                              │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ total_removals                     │ int  │ direct + cascade                                                     │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ max_cascade_depth                  │ int  │ Deepest iteration reached (1 = no cascade, 2+ = cascade occurred)    │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ removed_person_canon               │ str  │ Person whose row was removed (one row per removed person)            │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ removed_place                      │ int  │ The place that was removed                                           │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ removal_iteration                  │ int  │ 1 = direct, 2+ = cascade                                             │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ removal_type                       │ enum │ DIRECT / CASCADE                                                     │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ trigger_person_canon               │ str  │ Who triggered this removal (null for DIRECT)                         │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ trigger_place                      │ int  │ The trigger's place (null for DIRECT)                                │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ unique_place_that_justified_direct │ int  │ The unique-place row that made the trigger eligible (null for DIRECT │
  │                                    │      │  rows)                                                               │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ status_in_pipeline                 │ enum │ Should always be ABSENT_*                                            │
  ├────────────────────────────────────┼──────┼──────────────────────────────────────────────────────────────────────┤
  │ investigation_priority             │ enum │ HIGH (cascade + pool-format signals) / MEDIUM (cascade only) / LOW   │
  │                                    │      │ (direct, informational)                                              │
  └────────────────────────────────────┴──────┴──────────────────────────────────────────────────────────────────────┘

  One row per removed player. An event/division with 5 cascade-removed players produces 5 rows.

  Example rows

  event_id: 941418343
  year: 1999
  division_canon: "Open Circle Contest"
  division_category: freestyle
  format_signals: "circle|contest"
  total_players_in_division: 28
  distinct_places_in_division: 12
  direct_removals: 8
  cascade_removals: 5
  total_removals: 13
  max_cascade_depth: 3
  removed_person_canon: "Anders Karlsson"
  removed_place: 5
  removal_iteration: 1
  removal_type: DIRECT
  trigger_person_canon: null
  trigger_place: null
  unique_place_that_justified_direct: 2
  status_in_pipeline: ABSENT_NO_COVERAGE
  investigation_priority: HIGH

  event_id: 941418343
  year: 1999
  division_canon: "Open Circle Contest"
  division_category: freestyle
  format_signals: "circle|contest"
  total_players_in_division: 28
  distinct_places_in_division: 12
  direct_removals: 8
  cascade_removals: 5
  total_removals: 13
  max_cascade_depth: 3
  removed_person_canon: "Mikael Sundberg"
  removed_place: 5
  removal_iteration: 3
  removal_type: CASCADE
  trigger_person_canon: "Ville Haapasalo"
  trigger_place: 5
  unique_place_that_justified_direct: null
  status_in_pipeline: ABSENT_NO_COVERAGE
  investigation_priority: HIGH

  ---
  8. audit_summary.txt — Structure

  Section 1: Row Count Consistency

  === ROW COUNT CONSISTENCY ===

  PBP v85 input rows:                    27,980
  Post-02p5 Placements_Flat rows:        27,722
  Post-02p6 Placements_ByPerson rows:    26,553

  PBP v85 → post-02p6 difference:        1,427

  Rows in diff with status=PRESENT:          [N]
  Rows in diff with status=ABSENT_*:         [N]
    ABSENT_SLOT_COVERED:                     [N]
    ABSENT_NO_COVERAGE:                      [N]

  ABSENT_* count = PBP v85 - post-02p6:  [PASS / FAIL]

  The consistency check ABSENT_* count = PBP v85 - post-02p6 must PASS before any other output is considered valid. If
  it fails, the diff logic has a bug — this is the primary Phase 0 exit criterion.

  Section 2: Attribution Breakdown

  === ATTRIBUTION BREAKDOWN ===

  PP0_METADATA:              [N] rows   ([N] events affected)
  PP3_TEAM_SUPERSEDED:       [N] rows   ([N] events affected)
  PP4_EXACT_DUPLICATE:       [N] rows   ([N] events affected)
  PP5_DIRECT:                [N] rows   ([N] events affected)
  PP5_CASCADE:               [N] rows   ([N] events affected)
  MAIN_CITY_ARTIFACT:        [N] rows   ([N] events affected)
  UNKNOWN:                   [N] rows   ([N] events affected)

  Total attributed:          [N]
  Sum check = ABSENT_* count: [PASS / FAIL]

  Sum check must PASS. No row should appear in multiple attributions (first-match logic).

  Section 3: Investigation Candidates

  === INVESTIGATION CANDIDATES ===

  European-format candidates:
    HIGH confidence:           [N] rows   ([N] events)
    MEDIUM confidence:         [N] rows   ([N] events)
    LOW confidence:            [N] rows   ([N] events)
    Total:                     [N] rows

  Encoding artifact variant groups:
    Total groups:              [N]
    HIGH priority groups:      [N]  (have absent rows)
    Total affected PBP rows:   [N]
    Absent artifact rows:      [N]

  Cascade shadow:
    Events with cascade:       [N]
    Divisions with cascade:    [N]
    Direct removals:           [N] rows
    Cascade removals:          [N] rows
    Max cascade depth reached: [N] iterations
    Pool-format signal events: [N]  (HIGH investigation priority)

  UNKNOWN attribution:         [N] rows   ← require separate investigation

  Section 4: Internal Consistency Checks

  === CONSISTENCY CHECKS ===

  [PASS/FAIL] Attribution sum equals total absent rows
  [PASS/FAIL] No row appears in multiple attributions
  [PASS/FAIL] European candidates are subset of PP3_TEAM_SUPERSEDED + UNKNOWN attributions
  [PASS/FAIL] Cascade rows are subset of PP5_CASCADE attribution
  [PASS/FAIL] Encoding artifact absent rows are subset of ABSENT_* rows
  [PASS/FAIL] All ABSENT_NO_COVERAGE rows have slot_row_count_in_pipeline = 0
  [PASS/FAIL] All PRESENT rows have match_type != NONE

  All 7 checks must PASS. Any FAIL indicates a logic error in the audit tool and must be resolved before the outputs are
   used.

  ---
  9. Key Design Decisions and Rationale

  Why ABSENT_SLOT_COVERED vs. ABSENT_NO_COVERAGE?
  Investigation priority differs. A covered slot means something represents that placement — the question is whether the
   right thing does. An uncovered slot means a competitive result has no representation at all in the canonical dataset.
   The latter is always higher urgency.

  Why first-match attribution?
  A row removed by PP3 would also potentially satisfy PP4 or PP5 conditions. First-match reflects the actual pipeline
  behavior (pre-passes run in order) and prevents inflated counts. It also ensures attribution totals are additive and
  sum correctly.

  Why is reconstructed_name_tentative included?
  To save investigation time when reviewing candidates — not to automate recovery. The field name explicitly includes
  "tentative" to prevent it from being used as an identity assertion. The schema document must note this prominently.

  Why are format signals in the cascade file non-definitive?
  Because "Open Circle Contest" at a non-Worlds event might have a finals structure even though Worlds Circle Contest
  does not. Event-specific confirmation is required. The signals surface events worth investigating, not events
  confirmed as pool-only.

  What the audit does NOT produce:
  - A list of rows to recover
  - A confidence that any absent row was "incorrectly" removed
  - Any identity assignment or name reconstruction
  - Any modification to any pipeline input or output

  ┌──────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────┐
  │                 File                 │                                  Changes                                  │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_row_diff.csv                   │ Add match_multiplicity (SINGLE/MULTIPLE/NONE), pipeline_match_count;      │
  │                                      │ update investigation_note rule                                            │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_prepass_attribution.csv        │ Add unknown_priority (HIGH/MEDIUM/LOW/N/A)                                │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_european_format_candidates.csv │ Add pt_match_count, pt_match_ids, pt_match_status                         │
  │                                      │ (UNRESOLVED/UNIQUE/AMBIGUOUS)                                             │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_player_multi_place.csv         │ New file — full schema as specified above                                 │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_summary.txt                    │ Add UNKNOWN priority breakdown in Section 2; add 3 new consistency checks │
  │                                      │  in Section 4                                                             │
  ├──────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
  │ audit_cascade_shadow_events.csv      │ No changes — the multi-place file covers the overlapping ground           │
  └──────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────┘
