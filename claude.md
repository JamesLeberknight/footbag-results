CLAUDE.md
Project: Footbag Results Canonical Pipeline (3-Step Refactor)
North Star (Product Goal)

This repo produces an archive-quality canonical Excel workbook of footbag tournament results by reading an offline local HTML mirror of footbag.org event pages.

The pipeline must be refactored from one monolithic script into three small deterministic programs:

Parse HTML → Raw Extract CSV

Canonicalize + Structure Results → Canonical CSV

Render Canonical CSV → Final Excel Workbook

Primary requirement: correctness-first extraction and canonicalization, with explicit QC.
Never invent facts. Never guess missing fields.

Hard Constraints

Input is always the offline mirror (./mirror/)

No internet access is ever used

No dependency on hand-edited CSVs (aliases optional later)

Year is always available from source pages → no “unknown year” concept

The final Excel must be reproducible and deterministic

Architecture Overview (Three Programs)
Program 1 — 01_parse_mirror.py
Purpose

Read the raw mirrored HTML event pages and produce a complete raw fact extract:

➡️ out/stage1_raw_events.csv

This stage is purely extraction:

No semantic cleaning

No division canonicalization

No results interpretation beyond capturing structure

This CSV is the contract between parsing and analysis.

Output File 1: stage1_raw_events.csv
Row Granularity

One row per event page (event_id is the key).

Required Columns (Raw Fact Schema)
Column	Meaning
event_id	Footbag.org numeric event ID (string)
year	Event year extracted from page (must exist)
source_path	Local mirror file path
source_url	Canonical URL form (for reference only)
event_name_raw	Raw event/tournament name text
date_raw	Raw date string exactly as shown
location_raw	Raw location string exactly as shown
host_club_raw	Raw host club text
event_type_raw	Raw event type/category text
results_block_raw	Entire raw results text blob (pre.eventsPre)
html_parse_notes	Which selectors matched, fallback used
html_warnings	Missing fields, unusual structure
Extraction Rules

Preserve raw strings verbatim

Do not guess missing metadata

Always capture results_block_raw even if messy

If a field is missing, emit a warning (but do not fill)

Verification Gate: Stage 1

Program 1 is “done” only if it prints:

Total event pages discovered

Total events written

% missing for each raw field

Sample output of 5 events including raw results blobs

No stage 2 work begins until Stage 1 passes.

Program 2 — 02_canonicalize_results.py
Purpose

Read stage1_raw_events.csv and produce fully structured canonical event + results data:

➡️ out/stage2_canonical_events.csv

This is the intelligence layer:

Normalize metadata

Parse divisions and placements

Produce canonical results structures

Output File 2: stage2_canonical_events.csv
Row Granularity

Still one row per event, but with structured canonical fields.

Canonical Event Metadata Columns
Column	Meaning
event_id	Stable ID
year	Integer year
event_name	Clean display tournament name
date	Canonical date format (ISO if possible)
location	Canonical location string (City, Country)
host_club	Canonical club name (if parseable)
event_type	Canonical event type bucket
footbag_org_url	Deterministic URL
Canonical Results Representation (Core Requirement)

Footbag events contain multiple divisions (net + freestyle):

Open Singles Net

Open Doubles Net

Shred30

Circle Contest

Women’s divisions

Mixed doubles

Etc.

The canonical structure must represent:

Division name

Placement order

Competitors (player or team)

Clean names without clubs/cities injected

Canonical Results Columns
Column	Meaning
results_raw	Original raw results blob (unchanged)
placements_json	Structured list of parsed placements
Placement JSON Schema (Per Result Line)

Each placement entry must include:

{
  "division_raw": "...",
  "division_canon": "...",
  "place": 1,
  "competitor_type": "player|team",
  "player1_name": "...",
  "player2_name": "...",
  "entry_raw": "...",
  "parse_confidence": "high|medium|low",
  "notes": "why parsed this way"
}

Canonicalization Rules

Player fields must contain only player names (no clubs, no countries)

Location must resolve to a plausible geographic string

Divisions must map into a small canonical set when confident

If parsing fails, preserve raw text and mark confidence low

Verification Gate: Stage 2

Program 2 must output:

Events processed

Total placements parsed

Events with non-empty results but zero placements flagged

Division frequency report

List of low-confidence parses for human review

Stage 2 is only “done” when structured results are stable.

Program 3 — 03_build_excel.py
Purpose

Render the final archive workbook:

➡️ Footbag_Results_Canonical.xlsx

This stage performs no parsing.

It only formats the canonical dataset into the final spreadsheet.

Final Excel Contract

One sheet per year: YYYY.0

Columns are event_id

Fixed metadata rows:

Row Label
Tournament Name
Date
Location
Event Type
Host Club
footbag.org URL
Original Name
Results
Results Cell Rendering Rule

The Excel “Results” cell must be generated ONLY from:

placements_json

Never from raw text.

Results must print deterministically:

Divisions grouped

Placements sorted by place

Stable formatting across all events

Verification Gate: Stage 3

Program 3 must validate:

Sheet names match all years present

Row labels match the required template

Event columns are stable sorted

Spot-check 10 events: metadata matches stage2, results match placements

Claude Code Workflow Expectations

When working in this repo, Claude must:

Make only surgical refactors

Maintain deterministic output

Add QC summaries after each stage

Never introduce new manual CSV dependencies

Ask the user only when the HTML evidence is insufficient

Definition of Done

The refactor is complete when:

Running these commands works from repo root:

python 01_parse_mirror.py
python 02_canonicalize_results.py
python 03_build_excel.py


Produces:

out/stage1_raw_events.csv
out/stage2_canonical_events.cs
Footbag_Results_Canonical.xlsx


Final workbook matches the archive format exactly

Results are structured, reproducible, and clean

Key Principle

The pipeline converges toward truth:

Raw HTML → Raw Facts → Canonical Structure → Archive Workbook

No guessing. No hidden state. No monolith.
