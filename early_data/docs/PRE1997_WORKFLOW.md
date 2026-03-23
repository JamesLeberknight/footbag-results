# Pre-1997 Historical Recovery — Workflow

## Purpose

This is a **separate, independent recovery project** for pre-1997 footbag competition history.

It does **not** modify the published post-1997 dataset. Nothing here touches:
- `out/canonical/` (published canonical CSVs)
- `inputs/identity_lock/` (post-1997 identity lock)
- `inputs/Persons_Truth.csv` or `Placements_ByPerson.csv`

All pre-1997 recovery work is isolated under `early_data/` until it reaches sufficient quality to be considered for a separate canonical release.

---

## Core Philosophy

**Provenance-first.**
Every extracted result must be traceable to a specific source image, slide, and issue.

**No guessing.**
If a name, date, or result is ambiguous, leave the field blank or mark it `uncertain`.
Do not infer what is not clearly visible.

**Prefer blank over wrong.**
An empty field is better than a fabricated one.
Incomplete data is expected and acceptable at this stage.

**Unresolved names remain unresolved.**
When a person extracted from a pre-1997 source cannot be confidently matched to a known person in Persons_Truth.csv, record them with `matched_person_id` blank. Do not force a match.

---

## Data Stages

### Stage 1 — Page Inventory

**Script:** `early_data/scripts/01_build_page_inventory.py`
**Output:** `early_data/page_inventory/fbw_page_inventory.csv`

Build the master inventory of all source pages. One row per slide/image. Captures issue, volume, approximate year, and source type. Leaves `has_results` and `has_worlds_data` blank for human annotation.

### Stage 2 — Image Preprocessing

**Script:** `early_data/scripts/03_rename_pages.py`
**Script:** `early_data/scripts/02_image_qc_report.py`
**Output:** `early_data/preprocessed_pages/` (canonical filenames), `early_data/page_inventory/image_qc_report.csv`

Copy/rename page images to stable canonical filenames (`FBW_V03_N01_p001.jpeg`).
Run QC report to confirm dimensions and orientation.
Any additional rotation or cropping corrections happen here before transcription.

### Stage 3 — Event Block Extraction

**Output:** `early_data/event_blocks/fbw_event_blocks.csv`

For each page that contains results, create one row per event/division block.
Capture the raw event name, date, location, and division heading as seen on the page.
Do not normalize or interpret yet.

`extraction_status` values:
- `pending` — not yet extracted
- `in_progress` — partially extracted
- `complete` — all visible blocks extracted
- `partial` — some blocks extracted; rest unclear or unreadable
- `skip` — page does not appear to contain results

### Stage 4 — Placement Extraction

**Output:** `early_data/placements/fbw_placements_flat.csv`

For each event block, record individual placement rows.
Capture raw placement number, player name(s), team name, and division exactly as they appear.
Do not canonicalize names at this stage.

`confidence` values:
- `high` — text is clear and unambiguous
- `medium` — text is slightly unclear but likely correct
- `low` — text is difficult to read; transcription uncertain
- `uncertain` — could not be confidently read

`review_status` values:
- `pending`
- `accepted`
- `flagged` — needs human review
- `rejected` — confirmed error

### Stage 5 — Person Matching

**Output:** `early_data/review/fbw_person_match.csv`

Match extracted player names against the existing identity backbone:
- `out/Persons_Truth.csv` (post-1997 persons, 3,468 entries as of v2.15.0)
- `out/canonical/persons.csv`

Record each match attempt with method and confidence.

`match_method` values:
- `exact` — exact string match
- `fuzzy` — fuzzy string match (RapidFuzz or similar)
- `alias` — matched via known alias
- `manual` — human reviewer confirmed match
- `no_match` — no plausible match found

Leave `matched_person_id` blank when unresolved. Do not invent a match.

### Stage 6 — Pre-1997 Canonical Outputs

**Future work.** Once sufficient extraction and review is complete, a separate pipeline stage will produce pre-1997 canonical CSVs, analogous to the post-1997 pipeline but with explicit uncertainty markers.

These outputs will be:
- clearly labeled as pre-1997 (incomplete)
- kept separate from the post-1997 published dataset
- subject to their own QC gate before any merge is considered

---

## Transcription Assistance

At Stage 3–4, **Gemini or another large multimodal model** may be used to assist with transcription from cleaned page images. The prompts directory (`early_data/prompts/`) is reserved for prompt templates.

Key constraints for model-assisted transcription:
- The model output is **a first draft only**, subject to human review
- All extracted data must be reviewed before `review_status = accepted`
- Confidence scores from the model should be recorded in the `confidence` field
- Model transcription does not bypass the provenance requirement — `source_file` and `slide_num` must always be populated

---

## Known Sources

| Source | Coverage | Notes |
|---|---|---|
| Footbag World Vol. 2–14 | ~1980–1995 | Primary source |
| IFAB Rulebook (Worlds History) | Pre-1997 Worlds | 4 pages, dense data |
| `inputs/OLD_RESULTS.txt` | Various pre-1997 | Legacy plaintext; partially integrated |
| `inputs/magazine_scan_index.csv` | Vol. 2–14 | Existing index of scanned pages by event |

---

## File Naming Convention

Canonical image filenames (produced by `03_rename_pages.py`):

```
FBW_V{vol:02d}_N{num:02d}_p{page:03d}.{ext}
```

Examples:
- `FBW_V03_N01_p001.jpeg` — Footbag World Vol. 3 No. 1
- `FBW_V10_N02_p001a.jpeg` — Footbag World Vol. 10 No. 2, page 1a
- `IFAB_WH_p001.jpeg` — IFAB Rulebook Worlds History, page 1

---

## What This Is Not

- This is not a quick import or backfill of `OLD_RESULTS.txt` into the post-1997 pipeline
- This is not a complete historical archive — it is an evidence-based reconstruction
- Results that cannot be sourced to a specific image or document are excluded
