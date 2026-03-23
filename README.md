# Footbag Results Dataset & Reconstruction Project

**Current release: v2.15.0**

## Overview

This repository contains a canonical, reproducible dataset of footbag competition results, along with tools for data extraction, normalization, quality control, and analysis.

The project is now structured around **two distinct tracks**:

### 1. Post-1997 Mirror-Era Dataset (Published)

- **Status:** Complete (for defined scope)
- **Coverage:** 1997–present
- **Source:** Footbag.org mirror (internet-era records)
- **Quality:** High-confidence, reproducible

This is the **primary published dataset** and the recommended source for:
- community use
- statistical analysis
- downstream database applications

---

### 2. Pre-1997 Historical Recovery (Ongoing)

- **Status:** Incomplete, under active development
- **Sources:**
  - Footbag World magazine (FBW) image extraction
  - `OLD_RESULTS.txt`
  - manual reconstruction and review

This effort is **evidence-based and provenance-driven**.  
Recovered data is not yet considered complete or release-quality.

---

## Output Artifacts

The project produces the following **official outputs**:

### 1. Community Spreadsheet (Post-1997)

- Clean, human-readable Excel workbook
- Organized by year
- Includes:
  - event results
  - player summaries
  - statistics
  - freestyle insights

This is the **primary public-facing deliverable**.

---

### 2. Canonical CSV Dataset

Relational dataset suitable for database ingestion:

- `events.csv`
- `event_results.csv`
- `event_result_participants.csv`
- `persons.csv`
- `event_disciplines.csv`

This dataset is:
- normalized
- deterministic
- version-controlled

---

### 3. HTML Event Comparison Viewer

A purpose-built QC tool for validating event-level data.

Features:
- side-by-side comparison of raw vs canonical results
- mismatch highlighting
- structured event inspection

This viewer is a **core artifact**, not just a diagnostic tool.  
It enables:
- human validation
- rapid QA of transformations
- confidence in final outputs

---

## Data Coverage

| Era        | Coverage | Status       |
|------------|----------|-------------|
| 1997–present | High     | Complete (for scope) |
| Pre-1997   | Partial  | Under reconstruction |

Important:

- The post-1997 dataset is the **only fully validated release**
- Pre-1997 data is **incomplete and evolving**
- No attempt is made to present early data as a complete historical record

---

## Data Philosophy

This project follows strict data engineering principles:

- **No guessing**
- **Provenance-first**
- **Deterministic outputs**
- **Reproducibility over completeness**

When uncertainty exists:
> Data is left incomplete rather than inferred.

---

## Pipeline Overview

The pipeline transforms raw sources into canonical outputs:

1. Parse source data (mirror HTML, legacy files)
2. Normalize events, divisions, and results
3. Resolve player identities
4. Generate canonical relational dataset
5. Produce spreadsheet outputs
6. Validate using event comparison viewer

The pipeline is stable for the post-1997 dataset.

Pre-1997 reconstruction follows a **separate workflow** and is not yet part of the finalized release pipeline.

---

## Repository Structure (Simplified)

```
pipeline/           # core pipeline scripts (stages 01–05p5)
tools/              # QC, identity resolution, workbook builder, viewer, patch scripts
qc/                 # per-check QC modules and master orchestrator
inputs/             # curated source data and overrides
  identity_lock/    # immutable lock snapshots (Persons_Truth, Placements_ByPerson)
out/
  canonical/        # AUTHORITATIVE output: events, results, participants, persons CSVs
overrides/          # person aliases, event metadata overrides, known issues
```

The `mirror/` directory (raw HTML source) is **not committed** — it is distributed as a release asset (`mirror.tar.gz`) and must be extracted locally before running the rebuild stage.


---

## Pre-1997 Recovery Project

This is a separate effort focused on reconstructing early footbag history.

Key characteristics:

- Source-driven (FBW images, legacy text)
- Requires OCR / AI-assisted extraction
- Human-in-the-loop validation
- Strong provenance tracking

Outputs from this effort are:
- incomplete
- subject to revision
- not yet merged into the primary dataset

---

## Status

- ✅ Post-1997 dataset published and stable
- 🚧 Pre-1997 recovery in progress
- 🔄 Continuous QC and incremental improvements

---

## Intended Use

This dataset is designed for:

- footbag community reference
- historical analysis
- statistical modeling
- database-backed applications

---

## Contributing

Contributions are welcome, especially for:

- early data recovery (pre-1997)
- data validation
- identity resolution (player matching)
- pipeline improvements

---

## License

Dataset: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
Pipeline code: MIT

---

## Notes

This project represents an ongoing effort to preserve and structure footbag competition history.

The post-1997 dataset provides a reliable foundation.  
The early-era reconstruction remains an open challenge.
