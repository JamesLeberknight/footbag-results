# Footbag Results — Canonical Dataset (Merged)

This dataset provides a unified view of historical footbag competition results by combining:

- **POST-1997 canonical dataset** (primary, production dataset)
- **PRE-1997 reconstructed dataset** (historical recovery from archival sources)

The two datasets are **intentionally preserved side-by-side** to maintain accuracy, provenance, and auditability.

---

## 📦 Contents


events_all.csv
event_results_all.csv
event_result_participants_all.csv
persons_all.csv


These files form a complete relational dataset suitable for:

- database ingestion
- analytics
- web applications
- historical research

---

## 🧭 Data Sources

### POST-1997 (Primary Dataset)
- Derived from structured competition data (e.g., footbag.org mirror)
- High completeness and consistency
- Includes some **legacy placeholder events** for pre-1997 years

### PRE-1997 (Historical Reconstruction)
- Extracted from:
  - Footbag World (FBW) magazine scans
  - IFAB historical summaries
  - `OLD_RESULTS.txt`
- Conservative reconstruction:
  - no guessing
  - full provenance preserved
  - conflicts retained, not resolved

---

## ⚠️ Important Concept: Dual Representation of Early Events

For years **before 1997**, you may see **two versions of the same real-world event**:

### 1. POST-1997 Legacy Events
- Minimal or placeholder records
- Created for system completeness
- Example:

1980_worlds_oregon_city


### 2. PRE-1997 Reconstructed Events
- Rich, evidence-based reconstruction
- Derived from historical sources
- Example:

WORLD_CHAMPIONSHIPS_1980


👉 These are **not duplicates to be merged automatically**.  
They represent **different levels of historical fidelity**.

---

## 🔑 Key Field: `data_source`

Each row includes:

```text
data_source:
- POST1997
- PRE1997
Recommended usage:
Use only modern dataset
WHERE data_source = 'POST1997'
Use only reconstructed early dataset
WHERE data_source = 'PRE1997'
Use full dataset (advanced users)
-- no filter
👥 Persons

persons_all.csv contains three groups:

Category	Description
POST1997	Appears only in modern dataset
PRE1997_AND_POST1997	Appears in both datasets
PRE1997_ONLY	Historical-only players

Additional enrichment (optional, separate layer):

IFPA member IDs
BAP membership
Footbag Hall of Fame
📊 Data Integrity
Referential integrity: PASS
No missing event or person references
All datasets validated end-to-end
🧠 Design Principles

This dataset follows strict rules:

No guessing — uncertain data is preserved as unresolved
Provenance-first — all sources are retained
Non-destructive merging — conflicts are not silently resolved
Reproducible pipeline — all outputs can be rebuilt
🏁 Scope
Dataset	Coverage
PRE-1997	1980–1996
POST-1997	1997–present
Combined	1980–present
🚀 Intended Use

This dataset is designed for:

historical analysis of footbag competitions
player statistics and rankings
web applications and APIs
community research and preservation
📌 Notes
PRE-1997 data is incomplete but high-quality
Some early identities remain unresolved by design
Duplicate-looking events across datasets are expected and intentional
🧾 Version

Merged Canonical Dataset v1.0

PRE-1997 reconstruction finalized with expert review
POST-1997 dataset unchanged
Fully validated and release-ready
