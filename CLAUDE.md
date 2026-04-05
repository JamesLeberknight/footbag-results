# Footbag Results Pipeline — Canonical Contract (v4.0)

---

# 0. PURPOSE

The pipeline produces a **canonical historical dataset** of footbag competition results.

The PRIMARY output is:

```
out/canonical/*.csv
```

The Excel workbook is a **derived artifact only**.

A build is publishable ONLY if it passes the QC Gate.

---

# 1. PRIMARY DELIVERABLE (AUTHORITATIVE)

Canonical relational dataset:

* events.csv
* event_disciplines.csv
* event_results.csv
* event_result_participants.csv
* persons.csv

These files define the dataset.

---

# 2. SECONDARY DELIVERABLE

Community workbook

* Derived from canonical CSVs
* Must NOT introduce or modify data
* Must NOT be used for QC authority

---

# 3. CORE PRINCIPLES (NON-NEGOTIABLE)

1. Deterministic builds
2. No silent mutation
3. No guessing identities
4. No dropped competitors
5. Results fidelity is absolute
6. Canonical CSVs are the source of truth
7. Mirror-derived results are the highest-priority data

---

# 4. SOURCE PRIORITY POLICY

## 4.1 Mirror-era results (CRITICAL)

Mirror-derived results are authoritative.

The following are HARD FAIL:

* missing mirror placements
* truncated mirror divisions
* incorrect participant structure
* malformed results from mirror parsing
* loss of mirror data during canonicalization

---

## 4.2 Pre-1997 / Magazine / Legacy (SECONDARY)

Legacy recovery is valuable but not blocking unless it breaks structure.

Allowed as WARN:

* incomplete dates
* partial coverage
* missing metadata
* unresolved identities
* approximate reconstruction

NOT allowed:

* fabricated results
* corrupted placements
* structural inconsistencies

---

# 5. QC GATE (AUTHORITATIVE)

A build is publishable ONLY if:

```
QC_STATUS = PASS
```

QC is evaluated on canonical CSVs AFTER Stage 05p5.

---

## 5.1 HARD FAIL CONDITIONS (must be zero)

### EVENTS

* duplicate event_id
* missing required fields

### EVENT_RESULTS

* duplicate (event_id, discipline, place)
* invalid placement structure
* truncated mirror divisions

### EVENT_RESULT_PARTICIPANTS

* singles ≠ 1 participant
* doubles ≠ 2 participants
* duplicate participant in placement

### PERSONS

* duplicate person_id
* missing referenced person_id

### CROSS-TABLE INTEGRITY

* orphan event_id
* orphan result_id
* orphan person_id

### DATA PURITY

* non-person artifacts in participant fields
* club/city contamination
* malformed names

### MIRROR FIDELITY

* mirror placements missing from canonical dataset
* mirror divisions missing or truncated
* incorrect parsing of mirror results

---

If ANY fail:

```
BUILD = INVALID
```

---

## 5.2 WARNING CONDITIONS (allowed)

* pre-1997 incomplete dates
* partial legacy coverage
* unresolved identities
* mirror vs parsed minor mismatches (non-structural)
* split doubles pairs (recoverable)

Warnings must be documented.

---

## 5.3 INFO CONDITIONS

* formatting issues
* event key inconsistencies
* cosmetic issues

---

## 5.4 QC EXECUTION CONTRACT

Must run:

```
python pipeline/qc/run_qc.py
```

QC must evaluate canonical CSVs only.

Workbook QC is secondary.

---

## 5.5 NO MANUAL OVERRIDES

QC failures must NOT be ignored.

Fix at:

* parser
* overrides
* canonical inputs

---
## 5.6 DEVELOPMENT LOOP (IMPORTANT)

During active development and pipeline refinement:

- Temporary QC failures are allowed.
- Structural changes to parsing or canonicalization may cause transient QC failures.
- These failures must be resolved before completing the task.

The required workflow is:

    modify → rebuild → run QC → fix → repeat → PASS

A task is NOT complete until QC_STATUS = PASS.

QC_STATUS = PASS is required for:
- release builds
- publishable datasets
- checkpoint commits

But it is NOT required at every intermediate step during development.

# 6. PIPELINE CONTRACT

From run_pipeline.sh:

```
rebuild → release → qc
```

QC runs ONLY after canonical CSV generation.

---

# 7. STAGE RESPONSIBILITIES

## Stage 01–02

* Parse mirror
* Preserve all tokens

## Stage 02p5–02p6

* Apply identity lock
* Structural cleanup

## Stage 03–04

* Workbook + analytics (non-authoritative)

## Stage 05

* Export canonical CSVs (AUTHORITATIVE)

## Stage 05p5

* Final remediation

No stage may violate QC constraints.

---

# 8. DEFINITION OF DONE

The dataset is COMPLETE when:

1. canonical CSVs exist
2. QC gate returns PASS
3. no hard-fail conditions exist
4. mirror results are fully preserved

---

END OF CONTRACT
