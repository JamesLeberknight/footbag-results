# claude.md v5 — Archive‑Quality Footbag Results Pipeline

## Mission

Build an **archive‑quality, deterministic, auditable dataset** of historical footbag competition results.

The goal is **not maximal recall**, but **maximal trust**:

* No guessing
* No silent repairs
* No heuristic identity merges
* Clear separation between *raw*, *cleaned*, *canonical*, and *human‑verified truth*

The final Excel workbook must be suitable for:

* Human review
* Statistical analysis
* Long‑term archival use

---

## Core Principles (Non‑Negotiable)

1. **Presentability > Correctness**

   * Correctness is evaluated *only* on presentable values.
   * Non‑presentable values are excluded, not repaired.

2. **Omission is safer than misrepresentation**

   * It is acceptable to drop data.
   * It is not acceptable to fabricate clarity.

3. **Human truth beats heuristics**

   * Identity merges occur **only** via explicit human‑maintained files.

4. **Determinism**

   * Same inputs → same outputs, byte‑for‑byte.

5. **Auditability**

   * Every transformation must be explainable and reversible.

---

## Key Definitions

### Presentable Value

A string that:

* Represents **exactly one real‑world concept**
* Contains **no embedded metadata** (locations, rankings, tricks, notes, emojis)
* Contains **no conjunctions** (and/or/+/\/=)
* Is suitable for direct display in a publication

Examples:

* ✅ `Rick Reese`
* ❌ `CO, USA) and Rick Reese`
* ❌ `Andreas Wolff 🇩🇪 Germany`

Only presentable values may be evaluated for correctness.

### Division Categorization

Division categories (freestyle, net, golf, sideline, unknown) are derived **programmatically** from division name keywords — there is no human-maintained division override file. Divisions that cannot be safely mapped are explicitly labeled `unknown`.

---

### player_id

* A **raw identity token** derived from source text
* Preserved aggressively
* Never merged automatically
* Many player_ids may refer to the same human

player_id answers: *“What did the source say?”*

---

### person_id

* A **stable, canonical identity** representing a real human
* Assigned only via human verification
* One person_id ⇔ one real person

person_id answers: *“Who is this actually?”*

---

## Pipeline Overview

```
01  → HTML mirror ingestion (raw)
02  → Structural parsing & normalization
02p5→ Player token cleanup (NO identity merges)
03  → Canonical tables & QC datasets
04  → Excel presentation & human QC surface
04b → Recovery layer (confidence-labeled, optional)
```

Each stage has a **strict responsibility boundary**.

---

## Stage 01 — Raw Ingestion

**Purpose:**

* Mirror historical HTML
* Preserve original content faithfully

**Rules:**

* No cleaning
* No interpretation
* No normalization

**Outputs:**

* Raw mirrored text

---

## Stage 02 — Structural Parsing

**Purpose:**

* Extract events, divisions, placements, players, teams
* Normalize structure, *not meaning*

**Rules:**

* Preserve all tokens
* Do not modify names beyond whitespace normalization

**Outputs:**

* `events_df`
* `placements_df`
* `players_df`
* `teams_df`

---

## Stage 02.5 — Player Token Cleanup (NO IDENTITY MERGES)

**Purpose:**
Clean *name strings only* while preserving identity multiplicity.

**Allowed:**

* Remove rankings, ages, IFPA numbers
* Remove locations and parenthetical metadata
* Normalize diacritics for comparison (not display)

**Forbidden:**

* Merging player_ids
* Guessing identities
* Collapsing similar names

**Outputs:**

* `player_name_clean`
* `name_status`: `ok | suspicious | needs_review | junk`
* Alias *suggestions* only

---

## Stage 03 — Canonical & QC Tables

**Purpose:**

* Produce normalized datasets
* Surface ambiguity explicitly

**Key Outputs:**

* `Placements_Flat`
* `Persons_Raw`
* `Players_Alias_Candidates`
* `Teams_Alias_Candidates`

**Rules:**

* No human truth is created here
* QC is additive, never destructive

---

## Human Truth Layer (Out‑of‑Band)

**Files:**

* `person_aliases.csv`
* `events_overrides.jsonl`

**Rules:**

* Human‑maintained only
* Version‑controlled
* Explicit decisions only

This is the *only* place identity merges occur.

---

## Stage 04 — Excel Presentation & QC Surface

**Purpose:**
Produce the **final Excel workbook** used for:

* Human inspection
* Manual verification
* Long‑term archival reference

Stage 04 is a **presentation layer**, not a cleaning layer.

### Responsibilities

* Apply human truth (person_id mappings)
* Enforce presentability constraints
* Exclude junk and non‑presentable values
* Produce clearly labeled QC sheets

### Key Rule

> **“Done” means every visible cell is as clean as possible.**

If a value is not presentable, it must not appear in the workbook.

---

## Stage 04b — Recovery Layer

**Purpose:**
Recover rejected ByPerson placements using confidence-labeled methods, without modifying canonical data.

### Methods (in priority order)

1. **Same-event exact** — player_id matches a person_id already in the same event
2. **Cross-event exact** — player_id matches a person_id seen in other events
3. **Last-name expansion** — unambiguous last-name match within event context
4. **Event context** — contextual signals from co-competitors

### Key Rule

> **Canonical data is never modified.** Recovery is a derived, optional surface.

Recovered placements carry explicit confidence labels so downstream consumers can filter by trust level.

### Outputs

* `Recovery_Candidates.csv` — all candidate recoveries with confidence labels
* `Placements_ByPerson_WithRecovery.csv` — merged canonical + recovered placements
* `Recovery_Summary.json` — aggregate statistics

---

## Persons_Truth Table

**Invariant:**

* One row = one real human
* One person_id
* One presentable canonical name

No duplicates.
No junk.
No metadata.

---

## QC Philosophy

* QC detects; it does not repair
* Ambiguity is surfaced, not hidden
* Every exclusion is intentional

If something looks wrong, the answer is:

> “Which stage is responsible?”

---

## QC Validation Results

Manual QC validation was performed on the Excel workbook using analytical pivots.

### Anomaly Classification

* **Tier-1 (Structural Failures):** None detected
* **Tier-2 (Visible, Bounded, Acceptable):** narrative winner rows, format-based unknown divisions, legacy unmapped labels

### Pivot Results

| Pivot | Description | Result |
|-------|-------------|--------|
| #2 | Narrative Winners | PASS — low-frequency, non-distorting |
| #3 | Partner Realism | PASS — realistic sparsity, no identity inflation |
| #4 | Temporal Plausibility | PASS — temporally clustered careers, no cross-decade inflation |
| #5 | Division Consistency | PASS — unknown divisions are explicit and low-frequency |

### Known Limitation

Pivot #1 (Full Career Timeline) deferred — requires person-long analytical sheet not yet created. Temporal plausibility validated via Pivot #4 on raw surface.

---

## Definition of Done

The pipeline is **done** when:

* All Excel cells are presentable
* Every person_id maps to exactly one human
* No heuristic identity merges remain
* All remaining ambiguity is explicit and reviewable
* Structural QC validation passes with zero Tier-1 anomalies

---

## Final Warning to Future Agents

> **Do not be helpful. Be correct.**

If you are unsure:

* Stop
* Emit QC
* Ask for human input

Silence is failure. Guessing is corruption.
