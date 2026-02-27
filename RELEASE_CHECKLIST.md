# v1.0 Canonical Release Checklist

This checklist defines the exact steps required to produce and publish
a **canonical, identity-locked, archive-quality release**.

No step may be skipped.

---

## 0. Preconditions (Must Be True)

☐ `inputs/identity_lock/` contains **only**:
  - `Persons_Truth_Final_v26.csv`
  - `Persons_Unresolved_Organized_v23.csv`
  - `Placements_ByPerson_v27.csv`

☐ These files are:
  - Human-verified
  - Collision-free by construction
  - Treated as immutable for this release

☐ No intent to modify identity, merge people, or reclassify garbage

---

## 1. Repository Hygiene

☐ Core scripts only in root (or `scripts/`):
  - `02p5_player_token_cleanup.py`
  - `03_build_excel.py`
  - `04_build_analytics.py`

☐ Non-core code moved to:
  - `qc/`, `tools/`, or `legacy/`

☐ `out/`, mirrors, and `.xlsx` files are gitignored

☐ README.md reflects **identity-lock release mode**

☐ CHANGELOG.md contains a v1.0.0 entry describing identity lock

☐ `CLAUDE.md` preserved (unchanged)

---

## 2. Clean Environment Test (Mandatory)

☐ New shell / fresh clone (no cached outputs)
☐ Virtual environment created
☐ Dependencies installed from `requirements.txt`
☐ `out/` directory created empty

---

## 3. Identity-Locked Pipeline Run

### 3.1 Generate Placements_Flat (Identity Lock Mode)

☐ Run:
```bash
python 02p5_player_token_cleanup.py \
  --identity_lock_placements_csv inputs/identity_lock/Placements_ByPerson_v27.csv \
  --out_dir out
```

☐ Verify output: `out/Placements_Flat.csv` exists, row count = 25679

### 3.2 Build Excel Workbook

☐ Ensure `out/stage2_canonical_events.csv` exists (from Rebuild Mode or prior run)

☐ Run:
```bash
python 03_build_excel.py
```

☐ Verify: `Footbag_Results_Canonical.xlsx` created/updated

### 3.3 Build Analytics

☐ Run:
```bash
python 04_build_analytics.py
```

☐ Verify output contains:
  - `[Gate3] PASS: COUNT(person_id) == COUNT(person_canon) = 3365`
  - `INFO: Lock sentinel written → out/persons_truth.lock`

### 3.4 Verify Lock Sentinel

☐ `out/persons_truth.lock` is written automatically by stage 04 after Gate 3 PASS.
   Confirm the printed output contains the expected filename and row count:
   - `"file": "Persons_Truth_Final_v26.csv"`, `"rows": 3365`
   - `"file": "Persons_Unresolved_Organized_v23.csv"`, `"rows": 267`

---

## 4. QC Verification

☐ Stage 2 QC: 0 errors
☐ Gate 3: PASS (3365)
☐ Tier-1 QC: 0 T1_UNMAPPED, 0 T1_MULTI
☐ `out/Analytics_Safe_Surface.csv`: 22935 rows

---

## 5. Release

☐ Commit all outputs (except `out/` which is gitignored)
☐ Tag release: `git tag v1.0.6` (or next appropriate version)
☐ Update CHANGELOG.md with release notes
