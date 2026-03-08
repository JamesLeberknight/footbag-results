# v1.0 Canonical Release Checklist

This checklist defines the exact steps required to produce and publish
a **canonical, identity-locked, archive-quality release**.

No step may be skipped.

---

## 0. Preconditions (Must Be True)

☐ `inputs/identity_lock/` contains **only**:
  - `Persons_Truth_Final_v31.csv`
  - `Persons_Unresolved_Organized_v27.csv`
  - `Placements_ByPerson_v33.csv`

☐ These files are:
  - Human-verified
  - Collision-free by construction
  - Treated as immutable for this release

☐ No intent to modify identity, merge people, or reclassify garbage

---

## 1. Repository Hygiene

☐ Core release scripts only at repo root:
  - `02p5_player_token_cleanup.py`
  - `03_build_excel.py`
  - `04_build_analytics.py`
  - `Makefile`

☐ `pipeline/` contains Rebuild Mode scripts (01, 01b, 01c, 02)
☐ `legacy/OLD_RESULTS.txt` present (required by 01b)
☐ `out/`, mirrors, and `.xlsx` files are gitignored
☐ README.md reflects current version and identity-lock files
☐ CHANGELOG.md contains a release entry for this version
☐ `CLAUDE.md` preserved (unchanged)

---

## 2. Clean Environment Test (Mandatory)

☐ New shell / fresh clone (no cached outputs)
☐ `make setup` succeeds (venv created, deps installed)
☐ `out/` directory created empty

---

## 3. Rebuild Mode

### 3.1 Parse mirror and build stage-2 events

☐ Mirror extracted: `tar -xzf mirror.tar.gz` → `mirror/`
☐ Run:
```bash
make rebuild
```
☐ Verify: `out/stage2_canonical_events.csv` exists

---

## 4. Release Mode

### 4.1 Generate Placements_Flat (Identity Lock Mode)

☐ Run:
```bash
python 02p5_player_token_cleanup.py \
  --identity_lock_placements_csv inputs/identity_lock/Placements_ByPerson_v33.csv \
  --out_dir out
```
☐ Verify: `out/Placements_Flat.csv` exists, row count = 25,679

### 4.2 Build Excel Workbook

☐ Ensure `out/stage2_canonical_events.csv` exists (from Rebuild Mode)
☐ Run:
```bash
python 03_build_excel.py
```
☐ Verify: `Footbag_Results_Canonical.xlsx` created/updated

### 4.3 Build Analytics

☐ Run:
```bash
python 04_build_analytics.py
```
☐ Verify output contains:
  - `[Gate3] PASS: COUNT(person_id) == COUNT(person_canon) = 3451`
  - `INFO: Lock sentinel written → out/persons_truth.lock`

### 4.4 Verify Lock Sentinel

☐ `out/persons_truth.lock` written by stage 04 after Gate 3 PASS.
   Confirm printed output shows:
   - `"file": "Persons_Truth_Final_v31.csv"`, `"rows": 3451`
   - `"file": "Persons_Unresolved_Organized_v27.csv"`, `"rows": 76`

---

## 5. QC Verification

```bash
make qc
```

☐ `qc/qc_master.py`: 0 errors, Gate 3 PASS (3451)
☐ `tools/32_post_release_qc.py`: all 6 checks pass (exit 0)
☐ `tools/33_schema_logic_qc.py`: all 7 checks pass (exit 0)
☐ `out/Analytics_Safe_Surface.csv`: 22,958 rows
☐ Tier-1 QC: 0 T1_UNMAPPED, 0 T1_MULTI

---

## 6. Release

☐ Commit all staged changes (docs, overrides, baselines — not `out/`)
☐ Update `CHANGELOG.md` with release notes
☐ Tag release:
```bash
git tag v1.0.18
git push origin main --tags
```
☐ Create GitHub Release: attach `mirror.tar.gz` as release asset
