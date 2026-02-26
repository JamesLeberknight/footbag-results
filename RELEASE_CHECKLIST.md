# v1.0 Canonical Release Checklist

This checklist defines the exact steps required to produce and publish
a **canonical, identity-locked, archive-quality release**.

No step may be skipped.

---

## 0. Preconditions (Must Be True)

☐ `inputs/identity_lock/` contains **only**:
  - `Persons_Truth_Final_v14.csv`
  - `Persons_Unresolved_Organized_v12.csv`
  - `Placements_ByPerson_v14.csv`

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
  --identity_lock_placements_csv inputs/identity_lock/Placements_ByPerson_v14.csv \
  --identity_lock_persons_truth_csv inputs/identity_lock/Persons_Truth_Final_v14.csv \
  --identity_lock_unresolved_csv inputs/identity_lock/Persons_Unresolved_Organized_v12.csv \
  --out_dir out
