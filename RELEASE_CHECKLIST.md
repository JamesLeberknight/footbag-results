# Release Checklist

Exact steps required to produce and publish a canonical, identity-locked release.
No step may be skipped.

---

## 0. Preconditions

Confirm `inputs/identity_lock/` contains exactly:

| File | Version | Rows |
|---|---|---|
| `Persons_Truth_Final_v36.csv` | v36 | 3,455 |
| `Persons_Unresolved_Organized_v28.csv` | v28 | 82 |
| `Placements_ByPerson_v44.csv` | v44 | 26,401 |

These files are human-verified and immutable for this release.

---

## 1. Repository Hygiene

- [ ] `pipeline/` contains all 9 pipeline scripts (01 through 05)
- [ ] `inputs/identity_lock/` references in `Makefile` and `run_pipeline.sh` match current versions
- [ ] `out/`, mirrors, and `.xlsx` files are gitignored
- [ ] `README.md` reflects current version and identity lock versions
- [ ] `CHANGELOG.md` has a release entry for this version

---

## 2. Setup (first time or new clone)

```bash
make setup
```

Verify: `.venv/` created, `out/` directory exists.

---

## 3. Rebuild Mode

Requires `mirror/` extracted in the repo root.

```bash
tar -xzf mirror.tar.gz
make rebuild
```

Verify:
- [ ] `out/stage2_canonical_events.csv` exists
- [ ] Stage 2 QC: 0 errors, ≤15 warnings (check output)

---

## 4. Release Mode

```bash
make release
```

Runs stages 02p5 → 03 → 04 → 04B → 05 in sequence.

Verify after completion:

**Stage 02p5:**
- [ ] `out/Placements_Flat.csv` exists, 26,401 rows
- [ ] `out/Placements_ByPerson.csv` exists, 26,401 rows

**Stage 03:**
- [ ] `Footbag_Results_Canonical.xlsx` created/updated
- [ ] Stage 3 QC: 0 errors, 0 warnings

**Stage 04:**
- [ ] Output contains: `[Gate3] PASS: COUNT(person_id) == COUNT(person_canon) = 3456`
- [ ] `out/persons_truth.lock` written
- [ ] Lock sentinel shows `Persons_Truth_Final_v36.csv`, rows: 3456

**Stage 04B:**
- [ ] `Footbag_Results_Community.xlsx` created/updated
- [ ] Output shows: `Honours: 84/84 BAP names matched`

**Stage 05:**
- [ ] `out/canonical/events.csv` — 778 rows
- [ ] `out/canonical/event_disciplines.csv` — (check output)
- [ ] `out/canonical/event_results.csv` — (check output)
- [ ] `out/canonical/event_result_participants.csv` — (check output)
- [ ] `out/canonical/persons.csv` — 3,455 rows
- [ ] All 4 key uniqueness checks: PASS

---

## 5. QC Verification

```bash
make qc
```

- [ ] `qc/qc_master.py`: 0 errors
- [ ] `tools/32_post_release_qc.py`: all 6 checks pass (exit 0), ≤1 warning
- [ ] `tools/33_schema_logic_qc.py`: all 7 checks pass (exit 0)

---

## 6. Release

```bash
# Commit all staged changes
git add -p
git commit -m "release: vX.Y.Z — <summary>"

# Update changelog
# Edit CHANGELOG.md with release notes

# Tag and push
git tag vX.Y.Z
git push origin main --tags
```

- [ ] GitHub Release created
- [ ] `mirror.tar.gz` attached as release asset
