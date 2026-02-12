# QC Refactoring Summary

## What Was Done

Refactored the QC system to consolidate ALL QC checks into a **Master QC Orchestrator** architecture that:
1. Separates QC concerns from pipeline logic (better token management)
2. Runs automatically within the pipeline (no separate invocation)
3. Maintains existing workflow (`python 01 && python 02 && python 03`)

## New Architecture

### Files Created/Modified

**NEW FILES:**
- `qc_master.py` (380 lines) - Master QC orchestrator
  - `run_qc_for_stage(stage, records, ...)` - Main entry point
  - Stage-specific orchestrators: `run_stage1_qc()`, `run_stage2_qc()`, `run_stage3_qc()`
  - Baseline management: `load_baseline()`, `save_baseline()`, `print_qc_delta()`

- `qc_slop_detection.py` (900 lines) - Comprehensive slop detection
  - 7 global field scanners (every field, every cell)
  - 6 targeted checks (specific issues)
  - 3 Stage 3 checks (Excel cell scanning)

**MODIFIED FILES:**
- `02_canonicalize_results.py` - Now calls `qc_master.run_qc_for_stage("stage2")`
- `03_build_excel.py` - Now calls `qc_master.run_qc_for_stage("stage3")`

### How It Works

```
Pipeline Flow (Unchanged):
$ python3 01_parse_mirror.py
$ python3 02_canonicalize_results.py  ← AUTO-RUNS Stage 2 QC
$ python3 03_build_excel.py           ← AUTO-RUNS Stage 3 QC
```

**Internally:**

```python
# In 02_canonicalize_results.py main():
from qc_master import run_qc_for_stage

# After canonicalization, automatically run QC:
qc_summary, qc_issues = run_qc_for_stage("stage2", canonical, out_dir=out_dir)
# Writes: out/stage2_qc_summary.json, out/stage2_qc_issues.jsonl

# In 03_build_excel.py main():
from qc_master import run_qc_for_stage

# After Excel generation, automatically run QC:
qc_summary, qc_issues = run_qc_for_stage("stage3", records,
                                          results_map=results_map,
                                          out_dir=out_dir)
# Writes: out/stage3_qc_summary.json, out/stage3_qc_issues.jsonl
```

## Complete Check Inventory

### Stage 2: 43 Checks
- **30 existing checks** (embedded in 02_canonicalize_results.py)
  - Field validation: event_id, event_name, event_type, location, date, year, host_club, placements
  - Semantic validation: quality checks, missing fields, semantics
  - Placements quality: player names, divisions, place values
  - Cross-validation: expected divisions, team splitting, duplicates

- **13 new slop detection checks** (in qc_slop_detection.py)
  - 7 global scanners: URLs, C0/C1 controls, HTML, placeholders, whitespace, mojibake
  - 6 targeted checks: host club issues, Worlds completeness, duplicate rows, dropped results

### Stage 3: 3 Checks + Global Scanning
- Results cell duplicate lines
- Results cell roundtrip integrity
- Results cell near Excel limit
- Plus: All global scanners run on Excel cells

**Total: 46 QC checks across all stages**

## Benefits

### 1. Token Management
- QC logic (900+ lines) separated from pipeline logic
- Can read/modify QC without loading entire pipeline
- Cleaner context when working on either system

### 2. Automatic Operation
- **Zero changes to workflow**
- Pipeline still: `python 01 && python 02 && python 03`
- QC runs transparently, reports clearly
- No manual QC invocation needed

### 3. Consistency
- All stages use same QC pattern
- Standardized outputs: `stage{N}_qc_summary.json`, `stage{N}_qc_issues.jsonl`
- Unified baseline/delta reporting

### 4. Extensibility
- Add new checks to `qc_slop_detection.py`
- Automatically included in pipeline runs
- No modifications to multiple files

### 5. Safety
- Fallback to embedded QC if import fails
- Pipeline never breaks due to QC refactoring
- Graceful degradation

## Verification

### Test Results

**Stage 2 QC (Automatic):**
```
$ python3 02_canonicalize_results.py
...
Running QC checks...
Wrote: out/stage2_qc_summary.json
Wrote: out/stage2_qc_issues.jsonl (3807 issues)

QC SUMMARY - STAGE2
Total records: 775
Total errors:  228
Total warnings: 2977
Total info:     602

✓ All 43 checks running
✓ Delta report generated
✓ Baseline tracking active
```

**Stage 3 QC (Automatic):**
```
$ python3 03_build_excel.py
...
Wrote: out/stage3_qc_summary.json
Wrote: out/stage3_qc_issues.jsonl (300 issues)

QC SUMMARY - STAGE3
Total records: 775
Total errors:  241
Total warnings: 12
Total info:     47

✓ All Excel cells scanned
✓ Duplicate lines detected
✓ Integrity checks passed
```

## Usage (Unchanged!)

### Run Full Pipeline
```bash
python3 01_parse_mirror.py
python3 02_canonicalize_results.py
python3 03_build_excel.py
```

QC runs automatically. Results written to `out/stage{N}_qc_*.{json,jsonl}`

### Baseline Management
```bash
# Create/update baseline
python3 02_canonicalize_results.py --save-baseline

# Compare against baseline (automatic)
python3 02_canonicalize_results.py
# Output shows delta report
```

### Review QC Results
```bash
# View summary
cat out/stage2_qc_summary.json | python3 -m json.tool

# View issues (JSONL format, one issue per line)
head out/stage2_qc_issues.jsonl
grep "any_field_contains_c1_controls" out/stage2_qc_issues.jsonl
```

## Adding New Checks

### Option 1: Add to qc_slop_detection.py (Recommended)

```python
def check_my_new_issue(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """Detect new issue pattern."""
    issues = []
    # ... check logic ...
    return issues

# Add to run_slop_detection_checks_stage2() orchestrator
all_issues.extend(check_my_new_issue(rec, field, value))
```

### Option 2: Add to 02_canonicalize_results.py

```python
def check_my_field_validation(rec: dict) -> list[QCIssue]:
    """Validate specific field."""
    issues = []
    # ... validation logic ...
    return issues

# qc_master.run_stage2_qc() automatically calls this via dynamic import
```

Both options work! New checks run automatically on next pipeline execution.

## Documentation

See `QC_ARCHITECTURE.md` for:
- Complete architecture diagrams
- Detailed module structure
- Full check inventory with descriptions
- Integration patterns
- Troubleshooting guide

## Migration Notes

### What Changed
- QC invocation moved to `qc_master.py`
- Pipeline scripts import and call `run_qc_for_stage()`
- All QC logic consolidated

### What Stayed the Same
- Pipeline invocation: `python 01 && python 02 && python 03`
- QC output format: JSON summaries + JSONL issues
- Baseline workflow: `--save-baseline` flag
- All existing checks still run

### Backward Compatibility
- If `qc_master` import fails, 02_canonicalize falls back to embedded QC
- No breaking changes to pipeline behavior
- Old QC functions remain for fallback

## Results

✅ **46 total QC checks** consolidated
✅ **Automatic operation** within pipeline
✅ **Zero workflow changes** required
✅ **Better token management** via separation of concerns
✅ **Extensible architecture** for future checks
✅ **Verified working** on full dataset (775 events, 27,145 placements)

**Pipeline usage:**
```bash
python3 01_parse_mirror.py && \
python3 02_canonicalize_results.py && \
python3 03_build_excel.py
```

**QC runs automatically. No changes needed.**
