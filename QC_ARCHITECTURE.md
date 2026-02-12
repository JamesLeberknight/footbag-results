# QC Architecture Documentation

## Overview

The QC (Quality Control) system is now consolidated into a **Master QC Orchestrator** that separates QC concerns from main pipeline logic. This architecture improves token management, code organization, and maintainability.

## Architecture Diagram

```
Pipeline Flow:
┌─────────────────────────────────────────────────────────────────┐
│  01_parse_mirror.py                                             │
│  ├─ Extract HTML → raw events                                   │
│  └─ Output: out/stage1_raw_events.csv                           │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  02_canonicalize_results.py                                     │
│  ├─ Canonicalize raw → structured                               │
│  ├─ Output: out/stage2_canonical_events.csv                     │
│  └─ **AUTO-INVOKES: qc_master.run_qc_for_stage("stage2")**     │
│     ├─ Runs 30+ existing field validation checks                │
│     ├─ Runs 13 new slop detection checks                        │
│     └─ Outputs:                                                 │
│         ├─ out/stage2_qc_summary.json                           │
│         └─ out/stage2_qc_issues.jsonl                           │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  03_build_excel.py                                              │
│  ├─ Build Excel workbook                                        │
│  ├─ Output: Footbag_Results_Canonical.xlsx                      │
│  └─ **AUTO-INVOKES: qc_master.run_qc_for_stage("stage3")**     │
│     ├─ Scans all Excel cells                                    │
│     ├─ Checks duplicate lines, integrity                        │
│     └─ Outputs:                                                 │
│         ├─ out/stage3_qc_summary.json                           │
│         └─ out/stage3_qc_issues.jsonl                           │
└─────────────────────────────────────────────────────────────────┘
```

## QC Module Structure

```
QC System Components:
├─ qc_master.py (Master Orchestrator)
│  ├─ run_qc_for_stage(stage, records, ...) ← Main entry point
│  ├─ run_stage1_qc() → Stage 1 checks
│  ├─ run_stage2_qc() → Stage 2 checks + slop detection
│  ├─ run_stage3_qc() → Stage 3 Excel checks
│  ├─ load_baseline() / save_baseline()
│  ├─ print_qc_delta() / print_qc_summary()
│  └─ Helper functions
│
├─ qc_slop_detection.py (Comprehensive Slop Detection)
│  ├─ Global field scanners (7 checks)
│  │  ├─ any_field_contains_url
│  │  ├─ any_field_contains_c0_controls
│  │  ├─ any_field_contains_c1_controls
│  │  ├─ any_field_contains_html_or_entities
│  │  ├─ any_field_contains_placeholder_or_instructional_text
│  │  ├─ any_field_has_whitespace_slop
│  │  └─ any_field_has_bogus_character_sequences
│  │
│  ├─ Targeted checks (6 checks)
│  │  ├─ host_club_suspicious_prefix_or_markup
│  │  ├─ host_club_contains_url_or_contact
│  │  ├─ worlds_missing_expected_disciplines
│  │  ├─ worlds_results_suspiciously_small
│  │  ├─ results_raw_has_strong_signals_but_output_empty
│  │  └─ placements_duplicate_rows
│  │
│  ├─ Stage 3 checks (3 checks)
│  │  ├─ results_cell_duplicate_lines
│  │  ├─ results_cell_roundtrip_missing_any_placement
│  │  └─ results_cell_near_excel_limit
│  │
│  └─ Orchestrators
│     ├─ run_slop_detection_checks_stage2()
│     └─ run_slop_detection_checks_stage3_excel()
│
└─ 02_canonicalize_results.py (Embedded Checks - 30 checks)
   ├─ Field validation checks
   │  ├─ check_event_id, check_event_name, check_event_type
   │  ├─ check_location, check_date, check_year, check_host_club
   │  └─ check_placements_json, check_results_extraction
   │
   ├─ Semantic validation checks
   │  ├─ check_event_name_quality, check_year_range
   │  ├─ check_missing_required_fields
   │  ├─ check_location_semantics, check_date_semantics
   │  ├─ check_host_club_semantics, check_country_names
   │  └─ check_field_leakage
   │
   ├─ Placements quality checks
   │  ├─ check_player_name_quality, check_division_name_quality
   │  ├─ check_place_values, check_place_sequences
   │  └─ check_string_hygiene
   │
   └─ Cross-validation checks
      ├─ check_expected_divisions, check_division_quality
      ├─ check_team_splitting, check_year_date_consistency
      ├─ check_event_id_uniqueness, check_worlds_per_year
      ├─ check_duplicates
      └─ check_host_club_location_consistency
```

## How QC is Invoked

### Automatic Invocation (Default)

The pipeline runs **automatically** with QC checks integrated:

```bash
# Run the full pipeline - QC runs automatically
python3 01_parse_mirror.py
python3 02_canonicalize_results.py
python3 03_build_excel.py
```

**No separate QC invocation needed!** The QC checks run automatically within each stage.

### How It Works Internally

1. **Stage 2 (02_canonicalize_results.py)**:
   ```python
   # At end of main():
   from qc_master import run_qc_for_stage

   qc_summary, qc_issues = run_qc_for_stage("stage2", canonical, out_dir=out_dir)
   ```

2. **Stage 3 (03_build_excel.py)**:
   ```python
   # After Excel generation:
   from qc_master import run_qc_for_stage

   qc_summary, qc_issues = run_qc_for_stage("stage3", records,
                                             results_map=results_map,
                                             out_dir=out_dir)
   ```

### Optional: Direct QC Invocation

If you ever need to run QC checks separately (not recommended):

```python
from qc_master import run_qc_for_stage

# Stage 2 QC
records = load_stage2_records()
summary, issues = run_qc_for_stage("stage2", records, out_dir=Path("out"))

# Stage 3 QC
records = load_stage2_records()
results_map = build_results_map(records)
summary, issues = run_qc_for_stage("stage3", records, results_map=results_map)
```

## QC Outputs

Each stage produces standardized outputs:

### Stage 2 Outputs
- **out/stage2_qc_summary.json** - Summary statistics
  ```json
  {
    "stage": "stage2",
    "total_records": 775,
    "total_errors": 228,
    "total_warnings": 2977,
    "total_info": 602,
    "counts_by_check": {
      "any_field_contains_c1_controls": {"ERROR": 55, "WARN": 0, "INFO": 0},
      ...
    }
  }
  ```

- **out/stage2_qc_issues.jsonl** - Detailed issues (one JSON object per line)
  ```json
  {"check_id": "any_field_contains_c1_controls", "severity": "ERROR",
   "event_id": "1471686537", "field": "results_raw",
   "message": "Field contains C1 control characters: U+0092",
   "example_value": "...", "context": {"char_codes": ["U+0092"]}}
  ```

### Stage 3 Outputs
- **out/stage3_qc_summary.json** - Summary statistics
- **out/stage3_qc_issues.jsonl** - Detailed issues

### Baseline Management

QC baselines track expected issue counts to detect regressions:

```bash
# Create/update baseline
python3 02_canonicalize_results.py --save-baseline

# Compare against baseline (automatic)
python3 02_canonicalize_results.py
# Output will show delta report if baseline exists
```

Baseline files:
- **data/qc_baseline_stage2.json**
- **data/qc_baseline_stage3.json**

## Check Types and Severities

### Severity Levels

- **ERROR**: Unacceptable data corruption or integrity violations
  - URLs in canonical fields
  - Control characters (C0, C1)
  - Duplicate rows
  - Missing required fields for Worlds events
  - Dropped results (source has data but output is empty)

- **WARN**: Data quality issues that should be reviewed
  - Whitespace slop (trailing spaces, tabs)
  - Suspicious patterns (host club markup)
  - Placeholder text (TBD, ??)
  - Team splitting issues
  - Division categorization problems

- **INFO**: Observations for awareness, not actionable errors
  - Placeholder text in non-critical fields
  - Multiple locations for same host club (could be legitimate)
  - Place sequences that don't start at 1 (tied/partial results)

## Complete Check Inventory

### Stage 2 Checks (43 total)

**Field Validation (9 checks)**
1. event_id_missing, event_id_pattern
2. event_name_missing, event_name_html, event_name_url, event_name_placeholder
3. event_type_invalid
4. location_missing, location_broken_source, location_url, location_email, location_hosted_by, location_multi_sentence, location_too_long, location_has_tba, location_has_tbd, location_has_narrative
5. date_missing_worlds, date_ical_remnant, date_year_mismatch
6. year_missing_worlds, year_out_of_range
7. host_club_url
8. placements_json_invalid, placements_place_invalid, placements_competitor_type_invalid, placements_name_empty, placements_name_short, placements_name_noise, placements_merged_team, placements_unsplit_team, placements_division_noise, cv_player_name_leading_dash, placements_unknown_with_keywords
9. results_not_extracted

**Semantic Validation (7 checks)**
10. check_event_name_quality
11. check_year_range
12. check_missing_required_fields
13. check_location_semantics (location_has_street_address, location_multiple_venues, location_parenthetical, location_tba)
14. check_date_semantics
15. check_host_club_semantics (host_club_numbered_prefix, host_club_too_long, host_club_contains_location)
16. check_country_names

**Field Leakage (1 check)**
17. check_field_leakage

**Placements Quality (5 checks)**
18. check_player_name_quality (player_has_score, player_has_slash, player_has_semicolon, player_duplicate_in_team, player_name_too_long, player_has_admin_text)
19. check_division_name_quality (cv_division_french, cv_division_spanish)
20. check_place_values (place_does_not_start_at_1)
21. check_place_sequences (place_large_gap)
22. check_string_hygiene (string_double_space)

**Cross-Validation (5 checks)**
23. check_expected_divisions (cv_worlds_missing_net, cv_net_event_no_net_divs, cv_freestyle_event_no_freestyle_divs, cv_worlds_missing_freestyle)
24. check_division_quality
25. check_team_splitting (cv_doubles_unsplit_team)
26. check_year_date_consistency
27. check_event_id_uniqueness, check_worlds_per_year, check_duplicates
28. check_host_club_location_consistency (host_club_multiple_locations)

**Global Slop Detection (7 checks)**
29. any_field_contains_url
30. any_field_contains_c0_controls
31. any_field_contains_c1_controls
32. any_field_contains_html_or_entities
33. any_field_contains_placeholder_or_instructional_text
34. any_field_has_whitespace_slop
35. any_field_has_bogus_character_sequences

**Targeted Slop Checks (6 checks)**
36. host_club_suspicious_prefix_or_markup
37. host_club_contains_url_or_contact
38. worlds_missing_expected_disciplines
39. worlds_results_suspiciously_small
40. results_raw_has_strong_signals_but_output_empty
41. placements_duplicate_rows

### Stage 3 Checks (3 total)

1. results_cell_duplicate_lines
2. results_cell_roundtrip_missing_any_placement
3. results_cell_near_excel_limit

Plus global slop detection on all Excel cells.

## Benefits of This Architecture

1. **Separation of Concerns**
   - QC logic isolated from pipeline logic
   - Main scripts focus on transformation
   - QC logic consolidated in one place

2. **Token Management**
   - Large QC check functions don't clutter main pipeline context
   - Can read/modify QC checks without loading entire pipeline
   - Easier to understand and maintain each component

3. **Consistency**
   - All stages use same QC orchestration pattern
   - Standardized output format (JSON summary + JSONL issues)
   - Unified baseline/delta reporting

4. **Extensibility**
   - Add new checks to qc_slop_detection.py or qc_master.py
   - Checks automatically included in pipeline runs
   - No need to modify multiple files

5. **Automatic Operation**
   - Zero changes to pipeline invocation
   - Still just: `python 01 && python 02 && python 03`
   - QC runs transparently, reports clearly

6. **Fallback Safety**
   - If qc_master import fails, 02_canonicalize uses embedded QC
   - Pipeline never breaks due to QC refactoring
   - Graceful degradation

## Adding New QC Checks

### Add a Global Field Scanner

Edit `qc_slop_detection.py`:

```python
def check_my_new_pattern(rec: dict, field_name: str, value: Any) -> list[QCIssue]:
    """Detect some new pattern in any field."""
    issues = []
    if not isinstance(value, str) or not value:
        return issues

    event_id = rec.get("event_id", "")

    if "BAD_PATTERN" in value:
        issues.append(QCIssue(
            check_id="my_new_check",
            severity="ERROR",
            event_id=str(event_id),
            field=field_name,
            message=f"Field '{field_name}' contains bad pattern",
            example_value=value[:100],
        ))

    return issues

# Add to run_slop_detection_checks_stage2():
for field in text_fields:
    value = rec.get(field)
    if value is None:
        continue
    all_issues.extend(check_my_new_pattern(rec, field, value))
```

### Add a Targeted Check

Edit `qc_slop_detection.py`:

```python
def check_specific_issue(rec: dict) -> list[QCIssue]:
    """Check for specific issue in specific field."""
    issues = []
    event_id = rec.get("event_id", "")
    field_value = rec.get("field_name", "")

    if some_condition(field_value):
        issues.append(QCIssue(
            check_id="specific_issue_check",
            severity="WARN",
            event_id=str(event_id),
            field="field_name",
            message="Specific issue detected",
            example_value=field_value[:100],
        ))

    return issues

# Add to run_slop_detection_checks_stage2():
all_issues.extend(check_specific_issue(rec))
```

### Add an Existing-Style Check

Edit `02_canonicalize_results.py`, add a new function:

```python
def check_my_validation(rec: dict) -> list[QCIssue]:
    """Check some field validation."""
    issues = []
    # ... validation logic ...
    return issues

# Add to run_qc():
for rec in records:
    all_issues.extend(check_my_validation(rec))
```

The check will automatically run on every pipeline execution.

## Troubleshooting

### "Could not import qc_master" warning

The pipeline falls back to embedded QC. This is safe but you'll miss the consolidated architecture benefits. Ensure:
- `qc_master.py` is in the same directory as `02_canonicalize_results.py`
- No syntax errors in `qc_master.py`
- Python can import the module

### QC taking too long

Stage 2 QC scans 50,000+ field instances. Typical runtime: ~3 seconds.

If slower:
- Check if you're running on a slow filesystem
- Verify Python is not in debug mode
- Consider profiling specific checks

### High error counts after adding new checks

This is expected! New checks detect issues that were previously undetected. This is good - you now have visibility into data quality issues.

- Review the delta report to see which checks found issues
- Triage by severity: fix ERRORs first, then WARNs
- Use baseline after fixes to track improvements

### Stage 3 QC not running

Check that:
- `qc_master.py` is present and importable
- `03_build_excel.py` has the qc_master import
- Stage 2 completed successfully (Stage 3 needs canonical events)

## Summary

The QC system is now:
- ✅ **Consolidated** - All checks in dedicated modules
- ✅ **Automatic** - Runs on every pipeline execution
- ✅ **Comprehensive** - 46 total checks across 3 stages
- ✅ **Token-efficient** - Separated concerns, cleaner context
- ✅ **Transparent** - Pipeline invocation unchanged
- ✅ **Extensible** - Easy to add new checks
- ✅ **Safe** - Fallback to embedded QC if needed

**Pipeline usage remains simple:**
```bash
python3 01_parse_mirror.py
python3 02_canonicalize_results.py
python3 03_build_excel.py
```

QC runs automatically and reports clearly.
