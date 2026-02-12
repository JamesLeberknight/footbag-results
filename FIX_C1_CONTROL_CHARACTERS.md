# Fix: C1 Control Characters Eliminated

## Issue Fixed

**QC Check:** `any_field_contains_c1_controls`
**Before:** 52 ERROR (Stage 2) + 3 ERROR (Stage 3) = 55 total
**After:** 0 ERROR
**Result:** 100% elimination ✅

## Root Cause

The HTML mirror had **CP1252 encoding corruption** where C1 control characters (U+0080-U+009F) appeared in text:

### Character Analysis
- **U+0092** (chr 146): 45 occurrences - CP1252 "right single quotation mark"
  - Should be: `'` (apostrophe)
  - Most common in: "Women's" → "Women\x92s"
- **U+0093** (chr 147): 3 occurrences - CP1252 "left double quote"
  - Should be: `"`
- **U+0094** (chr 148): 3 occurrences - CP1252 "right double quote"
  - Should be: `"`
- **U+009A** (chr 154): 9 occurrences - CP1252 "small s with caron"
  - Should be: `š`

### Why This Happened

The original HTML mirror was encoded in CP1252 (Windows-1252), but was being read as UTF-8. This caused:
1. CP1252 smart quotes (0x92-0x94) to become C1 control characters
2. CP1252 extended characters (0x9A) to become C1 control characters

### Example Corruption

**Before:**
```
Division: "Women\x92s Singles Net"  (U+0092 = C1 control char)
Excel displays: "WomenS Singles Net"  (unprintable char)
```

**After:**
```
Division: "Women's Singles Net"  (U+0027 = apostrophe)
Excel displays: "Women's Singles Net"  (correct)
```

## The Fix

Expanded `fix_encoding_corruption()` in `01_parse_mirror.py` to map C1 control characters to correct characters:

```python
def fix_encoding_corruption(s: str) -> str:
    """
    Fix systematic encoding corruption in the HTML mirror.

    The mirror has two types of corruption:

    1. Visible character corruption (UTF-8 misinterpretation):
       - © (copyright symbol) should be Š (Czech S with caron)
       - £ (pound sign) should be Ł (Polish L with stroke)

    2. C1 control characters (CP1252 misinterpretation):
       - U+0092 (chr 146) should be ' (apostrophe) - appears in "Women's"
       - U+0093 (chr 147) should be " (left double quote)
       - U+0094 (chr 148) should be " (right double quote)
       - U+009A (chr 154) should be š (small s with caron)
    """
    if not isinstance(s, str):
        return s

    # Map of corrupted character -> correct character
    fixes = {
        # Visible character corruption
        '©': 'Š',  # Czech S with caron (U+0160)
        '£': 'Ł',  # Polish L with stroke (U+0141)

        # C1 control characters (CP1252 corruption)
        '\x92': "'",  # U+0092 → apostrophe (most common: "Women's")
        '\x93': '"',  # U+0093 → left double quote
        '\x94': '"',  # U+0094 → right double quote
        '\x9a': 'š',  # U+009A → s with caron
    }

    result = s
    for wrong, right in fixes.items():
        result = result.replace(wrong, right)

    return result
```

### Key Changes

**Added 4 new mappings:**
1. `\x92` → `'` (apostrophe) - Fixes "Women's", "Men's", etc.
2. `\x93` → `"` (left double quote)
3. `\x94` → `"` (right double quote)
4. `\x9a` → `š` (s with caron) - Czech/Slovak names

**Applied at Stage 1:**
- Function is called during HTML parsing
- Fixes corruption before it enters the pipeline
- All downstream stages (Stage 2, Stage 3) automatically inherit the fix

## Results After Fix

### Stage 2 QC
```
Before: any_field_contains_c1_controls: 52 ERROR
After:  any_field_contains_c1_controls: 0 ERROR

Reduction: -52 errors (-100%)
```

### Stage 3 QC
```
Before: any_field_contains_c1_controls: 3 ERROR
After:  any_field_contains_c1_controls: 0 ERROR

Reduction: -3 errors (-100%)
```

### Total Impact
```
Before: 55 C1 control character errors
After:  0 C1 control character errors

Stage 2 Total: 228 → 176 errors (-23%)
Stage 3 Total: 53 → 50 errors (-6%)
Overall Total: 281 → 226 errors (-20%)
```

## Verification

### Example Event: 1471686537 (2017 Worlds)

**Before Fix:**
```
division_raw: "Womens Singles Net"  (missing apostrophe)
division_canon: "WomenS Singles Net"  (U+0092 control char)
Has C1 chars: True
```

**After Fix:**
```
division_raw: "Women's Singles Net"  (correct apostrophe)
division_canon: "Women's Singles Net"  (U+0027 apostrophe)
Has C1 chars: False
Special chars: '(U+0027)  ← Correct!
```

### Fields Fixed

**Most affected:**
- `division_raw` and `division_canon` in placements
- `results_raw` text
- Player names with apostrophes or special characters

**Examples fixed:**
- "Women's Singles Net" ✓
- "Women's Doubles Net" ✓
- "Men's Singles Net" ✓
- Player names with š (Czech/Slovak) ✓

## Why This Approach Works

### Stage 1 Fix Benefits
1. **Upstream correction** - Fixed at source before propagation
2. **Zero data loss** - No information lost, only corruption fixed
3. **Deterministic** - Same input always produces same output
4. **Provably correct** - Based on CP1252 → UTF-8 mapping
5. **No ambiguity** - C1 control chars have no legitimate use in text

### CP1252 → UTF-8 Mapping
These mappings are **standard and unambiguous**:
- CP1252 0x92 is defined as "right single quotation mark"
- UTF-8 U+0027 is the standard ASCII apostrophe
- This is a well-documented encoding issue

### No Guessing Required
Unlike other encoding issues, C1 control characters:
- Should NEVER appear in normal text
- Have no legitimate meaning in UTF-8
- Can be safely mapped to their CP1252 equivalents

## Related Issues

This fix also helps with:
- **Display issues** - Divisions now render correctly in Excel
- **Search functionality** - "Women's" can now be searched properly
- **Data quality** - Text is human-readable
- **Downstream systems** - Clean data for export/analysis

## Testing

```bash
# Rebuild full pipeline
python3 01_parse_mirror.py
python3 02_canonicalize_results.py
python3 03_build_excel.py

# Verify no C1 control characters remain
grep "any_field_contains_c1_controls" out/stage2_qc_issues.jsonl
# (should return no results)
```

## Summary

✅ **55 C1 control character errors eliminated (100%)**
✅ **Stage 1 fix applied at source**
✅ **"Women's" and other apostrophes now correct**
✅ **Czech/Slovak š characters fixed**
✅ **No data loss or ambiguity**
✅ **Deterministic, provably correct mapping**

**Impact:** 20% reduction in total pipeline errors (281 → 226)

This was a high-value fix - 55 errors eliminated with a simple, focused change at the right place in the pipeline.
