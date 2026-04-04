#!/usr/bin/env python3
"""
normalize_canonical.py
======================
FINAL CANONICAL NORMALIZATION + VALIDATION

Sections:
  1. Persons normalization
  2. Discipline normalization (critical)
  3. Event name consistency
  4. Global string normalization
  5. Validation checks
  6. Output report + apply fixes

Usage:
  python tools/normalize_canonical.py           # dry-run (report only)
  python tools/normalize_canonical.py --apply   # apply fixes in-place
"""

import csv
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

APPLY = "--apply" in sys.argv

CA = Path("out/canonical_all")
FILES = {
    "events":       CA / "events.csv",
    "disciplines":  CA / "event_disciplines.csv",
    "results":      CA / "event_results.csv",
    "participants": CA / "event_result_participants.csv",
    "persons":      CA / "persons.csv",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def load(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def norm_key(s):
    """Whitespace-stripped lowercase — used for grouping variants."""
    return re.sub(r"\s+", " ", s.strip()).lower()

def has_encoding_artifact(s):
    """
    Flag strings containing confirmed CP1250→Latin-1 corruption artifacts.
    These characters are near-impossible in legitimate person/event names:
      U+00B9  ¹  (superscript 1)   — maps from š
      U+00B8  ¸  (cedilla alone)   — maps from ü in corrupt context
      U+00B3  ³  (superscript 3)   — maps from ł
      U+00BF  ¿  (inv. question)   — maps from ż  (OK in Spanish punctuation
                                       but never mid-name)
      U+00B6  ¶  (pilcrow)         — maps from ś
      U+FFFD  replacement char
    Valid accented letters (é è à ù ó í ú á ñ ç ü ö ä ő ű ě š č ž etc.)
    are NOT flagged here — they are legitimate UTF-8.
    """
    CORRUPT = {"\u00b9", "\u00b8", "\u00b3", "\u00b6", "\ufffd"}
    return bool(set(s) & CORRUPT)

def trim_string(s):
    return re.sub(r"\s+", " ", s.strip())

# ── Load all CSVs ─────────────────────────────────────────────────────────────

events       = load(FILES["events"])
disciplines  = load(FILES["disciplines"])
results      = load(FILES["results"])
participants = load(FILES["participants"])
persons      = load(FILES["persons"])

ev_fields    = list(csv.DictReader(open(FILES["events"])).fieldnames or [])
disc_fields  = list(csv.DictReader(open(FILES["disciplines"])).fieldnames or [])
res_fields   = list(csv.DictReader(open(FILES["results"])).fieldnames or [])
part_fields  = list(csv.DictReader(open(FILES["participants"])).fieldnames or [])
per_fields   = list(csv.DictReader(open(FILES["persons"])).fieldnames or [])

issues   = []   # (severity, section, message)
fixes    = []   # (description)

def issue(severity, section, msg):
    issues.append((severity, section, msg))

def fix(desc):
    fixes.append(desc)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — PERSONS NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── SECTION 1: PERSONS ─────────────────────────────────────────────")

# 1a. Trailing spaces / hidden characters
trailing_space = [r for r in persons if r["person_canon"] != r["person_canon"].strip()
                  or re.search(r"\s{2,}", r["person_canon"])]
if trailing_space:
    issue("ERROR", "S1", f"{len(trailing_space)} persons with whitespace issues")
    for r in trailing_space:
        print(f"  WHITESPACE: {repr(r['person_canon'])}")
    if APPLY:
        for r in persons:
            orig = r["person_canon"]
            r["person_canon"] = trim_string(orig)
        fix("S1: trimmed whitespace in person_canon")
else:
    print("  [OK] No whitespace issues in person_canon")

# 1b. Encoding artifacts
enc_persons = [r for r in persons if has_encoding_artifact(r["person_canon"])]
if enc_persons:
    issue("ERROR", "S1", f"{len(enc_persons)} persons with encoding artifacts")
    for r in enc_persons:
        print(f"  ENCODING: {repr(r['person_canon'])}")
else:
    print("  [OK] No encoding artifacts in person_canon")

# 1c. Duplicate person_canon
canon_counts = Counter(r["person_canon"] for r in persons)
dupe_canons = {k: v for k, v in canon_counts.items() if v > 1}
if dupe_canons:
    issue("ERROR", "S1", f"{len(dupe_canons)} duplicate person_canon values")
    for name, cnt in sorted(dupe_canons.items()):
        rows = [r for r in persons if r["person_canon"] == name]
        ids  = [r["person_id"] for r in rows]
        print(f"  DUPE person_canon: {repr(name)} ({cnt}x) ids={ids}")
else:
    print("  [OK] No duplicate person_canon values")

# 1d. Sort check (for release — persons.csv should be alpha-sorted)
canon_vals = [r["person_canon"] for r in persons]
sorted_vals = sorted(canon_vals, key=str.lower)
if canon_vals != sorted_vals:
    issue("WARN", "S1", "persons.csv not sorted by person_canon")
    if APPLY:
        persons.sort(key=lambda r: r["person_canon"].lower())
        fix("S1: sorted persons by person_canon")
else:
    print("  [OK] persons.csv is sorted")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — DISCIPLINE NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── SECTION 2: DISCIPLINES ─────────────────────────────────────────")

# Collect all discipline strings from all three tables
disc_sources = {}  # norm_key → Counter of raw forms
for field, table in [("discipline", disciplines),
                     ("discipline", results),
                     ("discipline", participants)]:
    for row in table:
        val = row.get(field, "")
        if val:
            k = norm_key(val)
            disc_sources.setdefault(k, Counter())
            disc_sources[k][val] += 1

# Build normalization map: only where variants differ from canonical
disc_norm_map = {}   # raw_form → canonical_form
variant_groups = []  # for reporting

for nk, counter in sorted(disc_sources.items()):
    forms = list(counter.keys())

    # Always apply whitespace trim regardless of variant count
    for f in forms:
        trimmed = trim_string(f)
        if f != trimmed:
            disc_norm_map[f] = trimmed

    if len(forms) == 1:
        continue

    # Multiple forms with same norm_key — check if they differ ONLY by whitespace.
    # If they differ by case as well (e.g. "Golf" vs "golf"), treat as a cross-era
    # convention difference — flag as INFO only, do NOT auto-remap.
    trimmed_forms = {trim_string(f) for f in forms}
    if len(trimmed_forms) == 1:
        # All forms are identical after trim — map non-canonical variants
        canonical = max(forms, key=lambda f: (counter[f], f))
        has_variant = False
        for f in forms:
            if f != canonical:
                disc_norm_map[f] = canonical
                has_variant = True
        if has_variant:
            variant_groups.append((canonical, counter))
    else:
        # Forms differ by case — cross-era convention, report as INFO only
        issue("INFO", "S2",
              f"Cross-era case variant for discipline: {sorted(forms)} "
              f"(most common={counter.most_common(1)[0][0]!r})")
        print(f"  INFO cross-era case: {sorted(forms)}")

if variant_groups:
    issue("WARN", "S2", f"{len(variant_groups)} discipline variant groups found")
    print(f"\n  DISCIPLINE NORMALIZATION TABLE ({len(disc_norm_map)} mappings):")
    print(f"  {'OLD':50s}  →  NEW")
    print(f"  {'-'*50}     {'-'*40}")
    for old, new in sorted(disc_norm_map.items()):
        print(f"  {repr(old):50s}  →  {repr(new)}")
else:
    print("  [OK] No discipline variants requiring normalization")

# Apply discipline normalization to all three tables
if APPLY and disc_norm_map:
    n_disc = n_res = n_part = 0
    for row in disciplines:
        if row.get("discipline") in disc_norm_map:
            row["discipline"] = disc_norm_map[row["discipline"]]
            n_disc += 1
        if row.get("discipline_name") in disc_norm_map:
            row["discipline_name"] = disc_norm_map[row["discipline_name"]]
    for row in results:
        if row.get("discipline") in disc_norm_map:
            row["discipline"] = disc_norm_map[row["discipline"]]
            n_res += 1
    for row in participants:
        if row.get("discipline") in disc_norm_map:
            row["discipline"] = disc_norm_map[row["discipline"]]
            n_part += 1
    fix(f"S2: normalized {n_disc} discipline rows, {n_res} result rows, {n_part} participant rows")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — EVENT NAME CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── SECTION 3: EVENT NAMES ─────────────────────────────────────────")

ev_name_issues = []
for row in events:
    name = row.get("event_name", "")
    trimmed = trim_string(name)
    if name != trimmed:
        ev_name_issues.append((row["event_id"], name, trimmed))
        if APPLY:
            row["event_name"] = trimmed

if ev_name_issues:
    issue("WARN", "S3", f"{len(ev_name_issues)} event names with whitespace issues")
    for eid, old, new in ev_name_issues:
        print(f"  TRIM: {eid}: {repr(old)} → {repr(new)}")
    if APPLY:
        fix(f"S3: trimmed {len(ev_name_issues)} event names")
else:
    print("  [OK] No event name whitespace issues")

# Check for near-duplicate event names (same norm_key, different event_id)
ev_name_groups = defaultdict(list)
for row in events:
    ev_name_groups[norm_key(row.get("event_name", ""))].append(row)
near_dupes = {k: v for k, v in ev_name_groups.items() if len(v) > 1}
if near_dupes:
    issue("INFO", "S3", f"{len(near_dupes)} near-duplicate event name groups (same name, different event_id — may be intentional multi-year events)")
    print(f"  INFO: {len(near_dupes)} event name groups with >1 event_id")
    # Only show ones where the names actually differ (variant spellings, not same name different years)
    spelling_variants = {k: v for k, v in near_dupes.items()
                         if len(set(r["event_name"] for r in v)) > 1}
    if spelling_variants:
        # Check if variants are purely case differences (ALL CAPS vs Title Case)
        # — these are cross-era naming conventions, not bugs
        true_variants = {}
        case_only = {}
        for nk, rows in spelling_variants.items():
            names = set(r["event_name"] for r in rows)
            if all(a.upper() == b.upper() for a in names for b in names):
                case_only[nk] = rows
            else:
                true_variants[nk] = rows
        if case_only:
            issue("INFO", "S3", f"{len(case_only)} event name groups differ only by ALL CAPS vs Title Case (cross-era)")
            for nk, rows in sorted(case_only.items()):
                names = set(r["event_name"] for r in rows)
                ids   = [r["event_id"] for r in rows]
                print(f"  INFO case-only: {names} | ids={ids}")
        if true_variants:
            issue("WARN", "S3", f"{len(true_variants)} event name true spelling variant groups")
            for nk, rows in sorted(true_variants.items()):
                names = set(r["event_name"] for r in rows)
                ids   = [r["event_id"] for r in rows]
                print(f"  VARIANT: {names} | ids={ids}")
    else:
        print("  [OK] All near-duplicate event names are identical strings (same name, different event_id)")
else:
    print("  [OK] No near-duplicate event names")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — GLOBAL STRING NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── SECTION 4: GLOBAL STRING NORMALIZATION ─────────────────────────")

SKIP_FIELDS = {"event_id", "person_id", "result_row_id", "team_person_key",
               "person_id_raw", "source_url", "notes", "score_text",
               "player_raw", "team_raw", "player_name_raw", "division_canonical"}

def check_and_fix_row(row, table_name, row_idx):
    changed = False
    for field, val in row.items():
        if field in SKIP_FIELDS or not val:
            continue
        trimmed = trim_string(val)
        if trimmed != val:
            if APPLY:
                row[field] = trimmed
                changed = True
    return changed

global_ws_fixes = defaultdict(int)
for table_name, table in [("events", events), ("disciplines", disciplines),
                            ("results", results), ("participants", participants),
                            ("persons", persons)]:
    n = 0
    for i, row in enumerate(table):
        if check_and_fix_row(row, table_name, i):
            n += 1
    if n:
        global_ws_fixes[table_name] = n
        fix(f"S4: {table_name}: {n} rows had whitespace normalized")

if global_ws_fixes:
    issue("WARN", "S4", f"Global whitespace normalization applied to {sum(global_ws_fixes.values())} rows")
    for t, n in global_ws_fixes.items():
        print(f"  {t}: {n} rows fixed")
else:
    print("  [OK] No global whitespace issues")

# Encoding artifact scan across all tables
enc_hits = defaultdict(list)
for table_name, table, key_field in [
    ("events",       events,       "event_id"),
    ("disciplines",  disciplines,  "event_id"),
    ("results",      results,      "result_row_id"),
    ("participants", participants, "event_id"),
    ("persons",      persons,      "person_id"),
]:
    for row in table:
        for field, val in row.items():
            if field in SKIP_FIELDS or not val:
                continue
            if has_encoding_artifact(val):
                enc_hits[table_name].append((row.get(key_field, "?"), field, val))

if enc_hits:
    total = sum(len(v) for v in enc_hits.values())
    issue("ERROR", "S4", f"{total} encoding artifacts found across tables")
    for table_name, hits in enc_hits.items():
        print(f"\n  ENCODING ARTIFACTS in {table_name}:")
        for key, field, val in hits[:20]:
            print(f"    [{key}] {field}: {repr(val)}")
        if len(hits) > 20:
            print(f"    ... and {len(hits)-20} more")
else:
    print("  [OK] No encoding artifacts found across tables")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — VALIDATION CHECKS
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── SECTION 5: VALIDATION ──────────────────────────────────────────")

# 5a. No duplicate (event_id, discipline) pairs in event_disciplines
disc_pairs = Counter((r["event_id"], r["discipline"]) for r in disciplines)
dupe_discs = {k: v for k, v in disc_pairs.items() if v > 1}
if dupe_discs:
    issue("ERROR", "S5", f"{len(dupe_discs)} duplicate (event_id, discipline) pairs in event_disciplines")
    for (eid, disc), cnt in sorted(dupe_discs.items()):
        print(f"  DUPE DISC: ({eid}, {repr(disc)}) x{cnt}")
else:
    print("  [OK] No duplicate (event_id, discipline) pairs")

# 5b. Orphan discipline rows — disciplines referencing unknown event_id
known_eids = {r["event_id"] for r in events}
orphan_discs = [r for r in disciplines if r["event_id"] not in known_eids]
if orphan_discs:
    issue("ERROR", "S5", f"{len(orphan_discs)} orphan discipline rows (event_id not in events)")
    for r in orphan_discs[:10]:
        print(f"  ORPHAN DISC: {r['event_id']} / {r['discipline']}")
else:
    print("  [OK] No orphan discipline rows")

# 5c. Discipline names in results not in disciplines for that event
disc_vocab = defaultdict(set)  # event_id → set of known disciplines
for r in disciplines:
    disc_vocab[r["event_id"]].add(r["discipline"])

res_disc_mismatch = []
for r in results:
    eid  = r["event_id"]
    disc = r["discipline"]
    if eid in disc_vocab and disc not in disc_vocab[eid]:
        res_disc_mismatch.append((eid, disc))
if res_disc_mismatch:
    issue("ERROR", "S5", f"{len(res_disc_mismatch)} result rows with discipline not in event_disciplines")
    for eid, disc in sorted(set(res_disc_mismatch))[:20]:
        print(f"  RESULT/DISC MISMATCH: {eid} / {repr(disc)}")
else:
    print("  [OK] All result disciplines present in event_disciplines")

# 5d. Discipline names in participants not in disciplines for that event
part_disc_mismatch = []
for r in participants:
    eid  = r["event_id"]
    disc = r["discipline"]
    if eid in disc_vocab and disc not in disc_vocab[eid]:
        part_disc_mismatch.append((eid, disc))
if part_disc_mismatch:
    issue("ERROR", "S5", f"{len(part_disc_mismatch)} participant rows with discipline not in event_disciplines")
    for eid, disc in sorted(set(part_disc_mismatch))[:20]:
        print(f"  PART/DISC MISMATCH: {eid} / {repr(disc)}")
else:
    print("  [OK] All participant disciplines present in event_disciplines")

# 5e. Duplicate persons with same canonical name (cross-check with person_id)
person_id_map = defaultdict(list)
for r in persons:
    person_id_map[norm_key(r["person_canon"])].append(r)
norm_dupes = {k: v for k, v in person_id_map.items() if len(v) > 1}
if norm_dupes:
    issue("WARN", "S5", f"{len(norm_dupes)} near-duplicate person names (same normalized form)")
    for nk, rows in sorted(norm_dupes.items()):
        print(f"  NEAR-DUPE PERSON: norm={repr(nk)}")
        for r in rows:
            print(f"    {r['person_id']} | {repr(r['person_canon'])}")
else:
    print("  [OK] No near-duplicate person names")

# 5f. Person IDs referenced in participants but not in persons
known_pids = {r["person_id"] for r in persons if r["person_id"]}
orphan_pids = set()
for r in participants:
    pid = r.get("person_id", "")
    if pid and pid not in known_pids:
        orphan_pids.add(pid)
if orphan_pids:
    issue("ERROR", "S5", f"{len(orphan_pids)} person_ids in participants not in persons.csv")
    for pid in sorted(orphan_pids)[:10]:
        print(f"  ORPHAN PID: {pid}")
else:
    print("  [OK] All participant person_ids found in persons.csv")

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — OUTPUT REPORT
# ═══════════════════════════════════════════════════════════════════════════════
print("\n══════════════════════════════════════════════════════════════════")
print("SECTION 6 — VALIDATION REPORT")
print("══════════════════════════════════════════════════════════════════")

errors   = [i for i in issues if i[0] == "ERROR"]
warnings = [i for i in issues if i[0] == "WARN"]
infos    = [i for i in issues if i[0] == "INFO"]

for sev, sec, msg in errors:
    print(f"  [ERROR]  {sec}: {msg}")
for sev, sec, msg in warnings:
    print(f"  [WARN]   {sec}: {msg}")
for sev, sec, msg in infos:
    print(f"  [INFO]   {sec}: {msg}")

if errors:
    status = "FAIL"
elif warnings:
    status = "PASS (with warnings)"
else:
    status = "PASS"

print(f"\n  QC_STATUS = {status}")
print(f"  Errors:   {len(errors)}")
print(f"  Warnings: {len(warnings)}")
print(f"  Infos:    {len(infos)}")

if APPLY and fixes:
    print("\n── FIXES APPLIED ──────────────────────────────────────────────────")
    for f in fixes:
        print(f"  + {f}")
    save(FILES["events"],       events,       ev_fields)
    save(FILES["disciplines"],  disciplines,  disc_fields)
    save(FILES["results"],      results,      res_fields)
    save(FILES["participants"], participants, part_fields)
    save(FILES["persons"],      persons,      per_fields)
    print(f"\n  All {len(FILES)} canonical CSVs written.")
elif not APPLY:
    if fixes or errors or warnings:
        print("\n  (dry-run — pass --apply to write fixes)")
