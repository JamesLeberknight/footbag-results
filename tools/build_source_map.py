#!/usr/bin/env python3
"""
tools/build_source_map.py

Builds a deterministic source-linkage map connecting canonical events to:
  1. PDF archive source  (926-page footbag.org results archive)
  2. Magazine scan JPEGs (Footbag World magazine scans via scan index)
  3. Magazine ingestion CSV (structured placements derived from JPG scans)

Source priority:
  - Year < 1997 : magazine_scan > magazine_ingestion > pdf_archive
  - Year >= 1997 : pdf_archive only

Outputs (written to out/pdf_compare/):
  event_to_source_map.json
  unresolved_source_matches.csv
  source_map_summary.md

Usage:
  python tools/build_source_map.py [--canonical-dir out/canonical]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]

CANONICAL_EVENTS    = ROOT / "out/canonical/events.csv"
CANONICAL_PARTS     = ROOT / "out/canonical/event_result_participants.csv"
PDF_CANDIDATES      = ROOT / "out/pdf_compare/pdf_event_candidates.csv"
PDF_COMPARISON      = ROOT / "out/pdf_compare/pdf_vs_current_event_comparison.csv"
MAGAZINE_INDEX      = ROOT / "inputs/magazine_scan_index.csv"
MAGAZINE_INGESTION  = ROOT / "inputs/magazine_ingestion_comprehensive_v1.csv"
SCAN_DIR            = ROOT / "out/scans"
CROP_DIR            = ROOT / "out/manual_crops_raw"
PREP_DIR            = ROOT / "out/preprocessed"          # PREP_*.png from full scans
CROP_PREP_DIR       = ROOT / "out/manual_crops_prepped"  # PREP_*.png from manual crops

OUT_DIR             = ROOT / "out/pdf_compare"
OUT_MAP             = OUT_DIR / "event_to_source_map.json"
OUT_UNRESOLVED      = OUT_DIR / "unresolved_source_matches.csv"
OUT_SUMMARY         = OUT_DIR / "source_map_summary.md"

# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------
_NOISE = re.compile(
    r"\b(the|a|an|de|la|le|les|of|and|for|in|at|to|by|"
    r"open|cup|jam|tournament|championship|championships|"
    r"annual|footbag|international|national|regional|regionals|"
    r"1st|2nd|3rd|4th|5th|6th|7th|8th|9th|10th|"
    r"i+v?|vi+|xi+|xx+|xv|iv|ix)\b"
)
_PUNCT = re.compile(r"[^\w\s]")
_SPACE = re.compile(r"\s+")

DIVISION_XLAT = {
    "dobles": "doubles", "individual": "singles", "simple": "singles",
    "double": "doubles", "double mixte": "mixed doubles",
    "singles net": "open singles net", "doubles net": "open doubles net",
}


def norm(text: str) -> str:
    """Lowercase, strip accents, remove punctuation and noise words."""
    t = text.lower()
    t = "".join(
        c for c in unicodedata.normalize("NFD", t)
        if unicodedata.category(c) != "Mn"
    )
    t = _PUNCT.sub(" ", t)
    t = _NOISE.sub(" ", t)
    t = _SPACE.sub(" ", t).strip()
    return t


def norm_division(div: str) -> str:
    d = norm(div)
    return DIVISION_XLAT.get(d, d)


def name_sim(a: str, b: str) -> float:
    """Jaccard similarity on word tokens."""
    sa = set(norm(a).split())
    sb = set(norm(b).split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def extract_pdf_players(results_block_raw: str) -> list[str]:
    """Parse 'N Name | N Name | ...' format from PDF results_block_raw."""
    if not results_block_raw:
        return []
    parts = re.split(r"\|", results_block_raw)
    names = []
    for p in parts:
        p = p.strip()
        cleaned = re.sub(r"^\d+[\.\):]?\s*", "", p).strip()
        if cleaned and len(cleaned) > 2:
            names.append(cleaned)
    return names[:10]  # top 10 max


def player_overlap(raw_names: list[str], canon_names: set[str]) -> list[str]:
    """Return unique raw names that fuzzy-match a canonical display name."""
    matched = []
    seen: set[str] = set()
    for rn in raw_names:
        rn_key = norm(rn)
        if rn_key in seen:
            continue
        for cn in canon_names:
            cn_norm = norm(cn)
            rn_toks = set(rn_key.split())
            cn_toks = set(cn_norm.split())
            shared = rn_toks & cn_toks
            if shared and max(len(t) for t in shared) >= 3:
                matched.append(rn)
                seen.add(rn_key)
                break
    return matched


# ---------------------------------------------------------------------------
# JPEG resolution helper
# ---------------------------------------------------------------------------
def resolve_jpg(source_jpg: str) -> tuple[str | None, str]:
    """
    Given a source_jpg filename from the scan index, find it on disk.
    Returns (relative_path_from_root, status) where status is one of
    'resolved', 'inferred', 'unresolved'.
    """
    if not source_jpg:
        return None, "unresolved"

    candidates = [
        SCAN_DIR / source_jpg,
        CROP_DIR / source_jpg,
        SCAN_DIR / ("CLEAN_" + source_jpg),
        CROP_DIR / ("CLEAN_" + source_jpg),
    ]
    # Also try without "CLEAN_" prefix
    if source_jpg.startswith("CLEAN_"):
        bare = source_jpg[6:]
        candidates += [SCAN_DIR / bare, CROP_DIR / bare]

    for c in candidates:
        if c.exists():
            return str(c.relative_to(ROOT)), "resolved"

    # Fall back: strip page suffix, look for page 1
    base = re.sub(r"\s+page\s+\d+", "", source_jpg, flags=re.IGNORECASE)
    for d in [SCAN_DIR, CROP_DIR]:
        for f in d.iterdir():
            if norm(f.stem) == norm(Path(base).stem):
                return str(f.relative_to(ROOT)), "inferred"

    return source_jpg, "inferred"  # filename known but file not found at expected path


# ---------------------------------------------------------------------------
# Preprocessed image resolution
# ---------------------------------------------------------------------------
def resolve_preprocessed(jpg_path: str | None) -> tuple[str | None, str]:
    """
    Given a raw jpg_path (relative to ROOT, as stored in source map),
    find the corresponding preprocessed PNG in out/preprocessed/ or
    out/manual_crops_prepped/.

    Naming convention (deterministic, no guessing):
      out/scans/CLEAN_X.jpeg        → out/preprocessed/PREP_X.png
      out/manual_crops_raw/X.jpeg   → out/manual_crops_prepped/PREP_X.png

    Returns (relative_path_from_root, status) where status is one of:
      'resolved'   – file found on disk
      'unresolved' – no preprocessed version found
    """
    if not jpg_path:
        return None, "unresolved"

    p = Path(jpg_path)
    filename = p.name
    parent   = str(p.parent)

    if "scans" in parent:
        # Strip leading CLEAN_ prefix, swap extension to .png, add PREP_
        bare = filename[6:] if filename.startswith("CLEAN_") else filename
        stem = bare.rsplit(".", 1)[0]
        prep = PREP_DIR / f"PREP_{stem}.png"
        if prep.exists():
            return str(prep.relative_to(ROOT)), "resolved"
    elif "manual_crops_raw" in parent:
        stem = filename.rsplit(".", 1)[0]
        prep = CROP_PREP_DIR / f"PREP_{stem}.png"
        if prep.exists():
            return str(prep.relative_to(ROOT)), "resolved"

    return None, "unresolved"


# ---------------------------------------------------------------------------
# SOURCE-REF → JPEG filename
# ---------------------------------------------------------------------------
def source_ref_to_jpg(source_ref: str) -> str | None:
    """Convert ingestion source_ref code to expected JPEG filename."""
    m = re.match(r"FBW-V(\d+)N(\d+)$", source_ref)
    if m:
        v, n = m.groups()
        return f"CLEAN_Footbag World Vol. {v} No. {n}.jpeg"
    m = re.match(r"IFAB-RB-P(\d+)$", source_ref)
    if m:
        return f"CLEAN_IFAB Rulebook Worlds History Page {m.group(1)}.jpeg"
    return None


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------
def load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--canonical-dir", default=str(ROOT / "out/canonical"))
    args = ap.parse_args()

    canon_dir = Path(args.canonical_dir)

    # ------------------------------------------------------------------
    # Load canonical data
    # ------------------------------------------------------------------
    canon_events = load_csv(canon_dir / "events.csv")
    canon_parts  = load_csv(canon_dir / "event_result_participants.csv")

    # Build participant lookup: event_key -> set of display_name
    event_participants: dict[str, set[str]] = defaultdict(set)
    for row in canon_parts:
        dn = row.get("display_name", "").strip()
        if dn and dn != "__NON_PERSON__":
            event_participants[row["event_key"]].add(dn)

    # Build legacy_event_id -> event_key lookup (one-to-one where possible)
    legacy_to_canon: dict[str, str] = {}
    for row in canon_events:
        lid = row.get("legacy_event_id", "").strip()
        if lid:
            legacy_to_canon[lid] = row["event_key"]

    # ------------------------------------------------------------------
    # SOURCE A: PDF archive comparison (post-1997 primary)
    # ------------------------------------------------------------------
    pdf_candidates = {
        row["pdf_event_id"]: row
        for row in load_csv(PDF_CANDIDATES)
    }
    # Build: canonical event_key -> pdf comparison row
    pdf_match: dict[str, dict] = {}
    for row in load_csv(PDF_COMPARISON):
        ekey = row.get("matched_event_key", "").strip()
        mtype = row.get("match_type", "").strip()
        if ekey and mtype in ("MATCHED_STRONG", "MATCHED_POSSIBLE"):
            # Keep highest-scoring match per event_key
            if ekey not in pdf_match or float(row.get("match_score", 0)) > float(pdf_match[ekey].get("match_score", 0)):
                pdf_match[ekey] = row

    # ------------------------------------------------------------------
    # SOURCE B: Magazine scan index (pre-1997 primary, direct ID match)
    # ------------------------------------------------------------------
    # Build: event_id -> scan row  (multiple rows with same event_id possible)
    scan_by_event_id: dict[str, dict] = {}
    for row in load_csv(MAGAZINE_INDEX):
        eid = row.get("event_id", "").strip()
        if eid:
            scan_by_event_id[eid] = row  # last row wins (they share same source_jpg)

    # ------------------------------------------------------------------
    # SOURCE C: Magazine ingestion (name+year matching, supplementary)
    # ------------------------------------------------------------------
    # Group rows into event blocks by (year, raw_event_name)
    ingestion_blocks: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in load_csv(MAGAZINE_INGESTION):
        yr   = row.get("raw_year", "").strip()
        name = row.get("raw_event_name", "").strip()
        if yr and name:
            ingestion_blocks[(yr, name)].append(row)

    # Pre-compute normalised keys for ingestion blocks
    ingestion_norm: list[tuple[tuple[str, str], str, list[dict]]] = [
        (key, norm(key[1]), rows)
        for key, rows in ingestion_blocks.items()
    ]

    # ------------------------------------------------------------------
    # Build the source map
    # ------------------------------------------------------------------
    results     = []
    unresolved  = []

    stat_pdf             = 0
    stat_scan_direct     = 0
    stat_ingestion_name  = 0
    stat_unresolved      = 0
    stat_jpg_resolved    = 0
    stat_jpg_inferred    = 0

    for event in canon_events:
        ekey  = event["event_key"]
        year  = event.get("year", "").strip()
        ename = event.get("event_name", "").strip()
        lid   = event.get("legacy_event_id", "").strip()
        y_int = int(year) if year.isdigit() else 9999

        entry: dict = {
            "event_id":             ekey,
            "canonical_event_name": ename,
            "canonical_year":       year,
            "raw_source_match":     None,
            "scan_jpg":             None,
            "jpg_path":             None,
            "jpg_link_status":      "unresolved",
            "preprocessed_path":    None,
            "status":               "not_found",
        }

        # ---- Try SOURCE B first for pre-1997 -------------------------
        scan_match_found = False
        if y_int < 1997 and lid and lid in scan_by_event_id:
            scan_row  = scan_by_event_id[lid]
            src_jpg   = scan_row.get("source_jpg", "").strip()
            jpg_path, jpg_status = resolve_jpg(src_jpg)

            # Collect ingestion rows for this event to get player/division signals
            matching_ingestion = []
            for (iy, iname), irows in ingestion_blocks.items():
                if iy == year and name_sim(iname, ename) >= 0.4:
                    matching_ingestion.extend(irows)

            ingestion_players   = [r.get("raw_player_names", "") for r in matching_ingestion]
            ingestion_divisions = list({r.get("raw_discipline", "") for r in matching_ingestion if r.get("raw_discipline")})
            canon_names         = event_participants.get(ekey, set())

            flat_players = []
            for p in ingestion_players:
                flat_players += [x.strip() for x in re.split(r"[/&,]", p) if x.strip()]

            overlap = player_overlap(flat_players, canon_names)

            entry["raw_source_match"] = {
                "source_type":   "magazine_scan",
                "csv_file":      str(MAGAZINE_INDEX.relative_to(ROOT)),
                "raw_event_key": lid,
                "confidence":    0.99,
                "match_basis":   "legacy_event_id",
                "matched_signals": {
                    "year_match":          True,
                    "event_name_overlap":  [ename],
                    "player_overlap":      overlap[:10],
                    "division_overlap":    ingestion_divisions[:10],
                },
                "supplementary_csv": str(MAGAZINE_INGESTION.relative_to(ROOT))
                    if matching_ingestion else None,
            }
            entry["scan_jpg"]           = src_jpg or None
            entry["jpg_path"]           = jpg_path
            entry["jpg_link_status"]    = jpg_status
            prep_path, _ = resolve_preprocessed(jpg_path)
            entry["preprocessed_path"] = prep_path
            entry["status"]             = "mapped"

            if jpg_status == "resolved":
                stat_jpg_resolved += 1
            else:
                stat_jpg_inferred += 1

            stat_scan_direct    += 1
            scan_match_found     = True

        # ---- Try SOURCE A: PDF comparison ----------------------------
        if not scan_match_found and ekey in pdf_match:
            prow    = pdf_match[ekey]
            pid     = prow.get("pdf_event_id", "")
            cand    = pdf_candidates.get(pid, {})
            page    = cand.get("source_page", prow.get("source_page", ""))
            pdf_fn  = cand.get("source_pdf", "926-pages-results-footbag.org-2021-02-14.pdf")
            mtype   = prow.get("match_type", "MATCHED_STRONG")
            mscore  = float(prow.get("match_score", 1.0))

            raw_players  = extract_pdf_players(cand.get("results_block_raw", ""))
            canon_names  = event_participants.get(ekey, set())
            overlap      = player_overlap(raw_players, canon_names)

            # Confidence: strong match = 0.95, possible = 0.70
            confidence = 0.95 if mtype == "MATCHED_STRONG" else 0.70
            # Blend with match_score
            confidence = round((confidence + mscore) / 2, 3)

            entry["raw_source_match"] = {
                "source_type":   "pdf_archive",
                "csv_file":      str(PDF_CANDIDATES.relative_to(ROOT)),
                "raw_event_key": pid,
                "confidence":    confidence,
                "match_basis":   mtype,
                "pdf_source":    pdf_fn,
                "pdf_page":      int(page) if str(page).isdigit() else page,
                "matched_signals": {
                    "year_match":         prow.get("matched_year", "") == year,
                    "event_name_overlap": [prow.get("event_name_raw", "")],
                    "player_overlap":     overlap[:10],
                    "division_overlap":   [],  # pdf divisions_raw often empty
                },
            }
            entry["scan_jpg"]           = None
            entry["jpg_path"]           = None   # no individual page JPG extracted
            entry["jpg_link_status"]    = "inferred"  # page ref in PDF
            entry["preprocessed_path"]  = None   # PDF source — no scan to preprocess
            entry["status"]             = "mapped"

            stat_jpg_inferred += 1
            stat_pdf          += 1

        # ---- For pre-1997 without a scan-index hit: try ingestion ----
        if entry["status"] == "not_found" and y_int < 1997:
            best_score  = 0.0
            best_block: tuple | None = None

            ename_norm = norm(ename)
            for (iy, iname), inorm_key, irows in ingestion_norm:
                if iy != year:
                    continue
                sim = name_sim(ename_norm, inorm_key)
                if sim > best_score:
                    best_score = sim
                    best_block = ((iy, iname), irows)

            if best_score >= 0.45 and best_block:
                (iy, iname), irows = best_block

                ingestion_players   = [r.get("raw_player_names", "") for r in irows]
                ingestion_divisions = list({r.get("raw_discipline", "") for r in irows if r.get("raw_discipline")})
                canon_names         = event_participants.get(ekey, set())

                flat_players = []
                for p in ingestion_players:
                    flat_players += [x.strip() for x in re.split(r"[/&,]", p) if x.strip()]

                overlap = player_overlap(flat_players, canon_names)

                # Source refs from ingestion rows -> infer JPG
                source_refs = list({r.get("source_ref", "") for r in irows if r.get("source_ref") and r["source_ref"] != "FBW-ARCHIVE"})
                jpg_candidates = [source_ref_to_jpg(sr) for sr in source_refs if source_ref_to_jpg(sr)]

                jpg_path_found, jpg_status_found, jpg_fn = None, "unresolved", None
                for jfn in jpg_candidates:
                    jpath, jstat = resolve_jpg(jfn)
                    if jstat in ("resolved", "inferred"):
                        jpg_fn, jpg_path_found, jpg_status_found = jfn, jpath, jstat
                        break

                if jpg_status_found == "resolved":
                    stat_jpg_resolved += 1
                elif jpg_status_found == "inferred":
                    stat_jpg_inferred += 1

                confidence = round(min(0.85, best_score * 1.1), 3)
                entry["raw_source_match"] = {
                    "source_type":   "magazine_ingestion",
                    "csv_file":      str(MAGAZINE_INGESTION.relative_to(ROOT)),
                    "raw_event_key": f"{iy}|{iname}",
                    "confidence":    confidence,
                    "match_basis":   f"year+name_sim={best_score:.2f}",
                    "matched_signals": {
                        "year_match":         True,
                        "event_name_overlap": [iname],
                        "player_overlap":     overlap[:10],
                        "division_overlap":   ingestion_divisions[:10],
                    },
                    "source_refs": source_refs,
                }
                entry["scan_jpg"]           = jpg_fn
                entry["jpg_path"]           = jpg_path_found
                entry["jpg_link_status"]    = jpg_status_found
                prep_path, _ = resolve_preprocessed(jpg_path_found)
                entry["preprocessed_path"] = prep_path
                entry["status"]          = "mapped" if best_score >= 0.65 else "low_confidence"

                stat_ingestion_name += 1

        # ---- Unresolved ----------------------------------------------
        if entry["status"] == "not_found":
            stat_unresolved += 1
            unresolved.append({
                "event_id":   ekey,
                "year":       year,
                "event_name": ename,
                "legacy_id":  lid,
                "reason":     "no_source_match",
            })

        results.append(entry)

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUT_MAP, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(results)} entries → {OUT_MAP}")

    unresolved_fields = ["event_id", "year", "event_name", "legacy_id", "reason"]
    with open(OUT_UNRESOLVED, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=unresolved_fields)
        w.writeheader()
        w.writerows(unresolved)
    print(f"Wrote {len(unresolved)} unresolved → {OUT_UNRESOLVED}")

    total   = len(results)
    mapped  = sum(1 for r in results if r["status"] == "mapped")
    low_c   = sum(1 for r in results if r["status"] == "low_confidence")
    not_f   = sum(1 for r in results if r["status"] == "not_found")

    summary = f"""# Source Map Summary

Generated by `tools/build_source_map.py`

## Coverage

| Metric | Count |
|--------|-------|
| Canonical events | {total} |
| Mapped (high confidence) | {mapped} |
| Mapped (low confidence) | {low_c} |
| Not found / unresolved | {not_f} |

## Source Breakdown

| Source type | Events |
|-------------|--------|
| PDF archive (footbag.org 926p) | {stat_pdf} |
| Magazine scan index (direct ID) | {stat_scan_direct} |
| Magazine ingestion (name+year) | {stat_ingestion_name} |
| Unresolved | {stat_unresolved} |

## JPG Linkage

| Status | Count |
|--------|-------|
| Resolved (file confirmed on disk) | {stat_jpg_resolved} |
| Inferred (page/filename known) | {stat_jpg_inferred} |
| Unresolved | {total - stat_jpg_resolved - stat_jpg_inferred} |

## Source Files

- PDF candidates: `out/pdf_compare/pdf_event_candidates.csv`
- PDF comparison: `out/pdf_compare/pdf_vs_current_event_comparison.csv`
- Magazine scan index: `inputs/magazine_scan_index.csv`
- Magazine ingestion: `inputs/magazine_ingestion_comprehensive_v1.csv`

## Outputs

- `out/pdf_compare/event_to_source_map.json` — full mapping
- `out/pdf_compare/unresolved_source_matches.csv` — events with no source match
- `out/pdf_compare/source_map_summary.md` — this file
"""

    with open(OUT_SUMMARY, "w", encoding="utf-8") as f:
        f.write(summary)
    print(f"Wrote summary → {OUT_SUMMARY}")

    print()
    print(f"  Mapped:           {mapped:4d} / {total}")
    print(f"  Low-confidence:   {low_c:4d} / {total}")
    print(f"  Not found:        {not_f:4d} / {total}")
    print(f"  JPG resolved:     {stat_jpg_resolved}")
    print(f"  JPG inferred:     {stat_jpg_inferred}")
    print(f"    via PDF archive:       {stat_pdf}")
    print(f"    via scan index:        {stat_scan_direct}")
    print(f"    via ingestion name:    {stat_ingestion_name}")


if __name__ == "__main__":
    main()
