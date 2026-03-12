#!/usr/bin/env python3
"""
54_source_coverage_qc.py

Strict source-line preservation QC.

Compares stage2_canonical_events.csv (ground truth — every parsed placement)
against Placements_Flat.csv (identity-locked output) to find:

  BLOCKER_GENUINE — entire division absent from PF with no similar PBP division
                    (data is genuinely not covered by the identity lock)
  BLOCKER_DRIFT   — entire division absent from PF, but a PBP division with
                    overlapping keywords exists (canonicalization name change)
  PARTIAL         — some places missing from PF (expected for single-name entries)
  JUSTIFIED       — missing but event is in known_issues.csv (documented gap)
  OK              — all stage2 places accounted for in PF

Quarantined events are excluded entirely.

Outputs:
  out/final_validation/source_coverage_blocker_report.csv
  out/final_validation/source_coverage_summary.md
"""

import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(10_000_000)

REPO = Path(__file__).resolve().parent.parent
OUT  = REPO / "out"
VAL  = OUT / "final_validation"
VAL.mkdir(parents=True, exist_ok=True)

STAGE2       = OUT / "stage2_canonical_events.csv"
PF           = OUT / "Placements_Flat.csv"
QUARANTINE   = REPO / "inputs" / "review_quarantine_events.csv"
KNOWN_ISSUES = REPO / "overrides" / "known_issues.csv"
REPORT_CSV   = VAL / "source_coverage_blocker_report.csv"
SUMMARY_MD   = VAL / "source_coverage_summary.md"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_quarantine() -> set:
    if not QUARANTINE.exists():
        return set()
    with open(QUARANTINE) as f:
        return {r["event_id"].strip() for r in csv.DictReader(f) if r.get("event_id")}


def load_known_issues() -> set:
    if not KNOWN_ISSUES.exists():
        return set()
    with open(KNOWN_ISSUES) as f:
        return {r["event_id"].strip() for r in csv.DictReader(f) if r.get("event_id")}


def load_stage2() -> dict:
    """event_id → {event_name, year, divs: {division_canon: set(places)}}"""
    events = {}
    with open(STAGE2, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            eid = row["event_id"].strip()
            try:
                placements = json.loads(row.get("placements_json") or "[]")
            except (json.JSONDecodeError, TypeError):
                placements = []

            divs: dict[str, set] = defaultdict(set)
            for p in placements:
                dc = (p.get("division_canon") or "").strip()
                pl = p.get("place")
                if dc and pl is not None:
                    try:
                        divs[dc].add(int(float(pl)))
                    except (ValueError, TypeError):
                        pass

            events[eid] = {
                "event_name": (row.get("event_name") or "").strip(),
                "year":       (row.get("year") or "").strip(),
                "divs":       dict(divs),
            }
    return events


def load_pf_places() -> dict:
    """event_id → {division_canon: set(places)}"""
    result: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    with open(PF, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            eid = row["event_id"].strip()
            dc  = (row.get("division_canon") or "").strip()
            pl  = row.get("place")
            if eid and dc and pl:
                try:
                    result[eid][dc].add(int(float(pl)))
                except (ValueError, TypeError):
                    pass
    return {eid: dict(divs) for eid, divs in result.items()}


_RE_COMPACT = re.compile(r"([A-Za-z]+)(\d+)")
_RE_PUNCT   = re.compile(r"[^a-z0-9 ]")


def _div_keywords(div: str) -> set:
    """Return set of lowercase words from a division name, stripping punctuation.
    Also splits compact tokens like 'Shred30' → {'shred', '30'}."""
    s = _RE_COMPACT.sub(r"\1 \2", div.lower())  # Shred30 → shred 30
    return set(_RE_PUNCT.sub(" ", s).split())


def _is_artifact(div: str) -> bool:
    """True for clearly unparseable division names that should not count as blockers."""
    d = div.strip().lower()
    return d in ("unknown", "doubles", "") or (
        div.endswith(".") and len(div) > 20   # sentence fragment
    )


def _is_drift(s2_div: str, pf_div_names: set) -> bool:
    """Return True if any PF division shares ≥2 keywords with the stage2 division."""
    s2_kw = _div_keywords(s2_div)
    for pd in pf_div_names:
        pd_kw = _div_keywords(pd)
        if len(s2_kw & pd_kw) >= 2:
            return True
    return False


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    quarantine   = load_quarantine()
    known_issues = load_known_issues()
    s2_data      = load_stage2()
    pf           = load_pf_places()

    rows   = []
    counts = {"BLOCKER_GENUINE": 0, "BLOCKER_DRIFT": 0,
              "PARTIAL": 0, "JUSTIFIED": 0, "OK": 0}
    genuine_events: set[str] = set()
    drift_events:   set[str] = set()
    partial_events: set[str] = set()

    for eid, ev in sorted(s2_data.items(), key=lambda x: (x[1]["year"], x[0])):
        if eid in quarantine:
            continue
        if not ev["divs"]:
            continue  # metadata-only

        pf_divs     = pf.get(eid, {})
        pf_div_keys = set(pf_divs.keys())
        is_ki       = eid in known_issues

        for div, s2_places in sorted(ev["divs"].items()):
            # Skip artifact division names (parsing failures, not real data gaps)
            if _is_artifact(div):
                continue

            pf_places      = pf_divs.get(div, set())
            missing_places = sorted(s2_places - pf_places)

            if not missing_places:
                counts["OK"] += 1
                continue

            s2_count      = len(s2_places)
            pf_count      = len(pf_places)
            missing_count = len(missing_places)

            if is_ki:
                severity = "JUSTIFIED"
                root_cause = "known_issue"
            elif pf_count == 0:
                # Entire division absent from PF — check if it's name drift
                if pf_div_keys and _is_drift(div, pf_div_keys):
                    severity   = "BLOCKER_DRIFT"
                    root_cause = "division_name_changed_since_pbp_was_built"
                    drift_events.add(eid)
                else:
                    severity   = "BLOCKER_GENUINE"
                    root_cause = "division_absent_from_identity_lock"
                    genuine_events.add(eid)
            else:
                severity   = "PARTIAL"
                root_cause = "some_places_not_in_identity_lock"
                partial_events.add(eid)

            counts[severity] += 1
            rows.append({
                "severity":       severity,
                "event_id":       eid,
                "year":           ev["year"],
                "event_name":     ev["event_name"],
                "division_canon": div,
                "s2_places":      s2_count,
                "pf_places":      pf_count,
                "missing_places": ",".join(str(p) for p in missing_places),
                "root_cause":     root_cause,
            })

    # Sort: BLOCKER_GENUINE first
    _sev_order = {"BLOCKER_GENUINE": 0, "BLOCKER_DRIFT": 1,
                  "PARTIAL": 2, "JUSTIFIED": 3}
    rows.sort(key=lambda r: (_sev_order.get(r["severity"], 9), r["year"], r["event_id"]))

    # Write CSV
    fields = ["severity","event_id","year","event_name","division_canon",
              "s2_places","pf_places","missing_places","root_cause"]
    with open(REPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    # Write summary
    total_events = sum(
        1 for eid, ev in s2_data.items()
        if eid not in quarantine and ev["divs"]
    )
    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write("# Source Coverage QC Summary\n\n")
        f.write(f"Events audited (non-quarantine, with placements): {total_events}  \n")
        f.write(f"Report generated from: stage2_canonical_events.csv vs Placements_Flat.csv\n\n")
        f.write("## Root Cause Classification\n\n")
        f.write("| Severity | Description | Division-rows | Events |\n")
        f.write("|---|---|---|---|\n")
        f.write(f"| OK | All source places in PF | {counts['OK']} | — |\n")
        f.write(f"| BLOCKER_GENUINE | Division entirely absent — data never in identity lock | {counts['BLOCKER_GENUINE']} | {len(genuine_events)} |\n")
        f.write(f"| BLOCKER_DRIFT | Division absent under this name — likely renamed since PBP was built | {counts['BLOCKER_DRIFT']} | {len(drift_events)} |\n")
        f.write(f"| PARTIAL | Some places absent — single-name entries not in PT/PU | {counts['PARTIAL']} | {len(partial_events)} |\n")
        f.write(f"| JUSTIFIED | Missing but in known_issues.csv | {counts['JUSTIFIED']} | — |\n\n")

        f.write("## Root Cause Explanation\n\n")
        f.write("**Why the previous QC did not catch these:**\n\n")
        f.write("The previous `final_presentation_sync_qc.py` compared the community workbook\n")
        f.write("Index and year sheets against `Placements_Flat.csv` — the same filtered source\n")
        f.write("the workbook was generated from. Both sides were downstream of the identity lock.\n")
        f.write("Neither side was compared against `stage2_canonical_events.csv` (the parser's\n")
        f.write("ground truth). So any placement dropped during identity-lock generation was\n")
        f.write("invisible to both the workbook and the QC — they agreed perfectly on the\n")
        f.write("filtered subset while the stage2 source had more data.\n\n")
        f.write("**BLOCKER_GENUINE events**: PBP v61 was built before these divisions were\n")
        f.write("added to stage2 (parser improvements, new events, RESULTS_FILE_OVERRIDES),\n")
        f.write("or the placements mapped to single-name entries that were never identity-resolved.\n\n")
        f.write("**BLOCKER_DRIFT events**: Stage2 division names changed between PBP v61 build\n")
        f.write("and the current parser (e.g. 'Shred:30' → 'Shred 30', 'Freestyle - X' → 'Freestyle ? X').\n")
        f.write("PBP may have equivalent coverage under the old division name.\n\n")
        f.write("**PARTIAL events**: Expected — lower-placed competitors with single-name tokens\n")
        f.write("(Kris, Yavor, Alex) are not in PT or PU and therefore absent from PF.\n\n")

        if genuine_events:
            f.write("## BLOCKER_GENUINE events (publication blockers)\n\n")
            f.write("| event_id | year | event_name | missing divisions |\n")
            f.write("|---|---|---|---|\n")
            by_event: dict[str, list] = defaultdict(list)
            for r in rows:
                if r["severity"] == "BLOCKER_GENUINE":
                    by_event[r["event_id"]].append(r["division_canon"])
            for eid in sorted(genuine_events, key=lambda e: s2_data[e]["year"]):
                ev   = s2_data[eid]
                divs = "; ".join(by_event[eid])
                f.write(f"| {eid} | {ev['year']} | {ev['event_name']} | {divs} |\n")
            f.write("\n")

        if drift_events:
            f.write("## BLOCKER_DRIFT events (name mismatch — verify manually)\n\n")
            f.write("| event_id | year | event_name | stage2 division (missing) |\n")
            f.write("|---|---|---|---|\n")
            by_event2: dict[str, list] = defaultdict(list)
            for r in rows:
                if r["severity"] == "BLOCKER_DRIFT":
                    by_event2[r["event_id"]].append(r["division_canon"])
            for eid in sorted(drift_events, key=lambda e: s2_data[e]["year"]):
                ev   = s2_data[eid]
                divs = "; ".join(by_event2[eid])
                f.write(f"| {eid} | {ev['year']} | {ev['event_name']} | {divs} |\n")
            f.write("\n")

        f.write("---\n\n")
        f.write("## Publication Assessment\n\n")
        if counts["BLOCKER_GENUINE"] == 0:
            f.write("**SOURCE_COVERAGE_PASS** — no genuinely missing divisions.\n")
        else:
            f.write(f"**SOURCE_COVERAGE_FAIL** — {len(genuine_events)} events have divisions entirely\n")
            f.write("absent from the identity lock. These divisions are invisible in the community\n")
            f.write("workbook. This is a publication blocker unless:\n")
            f.write("- The missing divisions are documented as known limitations, OR\n")
            f.write("- PBP is updated to cover them (requires a new identity lock version).\n")

    # Console output
    print(f"  Report: {REPORT_CSV}")
    print(f"  Summary: {SUMMARY_MD}")
    print()
    print("Division-level results:")
    print(f"  OK:                {counts['OK']}")
    print(f"  BLOCKER_GENUINE:   {counts['BLOCKER_GENUINE']}  ({len(genuine_events)} events)")
    print(f"  BLOCKER_DRIFT:     {counts['BLOCKER_DRIFT']}  ({len(drift_events)} events)")
    print(f"  PARTIAL:           {counts['PARTIAL']}  ({len(partial_events)} events)")
    print(f"  JUSTIFIED:         {counts['JUSTIFIED']}")
    print()
    if counts["BLOCKER_GENUINE"] == 0:
        print("SOURCE_COVERAGE_PASS")
    else:
        print(f"SOURCE_COVERAGE_FAIL — {len(genuine_events)} events with genuinely missing divisions")
        sys.exit(1)


if __name__ == "__main__":
    main()
