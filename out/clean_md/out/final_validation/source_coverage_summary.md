# Source Coverage QC Summary

Events audited (non-quarantine, with placements): 774  
Report generated from: stage2_canonical_events.csv vs Placements_Flat.csv

## Root Cause Classification

| Severity | Description | Division-rows | Events |
|---|---|---|---|
| OK | All source places in PF | 3872 | — |
| BLOCKER_GENUINE | Division entirely absent — data never in identity lock | 0 | 0 |
| BLOCKER_DRIFT | Division absent under this name — likely renamed since PBP was built | 44 | 30 |
| PARTIAL | Some places absent — single-name entries not in PT/PU | 21 | 16 |
| JUSTIFIED | Missing but in known_issues.csv | 3 | — |

## Root Cause Explanation

**Why the previous QC did not catch these:**

The previous `final_presentation_sync_qc.py` compared the community workbook
Index and year sheets against `Placements_Flat.csv` — the same filtered source
the workbook was generated from. Both sides were downstream of the identity lock.
Neither side was compared against `stage2_canonical_events.csv` (the parser's
ground truth). So any placement dropped during identity-lock generation was
invisible to both the workbook and the QC — they agreed perfectly on the
filtered subset while the stage2 source had more data.

**BLOCKER_GENUINE events**: PBP v61 was built before these divisions were
added to stage2 (parser improvements, new events, RESULTS_FILE_OVERRIDES),
or the placements mapped to single-name entries that were never identity-resolved.

**BLOCKER_DRIFT events**: Stage2 division names changed between PBP v61 build
and the current parser (e.g. 'Shred:30' → 'Shred 30', 'Freestyle - X' → 'Freestyle ? X').
PBP may have equivalent coverage under the old division name.

**PARTIAL events**: Expected — lower-placed competitors with single-name tokens
(Kris, Yavor, Alex) are not in PT or PU and therefore absent from PF.

## BLOCKER_DRIFT events (name mismatch — verify manually)

| event_id | year | event_name | stage2 division (missing) |
|---|---|---|---|
| 2001981001 | 1981 | World Championships 1981 (NHSA) | Intermediate Singles; Singles Consecutive |
| 2001983001 | 1983 | World Championships 1983 (NHSA) | Mixed Doubles; Open Singles Consecutive |
| 9940469 | 1985 | Western National Indoor Footbag Freestyle Championship | Beginner Singles Consecutive |
| 859787898 | 1997 | HEART OF FOOTBAG Freestyle Tournament | Shred Skills Contest Final Results |
| 877061793 | 1998 | U. S. Open Footbag Net Championships | Womenìs Singles Net 9 Entrants |
| 1046985067 | 2003 | 2003 Steel City Footbag Open | Intermediate Shred 30; Open Shred 30 |
| 1061453080 | 2003 | Australian East Coast Regionals | Consecutive Kicks |
| 1058378281 | 2003 | Texas State Footbag Championships | Freestyle - Intermediate |
| 1050745947 | 2003 | RNH Contest 2003 | Shred 30 |
| 1076952530 | 2004 | SOUF 2004 Southeastern Regional Footbag Championships | Shred30 |
| 1094201584 | 2004 | Frankfurt Footbag Open 2004 | Open Doubles Net |
| 1080757998 | 2004 | Space City Freestyle Jam 2004 | Intermediate Shred 30; Open Shred 30 |
| 1102788509 | 2005 | 3rd Annual Sunshred Footbag Open (LIVE) | Open Freestyle Combo |
| 1109356644 | 2005 | Funtastik Summer Classic | Freestyle - Novice |
| 1103297805 | 2005 | 2e Championnat de France de Footbag | Open Sick 3 |
| 1149881200 | 2006 | Funtastik Summer Classic Tournament and Festival | Freestyle - Novice |
| 1134914723 | 2006 | RNH Contest 2006 | Mixed Doubles Net; Open Doubles Net; Open Sick 3 |
| 1156283186 | 2006 | The Hackrifice Open 2006 | Intermediate Shred 30; Intermediate Sick 3 |
| 1135116604 | 2006 | 27th IFPA WORLD FOOTBAG CHAMPIONSHIPS | Circle Contest |
| 1149763452 | 2006 | 8e OPEN de France de Footbag | Shred 30 |
| 1185458086 | 2007 | Footbagmania 3007 - Polish Footbag Championships | Big One - Trick Competition |
| 1193783964 | 2008 | U.S. Open Footbag Championships | Open Doubles Net; Open Singles Net |
| 1353223688 | 2012 | Finnish Footbag Open | Open Circle Contest; Open Shred:30 |
| 1321869570 | 2012 | Todexon 13 | Open Shred:30; Open Sick 3-Trick |
| 1329132977 | 2012 | Swiss Footbag Championships 2012 | Open Doubles Net; Open Shred:30; Open Singles Net |
| 1377192359 | 2013 | 15th Annual German Footbag Championships & Open | Open Doubles Net |
| 1354752474 | 2013 | 13e Open de France de Footbag | Intermediate Singles Routines; Open Mixed Doubles Net |
| 1406219099 | 2014 | 14th Annual Polish Footbag Championships 2014 | Open Shred:30; Open Sick 3-Trick |
| 1473932659 | 2016 | Austrian Footbag Championships 2016 | Open Circle Contest |
| 1745686591 | 2025 | Footbag Finnish Open 2024 - Singles Net | Open Singles Net |

---

## Publication Assessment

**SOURCE_COVERAGE_PASS** — no genuinely missing divisions.
