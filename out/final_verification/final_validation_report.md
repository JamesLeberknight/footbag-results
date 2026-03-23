# Final Dataset Verification Report

## Summary Statistics

| Metric | Count |
|--------|-------|
| Total source event-divisions evaluated | 3,814 |
| Quarantined (skipped) | 101 |
| METADATA_ONLY (skipped) | 4 |
| SOURCE_PARTIAL (flagged separately) | 174 |
| Exact division match | 3,722 |
| Fuzzy/drift match | 70 |
| Missing — BLOCKER (no match) | 22 |
| Missing — SOURCE_PARTIAL | 0 |
| Count mismatches | 381 |
| Max-place mismatches | 132 |

## BLOCKER Divisions (genuine gaps)

**22 BLOCKER(s) found:**

| Event ID | Year | Event Name | Division | Source Count | Source Max |
|----------|------|------------|----------|--------------|------------|
| 1061453080 | 2003 | Australian East Coast Regionals | Intermediate Shred 30 | 3 | 3 |
| 1079605499 | 2004 | 6th Annual IFPA European Footbag Championships | Womens Singles Freestyle [Very, Very Clo | 11 | 11 |
| 1079605499 | 2004 | 6th Annual IFPA European Footbag Championships | Shred30 | 6 | 4 |
| 1092073845 | 2004 | Czech Championships 2004 | Shred 30 | 3 | 3 |
| 1103297805 | 2005 | 2e Championnat de France de Footbag | Open Shred 30 | 3 | 3 |
| 1134914723 | 2006 | RNH Contest 2006 | Open Shred 30 | 6 | 6 |
| 1156283186 | 2006 | The Hackrifice Open 2006 | Intermediate Shred 30 | 1 | 1 |
| 1181021804 | 2007 | ShrEdmonton Freestyle Assembly 2007 | Inter Sick1 | 1 | 2 |
| 1216058526 | 2008 | Funtastik Summer Classic Footbag Tournament | Unknown | 1 | 23 |
| 1231432184 | 2009 | Todexon 10 | Open Singles Routines | 26 | 24 |
| 1231432184 | 2009 | Todexon 10 | Women's Singles Routines | 3 | 3 |
| 1235653935 | 2009 | 11th Annual IFPA European Footbag Championships -  | Unknown | 1 | 11 |
| 1295842442 | 2011 | US OPEN Footbag Net Championships 2011 | Unknown | 10 | 10 |
| 1321869570 | 2012 | Todexon 13 | Open Sick 3-Trick | 8 | 8 |
| 1435110091 | 2015 | 33rd Annual East Coast Footbag Championships | Unknown | 1 | 13 |
| 1473932659 | 2016 | Austrian Footbag Championships 2016 | Open Circle Contest | 4 | 4 |
| 859400929 | 1997 | Montreal International Footbag Championships | Unknown | 1 | 47 |
| 859787898 | 1997 | HEART OF FOOTBAG Freestyle Tournament | Shred Skills Contest Final Results | 1 | 1 |
| 892446131 | 1998 | Kansas Footbag Open | Unknown | 1 | 2 |
| 920579000 | 1999 | East Coast Footbag Championships | And Qdog Pro Footbag. | 1 | 1 |
| 947196813 | 2000 | New Zealand Footbag Championships | Unknown | 1 | 1 |
| 979816633 | 2001 | 1st Hungarian Footbag Cup | Doubles | 4 | 4 |

## Count Mismatches (source vs canonical)

**381 count mismatch(es):**

| Event ID | Year | Division | Src Count | Can Count | Δ | SOURCE_PARTIAL? |
|----------|------|----------|-----------|-----------|---|----------------|
| 1449259560 | 2016 | Open Doubles Net | 27 | 54 | -27 | Y |
| 915561090 | 1999 | Open Doubles Net | 27 | 54 | -27 | N |
| 1035277529 | 2003 | Intermediate Doubles Net | 25 | 50 | -25 | N |
| 1268445253 | 2010 | Open Singles Net | 24 | 1 | +23 | N |
| 1337617980 | 2012 | Open Singles Net | 26 | 4 | +22 | N |
| 1354752474 | 2013 | Open Singles Routines | 9 | 31 | -22 | N |
| 1035277529 | 2003 | Open Doubles Net | 20 | 40 | -20 | N |
| 1377192359 | 2013 | Open Singles Net | 38 | 19 | +19 | N |
| 1568961264 | 2019 | Open Singles Net | 20 | 1 | +19 | N |
| 1353223688 | 2012 | Open Singles Net | 28 | 11 | +17 | N |
| 1268445253 | 2010 | Open Doubles Net | 16 | 1 | +15 | N |
| 1195585401 | 2008 | Open Singles | 32 | 18 | +14 | N |
| 1134914723 | 2006 | Open Singles Net | 15 | 3 | +12 | N |
| 1378666423 | 2013 | Circle | 16 | 4 | +12 | N |
| 1449259560 | 2016 | Open Mixed Doubles Net | 12 | 24 | -12 | Y |
| 1706536250 | 2024 | Open Doubles Net | 12 | 24 | -12 | N |
| 915561090 | 1999 | Mixed Doubles Net | 12 | 24 | -12 | N |
| 974683678 | 2001 | Intermediate Shred | 26 | 14 | +12 | N |
| 1092073845 | 2004 | Sick 3 | 3 | 14 | -11 | N |
| 1378666423 | 2013 | Routines | 20 | 9 | +11 | N |
| 1405875596 | 2014 | Open Circle Contest | 14 | 4 | +10 | N |
| 1353223688 | 2012 | Open Doubles Net | 16 | 7 | +9 | N |
| 1354752474 | 2013 | Open Singles Net | 40 | 31 | +9 | N |
| 1161167528 | 2006 | Double Net | 8 | 16 | -8 | N |
| 1288096032 | 2010 | Circle Contest | 4 | 12 | -8 | N |
| 1449259560 | 2016 | Women's Singles Net | 13 | 21 | -8 | Y |
| 915561090 | 1999 | Open Doubles Distance One-Pass | 8 | 16 | -8 | N |
| 990905420 | 2001 | Open Doubles | 12 | 4 | +8 | N |
| 1035277529 | 2003 | Mixed Doubles Net | 7 | 14 | -7 | N |
| 1080138513 | 2004 | Sick 3 | 11 | 4 | +7 | N |

*... and 351 more (see division_count_mismatch.csv)*

## Max-Place Mismatches (source vs canonical)

**132 max-place mismatch(es):**

| Event ID | Year | Division | Src Max | Can Max | SOURCE_PARTIAL? |
|----------|------|----------|---------|---------|----------------|
| 1337617980 | 2012 | Open Singles Net | 26 | 4 | N |
| 1354752474 | 2013 | Open Singles Routines | 9 | 31 | N |
| 1268445253 | 2010 | Open Singles Net | 24 | 4 | N |
| 1137942486 | 2006 | Open Singles Net | 31 | 17 | N |
| 1092073845 | 2004 | Sick 3 | 3 | 15 | N |
| 1134914723 | 2006 | Open Singles Net | 15 | 3 | N |
| 1268445253 | 2010 | Open Doubles Net | 16 | 4 | N |
| 990905420 | 2001 | Open Doubles | 12 | 4 | N |
| 1353223688 | 2012 | Open Singles Routines | 6 | 13 | N |
| 979334083 | 2001 | Women's Singles Freestyle | 4 | 11 | N |
| 1157398732 | 2006 | Sick 3 | 8 | 2 | N |
| 1354752474 | 2013 | Open Shred:30 | 9 | 3 | N |
| 1568961264 | 2019 | Open Singles Net | 20 | 14 | N |
| 1218721346 | 2008 | Open Doubles Net | 8 | 3 | N |
| 1721817655 | 2024 | Open Singles Net | 11 | 6 | N |
| 990626988 | 2001 | Results Freestyle Man | 6 | 1 | N |
| 1028800969 | 2002 | Doubles Net Open | 8 | 4 | N |
| 1080138513 | 2004 | Sick 3 | 8 | 4 | N |
| 1137347261 | 2006 | Open Sick 3 | 5 | 1 | N |
| 1195585401 | 2008 | Open Singles | 32 | 28 | N |
| 1288096032 | 2010 | Circle Contest | 4 | 8 | N |
| 1354752474 | 2013 | Women's Singles Routines | 3 | 7 | N |
| 1377192359 | 2013 | Open Doubles Net | 15 | 19 | N |
| 1386623061 | 2014 | Open Singles Net | 7 | 3 | N |
| 1489248474 | 2017 | Footbag Net: Doubles | 4 | 8 | N |
| 1079444598 | 2004 | Intermediate Singles Net | 4 | 1 | N |
| 1354752474 | 2013 | Intermediate Singles Routines | 3 | 6 | N |
| 1357101984 | 2013 | Open Mixed Doubles Net | 11 | 8 | N |
| 1368559562 | 2013 | Open Doubles Net | 17 | 14 | N |
| 1386623061 | 2014 | Intermediate Singles Net | 5 | 2 | N |

*... and 102 more (see placement_max_mismatch.csv)*

## Conclusion

**SOURCE_COVERAGE: FAIL** — 22 genuine BLOCKER division(s) found. See BLOCKER section above.
