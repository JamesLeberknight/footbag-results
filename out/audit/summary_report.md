# Deep Mirror Audit — Summary Report

Generated: 2026-03-09 21:12 UTC

## 1. Overall Archive Completeness

| Metric | Value |
|--------|-------|
| Total mirror placements | 21,475 |
| Total sheet placements (stage2) | 27,945 |
| Overall completeness ratio | 130.1% |
| Events ≥ 90% complete | 655 |
| Events < 90% — unexplained (actionable) | 0 |
| Events < 90% — explained (false pos / known) | 52 |
| Events without mirror HTML | 6 |

## 2. Top 20 Worst Events by Data Loss

| Year | Event ID | Event Name | Mirror | Sheet | Ratio | Failure Mode |
|------|----------|------------|--------|-------|-------|--------------|

## 3. Top 20 Worst Divisions by Truncation

| Year | Event | Sport | Division | Mirror | Sheet | Ratio | Failure Mode |
|------|-------|-------|----------|--------|-------|-------|--------------|
| 2019 | 1547415984 | – |  | 298 | 0 | 0.0% | division_header_not_detected |
| 2025 | 1741024635 | – |  | 211 | 0 | 0.0% | division_header_not_detected |
| 2024 | 1706036811 | – |  | 168 | 0 | 0.0% | division_header_not_detected |
| 2009 | 1235149093 | – |  | 135 | 0 | 0.0% | division_header_not_detected |
| 1998 | 876356874 | – | Competitor Breakdown | 112 | 0 | 0.0% | sport_header_not_detected |
| 2017 | 1487797845 | – |  | 108 | 0 | 0.0% | division_header_not_detected |
| 2011 | 1301675662 | – |  | 90 | 0 | 0.0% | division_header_not_detected |
| 2023 | 1694170899 | – |  | 88 | 0 | 0.0% | division_header_not_detected |
| 2011 | 1297909685 | – |  | 74 | 0 | 0.0% | division_header_not_detected |
| 2011 | 1311699287 | – |  | 72 | 0 | 0.0% | division_header_not_detected |
| 2017 | 1499025778 | – |  | 71 | 0 | 0.0% | division_header_not_detected |
| 1999 | 910551956 | – | OPEN RESULTS | 68 | 0 | 0.0% | division_header_not_detected |
| 2008 | 1195585401 | – |  | 67 | 0 | 0.0% | division_header_not_detected |
| 2019 | 1566532030 | – |  | 66 | 0 | 0.0% | division_header_not_detected |
| 2007 | 1182542776 | – |  | 65 | 0 | 0.0% | division_header_not_detected |
| 2001 | 972427576 | – |  | 58 | 0 | 0.0% | division_header_not_detected |
| 2006 | 1134914723 | – |  | 58 | 0 | 0.0% | division_header_not_detected |
| 2010 | 1270631935 | – | Finals | 58 | 0 | 0.0% | division_header_not_detected |
| 1999 | 919703711 | – |  | 57 | 0 | 0.0% | division_header_not_detected |
| 2001 | 988434364 | – |  | 57 | 0 | 0.0% | division_header_not_detected |

## 4. Parser Failure Mode Counts

| Failure Mode | Count |
|-------------|-------|
| `division_header_not_detected` | 533 |
| `partial_data_loss` | 38 |
| `large_data_loss` | 34 |
| `seeding_plus_final_double_count` | 25 |
| `sport_header_not_detected` | 17 |
| `placement_regex_too_strict` | 10 |
| `unicode_corruption` | 1 |

## 5. Place-Gap Issue Types

| Issue Type | Count |
|-----------|-------|
| `missing_middle` | 479 |
| `truncated_tail` | 148 |
| `extra_in_sheet` | 71 |
| `tie_lost` | 17 |

## 6. Explained Events (False Positives / Known Causes)

These 52 events appear below the 90% threshold but are
not genuine stage2 failures. They are excluded from `events_with_major_loss.csv`
and written separately to `events_with_known_cause.csv`.

| Year | Event ID | Event Name | Mirror | Sheet | Ratio | Known Cause |
|------|----------|------------|--------|-------|-------|-------------|
| 1997 | 857852604 | Southern California Footbag Championship | 8 | 6 | 75.0% | source_corrupt |
| 2000 | 959353403 | 2000 Southeast Idaho Footbag Championshi | 20 | 17 | 85.0% | seeding_double_count |
| 2001 | 979089216 | First Annual Eugene Freestyle Freekout!! | 19 | 17 | 89.5% | no_results_posted |
| 2003 | 1063109533 | 2003 Philly Area Oktoberfest Freestyle J | 1 | 0 | 0.0% | no_results_posted |
| 2003 | 1070400528 | West Coast Xmas Shred | 1 | 0 | 0.0% | no_results_posted |
| 2003 | 1057773472 | 23rd Annual Moonin' & Noonin' Beaver Ope | 5 | 2 | 40.0% | no_results_posted |
| 2003 | 1036298726 | Colorado Shred Symposium 4 | 55 | 41 | 74.6% | seeding_double_count |
| 2003 | 1032472601 | The 3rd Annual Chilly Philly Freestyle J | 15 | 13 | 86.7% | no_results_posted |
| 2004 | 1079024287 | Shercle Session #1 | 8 | 6 | 75.0% | no_results_posted |
| 2004 | 1081766954 | Russian Open Footbag Series/ Stage 2 | 14 | 11 | 78.6% | no_results_posted |
| 2004 | 1092073845 | Czech Championships 2004 | 53 | 44 | 83.0% | seeding_double_count |
| 2004 | 1093955766 | Russian Open Footbag Series/ Stage 5, Br | 7 | 6 | 85.7% | no_results_posted |
| 2006 | 1163624128 | Moonin' & Noonin' Beaver Open Post Turke | 2 | 1 | 50.0% | no_results_posted |
| 2006 | 1148241486 | 2006 Steel City Shred Off | 21 | 15 | 71.4% | seeding_double_count |
| 2006 | 1131659634 | 2006 Green Cup Presented by Chaos | 25 | 21 | 84.0% | seeding_double_count |
| 2007 | 1181021804 | ShrEdmonton Freestyle Assembly 2007 | 29 | 15 | 51.7% | seeding_double_count |
| 2007 | 1172931308 | Montreal Spring Jam 2 | 20 | 16 | 80.0% | seeding_double_count |
| 2008 | 1200325415 | Greater Rochester Area Shred Symposium 2 | 23 | 15 | 65.2% | seeding_double_count |
| 2008 | 1203604560 | CommALaMaison Contest 2008 | 21 | 18 | 85.7% | seeding_double_count |
| 2009 | 1235149093 | RNH Contest 2009 | 135 | 97 | 71.9% | seeding_double_count |
| 2009 | 1250478677 | Montreal End-of-Summer Jam 1 | 9 | 8 | 88.9% | no_results_posted |
| 2010 | 1268338685 | II Spanish OPEN | 1 | 0 | 0.0% | no_results_posted |
| 2010 | 1270631935 | RNH Contest 2010 | 91 | 51 | 56.0% | seeding_double_count |
| 2011 | 1320232300 | RNH Contest 2011 | 50 | 24 | 48.0% | seeding_double_count |
| 2011 | 1301675662 | 2da Copa Venezuela | 90 | 45 | 50.0% | seeding_double_count |
| 2011 | 1297909685 | 2da Copa Ciencias | 74 | 37 | 50.0% | seeding_double_count |
| 2011 | 1311699287 | 3ra Copa X-PRO | 72 | 36 | 50.0% | seeding_double_count |
| 2013 | 1369141018 | Burgas Summer Footbag Jam V | 13 | 11 | 84.6% | seeding_double_count |
| 2013 | 1378666423 | Danish Footbag Open | 62 | 54 | 87.1% | seeding_double_count |
| 2014 | 1389730147 | Todexon 15 | 39 | 33 | 84.6% | seeding_double_count |
| 2016 | 1466942562 | 18th Annual German Footbag Open | 68 | 48 | 70.6% | seeding_double_count |
| 2017 | 1487797845 | 19th Annual IFPA European Footbag Champi | 217 | 167 | 77.0% | seeding_double_count |
| 2018 | 1516978874 | RNH Contest 2018 | 26 | 13 | 50.0% | seeding_double_count |
| 2019 | 1568289502 | Bulgarian Footbag Open Vol.2 | 22 | 12 | 54.5% | seeding_double_count |
| 2019 | 1557144269 | 11th Bembel Cup 2019 - a jam with friend | 20 | 12 | 60.0% | seeding_double_count |
| 2019 | 1566500647 | I. Basque Tournament of Footbag Net (Ind | 7 | 5 | 71.4% | no_results_posted |
| 2019 | 1564005204 | I. Ereaga Footbag Net Tournament (Double | 5 | 4 | 80.0% | no_results_posted |
| 2019 | 1574365921 | Basque Open Footbag Net Tournament (Doub | 5 | 4 | 80.0% | no_results_posted |
| 2020 | 1598616506 | II. Basque Tournament of Footbag Net (In | 10 | 8 | 80.0% | no_results_posted |
| 2021 | 1634938934 | II.Basque Open Footbag Net Tournament (D | 6 | 4 | 66.7% | no_results_posted |
| 2021 | 1617902706 | III.Basque Tournament of Footba Net (Ind | 6 | 5 | 83.3% | no_results_posted |
| 2022 | 1645621833 | IV.Basque Tournament of Footbag Net (Ind | 13 | 6 | 46.2% | no_results_posted |
| 2022 | 1653647467 | I. Elorrieta Tournament of Footbag Net ( | 5 | 4 | 80.0% | no_results_posted |
| 2022 | 1657486956 | II. Ereaga Tournament of Footbag Net | 5 | 4 | 80.0% | no_results_posted |
| 2022 | 1669556651 | III.Basque Tournament of Footbag Net (Do | 5 | 4 | 80.0% | no_results_posted |
| 2023 | 1677285621 | V. Basque Tournament of Footbag Net (Ind | 8 | 6 | 75.0% | no_results_posted |
| 2024 | 1721923932 | V.Basque Tournament of Footbag Net (Doub | 4 | 3 | 75.0% | no_results_posted |
| 2024 | 1711181388 | Bulgarian Footbag Open 2024 | 9 | 8 | 88.9% | no_results_posted |
| 2025 | 1737312020 | Canadian Closed | 2 | 1 | 50.0% | no_results_posted |
| 2025 | 1742511366 | VII.Basque Tournament of Footbag Net (In | 11 | 7 | 63.6% | no_results_posted |
| 2025 | 1745686591 | Footbag Finnish Open 2024 - Singles Net | 8 | 6 | 75.0% | no_results_posted |
| 2025 | 1756448770 | Bulgarian Footbag Championships 2025 | 18 | 16 | 88.9% | no_results_posted |

---
*Generated by tools/43_deep_mirror_audit.py*