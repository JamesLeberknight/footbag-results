#!/usr/bin/env bash

FILE="Footbag_Event_Review_Packet.xlsx"

if [ ! -f "$FILE" ]; then
  echo "ERROR: $FILE not found in current directory."
  exit 1
fi

echo
echo "Checking workbook: $FILE"
echo

python3 <<'PY'
import pandas as pd

file = "Footbag_Event_Review_Packet.xlsx"

xls = pd.ExcelFile(file)

print("\n==================== SHEETS ====================")
for s in xls.sheet_names:
    print(" -", s)

df = pd.read_excel(file, sheet_name="Queue")

print("\n==================== BASIC INFO ====================")
print("Row count:", len(df))
print("Column count:", len(df.columns))

print("\n==================== HEATMAP ====================")
if "review_heat_label" in df.columns:
    print(df["review_heat_label"].value_counts(dropna=False))

print("\n==================== PRIORITY TIERS ====================")
if "priority_tier" in df.columns:
    print(df["priority_tier"].value_counts(dropna=False))

print("\n==================== TOP 20 HIGH PRIORITY EVENTS ====================")
cols = [
    "event_id",
    "year",
    "event_name",
    "review_heat_score",
    "review_heat_label",
    "priority_tier",
    "diff_summary"
]

cols = [c for c in cols if c in df.columns]

if "review_heat_score" in df.columns:
    print(
        df.sort_values("review_heat_score", ascending=False)[cols]
        .head(20)
        .to_string(index=False)
    )

print("\n==================== DIVISION MISMATCH ====================")
if "division_count_raw" in df.columns and "division_count_canonical" in df.columns:
    mism = (df["division_count_raw"] != df["division_count_canonical"]).sum()
    print("Division mismatches:", mism)

print("\n==================== PLACEMENT MISMATCH ====================")
if "placement_count_raw" in df.columns and "placement_count_canonical" in df.columns:
    mism = (df["placement_count_raw"] != df["placement_count_canonical"]).sum()
    print("Placement mismatches:", mism)

print("\n==================== DONE ====================")
PY
