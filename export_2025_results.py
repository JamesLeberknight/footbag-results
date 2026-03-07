import pandas as pd
from pathlib import Path

INPUT = "out/Placements_Flat.csv"
OUT = Path("legacy_data/event_results/2025_results_long.csv")

df = pd.read_csv(INPUT)

df = df[df["year"] == 2025].copy()

df = df.sort_values(
    ["event_id", "division_canon", "place", "person_canon"],
    na_position="last"
)

df["participant_order"] = (
    df.groupby(["event_id", "year", "division_canon", "place"])
      .cumcount() + 1
)

out = df.rename(columns={
    "event_id": "legacy_event_id",
    "division_canon": "discipline",
    "place": "placement",
    "person_canon": "participant_name"
})[
    ["legacy_event_id", "year", "discipline", "placement", "participant_order", "participant_name"]
].copy()

out["score_text"] = ""

OUT.parent.mkdir(parents=True, exist_ok=True)

out.to_csv(OUT, index=False, encoding="utf-8")

print("Rows written:", len(out))
print("Output file:", OUT)
print(out.head(10))
