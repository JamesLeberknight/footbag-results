import pandas as pd
from pathlib import Path

INPUT = "out/Placements_Flat.csv"
OUT = Path("legacy_data/event_results/2025_events.csv")

df = pd.read_csv(INPUT)

df = df[df["year"] == 2025].copy()

events = (
    df[["event_id", "year"]]
    .drop_duplicates()
    .rename(columns={
        "event_id": "legacy_event_id"
    })
)

# placeholders until richer metadata is added
events["event_title"] = ""
events["start_date"] = ""
events["end_date"] = ""
events["city"] = ""
events["region"] = ""
events["country"] = ""

OUT.parent.mkdir(parents=True, exist_ok=True)

events.to_csv(OUT, index=False, encoding="utf-8")

print("Events exported:", len(events))
print(events)
