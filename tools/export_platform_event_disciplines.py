from pathlib import Path
import pandas as pd

in_path = Path("/home/james/projects/FOOTBAG_DATA/out/release_publication/event_disciplines.csv")
out_path = Path("/home/james/projects/fb-bw/legacy_data/event_results/canonical_input/event_disciplines.csv")
out_path.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(in_path, dtype=str).fillna("")

df["event_key"] = df["event_id"].str.strip()

df["discipline_norm"] = (
    df["discipline"]
    .astype(str)
    .str.strip()
    .str.lower()
    .str.replace(r"\s+", "_", regex=True)
)

df["discipline_key"] = df["event_key"] + "__" + df["discipline_norm"]

# 🔥 CRITICAL FIX: deduplicate per (event_key, discipline_key)
df = df.sort_values(["event_key", "discipline_key"])
df = df.drop_duplicates(subset=["event_key", "discipline_key"], keep="first")

out = pd.DataFrame({
    "event_key": df["event_key"],
    "discipline_key": df["discipline_key"],
    "discipline_name": df["discipline_name"].str.strip(),
    "discipline_category": df["discipline_category"].str.strip(),
    "team_type": df["team_type"].str.strip(),
    "sort_order": df["sort_order"].str.strip(),
    "coverage_flag": df["coverage_flag"].str.strip(),
    "notes": df["notes"].str.strip(),
})

for col in out.columns:
    out[col] = out[col].fillna("").astype(str)

out.to_csv(out_path, index=False)

print("Wrote:", out_path)
print("Rows:", len(out))
