from pathlib import Path
import pandas as pd

in_path = Path("/home/james/projects/FOOTBAG_DATA/out/release_publication/event_result_participants.csv")
out_path = Path("/home/james/projects/fb-bw/legacy_data/event_results/canonical_input/event_result_participants.csv")
out_path.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(in_path, dtype=str).fillna("")

out = pd.DataFrame({
    "event_key": df["event_id"].astype(str).str.strip(),

    "discipline_key": (
        df["event_id"].astype(str).str.strip()
        + "__" +
        df["discipline"].astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
    ),

    "placement": df["placement"].astype(str).str.strip(),
    "participant_order": df["participant_order"].astype(str).str.strip(),
    "display_name": df["display_name"].astype(str).str.strip(),
    "person_id": df["person_id"].astype(str).str.strip(),
    "team_person_key": df["team_person_key"].astype(str).str.strip(),

    # platform requires notes → safe default
    "notes": "",
    # carry data_source for dedup sort; dropped before output
    "_data_source": df["data_source"].astype(str).str.strip(),
})

# clean
for col in out.columns:
    out[col] = out[col].fillna("").astype(str)

# CRITICAL: deduplicate on (event_key, discipline_key, placement, participant_order).
# "PRE1997" > "POST1997" alphabetically (R > O), so descending sort puts PRE1997 first.
n_before = len(out)
out = out.sort_values(
    ["event_key", "discipline_key", "placement", "participant_order", "_data_source"],
    ascending=[True, True, True, True, False],
)
out = out.drop_duplicates(
    subset=["event_key", "discipline_key", "placement", "participant_order"],
    keep="first",
)
n_dupes = n_before - len(out)
if n_dupes:
    print(f"  Deduped {n_dupes} rows (PRE1997 kept over POST1997)")

out = out.drop(columns=["_data_source"])

out.to_csv(out_path, index=False)

print(f"Wrote: {out_path}")
print(out.head(2).to_csv(index=False))



