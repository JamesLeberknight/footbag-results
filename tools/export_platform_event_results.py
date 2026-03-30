from pathlib import Path
import pandas as pd

in_path = Path("/home/james/projects/FOOTBAG_DATA/out/release_publication/event_results.csv")
out_path = Path("/home/james/projects/footbag-platform/legacy_data/event_results/canonical_input/event_results.csv")
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
    "score_text": df["score_text"].astype(str).str.strip(),
    "notes": "",
    "source": (
        df["source_type"].astype(str).str.strip()
        + "|" +
        df["data_source"].astype(str).str.strip()
    ).str.strip("|"),
})

for col in out.columns:
    out[col] = out[col].fillna("").astype(str)

# CRITICAL: deduplicate on (event_key, discipline_key, placement).
# PRE1997 source values start with "FBW" which sorts before "MIRROR" (POST1997),
# so ascending sort on source keeps PRE1997 first → authoritative for pre-1997 events.
n_before = len(out)
out = out.sort_values(
    ["event_key", "discipline_key", "placement", "source"],
    ascending=[True, True, True, True],
)
out = out.drop_duplicates(subset=["event_key", "discipline_key", "placement"], keep="first")
n_dupes = n_before - len(out)
if n_dupes:
    print(f"  Deduped {n_dupes} rows (PRE1997 kept over POST1997 where discipline names collide after normalisation)")

out.to_csv(out_path, index=False)

print(f"Wrote: {out_path}")
print(out.head(2).to_csv(index=False))
