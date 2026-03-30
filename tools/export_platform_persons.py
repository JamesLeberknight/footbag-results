from pathlib import Path
import pandas as pd

in_path = Path("/home/james/projects/FOOTBAG_DATA/out/release_publication/persons.csv")
out_path = Path("/home/james/projects/footbag-platform/legacy_data/event_results/canonical_input/persons.csv")
out_path.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(in_path, dtype=str).fillna("")

out = pd.DataFrame({
    "person_id": df["person_id"].astype(str).str.strip(),
    "person_name": df["person_canon"].astype(str).str.strip(),
    "ifpa_member_id": df["ifpa_member_id"].astype(str).str.strip() if "ifpa_member_id" in df.columns else "",
    "country": df["country"].astype(str).str.strip(),
    "first_year": df["first_year"].astype(str).str.strip(),
    "last_year": df["last_year"].astype(str).str.strip(),
    "event_count": "",
    "placement_count": "",
    "bap_member": df["bap_member"].astype(str).str.strip().map(lambda v: "1" if v.upper() == "Y" else "0"),
    "bap_nickname": df["bap_nickname"].astype(str).str.strip(),
    "bap_induction_year": df["bap_induction_year"].astype(str).str.strip(),
    "fbhof_member": df["fbhof_member"].astype(str).str.strip().map(lambda v: "1" if v.upper() == "Y" else "0"),
    "fbhof_induction_year": df["fbhof_induction_year"].astype(str).str.strip(),
    "freestyle_sequences": "",
    "freestyle_max_add": "",
    "freestyle_unique_tricks": "",
    "freestyle_diversity_ratio": "",
    "signature_trick_1": "",
    "signature_trick_2": "",
    "signature_trick_3": "",
})

for col in out.columns:
    out[col] = out[col].fillna("").astype(str)

# drop blank names just in case
out = out[out["person_name"].str.strip() != ""].copy()

out.to_csv(out_path, index=False)

print(f"Wrote: {out_path}")
print(out.head(2).to_csv(index=False))
print("Rows:", len(out))
