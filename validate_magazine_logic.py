import pandas as pd
from pathlib import Path

def test_pipeline_results():
    print("--- Running Pipeline Validation Suite ---")
    
    # 1. Check Identity Resolution
    # We now check out/Placements_Flat.csv (the final output of 02p5)
    flat = pd.read_csv("out/Placements_Flat.csv")
    al_rows = flat[flat['person_canon'] == 'Alan Cook']
    ids_found = al_rows['person_id'].nunique()
    print(f"[Identity] Alan Cook rows: {len(al_rows)} | Unique IDs: {ids_found}")
    assert ids_found == 1, "FAILURE: 'Big Al' and 'Alan Cook' did not resolve to one ID!"

    # 2. Check Quarantine Logic
    # We check stage2_canonical_events.csv for the metadata status
    events = pd.read_csv("out/stage2_canonical_events.csv")
    
    # Use a string contains check to be safer against whitespace
    jam_rows = events[events['event_name'].str.contains('Secret Underground Jam', na=False)]
    
    if jam_rows.empty:
        print("[Quarantine] ERROR: 'Secret Underground Jam' not found in canonical events!")
        print(f"Available events: {events['event_name'].unique()[:5]}...") # Show first 5 for debugging
        return

    jam = jam_rows.iloc[0]
    print(f"[Quarantine] Secret Jam status: {jam['status']}")
    assert jam['status'] == 'quarantine', f"FAILURE: Event was {jam['status']} but expected 'quarantine'!"

    print("\n✅ ALL TESTS PASSED: Magazine Pipeline is internally consistent.")

if __name__ == "__main__":
    test_pipeline_results()
