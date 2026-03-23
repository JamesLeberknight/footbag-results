import pandas as pd
from pathlib import Path

# --- CONFIG ---
ROOT = Path(__file__).resolve().parent
TRUTH_CSV = ROOT / "inputs" / "Persons_Truth.csv"
SUCCESS_CSV = ROOT / "out" / "master_results.csv" # Your existing 400 lines
LEAN_CSV = ROOT / "inputs" / "Persons_Truth_Lean.csv"

def create_lean_truth():
    # Load the master list
    master_df = pd.read_csv(TRUTH_CSV)
    
    # Load your successful 400 lines to see who is "Active" in this era
    try:
        success_df = pd.read_csv(SUCCESS_CSV)
        active_names = success_df['Name'].unique().tolist()
        print(f"Found {len(active_names)} active players in current results.")
    except FileNotFoundError:
        active_names = []
        print("Master results not found, skipping frequency filter.")

    # Strategy: Keep active players + Top 500 historical players
    # (Adjust '500' based on how much quota you want to save)
    if active_names:
        lean_df = master_df[master_df['person_canon'].isin(active_names)]
        # Add a buffer of top names to catch new people
        buffer_df = master_df.head(500) 
        lean_df = pd.concat([lean_df, buffer_df]).drop_duplicates()
    else:
        # Fallback: Just take the top 1000 names (usually the most famous/active)
        lean_df = master_df.head(1000)

    lean_df.to_csv(LEAN_CSV, index=False)
    print(f"✅ Created Lean Truth: {len(lean_df)} records (Significant token savings!)")

if __name__ == "__main__":
    create_lean_truth()
