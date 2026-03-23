import os
import pandas as pd
import re
from pathlib import Path

# --- CONFIG ---
ROOT = Path(__file__).resolve().parent.parent
TRANSCRIPTION_DIR = ROOT / "out" / "vlm_results"
TRUTH_CSV = ROOT / "inputs" / "Persons_Truth.csv"
FINAL_OUTPUT = ROOT / "out" / "stage2_vlm_extracted_results.csv"

def parse_markdown_table(text):
    """Simple parser to extract data from Markdown tables."""
    rows = []
    lines = text.strip().split('\n')
    for line in lines:
        # Look for lines that look like table rows: | cell | cell |
        if '|' in line and '---' not in line:
            cells = [c.strip() for c in line.split('|') if c.strip()]
            if len(cells) > 1:
                rows.append(cells)
    return rows

def harvest_results():
    all_data = []
    
    # Load Truth table for a final local verification
    truth_df = pd.read_csv(TRUTH_CSV)
    valid_names = set(truth_df['person_canon'].unique())

    files = [f for f in os.listdir(TRANSCRIPTION_DIR) if f.endswith(".md")]
    print(f"Harvesting data from {len(files)} transcription files...")

    for file_name in files:
        # Extract Vol and Page from filename (e.g., vol2_page14)
        match = re.search(r'(vol\d+)_page(\d+)', file_name)
        vol = match.group(1) if match else "unknown"
        page = match.group(2) if match else "unknown"

        with open(TRANSCRIPTION_DIR / file_name, 'r') as f:
            content = f.read()
            
            # Extract tables
            table_rows = parse_markdown_table(content)
            
            for row in table_rows:
                # Basic cleanup and normalization
                # Expected row: [Rank, Name, Score/Result, Location/Notes]
                if len(row) >= 2:
                    name = row[1]
                    verified = "Yes" if name in valid_names else "No"
                    
                    all_data.append({
                        "source_vol": vol,
                        "source_page": page,
                        "rank": row[0],
                        "person_canon": name,
                        "result_value": row[2] if len(row) > 2 else "",
                        "location": row[3] if len(row) > 3 else "",
                        "is_verified_entity": verified
                    })

    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(FINAL_OUTPUT, index=False)
        print(f"Success! {len(df)} records harvested to {FINAL_OUTPUT}")
        
        unverified_count = len(df[df['is_verified_entity'] == "No"])
        if unverified_count > 0:
            print(f"Note: {unverified_count} names were not found in Persons_Truth and may need manual review.")
    else:
        print("No data found in transcription files.")

if __name__ == "__main__":
    harvest_results()
