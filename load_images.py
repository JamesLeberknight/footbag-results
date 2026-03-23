import pandas as pd
import os
import re
from pathlib import Path

# 1. Setup Paths
IMAGE_DIR = Path("~/projects/FOOTBAG_DATA/out/images").expanduser()
INDEX_PATH = Path("~/projects/FOOTBAG_DATA/inputs/magazine_scan_index.csv").expanduser()

def normalize(text):
    """Normalize strings for better matching: lowercase and alphanumeric only."""
    return re.sub(r'[^a-z0-9]', '', str(text).lower())

def main():
    if not INDEX_PATH.exists():
        print(f"Error: Index not found at {INDEX_PATH}")
        return

    # Load the current index
    df = pd.read_csv(INDEX_PATH)
    
    # 2. Index the Image Directory
    if not IMAGE_DIR.exists():
        print(f"Error: Image directory not found at {IMAGE_DIR}")
        return
        
    all_jpgs = [f for f in os.listdir(IMAGE_DIR) if f.lower().endswith(('.jpg', '.jpeg'))]
    unassigned_jpgs = set(all_jpgs)
    
    # Track which ones were already assigned in the CSV
    already_assigned = set(df['source_jpg'].dropna().unique())
    unassigned_jpgs -= already_assigned

    print(f"Scanning {len(all_jpgs)} total images...")
    print(f"{len(unassigned_jpgs)} images are currently unlinked. Starting matching...\n")

    # 3. Matching Logic
    matches_found = 0
    
    for idx, row in df.iterrows():
        # Skip if already has a valid JPG
        if pd.notna(row['source_jpg']) and str(row['source_jpg']).strip() != "":
            continue
            
        event_name = str(row['event_name'])
        norm_name = normalize(event_name)
        year = str(row['year'])
        
        best_match = None
        
        # Strategy A: Exact Slug Match (e.g., "1985_Mud_Island_Jam.jpg")
        slug_name = event_name.replace(" ", "_")
        for jpg in list(unassigned_jpgs):
            if slug_name.lower() in jpg.lower():
                best_match = jpg
                break
        
        # Strategy B: Normalized String Containment
        if not best_match:
            for jpg in list(unassigned_jpgs):
                if norm_name in normalize(jpg):
                    best_match = jpg
                    break
        
        # Strategy C: Year + Partial Name (for Footbag World scans)
        if not best_match and "fbw" in event_name.lower():
            for jpg in list(unassigned_jpgs):
                if year in jpg and ("fbw" in jpg.lower() or "magazine" in jpg.lower()):
                    best_match = jpg
                    break

        if best_match:
            df.at[idx, 'source_jpg'] = best_match
            unassigned_jpgs.discard(best_match)
            matches_found += 1
            print(f"Linked: '{event_name}' -> {best_match}")

    # 4. Save and Report
    df.to_csv(INDEX_PATH, index=False)
    
    print("\n" + "="*30)
    print(f"UPDATED INDEX: {INDEX_PATH}")
    print(f"New Matches Found: {matches_found}")
    print(f"Remaining Unlinked JPGs: {len(unassigned_jpgs)}")
    
    if unassigned_jpgs:
        print("\nManual Action Required for these files:")
        for f in sorted(list(unassigned_jpgs))[:10]: # Show first 10
            print(f" - {f}")
        if len(unassigned_jpgs) > 10:
            print(f" ... and {len(unassigned_jpgs)-10} more.")

if __name__ == "__main__":
    main()
