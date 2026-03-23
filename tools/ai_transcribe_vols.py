import os
import time
import base64
import requests
import pandas as pd
from pathlib import Path

# --- PATH CONFIG ---
ROOT = Path(__file__).resolve().parent
TILED_DIR = ROOT / "out" / "vlm_prep"
LEAN_CSV = ROOT / "inputs" / "Persons_Truth_Lean.csv"
TRIAGE_CSV = ROOT / "out" / "triage_results.csv" # Created by the triage script
RESULTS_DIR = ROOT / "out" / "vlm_results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# --- API CONFIG ---
API_KEY = os.getenv("GENAI_API_KEY")
# Using v1beta for Gemini 2.0 Flash-Lite stability
API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent?key={API_KEY}"

def load_lean_grounding():
    df = pd.read_csv(LEAN_CSV)
    return ", ".join(df.iloc[:, 0].dropna().unique().tolist())

def load_triage_list():
    if not TRIAGE_CSV.exists():
        return None
    df = pd.read_csv(TRIAGE_CSV)
    # Return a set of filenames where contains_table is YES
    return set(df[df['contains_table'] == 'YES']['filename'].tolist())

def encode_image(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode('utf-8')

def run_transcription():
    grounding_list = load_lean_grounding()
    valid_tiles = load_triage_list()
    
    # Get all tiles in the directory
    all_tiles = sorted(list(TILED_DIR.glob("*.jpg")))
    
    # Filter by Triage if available
    if valid_tiles:
        tiles_to_process = [t for t in all_tiles if t.name in valid_tiles]
        print(f"Triage Filter: Processing {len(tiles_to_process)} of {len(all_tiles)} total tiles.")
    else:
        tiles_to_process = all_tiles
        print("No triage log found. Processing all tiles (Warning: high quota usage).")

    for tile in tiles_to_process:
        out_file = RESULTS_DIR / f"{tile.stem}.md"
        if out_file.exists(): continue

        print(f"Transcribing {tile.name}...")
        
        payload = {
            "contents": [{
                "parts": [
                    {"text": f"Reference Player List: {grounding_list}"},
                    {"text": "Transcribe the results table from this scan into Markdown. Use the reference list to fix spelling. Columns: Rank, Name, Score, Location."},
                    {"inline_data": {"mime_type": "image/jpeg", "data": encode_image(tile)}}
                ]
            }],
            "generationConfig": {"temperature": 0.0}
        }

        # --- EXECUTION LOOP ---
        while True:
            response = requests.post(API_URL, json=payload)
            
            if response.status_code == 200:
                text = response.json()['candidates'][0]['content']['parts'][0]['text']
                with open(out_file, "w") as f:
                    f.write(text)
                print(f"   ✓ Saved to {out_file.name}")
                time.sleep(12) # 5 RPM safety buffer for Free Tier
                break
            
            elif response.status_code == 429:
                print("   !! Daily/Minute Quota Hit. Sleeping 60s...")
                time.sleep(60)
            
            else:
                print(f"   !! API Error {response.status_code}: {response.text}")
                break

if __name__ == "__main__":
    run_transcription()
