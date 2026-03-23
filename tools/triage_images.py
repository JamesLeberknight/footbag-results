import os
import base64
import requests
from pathlib import Path

# --- CONFIG ---
API_KEY = os.getenv("AIzaSyDC0FwIbZYSgilLdsE1Q0GWTsYZE_wzUoU")
TILED_DIR = Path("out/vlm_prep")
TRIAGE_LOG = Path("out/triage_results.csv")
#API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent?key={API_KEY}"
# Change the model string from 2.0-flash-lite to 1.5-flash
#API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={API_KEY}"
# Copy and paste this exactly:
#API_URL = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={API_KEY}"
# Change the URL to use the 2.5 Flash-Lite model
#API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key={API_KEY}"
# Copy this EXACTLY - it uses the v1 (Production) endpoint and 1.5-flash
API_URL = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={API_KEY}"
def encode_image(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode('utf-8')

def run_triage():
    tiles = sorted(list(TILED_DIR.glob("*.jpg")))
    results = []

    print(f"Starting Triage on {len(tiles)} tiles...")

    for tile in tiles:
        payload = {
            "contents": [{
                "parts": [
                    {"text": "Does this image contain a data table of tournament results? Answer only 'YES' or 'NO'."},
                    {"inline_data": {"mime_type": "image/jpeg", "data": encode_image(tile)}}
                ]
            }]
        }

        try:
            res = requests.post(API_URL, json=payload)
            answer = res.json()['candidates'][0]['content']['parts'][0]['text'].strip().upper()
            results.append(f"{tile.name},{answer}")
            print(f"{tile.name}: {answer}")
        except:
            print(f"Error triaging {tile.name}")

    with open(TRIAGE_LOG, "w") as f:
        f.write("filename,contains_table\n")
        f.writelines("\n".join(results))

if __name__ == "__main__":
    run_triage()
