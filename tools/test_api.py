import os
import requests
import base64
from pathlib import Path

API_KEY = os.getenv("GENAI_API_KEY")
TILED_DIR = Path("out/vlm_prep")

def test_call(model_name, payload, description):
    url = f"https://generativelanguage.googleapis.com/v1/models/{model_name}:generateContent?key={API_KEY}"
    print(f"\n--- Testing {description} ({model_name}) ---")
    try:
        response = requests.post(url, json=payload, headers={'Content-Type': 'application/json'})
        if response.status_code == 200:
            print(f"✅ SUCCESS: {response.json()['candidates'][0]['content']['parts'][0]['text'][:50]}...")
            return True
        else:
            print(f"❌ FAILED {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def run_diagnostics():
    # 1. Text Only Test
    test_call("gemini-2.0-flash", {"contents": [{"parts":[{"text": "Hello"}]}]}, "Text-Only")

    # 2. Flash-Lite Test (Often has more quota)
    test_call("gemini-2.0-flash-lite", {"contents": [{"parts":[{"text": "Hello"}]}]}, "Flash-Lite Text")

    # 3. Tiny Image Test (Pick first available tile)
    tiles = list(TILED_DIR.glob("*.jpg"))
    if tiles:
        with open(tiles[0], "rb") as f:
            b64 = base64.b64encode(f.read()).decode('utf-8')
        
        img_payload = {
            "contents": [{
                "parts": [
                    {"text": "What is in this image?"},
                    {"inline_data": {"mime_type": "image/jpeg", "data": b64}}
                ]
            }]
        }
        test_call("gemini-2.0-flash-lite", img_payload, "Single Image (No List)")
    else:
        print("No images found in out/vlm_prep/ to test.")

if __name__ == "__main__":
    run_diagnostics()
