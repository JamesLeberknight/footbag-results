#!/usr/bin/env python3
from __future__ import annotations

import os
import time
import base64
import requests
import pandas as pd
from pathlib import Path
from io import BytesIO

# --- PATHS ---
ROOT = Path(__file__).resolve().parent.parent
OCR_DIR = ROOT / "out" / "ocr_text"
IMG_DIR = ROOT / "out" / "preprocessed"
LEAN_CSV = ROOT / "inputs" / "Persons_Truth_Lean.csv"
OUT_DIR = ROOT / "out" / "clean_md"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- API ---
#API_KEY = os.getenv("GENAI_API_KEY")
#API_URL = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={API_KEY}"
#API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={API_KEY}"
#API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={API_KEY}"
#API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={API_KEY}"
API_KEY = os.getenv("GEMINI_API_KEY")
#API_URL = f"https://generativelanguage.googleapis.com/v1/models/gemini-2.5-flash:generateContent"
API_URL = "https://generativelanguage.googleapis.com/v1/models/gemini-2.5-flash:generateContent"
# --- SETTINGS ---
SLEEP_SECONDS = 12  # stay under free tier RPM
MAX_RETRIES = 3
MAX_GROUNDING_CHARS = int(os.getenv("MAX_GROUNDING_CHARS", "12000"))
GEMINI_REQUEST_TIMEOUT_SECONDS = int(
    os.getenv("GEMINI_REQUEST_TIMEOUT_SECONDS", "120")
)


def load_grounding():
    if not LEAN_CSV.exists():
        return ""
    df = pd.read_csv(LEAN_CSV)
    grounding = ", ".join(df.iloc[:, 0].dropna().unique().tolist())
    if len(grounding) <= MAX_GROUNDING_CHARS:
        return grounding

    # Trim by character budget to keep requests fast and under model limits.
    trimmed = grounding[:MAX_GROUNDING_CHARS]
    last_comma = trimmed.rfind(",")
    if last_comma > 0:
        trimmed = trimmed[:last_comma]
    return trimmed


def read_ocr_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def encode_image(path: Path) -> str:
    # Inline images heavily increase request payload size; downscale to
    # improve reliability and reduce timeouts.
    try:
        from PIL import Image
    except ImportError:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    img = Image.open(path)
    max_dim = int(os.getenv("GEMINI_IMAGE_MAX_DIM", "800"))
    if img.width > max_dim or img.height > max_dim:
        scale = min(max_dim / img.width, max_dim / img.height)
        new_size = (max(1, int(img.width * scale)), max(1, int(img.height * scale)))
        img = img.resize(new_size, resample=Image.LANCZOS)

    # Save optimized PNG to keep mime_type consistent with the prompt.
    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def build_prompt(ocr_text: str, grounding: str) -> str:
    return f"""
You are cleaning OCR from historical footbag results pages.

Use BOTH:
1. the OCR text
2. the page image

REFERENCE PLAYER LIST:
{grounding}

Rules:
- Do not invent names, ranks, or scores.
- Prefer a reference-list spelling only when clearly supported by OCR/image.
- Preserve uncertainty rather than guessing.
- If the page contains several events, separate them by headings.
- If only partial results are readable, output only the readable rows.

Output format:
- Markdown
- For each event, use:
  ## Event Name
  | Rank | Name | Score | Notes |
- Leave Score blank if missing.
- Put uncertain readings in Notes.
- Do not add Location unless explicitly visible.

OCR text:
{ocr_text}
"""

def call_gemini(prompt: str, img_path: Path | None = None):
    if not API_KEY:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Export GEMINI_API_KEY before running."
        )

    parts = [{"text": prompt}]

    if img_path and img_path.exists():
        parts.append(
            {
                "inline_data": {
                    "mime_type": "image/png",
                    "data": encode_image(img_path),
                }
            }
        )

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {"temperature": 0.0},
    }

    headers = {
        "x-goog-api-key": API_KEY,
        "Content-Type": "application/json",
    }

    for attempt in range(MAX_RETRIES):
        try:
            res = requests.post(
                API_URL,
                headers=headers,
                json=payload,
                timeout=GEMINI_REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            time.sleep(10)
            continue

        if res.status_code == 200:
            try:
                data = res.json()
            except ValueError as e:
                print(f"Failed to parse JSON response: {e}; body={res.text[:200]}")
                time.sleep(10)
                continue

            return (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
            )

        elif res.status_code == 429:
            print("Rate limit hit, sleeping...")
            time.sleep(60)

        else:
            print(f"API error {res.status_code}: {res.text}")
            time.sleep(10)

    return None

def process_one(txt_path: Path, grounding: str):
    img_path = IMG_DIR / (txt_path.stem + ".png")
    out_path = OUT_DIR / (txt_path.stem + ".md")

    if out_path.exists():
        print(f"SKIP {out_path.name}")
        return

    ocr_text = read_ocr_text(txt_path)

    prompt = build_prompt(ocr_text, grounding)

    print(f"Processing {txt_path.name}...")

    result = call_gemini(prompt, img_path=img_path)

    if result:
        out_path.write_text(result, encoding="utf-8")
        print(f"✓ Saved {out_path.name}")
    else:
        print(f"FAILED {txt_path.name}")

    time.sleep(SLEEP_SECONDS)


def main():
    grounding = load_grounding()

    txt_files = sorted(OCR_DIR.glob("*.txt"))

    if not txt_files:
        print("No OCR files found.")
        return

    for txt in txt_files:
        process_one(txt, grounding)


if __name__ == "__main__":
    main()
