#!/usr/bin/env python3
"""
02_image_qc_report.py — Image QC report for preprocessed page scans.

Scans all images in a target folder and reports:
  - filename
  - image dimensions (width x height in pixels)
  - format (JPEG, PNG, etc.)
  - orientation (portrait / landscape / square)
  - likely multi-column layout (simple heuristic)
  - file size in KB

Outputs:
  early_data/page_inventory/image_qc_report.csv

Usage:
  python early_data/scripts/02_image_qc_report.py [IMAGE_DIR]

  Default IMAGE_DIR: early_data/preprocessed_pages/
  (relative to repo root)

This script is for preparation and QC only. It performs no OCR or transcription.
"""

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_IMAGE_DIR = REPO_ROOT / "early_data" / "preprocessed_pages"
OUTPUT_CSV = REPO_ROOT / "early_data" / "page_inventory" / "image_qc_report.csv"

SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".gif", ".webp"}

# Aspect ratio thresholds
LANDSCAPE_RATIO = 1.15   # width/height > this → landscape
SQUARE_RATIO    = 0.87   # width/height between this and LANDSCAPE_RATIO → square

# Multi-column heuristic: magazine pages that are landscape OR very wide relative
# to a typical portrait scan are candidates for multi-column layout.
# This is a rough signal only — confirm by visual inspection.
MULTICOLUMN_RATIO = 1.3  # width/height > this suggests multi-column or spread


def classify_orientation(width: int, height: int) -> str:
    ratio = width / height if height else 0
    if ratio > LANDSCAPE_RATIO:
        return "landscape"
    elif ratio < (1 / LANDSCAPE_RATIO):
        return "portrait"
    else:
        return "square"


def likely_multicolumn(width: int, height: int) -> str:
    """
    Rough heuristic: if width > height * MULTICOLUMN_RATIO it may be a two-page
    spread or a multi-column layout rotated to landscape.
    Returns 'yes', 'maybe', or 'no'.
    """
    ratio = width / height if height else 0
    if ratio > MULTICOLUMN_RATIO * 1.4:
        return "yes"
    elif ratio > MULTICOLUMN_RATIO:
        return "maybe"
    return "no"


def process_image(path: Path) -> dict:
    """Return image metadata dict. Uses PIL if available, falls back to basic info."""
    row = {
        "filename": path.name,
        "path": str(path.relative_to(REPO_ROOT)),
        "format": path.suffix.lstrip(".").upper(),
        "width_px": "",
        "height_px": "",
        "orientation": "",
        "likely_multicolumn": "",
        "file_size_kb": "",
        "notes": "",
    }

    try:
        row["file_size_kb"] = f"{path.stat().st_size / 1024:.1f}"
    except OSError:
        pass

    try:
        from PIL import Image
        with Image.open(path) as img:
            w, h = img.size
            row["width_px"] = w
            row["height_px"] = h
            row["format"] = img.format or row["format"]
            row["orientation"] = classify_orientation(w, h)
            row["likely_multicolumn"] = likely_multicolumn(w, h)
    except ImportError:
        row["notes"] = "Pillow not installed — dimensions unavailable. Run: pip install Pillow"
    except Exception as e:
        row["notes"] = f"Could not read image: {e}"

    return row


def build_qc_report(image_dir: Path, output_path: Path) -> None:
    if not image_dir.exists():
        print(f"WARNING: Image directory not found: {image_dir}")
        print("  Create it and populate with images, then re-run.")
        # Write empty report with headers so the file exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "filename", "path", "format", "width_px", "height_px",
            "orientation", "likely_multicolumn", "file_size_kb", "notes",
        ]
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()
        print(f"  Empty report written: {output_path}")
        return

    images = sorted(
        p for p in image_dir.iterdir()
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    )

    if not images:
        print(f"No images found in: {image_dir}")
        print(f"  Supported extensions: {', '.join(sorted(SUPPORTED_EXTENSIONS))}")

    print(f"Processing {len(images)} images in: {image_dir}")
    rows = [process_image(img) for img in images]

    # Summary
    orientations = {}
    for r in rows:
        k = r["orientation"] or "unknown"
        orientations[k] = orientations.get(k, 0) + 1
    for k, v in sorted(orientations.items()):
        print(f"  {k}: {v}")

    multicolumn = [r for r in rows if r["likely_multicolumn"] in ("yes", "maybe")]
    if multicolumn:
        print(f"  Possible multi-column pages: {len(multicolumn)}")
        for r in multicolumn:
            print(f"    {r['filename']} ({r['width_px']}x{r['height_px']}, {r['likely_multicolumn']})")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "filename", "path", "format", "width_px", "height_px",
        "orientation", "likely_multicolumn", "file_size_kb", "notes",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Report written: {output_path} ({len(rows)} rows)")


if __name__ == "__main__":
    image_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_IMAGE_DIR
    build_qc_report(image_dir, OUTPUT_CSV)
