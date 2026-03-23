#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import cv2
import pytesseract


VALID_EXTS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}


def iter_images(in_dir: Path) -> list[Path]:
    return sorted(
        p for p in in_dir.iterdir()
        if p.is_file() and p.suffix.lower() in VALID_EXTS
    )


def load_gray(path: Path):
    img = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
    if img is None:
        raise ValueError(f"Could not read image: {path}")
    return img


def ocr_image(img, psm: int = 4, oem: int = 3, lang: str = "eng") -> str:
    config = f"--oem {oem} --psm {psm}"
    return pytesseract.image_to_string(img, lang=lang, config=config)


def process_one(path: Path, out_dir: Path, psm: int, oem: int, lang: str, overwrite: bool) -> None:
    out_path = out_dir / f"{path.stem}.txt"
    if out_path.exists() and not overwrite:
        print(f"SKIP {path.name} -> {out_path.name}")
        return

    img = load_gray(path)
    text = ocr_image(img, psm=psm, oem=oem, lang=lang)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")

    preview = text[:120].replace("\n", " ")
    print(f"OK   {path.name} -> {out_path.name} | {preview}")


def main() -> int:
    parser = argparse.ArgumentParser(description="OCR preprocessed images into text files.")
    parser.add_argument("--input", default="out/preprocessed", help="Input directory of preprocessed images")
    parser.add_argument("--output", default="out/ocr_text", help="Output directory for OCR .txt files")
    parser.add_argument("--psm", type=int, default=4, help="Tesseract page segmentation mode (default: 4)")
    parser.add_argument("--oem", type=int, default=3, help="Tesseract OCR engine mode (default: 3)")
    parser.add_argument("--lang", default="eng", help="Tesseract language (default: eng)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing OCR files")
    args = parser.parse_args()

    in_dir = Path(args.input)
    out_dir = Path(args.output)

    if not in_dir.exists():
        print(f"Input directory not found: {in_dir}", file=sys.stderr)
        return 1

    files = iter_images(in_dir)
    if not files:
        print(f"No image files found in {in_dir}", file=sys.stderr)
        return 1

    for path in files:
        try:
            process_one(
                path=path,
                out_dir=out_dir,
                psm=args.psm,
                oem=args.oem,
                lang=args.lang,
                overwrite=args.overwrite,
            )
        except Exception as e:
            print(f"ERROR {path.name}: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
