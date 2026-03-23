#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import cv2
import numpy as np

VALID_EXTS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}


def iter_images(in_dir: Path) -> list[Path]:
    return sorted(
        p for p in in_dir.iterdir()
        if p.is_file() and p.suffix.lower() in VALID_EXTS
    )


def load_image(path: Path) -> np.ndarray:
    img = cv2.imread(str(path), cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError(f"Could not read image: {path}")
    return img


def preprocess_basic(img_bgr: np.ndarray, scale: float = 2.5) -> np.ndarray:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

    if scale != 1.0:
        gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

    # Mild denoise that preserves text edges
    gray = cv2.bilateralFilter(gray, d=9, sigmaColor=75, sigmaSpace=75)

    # Local contrast enhancement
    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
    gray = clahe.apply(gray)

    return gray


def rotate_image_bound(
    img: np.ndarray,
    angle_deg: float,
    border_value: int = 255,
) -> np.ndarray:
    h, w = img.shape[:2]
    center = (w / 2.0, h / 2.0)

    M = cv2.getRotationMatrix2D(center, angle_deg, 1.0)
    cos = abs(M[0, 0])
    sin = abs(M[0, 1])

    new_w = int((h * sin) + (w * cos))
    new_h = int((h * cos) + (w * sin))

    M[0, 2] += (new_w / 2) - center[0]
    M[1, 2] += (new_h / 2) - center[1]

    return cv2.warpAffine(
        img,
        M,
        (new_w, new_h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=border_value,
    )


def deskew_small_angle(gray: np.ndarray, max_abs_angle: float = 8.0) -> tuple[np.ndarray, float]:
    """
    Gentle deskew only. Intended for already roughly upright manual crops.
    """
    inv = cv2.bitwise_not(gray)
    _, bw = cv2.threshold(inv, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 3))
    connected = cv2.morphologyEx(bw, cv2.MORPH_CLOSE, kernel)

    coords = np.column_stack(np.where(connected > 0))
    if len(coords) < 100:
        return gray, 0.0

    rect = cv2.minAreaRect(coords)
    angle = rect[-1]

    if angle < -45:
        angle = 90 + angle
    elif angle > 45:
        angle = angle - 90

    if abs(angle) > max_abs_angle:
        return gray, 0.0

    corrected = rotate_image_bound(gray, angle, border_value=255)
    return corrected, angle


def process_one(path: Path, out_dir: Path, scale: float, deskew: bool, overwrite: bool) -> None:
    out_path = out_dir / f"PREP_{path.stem}.png"
    if out_path.exists() and not overwrite:
        print(f"SKIP {path.name}")
        return

    img = load_image(path)
    gray = preprocess_basic(img, scale=scale)

    skew_angle = 0.0
    if deskew:
        gray, skew_angle = deskew_small_angle(gray)

    out_dir.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(out_path), gray)

    print(f"OK   {path.name} -> {out_path.name} | deskew={skew_angle:.2f}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Preprocess manually cropped images for OCR.")
    parser.add_argument("--input", default="out/manual_crops_raw", help="Input directory of manual raw crops")
    parser.add_argument("--output", default="out/manual_crops_prepped", help="Output directory")
    parser.add_argument("--scale", type=float, default=2.5, help="Upscale factor (default: 2.5)")
    parser.add_argument("--no-deskew", action="store_true", help="Disable small-angle deskew")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
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
                scale=args.scale,
                deskew=not args.no_deskew,
                overwrite=args.overwrite,
            )
        except Exception as e:
            print(f"ERROR {path.name}: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
