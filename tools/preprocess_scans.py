#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import cv2
import numpy as np

try:
    import pytesseract
except ImportError:
    pytesseract = None


VALID_EXTS = {".jpg", ".jpeg", ".JPG", ".JPEG"}


def load_image(path: Path) -> np.ndarray:
    img = cv2.imread(str(path), cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError(f"Could not read image: {path}")
    return img


def upscale_gray(gray: np.ndarray, scale: float) -> np.ndarray:
    if scale == 1.0:
        return gray
    return cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)


def preprocess_gray(img_bgr: np.ndarray, scale: float = 3.0) -> np.ndarray:
    gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

    # Upscale first so later operations have more signal to work with.
    gray = upscale_gray(gray, scale)

    # Mild denoise that preserves edges.
    gray = cv2.bilateralFilter(gray, d=9, sigmaColor=75, sigmaSpace=75)

    # Local contrast enhancement. Better than hard thresholding for old scans.
    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
    gray = clahe.apply(gray)

    return gray


def rotate_image_bound(img: np.ndarray, angle_deg: float, border_value: int | tuple[int, int, int] = 255) -> np.ndarray:
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


def detect_orientation_osd(gray: np.ndarray) -> int:
    """
    Returns one of {0, 90, 180, 270} meaning the image should be rotated
    clockwise by that many degrees to become upright.
    """
    if pytesseract is None:
        return 0

    try:
        osd = pytesseract.image_to_osd(gray)
    except Exception:
        return 0

    rotate = 0
    for line in osd.splitlines():
        if line.lower().startswith("rotate:"):
            try:
                rotate = int(line.split(":")[1].strip())
            except Exception:
                rotate = 0
            break
    return rotate


def deskew_small_angle(gray: np.ndarray, max_abs_angle: float = 12.0) -> tuple[np.ndarray, float]:
    """
    Corrects small skew after major orientation is fixed.
    Uses a text-mask approach and minAreaRect.
    """
    # Invert so text is white-ish on black for morphology.
    inv = cv2.bitwise_not(gray)

    # Threshold with Otsu; do not use this as final output, only for angle estimation.
    _, bw = cv2.threshold(inv, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # Join text into larger components.
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 3))
    connected = cv2.morphologyEx(bw, cv2.MORPH_CLOSE, kernel)

    coords = np.column_stack(np.where(connected > 0))
    if len(coords) < 100:
        return gray, 0.0

    rect = cv2.minAreaRect(coords)
    angle = rect[-1]

    # OpenCV angle normalization.
    if angle < -45:
        angle = 90 + angle
    elif angle > 45:
        angle = angle - 90

    # We only want gentle deskew here, not 90-degree orientation.
    if abs(angle) > max_abs_angle:
        return gray, 0.0

    corrected = rotate_image_bound(gray, angle, border_value=255)
    return corrected, angle


def crop_border(gray: np.ndarray, margin: int = 10) -> np.ndarray:
    """
    Tries to trim empty border/noise around the page.
    Conservative on purpose.
    """
    inv = cv2.bitwise_not(gray)
    _, bw = cv2.threshold(inv, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    # Merge text/page areas.
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
    merged = cv2.morphologyEx(bw, cv2.MORPH_CLOSE, kernel)

    contours, _ = cv2.findContours(merged, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return gray

    cnt = max(contours, key=cv2.contourArea)
    x, y, w, h = cv2.boundingRect(cnt)

    x0 = max(0, x - margin)
    y0 = max(0, y - margin)
    x1 = min(gray.shape[1], x + w + margin)
    y1 = min(gray.shape[0], y + h + margin)

    cropped = gray[y0:y1, x0:x1]
    return cropped if cropped.size else gray


def process_one(path: Path, out_dir: Path, scale: float, crop: bool) -> None:
    img = load_image(path)
    gray = preprocess_gray(img, scale=scale)

    # Major orientation using Tesseract OSD if available.
    rotate_cw = detect_orientation_osd(gray)
    if rotate_cw:
        gray = rotate_image_bound(gray, -rotate_cw, border_value=255)

    # Fine deskew.
    gray, skew_angle = deskew_small_angle(gray)

    if crop:
        gray = crop_border(gray)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"PREP_{path.stem}.png"
    cv2.imwrite(str(out_path), gray)

    print(
        f"{path.name} -> {out_path.name} | "
        f"osd_rotate={rotate_cw} cw | deskew={skew_angle:.2f}"
    )


def iter_images(in_dir: Path) -> list[Path]:
    return sorted(
        p for p in in_dir.iterdir()
        if p.is_file() and p.suffix in VALID_EXTS
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Preprocess raw JPEG scans for OCR.")
    parser.add_argument("--input", default="out/images", help="Input directory of raw JPEGs")
    parser.add_argument("--output", default="out/preprocessed", help="Output directory")
    parser.add_argument("--scale", type=float, default=3.0, help="Upscale factor (default: 3.0)")
    parser.add_argument("--no-crop", action="store_true", help="Disable conservative border crop")
    args = parser.parse_args()

    in_dir = Path(args.input)
    out_dir = Path(args.output)

    if not in_dir.exists():
        print(f"Input directory not found: {in_dir}", file=sys.stderr)
        return 1

    files = iter_images(in_dir)
    if not files:
        print(f"No JPEG files found in {in_dir}", file=sys.stderr)
        return 1

    if pytesseract is None:
        print("Warning: pytesseract not installed; 90/180/270 orientation detection will be skipped.", file=sys.stderr)

    for path in files:
        try:
            process_one(path, out_dir, scale=args.scale, crop=not args.no_crop)
        except Exception as e:
            print(f"ERROR processing {path.name}: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
