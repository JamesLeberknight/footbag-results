#!/usr/bin/env python3
from pathlib import Path
import cv2

VALID_EXTS = (".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp")

def main():
    img_dir = Path("out/manual_crops_prepped")
    files = sorted([p for p in img_dir.iterdir() if p.suffix.lower() in VALID_EXTS])

    if not files:
        print("No images found.")
        return

    idx = 0

    while True:
        img = cv2.imread(str(files[idx]))
        display = img.copy()

        text = f"{idx+1}/{len(files)}  {files[idx].name}"
        cv2.putText(display, text, (20, 40),
                    cv2.FONT_HERSHEY_SIMPLEX, 1, (0,255,0), 2)

        cv2.imshow("Viewer", display)
        key = cv2.waitKey(0)

        if key == ord('q'):
            break
        elif key == ord('d'):   # next
            idx = min(idx + 1, len(files) - 1)
        elif key == ord('a'):   # previous
            idx = max(idx - 1, 0)

    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()
