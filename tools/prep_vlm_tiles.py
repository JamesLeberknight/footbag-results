import os
import re
from pathlib import Path
from PIL import Image, ImageOps, ImageEnhance

# --- CONFIG ---
ROOT = Path(__file__).resolve().parent.parent
IMG_DIR = ROOT / "out" / "scans"
TILED_DIR = ROOT / "out" / "vlm_prep"
TILED_DIR.mkdir(parents=True, exist_ok=True)

def slice_vols():
    # Improved Regex: Looks for 'Vol.' followed by a space and a number between 2 and 14
    # It also handles the 'CLEAN_' prefix and '.jpeg' extension
    pattern = re.compile(r"Vol\.\s+([2-9]|1[0-4])", re.IGNORECASE)
    
    try:
        all_files = os.listdir(IMG_DIR)
    except FileNotFoundError:
        print(f"!! Error: {IMG_DIR} not found.")
        return

    images = [f for f in all_files if pattern.search(f) and f.lower().endswith(('.jpeg', '.jpg'))]
    
    if not images:
        print(f"!! No matching images found in {IMG_DIR}")
        print("Example filename expected: 'CLEAN_Footbag World Vol. 2 No. 1.jpeg'")
        return

    print(f"Slicing {len(images)} pages into high-res quadrants...")

    for img_name in sorted(images):
        path = IMG_DIR / img_name
        # Clean filename for the output (remove spaces and dots for safer OS handling)
        safe_name = img_name.replace(" ", "_").replace(".", "").replace("CLEAN_", "")
        
        with Image.open(path) as img:
            if img.width > img.height:
                img = img.rotate(90, expand=True)
            
            img = ImageOps.grayscale(img)
            img = ImageEnhance.Contrast(img).enhance(2.0)
            
            w, h = img.size
            mid_w, mid_h = w // 2, h // 2
            overlap = 100 

            quads = {
                "TL": (0, 0, mid_w + overlap, mid_h + overlap),
                "TR": (mid_w - overlap, 0, w, mid_h + overlap),
                "BL": (0, mid_h - overlap, mid_w + overlap, h),
                "BR": (mid_w - overlap, mid_h - overlap, w, h)
            }

            for label, box in quads.items():
                tile = img.crop(box)
                tile_name = f"{Path(safe_name).stem}_{label}.jpg"
                tile.save(TILED_DIR / tile_name, "JPEG", quality=95)

    print(f"Done! {len(images) * 4} tiles created in {TILED_DIR}")

if __name__ == "__main__":
    slice_vols()
