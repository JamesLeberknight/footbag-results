from pathlib import Path
import pandas as pd
import shutil
import re

# -----------------------
# CONFIG
# -----------------------
BASE_DIR = Path(__file__).resolve().parent.parent   # early_data/

SRC_DIR = BASE_DIR / "preprocessed_pages" / "raw_images_for_manual_crop_rotation2"
DST_DIR = BASE_DIR / "preprocessed_pages" / "final"
INVENTORY = BASE_DIR / "page_inventory" / "fbw_page_inventory.csv"

DST_DIR.mkdir(parents=True, exist_ok=True)


def parse_filename(source_file):
    """
    Extract Vol / No from original filename.
    """
    if not source_file:
        return None

    if "IFAB" in source_file.upper():
        return "IFAB_WORLDS_HISTORY"

    m = re.search(r'Vol\.\s*(\d+)[,\s]*No\.\s*(\d+)', source_file, re.I)
    if m:
        vol = int(m.group(1))
        num = int(m.group(2))
        return f"FBW_V{vol:02d}_N{num:02d}"
    return None


def main():
    print(f"Using inventory: {INVENTORY}")
    print(f"Using source dir: {SRC_DIR}")
    print(f"Using dest dir: {DST_DIR}")

    if not INVENTORY.exists():
        raise FileNotFoundError(f"Inventory CSV not found: {INVENTORY}")

    if not SRC_DIR.exists():
        raise FileNotFoundError(f"Source image folder not found: {SRC_DIR}")

    df = pd.read_csv(INVENTORY)

    for _, row in df.iterrows():
        slide_num = int(row["slide_num"])
        source_file = str(row.get("source_file", ""))

        src = SRC_DIR / f"Slide{slide_num}.jpg"
        if not src.exists():
            src = SRC_DIR / f"Slide{slide_num}.JPG"

        if not src.exists():
            print(f"Missing slide image: {src}")
            continue

        base = parse_filename(source_file)

        if base:
            new_name = f"{base}_p{slide_num:03d}.jpg"
        else:
            new_name = f"UNKNOWN_p{slide_num:03d}.jpg"

        dst = DST_DIR / new_name
        shutil.copy2(src, dst)

        print(f"{src.name} -> {new_name}")


if __name__ == "__main__":
    main()
