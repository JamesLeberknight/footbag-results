# How to Run the Pre-1997 Recovery Scripts

All scripts are in `early_data/scripts/` and run from the **repo root**.
All scripts use the project virtual environment (`.venv/`).

---

## 1. Build the page inventory

Reads `raw_images_for_manual_crop_rotation2.pptx` and writes
`early_data/page_inventory/fbw_page_inventory.csv`.

```bash
.venv/bin/python early_data/scripts/01_build_page_inventory.py
```

To use a different PPTX file:

```bash
.venv/bin/python early_data/scripts/01_build_page_inventory.py path/to/other.pptx
```

After running, open `fbw_page_inventory.csv` and fill in:
- `has_results` — `yes` / `no` / `partial` for each page
- `has_worlds_data` — `yes` / `no`
- `notes` — anything notable about the page

---

## 2. Run image QC report

Scans images in `early_data/preprocessed_pages/` and writes
`early_data/page_inventory/image_qc_report.csv`.

```bash
.venv/bin/python early_data/scripts/02_image_qc_report.py
```

To scan a different folder:

```bash
.venv/bin/python early_data/scripts/02_image_qc_report.py path/to/images/
```

Requires `Pillow` for dimension reading:

```bash
.venv/bin/pip install Pillow
```

---

## 3. Copy/rename page images to canonical names

Extracts embedded images from the PPTX and copies them to
`early_data/preprocessed_pages/` using stable canonical filenames.

**Dry run first (recommended):**

```bash
.venv/bin/python early_data/scripts/03_rename_pages.py --dry-run
```

**Extract from PPTX:**

```bash
.venv/bin/python early_data/scripts/03_rename_pages.py --pptx raw_images_for_manual_crop_rotation2.pptx
```

**Copy from an existing folder of raw images:**

```bash
.venv/bin/python early_data/scripts/03_rename_pages.py --source path/to/raw_jpegs/
```

The script always **copies** — originals are never modified or deleted.
Existing destination files are skipped (idempotent).

---

## Dependencies

All scripts use only packages already in `requirements.txt` except:

| Package | Used by | Install |
|---|---|---|
| `python-pptx` | 01, 03 | `pip install python-pptx` |
| `Pillow` | 02 | `pip install Pillow` |

`python-pptx` was added to the project venv during initial setup.
