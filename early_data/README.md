# early_data — Pre-1997 Historical Recovery

This directory contains all work for the **pre-1997 historical recovery project**.

It is a completely separate workflow from the published post-1997 dataset.
Nothing here modifies `out/canonical/`, `inputs/identity_lock/`, or any published release artifact.

---

## Folder Purpose

| Folder | Purpose |
|---|---|
| `raw_pages/` | Original unmodified source images as received (JPEG/PNG scans) |
| `preprocessed_pages/` | Cleaned, cropped, and rotation-corrected images ready for extraction |
| `page_inventory/` | CSV inventory of all pages and image QC reports |
| `event_blocks/` | Event-level extracted blocks (one row per event/division block) |
| `placements/` | Placement-level extracted data (raw, not yet resolved) |
| `review/` | Person matching and human review files |
| `prompts/` | Prompt templates for Gemini or other model transcription |
| `docs/` | Workflow documentation |
| `scripts/` | Pipeline scripts for this recovery workflow |

---

## Primary Sources

- **Footbag World magazine** (Vol. 2–14, approx. 1980–1995)
  - Cleaned page images exported from `raw_images_for_manual_crop_rotation2.pptx`
- **IFAB Rulebook** (Worlds History pages)
- **OLD_RESULTS.txt** (legacy plaintext, in `inputs/`)

---

## Status

- [ ] Page inventory built
- [ ] Images preprocessed and renamed
- [ ] Event blocks extracted
- [ ] Placements extracted
- [ ] Person matching complete
- [ ] Pre-1997 canonical outputs produced

See `docs/PRE1997_WORKFLOW.md` for the full workflow description.
