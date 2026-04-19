#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — FOOTBAG_DATA (CSV-only derivative)
#
# This repo produces canonical CSVs, QC validation, and release/export
# artifacts. It does NOT load data into any database.
#
# Modes:
#   canonical_only  — rebuild from mirror + curated → canonical CSVs → QC
#   release         — canonical_only + workbook + platform export + seed CSVs
#   full_csv        — release (alias for completeness; same as release)
#
# Run from: FOOTBAG_DATA/
# Assumes:  venv already active
#
# Relationship to footbag-platform:
#   footbag-platform/legacy_data is the authoritative source of truth.
#   This repo reproduces its canonical CSV pipeline without DB interaction.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

for candidate in "${VENV_DIR:-}" .venv footbag_venv venv; do
  if [ -n "$candidate" ] && [ -f "$candidate/bin/activate" ]; then
    . "$candidate/bin/activate"
    break
  fi
done

MODE="${1:-release}"

# =============================================================================
# STAGE 1: REBUILD — mirror + curated → stage1 → stage2
# =============================================================================
run_rebuild() {
    echo ""
    echo "── [1/6] REBUILD ──────────────────────────────────────"
    python pipeline/adapters/mirror_results_adapter.py --mirror mirror_full
    python pipeline/adapters/curated_events_adapter.py
    python pipeline/01c_merge_stage1.py
    python pipeline/02_canonicalize_results.py
    python pipeline/02p5_player_token_cleanup.py \
        --identity_lock_persons_csv inputs/identity_lock/Persons_Truth_Final_v53.csv \
        --identity_lock_placements_csv inputs/identity_lock/Placements_ByPerson_v97.csv
    python pipeline/02p6_structural_cleanup.py
    echo ""
}

# =============================================================================
# STAGE 2: RELEASE — canonical CSV export + platform export
# =============================================================================
run_release() {
    echo ""
    echo "── [2/6] RELEASE (canonical CSVs) ─────────────────────"
    python pipeline/historical/export_historical_csvs.py
    python pipeline/05p5_remediate_canonical.py
    python pipeline/platform/export_canonical_platform.py
    echo ""
}

# =============================================================================
# STAGE 3: SUPPLEMENT — Class B injection into Placements_Flat
# =============================================================================
run_supplement() {
    echo ""
    echo "── [3/6] SUPPLEMENT CLASS B ───────────────────────────"
    python pipeline/02p5b_supplement_class_b.py
    echo ""
}

# =============================================================================
# STAGE 4: QC GATE — validation (hard failures block release)
# =============================================================================
run_qc() {
    echo ""
    echo "── [4/6] QC GATE ──────────────────────────────────────"
    python pipeline/qc/run_qc.py
    echo ""

    echo "── [4b] QC VIEWER ─────────────────────────────────────"
    python pipeline/event_comparison_viewerV13.py
    echo ""
}

# =============================================================================
# STAGE 5: WORKBOOK — Excel release build
# =============================================================================
run_workbook() {
    echo ""
    echo "── [5/6] WORKBOOK ─────────────────────────────────────"
    python pipeline/build_workbook_release.py
    echo ""
}

# =============================================================================
# STAGE 6: SEED CSV BUILD — platform seed CSVs (CSV export only, no DB)
# =============================================================================
run_seed_csv() {
    echo ""
    echo "── [6/6] SEED CSV BUILD ───────────────────────────────"
    python event_results/scripts/07_build_mvfp_seed_full.py
    echo ""
}

# =============================================================================
# Main
# =============================================================================
case "$MODE" in
    canonical_only)
        echo ""
        echo "╔══════════════════════════════════════════════════════╗"
        echo "║  FOOTBAG CSV PIPELINE — canonical_only               ║"
        echo "╚══════════════════════════════════════════════════════╝"
        run_rebuild
        run_release
        run_supplement
        run_qc
        echo ""
        echo "╔══════════════════════════════════════════════════════╗"
        echo "║  canonical_only DONE                                 ║"
        echo "║  Outputs: out/canonical/*.csv                        ║"
        echo "║           event_results/canonical_input/*.csv        ║"
        echo "╚══════════════════════════════════════════════════════╝"
        ;;

    release|full_csv)
        echo ""
        echo "╔══════════════════════════════════════════════════════╗"
        echo "║  FOOTBAG CSV PIPELINE — release                      ║"
        echo "╚══════════════════════════════════════════════════════╝"
        run_rebuild
        run_release
        run_supplement
        run_qc
        run_workbook
        run_seed_csv
        echo ""
        echo "╔══════════════════════════════════════════════════════╗"
        echo "║  release DONE                                        ║"
        echo "║  Outputs: out/canonical/*.csv                        ║"
        echo "║           event_results/canonical_input/*.csv        ║"
        echo "║           event_results/seed/mvfp_full/*.csv         ║"
        echo "║           out/Footbag_Results_Release.xlsx           ║"
        echo "╚══════════════════════════════════════════════════════╝"
        ;;

    *)
        echo "Usage: $0 {canonical_only|release|full_csv}" >&2
        echo "" >&2
        echo "  canonical_only  — rebuild → canonical CSVs → QC" >&2
        echo "  release         — canonical_only + workbook + seed CSVs" >&2
        echo "  full_csv        — alias for release" >&2
        echo "" >&2
        echo "This repo does not support database operations." >&2
        echo "DB loading is handled by footbag-platform/legacy_data." >&2
        exit 1
        ;;
esac
