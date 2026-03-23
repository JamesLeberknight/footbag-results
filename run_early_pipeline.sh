#!/usr/bin/env bash
# run_early_pipeline.sh — Pre-1997 historical footbag data pipeline
#
# Runs the complete pre-1997 recovery pipeline from source data to
# release-ready artifacts. Does NOT touch the post-1997 dataset.
#
# Usage:
#   ./run_early_pipeline.sh [STAGE]
#
# Stages:
#   all         — full pipeline (default)
#   ingest      — stages 04-05: parse Gemini JSON + OLD_RESULTS → flat CSVs
#   canonical   — stage 05: build canonical grouping (event dedup + cross-source)
#   identity    — stage 06: match raw names against Persons_Truth
#   release     — stage 07: build V1 canonical CSVs + Excel workbook
#   review      — stage 08: build human review package (aliases + event groups)
#   apply       — stage 09: apply review decisions → V2 canonical CSVs + workbook
#   feed        — stage 10: build event comparison viewer feed files + render HTML
#   finalize    — stage 11: apply expert corrections → final_pre1997/ + v1.0 workbook
#
# State:
#   The pipeline is currently at: FINALIZED (v1.0)
#   All stages have been run. Re-run specific stages if inputs change.
#
# Input files required:
#   inputs/Persons_Truth.csv
#   early_data/placements/placements_flat.csv
#   early_data/old_results/old_results_placements_flat.csv
#   early_data/event_blocks/event_blocks.csv
#
# Output directories:
#   early_data/canonical/    — working canonical state (V2 after decisions)
#   early_data/final_pre1997/ — release-ready artifacts (v1.0)
#   early_data/out/          — spreadsheets + viewer feeds
#   out/                     — pre-1997 comparison viewer HTML

set -euo pipefail

PYTHON=".venv/bin/python"
STAGE="${1:-all}"

step() { echo; echo ">>> $*"; }

require_venv() {
    if [[ ! -f "$PYTHON" ]]; then
        echo "ERROR: .venv not found. Run: ./run_pipeline.sh setup" >&2
        exit 1
    fi
}

# ── Stage functions ────────────────────────────────────────────────────────────

do_ingest() {
    step "Stage 04: Gemini JSON → event_blocks + placements_flat"
    "$PYTHON" early_data/scripts/04_json_to_csv.py
}

do_canonical() {
    step "Stage 05: Build historical dataset + canonical grouping"
    "$PYTHON" early_data/scripts/05_build_historical_dataset.py
}

do_identity() {
    step "Stage 06: Identity resolution (match raw names → Persons_Truth)"
    "$PYTHON" early_data/scripts/06_identity_resolution.py
}

do_release() {
    step "Stage 07: Build V1 canonical CSVs + Excel workbook"
    "$PYTHON" early_data/scripts/07_build_early_release.py
}

do_review() {
    step "Stage 08: Build human review package"
    "$PYTHON" early_data/scripts/08_build_review_package.py
    echo "  Review files written to early_data/review/"
    echo "  Fill in DECISION columns, then run: ./run_early_pipeline.sh apply"
}

do_apply() {
    step "Stage 09: Apply review decisions → V2"
    "$PYTHON" early_data/scripts/09_apply_decisions.py
}

do_feed() {
    step "Stage 10a: Build event comparison viewer feed files"
    "$PYTHON" early_data/scripts/10_build_early_comparison_feed.py

    step "Stage 10b: Render pre-1997 comparison viewer"
    "$PYTHON" tools/event_comparison_viewerV10.py \
        --stage2 early_data/out/early_stage2_feed.csv \
        --pf     early_data/out/early_placements_feed.csv \
        --output out/event_comparison_viewer_pre1997.html
    echo "  Viewer → out/event_comparison_viewer_pre1997.html"
}

do_finalize() {
    step "Stage 11: Finalize pre-1997 v1.0 release artifacts"
    "$PYTHON" early_data/scripts/11_finalize_pre1997.py
    echo "  Release artifacts → early_data/final_pre1997/"
    echo "  Workbook → early_data/out/footbag_results_pre1997_v1.xlsx"
}

# ── Dispatch ───────────────────────────────────────────────────────────────────

require_venv

case "$STAGE" in
    ingest)    do_ingest ;;
    canonical) do_canonical ;;
    identity)  do_identity ;;
    release)   do_release ;;
    review)    do_review ;;
    apply)     do_apply ;;
    feed)      do_feed ;;
    finalize)  do_finalize ;;
    all)
        do_ingest
        do_canonical
        do_identity
        do_release
        do_review
        echo
        echo ">>> PAUSE: Fill in review decisions in early_data/review/ then re-run:"
        echo "    ./run_early_pipeline.sh apply"
        echo "    ./run_early_pipeline.sh feed"
        echo "    ./run_early_pipeline.sh finalize"
        ;;
    *)
        echo "Unknown stage: $STAGE"
        echo "Usage: $0 [all|ingest|canonical|identity|release|review|apply|feed|finalize]"
        exit 1
        ;;
esac
