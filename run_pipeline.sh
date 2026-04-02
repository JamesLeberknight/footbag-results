#!/usr/bin/env bash
# run_pipeline.sh — Footbag Results Pipeline runner
#
# ============================================================
# PIPELINE LANES
# ============================================================
#
#   POST-1997 PRODUCTION (this script, rebuild + release + qc)
#     Source: mirror only (www.footbag.org HTML archive)
#     Output: out/canonical/*.csv  — authoritative relational dataset
#
#   PRE-1997 HISTORICAL  (./run_early_pipeline.sh)
#     Sources: FBW magazine scans, OLD_RESULTS.txt, Gemini JSON extractions
#     Output: early_data/canonical/*_pre1997.csv
#
#   MERGED BUILD         (this script, merged mode)
#     Combines post-1997 + pre-1997 canonical outputs
#     Requires both pipelines to have completed first
#     Output: out/canonical_all/*.csv, merged workbook, merged viewer
#
#   AUXILIARY / REFERENCE (inline as needed)
#     Consecutives reference data — consumed by stage 04B
#     Enrichment, honors, merged viewer support
#
# ============================================================
# USAGE
# ============================================================
#
#   ./run_pipeline.sh [MODE]
#
# Modes:
#   setup    — Create .venv, install dependencies, create out/ directory
#   rebuild  — Parse HTML mirror → canonical stage-2 events  (stages 01, 01c, 02)
#              Mirror-only: does NOT run OLD_RESULTS / FBW / magazine ingestion.
#              Those belong to the pre-1997 pipeline (run_early_pipeline.sh).
#   release  — Apply identity lock → workbooks + canonical CSVs (stages 02p5–05p5)
#              Order: 02p5 → 02p6 → 03 → 04 → [01b1 aux] → 04B → 05 → 05p5
#   qc       — Run all QC checks (master, post-release, schema/logic)
#   merged   — Build merged canonical_all/ dataset + workbook + viewer
#              Requires: run_pipeline.sh release + run_early_pipeline.sh finalize
#              Requires: out/canonical_all_union/ (from early_data/scripts/12_*)
#   all      — Full post-1997 pipeline: rebuild → release → qc  [default]
#   pre1997  — Build pre-1997 comparison feed → viewer HTML
#              Reads early_data/canonical/*.csv (does NOT touch post-1997 release)
#
# Examples:
#   ./run_pipeline.sh setup
#   ./run_pipeline.sh rebuild
#   ./run_pipeline.sh release
#   ./run_pipeline.sh qc
#   ./run_pipeline.sh all
#   ./run_pipeline.sh pre1997
#   ./run_pipeline.sh merged

set -euo pipefail

PYTHON=".venv/bin/python"
MODE="${1:-all}"

# ── Helpers ────────────────────────────────────────────────────────────────────

step() { echo; echo ">>> $*"; }

require_venv() {
    if [[ ! -f "$PYTHON" ]]; then
        echo "ERROR: .venv not found. Run: ./run_pipeline.sh setup" >&2
        exit 1
    fi
}

require_mirror() {
    if [[ ! -d "mirror" ]]; then
        echo "ERROR: mirror/ directory not found." >&2
        echo "       Obtain mirror.tar.gz from the GitHub Release assets and extract it:" >&2
        echo "         tar -xzf mirror.tar.gz" >&2
        echo "       Or, if you have mirror_full/:" >&2
        echo "         ln -s mirror_full mirror" >&2
        exit 1
    fi
}

require_stage2() {
    if [[ ! -f "out/stage2_canonical_events.csv" ]]; then
        echo "ERROR: out/stage2_canonical_events.csv not found." >&2
        echo "       Run: ./run_pipeline.sh rebuild" >&2
        exit 1
    fi
}

require_canonical_all_union() {
    if [[ ! -d "out/canonical_all_union" ]]; then
        echo "ERROR: out/canonical_all_union/ not found." >&2
        echo "       This directory is built by early_data/scripts/12_build_enrichment_and_merged.py" >&2
        echo "       Run: ./run_early_pipeline.sh finalize   (if not already done)" >&2
        echo "       Then: python3 early_data/scripts/12_build_enrichment_and_merged.py" >&2
        exit 1
    fi
}

# ── Modes ──────────────────────────────────────────────────────────────────────

do_setup() {
    step "Setting up virtual environment…"
    python3 -m venv .venv
    .venv/bin/pip install --quiet -r requirements.txt
    mkdir -p out
    echo "Setup complete. Run ./run_pipeline.sh rebuild to parse the mirror."
}

do_rebuild() {
    require_venv
    require_mirror

    # POST-1997 PRODUCTION REBUILD — mirror data only
    # OLD_RESULTS / FBW / magazine ingestion are PRE-1997 pipeline concerns
    # (see run_early_pipeline.sh). Stage 01c gracefully skips any missing
    # non-mirror source files, so this produces a mirror-only merged stage 1.

    step "Stage 01: parse HTML mirror → stage1_raw_events_mirror.csv"
    "$PYTHON" pipeline/01_parse_mirror.py

    step "Stage 01c: merge stage-1 sources (mirror-only in production)"
    "$PYTHON" pipeline/01c_merge_stage1.py

    step "Stage 02: canonicalize results → stage2_canonical_events.csv"
    "$PYTHON" pipeline/02_canonicalize_results.py

    echo
    echo "Rebuild complete. Run ./run_pipeline.sh release to produce final outputs."
}

do_release() {
    require_venv
    require_stage2

    step "Stage 02p5: apply identity lock (PT v49 / PBP v91)"
    "$PYTHON" pipeline/02p5_player_token_cleanup.py \
        --identity_lock_placements_csv inputs/identity_lock/Placements_ByPerson_v91.csv \
        --persons_truth_csv            inputs/identity_lock/Persons_Truth_Final_v49.csv \
        --out_dir                      out

    step "Stage 02p6: structural cleanup (artifact removal + pool-shadow fixes)"
    "$PYTHON" pipeline/02p6_structural_cleanup.py

    # Release workbook / export order (do not reorder without cause):
    #   03 → 04 → [01b1 aux ref] → 04B → 05 → 05p5
    step "Stage 03: build canonical Excel workbook"
    "$PYTHON" pipeline/03_build_excel.py

    step "Stage 04: build analytics + coverage flags + identity lock sentinel"
    "$PYTHON" pipeline/04_build_analytics.py

    # AUXILIARY — merge consecutives reference data before community workbook
    # 01b1 is not event ingestion; it merges trick-record reference CSVs
    # consumed by stage 04B (Trick Records sheet).
    step "Stage 01b1 (auxiliary): merge consecutives reference data"
    "$PYTHON" pipeline/01b1_merge_consecutives.py

    step "Stage 04B: build community Excel workbook"
    "$PYTHON" tools/build_final_workbook_v13.py

    step "Stage 05: export relational CSV files → out/canonical/"
    "$PYTHON" pipeline/05_export_canonical_csv.py

    step "Stage 05p5: remediate canonical CSVs (final integrity pass)"
    "$PYTHON" pipeline/05p5_remediate_canonical.py

    echo
    echo "Release complete."
    echo "  Canonical CSVs: out/canonical/"
    echo "  Community workbook: Footbag_Results_Community_FINAL_v13.xlsx"
}

do_qc() {
    require_venv
    require_stage2

    step "QC: stage-2 and stage-3 integrity"
    "$PYTHON" qc/qc_master.py

    step "QC: post-release data integrity (6 checks)"
    "$PYTHON" tools/32_post_release_qc.py

    step "QC: schema and logic consistency (7 checks)"
    "$PYTHON" tools/33_schema_logic_qc.py

    echo
    echo "QC complete."
}

do_merged() {
    require_venv
    require_canonical_all_union

    # MERGED BUILD — combines post-1997 canonical + pre-1997 canonical
    # Requires both pipelines to have completed:
    #   ./run_pipeline.sh all
    #   ./run_early_pipeline.sh finalize
    #   python3 early_data/scripts/12_build_enrichment_and_merged.py

    step "Merged: apply overlap suppression → out/canonical_all/"
    "$PYTHON" tools/build_appsafe_merged.py

    step "Merged: generate merged feed files"
    "$PYTHON" tools/build_merged_feeds.py

    step "Merged: build merged Excel workbook"
    "$PYTHON" tools/build_merged_workbook_v14.py

    step "Merged: render merged event comparison viewer"
    "$PYTHON" tools/event_comparison_viewerV10.py

    echo
    echo "Merged build complete."
    echo "  Canonical dataset: out/canonical_all/"
    echo "  Merged workbook:   Footbag_Results_Merged_FINAL.xlsx"
    echo "  Merged viewer:     out/merged_event_viewer.html"
}

do_pre1997() {
    require_venv

    step "Pre-1997: build early comparison feed"
    "$PYTHON" early_data/scripts/10_build_early_comparison_feed.py

    step "Pre-1997: render event comparison viewer"
    "$PYTHON" tools/event_comparison_viewerV10.py \
        --stage2 early_data/out/early_stage2_feed.csv \
        --pf     early_data/out/early_placements_feed.csv \
        --output out/event_comparison_viewer_pre1997.html

    echo
    echo "Pre-1997 comparison viewer → out/event_comparison_viewer_pre1997.html"
}

# ── Dispatch ───────────────────────────────────────────────────────────────────

case "$MODE" in
    setup)   do_setup   ;;
    rebuild) do_rebuild ;;
    release) do_release ;;
    qc)      do_qc      ;;
    merged)  do_merged  ;;
    pre1997) do_pre1997 ;;
    all)
        do_rebuild
        do_release
        do_qc
        echo
        echo "Full post-1997 pipeline complete."
        echo "To build the merged 1980-present dataset, run:"
        echo "  ./run_early_pipeline.sh finalize   (if not already done)"
        echo "  python3 early_data/scripts/12_build_enrichment_and_merged.py"
        echo "  ./run_pipeline.sh merged"
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: $0 [setup|rebuild|release|qc|merged|all|pre1997]"
        exit 1
        ;;
esac
