#!/usr/bin/env bash
# =============================================================================
# check_parity.sh — FOOTBAG_DATA vs footbag-platform output parity check
#
# Compares canonical CSV outputs from FOOTBAG_DATA against the authoritative
# outputs in footbag-platform/legacy_data.
#
# Checks:
#   1. Row counts
#   2. Column structure (header match)
#   3. Sorted content hash (md5)
#
# Usage:
#   ./check_parity.sh [FP_PATH]
#
# FP_PATH defaults to ~/projects/footbag-platform/legacy_data
# =============================================================================
set -euo pipefail

FD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FP="${1:-$HOME/projects/footbag-platform/legacy_data}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass=0
fail=0
warn=0

check_file() {
    local rel_path="$1"
    local fp_file="$FP/$rel_path"
    local fd_file="$FD/$rel_path"

    if [[ ! -f "$fp_file" ]]; then
        echo -e "  ${YELLOW}SKIP${NC}: $rel_path (not in FP)"
        ((warn++))
        return
    fi
    if [[ ! -f "$fd_file" ]]; then
        echo -e "  ${RED}FAIL${NC}: $rel_path (missing in FD)"
        ((fail++))
        return
    fi

    # Row count
    local fp_rows fd_rows
    fp_rows=$(wc -l < "$fp_file")
    fd_rows=$(wc -l < "$fd_file")

    # Header match
    local fp_header fd_header
    fp_header=$(head -1 "$fp_file")
    fd_header=$(head -1 "$fd_file")

    # Sorted content hash
    local fp_hash fd_hash
    fp_hash=$(sort "$fp_file" | md5sum | cut -d' ' -f1)
    fd_hash=$(sort "$fd_file" | md5sum | cut -d' ' -f1)

    if [[ "$fp_hash" == "$fd_hash" ]]; then
        echo -e "  ${GREEN}PASS${NC}: $rel_path (rows=$fp_rows, hash match)"
        ((pass++))
    elif [[ "$fp_rows" != "$fd_rows" ]]; then
        echo -e "  ${RED}FAIL${NC}: $rel_path"
        echo "         rows: FP=$fp_rows FD=$fd_rows"
        echo "         hash: FP=$fp_hash FD=$fd_hash"
        ((fail++))
    elif [[ "$fp_header" != "$fd_header" ]]; then
        echo -e "  ${RED}FAIL${NC}: $rel_path"
        echo "         rows match ($fp_rows) but HEADERS DIFFER"
        echo "         FP: $fp_header"
        echo "         FD: $fd_header"
        ((fail++))
    else
        echo -e "  ${RED}FAIL${NC}: $rel_path"
        echo "         rows match ($fp_rows), headers match, but CONTENT DIFFERS"
        echo "         hash: FP=$fp_hash FD=$fd_hash"
        # Show first diff
        diff <(sort "$fp_file") <(sort "$fd_file") | head -10
        ((fail++))
    fi
}

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  PARITY CHECK: FOOTBAG_DATA vs footbag-platform      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "FP: $FP"
echo "FD: $FD"
echo ""

echo "── Canonical CSVs (out/canonical/) ────────────────────"
for f in events.csv event_disciplines.csv event_results.csv event_result_participants.csv persons.csv; do
    check_file "out/canonical/$f"
done

echo ""
echo "── Platform Export (event_results/canonical_input/) ────"
for f in events.csv event_disciplines.csv event_results.csv event_result_participants.csv persons.csv; do
    check_file "event_results/canonical_input/$f"
done

echo ""
echo "── Seed CSVs (event_results/seed/mvfp_full/) ──────────"
for f in seed_events.csv seed_event_disciplines.csv seed_event_results.csv seed_event_result_participants.csv seed_persons.csv; do
    check_file "event_results/seed/mvfp_full/$f"
done

echo ""
echo "══════════════════════════════════════════════════════"
echo -e "  ${GREEN}PASS${NC}: $pass"
echo -e "  ${RED}FAIL${NC}: $fail"
echo -e "  ${YELLOW}SKIP${NC}: $warn"
echo "══════════════════════════════════════════════════════"

if [[ $fail -gt 0 ]]; then
    echo -e "\n${RED}PARITY CHECK FAILED${NC}"
    exit 1
else
    echo -e "\n${GREEN}PARITY CHECK PASSED${NC}"
fi
