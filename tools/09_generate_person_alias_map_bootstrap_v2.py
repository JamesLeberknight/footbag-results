#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


OUT_HEADER = [
    "alias_group_key",
    "alias_name",
    "suggested_canonical_player_id",
    "suggested_canonical_name",
    "confidence",
    "usage_count_total",
    "player_ids_in_group",
    "countries_seen",
    "decision",
    "notes",
]


def _clean_str(s: str) -> str:
    return (s or "").strip()


def _join_sorted(items) -> str:
    items = [x for x in items if x]
    return " | ".join(sorted(set(items), key=lambda x: x.lower()))


# Paths relative to repo root (parent of tools/) so script works from any cwd
_REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Generate person_alias_map_bootstrap.csv (v2) from stage2p5_players_clean.csv, "
                    "excluding junk. Use --min-ids 2 to keep only multi-player-id groups."
    )
    ap.add_argument(
        "--players",
        default="out/stage2p5_players_clean.csv",
        help="Input stage2p5 players clean CSV",
    )
    ap.add_argument(
        "--out",
        default="out/person_alias_map_bootstrap.csv",
        help="Output bootstrap alias map CSV",
    )
    ap.add_argument(
        "--min-ids",
        dest="min_ids",
        type=int,
        default=1,
        help="Minimum distinct player_ids per group to emit (default: 1). Use 2 for multi-id groups only.",
    )
    ap.add_argument(
        "--min_usage_total",
        type=int,
        default=1,
        help="Minimum total usage_count per group to emit (default: 1). "
             "Set 0 if you want to see never-used tokens (usually noise).",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    players_path = (_REPO_ROOT / args.players).resolve()
    out_path = (_REPO_ROOT / args.out).resolve()

    if not players_path.exists():
        raise SystemExit(f"Missing input: {players_path}")

    # Group by name_key (already computed by stage2p5).
    # Only include non-junk rows.
    groups: Dict[str, List[dict]] = defaultdict(list)

    with players_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        required_cols = {"player_id", "player_name_raw", "player_name_clean", "name_status", "usage_count", "name_key"}
        missing = required_cols - set(r.fieldnames or [])
        if missing:
            raise SystemExit(f"{players_path} missing required columns: {sorted(missing)}")

        for row in r:
            status = _clean_str(row.get("name_status", ""))
            if status.lower() == "junk":
                continue

            name_key = _clean_str(row.get("name_key", ""))
            if not name_key:
                continue

            # Defensive: require clean name (otherwise these are usually headings / artifacts)
            name_clean = _clean_str(row.get("player_name_clean", ""))
            if not name_clean:
                continue

            groups[name_key].append(row)

    out_rows: List[dict] = []

    for name_key, rows in groups.items():
        player_ids = sorted({_clean_str(r["player_id"]) for r in rows if _clean_str(r.get("player_id", ""))})
        if len(player_ids) < args.min_ids:
            continue

        # usage_count_total
        usage_total = 0
        for r in rows:
            u = _clean_str(r.get("usage_count", "0"))
            try:
                usage_total += int(u)
            except ValueError:
                # If malformed, treat as 0 but do not crash
                pass

        if usage_total < args.min_usage_total:
            continue

        # Countries seen (if present)
        countries = []
        for r in rows:
            c = _clean_str(r.get("country_clean", ""))
            if c:
                countries.append(c)
        countries_seen = _join_sorted(countries)

        # Suggested canonical is the highest-usage player_id in the group.
        # This is NOT identity inference; it is just "best representative token".
        def usage_of(r: dict) -> int:
            try:
                return int(_clean_str(r.get("usage_count", "0")) or "0")
            except ValueError:
                return 0

        best_row = max(rows, key=usage_of)
        suggested_pid = _clean_str(best_row.get("player_id", ""))
        suggested_name = _clean_str(best_row.get("player_name_clean", "")) or _clean_str(best_row.get("player_name_raw", ""))

        # Confidence is only about string normalization consistency, NOT about real-person identity.
        # high: all player_name_clean identical (byte-for-byte) within the group
        # med : otherwise
        clean_set = {(_clean_str(r.get("player_name_clean", ""))) for r in rows if _clean_str(r.get("player_name_clean", ""))}
        confidence = "high" if len(clean_set) == 1 else "med"

        # alias_name: the suggested canonical name (purely a label for the group)
        alias_name = suggested_name

        # Notes: show distinct raw variants (helps review)
        raw_variants = _join_sorted([_clean_str(r.get("player_name_raw", "")) for r in rows])

        out_rows.append({
            "alias_group_key": name_key,
            "alias_name": alias_name,
            "suggested_canonical_player_id": suggested_pid,
            "suggested_canonical_name": suggested_name,
            "confidence": confidence,
            "usage_count_total": str(usage_total),
            "player_ids_in_group": " | ".join(player_ids),
            "countries_seen": countries_seen,
            "decision": "",   # human-filled later
            "notes": raw_variants,
        })

    # Stable sort for determinism:
    out_rows.sort(key=lambda d: (d["alias_group_key"].lower(), -int(d["usage_count_total"])))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUT_HEADER)
        w.writeheader()
        w.writerows(out_rows)

    print(f"Wrote: {out_path}")
    print(f"Groups emitted (min_ids>={args.min_ids}, min_usage_total>={args.min_usage_total}): {len(out_rows)}")


if __name__ == "__main__":
    main()
