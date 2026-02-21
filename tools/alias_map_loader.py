"""
Read-only alias map: player_id -> alias_group_id.

CSV format: player_id, alias_group_id (blank = unknown / not yet reviewed).
No guessing. No transitive closure.
"""

from __future__ import annotations

import csv
from pathlib import Path


def load_alias_map(path: str | Path) -> dict[str, str]:
    """
    Read-only alias map.
    Returns: dict[player_id] -> alias_group_id

    No guessing.
    No transitive closure.
    Blank means 'unknown / not yet reviewed'.
    """
    alias_map = {}
    p = Path(path)

    if not p.exists():
        return alias_map

    with p.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            pid = row.get("player_id", "").strip()
            agid = row.get("alias_group_id", "").strip()

            if pid and agid:
                alias_map[pid] = agid

    return alias_map
