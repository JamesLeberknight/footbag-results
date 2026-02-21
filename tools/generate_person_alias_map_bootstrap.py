from pathlib import Path
import csv

# Resolve paths relative to repo root (parent of tools/)
REPO_ROOT = Path(__file__).resolve().parent.parent
OUT = REPO_ROOT / "out/person_alias_map_bootstrap.csv"
STAGE2P5_PLAYERS = REPO_ROOT / "out/stage2p5_players_clean.csv"

HEADER = [
    "alias_group_id",
    "player_id",
    "player_name_raw",
    "alias_evidence",
    "alias_confidence",
    "alias_decided_by",
    "alias_decided_at",
    "notes",
]


def _rows_from_stage2p5():
    """Yield data rows from stage2p5_players_clean.csv for bootstrap (one per player_id in each name_key group)."""
    if not STAGE2P5_PLAYERS.exists():
        return
    with STAGE2P5_PLAYERS.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fn = r.fieldnames or []
        if "player_id" not in fn:
            return
        name_key_col = "name_key" if "name_key" in fn else None
        name_status_col = "name_status" if "name_status" in fn else None
        for row in rows:
            pid = (row.get("player_id") or "").strip()
            if not pid:
                continue
            if name_status_col and (row.get(name_status_col) or "").strip().lower() == "junk":
                continue
            # Use name_key as alias_group_id so all same-name tokens map to one group
            if name_key_col:
                agid = (row.get(name_key_col) or "").strip()
            else:
                agid = (row.get("player_name_clean") or row.get("player_name_raw") or "").strip()
            if not agid:
                continue
            raw = (row.get("player_name_raw") or "").strip()
            yield [agid, pid, raw, "", "", "", "", ""]


def main():
    if OUT.exists():
        print(f"Already exists: {OUT}")
        return

    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = list(_rows_from_stage2p5())

    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)

    if rows:
        print(f"Wrote alias bootstrap file: {OUT} ({len(rows)} rows)")
    else:
        print(f"Wrote empty alias bootstrap file: {OUT}")
    print("Populate ONLY with human-verified mappings.")


if __name__ == "__main__":
    main()
