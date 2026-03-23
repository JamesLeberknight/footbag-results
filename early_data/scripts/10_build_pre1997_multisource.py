from __future__ import annotations
from pathlib import Path
import json
import csv
import re
import hashlib
from collections import defaultdict

BASE = Path("early_data")
REVIEW_DIR = BASE / "review"
OUT_DIR = BASE / "normalized"
OUT_DIR.mkdir(exist_ok=True)

POST1997_CUTOFF = 1997


def stable_hash(text: str, n: int = 10) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:n]


def safe_year(date_raw: str) -> str:
    if not date_raw:
        return ""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", str(date_raw))
    return m.group(1) if m else ""


def source_type_from_file(source_file: str) -> str:
    s = (source_file or "").upper()
    if "IFAB" in s:
        return "IFAB"
    return "FBW"


def normalize_event_type(name: str) -> str:
    s = (name or "").lower()
    if "world" in s and "champ" in s:
        return "WORLD_CHAMPIONSHIPS"
    if "europe" in s and "champ" in s:
        return "EURO_CHAMPIONSHIPS"
    if "u.s." in s or "us national" in s or "national footbag championships" in s:
        return "US_NATIONALS"
    return "OTHER"


def make_event_id(source_file: str, event_name_raw: str, date_raw: str) -> str:
    key = f"{source_file}|{event_name_raw}|{date_raw}"
    return stable_hash(key, 12)


def load_gemini_json_files() -> tuple[list[dict], list[dict]]:
    event_blocks = []
    placements = []

    for fp in sorted(REVIEW_DIR.glob("gemini_batch_*.json")):
        data = json.loads(fp.read_text(encoding="utf-8"))
        for page_obj in data:
            source_file = page_obj.get("source_file", "")
            source_type = source_type_from_file(source_file)

            for ev in page_obj.get("events", []):
                event_name_raw = ev.get("event_name_raw", "")
                date_raw = ev.get("date_raw", "")
                location_raw = ev.get("location_raw", "")
                year = safe_year(date_raw)
                event_id = make_event_id(source_file, event_name_raw, date_raw)

                event_blocks.append({
                    "event_id": event_id,
                    "event_name_raw": event_name_raw,
                    "date_raw": date_raw,
                    "year": year,
                    "location_raw": location_raw,
                    "source_file": source_file,
                    "source_type": source_type,
                    "normalized_event_type": normalize_event_type(event_name_raw),
                    "exclude_pre1997": "TRUE" if year and int(year) >= POST1997_CUTOFF else "FALSE",
                })

                for div in ev.get("divisions", []):
                    division_raw = div.get("division_raw", "")
                    for r in div.get("results", []):
                        placements.append({
                            "event_id": event_id,
                            "division_raw": division_raw,
                            "placement_raw": r.get("placement_raw", ""),
                            "placement_num": r.get("placement_num", ""),
                            "player_raw": r.get("player_raw", ""),
                            "team_raw": r.get("team_raw", ""),
                            "score_raw": r.get("score_raw", ""),
                            "notes": r.get("notes", ""),
                            "source_file": source_file,
                            "source_type": source_type,
                        })
    return event_blocks, placements


def parse_old_results_text(text: str) -> tuple[list[dict], list[dict]]:
    # Start simple. This parser can be improved incrementally.
    event_blocks = []
    placements = []

    current_event = None
    current_division = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Event header like "1983 NHSA:"
        m_event = re.match(r"^(19\d{2})\s+(.+?):\s*$", line)
        if m_event:
            year, name = m_event.groups()
            event_name_raw = f"{year} {name}".strip()
            current_event = {
                "event_id": make_event_id("OLD_RESULTS.txt", event_name_raw, year),
                "event_name_raw": event_name_raw,
                "date_raw": year,
                "year": year,
                "location_raw": "",
                "source_file": "OLD_RESULTS.txt",
                "source_type": "OLD_RESULTS",
                "normalized_event_type": normalize_event_type(event_name_raw),
                "exclude_pre1997": "TRUE" if int(year) >= POST1997_CUTOFF else "FALSE",
            }
            event_blocks.append(current_event)
            current_division = None
            continue

        # Division like "Singles Net - 1st - John ..."
        if current_event and " - 1st -" in line:
            parts = line.split(" - ")
            current_division = parts[0].strip()

            # Parse placements from one-line format
            chunks = re.findall(r"(\d+(?:st|nd|rd|th)?)\s*-\s*([^,]+(?:,[^,]+)*)", line)
            if not chunks:
                first = re.search(r"^(.*?)\s*-\s*(1st|2nd|3rd|4th|5th)\s*-\s*(.+)$", line)
                if first:
                    chunks = [(first.group(2), first.group(3))]

            for placement_raw, entrant in chunks:
                placements.append({
                    "event_id": current_event["event_id"],
                    "division_raw": current_division,
                    "placement_raw": placement_raw.strip(),
                    "placement_num": re.sub(r"\D", "", placement_raw),
                    "player_raw": entrant.strip() if "/" not in entrant else "",
                    "team_raw": entrant.strip() if "/" in entrant else "",
                    "score_raw": "",
                    "notes": "",
                    "source_file": "OLD_RESULTS.txt",
                    "source_type": "OLD_RESULTS",
                })
            continue

        # Simple division-only headers
        if current_event and not re.match(r"^\d", line):
            current_division = line

    return event_blocks, placements


def build_event_groups(all_event_blocks: list[dict]) -> list[dict]:
    buckets = defaultdict(list)
    for ev in all_event_blocks:
        key = (ev["normalized_event_type"], ev["year"])
        buckets[key].append(ev)

    rows = []
    for (etype, year), group in buckets.items():
        if not year or etype == "OTHER":
            # conservative: don't auto-group OTHER or no-year events
            for ev in group:
                gid = f"UNGROUPED_{ev['event_id']}"
                rows.append({
                    "group_id": gid,
                    "normalized_event_type": ev["normalized_event_type"],
                    "year": ev["year"],
                    "candidate_event_ids": ev["event_id"],
                    "source_types": ev["source_type"],
                    "confidence": "LOW",
                    "notes": "Left ungrouped conservatively",
                })
            continue

        gid = f"{etype}_{year}"
        source_types = sorted({ev["source_type"] for ev in group})
        rows.append({
            "group_id": gid,
            "normalized_event_type": etype,
            "year": year,
            "candidate_event_ids": "|".join(ev["event_id"] for ev in group),
            "source_types": "|".join(source_types),
            "confidence": "MEDIUM" if len(group) == 1 else "HIGH",
            "notes": "",
        })
    return rows


def build_canonical_events(event_groups: list[dict]) -> list[dict]:
    out = []
    for g in event_groups:
        out.append({
            "canonical_event_id": g["group_id"],
            "normalized_event_type": g["normalized_event_type"],
            "year": g["year"],
            "source_count": len(g["source_types"].split("|")),
            "source_types": g["source_types"],
        })
    return out


def build_event_id_mapping(event_groups: list[dict], event_blocks: list[dict]) -> list[dict]:
    event_lookup = {e["event_id"]: e for e in event_blocks}
    rows = []
    for g in event_groups:
        for eid in g["candidate_event_ids"].split("|"):
            ev = event_lookup[eid]
            rows.append({
                "event_id": eid,
                "canonical_event_id": g["group_id"],
                "source_file": ev["source_file"],
                "source_type": ev["source_type"],
            })
    return rows


def build_comparison(placements: list[dict], mapping: list[dict]) -> list[dict]:
    map_lookup = {m["event_id"]: m["canonical_event_id"] for m in mapping}
    rows = []
    for p in placements:
        rows.append({
            "canonical_event_id": map_lookup.get(p["event_id"], ""),
            "division_raw": p["division_raw"],
            "placement_raw": p["placement_raw"],
            "placement_num": p["placement_num"],
            "player_raw": p["player_raw"],
            "team_raw": p["team_raw"],
            "score_raw": p["score_raw"],
            "source_type": p["source_type"],
            "source_file": p["source_file"],
            "validation_status": "",  # fill below if desired
        })

    sig_counts = defaultdict(set)
    for r in rows:
        sig = (
            r["canonical_event_id"], r["division_raw"], str(r["placement_num"]),
            r["player_raw"], r["team_raw"], r["score_raw"]
        )
        sig_counts[sig].add(r["source_type"])

    for r in rows:
        sig = (
            r["canonical_event_id"], r["division_raw"], str(r["placement_num"]),
            r["player_raw"], r["team_raw"], r["score_raw"]
        )
        source_count = len(sig_counts[sig])
        if source_count >= 2:
            r["validation_status"] = "CONFIRMED_MULTI_SOURCE"
        else:
            r["validation_status"] = "SINGLE_SOURCE"

    return rows


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main():
    gem_events, gem_places = load_gemini_json_files()

    old_text = (BASE.parent / "OLD_RESULTS.txt").read_text(encoding="utf-8")
    old_events, old_places = parse_old_results_text(old_text)

    write_csv(OUT_DIR / "event_blocks.csv", gem_events)
    write_csv(OUT_DIR / "placements_flat.csv", gem_places)
    write_csv(OUT_DIR / "old_results_event_blocks.csv", old_events)
    write_csv(OUT_DIR / "old_results_placements_flat.csv", old_places)

    all_events = gem_events + old_events
    all_places = gem_places + old_places

    event_groups = build_event_groups(all_events)
    canonical_events = build_canonical_events(event_groups)
    event_mapping = build_event_id_mapping(event_groups, all_events)
    comparison = build_comparison(all_places, event_mapping)

    write_csv(OUT_DIR / "event_groups.csv", event_groups)
    write_csv(OUT_DIR / "canonical_events.csv", canonical_events)
    write_csv(OUT_DIR / "event_id_mapping.csv", event_mapping)
    write_csv(OUT_DIR / "event_source_comparison.csv", comparison)

    print("Done.")


if __name__ == "__main__":
    main()
