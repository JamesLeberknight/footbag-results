#!/usr/bin/env python3
"""
master.py — Minimal Footbag mirror → Excel (one sheet per year)

Option A implemented:
Extract event metadata (date/location/event_type/host_club)
directly from the mirrored HTML DOM (no canonical/backfill).

This script:
- Reads local offline mirror under ./mirror (or --mirror)
- Extracts event_id, event_name, date, location, event_type, host_club, url, results_text
- Parses placements from <pre class="eventsPre"> (light heuristics only)
- Writes one Excel workbook with one sheet per year + unknown_year

No intermediate CSV files.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from bs4 import BeautifulSoup


# Excel/openpyxl rejects control chars: 0x00-0x08, 0x0B-0x0C, 0x0E-0x1F
_ILLEGAL_XLSX_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def sanitize_excel_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Required to write .xlsx safely (not semantic cleaning)."""
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_string_dtype(out[col]) or out[col].dtype == object:
            out[col] = out[col].apply(
                lambda v: _ILLEGAL_XLSX_RE.sub("", v) if isinstance(v, str) else v
            )
    return out


# Stable UUID namespace for players
NAMESPACE_PLAYERS = uuid.UUID("12345678-1234-5678-1234-567812345678")


def stable_uuid(ns: uuid.UUID, s: str) -> str:
    return str(uuid.uuid5(ns, s))


# ------------------------------------------------------------
# Mirror discovery
# ------------------------------------------------------------
def find_events_show_dir(mirror_dir: Path) -> Path:
    mirror_dir = mirror_dir.resolve()
    candidates = [
        mirror_dir / "www.footbag.org" / "events" / "show",
        mirror_dir / "events" / "show",
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(f"No events/show directory found under {mirror_dir}")


def iter_event_html_files(events_show: Path) -> Iterable[Path]:
    for subdir in sorted(events_show.iterdir()):
        if not subdir.is_dir():
            continue
        if not subdir.name.isdigit():
            continue

        html_file = subdir / "index.html"
        if not html_file.exists():
            html_file = subdir / f"{subdir.name}.html"

        if html_file.exists():
            yield html_file.resolve()


# ------------------------------------------------------------
# Lightweight results parsing
# ------------------------------------------------------------
DIVISION_KEYWORDS = {
    "open", "pro", "women", "womens", "men", "mens",
    "net", "freestyle", "circle", "shred",
    "doubles", "singles", "mixed",
    "consecutive", "routine",
    "golf", "distance", "accuracy",
}


def looks_like_division_header(line: str) -> bool:
    low = line.lower()
    if len(line) > 70:
        return False
    if "place" in low:
        return False
    if re.match(r"^\d+\s*[.)]\s+\S", line):
        return False
    return any(k in low for k in DIVISION_KEYWORDS)


def parse_results_text(results_text: str, event_id: str) -> list[dict]:
    """Very light heuristic: division headers + numbered placements."""
    rows = []
    division = "Unknown"

    place_re = re.compile(r"^\s*(\d{1,3})\s*[.)\-:]?\s*(.+)$")

    for raw_line in (results_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if looks_like_division_header(line):
            division = line.rstrip(":")
            continue

        m = place_re.match(line)
        if m:
            rows.append(
                {
                    "event_id": event_id,
                    "division": division,
                    "place": int(m.group(1)),
                    "entry_raw": m.group(2).strip(),
                }
            )

    return rows


def split_entry(entry: str):
    """Detect doubles teams separated by '/'."""
    entry = " ".join(entry.split()).strip()
    if "/" in entry:
        a, b = entry.split("/", 1)
        return a.strip(), b.strip(), "team"
    return entry, None, "player"


def format_results_from_placements(pl_sub: pd.DataFrame) -> Optional[str]:
    """
    Build a deterministic, consistent results blob from canonical placements.
    Format matches the archive workbook style:
      DIVISION
      1. Name
      2. Name / Name

    We do NOT invent missing facts. If no placements exist -> None.
    """
    if pl_sub is None or pl_sub.empty:
        return None

    # Normalize minimal columns we rely on (do not change upstream schema)
    df = pl_sub.copy()

    # Ensure numeric sort on place where possible
    if "place" in df.columns:
        df["place_sort"] = pd.to_numeric(df["place"], errors="coerce")
    else:
        df["place_sort"] = pd.NA

    # Build a canonical display name per row
    def _row_name(r) -> str:
        p1 = (r.get("player1_name") or "").strip()
        p2 = (r.get("player2_name") or "")
        p2 = p2.strip() if isinstance(p2, str) else ""
        if p2:
            return f"{p1} / {p2}"
        return p1

    df["name_line"] = df.apply(_row_name, axis=1)

    # Keep only rows that have something to print
    df = df[df["name_line"].astype(str).str.strip().ne("")]

    # Division grouping
    if "division" not in df.columns:
        # If division is missing, we still print something deterministic
        df["division"] = "Unknown"

    # Stable ordering: division (case-insensitive), then place, then name
    df["division_sort"] = df["division"].astype(str).str.casefold()

    out_lines: list[str] = []
    for div, g in df.sort_values(["division_sort", "place_sort", "name_line"]).groupby("division", sort=False):
        div_title = (str(div) if div is not None else "Unknown").strip()
        if not div_title:
            div_title = "Unknown"

        out_lines.append(div_title.upper())
        for _, r in g.iterrows():
            place = r.get("place")
            try:
                place_int = int(place)
                place_txt = f"{place_int}."
            except Exception:
                # If place isn't usable, keep raw but don't guess
                place_txt = f"{place}." if place is not None else ""
            name = r["name_line"].strip()
            if place_txt:
                out_lines.append(f"{place_txt} {name}".rstrip())
            else:
                out_lines.append(name)

        out_lines.append("")  # blank line between divisions

    # Remove trailing blank line
    while out_lines and out_lines[-1] == "":
        out_lines.pop()

    return "\n".join(out_lines) if out_lines else None


# ------------------------------------------------------------
# Option A: DOM-based metadata extraction
# ------------------------------------------------------------
def _text_or_none(node) -> Optional[str]:
    if not node:
        return None
    txt = node.get_text(" ", strip=True)
    return txt.strip() if txt else None


def extract_by_bold_label(soup: BeautifulSoup, label: str) -> Optional[str]:
    """
    Extract value following bold label like:
      <b>Host Club:</b> VALUE
    Best-effort only.
    """
    b = soup.find("b", string=re.compile(rf"^{label}\s*:?\s*$", re.I))
    if not b:
        return None

    sib = b.find_next_sibling()
    if sib:
        v = _text_or_none(sib)
        if v:
            return v

    parent = b.parent
    if parent:
        full = parent.get_text(" ", strip=True)
        full = re.sub(rf"^{label}\s*:?\s*", "", full, flags=re.I).strip()
        return full or None

    return None


def extract_event_record(html: str, file_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # event_id from URL path
    parts = file_url.split("/")
    event_id = next((p for p in reversed(parts) if p.isdigit()), None)

    # event name
    title = soup.title.string.strip() if soup.title else "Unknown Event"

    # ✅ Date from DOM block
    date_raw = _text_or_none(soup.select_one("div.eventsDateHeader"))
    if date_raw:
        date_raw = re.sub(r"\(\s*concluded\s*\)$", "", date_raw, flags=re.I).strip()

    # ✅ Location from DOM block
    location = _text_or_none(soup.select_one("div.eventsLocationInner"))

    # Best-effort Host Club + Event Type
    event_type = extract_by_bold_label(soup, "Event Type") or extract_by_bold_label(soup, "Type")
    host_club = extract_by_bold_label(soup, "Host Club") or extract_by_bold_label(soup, "Host")

    # Year detection
    year = None
    for source in (date_raw or "", title):
        m = re.search(r"\b(19\d{2}|20\d{2})\b", source)
        if m:
            year = int(m.group(1))
            break

    # Raw results blob
    pre = soup.select_one("pre.eventsPre")
    results_text = pre.get_text("\n", strip=False) if pre else ""

    return {
        "event_id": event_id,
        "event_name": title,
        "year": year,
        "date": date_raw,
        "location": location,
        "event_type": event_type,
        "host_club": host_club,
        "url": file_url,
        "results_text": results_text,
    }


# ------------------------------------------------------------
# Build tables
# ------------------------------------------------------------
def build_from_mirror(mirror_dir: Path):
    events_show = find_events_show_dir(mirror_dir)

    event_records = []
    placement_rows = []

    for html_file in iter_event_html_files(events_show):
        html = html_file.read_text(encoding="utf-8", errors="replace")
        file_url = "file://" + str(html_file).replace("\\", "/")

        rec = extract_event_record(html, file_url)
        event_records.append(rec)

        eid = rec["event_id"]
        if eid:
            placement_rows.extend(parse_results_text(rec["results_text"], eid))

    events_df = pd.DataFrame(event_records)
    placements_df = pd.DataFrame(placement_rows)

    # Infer player IDs
    if not placements_df.empty:
        p1, p2, ctype = [], [], []
        for entry in placements_df["entry_raw"]:
            a, b, t = split_entry(entry)
            p1.append(a)
            p2.append(b)
            ctype.append(t)

        placements_df["player1_name"] = p1
        placements_df["player2_name"] = p2
        placements_df["competitor_type"] = ctype

        names = set([n for n in p1 if n] + [n for n in p2 if n])
        name_to_id = {n: stable_uuid(NAMESPACE_PLAYERS, n.lower()) for n in names}

        placements_df["player1_id"] = placements_df["player1_name"].map(name_to_id)
        placements_df["player2_id"] = placements_df["player2_name"].map(name_to_id)

        players_df = pd.DataFrame(
            [{"player_id": pid, "name_raw": name} for name, pid in name_to_id.items()]
        )
    else:
        players_df = pd.DataFrame(columns=["player_id", "name_raw"])

    return events_df, placements_df, players_df


# ------------------------------------------------------------
# Excel writer
# ------------------------------------------------------------
def write_excel(out_xlsx: Path, events_df, placements_df, players_df):
    """
    Archive workbook writer (matches Footbag_Results_Canonical.xlsx layout):
    - One sheet per year named YYYY.0
    - Columns are event_id
    - Rows are fixed labels (Tournament Name, Date, Location, ...)
    - Results are generated from placements_df (canonical), not copied raw
    """
    events_df = sanitize_excel_strings(events_df)
    placements_df = sanitize_excel_strings(placements_df)

    # Build a per-event Results blob from canonical placements
    results_map: dict[str, Optional[str]] = {}
    if not placements_df.empty and "event_id" in placements_df.columns:
        for eid, g in placements_df.groupby("event_id"):
            if eid is None or str(eid).strip() == "":
                continue
            results_map[str(eid)] = format_results_from_placements(g)

    # Canonical footbag.org URL (stable even though you run offline)
    def canonical_event_url(eid: str) -> str:
        return f"https://www.footbag.org/events/show/{eid}/"

    # Fixed row labels (index) to match the example workbook
    row_labels = [
        "Tournament Name",
        "Date",
        "Location",
        "Event Type",
        "footbag.org URL",
        "Original Name",
        "Host Club",
        "Results",
    ]

    # Ensure event_id is string for stable columns
    ev = events_df.copy()
    ev["event_id"] = ev["event_id"].astype(str)

    # Sort events deterministically: by year, then numeric-ish event_id
    def _eid_sort_key(x: str):
        try:
            return int(re.sub(r"\D+", "", x) or "0")
        except Exception:
            return 0

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
        # Build one sheet per year
        years = sorted([y for y in ev["year"].dropna().unique()])

        for y in years:
            sub = ev[ev["year"] == y].copy()
            # order event_id columns
            eids = sorted(sub["event_id"].tolist(), key=_eid_sort_key)

            data = {}
            for eid in eids:
                r = sub[sub["event_id"] == eid].iloc[0]

                tournament_name = r.get("event_name")
                date = r.get("date")
                location = r.get("location")
                event_type = r.get("event_type")
                host_club = r.get("host_club")

                # "Original Name" in your current pipeline is the raw title;
                # keep identical for now (no guessing).
                original_name = r.get("event_name")

                data[eid] = [
                    tournament_name,
                    date,
                    location,
                    event_type,
                    canonical_event_url(eid),
                    original_name,
                    host_club,
                    results_map.get(eid),
                ]

            df_year = pd.DataFrame(data, index=row_labels)
            df_year.index.name = "event_id"  # puts "event_id" in A1 like the example

            sheet_name = f"{int(y)}.0"
            df_year = sanitize_excel_strings(df_year)
            df_year.to_excel(xw, sheet_name=sheet_name)

        # Unknown-year sheet (optional but consistent with your earlier approach)
        unk = ev[ev["year"].isna()].copy()
        if not unk.empty:
            eids = sorted(unk["event_id"].tolist(), key=_eid_sort_key)
            data = {}
            for eid in eids:
                r = unk[unk["event_id"] == eid].iloc[0]
                data[eid] = [
                    r.get("event_name"),
                    r.get("date"),
                    r.get("location"),
                    r.get("event_type"),
                    canonical_event_url(eid),
                    r.get("event_name"),
                    r.get("host_club"),
                    results_map.get(eid),
                ]
            df_unk = pd.DataFrame(data, index=row_labels)
            df_unk.index.name = "event_id"
            df_unk = sanitize_excel_strings(df_unk)
            df_unk.to_excel(xw, sheet_name="unknown_year")

    print("Wrote:", out_xlsx)


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    """
    No CLI. Always run the same deterministic pipeline:
    - mirror directory: <repo>/mirror
    - output workbook: <repo>/Footbag_Results_Canonical.xlsx
    """
    repo_dir = Path(__file__).resolve().parent
    mirror_dir = (repo_dir / "mirror").resolve()
    out_xlsx = (repo_dir / "Footbag_Results_Canonical.xlsx").resolve()

    events_df, placements_df, players_df = build_from_mirror(mirror_dir)
    write_excel(out_xlsx, events_df, placements_df, players_df)


if __name__ == "__main__":
    main()
