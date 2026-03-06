#!/usr/bin/env python3
"""
04B_create_community_excel.py

Produces Footbag_Results_Community.xlsx — a reader-friendly presentation of
the canonical dataset targeted at the footbag community.

Read-only transformation only. No identity changes, no canonical mutations.

Inputs  (from out/):
    stage2_canonical_events.csv  — event metadata + division source order
    index.csv                    — clean display metadata (name, date, location)
    Placements_Flat.csv          — identity-resolved placements
    Placements_ByPerson.csv      — for leaderboard computation
    Persons_Truth.csv            — player roster

Output:
    Footbag_Results_Community.xlsx
"""

import csv
import json
import sys
from collections import defaultdict, OrderedDict
from datetime import datetime
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

csv.field_size_limit(10_000_000)

REPO    = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "out"
XLSX    = REPO / "Footbag_Results_Community.xlsx"


# ── Palette & styles ──────────────────────────────────────────────────────────

def _fill(hex6: str) -> PatternFill:
    return PatternFill("solid", fgColor=hex6)

def _font(**kw) -> Font:
    return Font(**kw)

def _border_top() -> Border:
    return Border(top=Side(style="thin", color="BBBBBB"))

FILL_BANNER   = _fill("1F4E79")   # dark navy — event banner
FILL_META     = _fill("EBF3FB")   # pale blue — location / host / date
FILL_PLAYERS  = _fill("F5F5F5")   # near-white — players count
FILL_DIV      = _fill("E2E2E2")   # light grey — division header
FILL_GOLD     = _fill("FFF3CC")   # soft gold  — 1st place
FILL_SILVER   = _fill("F0F0F0")   # near-white — 2nd place
FILL_BRONZE   = _fill("FDEBD0")   # pale orange — 3rd place
FILL_WHITE    = _fill("FFFFFF")
FILL_HDR      = _fill("1F4E79")   # sheet header row

FONT_BANNER   = Font(bold=True,   size=11, color="FFFFFF")
FONT_META     = Font(             size=9,  color="1F4E79")
FONT_HOST     = Font(italic=True, size=9,  color="444444")
FONT_PLAYERS  = Font(             size=9,  color="888888")
FONT_DIV      = Font(bold=True,   size=9)
FONT_PODIUM   = Font(bold=True,   size=9)
FONT_PLACE    = Font(             size=9)
FONT_TITLE    = Font(bold=True,   size=16)
FONT_SECTION  = Font(bold=True,   size=12)
FONT_SUBHEAD  = Font(bold=True,   size=10)
FONT_NORMAL   = Font(             size=10)
FONT_SMALL    = Font(             size=9,  color="555555")
FONT_HDR      = Font(bold=True,   size=10, color="FFFFFF")
FONT_LINK     = Font(             size=10, color="0563C1", underline="single")

ALIGN_WRAP    = Alignment(wrap_text=True, vertical="top")
ALIGN_TOP     = Alignment(vertical="top")
ALIGN_CENTER  = Alignment(horizontal="center", vertical="center")

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

COL_W_EVENT   = 34   # standard event column width
COL_W_MIN     = 24   # minimum column width


# ── Data loading ──────────────────────────────────────────────────────────────

def load_index() -> dict:
    """Load index.csv → dict event_id → {name, date, location, host_club}."""
    path = OUT_DIR / "index.csv"
    meta = {}
    try:
        df = pd.read_csv(path, dtype=str, encoding="latin-1").fillna("")
        for _, row in df.iterrows():
            eid = str(row.get("event_id", "")).strip()
            if not eid:
                continue
            meta[eid] = {
                "event_name": row.get("Tournament Name", "").strip(),
                "date":       row.get("Date", "").strip(),
                "location":   row.get("Location", "").strip(),
                "host_club":  row.get("Host Club", "").strip(),
                "year":       _to_int(row.get("year", "")),
            }
    except Exception as exc:
        print(f"  WARN: could not load index.csv: {exc}", file=sys.stderr)
    return meta


def load_stage2_events() -> dict:
    """
    Load stage2_canonical_events.csv.
    Returns dict event_id → {year, event_name, date, location, host_club,
                              div_order: [division_canon ...]}
    """
    path = OUT_DIR / "stage2_canonical_events.csv"
    events = {}
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        for row in csv.DictReader(fh):
            eid = row["event_id"].strip()
            try:
                placements = json.loads(row.get("placements_json") or "[]")
            except (json.JSONDecodeError, TypeError):
                placements = []

            seen, div_order = set(), []
            for p in placements:
                dc = (p.get("division_canon") or "").strip()
                if dc and dc not in seen:
                    div_order.append(dc)
                    seen.add(dc)

            events[eid] = {
                "event_id":   eid,
                "year":       _to_int(row.get("year")),
                "event_name": (row.get("event_name") or "").strip(),
                "date":       (row.get("date") or "").strip(),
                "location":   (row.get("location") or "").strip(),
                "host_club":  (row.get("host_club") or "").strip(),
                "div_order":  div_order,
            }
    return events


def load_placements_flat() -> pd.DataFrame:
    return pd.read_csv(
        OUT_DIR / "Placements_Flat.csv", dtype=str, encoding="utf-8",
    ).fillna("")


def load_placements_by_person() -> pd.DataFrame:
    return pd.read_csv(
        OUT_DIR / "Placements_ByPerson.csv", dtype=str, encoding="utf-8",
    ).fillna("")


def load_persons_truth() -> pd.DataFrame:
    df = pd.read_csv(
        OUT_DIR / "Persons_Truth.csv", dtype=str, encoding="utf-8",
    ).fillna("")
    # Drop columns not useful to community readers
    internal = {"effective_person_id", "player_ids_seen", "player_names_seen",
                "alias_statuses", "norm_key", "last_token",
                "person_canon_clean", "person_canon_clean_reason"}
    return df.drop(columns=[c for c in df.columns if c in internal])


def _to_int(v) -> int:
    try:
        return int(float(v or 0))
    except (ValueError, TypeError):
        return 0


# ── Placement data for year sheets ────────────────────────────────────────────

def build_event_placements(pf: pd.DataFrame, events: dict) -> dict:
    """
    Returns dict: event_id → OrderedDict{division_canon: [(place_int, display, cat)]}.
    Divisions are in source order (from events[eid]['div_order']).
    Doubles are deduplicated by team_person_key; team_display_name used for display.
    __NON_PERSON__ rows are excluded.
    """
    result = {}

    for eid, edf in pf.groupby("event_id"):
        if eid not in events:
            continue

        div_order = events[eid]["div_order"]
        div_placements: dict = {}

        for div_canon, ddf in edf.groupby("division_canon"):
            ddf = ddf.copy()
            ddf["_place"] = pd.to_numeric(ddf["place"], errors="coerce")
            ddf = ddf.sort_values(["_place", "team_person_key", "person_canon"],
                                  na_position="last")

            entries = []
            seen_teams: set = set()

            for _, row in ddf.iterrows():
                person = (row.get("person_canon") or "").strip()
                if not person or person == "__NON_PERSON__":
                    continue
                if (row.get("person_unresolved") or "").lower() == "true":
                    continue

                try:
                    place_int = int(float(row["place"]))
                except (ValueError, TypeError):
                    continue

                comp = (row.get("competitor_type") or "player").lower()
                tpk  = (row.get("team_person_key") or "").strip()
                cat  = (row.get("division_category") or "").strip()

                if comp == "team" and tpk:
                    if tpk in seen_teams:
                        continue
                    seen_teams.add(tpk)
                    display = (row.get("team_display_name") or "").strip()
                    if not display:
                        # Reconstruct from all rows sharing this team key
                        members = ddf[ddf["team_person_key"] == tpk]["person_canon"].tolist()
                        display = " / ".join(m for m in members if m)
                else:
                    display = person

                entries.append((place_int, display, cat))

            if entries:
                div_placements[div_canon] = entries

        # Reorder: source order first, then any divisions not seen in stage2
        ordered: OrderedDict = OrderedDict()
        for dc in div_order:
            if dc in div_placements:
                ordered[dc] = div_placements[dc]
        for dc, dp in div_placements.items():
            if dc not in ordered:
                ordered[dc] = dp

        result[eid] = ordered

    return result


# ── Leaderboard computation ───────────────────────────────────────────────────

def compute_leaderboards(pbp: pd.DataFrame) -> pd.DataFrame:
    """Compute wins / podiums / placements / events / career_span per person."""
    df = pbp.copy()
    df = df[df["person_unresolved"].str.lower() != "true"]
    df = df[df["person_canon"].str.strip() != ""]
    df = df[df["person_canon"].str.strip() != "__NON_PERSON__"]
    df["_place"] = pd.to_numeric(df["place"], errors="coerce")
    df["_year"]  = pd.to_numeric(df["year"],  errors="coerce")

    wins      = df[df["_place"] == 1].groupby("person_canon").size().rename("wins")
    podiums   = df[df["_place"] <= 3].groupby("person_canon").size().rename("podiums")
    total     = df.groupby("person_canon").size().rename("placements")
    events    = df.groupby("person_canon")["event_id"].nunique().rename("events")
    first_yr  = df.groupby("person_canon")["_year"].min().rename("first_year")
    last_yr   = df.groupby("person_canon")["_year"].max().rename("last_year")

    stats = pd.concat([wins, podiums, total, events, first_yr, last_yr], axis=1).fillna(0)
    stats["wins"]     = stats["wins"].astype(int)
    stats["podiums"]  = stats["podiums"].astype(int)
    stats["placements"] = stats["placements"].astype(int)
    stats["events"]   = stats["events"].astype(int)
    stats["first_year"] = stats["first_year"].astype(int)
    stats["last_year"]  = stats["last_year"].astype(int)
    stats["career_span"] = stats["last_year"] - stats["first_year"]
    return stats.reset_index()


def compute_leaderboards_by_cat(pbp: pd.DataFrame) -> dict:
    """Wins per person per division_category."""
    df = pbp.copy()
    df = df[df["person_unresolved"].str.lower() != "true"]
    df = df[df["person_canon"].str.strip().isin(["", "__NON_PERSON__"]) == False]
    df["_place"] = pd.to_numeric(df["place"], errors="coerce")
    wins = df[df["_place"] == 1]
    by_cat = {}
    for cat, cdf in wins.groupby("division_category"):
        by_cat[cat] = (
            cdf.groupby("person_canon").size()
               .rename("wins")
               .sort_values(ascending=False)
               .reset_index()
        )
    return by_cat


# ── Cell helper ───────────────────────────────────────────────────────────────

def _c(ws, row: int, col: int, value=None, *,
       font=None, fill=None, align=None, border=None):
    cell = ws.cell(row=row, column=col, value=value)
    if font:   cell.font      = font
    if fill:   cell.fill      = fill
    if align:  cell.alignment = align
    if border: cell.border    = border
    return cell


# ── ReadMe sheet ──────────────────────────────────────────────────────────────

def build_readme(wb: Workbook, events: dict, pf: pd.DataFrame):
    ws = wb.create_sheet("ReadMe")
    ws.column_dimensions["A"].width = 45

    n_events     = len(events)
    n_placements = len(pf[~pf["person_canon"].isin(["", "__NON_PERSON__"])])
    years        = sorted({ev["year"] for ev in events.values() if ev["year"]})
    yr_range     = f"{years[0]}–{years[-1]}" if years else "?"

    rows = [
        ("Footbag Historical Results Archive", FONT_TITLE,   None),
        ("",                                   None,          None),
        ("Coverage",  FONT_SECTION, None),
        (yr_range,    FONT_NORMAL,  None),
        ("",          None,         None),
        ("Events",    FONT_SECTION, None),
        (f"{n_events:,}", FONT_NORMAL, None),
        ("",          None,         None),
        ("Placements", FONT_SECTION, None),
        (f"{n_placements:,}", FONT_NORMAL, None),
        ("",          None,         None),
        ("Sources",   FONT_SECTION, None),
        ("footbag.org archive",          FONT_NORMAL, None),
        ("historical tournament records", FONT_NORMAL, None),
        ("",          None,         None),
        ("Compiled by", FONT_SECTION, None),
        ("James Leberknight", FONT_NORMAL, None),
        (str(datetime.now().year), FONT_SMALL, None),
    ]

    for r, (text, font, fill) in enumerate(rows, start=1):
        _c(ws, r, 1, text, font=font or FONT_NORMAL)


# ── Summary sheet ─────────────────────────────────────────────────────────────

def build_summary(wb: Workbook, events: dict, event_placements: dict,
                  stats: pd.DataFrame, pbp: pd.DataFrame):
    ws = wb.create_sheet("Summary")
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 4
    ws.column_dimensions["D"].width = 10
    ws.column_dimensions["E"].width = 10
    ws.column_dimensions["F"].width = 10

    # Title
    ws.merge_cells("A1:F1")
    _c(ws, 1, 1, "Footbag Historical Results Archive",
       font=FONT_TITLE, align=ALIGN_CENTER)

    # ── Dataset overview ──────────────────────────────────────────────────────
    _c(ws, 3, 1, "Dataset Overview", font=FONT_SECTION)

    n_events     = len(events)
    n_placements = sum(len(v) for ep in event_placements.values() for v in ep.values())
    n_players    = len(stats) if not stats.empty else "?"
    years        = sorted({ev["year"] for ev in events.values() if ev["year"]})
    yr_range     = f"{years[0]}–{years[-1]}" if years else "?"

    for r, (label, value) in enumerate([
        ("Events",         f"{n_events:,}"),
        ("Years covered",  yr_range),
        ("Placements",     f"{n_placements:,}"),
        ("Unique players", f"{n_players:,}" if isinstance(n_players, int) else n_players),
    ], start=4):
        _c(ws, r, 1, label, font=FONT_SUBHEAD)
        _c(ws, r, 2, value, font=FONT_NORMAL)

    # ── Per-year stats table (D column — used only for charts) ────────────────
    from collections import Counter
    events_per_year: Counter = Counter()
    players_per_year: dict   = defaultdict(set)

    for eid, ep in event_placements.items():
        yr = events.get(eid, {}).get("year", 0)
        if not yr:
            continue
        events_per_year[yr] += 1
        for div_placements in ep.values():
            for _, display, _ in div_placements:
                if display and display != "__NON_PERSON__":
                    players_per_year[yr].add(display)

    chart_years     = sorted(events_per_year)
    tbl_row_start   = 3
    tbl_col         = 4  # column D

    _c(ws, tbl_row_start, tbl_col,   "Year",    font=FONT_SUBHEAD)
    _c(ws, tbl_row_start, tbl_col+1, "Events",  font=FONT_SUBHEAD)
    _c(ws, tbl_row_start, tbl_col+2, "Players", font=FONT_SUBHEAD)

    for i, yr in enumerate(chart_years):
        r = tbl_row_start + 1 + i
        ws.cell(row=r, column=tbl_col,   value=yr)
        ws.cell(row=r, column=tbl_col+1, value=events_per_year[yr])
        ws.cell(row=r, column=tbl_col+2, value=len(players_per_year[yr]))

    tbl_row_end = tbl_row_start + len(chart_years)

    # ── Chart 1: Events per year ──────────────────────────────────────────────
    c1 = BarChart()
    c1.type    = "col"
    c1.title   = "Events Per Year"
    c1.y_axis.title = "Events"
    c1.x_axis.title = "Year"
    c1.style   = 10
    c1.width   = 20
    c1.height  = 12
    c1.grouping = "clustered"
    data1 = Reference(ws, min_col=tbl_col+1, min_row=tbl_row_start,
                      max_row=tbl_row_end)
    cats1 = Reference(ws, min_col=tbl_col, min_row=tbl_row_start+1,
                      max_row=tbl_row_end)
    c1.add_data(data1, titles_from_data=True)
    c1.set_categories(cats1)
    ws.add_chart(c1, "A10")

    # ── Chart 2: Players per year ─────────────────────────────────────────────
    c2 = BarChart()
    c2.type    = "col"
    c2.title   = "Unique Players Per Year"
    c2.y_axis.title = "Players"
    c2.x_axis.title = "Year"
    c2.style   = 11
    c2.width   = 20
    c2.height  = 12
    c2.grouping = "clustered"
    data2 = Reference(ws, min_col=tbl_col+2, min_row=tbl_row_start,
                      max_row=tbl_row_end)
    c2.add_data(data2, titles_from_data=True)
    c2.set_categories(cats1)
    ws.add_chart(c2, "A32")

    # ── Leaderboards ─────────────────────────────────────────────────────────
    lb_row = 55
    _c(ws, lb_row, 1, "Leaderboards", font=FONT_SECTION)
    lb_row += 1

    def write_lb(ws, start_row, col, title, df_in, val_col, val_label, n=10):
        _c(ws, start_row, col,   title,      font=FONT_SUBHEAD)
        _c(ws, start_row, col+1, val_label,  font=FONT_SUBHEAD)
        r = start_row + 1
        try:
            top = (df_in[["person_canon", val_col]].copy()
                   .assign(**{val_col: pd.to_numeric(df_in[val_col], errors="coerce")})
                   .dropna(subset=[val_col])
                   .nlargest(n, val_col))
            for _, row in top.iterrows():
                ws.cell(row=r, column=col,   value=row["person_canon"])
                ws.cell(row=r, column=col+1, value=int(row[val_col]))
                r += 1
        except Exception:
            ws.cell(row=r, column=col, value="(unavailable)")
        return r + 1

    if not stats.empty:
        lb_row = write_lb(ws, lb_row, 1, "Most Wins",        stats, "wins",     "Wins")
        lb_row = write_lb(ws, lb_row, 1, "Most Podiums",     stats, "podiums",  "Podiums")
        lb_row = write_lb(ws, lb_row, 1, "Most Appearances", stats, "events",   "Events")
        write_lb(ws, lb_row, 1, "Longest Careers",    stats, "career_span", "Years")


# ── Records sheet ─────────────────────────────────────────────────────────────

def build_records(wb: Workbook, stats: pd.DataFrame, cat_stats: dict,
                  events: dict, event_placements: dict):
    ws = wb.create_sheet("Records")
    ws.column_dimensions["A"].width = 32
    ws.column_dimensions["B"].width = 12
    ws.column_dimensions["C"].width =  4
    ws.column_dimensions["D"].width = 40
    ws.column_dimensions["E"].width = 10
    ws.column_dimensions["F"].width = 10

    _c(ws, 1, 1, "Records", font=FONT_TITLE)

    def write_lb(ws, row, col, title, df_in, name_col, val_col, val_label, n=15):
        _c(ws, row, col,   title,      font=FONT_SUBHEAD, border=_border_top())
        _c(ws, row, col+1, val_label,  font=FONT_SUBHEAD, border=_border_top())
        r = row + 1
        try:
            top = (df_in[[name_col, val_col]].copy()
                   .assign(**{val_col: pd.to_numeric(df_in[val_col], errors="coerce")})
                   .dropna(subset=[val_col])
                   .nlargest(n, val_col))
            for _, row_data in top.iterrows():
                ws.cell(row=r, column=col,   value=row_data[name_col])
                ws.cell(row=r, column=col+1, value=int(row_data[val_col]))
                r += 1
        except Exception:
            ws.cell(row=r, column=col, value="(unavailable)")
        return r + 1

    r = 3
    if not stats.empty:
        r = write_lb(ws, r, 1, "Most Wins — All Time",       stats, "person_canon", "wins",        "Wins")
        r = write_lb(ws, r, 1, "Most Podium Finishes",       stats, "person_canon", "podiums",     "Podiums")
        r = write_lb(ws, r, 1, "Most Events Competed",       stats, "person_canon", "events",      "Events")
        r = write_lb(ws, r, 1, "Longest Careers (years)",    stats, "person_canon", "career_span", "Span")

    # By division category (column D onward)
    cat_labels = {"freestyle": "Freestyle Wins", "net": "Net Wins", "golf": "Golf Wins"}
    rc = 3
    for cat, label in cat_labels.items():
        if cat in cat_stats and not cat_stats[cat].empty:
            rc = write_lb(ws, rc, 4, label, cat_stats[cat], "person_canon", "wins", "Wins")

    # Largest events (by placement count) — column D after category stats
    event_sizes = []
    for eid, ep in event_placements.items():
        n = sum(len(v) for v in ep.values())
        ev = events.get(eid, {})
        event_sizes.append((ev.get("event_name", eid), ev.get("year", 0), n))
    top_events = sorted(event_sizes, key=lambda x: x[2], reverse=True)[:15]

    rc2 = rc + 1
    _c(ws, rc2, 4, "Largest Events", font=FONT_SUBHEAD, border=_border_top())
    _c(ws, rc2, 5, "Year",           font=FONT_SUBHEAD, border=_border_top())
    _c(ws, rc2, 6, "Players",        font=FONT_SUBHEAD, border=_border_top())
    rc2 += 1
    for name, year, n in top_events:
        ws.cell(row=rc2, column=4, value=name)
        ws.cell(row=rc2, column=5, value=year)
        ws.cell(row=rc2, column=6, value=n)
        rc2 += 1


# ── Index sheet ───────────────────────────────────────────────────────────────

def build_index(wb: Workbook, events: dict, event_placements: dict,
                event_col_map: dict):
    ws = wb.create_sheet("Index")
    ws.freeze_panes = "A2"

    hdrs = ["Year", "Event", "Location", "Date", "Divisions", "Players"]
    widths = [7, 48, 35, 22, 11, 10]
    for c, (h, w) in enumerate(zip(hdrs, widths), start=1):
        _c(ws, 1, c, h, font=FONT_HDR, fill=FILL_HDR, align=ALIGN_CENTER)
        ws.column_dimensions[get_column_letter(c)].width = w

    all_eids = sorted(
        events.keys(),
        key=lambda eid: (events[eid]["year"], events[eid].get("date", eid)),
    )

    for row_idx, eid in enumerate(all_eids, start=2):
        ev  = events[eid]
        ep  = event_placements.get(eid, {})
        n_p = sum(len(v) for v in ep.values())
        n_d = len(ep)

        ws.cell(row=row_idx, column=1, value=ev["year"] or "?")
        ws.cell(row=row_idx, column=3, value=ev["location"])
        ws.cell(row=row_idx, column=4, value=ev["date"])
        ws.cell(row=row_idx, column=5, value=n_d)
        ws.cell(row=row_idx, column=6, value=n_p)

        # Event name — hyperlink to year sheet column if available
        cell = ws.cell(row=row_idx, column=2, value=ev["event_name"])
        if eid in event_col_map:
            sheet_name, col_letter = event_col_map[eid]
            # Escape single quotes in sheet name
            safe_sheet = sheet_name.replace("'", "''")
            cell.hyperlink = f"#'{safe_sheet}'!{col_letter}1"
            cell.font = FONT_LINK
        else:
            cell.font = FONT_NORMAL

    # Alternating row shading
    for row_idx in range(2, len(all_eids) + 2):
        if row_idx % 2 == 0:
            for c in range(1, 7):
                ws.cell(row=row_idx, column=c).fill = _fill("F7F9FC")


# ── Players sheet ─────────────────────────────────────────────────────────────

def build_players(wb: Workbook, persons_df: pd.DataFrame):
    ws = wb.create_sheet("Players")
    ws.freeze_panes = "A2"

    rename_map = {
        "person_canon":        "Player",
        "aliases_presentable": "Also Known As",
        "notes":               "Notes",
        "exclusion_reason":    "Status",
        "source":              "Source",
    }
    show = [c for c in rename_map if c in persons_df.columns]
    df   = persons_df[show].rename(columns=rename_map)

    # Remove rows that are not presentable (blank player name)
    if "Player" in df.columns:
        df = df[df["Player"].str.strip().ne("")]

    # Sort alphabetically
    if "Player" in df.columns:
        df = df.sort_values("Player")

    hdrs = list(df.columns)
    col_widths = {"Player": 30, "Also Known As": 35, "Notes": 40,
                  "Status": 18, "Source": 12}

    for c, h in enumerate(hdrs, start=1):
        _c(ws, 1, c, h, font=FONT_HDR, fill=FILL_HDR)
        ws.column_dimensions[get_column_letter(c)].width = col_widths.get(h, 20)

    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=False),
                                 start=2):
        for c_idx, val in enumerate(row, start=1):
            ws.cell(row=r_idx, column=c_idx, value=val if val else None)

    ws.auto_filter.ref = (
        f"A1:{get_column_letter(len(hdrs))}1"
    )


# ── Player Results sheet ──────────────────────────────────────────────────────

def build_player_results(wb: Workbook, pf: pd.DataFrame, events: dict):
    ws = wb.create_sheet("Player Results")
    ws.freeze_panes = "A2"

    hdrs = ["Year", "Event", "Location", "Division", "Category",
            "Place", "Player", "Partner"]
    widths = [7, 48, 32, 28, 12, 7, 28, 28]

    for c, (h, w) in enumerate(zip(hdrs, widths), start=1):
        _c(ws, 1, c, h, font=FONT_HDR, fill=FILL_HDR)
        ws.column_dimensions[get_column_letter(c)].width = w

    # Filter: exclude __NON_PERSON__, unresolved, and blank
    df = pf.copy()
    df = df[df["person_unresolved"].str.lower() != "true"]
    df = df[~df["person_canon"].isin(["", "__NON_PERSON__"])]

    df["_place"] = pd.to_numeric(df["place"], errors="coerce")
    df["_year"]  = pd.to_numeric(df["year"],  errors="coerce")
    df = df.sort_values(["_year", "event_id", "division_canon", "_place",
                          "team_person_key", "person_canon"],
                        na_position="last")

    row_idx = 2
    seen_teams: dict = {}   # (event_id, division_canon, place, tpk) → partner name

    # Pre-build partner lookup for doubles
    for _, row in df[df["competitor_type"] == "team"].iterrows():
        tpk = (row.get("team_person_key") or "").strip()
        if not tpk:
            continue
        key = (row["event_id"], row["division_canon"], row["place"])
        grp = df[
            (df["event_id"] == row["event_id"]) &
            (df["division_canon"] == row["division_canon"]) &
            (df["place"] == row["place"]) &
            (df["team_person_key"] == tpk) &
            (df["person_canon"] != row["person_canon"])
        ]["person_canon"].tolist()
        seen_teams[(row["event_id"], row["division_canon"], row["place"],
                    tpk, row["person_canon"])] = " / ".join(grp) if grp else ""

    for _, row in df.iterrows():
        eid    = row["event_id"]
        ev     = events.get(eid, {})
        person = (row.get("person_canon") or "").strip()
        tpk    = (row.get("team_person_key") or "").strip()

        partner = ""
        if (row.get("competitor_type") or "").lower() == "team" and tpk:
            partner = seen_teams.get(
                (eid, row["division_canon"], row["place"], tpk, person), ""
            )
            if not partner:
                # Fallback: strip own name from team_display_name
                td = (row.get("team_display_name") or "").strip()
                if td and person in td:
                    partner = td.replace(person, "").strip(" /")
                elif td:
                    partner = td

        try:
            place_val = int(float(row["place"]))
        except (ValueError, TypeError):
            place_val = row["place"]

        ws.cell(row=row_idx, column=1, value=ev.get("year") or _to_int(row.get("year")))
        ws.cell(row=row_idx, column=2, value=ev.get("event_name") or eid)
        ws.cell(row=row_idx, column=3, value=ev.get("location", ""))
        ws.cell(row=row_idx, column=4, value=row.get("division_canon", ""))
        ws.cell(row=row_idx, column=5, value=row.get("division_category", ""))
        ws.cell(row=row_idx, column=6, value=place_val)
        ws.cell(row=row_idx, column=7, value=person)
        ws.cell(row=row_idx, column=8, value=partner)
        row_idx += 1

    ws.auto_filter.ref = f"A1:{get_column_letter(len(hdrs))}{row_idx - 1}"


# ── Year sheets ───────────────────────────────────────────────────────────────

# Fixed row positions for the event header block
_R_NAME    = 1   # Event name
_R_LOC     = 2   # Location
_R_HOST    = 3   # Host club
_R_DATE    = 4   # Date
_R_PLAYERS = 5   # Players count
_R_BLANK   = 6   # Spacer
_R_DATA    = 7   # First division header


def _write_event_col(ws, col: int, ev: dict, placements: OrderedDict) -> int:
    """
    Write one event into column `col`.
    Returns the last row written.
    """
    n_players = sum(len(v) for v in placements.values())

    _c(ws, _R_NAME,    col, ev["event_name"], font=FONT_BANNER, fill=FILL_BANNER, align=ALIGN_WRAP)
    _c(ws, _R_LOC,     col, ev["location"] or "—",    font=FONT_META,    fill=FILL_META,    align=ALIGN_TOP)
    _c(ws, _R_HOST,    col, ev["host_club"] or "",     font=FONT_HOST,    fill=FILL_META,    align=ALIGN_TOP)
    _c(ws, _R_DATE,    col, ev["date"] or "",          font=FONT_META,    fill=FILL_META,    align=ALIGN_TOP)
    _c(ws, _R_PLAYERS, col, f"Players: {n_players}",  font=FONT_PLAYERS, fill=FILL_PLAYERS, align=ALIGN_TOP)

    row = _R_DATA

    for div_name, entries in placements.items():
        # Division header
        _c(ws, row, col, div_name,
           font=FONT_DIV, fill=FILL_DIV, border=_border_top(), align=ALIGN_TOP)
        row += 1

        for place_int, display, _ in entries:
            medal = MEDALS.get(place_int, "")
            text  = f"{medal} {place_int}  {display}" if medal else f"  {place_int}  {display}"

            if place_int == 1:
                fill, font = FILL_GOLD,   FONT_PODIUM
            elif place_int == 2:
                fill, font = FILL_SILVER, FONT_PODIUM
            elif place_int == 3:
                fill, font = FILL_BRONZE, FONT_PODIUM
            else:
                fill, font = FILL_WHITE,  FONT_PLACE

            _c(ws, row, col, text, font=font, fill=fill, align=ALIGN_TOP)
            row += 1

        row += 1   # blank row between divisions

    return row - 1


def build_year_sheet(wb: Workbook, year: int, eids: list,
                     events: dict, event_placements: dict) -> dict:
    """
    Build one year sheet. Returns dict event_id → column_letter.
    Events are sorted by date then event_id.
    """
    ws = wb.create_sheet(title=str(year))

    sorted_eids = sorted(
        eids,
        key=lambda eid: (events[eid].get("date", ""), eid),
    )

    event_col_map: dict = {}

    for col_idx, eid in enumerate(sorted_eids, start=1):
        ev         = events[eid]
        placements = event_placements.get(eid, OrderedDict())
        _write_event_col(ws, col_idx, ev, placements)
        event_col_map[eid] = get_column_letter(col_idx)

    # ── Formatting ────────────────────────────────────────────────────────────
    ws.freeze_panes = "B1"   # freeze first event column when scrolling right

    for col_idx in range(1, len(sorted_eids) + 1):
        ltr = get_column_letter(col_idx)
        ws.column_dimensions[ltr].width = COL_W_EVENT

    ws.row_dimensions[_R_NAME].height    = 36
    ws.row_dimensions[_R_LOC].height     = 15
    ws.row_dimensions[_R_HOST].height    = 15
    ws.row_dimensions[_R_DATE].height    = 15
    ws.row_dimensions[_R_PLAYERS].height = 15

    return event_col_map


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading data…")
    index_meta    = load_index()
    s2_events     = load_stage2_events()
    pf            = load_placements_flat()
    pbp           = load_placements_by_person()
    persons_df    = load_persons_truth()

    # Merge index metadata (better display strings) into s2 events
    for eid, s2 in s2_events.items():
        if eid in index_meta:
            im = index_meta[eid]
            if im["event_name"]: s2["event_name"] = im["event_name"]
            if im["date"]:       s2["date"]       = im["date"]
            if im["location"]:   s2["location"]   = im["location"]
            if im["host_club"]:  s2["host_club"]  = im["host_club"]

    print("Building event placements…")
    event_placements = build_event_placements(pf, s2_events)

    print("Computing leaderboards…")
    stats    = compute_leaderboards(pbp)
    cat_stats = compute_leaderboards_by_cat(pbp)

    # Group events by year (only years with placements get a sheet)
    year_to_eids: dict = defaultdict(list)
    for eid in s2_events:
        yr = s2_events[eid]["year"]
        if yr and eid in event_placements and event_placements[eid]:
            year_to_eids[yr].append(eid)

    print("Creating workbook…")
    wb = Workbook()
    wb.remove(wb.active)   # remove default empty sheet

    # ── Create non-year sheets first (establishes desired order) ─────────────
    build_readme(wb, s2_events, pf)
    build_summary(wb, s2_events, event_placements, stats, pbp)
    build_records(wb, stats, cat_stats, s2_events, event_placements)

    # Index sheet placeholder — filled after year sheets are built
    idx_ws = wb.create_sheet("Index")   # keep position; content added below

    build_players(wb, persons_df)
    build_player_results(wb, pf, s2_events)

    # ── Year sheets ───────────────────────────────────────────────────────────
    all_event_col_map: dict = {}   # event_id → (sheet_title, col_letter)

    sorted_years = sorted(year_to_eids.keys())
    print(f"Building {len(sorted_years)} year sheets…")
    for year in sorted_years:
        col_map = build_year_sheet(wb, year, year_to_eids[year],
                                   s2_events, event_placements)
        for eid, col_letter in col_map.items():
            all_event_col_map[eid] = (str(year), col_letter)

    # ── Fill Index now that year sheet positions are known ────────────────────
    # Remove placeholder and rebuild with correct data
    wb.remove(idx_ws)
    # Re-insert at position 3 (after ReadMe=0, Summary=1, Records=2)
    build_index_real(wb, s2_events, event_placements, all_event_col_map,
                     insert_at=3)

    # ── Save ──────────────────────────────────────────────────────────────────
    print(f"Saving {XLSX}…")
    wb.save(XLSX)
    n_placements = sum(len(v) for ep in event_placements.values() for v in ep.values())
    print(f"Done. Events: {len(s2_events)}, Placements: {n_placements:,}, "
          f"Year sheets: {len(sorted_years)}")


def build_index_real(wb: Workbook, events: dict, event_placements: dict,
                     event_col_map: dict, insert_at: int):
    """Build the Index sheet and insert it at the correct position."""
    ws = wb.create_sheet("Index")
    wb.move_sheet("Index", offset=-(len(wb.sheetnames) - 1 - insert_at))

    ws.freeze_panes = "A2"

    hdrs   = ["Year", "Event", "Location", "Date", "Divisions", "Players"]
    widths = [7, 48, 35, 22, 11, 10]
    for c, (h, w) in enumerate(zip(hdrs, widths), start=1):
        _c(ws, 1, c, h, font=FONT_HDR, fill=FILL_HDR, align=ALIGN_CENTER)
        ws.column_dimensions[get_column_letter(c)].width = w

    all_eids = sorted(
        events.keys(),
        key=lambda eid: (events[eid]["year"], events[eid].get("date", eid)),
    )

    for row_idx, eid in enumerate(all_eids, start=2):
        ev  = events[eid]
        ep  = event_placements.get(eid, {})
        n_p = sum(len(v) for v in ep.values())
        n_d = len(ep)

        ws.cell(row=row_idx, column=1, value=ev["year"] or "?")
        ws.cell(row=row_idx, column=3, value=ev["location"])
        ws.cell(row=row_idx, column=4, value=ev["date"])
        ws.cell(row=row_idx, column=5, value=n_d)
        ws.cell(row=row_idx, column=6, value=n_p)

        cell = ws.cell(row=row_idx, column=2, value=ev["event_name"])
        if eid in event_col_map:
            sheet_name, col_letter = event_col_map[eid]
            safe = sheet_name.replace("'", "''")
            cell.hyperlink = f"#'{safe}'!{col_letter}1"
            cell.font = FONT_LINK
        else:
            cell.font = FONT_NORMAL

        if row_idx % 2 == 0:
            for c in range(1, 7):
                if ws.cell(row=row_idx, column=c).fill.fgColor.rgb == "00000000":
                    ws.cell(row=row_idx, column=c).fill = _fill("F7F9FC")


if __name__ == "__main__":
    main()
