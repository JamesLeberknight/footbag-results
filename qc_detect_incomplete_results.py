from pathlib import Path
import re
import pandas as pd

ROOT = Path(".")
MIRROR_DIR = ROOT / "mirror_full" / "www.footbag.org" / "events" / "show"
PLACEMENTS_CSV = ROOT / "out" / "Placements_Flat.csv"
EVENTS_CSV = ROOT / "out" / "stage2_canonical_events.csv"

OUT_PARSED_DIV = ROOT / "qc_parsed_division_summary.csv"
OUT_PARSED_EVT = ROOT / "qc_parsed_event_summary.csv"
OUT_MIRROR_DIV = ROOT / "qc_mirror_division_summary.csv"
OUT_MIRROR_EVT = ROOT / "qc_mirror_event_summary.csv"
OUT_CANDIDATES = ROOT / "qc_incomplete_results_candidates.csv"

DIVISION_KEYWORDS = [
    "freestyle",
    "net",
    "golf",
    "sick 3",
    "sick three",
    "circle",
    "shred",
    "doubles",
    "doubles routine",
    "request",
    "timed",
    "distance",
    "accuracy",
    "consecutive",
]

PLACE_PATTERNS = [
    re.compile(r"^\s*(\d+)\.?\s+(.+?)\s*$"),
    re.compile(r"^\s*(\d+)\)\s+(.+?)\s*$"),
    re.compile(r"^\s*T?(\d+)\.?\s+(.+?)\s*$"),
]

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_text(html: str) -> list[str]:
    text = html.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"</div\s*>", "\n", text, flags=re.I)
    text = re.sub(r"</tr\s*>", "\n", text, flags=re.I)
    text = re.sub(r"</li\s*>", "\n", text, flags=re.I)
    text = TAG_RE.sub(" ", text)
    text = text.replace("&nbsp;", " ")
    lines = [WS_RE.sub(" ", x).strip() for x in text.splitlines()]
    lines = [x for x in lines if x]
    return lines


def looks_like_division_header(line: str) -> bool:
    s = line.strip().lower()
    if len(s) < 3:
        return False
    if any(k in s for k in DIVISION_KEYWORDS):
        return True
    if s.isupper() and len(s) <= 60:
        return True
    return False


def parse_place_line(line: str):
    for pat in PLACE_PATTERNS:
        m = pat.match(line)
        if m:
            place = int(m.group(1))
            name = m.group(2).strip()
            if name and len(name) >= 2:
                return place, name
    return None


def normalize_division(line: str) -> str:
    s = line.strip()
    s = re.sub(r"^\W+|\W+$", "", s)
    s = WS_RE.sub(" ", s)
    return s


def parse_event_html(event_id: str, html: str):
    lines = clean_text(html)

    current_division = None
    found_rows = []
    found_divisions = set()

    for line in lines:
        if looks_like_division_header(line):
            current_division = normalize_division(line)
            found_divisions.add(current_division)
            continue

        place_result = parse_place_line(line)
        if place_result:
            place, name = place_result
            found_rows.append(
                {
                    "event_id": str(event_id),
                    "division_guess": current_division if current_division else "_NO_DIVISION_DETECTED",
                    "mirror_place": place,
                    "mirror_name_raw": name,
                }
            )

    return found_rows, found_divisions


def build_parsed_summaries():
    pf = pd.read_csv(PLACEMENTS_CSV)
    pf["event_id"] = pf["event_id"].astype(str)

    parsed_div = (
        pf.groupby(["event_id", "division_canon"])
        .agg(
            parsed_places=("place", "count"),
            parsed_max_place=("place", "max"),
        )
        .reset_index()
    )

    parsed_evt = (
        pf.groupby("event_id")
        .agg(
            parsed_total_places=("place", "count"),
            parsed_divisions=("division_canon", "nunique"),
        )
        .reset_index()
    )

    parsed_div.to_csv(OUT_PARSED_DIV, index=False)
    parsed_evt.to_csv(OUT_PARSED_EVT, index=False)
    return parsed_div, parsed_evt


def build_mirror_summaries():
    all_rows = []
    all_event_div_counts = []

    event_dirs = sorted([p for p in MIRROR_DIR.iterdir() if p.is_dir() and p.name.isdigit()])

    for p in event_dirs:
        event_id = p.name

        html_files = list(p.glob("*.html"))
        if not html_files:
            continue

        html_path = html_files[0]
        try:
            html = html_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        rows, divisions = parse_event_html(event_id, html)
        all_rows.extend(rows)

        all_event_div_counts.append(
            {
                "event_id": str(event_id),
                "mirror_divisions_detected": len(divisions),
                "mirror_total_places_detected": len(rows),
            }
        )

    if all_rows:
        mirror_rows = pd.DataFrame(all_rows)
        mirror_div = (
            mirror_rows.groupby(["event_id", "division_guess"])
            .agg(
                mirror_places_detected=("mirror_place", "count"),
                mirror_max_place_detected=("mirror_place", "max"),
            )
            .reset_index()
        )
    else:
        mirror_div = pd.DataFrame(
            columns=[
                "event_id",
                "division_guess",
                "mirror_places_detected",
                "mirror_max_place_detected",
            ]
        )

    mirror_evt = pd.DataFrame(all_event_div_counts)

    mirror_div.to_csv(OUT_MIRROR_DIV, index=False)
    mirror_evt.to_csv(OUT_MIRROR_EVT, index=False)
    return mirror_div, mirror_evt


def normalize_key(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    s = s.replace("&", "and")
    s = re.sub(r"women's", "women", s)
    s = re.sub(r"men's", "men", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = WS_RE.sub(" ", s).strip()
    return s


def build_candidates(parsed_div, parsed_evt, mirror_div, mirror_evt):
    events = pd.read_csv(EVENTS_CSV)
    events["event_id"] = events["event_id"].astype(str)

    parsed_div = parsed_div.copy()
    mirror_div = mirror_div.copy()
    parsed_evt = parsed_evt.copy()
    mirror_evt = mirror_evt.copy()

    parsed_div["event_id"] = parsed_div["event_id"].astype(str)
    mirror_div["event_id"] = mirror_div["event_id"].astype(str)
    parsed_evt["event_id"] = parsed_evt["event_id"].astype(str)
    mirror_evt["event_id"] = mirror_evt["event_id"].astype(str)

    parsed_div["division_key"] = parsed_div["division_canon"].map(normalize_key)
    mirror_div["division_key"] = mirror_div["division_guess"].map(normalize_key)

    div_compare = mirror_div.merge(
        parsed_div,
        on=["event_id", "division_key"],
        how="outer",
        suffixes=("_mirror", "_parsed"),
    )

    div_compare["mirror_places_detected"] = div_compare["mirror_places_detected"].fillna(0)
    div_compare["parsed_places"] = div_compare["parsed_places"].fillna(0)
    div_compare["mirror_max_place_detected"] = div_compare["mirror_max_place_detected"].fillna(0)
    div_compare["parsed_max_place"] = div_compare["parsed_max_place"].fillna(0)

    def classify(row):
        m = row["mirror_places_detected"]
        p = row["parsed_places"]

        if m > 0 and p == 0:
            return "missing_division_or_unmatched_division"
        if m >= p + 2:
            return "truncated_division"
        if p > 0 and m == 0:
            return "parsed_only_or_mirror_detection_failed"
        return "ok_or_small_difference"

    div_compare["problem_type_guess"] = div_compare.apply(classify, axis=1)
    div_compare["severity_score"] = (
        (div_compare["mirror_places_detected"] - div_compare["parsed_places"]).clip(lower=0) * 2
        + ((div_compare["parsed_places"] == 0) & (div_compare["mirror_places_detected"] > 0)).astype(int) * 5
    )

    evt_compare = mirror_evt.merge(parsed_evt, on="event_id", how="outer")
    evt_compare["mirror_divisions_detected"] = evt_compare["mirror_divisions_detected"].fillna(0)
    evt_compare["mirror_total_places_detected"] = evt_compare["mirror_total_places_detected"].fillna(0)
    evt_compare["parsed_divisions"] = evt_compare["parsed_divisions"].fillna(0)
    evt_compare["parsed_total_places"] = evt_compare["parsed_total_places"].fillna(0)

    candidates = div_compare.merge(
        evt_compare,
        on="event_id",
        how="left",
    ).merge(
        events[["event_id", "event_name", "year"]],
        on="event_id",
        how="left",
    )

    candidates = candidates.sort_values(
        ["severity_score", "mirror_places_detected", "event_id"],
        ascending=[False, False, True],
    )

    candidates.to_csv(OUT_CANDIDATES, index=False)
    return candidates


def main():
    print("Building parsed summaries...")
    parsed_div, parsed_evt = build_parsed_summaries()

    print("Building mirror summaries...")
    mirror_div, mirror_evt = build_mirror_summaries()

    print("Building candidate mismatch report...")
    candidates = build_candidates(parsed_div, parsed_evt, mirror_div, mirror_evt)

    flagged = candidates[candidates["severity_score"] > 0]
    print()
    print(f"Wrote: {OUT_PARSED_DIV}")
    print(f"Wrote: {OUT_PARSED_EVT}")
    print(f"Wrote: {OUT_MIRROR_DIV}")
    print(f"Wrote: {OUT_MIRROR_EVT}")
    print(f"Wrote: {OUT_CANDIDATES}")
    print()
    print(f"Total candidate rows: {len(candidates)}")
    print(f"Flagged rows (severity_score > 0): {len(flagged)}")

    if len(flagged):
        print("\nTop 20 flagged rows:")
        cols = [
            "event_id",
            "year",
            "event_name",
            "division_guess",
            "division_canon",
            "mirror_places_detected",
            "parsed_places",
            "problem_type_guess",
            "severity_score",
        ]
        print(flagged[cols].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
