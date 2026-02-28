#!/usr/bin/env python3
"""
Extract event location info from a local mirror of footbag.org event pages.

Inputs:
  - location_overrides_needed.csv (from our earlier step)
  - mirror root directory, e.g.
      ~/projects/_quarantine_v1_release_round3/mirror_full/www.footbag.org/events/show

Outputs:
  - location_from_html.csv  (raw extracted signals + provenance)
  - location_overrides_autofill.csv (best-effort city/state/country suggestions)
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd
from bs4 import BeautifulSoup


US_STATE_ABBREV = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas",
    "UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
    "DC":"District of Columbia",
}
US_STATES = set(US_STATE_ABBREV.values())

COUNTRY_ALIASES = {
    "USA": "United States",
    "U.S.A.": "United States",
    "United States of America": "United States",
}
COUNTRY_ISO3 = {
    "United States": "USA",
    "Canada": "CAN",
    "Finland": "FIN",
    "France": "FRA",
    "Germany": "DEU",
    "Poland": "POL",
    "Czech Republic": "CZE",
    "Slovakia": "SVK",
    "Slovenia": "SVN",
    "Croatia": "HRV",
    "Switzerland": "CHE",
    "Bulgaria": "BGR",
    "Venezuela": "VEN",
    "Colombia": "COL",
    "Austria": "AUT",
    "Hungary": "HUN",
    "Denmark": "DNK",
    "Spain": "ESP",
    "Italy": "ITA",
    "Belgium": "BEL",
    "Netherlands": "NLD",
    "Estonia": "EST",
    "Australia": "AUS",
    "New Zealand": "NZL",
    "Mexico": "MEX",
    "Argentina": "ARG",
    "Brazil": "BRA",
    "Chile": "CHL",
    "Peru": "PER",
    "Japan": "JPN",
    "Russia": "RUS",
}

# Some mirror layouts use event_id/index.html; others may be a single html file.
CANDIDATE_HTML_FILES = ("index.html", "show.html", "event.html")


def read_html_best_effort(event_dir: Path) -> Tuple[Optional[str], Optional[Path]]:
    """Return (html_text, path_used) for an event directory."""
    if event_dir.is_file() and event_dir.suffix.lower() in {".html", ".htm"}:
        return event_dir.read_text(encoding="utf-8", errors="replace"), event_dir

    if event_dir.is_dir():
        for name in CANDIDATE_HTML_FILES:
            p = event_dir / name
            if p.exists():
                return p.read_text(encoding="utf-8", errors="replace"), p

        # fallback: first html in dir
        htmls = sorted(event_dir.glob("*.html"))
        if htmls:
            p = htmls[0]
            return p.read_text(encoding="utf-8", errors="replace"), p

    return None, None


def soup_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # remove scripts/styles
    for t in soup(["script", "style", "noscript"]):
        t.extract()
    text = soup.get_text("\n")
    # normalize whitespace
    text = re.sub(r"[ \t\r]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def normalize_country(raw: str) -> str:
    x = raw.strip()
    x = COUNTRY_ALIASES.get(x, x)
    return x


def normalize_us_state(raw: str) -> Optional[str]:
    x = raw.strip().strip(".")
    if re.fullmatch(r"[A-Z]{2}", x):
        return US_STATE_ABBREV.get(x)
    if x in US_STATES:
        return x
    return None


def find_labeled_field(text: str, labels: List[str]) -> Optional[str]:
    """
    Find a 'Label: value' field in text.
    Returns value (single line) if found.
    """
    # Example patterns: "Location: ...", "City: ...", "Country: ..."
    for lab in labels:
        # match label at line start
        m = re.search(rf"(?im)^\s*{re.escape(lab)}\s*:\s*(.+?)\s*$", text)
        if m:
            return m.group(1).strip()
    return None


def parse_location_line(loc: str) -> Dict[str, Optional[str]]:
    """
    Conservative parse of a location-like string into city/state/country tokens.
    This is best-effort; we still preserve provenance.
    """
    loc = loc.strip()
    # strip leading "TBA" but remember incomplete
    contains_tba = bool(re.search(r"\bTBA\b", loc, flags=re.I))
    loc2 = re.sub(r"(?i)\bTBA\b", "", loc).strip(" ,")

    parts = [p.strip() for p in loc2.split(",") if p.strip()]
    out = {"city": None, "state": None, "country": None, "contains_tba": str(contains_tba)}

    if not parts:
        return out

    # Country token heuristic
    country = normalize_country(parts[-1])
    if country in COUNTRY_ISO3 or country in {"United States"} or country in {"USA"}:
        out["country"] = "United States" if country in {"USA"} else country
        # If USA, try state + city
        if out["country"] == "United States":
            if len(parts) >= 2:
                st = normalize_us_state(parts[-2])
                if st:
                    out["state"] = st
                    if len(parts) >= 3:
                        out["city"] = parts[-3]
                else:
                    # sometimes "..., City, StateName, USA" already OK, or state embedded elsewhere
                    out["state"] = parts[-2]
                    if len(parts) >= 3:
                        out["city"] = parts[-3]
        else:
            # non-US: set city as prior token if available
            if len(parts) >= 2:
                out["city"] = parts[-2]
    else:
        # country not recognized; leave blank; still provide city candidate
        if len(parts) >= 2:
            out["city"] = parts[-2]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mirror_show_dir", required=True, help="Path to mirror .../events/show")
    ap.add_argument("--overrides_needed_csv", required=True, help="location_overrides_needed.csv")
    ap.add_argument("--out_dir", default="out/location", help="Output directory")
    args = ap.parse_args()

    show_dir = Path(args.mirror_show_dir).expanduser()
    in_csv = Path(args.overrides_needed_csv).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    need = pd.read_csv(in_csv, dtype={"event_id": str})
    need["event_id"] = need["event_id"].astype(str)

    rows = []
    for _, r in need.iterrows():
        event_id = str(r["event_id"]).strip()
        if not event_id or event_id.lower() == "nan":
            continue

        event_path = show_dir / event_id
        html, used = read_html_best_effort(event_path)
        if html is None:
            # Sometimes mirrors store as show/<id>.html
            html2, used2 = read_html_best_effort(show_dir / f"{event_id}.html")
            html, used = html2, used2

        if html is None:
            rows.append({
                "event_id": event_id,
                "html_found": False,
                "html_path": None,
                "field_location": None,
                "field_city": None,
                "field_state": None,
                "field_country": None,
                "parsed_city": None,
                "parsed_state": None,
                "parsed_country": None,
                "contains_tba": None,
            })
            continue

        text = soup_text(html)

        # pull labeled fields if present
        field_location = find_labeled_field(text, ["Location", "Place", "Venue"])
        field_city = find_labeled_field(text, ["City", "Town"])
        field_state = find_labeled_field(text, ["State", "Province", "Region"])
        field_country = find_labeled_field(text, ["Country"])

        # choose the "best" location line for parsing
        best_line = field_location
        if best_line is None:
            # fallback: look for a line containing a country token from your known set
            # grab first matching line as weak signal
            lines = text.split("\n")
            cand = None
            for ln in lines:
                if re.search(r"\b(USA|United States|Canada|Finland|France|Germany|Poland|Switzerland|Austria|Hungary|Denmark|Spain|Italy|Netherlands|Belgium|Australia|New Zealand|Czech Republic|Slovakia|Slovenia|Croatia|Bulgaria|Venezuela|Colombia|Mexico|Argentina|Brazil|Chile|Peru|Japan|Russia)\b", ln):
                    cand = ln.strip()
                    break
            best_line = cand

        parsed = parse_location_line(best_line) if best_line else {"city": None, "state": None, "country": None, "contains_tba": None}

        rows.append({
            "event_id": event_id,
            "html_found": True,
            "html_path": str(used) if used else None,
            "field_location": field_location,
            "field_city": field_city,
            "field_state": field_state,
            "field_country": field_country,
            "best_line_used": best_line,
            "parsed_city": parsed.get("city"),
            "parsed_state": parsed.get("state"),
            "parsed_country": parsed.get("country"),
            "contains_tba": parsed.get("contains_tba"),
        })

    out_df = pd.DataFrame(rows)

    out_from_html = out_dir / "location_from_html.csv"
    out_df.to_csv(out_from_html, index=False)

    # Build an autofill overrides table (best-effort), preserving provenance
    auto = out_df.copy()
    auto["city_canon"] = auto["field_city"].fillna(auto["parsed_city"])
    auto["state_canon"] = auto["field_state"].fillna(auto["parsed_state"])
    auto["country_canon"] = auto["field_country"].fillna(auto["parsed_country"])
    auto["country_canon"] = auto["country_canon"].fillna("").apply(lambda x: normalize_country(x) if x else "")
    auto["country_iso3"] = auto["country_canon"].apply(lambda c: COUNTRY_ISO3.get(c) if c else None)

    def status_row(rr) -> str:
        if not rr["html_found"]:
            return "missing"
        if rr.get("contains_tba") == "True":
            # still OK if we got country/city, but call it incomplete
            return "incomplete"
        if rr["country_canon"] in ("", None) or pd.isna(rr["country_canon"]):
            return "needs_override"
        if rr["country_canon"] == "United States":
            # need state for US
            if not rr["state_canon"] or pd.isna(rr["state_canon"]):
                return "usa_needs_state_review"
        return "normalized"

    auto["location_status"] = auto.apply(status_row, axis=1)
    auto["source_type"] = "mirror_html"
    auto["source_path"] = auto["html_path"]

    out_auto = auto[[
        "event_id",
        "city_canon",
        "state_canon",
        "country_canon",
        "country_iso3",
        "location_status",
        "source_type",
        "source_path",
        "best_line_used",
        "field_location",
        "field_city",
        "field_state",
        "field_country",
    ]].copy()

    out_autofill = out_dir / "location_overrides_autofill.csv"
    out_auto.to_csv(out_autofill, index=False)

    print(f"Wrote:\n  {out_from_html}\n  {out_autofill}\n  rows: {len(out_df)}")


if __name__ == "__main__":
    main()
