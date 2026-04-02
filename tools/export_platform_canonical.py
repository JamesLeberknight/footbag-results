#!/usr/bin/env python3
"""
tools/export_platform_canonical.py

Export release_publication/*.csv → out/platform_release/*.csv in the schema
expected by footbag-platform script 07_build_mvfp_seed_full.py.

This is the final step of the merged pipeline (run_pipeline.sh merged).

Input:  out/release_publication/
Output: out/platform_release/

Run:
    python tools/export_platform_canonical.py
    python tools/export_platform_canonical.py --output-dir /path/to/canonical_input
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "out" / "release_publication"
DEFAULT_OUTPUT = ROOT / "out" / "platform_release"


def load(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}")
    return pd.read_csv(path, dtype=str).fillna("")


def discipline_key(event_key: pd.Series, discipline: pd.Series) -> pd.Series:
    norm = discipline.astype(str).str.strip().str.lower()
    norm = norm.str.replace(r"\s+", "_", regex=True)
    return event_key.astype(str).str.strip() + "__" + norm


def export_events(df: pd.DataFrame) -> pd.DataFrame:
    ek = df["event_id"].str.strip()
    return pd.DataFrame({
        "event_key":       ek,
        "legacy_event_id": "",
        "year":            df["year"].str.strip(),
        "event_name":      df["event_name"].str.strip(),
        "event_slug":      ek,
        "start_date":      df["start_date"].str.strip(),
        "end_date":        df["end_date"].str.strip(),
        "city":            df["city"].str.strip(),
        "region":          df["region"].str.strip(),
        "country":         df["country"].str.strip(),
        "host_club":       df["host_club"].str.strip(),
        "status":          df["status"].str.strip(),
        "notes":           df.get("validation_status", pd.Series([""] * len(df))).str.strip(),
        "source":          df.get("data_source", pd.Series([""] * len(df))).str.strip(),
    }).sort_values(["year", "event_name", "event_key"], kind="stable")


def export_event_disciplines(df: pd.DataFrame) -> pd.DataFrame:
    ek = df["event_id"].str.strip()
    dk = discipline_key(ek, df["discipline"])
    out = pd.DataFrame({
        "event_key":          ek,
        "discipline_key":     dk,
        "discipline_name":    df["discipline_name"].str.strip(),
        "discipline_category": df["discipline_category"].str.strip(),
        "team_type":          df["team_type"].str.strip(),
        "sort_order":         df["sort_order"].str.strip(),
        "coverage_flag":      df["coverage_flag"].str.strip(),
        "notes":              df["notes"].str.strip(),
    })
    out = out.sort_values(["event_key", "discipline_key"], kind="stable")
    out = out.drop_duplicates(subset=["event_key", "discipline_key"], keep="first")
    return out


def export_event_results(df: pd.DataFrame) -> pd.DataFrame:
    ek = df["event_id"].astype(str).str.strip()
    dk = discipline_key(ek, df["discipline"])
    source = (
        df.get("source_type", pd.Series([""] * len(df))).astype(str).str.strip()
        + "|" +
        df.get("data_source", pd.Series([""] * len(df))).astype(str).str.strip()
    ).str.strip("|")
    out = pd.DataFrame({
        "event_key":      ek,
        "discipline_key": dk,
        "placement":      df["placement"].astype(str).str.strip(),
        "score_text":     df["score_text"].astype(str).str.strip(),
        "notes":          "",
        "source":         source,
    })
    # PRE1997 source sorts before POST1997 (F < P), keep PRE1997 when deduping
    n_before = len(out)
    out = out.sort_values(
        ["event_key", "discipline_key", "placement", "source"],
        ascending=[True, True, True, True],
        kind="stable",
    )
    out = out.drop_duplicates(subset=["event_key", "discipline_key", "placement"], keep="first")
    n_dupes = n_before - len(out)
    if n_dupes:
        print(f"  event_results: deduped {n_dupes} rows (PRE1997 kept where discipline names collide)")
    return out


_SENTINEL_NAMES = {"__NON_PERSON__", "[UNKNOWN PARTNER]", "__UNKNOWN_PARTNER__"}


def export_event_result_participants(df: pd.DataFrame) -> pd.DataFrame:
    ek = df["event_id"].astype(str).str.strip()
    dk = discipline_key(ek, df["discipline"])
    display = df["display_name"].astype(str).str.strip().replace(
        {s: "Unknown" for s in _SENTINEL_NAMES}
    )
    out = pd.DataFrame({
        "event_key":        ek,
        "discipline_key":   dk,
        "placement":        df["placement"].astype(str).str.strip(),
        "participant_order": df["participant_order"].astype(str).str.strip(),
        "display_name":     display,
        "person_id":        df["person_id"].astype(str).str.strip(),
        "team_person_key":  df["team_person_key"].astype(str).str.strip(),
        "notes":            "",
        "_data_source":     df.get("data_source", pd.Series([""] * len(df))).astype(str).str.strip(),
    })
    # PRE1997 > POST1997 alphabetically (R > O) — descending keeps PRE1997 first
    n_before = len(out)
    out = out.sort_values(
        ["event_key", "discipline_key", "placement", "participant_order", "_data_source"],
        ascending=[True, True, True, True, False],
        kind="stable",
    )
    out = out.drop_duplicates(
        subset=["event_key", "discipline_key", "placement", "participant_order"],
        keep="first",
    )
    n_dupes = n_before - len(out)
    if n_dupes:
        print(f"  event_result_participants: deduped {n_dupes} rows (PRE1997 kept)")
    return out.drop(columns=["_data_source"])


def _yn_to_bit(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper().map(lambda v: "1" if v == "Y" else "0")


def export_persons(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "person_id":              df["person_id"].astype(str).str.strip(),
        "person_name":            df["person_canon"].astype(str).str.strip(),
        "country":                df["country"].astype(str).str.strip(),
        "first_year":             df["first_year"].astype(str).str.strip(),
        "last_year":              df["last_year"].astype(str).str.strip(),
        "event_count":            "",
        "placement_count":        "",
        "bap_member":             _yn_to_bit(df["bap_member"]),
        "bap_nickname":           df["bap_nickname"].astype(str).str.strip(),
        "bap_induction_year":     df["bap_induction_year"].astype(str).str.strip(),
        "hof_member":             _yn_to_bit(df["fbhof_member"]),
        "hof_induction_year":     df["fbhof_induction_year"].astype(str).str.strip(),
        "freestyle_sequences":    "",
        "freestyle_max_add":      "",
        "freestyle_unique_tricks": "",
        "freestyle_diversity_ratio": "",
        "signature_trick_1":      "",
        "signature_trick_2":      "",
        "signature_trick_3":      "",
    })
    out = out[out["person_name"].str.strip() != ""].copy()
    return out.sort_values(["person_name", "person_id"], kind="stable")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", default=str(DEFAULT_INPUT))
    ap.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    in_dir = Path(args.input_dir).expanduser().resolve()
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    steps = [
        ("events.csv",                   export_events),
        ("event_disciplines.csv",        export_event_disciplines),
        ("event_results.csv",            export_event_results),
        ("event_result_participants.csv", export_event_result_participants),
        ("persons.csv",                  export_persons),
    ]

    for filename, fn in steps:
        df = load(in_dir / filename)
        out = fn(df)
        for col in out.columns:
            out[col] = out[col].fillna("").astype(str).replace({"nan": ""})
        dest = out_dir / filename
        out.to_csv(dest, index=False)
        print(f"  {filename}: {len(out):,} rows → {dest}")

    print(f"\nplatform_release/ ready for footbag-platform script 07.")


if __name__ == "__main__":
    main()
