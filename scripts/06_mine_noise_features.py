#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


# -----------------------------
# Normalization helpers
# -----------------------------

def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def norm_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def norm_key(text: str) -> str:
    text = strip_accents(norm_space(text)).lower()
    text = re.sub(r"[^a-z0-9\s\-/']", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def presentable_name(text: str) -> str:
    return norm_space(text).strip(" -–—:;,.()[]{}")


# -----------------------------
# Data containers
# -----------------------------

@dataclass
class PersonMatch:
    person_id: Optional[str]
    person_canon: Optional[str]
    match_type: str   # exact_canon / exact_alias / unresolved / none
    matched_on: Optional[str]


# -----------------------------
# Persons index
# -----------------------------

def parse_aliases_cell(value) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []

    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]

    s = str(value).strip()
    if not s:
        return []

    # Try Python-list style first
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [str(x) for x in parsed if str(x).strip()]
        except Exception:
            pass

    # Fallback: split on common separators
    parts = re.split(r"[|;]+", s)
    return [p.strip() for p in parts if p.strip()]


def build_person_index(persons_df: pd.DataFrame) -> tuple[dict[str, tuple[str, str]], dict[str, list[str]]]:
    """
    Returns:
      exact_name_index: norm_name -> (person_id, person_canon)
      collisions: norm_name -> [person_ids]  # names that are ambiguous and must not be auto-matched
    """
    exact_name_index: dict[str, tuple[str, str]] = {}
    collisions: dict[str, list[str]] = defaultdict(list)

    person_id_col = None
    for c in ["effective_person_id", "person_id", "canonical_person_id"]:
        if c in persons_df.columns:
            person_id_col = c
            break
    if person_id_col is None:
        raise ValueError("Could not find person ID column in persons file.")

    person_name_col = None
    for c in ["person_canon", "player_name", "person_name", "canonical_name"]:
        if c in persons_df.columns:
            person_name_col = c
            break
    if person_name_col is None:
        raise ValueError("Could not find canonical person name column in persons file.")

    alias_cols = [c for c in ["aliases_presentable", "aliases", "player_names_seen", "player_ids_seen"] if c in persons_df.columns]

    temp_index: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for _, row in persons_df.iterrows():
        pid = str(row[person_id_col]).strip()
        canon = str(row[person_name_col]).strip()
        if not pid or not canon:
            continue

        canon_key = norm_key(canon)
        if canon_key:
            temp_index[canon_key].append((pid, canon))

        for alias_col in alias_cols:
            for alias in parse_aliases_cell(row.get(alias_col)):
                alias_key = norm_key(alias)
                if alias_key:
                    temp_index[alias_key].append((pid, canon))

    for k, vals in temp_index.items():
        uniq = list({(pid, canon) for pid, canon in vals})
        if len(uniq) == 1:
            exact_name_index[k] = uniq[0]
        else:
            collisions[k] = [pid for pid, _ in uniq]

    return exact_name_index, collisions


def safe_match_person(name: str, exact_name_index: dict[str, tuple[str, str]], collisions: dict[str, list[str]]) -> PersonMatch:
    nk = norm_key(name)
    if not nk:
        return PersonMatch(None, None, "none", None)
    if nk in collisions:
        return PersonMatch(None, None, "unresolved", name)
    if nk in exact_name_index:
        pid, canon = exact_name_index[nk]
        return PersonMatch(pid, canon, "exact", name)
    return PersonMatch(None, None, "unresolved", name)


# -----------------------------
# Trick lexicon
# -----------------------------

DEFAULT_TRICKS = [
    "blur", "blurry whirl", "blurriest", "ripwalk", "paradox mirage", "parkwalk",
    "torque", "mobius", "atom smasher", "atomsmasher", "atomic set",
    "whirl", "spinning whirl", "symposium whirl", "drifter", "barfly",
    "superfly", "double around the world", "double around-the-world",
    "butterfly", "osis", "legover", "pickup", "pixie", "atomic", "stepping",
    "ducking", "spinning", "symposium", "gyro", "rev whirl", "revup",
    "fog", "dimwalk", "blender", "smear", "flurry", "tomahawk",
    "spinning clipper", "ducking clipper", "eggbeater", "swirl",
    "food processor", "barraging", "blazing", "fusion", "tapping",
]

GENERIC_NON_TRICK_TERMS = {
    "men", "women", "open", "novice", "intermediate", "final", "semifinal",
    "routine", "freestyle", "net", "golf", "distance", "accuracy",
    "shred", "sick", "circle", "qualification", "qualifier", "finals",
}


def load_trick_lexicon(path: Optional[Path]) -> list[str]:
    tricks = set(DEFAULT_TRICKS)
    if path and path.exists():
        df = pd.read_csv(path)
        col = None
        for c in ["trick", "trick_name", "name"]:
            if c in df.columns:
                col = c
                break
        if col is None:
            raise ValueError("Trick lexicon CSV must contain one of: trick, trick_name, name")
        for v in df[col].dropna():
            vv = str(v).strip()
            if vv:
                tricks.add(vv)
    # sort longest-first to prefer specific matches
    return sorted(tricks, key=lambda s: (-len(s), s.lower()))


def compile_trick_patterns(tricks: list[str]) -> list[tuple[str, re.Pattern]]:
    compiled = []
    for trick in tricks:
        pat = re.compile(rf"(?<![A-Za-z0-9]){re.escape(trick)}(?![A-Za-z0-9])", re.IGNORECASE)
        compiled.append((trick, pat))
    return compiled


# -----------------------------
# Parsing heuristics
# -----------------------------

PLACEMENT_LINE_RE = re.compile(
    r"""^\s*
    (?P<place>\d{1,3})[\.\)]\s*
    (?P<name>[A-Za-zÀ-ÿ0-9'’.\-\/ ]{2,80}?)
    (?:\s*[-–—:]\s*(?P<tail>.*))?
    \s*$
    """,
    re.VERBOSE,
)

NAME_SCORE_RE = re.compile(
    r"""(?P<name>[A-Z][A-Za-zÀ-ÿ'’.\-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'’.\-]+){0,3})
        \s*[-–—:]\s*
        (?P<score>\d{1,3}(?:\.\d{1,3})?)
    """,
    re.VERBOSE,
)

SCORE_ONLY_RE = re.compile(r"\b(?P<score>\d{1,3}(?:\.\d{1,3})?)\b")
LIKELY_NAME_RE = re.compile(r"[A-Z][A-Za-zÀ-ÿ'’.\-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'’.\-]+){0,3}")


def looks_like_person_name(s: str) -> bool:
    s = presentable_name(s)
    if not s:
        return False
    if len(s) < 3:
        return False
    tokens = s.split()
    if len(tokens) == 1:
        # single tokens are allowed for unresolved export but not strong auto-match
        return tokens[0][0].isalpha()
    return all(t[0].isalpha() for t in tokens if t)


def looks_like_score_context(line: str) -> bool:
    lower = line.lower()
    keywords = [
        "score", "points", "final", "average", "technical", "artistic",
        "presentation", "execution", "routine", "freestyle", "shred", "sick 3",
    ]
    return any(k in lower for k in keywords) or bool(NAME_SCORE_RE.search(line))


def extract_score_mentions_from_line(line: str) -> list[dict]:
    out = []

    for m in NAME_SCORE_RE.finditer(line):
        out.append({
            "name_raw": presentable_name(m.group("name")),
            "score_raw": m.group("score"),
            "method": "name_score_inline",
        })

    pm = PLACEMENT_LINE_RE.match(line)
    if pm and pm.group("tail"):
        tail = pm.group("tail")
        score_match = SCORE_ONLY_RE.search(tail)
        if score_match and looks_like_score_context(line):
            out.append({
                "name_raw": presentable_name(pm.group("name")),
                "score_raw": score_match.group("score"),
                "method": "placement_line_tail_score",
            })

    return out


def extract_person_context_near_trick(line: str) -> list[str]:
    candidates = []

    pm = PLACEMENT_LINE_RE.match(line)
    if pm:
        n = presentable_name(pm.group("name"))
        if looks_like_person_name(n):
            candidates.append(n)

    for m in LIKELY_NAME_RE.finditer(line):
        n = presentable_name(m.group(0))
        if looks_like_person_name(n):
            candidates.append(n)

    # preserve order, unique
    seen = set()
    uniq = []
    for c in candidates:
        nk = norm_key(c)
        if nk and nk not in seen:
            uniq.append(c)
            seen.add(nk)
    return uniq


def extract_trick_mentions_from_line(line: str, compiled_trick_patterns: list[tuple[str, re.Pattern]]) -> list[dict]:
    out = []
    lower = line.lower()

    for canon_trick, pat in compiled_trick_patterns:
        for m in pat.finditer(line):
            raw = m.group(0)
            # avoid trivial generic tokens unless clearly in trick-ish context
            if canon_trick.lower() in GENERIC_NON_TRICK_TERMS:
                continue

            context_before = line[max(0, m.start() - 30):m.start()]
            context_after = line[m.end():m.end() + 30]
            context = f"{context_before}[{raw}]{context_after}"

            out.append({
                "trick_raw": raw,
                "trick_canon": canon_trick,
                "context_snippet": context,
                "line_lower": lower,
            })
    return out


# -----------------------------
# Main mining logic
# -----------------------------

def choose_text_col(df: pd.DataFrame) -> str:
    candidates = [
        "results_block_raw",
        "results_text",
        "raw_results",
        "results_raw",
        "event_text",
        "body_text",
    ]
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"Could not find text column. Tried: {candidates}")


def choose_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Mine scores and tricks from raw footbag event text.")
    ap.add_argument("--events", required=True, help="CSV with event-level raw text")
    ap.add_argument("--persons", required=True, help="Persons_Truth CSV")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--trick-lexicon", help="Optional CSV of trick names")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    events_df = pd.read_csv(args.events)
    persons_df = pd.read_csv(args.persons)

    text_col = choose_text_col(events_df)
    event_id_col = choose_col(events_df, ["event_id", "event_key", "legacy_event_id"])
    year_col = choose_col(events_df, ["year"])
    event_name_col = choose_col(events_df, ["event_name", "name"])
    city_col = choose_col(events_df, ["city"])
    country_col = choose_col(events_df, ["country"])

    exact_name_index, collisions = build_person_index(persons_df)
    tricks = load_trick_lexicon(Path(args.trick_lexicon) if args.trick_lexicon else None)
    compiled_tricks = compile_trick_patterns(tricks)

    score_rows = []
    trick_rows = []
    unresolved_counter = Counter()

    total_events = 0
    total_lines = 0

    for _, row in events_df.iterrows():
        raw_text = row.get(text_col)
        if pd.isna(raw_text) or not str(raw_text).strip():
            continue

        total_events += 1
        event_id = row.get(event_id_col) if event_id_col else None
        year = row.get(year_col) if year_col else None
        event_name = row.get(event_name_col) if event_name_col else None
        city = row.get(city_col) if city_col else None
        country = row.get(country_col) if country_col else None

        text = str(raw_text).replace("\r\n", "\n").replace("\r", "\n")
        lines = [norm_space(x) for x in text.split("\n") if norm_space(x)]
        total_lines += len(lines)

        for line_no, line in enumerate(lines, start=1):
            # ---- scores ----
            score_mentions = extract_score_mentions_from_line(line)
            for sm in score_mentions:
                pm = safe_match_person(sm["name_raw"], exact_name_index, collisions)
                if pm.match_type == "unresolved":
                    unresolved_counter[sm["name_raw"]] += 1

                score_rows.append({
                    "event_id": event_id,
                    "year": year,
                    "event_name": event_name,
                    "city": city,
                    "country": country,
                    "line_no": line_no,
                    "line_raw": line,
                    "name_raw": sm["name_raw"],
                    "person_id": pm.person_id,
                    "person_canon": pm.person_canon,
                    "match_type": pm.match_type,
                    "score_raw": sm["score_raw"],
                    "score_value": pd.to_numeric(sm["score_raw"], errors="coerce"),
                    "extract_method": sm["method"],
                })

            # ---- tricks ----
            trick_mentions = extract_trick_mentions_from_line(line, compiled_tricks)
            if trick_mentions:
                candidate_names = extract_person_context_near_trick(line)
                best_name = candidate_names[0] if candidate_names else None
                pm = safe_match_person(best_name, exact_name_index, collisions) if best_name else PersonMatch(None, None, "none", None)

                if best_name and pm.match_type == "unresolved":
                    unresolved_counter[best_name] += 1

                for tm in trick_mentions:
                    trick_rows.append({
                        "event_id": event_id,
                        "year": year,
                        "event_name": event_name,
                        "city": city,
                        "country": country,
                        "line_no": line_no,
                        "line_raw": line,
                        "trick_raw": tm["trick_raw"],
                        "trick_canon": tm["trick_canon"],
                        "name_raw": best_name,
                        "person_id": pm.person_id,
                        "person_canon": pm.person_canon,
                        "match_type": pm.match_type,
                        "context_snippet": tm["context_snippet"],
                        "extract_method": "inline_trick_match",
                    })

    score_df = pd.DataFrame(score_rows)
    trick_df = pd.DataFrame(trick_rows)

    unresolved_df = pd.DataFrame(
        [{"name_raw": k, "count": v} for k, v in unresolved_counter.most_common()],
        columns=["name_raw", "count"]
    )

    score_path = out_dir / "noise_score_mentions.csv"
    trick_path = out_dir / "noise_trick_mentions.csv"
    unresolved_path = out_dir / "noise_unresolved_names.csv"
    summary_path = out_dir / "noise_summary.json"

    score_df.to_csv(score_path, index=False, quoting=csv.QUOTE_MINIMAL)
    trick_df.to_csv(trick_path, index=False, quoting=csv.QUOTE_MINIMAL)
    unresolved_df.to_csv(unresolved_path, index=False, quoting=csv.QUOTE_MINIMAL)

    summary = {
        "events_processed": int(total_events),
        "lines_processed": int(total_lines),
        "score_mentions": int(len(score_df)),
        "score_mentions_resolved": int(score_df["person_id"].notna().sum()) if len(score_df) else 0,
        "trick_mentions": int(len(trick_df)),
        "trick_mentions_resolved": int(trick_df["person_id"].notna().sum()) if len(trick_df) else 0,
        "unique_unresolved_names": int(len(unresolved_df)),
        "trick_lexicon_size": int(len(tricks)),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("Wrote:")
    print(f"  {score_path}")
    print(f"  {trick_path}")
    print(f"  {unresolved_path}")
    print(f"  {summary_path}")
    print("\nSummary:")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
