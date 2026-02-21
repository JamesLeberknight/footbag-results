#!/usr/bin/env python3
"""
14_seed_person_ids_for_unmapped.py
Propose person_id candidates for unmapped, person-like names. Never writes to overrides.

NO GUESSING:
- Does NOT merge names.
- Only outputs candidates for exact strings that currently have no person_id.
- Human reviews out/person_seed_candidates.csv and promotes into overrides as desired.

Reads:
  out/Placements_Flat.csv
  overrides/person_aliases.csv (optional; existing aliases skipped)

Writes:
  out/person_seed_candidates.csv
    Columns: raw_name, proposed_person_id, seed_type, reason, appearances
"""

from __future__ import annotations

import csv
import re
import uuid
from pathlib import Path
from collections import Counter

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "out" / "Placements_Flat.csv"
ALIASES = REPO / "overrides" / "person_aliases.csv"
SEED_CANDIDATES = REPO / "out" / "person_seed_candidates.csv"

OUT_HEADERS = ["raw_name", "proposed_person_id", "seed_type", "reason", "appearances"]

def norm(s: str) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    return " ".join(s.strip().split())

def looks_like_person_candidate(s: str) -> bool:
    if not s:
        return False

    raw = s.strip()
    low = raw.lower()

    # reject obvious junk
    if len(raw) < 5:
        return False

    # numbers / score patterns
    if re.search(r"\d{2,}", raw):
        return False
    if re.search(r"\b(points?|kicks?|score|rank|position)\b", low):
        return False

    # multi-person joiners
    if any(x in raw for x in [" + ", " & ", " and ", "/", "\\"]):
        return False

    # commentary / analysis words
    BAD_WORDS = [
        "with", "points", "food", "processor", "symposium",
        "dictators", "fenix", "paradox", "whirl"
    ]
    if any(w in low for w in BAD_WORDS):
        return False

    # parentheses allowed ONLY for nationality
    if "(" in raw or ")" in raw:
        if not re.fullmatch(r".+\([A-Z]{2,3}\)", raw):
            return False

    # must look like a name
    tokens = re.findall(r"[A-Za-zÀ-ž]+", raw)
    if len(tokens) < 2:
        return False

    return True

def load_existing_aliases(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return {norm(row.get("alias", "")) for row in r if norm(row.get("alias", ""))}


def main() -> int:
    if not OUT.exists():
        print(f"ERROR: missing {OUT} (run 02p5 first)")
        return 2

    existing = load_existing_aliases(ALIASES)

    pf = pd.read_csv(OUT)

    # Count unmapped name appearances (both sides)
    appearance_counts: Counter[str] = Counter()
    for side in [1, 2]:
        name_col = f"player{side}_name"
        pid_col = f"player{side}_person_id"
        if name_col not in pf.columns or pid_col not in pf.columns:
            continue
        sub = pf[(pf[name_col].fillna("").astype(str).str.strip() != "") &
                 (pf[pid_col].fillna("").astype(str).str.strip() == "")]
        for n in sub[name_col].fillna("").astype(str).map(norm):
            if n:
                appearance_counts[n] += 1

    candidates = sorted(
        n for n in appearance_counts
        if looks_like_person_candidate(n) and n not in existing
    )
    if not candidates:
        print("INFO: no new unmapped person-like names to seed.")
        return 0

    new_rows = []
    for raw_name in candidates:
        new_rows.append({
            "raw_name": raw_name,
            "proposed_person_id": str(uuid.uuid4()),
            "seed_type": "person_candidate",
            "reason": "unmapped_personlike",
            "appearances": appearance_counts[raw_name],
        })

    SEED_CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    with SEED_CANDIDATES.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUT_HEADERS)
        w.writeheader()
        for row in new_rows:
            w.writerow(row)

    print(f"OK: wrote {len(new_rows)} seed candidates to {SEED_CANDIDATES}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
