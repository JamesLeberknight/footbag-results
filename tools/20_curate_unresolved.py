#!/usr/bin/env python3
"""
20_curate_unresolved.py — Curate Persons_Unresolved_Organized_v15 → v16

Operations (all human-approved 2026-02-26):
  1. NONPERSON: reclassify garbage entries as __NON_PERSON__ in Placements,
     remove from Unresolved.
  2. CANON_REMAP: strip city / nickname / trick noise from single-person
     entries.  If cleaned canon is in Truth, entry exits Unresolved and
     Placements rows link to Truth.  Otherwise Unresolved canon is updated
     and Placements person_canon is updated to match.
  3. SPLIT_NOTES: add confirmed-split notes to doubles-team entries.
  4. DEDUP: merge rows that share the same canon after all remaps.

Outputs:
  inputs/identity_lock/Persons_Unresolved_Organized_v16.csv
  inputs/identity_lock/Placements_ByPerson_v19.csv

Usage:
  python tools/20_curate_unresolved.py           # dry run
  python tools/20_curate_unresolved.py --apply   # write files
"""

from __future__ import annotations

import argparse
import sys
import unicodedata
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v15.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v18.csv"
TRUTH_IN       = IDENTITY_LOCK / "Persons_Truth_Final_v18.csv"

UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v16.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v19.csv"

# ---------------------------------------------------------------------------
# 1. NONPERSON — these entries are garbage; placements → __NON_PERSON__
# ---------------------------------------------------------------------------

NONPERSON: set[str] = {
    "Annual Vancouver Open Footbag Net Tournament",
    "Annual New Zealand Footbag Championships",
    "Annual Emerald City Open Footbag Tournament",
    "TORNEO NACIONAL DE FOOTBAG",
    "Score ADD Uniques Contacts",
    "Adds Contacts Ratio Uniques Score",
    "Uhr war traditioneller Treffpunkt beim Griechen",
    "time singles net world champion",
    "rd",
    "nd",
    "BS Assassin",
    "PS Eggbeater",
    "Platz nur Gnitzen und Ü 30 Connection Gimbel Heimer",
    "players competed in 7 pools",
    "French ConneXion 75 ADDS",
    "Ville &",
    "Poule A",
    "Müller Didn\u00b4t show up for semifinals....",
    "Vijay of UPENN footbag club",
}

# ---------------------------------------------------------------------------
# 2. CANON_REMAP — old person_canon → cleaned person_canon
#    If cleaned canon is in Truth the Unresolved row is dropped (Placements
#    rows will then link to Truth via the cleaned canon).
# ---------------------------------------------------------------------------

CANON_REMAP: dict[str, str] = {
    # --- Category D: person + city suffix ---
    "Monica Sandoval ? Dallas":     "Monica Sandoval",
    "Windsen Pan ? Houston":        "Windsen Pan",
    "Steven Sevilla ? Houston":     "Steven Sevilla",
    "Tina Lewis ? Austin":          "Tina Lewis",
    "Tim Vozar ? Houston":          "Tim Vozar",
    "Mike Lopez ? San Antonio":     "Mike Lopez",
    "Derric Scalf ? Dallas":        "Derric Scalf",
    "Curtis Taylor ? Houston":      "Curtis Taylor",
    "James Geraci ? Austin":        "James Geraci",
    "James Roberts ? Dallas":       "James Roberts",
    "Josh Bast ? San Antonio":      "Josh Bast",
    "Jose' Cocolan ? San Antonio":  "Jose Cocolan",
    "Jocelyn Sandoval ? Dallas":    "Jocelyn Sandoval",
    "Ryan Burt Idaho Falls":        "Ryan Burt",
    "Eagan Heath Idaho Falls":      "Eagan Heath",
    "Kelsey Duncan Idaho Falls":    "Kelsey Duncan",
    "Luke Anderson Salt Lake City": "Luke Anderson",
    "Cameron Dowie Salt Lake City": "Cameron Dowie",

    # --- Category E: nickname / alias / club noise ---
    "Arthur Ledain aka Tutur":                        "Arthur Ledain",
    "Ken The Hammer Hamric":                          "Ken Hamric",
    "Jochen Bauer aka Doctor Jay":                    "Jochen Bauer",
    "The Energizer Benny Kellman":                    "Benny Kellman",
    "Lionel Veluire aka Yommish":                     "Lionel Veluire",
    "Victor Old School Burnham":                      "Victor Burnham",
    "Jamie Top Dog Lepley":                           "Jamie Lepley",
    "KV aka Thorsten Schäfer":                        "Thorsten Schäfer",
    "Faris Larry aka der Feuersalamander Barakat":    "Faris Barakat",
    "Jake Jo Dodd ID":                                "Jake Dodd",
    "Nicolas De Zeeuw Icarus":                        "Nicolas De Zeeuw",
    "Andy Silvy Freedom Footbag":                     "Andy Silvy",
    "Olivier Fages PX Club":                          "Olivier Fages",
    "Baptiste Supan PX Club":                         "Baptiste Supan",
    "Ben Barrows Abshire Footbag":                    "Ben Barrows",
    "Brendan Erskine Melb Footbag Club":              "Brendan Erskine",
    "Matthias Lino Schmidt FCFoostar":                "Matthias Lino Schmidt",
    "Gareth Williams Logan HackStars":                "Gareth Williams",
    "Audrey Tumelin Stuff Nold":                      "Audrey Tumelin",

    # --- Category F: trick set / score annotation noise ---
    "Peter Holoien paradon swirl":                    "Peter Holoien",
    "Motorov Pavel blurry merlin":                    "Pavel Motorov",       # name reversed
    "Pomanov Andrey spinning ducking motion":          "Andrey Pomanov",      # name reversed
    "Egorov Andrey alpline blurry whirl":             "Andrey Egorov",       # name reversed
    "Evan Gatesman blurry whirling swirl":            "Evan Gatesman",
    "Tina Aeberli Blurry Whirling Swirl":             "Tina Aeberli",
    "Gabriel Gaudette with Pixie Ducking Butterfly":  "Gabriel Gaudette",
    "Serge Kaldany Pixie Zooloo Symposium Whirl":     "Serge Kaldany",
    "Stefan Siegert Quantum Butterfly Swirl":         "Stefan Siegert",
    "Max Kerkoff Back side Fury":                     "Max Kerkoff",
    "Karim Daouk Alpine Food Processor":              "Karim Daouk",

    # --- ACB entries with score / state / trick noise ---
    "René Rühr ? Whirr":            "René Rühr",
    "Garikoitz Casquero 1 point":   "Garikoitz Casquero",
    "Ander López 8 points":         "Ander López",
    "Josu Royuela 1 points":        "Josu Royuela",
    "Stephen R. Richardson ID":     "Stephen R. Richardson",
    "Ben De Bastos ID":             "Benjamin De Bastos",   # dedup → Benjamin De Bastos
    "Benjamin De Bastos ID":        "Benjamin De Bastos",
}

# ---------------------------------------------------------------------------
# 3. SPLIT_NOTES — confirmed doubles-team entries; add note with split point
# ---------------------------------------------------------------------------

SPLIT_NOTES: dict[str, str] = {
    # Category A — already have notes from previous session, skip
    # Category B — separator-word splits (new)
    "Andy Götze und Flo Wolff":                "confirmed split: Andy Götze / Flo Wolff",
    "Christian Löwe und Hanneé Tiger":         "confirmed split: Christian Löwe / Hannée Tiger",
    "Mikkel Frederiksen og Thomas Mortensen":  "confirmed split: Mikkel Frederiksen / Thomas Mortensen",
    "Ole Snack og Ryan Mulroney":              "confirmed split: Ole Snack / Ryan Mulroney",
    "Bjarne Everberg og Benny Leich":          "confirmed split: Bjarne Everberg / Benny Leich",
    "Pavel Hejra a Petr Stejskal":             "confirmed split: Pavel Hejra / Petr Stejskal",
    "Dexter a Pavel Èervený":                  "confirmed split: Dexter / Pavel Červený",
    "Martin a Honza Hulejovi":                 "confirmed split: Martin Hulejovi / Honza Hulejovi",
    "YEISON OCAMPO Y ANDRES GALLEGO":          "confirmed split: Yeison Ocampo / Andres Gallego",
    "SEBASTIAN CEBALLOS Y ANDRES ZAPATA":      "confirmed split: Sebastian Ceballos / Andres Zapata",
    "ALEX LOPEZ Y GABRIEL BOHORQUEZ":          "confirmed split: Alex Lopez / Gabriel Bohorquez",
    "ANDRES ARCE Y BERNARDO PALACIOS":         "confirmed split: Andres Arce / Bernardo Palacios",
    "ANTONO LINERO Y ALBERTO PEREZ":           "confirmed split: Antonio Linero / Alberto Perez",
    "EDISSON DUQUE Y GIANY":                   "confirmed split: Edisson Duque / Giany",
    # Category C — concatenated splits (new)
    "Nicolas De Zeeuw Serge Kaldany":          "confirmed split: Nicolas De Zeeuw / Serge Kaldany",
    "Benjamin De Bastos Louis Marchadier":     "confirmed split: Benjamin De Bastos / Louis Marchadier",
    "Maude Landreville CAN Lena Mlakar":       "confirmed split: Maude Landreville / Lena Mlakar",
    "Luke Legault CAN Lena Mlakar":            "confirmed split: Luke Legault / Lena Mlakar",
    "Lena Mlakar Jereb Wlady Pachexo":         "confirmed split: Lena Mlakar Jereb / Wlady Pachexo",
    "Andreas Beimel Frenzel Eduardo Martinez": "confirmed split: Andreas Beimel Frenzel / Eduardo Martinez",
    "Xavier Lancret Boris Julien Ollivier":    "confirmed split: Xavier Lancret / Boris Julien Ollivier",
    # ACB ?-separator pairs — add note for clarity
    "Gina Meyer J.J. Jones":                   "confirmed split: Gina Meyer / J.J. Jones",
    "Marc Weber* Bob Silva":                   "confirmed split: Marc Weber / Bob Silva",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RE_PUNCT = re.compile(r"[^\w\s]")
_RE_WS    = re.compile(r"\s+")


def normalize_key(name: str) -> str:
    s = unicodedata.normalize("NFKD", name.strip())
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = _RE_PUNCT.sub(" ", s)
    s = _RE_WS.sub(" ", s).strip()
    return s


def token_count(name: str) -> int:
    return len(name.strip().split())


def append_note(existing: str, new_note: str) -> str:
    existing = existing.strip()
    if not existing:
        return new_note
    if new_note in existing:
        return existing
    return existing + " | " + new_note


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Curate Persons_Unresolved v15→v16.")
    parser.add_argument("--apply", action="store_true",
                        help="Write output files (default: dry run)")
    args = parser.parse_args()
    dry = not args.apply

    # Load inputs
    unr = pd.read_csv(UNRESOLVED_IN, dtype=str).fillna("")
    pla = pd.read_csv(PLACEMENTS_IN, dtype=str).fillna("")
    tru = pd.read_csv(TRUTH_IN,      dtype=str).fillna("")

    truth_canons: set[str] = set(tru["person_canon"].str.strip().unique())

    print(f"Loaded {len(unr)} Unresolved rows")
    print(f"Loaded {len(pla)} Placements rows")
    print(f"Loaded {len(tru)} Truth rows")
    print()

    stats = {
        "nonperson_removed":      0,
        "nonperson_pla_updated":  0,
        "remap_to_truth":         0,
        "remap_to_truth_pla":     0,
        "remap_kept":             0,
        "remap_kept_pla":         0,
        "split_notes_added":      0,
        "dedup_removed":          0,
    }

    # --- Step 1: apply CANON_REMAP to Unresolved ---
    remap_note = "2026-02-26: canon cleaned (stripped city/nickname/trick noise)"
    for old, new in CANON_REMAP.items():
        mask = unr["person_canon"] == old
        if not mask.any():
            continue
        unr.loc[mask, "person_canon"] = new
        unr.loc[mask, "token_count"] = str(token_count(new))
        unr.loc[mask, "norm_key"]    = normalize_key(new)
        unr.loc[mask, "notes"]       = unr.loc[mask, "notes"].apply(
            lambda n: append_note(n, remap_note)
        )

    # --- Step 2: apply CANON_REMAP to Placements ---
    for old, new in CANON_REMAP.items():
        pla_mask = pla["person_canon"] == old
        n = pla_mask.sum()
        if n == 0:
            continue
        pla.loc[pla_mask, "person_canon"] = new
        if new in truth_canons:
            stats["remap_to_truth_pla"] += n
        else:
            stats["remap_kept_pla"] += n

    # --- Step 3: determine which Unresolved rows exit (resolve to Truth) ---
    exits_to_truth = unr["person_canon"].isin(truth_canons)
    stats["remap_to_truth"] = int(exits_to_truth.sum())
    print(f"Step 2/3: {stats['remap_to_truth']} Unresolved rows now resolve to Truth "
          f"(canon matches Truth entry) — removing from Unresolved")

    # --- Step 4: NONPERSON — update Placements, mark for removal ---
    for c in NONPERSON:
        # Placements
        pla_mask = pla["person_canon"] == c
        n = pla_mask.sum()
        if n:
            pla.loc[pla_mask, "person_canon"] = "__NON_PERSON__"
            pla.loc[pla_mask, "person_id"]    = ""
            stats["nonperson_pla_updated"] += n
        # Unresolved — mark for removal
        unr_mask = unr["person_canon"] == c
        if unr_mask.any():
            stats["nonperson_removed"] += int(unr_mask.sum())

    print(f"Step 4: {stats['nonperson_removed']} NONPERSON Unresolved rows removed; "
          f"{stats['nonperson_pla_updated']} Placements rows → __NON_PERSON__")

    # --- Step 5: SPLIT_NOTES ---
    split_note_pfx = "2026-02-26: "
    for canon, note in SPLIT_NOTES.items():
        mask = unr["person_canon"] == canon
        if not mask.any():
            continue
        full_note = split_note_pfx + note
        unr.loc[mask, "notes"] = unr.loc[mask, "notes"].apply(
            lambda n: append_note(n, full_note)
        )
        stats["split_notes_added"] += int(mask.sum())

    print(f"Step 5: {stats['split_notes_added']} split notes added")

    # --- Step 6: Filter Unresolved (remove exits and NONPERSON) ---
    keep_mask = (~exits_to_truth) & (~unr["person_canon"].isin(NONPERSON))
    unr = unr[keep_mask].copy()
    print(f"After filtering: {len(unr)} Unresolved rows remain")

    # --- Step 7: Dedup — rows with identical person_canon, keep first ---
    before_dedup = len(unr)
    unr = unr.drop_duplicates(subset=["person_canon"], keep="first")
    stats["dedup_removed"] = before_dedup - len(unr)
    print(f"Step 7: {stats['dedup_removed']} duplicate-canon rows removed → {len(unr)} rows")

    # --- Step 8: Update token_count for all rows (recalculate) ---
    unr["token_count"] = unr["person_canon"].apply(lambda c: str(token_count(c)))

    # --- Summary ---
    print()
    print("=== DRY RUN SUMMARY ===" if dry else "=== APPLYING ===")
    print(f"  Unresolved: {419} → {len(unr)} rows "
          f"(-{419 - len(unr)} removed)")
    print(f"    of which exits-to-Truth:  {stats['remap_to_truth']}")
    print(f"    of which NONPERSON drops: {stats['nonperson_removed']}")
    print(f"    of which dedup drops:     {stats['dedup_removed']}")
    print(f"  Placements updated:")
    print(f"    canon→Truth remaps:       {stats['remap_to_truth_pla']} rows")
    print(f"    canon→newcanon remaps:    {stats['remap_kept_pla']} rows")
    print(f"    canon→__NON_PERSON__:     {stats['nonperson_pla_updated']} rows")
    print(f"  Placements total: {len(pla)} rows (unchanged count)")

    # --- Verify: no Unresolved canon should be in Truth ---
    collisions = set(unr["person_canon"]) & truth_canons
    if collisions:
        print(f"\nWARNING: {len(collisions)} Unresolved canons still in Truth:")
        for c in sorted(collisions):
            print(f"  {c!r}")

    if dry:
        print()
        print("Dry run complete. Pass --apply to write files.")
        return 0

    # --- Write outputs ---
    IDENTITY_LOCK.mkdir(exist_ok=True)
    unr.to_csv(UNRESOLVED_OUT, index=False)
    pla.to_csv(PLACEMENTS_OUT, index=False)
    print()
    print(f"Wrote: {UNRESOLVED_OUT} ({len(unr)} rows)")
    print(f"Wrote: {PLACEMENTS_OUT} ({len(pla)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
