#!/usr/bin/env python3
"""
29_auto_coverage_backfill_resolution.py — Resolve all 194 AUTO_COVERAGE_BACKFILL
Unresolved entries remaining in v25.

Five groups:

  Group 1 — STALE_REMOVE (26 entries): Unresolved entries with 0 PBP rows.
    Already handled by earlier tools or the ? separator form was never assigned
    in PBP. Remove from Unresolved only — no PBP change.

  Group 2 — NOISE (18 entries): Hyphen-separated doubles pairs and non-person
    entries that have PBP rows. Set person_canon → __NON_PERSON__, clear UUID.
    Remove from Unresolved.

  Group 3 — CANON_CORRECT_TO_TRUTH (11 entries): Corrupted/variant person_canon
    in PBP → correct canonical form already in Truth. Assign existing UUID.
    Remove from Unresolved.

  Group 4 — DIRECT_NEW_PERSONS (29 entries): Real persons whose canonical form
    already exists correctly in PBP. Create Truth row, assign UUID, remove
    from Unresolved.

  Group 5 — CANON_CORRECT_NEW_PERSON (18 PBP corrections → 13 unique new persons):
    Corrupted/reversed/variant person_canon in PBP → correct canonical form that
    is also a NEW_PERSON. Create Truth row, update PBP canon, assign UUID.
    Remove source and (if in Unresolved) target from Unresolved.

Inputs:
  inputs/identity_lock/Persons_Truth_Final_v28.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v25.csv
  inputs/identity_lock/Placements_ByPerson_v29.csv

Outputs (with --apply):
  inputs/identity_lock/Persons_Truth_Final_v29.csv
  inputs/identity_lock/Persons_Unresolved_Organized_v26.csv
  inputs/identity_lock/Placements_ByPerson_v30.csv
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN       = IDENTITY_LOCK / "Persons_Truth_Final_v28.csv"
UNRESOLVED_IN  = IDENTITY_LOCK / "Persons_Unresolved_Organized_v25.csv"
PLACEMENTS_IN  = IDENTITY_LOCK / "Placements_ByPerson_v29.csv"

TRUTH_OUT      = IDENTITY_LOCK / "Persons_Truth_Final_v29.csv"
UNRESOLVED_OUT = IDENTITY_LOCK / "Persons_Unresolved_Organized_v26.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v30.csv"

_NS = uuid.uuid5(uuid.NAMESPACE_URL, "footbag-results-identity")

def make_uuid(canon: str) -> str:
    return str(uuid.uuid5(_NS, canon))


# ---------------------------------------------------------------------------
# Group 1 — STALE_REMOVE: 0 PBP rows, remove from Unresolved only
# ---------------------------------------------------------------------------
STALE_REMOVE: set[str] = {
    # 14 "? separator" doubles pairs — never assigned in PBP (0 PBP rows)
    "Lisa Uebele ? Kerstin Anhuth",
    "Ninni Liukko ? Piia Tantarimäki",
    "Piia Tantarimäki ? Uli Haase",
    "Petr Stejskal ? Pavel Cerveny",
    "Nicolas de Zeeuw ? Miquel Clemente",
    "Ninni Liukko ? Yassin Khateeb",
    "Yassin Khateeb ? Markus Portenkirchner",
    "Andreas Wolff ? Uli Haase",
    "Kerstin Anhuth ? Chris Löw",
    "Michał Róg ? Michał Klimczak",
    "Lino Landau ? Marc Brunet",
    "Lisa Uebele ? Andreas Wolff",
    "Ludovic Lacaze ? Eric Fonteneau",
    "Manuel Kruse ? Simon Voss",
    # Encoding-corrupted entries already handled in PBP by earlier tools
    "Mikulá¹ Èáp",           # Czech corruption of "Mikuláš Čáp"
    "Tomá¹ Mirovský",         # Czech corruption
    "Matja? Borič",           # Slovenian corruption
    "Robin P¸chel",           # encoding artifact
    "tivteřinová smr?ť triků", # Czech trick description, not a person
    "Gregor Morel Ale¹ Pelko",  # Slovenian doubles pair, encoding corrupt
    "Dominic O?Brien-Stéphane Tailleur",  # doubles pair, encoding corrupt
    "Marc Weber* Bob Silva",   # confirmed split (Marc Weber / Bob Silva), handled
    "Ian Pfeiffer ?",          # trailing ? is unknown partner, Ian Pfeiffer already in Truth
    "thru 8th",                # placeholder text, not a person
    "Ulrike Häßler",           # 0 PBP direct (PBP has corrupted form "Ulrike H?fller")
    "Robert Szymański",        # 0 PBP (PBP has reversed form "Szymański Robert")
}

# ---------------------------------------------------------------------------
# Group 2 — NOISE: → __NON_PERSON__ in PBP + remove from Unresolved
# ---------------------------------------------------------------------------
NOISE: set[str] = {
    # Hyphen-separated doubles pairs (two persons stored as one canon)
    "Eduardo Martinez-Diego Chávez",
    "Mark Hunsberger-Josh DeClercq",
    "Francis Guimond-Stéphane Roy",
    "Andreina Peńa-Reinaldo Pérez",
    "Julio Garcia-Ángel Vivas",
    "Victor Lezama-Ángel Hernández",
    "PAVEL HEJRA-PETR FUCIK",
    "PAVEL HEJRA-PATRIK CERNY",
    "Gina Meyer J.J. Jones",    # space-separated doubles pair (Gina Meyer / J.J. Jones)
    # Non-person entries
    "Ronalde plus H",           # "Ronalde" + his water carrier (joke team)
    "Team Magic",
    "Team Spirit",
    "Team Tüte",
    "DER CHAMP",                # nickname/title, not a real person name
    "IRON MAN",                 # event category / nickname
    "LEG OVER.",                # trick/move name
    "Min. Timed Consecutives",  # event sub-division label
    "Andre P. Aberration",      # trick name / performance label
}

# ---------------------------------------------------------------------------
# Group 3 — CANON_CORRECT → existing Truth
# (update PBP: reassign UUID from Truth target; remove from Unresolved)
# ---------------------------------------------------------------------------
CANON_CORRECT_TO_TRUTH: dict[str, str] = {
    "yves kreil":          "Yves Kreil",
    "Alex Trenne":         "Alexander Trenner",
    "Alex Trener":         "Alexander Trenner",
    "Krysiewicz Łukasz":   "Łukasz Krysiewicz",
    "Walter Houston ID":   "Walt Houston",
    "Luka WeyLav ID":      "Luka Weyler",
    "De Zeeuw":            "Nicolas De Zeeuw",
    "X. Anhuth":           "Kerstin Anhuth",
    "X. Hankins":          "Jim Hankins",
    "Arnaud Mamoute Saniez": "Arnaud Saniez",    # "Mamoute" is a nickname
    "Greg GFSmoothie Nelson": "Greg Nelson",      # "GFSmoothie" is an online handle
}

# ---------------------------------------------------------------------------
# Group 4 — DIRECT_NEW_PERSONS: canonical form already correct in PBP
# (create Truth row, assign UUID, remove from Unresolved)
# ---------------------------------------------------------------------------
DIRECT_NEW_PERSONS: list[str] = [
    # Polish competitors
    "Mikołaj Kulesz",
    "Przemysław Popławski",
    "Radek Łątka",
    "Paweł Ptaszyński",
    "Paweł Kosoń",
    "Paweł Ciepielski",
    "Paweł Holiczko",      # also has reversed form "Holiczko Paweł" in PBP (Group 5)
    "Łukasz Bochenek",
    "Filip Prędkiewicz",
    "Kacper Prędkiewicz",
    "Mateusz Związek",
    "Michał Pietryńczak",
    "Michał Przybyłowicz",
    "Jakub Ścisiński",
    "Grzesiek Łatuszyński",
    "Michał Zembaty",
    "Marcin Gadziński",    # also has reversed form "Gadziński Marcin" in PBP (Group 5)
    "Natalia Fryś",        # different from Natalia Fry (different events/years)
    # Czech competitors
    "Patrik Šmerda",
    "Michal Černý",
    # French/other
    "Olivier B.-Bergé",
    "Boris de nantes",
    "Ronald Ańez",
    # Other individuals
    "Dave Hill",
    "DJ Dourney",
    "Nils G. Unna",
    "Stephen R. Richardson",
    "Dr. Mike Stefanelli",   # also has quoted form "'Dr. Mike' Stefanelli" in PBP (Group 5)
    "Michal Hadaś",          # Polish (Michal without ł — matches PBP form)
]

# ---------------------------------------------------------------------------
# Group 5 — CANON_CORRECT → NEW_PERSON
# (source exists in PBP; target becomes NEW_PERSON if not already in Truth)
# ---------------------------------------------------------------------------
CANON_CORRECT_NEW: dict[str, str] = {
    # Reversed Polish surnames
    "Szymański Robert":  "Robert Szymański",
    "Gadziński Marcin":  "Marcin Gadziński",   # direct form also in Group 4
    "Holiczko Paweł":    "Paweł Holiczko",     # direct form also in Group 4
    "Śmigulski Cezary":  "Cezary Śmigulski",
    # Encoding corruption
    "Ulrike H?fller":         "Ulrike Häßler",
    "Alejandro Rueda Patińo": "Alejandro Rueda Patiño",
    # All-caps → proper case
    "KEN SAMS":     "Ken Sams",
    "KEN SCHUYLER": "Ken Schuyler",
    "IAN PRICE":    "Ian Price",
    "BEB RIEFER":   "Beb Riefer",
    # Noise prefix/middle
    "Nick A Szwarc":   "Nick Szwarc",
    "Jack s Bissell":  "Jack Bissell",
    "d Eric Schmidt":  "Eric Schmidt",
    "d Michael Wilson":"Michael Wilson",
    # JF Lemieux spelling variants (3 sources → 1 target)
    "JF Lemeiux": "JF Lemieux",
    "JF Lemiux":  "JF Lemieux",
    "JF Lonieux": "JF Lemieux",
    # Quoted nickname form
    "'Dr. Mike' Stefanelli": "Dr. Mike Stefanelli",  # direct form also in Group 4
}


def run(apply: bool) -> None:
    truth = pd.read_csv(TRUTH_IN, low_memory=False)
    unresolved = pd.read_csv(UNRESOLVED_IN, low_memory=False)
    pbp = pd.read_csv(PLACEMENTS_IN, low_memory=False)

    print(f"Truth in:      {len(truth)} rows")
    print(f"Unresolved in: {len(unresolved)} rows")
    print(f"PBP in:        {len(pbp)} rows")
    print()

    # ------------------------------------------------------------------
    # Build Truth lookup: canon → UUID
    # ------------------------------------------------------------------
    truth_lookup: dict[str, str] = dict(
        zip(truth["person_canon"], truth["effective_person_id"])
    )

    # ------------------------------------------------------------------
    # Verify Group 3 targets exist in Truth
    # ------------------------------------------------------------------
    ok = True
    for src, tgt in CANON_CORRECT_TO_TRUTH.items():
        if tgt not in truth_lookup:
            print(f"  ERROR: CANON_CORRECT_TO_TRUTH target not in Truth: {tgt!r}")
            ok = False
    if not ok:
        sys.exit(1)

    # ------------------------------------------------------------------
    # Build complete set of new person canons (Groups 4 + 5 targets)
    # Deduplicate: some canons appear in both Group 4 and Group 5.
    # ------------------------------------------------------------------
    all_new_canons: list[str] = []
    seen: set[str] = set()
    for canon in DIRECT_NEW_PERSONS:
        if canon not in truth_lookup and canon not in seen:
            all_new_canons.append(canon)
            seen.add(canon)
    for tgt in CANON_CORRECT_NEW.values():
        if tgt not in truth_lookup and tgt not in seen:
            all_new_canons.append(tgt)
            seen.add(tgt)

    print(f"Group 1 — STALE_REMOVE:          {len(STALE_REMOVE)} entries")
    print(f"Group 2 — NOISE:                  {len(NOISE)} entries")
    print(f"Group 3 — CANON_CORRECT_TO_TRUTH: {len(CANON_CORRECT_TO_TRUTH)} entries")
    print(f"Group 4 — DIRECT_NEW_PERSONS:     {len(DIRECT_NEW_PERSONS)} entries")
    print(f"Group 5 — CANON_CORRECT_NEW:      {len(CANON_CORRECT_NEW)} sources → "
          f"{len(set(CANON_CORRECT_NEW.values()))} unique targets")
    print(f"New Truth rows to create:         {len(all_new_canons)}")
    print()

    # ------------------------------------------------------------------
    # Verify all expected canons exist in PBP before applying
    # ------------------------------------------------------------------
    for canon in NOISE:
        if not (pbp["person_canon"] == canon).any():
            print(f"  WARN: NOISE canon not found in PBP: {canon!r}")
    for src in CANON_CORRECT_TO_TRUTH:
        if not (pbp["person_canon"] == src).any():
            print(f"  WARN: CANON_CORRECT_TO_TRUTH source not in PBP: {src!r}")
    for src in CANON_CORRECT_NEW:
        if not (pbp["person_canon"] == src).any():
            # Some targets may already be the PBP canon (reversed-name case where
            # the correct form also exists in PBP); source may still need correction.
            # Just warn, don't abort.
            print(f"  WARN: CANON_CORRECT_NEW source not in PBP: {src!r}")
    print()

    if not apply:
        # Dry-run: show what would happen
        print("=== DRY RUN (pass --apply to execute) ===")

        print("\nGroup 1 — STALE_REMOVE (remove from Unresolved only):")
        ur_canons = set(unresolved["person_canon"].dropna())
        for canon in sorted(STALE_REMOVE):
            in_ur = canon in ur_canons
            in_pbp = (pbp["person_canon"] == canon).any()
            print(f"  {'found' if in_ur else 'MISSING'} in Unresolved | "
                  f"{'found' if in_pbp else '0 rows'} in PBP | {canon!r}")

        print(f"\nGroup 2 — NOISE → __NON_PERSON__ ({len(NOISE)} entries)")
        for canon in sorted(NOISE):
            n = (pbp["person_canon"] == canon).sum()
            print(f"  {n} PBP rows → __NON_PERSON__ | {canon!r}")

        print(f"\nGroup 3 — CANON_CORRECT → Truth ({len(CANON_CORRECT_TO_TRUTH)} entries)")
        for src, tgt in CANON_CORRECT_TO_TRUTH.items():
            n = (pbp["person_canon"] == src).sum()
            uid = truth_lookup.get(tgt, "???")
            print(f"  {n} PBP rows | {src!r} → {tgt!r} (UUID {uid[:8]}...)")

        print(f"\nGroup 4 — DIRECT_NEW_PERSONS ({len(DIRECT_NEW_PERSONS)} entries)")
        for canon in DIRECT_NEW_PERSONS:
            n = (pbp["person_canon"] == canon).sum()
            uid = make_uuid(canon)
            print(f"  {n} PBP rows | NEW_PERSON {canon!r} (UUID {uid[:8]}...)")

        print(f"\nGroup 5 — CANON_CORRECT_NEW ({len(CANON_CORRECT_NEW)} corrections)")
        for src, tgt in CANON_CORRECT_NEW.items():
            n = (pbp["person_canon"] == src).sum()
            uid = make_uuid(tgt)
            print(f"  {n} PBP rows | {src!r} → {tgt!r} (UUID {uid[:8]}...)")

        return

    # ------------------------------------------------------------------
    # APPLY
    # ------------------------------------------------------------------

    # Step 1: Collect Unresolved canons to remove
    to_remove_from_unresolved: set[str] = set()
    to_remove_from_unresolved |= STALE_REMOVE
    to_remove_from_unresolved |= NOISE
    to_remove_from_unresolved |= set(CANON_CORRECT_TO_TRUTH.keys())
    to_remove_from_unresolved |= set(DIRECT_NEW_PERSONS)
    to_remove_from_unresolved |= set(CANON_CORRECT_NEW.keys())
    # Also remove canonical forms of Group 5 targets if they appear in Unresolved
    # (e.g., "Marcin Gadziński", "Paweł Holiczko", "Dr. Mike Stefanelli" are
    # both Group 4 direct and Group 5 targets; "Robert Szymański" is in STALE_REMOVE)
    to_remove_from_unresolved |= set(CANON_CORRECT_NEW.values())

    # Step 2: Apply NOISE → __NON_PERSON__ in PBP
    noise_mask = pbp["person_canon"].isin(NOISE)
    pbp.loc[noise_mask, "person_canon"] = "__NON_PERSON__"
    pbp.loc[noise_mask, "person_id"] = float("nan")
    pbp.loc[noise_mask, "person_unresolved"] = False
    print(f"Group 2: set {noise_mask.sum()} PBP rows → __NON_PERSON__")

    # Step 3: Apply CANON_CORRECT_TO_TRUTH in PBP
    cc_truth_count = 0
    for src, tgt in CANON_CORRECT_TO_TRUTH.items():
        mask = pbp["person_canon"] == src
        n = mask.sum()
        if n > 0:
            uid = truth_lookup[tgt]
            pbp.loc[mask, "person_canon"] = tgt
            pbp.loc[mask, "person_id"] = uid
            pbp.loc[mask, "person_unresolved"] = False
            cc_truth_count += n
    print(f"Group 3: corrected {cc_truth_count} PBP rows → existing Truth UUID")

    # Step 4: Create new Truth rows (Groups 4 + 5)
    new_truth_rows = []
    for canon in all_new_canons:
        uid = make_uuid(canon)
        new_truth_rows.append({
            "effective_person_id": uid,
            "person_canon": canon,
            "player_ids_seen": uid,
            "player_names_seen": canon,
            "aliases": float("nan"),
            "alias_statuses": float("nan"),
            "notes": float("nan"),
            "source": "NEW_PERSON_v29",
        })
        truth_lookup[canon] = uid  # update lookup for subsequent PBP steps
    print(f"Groups 4+5: creating {len(new_truth_rows)} new Truth rows")

    # Step 5: Assign UUIDs to direct new persons in PBP
    direct_count = 0
    for canon in DIRECT_NEW_PERSONS:
        mask = pbp["person_canon"] == canon
        n = mask.sum()
        if n > 0:
            uid = truth_lookup[canon]
            pbp.loc[mask, "person_id"] = uid
            pbp.loc[mask, "person_unresolved"] = False
            direct_count += n
    print(f"Group 4: assigned UUID to {direct_count} direct PBP rows")

    # Step 6: Apply CANON_CORRECT_NEW in PBP (update canon + UUID)
    cc_new_count = 0
    for src, tgt in CANON_CORRECT_NEW.items():
        mask = pbp["person_canon"] == src
        n = mask.sum()
        if n > 0:
            uid = truth_lookup[tgt]
            pbp.loc[mask, "person_canon"] = tgt
            pbp.loc[mask, "person_id"] = uid
            pbp.loc[mask, "person_unresolved"] = False
            cc_new_count += n
    # Also assign UUID to the direct form of Group 5 targets that were already
    # in PBP with the correct canon (e.g., Marcin Gadziński row from event 1219405634)
    # — these are covered by Step 5 since they're also in DIRECT_NEW_PERSONS.
    print(f"Group 5: corrected {cc_new_count} PBP rows → new Truth UUID")

    # Step 7: Append new rows to Truth
    if new_truth_rows:
        truth_new_df = pd.DataFrame(new_truth_rows)
        # Match column order of existing Truth
        for col in truth.columns:
            if col not in truth_new_df.columns:
                truth_new_df[col] = float("nan")
        truth_new_df = truth_new_df[truth.columns]
        truth = pd.concat([truth, truth_new_df], ignore_index=True)

    # Step 8: Remove resolved entries from Unresolved
    before = len(unresolved)
    unresolved = unresolved[~unresolved["person_canon"].isin(to_remove_from_unresolved)]
    removed = before - len(unresolved)
    print(f"Unresolved: removed {removed} rows ({before} → {len(unresolved)})")

    # Step 9: Validate
    print()
    print("=== Validation ===")
    # Check no new Truth UUIDs collide
    dupes = truth["effective_person_id"].duplicated()
    if dupes.any():
        print(f"  ERROR: {dupes.sum()} duplicate UUIDs in Truth!")
        print(truth[dupes][["effective_person_id", "person_canon"]])
        sys.exit(1)
    else:
        print(f"  Truth UUIDs: no duplicates (total {len(truth)} rows)")

    # Check PBP coverage
    non_person_mask = pbp["person_canon"] == "__NON_PERSON__"
    unresolved_mask = pbp["person_unresolved"] == True
    uuid_mask = pbp["person_id"].notna()
    print(f"  PBP rows: {len(pbp)}")
    print(f"    → with UUID:          {uuid_mask.sum()}")
    print(f"    → __NON_PERSON__:     {non_person_mask.sum()}")
    print(f"    → still unresolved:   {unresolved_mask.sum()}")

    if apply:
        truth.to_csv(TRUTH_OUT, index=False)
        unresolved.to_csv(UNRESOLVED_OUT, index=False)
        pbp.to_csv(PLACEMENTS_OUT, index=False)
        print()
        print(f"Written: {TRUTH_OUT.name} ({len(truth)} rows)")
        print(f"Written: {UNRESOLVED_OUT.name} ({len(unresolved)} rows)")
        print(f"Written: {PLACEMENTS_OUT.name} ({len(pbp)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Write output files (default: dry run)")
    args = parser.parse_args()
    run(apply=args.apply)


if __name__ == "__main__":
    main()
