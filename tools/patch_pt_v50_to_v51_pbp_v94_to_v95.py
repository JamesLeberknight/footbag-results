"""
patch_pt_v50_to_v51_pbp_v94_to_v95.py

Merge 67 duplicate person entries:
  - 56 "Last First" ↔ "First Last" format swaps (European name-order artifacts)
  - 10 confirmed name variants (confirmed by maintainer)
  - 1 mojibake nickname: Chris ÏGatorÓ Routh → Chris Routh

For each pair: loser is removed from PT; PBP rows are remapped to winner UUID.
Loser names are added to person_aliases.csv for future resolution.
"""

import pandas as pd
from pathlib import Path

ROOT   = Path(__file__).resolve().parent.parent
PT_IN  = ROOT / "inputs/identity_lock/Persons_Truth_Final_v50.csv"
PT_OUT = ROOT / "inputs/identity_lock/Persons_Truth_Final_v51.csv"
PBP_IN  = ROOT / "inputs/identity_lock/Placements_ByPerson_v94.csv"
PBP_OUT = ROOT / "inputs/identity_lock/Placements_ByPerson_v95.csv"
ALIASES_PATH = ROOT / "overrides/person_aliases.csv"

# (loser_canon, winner_canon)
# loser is merged INTO winner; loser is removed from PT
MERGES = [
    # --- Format swaps: Last First → First Last ---
    ("Chabannes David",       "David Chabannes"),
    ("Tamme Enzy",            "Enzy Tamme"),
    ("Inkinen Jaakko",        "Jaakko Inkinen"),
    ("Linnanen Jere",         "Jere Linnanen"),
    ("Laine Miikka",          "Miikka Laine"),
    ("Mai Jakob",             "Jakob Mai"),
    ("Veluire Lionel",        "Lionel Veluire"),
    ("Donner Oleg",           "Oleg Donner"),
    ("Zelinka Ales",          "Ales Zelinka"),
    ("Mortensen Kim",         "Kim Mortensen"),
    ("Regimbald Ianek",       "Ianek Regimbald"),
    ("Rendsvig Rasmus",       "Rasmus Rendsvig"),
    ("Busch Anne",            "Anne Busch"),
    ("Siegert Stefan",        "Stefan Siegert"),
    ("Mitrofanov Ilja",       "Ilja Mitrofanov"),
    ("Rebattu Yohann",        "Yohann Rebattu"),
    ("Weber Jan",             "Jan Weber"),
    ("Budzik Damian",         "Damian Budzik"),
    ("Daouk Karim",           "Karim Daouk"),
    ("Kirchner Viktor",       "Viktor Kirchner"),
    ("Debski Wiktor",         "Wiktor Debski"),
    ("Liukko Ninni",          "Ninni Liukko"),
    ("Nigisch Johanna",       "Johanna Nigisch"),
    ("Smirnov Alexander",     "Alexander Smirnov"),
    ("Locher Nora",           "Nora Locher"),
    ("Lenneis Verena",        "Verena Lenneis"),
    ("Kaufmann David",        "David Kaufmann"),
    ("Wagner Jakob",          "Jakob Wagner"),
    ("Lorenzen Nikolai",      "Nikolai Lorenzen"),
    ("Larsen Tobias",         "Tobias Larsen"),
    ("Cseh Szabolcs",         "Szabolcs Cseh"),
    ("Ostrowski Michal",      "Michal Ostrowski"),
    ("Piechocki Damian",      "Damian Piechocki"),
    ("Thygesen Lise",         "Lise Thygesen"),
    ("Modrzejewska Karolina", "Karolina Modrzejewska"),
    ("Bujko Marcin",          "Marcin Bujko"),
    ("Oishi Keita",           "Keita Oishi"),
    ("Gielnicki Damian",      "Damian Gielnicki"),
    ("Hejra Pavel",           "Pavel Hejra"),
    ("Popow Wojciech",        "Wojciech Popow"),
    ("Cornu Laurent",         "Laurent Cornu"),
    ("Pachucki Bartosz",      "Bartosz Pachucki"),
    ("Jamski Wojciech",       "Wojciech Jamski"),
    ("Zalewski Marek",        "Marek Zalewski"),
    ("Piesiewicz Agata",      "Agata Piesiewicz"),
    ("Voss Simon",            "Simon Voss"),
    ("Cisek Pawel",           "Pawel Cisek"),
    ("Wilk Mariusz",          "Mariusz Wilk"),
    ("Motorov Pavel",         "Pavel Motorov"),
    ("Wojtasiuk Dorota",      "Dorota Wojtasiuk"),
    ("Shikin Sergey",         "Sergey Shikin"),
    ("Dziewior Adam",         "Adam Dziewior"),
    ("Mora Darwins",          "Darwins Mora"),
    ("Häßler Ulrike",         "Ulrike Häßler"),
    ("Zabolotniy Artem",      "Artem Zabolotniy"),
    ("Maduro Franklin",       "Franklin Maduro"),
    # --- Confirmed name variants ---
    ("Red Husted",                     "Ethan Husted"),
    ("Byran Nelson",                   "Bryan Nelson"),
    ("Dylan Harper Fry",               "Dylan Fry"),
    ("Jamiro Egorov",                  "Andrey Jamiro Egorov"),
    ("Lee Sickle",                     "Lee Van Sickle"),
    ("Juan Palacios Lemos",            "Juan Bernardo Palacios Lemos"),
    ("Paloma Mayo",                    "Paloma Pujol Mayo"),
    ("Mag Hughes",                     "Scott Hughes"),
    ("Maude Laudreville",              "Maude Landreville"),
    ("Dan Botkin",                     "Daniel Botkin"),
    # --- Mojibake nickname ---
    ("Chris \u00cfGator\u00d3 Routh", "Chris Routh"),
]

# ── Load ──────────────────────────────────────────────────────────────────────

pt  = pd.read_csv(PT_IN,  dtype=str).fillna("")
pbp = pd.read_csv(PBP_IN, dtype=str).fillna("")

canon_to_row = {r["person_canon"]: r for _, r in pt.iterrows()}

# ── Validate all names present ────────────────────────────────────────────────

missing = []
for loser, winner in MERGES:
    if loser not in canon_to_row:
        missing.append(f"  LOSER missing in PT:  '{loser}'")
    if winner not in canon_to_row:
        missing.append(f"  WINNER missing in PT: '{winner}'")
if missing:
    raise RuntimeError("Missing PT entries:\n" + "\n".join(missing))

print(f"All {len(MERGES)} pairs validated in PT.")

# ── Build remap dicts ─────────────────────────────────────────────────────────

loser_to_winner_canon = {l: w for l, w in MERGES}
loser_to_winner_id    = {l: canon_to_row[w]["effective_person_id"] for l, w in MERGES}
loser_ids             = {canon_to_row[l]["effective_person_id"] for l, _ in MERGES}

# ── PT: merge player_ids_seen / player_names_seen into winner, remove losers ──

def merge_pipe(existing: str, extra: str) -> str:
    parts = set(filter(None, existing.split("|"))) | set(filter(None, extra.split("|")))
    return "|".join(sorted(parts))

pt_updated = pt.copy()
for loser, winner in MERGES:
    loser_row  = canon_to_row[loser]
    winner_idx = pt_updated.index[pt_updated["person_canon"] == winner]
    if len(winner_idx) == 0:
        raise RuntimeError(f"Winner '{winner}' not found in working PT")
    widx = winner_idx[0]
    pt_updated.at[widx, "player_ids_seen"]   = merge_pipe(
        pt_updated.at[widx, "player_ids_seen"], loser_row["player_ids_seen"]
    )
    pt_updated.at[widx, "player_names_seen"] = merge_pipe(
        pt_updated.at[widx, "player_names_seen"], loser_row["player_names_seen"]
    )

loser_canons = {l for l, _ in MERGES}
pt_out = pt_updated[~pt_updated["person_canon"].isin(loser_canons)].reset_index(drop=True)
print(f"PT: {len(pt)} → {len(pt_out)} rows  (-{len(pt) - len(pt_out)} losers removed)")
pt_out.to_csv(PT_OUT, index=False)
print(f"  Wrote {PT_OUT}")

# ── PBP: remap person_canon + person_id ───────────────────────────────────────

pbp_out = pbp.copy()
remapped = 0
for loser, winner in MERGES:
    winner_id = loser_to_winner_id[loser]
    mask = pbp_out["person_canon"] == loser
    n = mask.sum()
    if n:
        pbp_out.loc[mask, "person_canon"] = winner
        pbp_out.loc[mask, "person_id"]    = winner_id
        remapped += n
        print(f"  PBP: '{loser}' → '{winner}'  ({n} rows, id→{winner_id[:8]})")

print(f"PBP: {len(pbp)} → {len(pbp_out)} rows  ({remapped} rows remapped)")
pbp_out.to_csv(PBP_OUT, index=False)
print(f"  Wrote {PBP_OUT}")

# ── person_aliases.csv: add loser names as aliases ────────────────────────────

aliases = pd.read_csv(ALIASES_PATH, dtype=str).fillna("")
existing_aliases = set(aliases["alias"].str.strip())

new_rows = []
for loser, winner in MERGES:
    if loser not in existing_aliases:
        winner_id    = loser_to_winner_id[loser]
        new_rows.append({
            "alias":        loser,
            "person_id":    winner_id,
            "person_canon": winner,
            "status":       "verified",
            "notes":        "merged: Last-First format swap or confirmed variant",
        })

if new_rows:
    aliases_out = pd.concat([aliases, pd.DataFrame(new_rows)], ignore_index=True)
    aliases_out.to_csv(ALIASES_PATH, index=False)
    print(f"\nperson_aliases.csv: +{len(new_rows)} entries")
else:
    print("\nperson_aliases.csv: no new entries needed")

print("\nDone. Next steps:")
print("  PT v50 → v51, PBP v94 → v95")
print("  Update run_pipeline.sh + RELEASE_CHECKLIST.md")
print("  Rebuild pipeline and run QC")
