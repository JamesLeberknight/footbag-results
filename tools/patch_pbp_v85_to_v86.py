"""
patch_pbp_v85_to_v86.py

Targeted patch for event 1323272493 (2012 33rd IFPA World Footbag Championships, Warsaw).

Fixes in this patch:
  A. 6 division names with U+FFFD replacement-char artifacts → clean names
     (also fixes division_category for 4 divisions misclassified due to corrupt names)
  B. 65 Open Singles Net EUR-format team rows → individual player rows
     ("Surname / GivenName CC" → "GivenName Surname" with PT-resolved person_id)
  C. Women's Singles Net: p2 FFFD fix + p4-p9 comma-format reversal + add person_ids
  D. person_canon FFFD fixes in non-OSN/non-WSN player rows (8 rows)
  E. team_display_name FFFD fixes in doubles divisions (19 rows)
  F. Missing person_id for OSN p3 Tuomas Karki
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v85.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v86.csv"

EVENT_ID = 1323272493

# ---------------------------------------------------------------------------
# A. Division name + category fixes
# ---------------------------------------------------------------------------
DIV_RENAMES = {
    "Open Cir\uFFFDCle Con\uFFFDTest":          "Open Circle Contest",
    "Women's Cir\uFFFDCle Con\uFFFDTest":        "Women's Circle Contest",
    "Women's Freestyle Rou\uFFFDTines":          "Women's Freestyle Routines",
    "Open Dou\uFFFDBles Rou\uFFFDTines":         "Open Doubles Routines",
    "Mixed Dou\uFFFDBles Rou\uFFFDTines":        "Mixed Doubles Routines",
    "Open Freestyle Rou\uFFFDTines":             "Open Freestyle Routines",
}

DIV_CATEGORY_FIXES = {
    # After rename, what the category should be
    "Open Circle Contest":     "freestyle",
    "Women's Circle Contest":  "freestyle",
    "Open Doubles Routines":   "freestyle",
    "Mixed Doubles Routines":  "freestyle",
    # Open/Women's Freestyle Routines already 'freestyle' — listed for completeness
    "Open Freestyle Routines":   "freestyle",
    "Women's Freestyle Routines":"freestyle",
}

# ---------------------------------------------------------------------------
# B. Open Singles Net EUR-format conversions
#    Key = original team_display_name (may contain FFFD)
#    Value = (clean person_canon, person_id or None)
# ---------------------------------------------------------------------------
OSN_EUR_MAP = {
    "Daouk / Karim CH":                    ("Karim Daouk",              "eb025239-b9ab-5cef-9307-7639f88a225a"),
    "Pohjola / Matti FI":                  ("Matti Pohjola",            "ddffefad-c620-55ab-b1bc-ca01939671c6"),
    "Houston / Walt US":                   ("Walt Houston",             "c6704eb0-91e0-538b-8806-9a43b16e91cb"),
    "Lima / Greg FR":                      ("Greg Lima",                "c7a23f15-f2ed-5f2b-ac50-bb1adb185475"),
    "Kreil / Yves DE":                     ("Yves Kreil",               "b274fbcc-9283-5e2e-9c68-704f3212796e"),
    "Ledain / Arthur FR":                  ("Arthur Ledain",            "e528ca88-e462-5870-bd7f-45fb49bfbcfc"),
    "Alston / Ben US":                     ("Ben Alston",               "76079f4a-40db-5e3c-97e1-99e590791839"),
    "Lezama / Victor VE":                  ("Victor Lezama",            "2cfacf64-c589-5b3a-bf00-d42be8c82c8e"),
    "Rockel / Tammo DE":                   ("Tammo Rockel",             "ecf4cab4-a4f2-549d-85fb-5d8528db3948"),
    "Harris / Jack US":                    ("Jack Harris",              "7c8cfb20-ed0b-5203-89ea-46dea3ebf0e0"),
    "Depatie-Pelletier / Francois CA":     ("Francois Depatie-Pelletier","32c272b3-3d4c-59e9-8af9-7d15aee27c1f"),
    "Forsten / Oskari FI":                 ("Oskari Forsten",           None),
    "Degat / Guillaume FR":                ("Guillaume Degat",          "dad697e8-36ed-598e-84b3-8105a2868685"),
    "Da Silva / Roberto VE":               ("Roberto Da Silva",         "cd277323-fc63-5adf-bb0c-e05ab57a46e8"),
    "Fritsch / Antonio DE":                ("Antonio Fritsch",          "34055beb-ed72-59a5-9b38-c5ffbac4d70e"),
    "Debski / Wiktor PL":                  ("Wiktor Debski",            "c917c04b-83fa-5c1a-8f2c-292f24db5616"),
    "Marquez / Carlos VE":                 ("Carlos Marquez",           "dfb408b6-d225-5b9f-b1c7-3f9a76c4319a"),
    "Mingard / Yves CH":                   ("Yves Mingard",             "c4f49339-ef54-500b-bedc-3e3ae2e081e1"),
    "Diaz / Diego VE":                     ("Diego Diaz",               None),
    "Mora / Darwins VE":                   ("Darwins Mora",             "d9a7e5f1-4575-53df-8031-84d5ae7ac7f0"),
    "Tellenbach / Grischa FR":             ("Grischa Tellenbach",       "ec1d5118-cd4b-591f-a84a-04bbe20868bc"),
    "Isackson / Quentin FR":               ("Quentin Isackson",         "7d094f73-d2d9-5f99-9b73-6593696c90ef"),
    # FFFD variant: "Weyler-Lavall\uFFFDe / Luka CA"
    "Weyler-Lavall\uFFFDe / Luka CA":     ("Luka Weyler",              "deb1a724-c5cd-5173-bd6e-71d8f1d585bf"),
    "Jamski / Wojciech PL":               ("Wojciech Jamski",          "2fe08509-fcb3-5ecd-8828-40411bc4a036"),
    "Lessard / Philippe CA":               ("Philippe Lessard",         "87216aed-3048-50f7-8c54-d7e9e7bb52f3"),
    "Lagos / Jairo VE":                    ("Ja\u00edro Lagos",         "11d38fbe-cd3c-5bf3-b417-ea04da62c528"),
    "Kruse / Manuel DE":                   ("Manuel Kruse",             "fd8d3afc-a726-5ac8-841c-be6459e303bf"),
    "Voss / Simon DE":                     ("Simon Voss",               "04253241-2763-57c5-a946-fa08f4b8d8e0"),
    "Martinez / Eduardo VE":               ("Eduardo Martinez",         "7b729bc2-570a-543c-a460-24ed136deb5b"),
    "Hellberg / Hakan SE":                 ("Hakan Hellberg",           "1a1b1397-a4b8-5826-b51d-ff5391eba110"),
    "Lindner / Eurik DE":                  ("Eurik Lindner",            "fc767e35-5ca6-5aec-9080-269c6846e7fb"),
    "Rousseau / Vincent FR":               ("Vincent Rousseau",         "b5c82731-ff37-5170-86a5-f744afe4d154"),
    "Korff / Johannes DE":                 ("Johannes Korff",           "a751368f-dee2-50a8-98b1-6942a8d1ac1a"),
    "Barakat / Faris DE":                  ("Faris Barakat",            "0012c28e-3f60-5c87-8d19-869ef819e724"),
    "Staron / Marcin PL":                  ("Marcin Staron",            "21154756-b8d4-512e-872a-c5b206fac8d3"),
    "Castro / Nelson VE":                  ("Nelson Castro",            "c02bcce0-b34d-5928-bad6-9d2e198076c1"),
    # FFFD variant: "Z\uFFFDlli / Renato CH"
    "Z\uFFFDlli / Renato CH":             ("Renatto Z\u00fclli",       "73961edb-d45f-5638-9401-1a0de345da6f"),
    "Bock / Christian HR":                 ("Christian Bock",           "6f69628f-6e43-53b9-91a6-0bff36dc6ee7"),
    "Lirkki / Jani FI":                   ("Jani Lirkki",              "3d1a7216-f5c6-58ef-b850-743fe1db4cc9"),
    "Ostwaldt / Felix DE":                 ("Felix Ostwaldt",           "3e3269dc-9c8f-5a7d-a113-922c71054fdb"),
    "Loreto / Oscar VE":                   ("Oscar Loreto",             "3f78b9d6-0b17-5661-8280-f98ad91dadf1"),
    "Samborowski / Maciej PL":             ("Maciej Samborowski",       "514d386e-d6e1-5342-b2a1-6429daf35c0c"),
    # FFFD variant: "Tailleur / St\uFFFDphane CA"
    "Tailleur / St\uFFFDphane CA":        ("St\u00e9phane Tailleur",   "0295db32-9628-5fdd-9420-212a8adc5b6d"),
    "Budzik / Damian PL":                  ("Damian Budzik",            "a3781ac2-c529-5316-b22b-2bdcfa67bb67"),
    "Nold / Stefan DE":                    ("Stefan Nold",              "e2ad332e-4635-5c13-9bc3-9cd157eaee8d"),
    "Klimczak / Micha PL":                 ("Michal Klimczak",          "b980fe51-dd4b-54fd-bb60-21cbb482f674"),
    "Kaufmann / David DE":                 ("David Kaufmann",           "2cf525b7-ae53-56ff-98f2-0f845d6d998f"),
    "Boulay / Carl-Antoine CA":            ("Carl-Antoine Boulay",      None),
    "Rollmann / Simon DE":                 ("Simon Rollmann",           "0d030fab-ebbd-5ec9-a626-77afe441a35c"),
    "Brunet / Marc FR":                    ("Marc Brunet",              "a5c9d383-777c-5f21-9604-6fd1cba79fb4"),
    "Koscielny / Piotrek PL":              ("Piotrek Koscielny",        "153997cf-41c0-528b-85ef-d897c3186262"),
    "Rautenberg / Stephan DE":             ("Stephan Rautenberg",       "9188c2d1-9556-5ad8-b890-6c22425dbbf0"),
    "Laakso / Ville FI":                   ("Ville Laakso",             "7444ebf8-86b3-5daf-b170-aebb4f0c8dbf"),
    "Andryka / Rafal PL":                  ("Rafal Andryka",            "e9cc5378-aa20-5b2c-af79-e8e609a11a3c"),
    "Kuntze / Steve DE":                   ("Steve Kuntze",             "ee71cb5a-4b77-54c2-9bc7-c11165318ba5"),
    # FFFD variant: "P\uFFFDchel / Robin DE"
    "P\uFFFDchel / Robin DE":             ("Robin Puchel",             "3c50ab0f-2056-50ce-a5f2-c7c95e96702f"),
    "Ignaczak / Wojciech PL":              ("Wojciech Ignaczak",        "56ecef47-5ea7-5d81-8de8-3efb1e66c81b"),
    "Rog / Michal PL":                     ("Michal Rog",               "392ef8ff-d969-5917-8bc5-176805ecb787"),
    "Lobankov / Arkady RU":                ("Arkady Lobankov",          "80696601-2e8f-54d7-8573-0e0d015bd5a9"),
    "Hartmann / Ingo DE":                  ("Ingo Hartmann",            "9cc94706-a135-5721-a2a4-c9dc8547d2ec"),
    "Ollivier / Boris FR":                 ("Boris Ollivier",           "7150e55f-c6bd-5d27-a08b-9617313cac5e"),
    "Grabarczyk / Jakub PL":               ("Jakub Grabarczyk",         "3568ce18-bf1b-55b6-aefd-61f65f55b08c"),
    "Coblence / Julien FR":                ("Julien Coblence",          "42f12ca1-5286-55bd-8c7b-873a5f914069"),
    "Hejra / Pavel CZ":                    ("Pavel Hejra",              "fb09d2be-a773-5bc1-9da3-60cbabff4338"),
}

# p40 is a comma-format player row (not a team row)
OSN_P40_FIX = ("Tuan Vu", "28565dd0-2196-5404-bf23-6cf0617ce79b")

# ---------------------------------------------------------------------------
# C. Women's Singles Net fixes
#    Key = original person_canon, Value = (clean name, person_id or None)
# ---------------------------------------------------------------------------
WSN_FIXES = {
    "Piia Tantarim\uFFFDki":      ("Piia Tantarim\u00e4ki",     "1dc7bf3f-8a4d-57b0-912b-b6626fb29cfb"),
    "Tikhomirova, Julia":          ("Yulia Tikhomirova",          "0b73f6bd-2b2a-57ef-8de6-853138d11297"),
    "Schlichting, Helena":         ("Helena Schlichting",         "e023aa06-ace7-5617-8145-aaca829f4c54"),
    "Probst, Katharina":           ("Katharina Probst",           "dd033603-4714-5162-8de9-39f56f77e9e6"),
    "Zambrano Torres, Ivaneidy":   ("Ivaneidy Zambrano Torres",   "48cf50b4-bd65-5d0b-8cc5-a56a7923157e"),
    "Ulrike H\uFFFDssler":        ("Ulrike H\u00e4\u00dfler",    "66464366-b029-5e3c-b772-e196240ad434"),
    "Andrey, Sophie":              ("Sophie Andrey",              "a1eabf86-a8d4-571b-b69a-69bfb73d0685"),
}

# ---------------------------------------------------------------------------
# D. person_canon FFFD fixes for player rows in other divisions
#    Key = corrupt person_canon, Value = (clean name, person_id)
# ---------------------------------------------------------------------------
PERSON_CANON_FIXES = {
    "Pawe\u0142 \u015acier\uFFFDski":    ("Pawel Scierski",          "ac0d2fdd-9032-5edc-9023-e8d611dfc8c6"),
    "Prze\uFFFDmys\u0142aw Pietrzy\uFFFDcki": ("Przemyslaw Pietrzycki", "8a15006e-bc85-5621-bbe6-66090397b7d2"),
    "Jonathan Schei\uFFFDder":           ("Jonathan Schneider",       "2c16c43d-6111-5231-b813-23f20f6ba7a3"),
    "Brian Sher\uFFFDill":               ("Brian Sherrill",           "11542335-67de-5fd2-9d43-5f2b55ea1fef"),
    # Sylwia with FFFD — match existing PBP canonical for this player
    "Syl\uFFFDwia Kocyk":               ("Sylwia Kocyck",            "152714dc-cf47-5412-aa32-f44c311de83e"),
    "Ma\u0142\uFFFDgorzata Ol\u0119dzka":("Malgorzata Oledzka",       "21e3e2db-cc50-541f-9ce9-928da111bb6a"),
}

# ---------------------------------------------------------------------------
# E. team_display_name FFFD fixes (no structural change, display-only)
# ---------------------------------------------------------------------------
TEAM_DISPLAY_FIXES = {
    "Fran\uFFFDois Depatie-Pelletier / Marilyn Demuy":
        "Francois Depatie-Pelletier / Marilyn Demuy",
    "Janne Uusitalo / Piia Tantarim\uFFFDki":
        "Janne Uusitalo / Piia Tantarim\u00e4ki",
    "Stephan Rauthenberg / Ulrike H\uFFFDssler":
        "Stephan Rautenberg / Ulrike H\u00e4\u00dfler",
    "Ma\u0142\uFFFDgorzata D\u0119b\uFFFDska / Wiktor D\u0119bski":
        "Malgorzata Debska / Wiktor D\u0119bski",
    "Ma\u0142\uFFFDgorzata Ol\u0119dzka / Tomas Ostrowski":
        "Malgorzata Oledzka / Tomas Ostrowski",
    "Oskari Forst\uFFFDn / Jani Sakari Markkanen":
        "Oskari Forsten / Jani Sakari Markkanen",
    "Tuomas K\uFFFDrki / Jukka Peltola":
        "Tuomas Karki / Jukka Peltola",
    "Alex Bartsch / Chris L\uFFFDw":
        "Alex Bartsch / Chris Low",
    "Renato Z\uFFFDlli / Yves Mingard":
        "Renato Z\u00fclli / Yves Mingard",
    "Robin P\uFFFDchel / Steve Kuntze":
        "Robin Puchel / Steve Kuntze",
    "Luka Weyler-Lavall\uFFFDe / Carl-Antoine Boulay":
        "Luka Weyler-Lavall\u00e9e / Carl-Antoine Boulay",
    "Wiktor D\u0119bski / Ma\u0142\uFFFDgorzata D\u0119b\uFFFDska":
        "Wiktor D\u0119bski / Malgorzata Debska",
    # Women's Doubles Net
    "Kerstin Anhuth / Piia Tantarim\uFFFDki (Germany":
        "Kerstin Anhuth / Piia Tantarim\u00e4ki",
    "Ivaneidy Zambrano Torres / Ulrike H\uFFFD\uFFFDler":
        "Ivaneidy Zambrano Torres / Ulrike H\u00e4\u00dfler",
}


def main():
    print(f"Reading {IN_PATH} ...")
    df = pd.read_csv(IN_PATH)

    mask = df["event_id"] == EVENT_ID
    n_event = mask.sum()
    print(f"Event {EVENT_ID}: {n_event} rows")

    changes = 0

    # -----------------------------------------------------------------------
    # A. Division name + category fixes
    # -----------------------------------------------------------------------
    for old_div, new_div in DIV_RENAMES.items():
        m = mask & (df["division_canon"] == old_div)
        n = m.sum()
        if n:
            df.loc[m, "division_canon"] = new_div
            print(f"  A. div rename '{old_div}' → '{new_div}': {n} rows")
            changes += n

    for div_name, correct_cat in DIV_CATEGORY_FIXES.items():
        m = mask & (df["division_canon"] == div_name) & (df["division_category"] != correct_cat)
        n = m.sum()
        if n:
            df.loc[m, "division_category"] = correct_cat
            print(f"  A. category fix '{div_name}': → '{correct_cat}': {n} rows")
            changes += n

    # -----------------------------------------------------------------------
    # B. Open Singles Net: EUR-format team rows → player rows
    # -----------------------------------------------------------------------
    osn_mask = mask & (df["division_canon"] == "Open Singles Net")

    for tdname, (canon, pid) in OSN_EUR_MAP.items():
        m = osn_mask & (df["team_display_name"] == tdname)
        n = m.sum()
        if n:
            df.loc[m, "competitor_type"]   = "player"
            df.loc[m, "person_canon"]      = canon
            df.loc[m, "team_display_name"] = np.nan
            df.loc[m, "person_id"]         = pid if pid else np.nan
            changes += n
        else:
            print(f"  B. WARNING: no OSN row found for team_display_name={repr(tdname)}")

    # p40 comma-format player fix
    m40 = osn_mask & (df["person_canon"] == "Vu, Tuan US")
    if m40.sum():
        df.loc[m40, "person_canon"] = OSN_P40_FIX[0]
        df.loc[m40, "person_id"]    = OSN_P40_FIX[1]
        print(f"  B. OSN p40 comma-fix: 'Vu, Tuan US' → 'Tuan Vu': {m40.sum()} row(s)")
        changes += m40.sum()

    # p3 Tuomas Karki missing person_id
    m_karki = osn_mask & (df["person_canon"] == "Tuomas Karki") & df["person_id"].isna()
    if m_karki.sum():
        df.loc[m_karki, "person_id"] = "e7937047-c7f1-5739-bb84-aceb23c17dd7"
        print(f"  B. OSN p3 Tuomas Karki: added person_id: {m_karki.sum()} row(s)")
        changes += m_karki.sum()

    osn_team_remaining = (osn_mask & (df["competitor_type"] == "team")).sum()
    print(f"  B. OSN EUR conversion done. Remaining team rows in OSN: {osn_team_remaining}")

    # -----------------------------------------------------------------------
    # C. Women's Singles Net fixes
    # -----------------------------------------------------------------------
    wsn_mask = mask & (df["division_canon"] == "Women's Singles Net")

    for orig_canon, (clean_canon, pid) in WSN_FIXES.items():
        m = wsn_mask & (df["person_canon"] == orig_canon)
        n = m.sum()
        if n:
            df.loc[m, "person_canon"] = clean_canon
            df.loc[m, "person_id"]    = pid if pid else np.nan
            print(f"  C. WSN fix '{orig_canon}' → '{clean_canon}': {n} row(s)")
            changes += n
        else:
            print(f"  C. WARNING: WSN row not found for '{orig_canon}'")

    # -----------------------------------------------------------------------
    # D. person_canon FFFD fixes in other divisions
    # -----------------------------------------------------------------------
    for orig_canon, (clean_canon, pid) in PERSON_CANON_FIXES.items():
        m = mask & (df["person_canon"] == orig_canon)
        n = m.sum()
        if n:
            df.loc[m, "person_canon"] = clean_canon
            df.loc[m, "person_id"]    = pid
            print(f"  D. person_canon fix '{orig_canon}' → '{clean_canon}': {n} row(s)")
            changes += n
        else:
            print(f"  D. WARNING: person_canon not found: '{orig_canon}'")

    # -----------------------------------------------------------------------
    # E. team_display_name FFFD fixes
    # -----------------------------------------------------------------------
    for orig_tdname, clean_tdname in TEAM_DISPLAY_FIXES.items():
        m = mask & (df["team_display_name"] == orig_tdname)
        n = m.sum()
        if n:
            df.loc[m, "team_display_name"] = clean_tdname
            print(f"  E. team_display_name fix: {repr(orig_tdname[:40])} → {repr(clean_tdname[:40])}: {n} row(s)")
            changes += n
        else:
            print(f"  E. WARNING: team_display_name not found: '{orig_tdname}'")

    # -----------------------------------------------------------------------
    # F. Strip remaining U+FFFD from 'norm' column (pipeline doesn't use norm
    #    for logic — only used for Excel display hiding; stripping is safe)
    # -----------------------------------------------------------------------
    norm_fffd = mask & df["norm"].astype(str).str.contains("\ufffd", na=False)
    n = norm_fffd.sum()
    if n:
        df.loc[norm_fffd, "norm"] = df.loc[norm_fffd, "norm"].astype(str).str.replace("\ufffd", "", regex=False)
        print(f"  F. norm FFFD stripped: {n} rows")
        changes += n

    # -----------------------------------------------------------------------
    # Verify: no remaining FFFD in this event
    # -----------------------------------------------------------------------
    still_fffd = df[mask].apply(
        lambda r: any("\ufffd" in str(v) for v in r), axis=1
    ).sum()
    if still_fffd:
        print(f"\n  WARNING: {still_fffd} rows still contain U+FFFD in event {EVENT_ID}")
        problem = df[mask].apply(lambda r: any("\ufffd" in str(v) for v in r), axis=1)
        for _, row in df[mask][problem].iterrows():
            print(f"    div={row.division_canon} p={row.place} canon={repr(row.person_canon)} tdname={repr(str(row.team_display_name))}")
    else:
        print(f"\n  OK: no remaining U+FFFD in event {EVENT_ID}")

    print(f"\nTotal changes: {changes} fields across event {EVENT_ID}")
    print(f"Writing {OUT_PATH} ...")
    df.to_csv(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
