"""
patch_pbp_v95_to_v96.py

Remap 67 ghost UUIDs in PBP v95 to their canonical PT v51 UUIDs.

Root cause: PT v51 merged 67 Last-First format persons into their canonical
First-Last entries, but PBP v95 was never patched — it still references the
old ghost UUIDs in person_id (player rows) and team_person_key (team rows).
out/Persons_Truth.csv also retained these ghost rows, causing them to surface
as separate persons in the canonical export.

All 67 ghost→canonical mappings are derived from person_aliases.csv (59 correct)
plus manual correction of 8 stale alias entries that incorrectly pointed to the
ghost UUIDs instead of the PT v51 canonical UUIDs.

Expected changes: 125 rows updated (121 team rows + 4 player rows).
"""

import csv
import re
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
IN_PATH  = ROOT / "inputs/identity_lock/Placements_ByPerson_v95.csv"
OUT_PATH = ROOT / "inputs/identity_lock/Placements_ByPerson_v96.csv"

# ghost_uuid -> canonical_uuid
# 59 from aliases (correctly resolved) + 8 manual corrections for stale aliases
REMAP = {
    # --- correctly resolved via person_aliases.csv ---
    "df409c60-fc5b-5757-a665-94eb5d71cb1e": "a3781ac2-c529-5316-b22b-2bdcfa67bb67",  # Budzik Damian -> Damian Budzik
    "81b36797-8059-51b2-a937-4f0d3f1d6da5": "4f0fcdbc-8dfe-51fc-b92c-b3cda4dd0c00",  # Bujko Marcin -> Marcin Bujko
    "6f089ad0-2735-5620-9e42-dfca71c1b652": "c7308982-a2af-598b-baf9-a0515c121cf7",  # Busch Anne -> Anne Busch
    "3a9a28f9-3065-55af-8f68-049c7cf09cc9": "1e600282-c338-51d1-936e-e326f5c0eda8",  # Byran Nelson -> Bryan Nelson
    "0c108e2d-10f5-5971-8227-39f48bae3a59": "d3f99c1e-fd05-5579-b3eb-34da912c3256",  # Chabannes David -> David Chabannes
    "7d869fbe-ba9a-5829-b190-d9905be7caaa": "b08606fb-8072-5d14-ae24-b1881c22be54",  # Chris ÏGatorÓ Routh -> Chris Routh
    "5bb92347-9db3-5f3f-9284-d3c035ada3de": "603bbc2d-6488-5891-a942-b5c5c79e0018",  # Cisek Pawel -> Pawel Cisek
    "220c9ea6-46f9-5520-bf76-10ee56e70798": "fb06557e-f387-580c-8bea-740bee1055cb",  # Cseh Szabolcs -> Szabolcs Cseh
    "d59e63d4-5d17-5581-9530-e68cf90a9cac": "eb025239-b9ab-5cef-9307-7639f88a225a",  # Daouk Karim -> Karim Daouk
    "7872dd89-6275-547c-815e-b034d33f2408": "c917c04b-83fa-5c1a-8f2c-292f24db5616",  # Debski Wiktor -> Wiktor Debski
    "4c3321eb-1528-530b-8f38-c42260d9af08": "97290f88-58d9-5a3e-a593-ef02acf43ae5",  # Donner Oleg -> Oleg Donner
    "e06478ff-c2e5-5a5b-8aef-0150998283c2": "9aa9e85a-1dbb-54ad-8aed-10bd7e5719ad",  # Dylan Harper Fry -> Dylan Fry
    "f7aec15a-a574-5386-a992-b6428a652117": "57477659-5d12-5993-a43e-5a03384ccdff",  # Dziewior Adam -> Adam Dziewior
    "3e31d976-a00a-5cfa-a6c0-69f93579e3fe": "fb09d2be-a773-5bc1-9da3-60cbabff4338",  # Hejra Pavel -> Pavel Hejra
    "f0b313e7-522b-5e2f-9a5e-d40980638011": "66464366-b029-5e3c-b772-e196240ad434",  # Häßler Ulrike -> Ulrike Häßler
    "76056901-4c79-5c3c-9036-52783c5a3acf": "58cca5c0-0120-5bf1-a40e-ae4f46712dc1",  # Inkinen Jaakko -> Jaakko Inkinen
    "61cb574c-b24b-50ee-a2bc-10485c13c6e4": "6d74f8f5-c8d0-5e3d-90ad-bb6323776ad1",  # Jamiro Egorov -> Egorov Jamiro
    "b0e0b719-fec4-571e-8e0f-553b6d9f5a50": "2fe08509-fcb3-5ecd-8828-40411bc4a036",  # Jamski Wojciech -> Wojciech Jamski
    "414a3d58-ef57-5f36-9daa-eb5db25f2d7b": "2cf525b7-ae53-56ff-98f2-0f845d6d998f",  # Kaufmann David -> David Kaufmann
    "0e4c2b8e-963a-543f-9c57-ec42b04b5f73": "897f89db-a4cb-5f5b-acf8-a33a5842ed11",  # Kirchner Viktor -> Viktor Kirchner
    "bc3b5f42-2582-528a-a8ad-83a73cd62019": "0673ea51-3333-589c-8a73-54108c27e180",  # Laine Miikka -> Miikka Laine
    "458918b8-ce3c-5bf9-998d-cc11a53e1109": "7c17dac5-be0b-5fc2-a537-2c8d02be9019",  # Larsen Tobias -> Tobias Larsen
    "4ea3c32c-ba79-5e8b-8203-42efc78349ca": "205ca842-1055-506b-8c48-118afc6ae681",  # Lenneis Verena -> Verena Lenneis
    "83ba4a8c-d170-5c79-b698-e0fd6893a7c3": "fc3f381a-650e-5582-910d-6a83702e9dae",  # Linnanen Jere -> Jere Linnanen
    "145d1998-d217-5dd8-a5b4-239c1c7c7c59": "9aca54b3-8989-5d9e-8ca3-b5420d233d8f",  # Liukko Ninni -> Ninni Liukko
    "bfa1c5a0-584c-5283-8c58-9003507522d7": "2147dbe3-da70-55cd-a988-43bcec841389",  # Locher Nora -> Nora Locher
    "95fcd1df-e238-500f-9986-3d4e1e174a62": "2d8be0d2-af88-5902-92be-5e6171bee781",  # Lorenzen Nikolai -> Nikolai Lorenzen
    "5a652a79-c8e5-52a2-968a-0cb15910bff8": "99026947-f309-52d9-b7ae-2cf32e9c8ffc",  # Maduro Franklin -> Franklin Maduro
    "ca63a7da-a00a-5a83-ae43-2e17b5758fc8": "4cbf790d-c542-5318-9337-ee3dfd539ff1",  # Mag Hughes -> Hughes Mag
    "f2b97c45-f2a2-5351-b488-b859e548864b": "d9aecda1-b6f6-5445-9a48-4a47f0f66bd4",  # Mai Jakob -> Jakob Mai
    "444a3ba0-2772-5a68-9798-efc887a365fe": "5c134542-d0b3-55f2-8c4c-413ccb2e9d15",  # Mitrofanov Ilja -> Ilja Mitrofanov
    "4c048388-4b74-56db-87ed-30edcc564c62": "b05824d4-4a6f-5132-828d-51d1da0a1845",  # Modrzejewska Karolina -> Karolina Modrzejewska
    "7130fa11-4985-5ace-92a7-cb5828e7439c": "d9a7e5f1-4575-53df-8031-84d5ae7ac7f0",  # Mora Darwins -> Darwins Mora
    "49aa14eb-1f73-5417-91cd-89a98f772d3c": "25f020c8-edfa-5323-8ece-671cb88e0f50",  # Mortensen Kim -> Kim Mortensen
    "7b325017-26ce-5082-86fb-b6aa0b0bafd7": "ea605f22-107e-5bb7-9a85-a608e167038e",  # Nigisch Johanna -> Johanna Nigisch
    "b97dd5f6-d508-503b-913f-e236112708b7": "291d5062-d8d2-5fcb-90d4-ddc31be14399",  # Oishi Keita -> Keita Oishi
    "3204f439-61ee-5509-b59f-63c4e34174f0": "da3fbe76-ba2d-51ee-a019-97e7ba93089a",  # Ostrowski Michal -> Michal Ostrowski
    "41a74ef5-7c2a-5ce0-b626-0b369334cedb": "c81b3b72-dcd0-5759-9fd8-bf26b5e0aad9",  # Pachucki Bartosz -> Bartosz Pachucki
    "6909a2ae-2129-5460-9257-e080b25fb097": "c84f68fb-16c1-5231-83fa-46eaa2ef955d",  # Piesiewicz Agata -> Agata Piesiewicz
    "66f38cf4-4715-58f3-a91e-d851f09b3f03": "4d352728-74b6-51db-aa04-1cf6d24b7cc7",  # Piechocki Damian -> Damian Piechocki
    "708d680c-1247-5a76-b913-947858ed1ae9": "b5f711d3-60ac-5f4c-b552-be1aab57c93c",  # Popow Wojciech -> Wojciech Popow
    "6ea9c664-b6fd-5aca-ae33-2e7e4972ab8e": "93df01b3-35be-5b3b-9ce6-19c010c4474f",  # Rebattu Yohann -> Yohann Rebattu
    "451fc707-98a7-551d-b7e0-2610c54e10d6": "8fdea8f1-79bd-51a1-ab3f-04270d90e5af",  # Regimbald Ianek -> Ianek Regimbald
    "3428fd4e-3d1b-5939-8539-51158cdcfc27": "b1dbcec6-2d10-55c9-878c-df0104ad5d2a",  # Rendsvig Rasmus -> Rasmus Rendsvig
    "a1232162-f867-5ef7-abf3-a43230093fe5": "920bf962-a6d7-5c1f-8c08-1fb51be48ec8",  # Shikin Sergey -> Sergey Shikin
    "d120eea6-1bea-566f-b4a2-55a79f4f3b0a": "9b0f06c8-7ef6-5145-9e2b-e80c48a06615",  # Smirnov Alexander -> Alexander Smirnov
    "1e9ffb59-1632-5330-ad85-cce2f0b1a69f": "0c3a857f-fe99-5fba-8a8b-2596b08435b0",  # Tamme Enzy -> Enzy Tamme
    "e96d4d25-1446-570e-bfcc-277d318607e1": "43e31230-b102-5a58-a636-9da126870284",  # Thygesen Lise -> Lise Thygesen
    "7131dc09-cfa3-5c03-806f-93425e848ca4": "99f8f795-df84-5f76-a00c-935658d2914e",  # Veluire Lionel -> Lionel Veluire
    "6402b42f-6158-5606-80f1-ba175fca9ec8": "04253241-2763-57c5-a946-fa08f4b8d8e0",  # Voss Simon -> Simon Voss
    "144dcb48-1f6f-5195-8897-67b8887a651b": "c90a00eb-95d7-5375-a769-b71cbce34d87",  # Wagner Jakob -> Jakob Wagner
    "c6048fe2-e06b-5d16-997b-ad7fe443f372": "1e09ba09-ec4b-5961-8b95-e0d638dc4f5c",  # Weber Jan -> Jan Weber
    "ee90f870-f2b0-5d92-8f42-0cf164a1184a": "4ad82639-801d-5c4b-aae6-f07e5dfe67d2",  # Wilk Mariusz -> Mariusz Wilk
    "76d9547a-3046-5d09-81f6-28d92063e4d3": "7a8c8b83-9ae8-5b65-8f1f-6923058cc237",  # Wojtasiuk Dorota -> Dorota Wojtasiuk
    "c854dd97-c801-52c4-9331-2ab4447de881": "d114b01e-b1b3-5b88-81f2-ac15f275ff37",  # Zabolotniy Artem -> Artem Zabolotniy
    "664febd9-f84f-5553-8686-d81446081159": "509aadba-dc64-537a-b98b-ca71889c9f2c",  # Zalewski Marek -> Marek Zalewski
    "0f32d1e1-8895-5c15-b1ee-8d4b4b919abf": "f2e73bec-d859-51b9-8312-edb5a473e83e",  # Zelinka Ales -> Ales Zelinka
    "26d70b1f-bca9-579f-b9fa-0c95c3dde86a": "6d92df68-6db3-56cf-980f-4a36a11389ea",  # Paloma Mayo -> Mayo Paloma

    # --- 8 manual corrections (stale aliases pointed to ghost, not canonical) ---
    "2e290b8a-64f4-57ad-ae4e-32e4d197abfb": "c14c15c2-84dc-57a2-86f2-cec477d7662d",  # Cornu Laurent -> Laurent Cornu
    "359ec3bc-cfa6-5c16-8a78-51c9d395657f": "e127d87a-07d1-5dfb-a359-499f678ce286",  # Dan Botkin -> Daniel Botkin
    "2512e04d-9d40-5794-9807-5cbf616df0a6": "ac1268dc-a961-568f-860e-63e9ea815c01",  # Gielnicki Damian -> Damian Gielnicki
    "e5395a41-84fc-5c0f-8d71-1d79a06dddce": "3123701a-43bd-5273-8472-ba332dc67fed",  # Juan Palacios Lemos -> Juan Bernardo Palacios Lemos
    "b69d06e1-49be-5fac-ae61-a9194ef30ee0": "f2ce846c-fa31-52e4-a88e-d8f7bccbe92e",  # Maude Laudreville -> Maude Landreville
    "2eb79ea1-9ae9-512a-83bc-3f4a351650f8": "9d12fe41-0154-57a6-b312-5f3b46f621b5",  # Motorov Pavel -> Pavel Motorov
    "f7e4ff46-a45d-5bfc-95ca-d2f50b0dae82": "a67698e9-2dd8-5853-b75a-3c6e535d7610",  # Red Husted -> Red Fred Husted
    "2387456b-26ab-5254-ad15-2c78d59c4c71": "78d0c91f-b1ec-572e-855b-a7d3f706f8b7",  # Siegert Stefan -> Stefan Siegert
}

UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def remap_uuid_field(value: str) -> tuple[str, int]:
    """Replace any ghost UUID tokens in a pipe-separated UUID field. Returns (new_value, n_changed)."""
    if not value:
        return value, 0
    parts = value.split("|")
    changed = 0
    new_parts = []
    for p in parts:
        p = p.strip()
        if p in REMAP:
            new_parts.append(REMAP[p])
            changed += 1
        else:
            new_parts.append(p)
    return "|".join(new_parts), changed


def main():
    print(f"Reading {IN_PATH} ...")
    with open(IN_PATH, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        rows = list(reader)

    total_rows = len(rows)
    rows_changed = 0
    uuid_changes = 0

    for row in rows:
        changed_this_row = 0

        # player rows: ghost UUID in person_id
        pid = row.get("person_id", "")
        if pid in REMAP:
            row["person_id"] = REMAP[pid]
            changed_this_row += 1

        # team rows: ghost UUIDs in team_person_key (pipe-separated)
        tpk = row.get("team_person_key", "")
        new_tpk, n = remap_uuid_field(tpk)
        if n:
            row["team_person_key"] = new_tpk
            changed_this_row += n

        if changed_this_row:
            rows_changed += 1
            uuid_changes += changed_this_row

    print(f"Rows updated:   {rows_changed} (expected 125)")
    print(f"UUID swaps:     {uuid_changes}")
    print(f"Total rows:     {total_rows} (unchanged)")
    print()

    # Verify no ghost UUIDs remain
    ghost_remaining = 0
    for row in rows:
        for ghost in REMAP:
            if ghost in (row.get("person_id","") or "") or ghost in (row.get("team_person_key","") or ""):
                print(f"  WARNING: ghost still present: {ghost} in row event={row['event_id']} div={row['division_canon']} p={row['place']}")
                ghost_remaining += 1
    if ghost_remaining:
        print(f"WARNING: {ghost_remaining} ghost UUID references remain — patch incomplete")
    else:
        print("All ghost UUIDs cleared.")

    print(f"\nWriting {OUT_PATH} ...")
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("Done.")


if __name__ == "__main__":
    main()
