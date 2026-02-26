#!/usr/bin/env python3
"""
19_consolidate_truth.py — Merge duplicate/alias Truth rows into canonical rows.

Reads:  inputs/identity_lock/Persons_Truth_Final_v17.csv
        inputs/identity_lock/Placements_ByPerson_v17.csv
Writes: inputs/identity_lock/Persons_Truth_Final_v18.csv
        inputs/identity_lock/Placements_ByPerson_v18.csv

Usage:
  python tools/19_consolidate_truth.py           # dry run
  python tools/19_consolidate_truth.py --apply   # write v18 files
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IDENTITY_LOCK = ROOT / "inputs" / "identity_lock"

TRUTH_IN      = IDENTITY_LOCK / "Persons_Truth_Final_v17.csv"
PLACEMENTS_IN = IDENTITY_LOCK / "Placements_ByPerson_v17.csv"
TRUTH_OUT     = IDENTITY_LOCK / "Persons_Truth_Final_v18.csv"
PLACEMENTS_OUT = IDENTITY_LOCK / "Placements_ByPerson_v18.csv"

# ---------------------------------------------------------------------------
# Merge map: (keep_pid, new_canon_or_None, [merge_pids])
#
# keep_pid     — effective_person_id of the row that survives
# new_canon    — if not None, overwrite person_canon of the KEEP row (typo fix)
# merge_pids   — list of effective_person_ids to absorb and remove
# ---------------------------------------------------------------------------
MERGES: list[tuple[str, str | None, list[str]]] = [
    # G01
    ("34f7525c-19c0-5557-996f-cf770d365ed0", None, ["f2ae4e90-2897-50ba-9b73-ba9c756dd04e"]),
    # G02
    ("7c74c3f5-dc54-5e47-8365-e84497f4c943", None, ["401e7e05-8ce4-557c-be00-93f37ff605a2"]),
    # G03 – KEEP Dyakonov
    ("835fd269-0b77-57fe-9efc-cbdf4b567a8b", None, ["d89073bd-482b-56fa-9124-c124074cce19"]),
    # G04
    ("63846eb8-93a6-5795-a5bb-a9538ef3f1ee", None, ["e500b711-3e34-5191-b5c4-1b503d4710ee"]),
    # G05
    ("698fd3e8-a756-5c8e-bfdc-127a68fb6a58", None, ["40973be4-fcb3-53a0-b4f3-797d2afa13b4"]),
    # G06
    ("f825bd42-5056-55c5-8a4c-ba0788615e01", None, ["d9a98b36-9074-584e-8292-66d283f67f26"]),
    # G07
    ("123979e8-fdf3-53ef-90bb-e78d8f12c105", None, ["0d27e05e-8a5d-5858-adac-a70fa3ab20b5"]),
    # G08
    ("dfb408b6-d225-5b9f-b1c7-3f9a76c4319a", None, ["a9abce5e-8ccf-5cb0-88fe-129980008cd7"]),
    # G09
    ("dfc35597-39c1-5425-8b94-6e24dbc24696", None, [
        "d5ed0487-904c-5d5b-b324-655bb3f4886a",
        "4688d22b-c130-5ab9-971a-cd9206f35d24"]),
    # G10 – KEEP Cathy Clerc
    ("2d39bf4d-12bc-53b1-aa2e-ee0b4652e76a", None, ["56408d77-c8c0-52c2-9783-ad0054a5f33c"]),
    # G11
    ("d3f99c1e-fd05-5579-b3eb-34da912c3256", None, ["5d139352-5dab-5ee3-b397-6bb5d1044b2e"]),
    # G12
    ("94ffdc88-a565-59af-b73f-19c5f8d7e2ed", None, ["b39f8eea-92b2-5b87-b0ec-2036ecf95681"]),
    # G13
    ("5813a6cb-748f-5095-ba4d-50c77dee8281", None, ["11336800-703b-5349-b0a9-3dd4845602a5"]),
    # G14
    ("5ac74658-81ad-5c49-ba70-5834dfb04168", None, ["78cb7741-da32-5894-926c-168a190cc4d2"]),
    # G15
    ("737e36ea-4090-55ab-8844-2f4acc522880", None, [
        "5505be92-52c7-55c9-85a5-7af3db25d976",
        "f6e0fa79-dd47-5633-8599-fc534458a2e5"]),
    # G16 – fix typo Emilient→Emilien; merge doubles entry
    ("f81c53e1-a44e-5a20-894d-1cfeb2ed2b7a", "Emilien Groussin",
        ["97763f90-3a28-53e2-9f3f-2627cf961fc8"]),
    # G18
    ("ec1d5118-cd4b-591f-a84a-04bbe20868bc", None, [
        "bce17de7-c63c-5979-aa51-57c9de05550c",
        "3a22ab9a-3820-53d4-8eba-bab03ae0fc8b"]),
    # G19
    ("63b42121-63a4-5b68-a8b3-cceb8d6d4420", None, ["03d4a059-28e4-5b42-ab3f-12d988db5f86"]),
    # G20 – KEEP Jere Linnanen
    ("fc3f381a-650e-5582-910d-6a83702e9dae", None, ["e3f509a8-75b9-50b8-a4c7-47132a74dfe7"]),
    # G21
    ("9f21915c-0f75-5d9e-a5ab-50393d24e114", None, ["836d78ca-cf74-55d0-8911-89a3d8908c4e"]),
    # G22
    ("493ba501-d49d-54ed-98d3-1c3c4a8cc427", None, ["816a4d05-535f-5183-806e-84dd0f80b73d"]),
    # G24
    ("1cf1eda3-ba6d-5074-aaa7-85e6c5bb6567", None, ["69bd37f9-83fc-5e55-ac5b-68c3e52275e2"]),
    # G25
    ("16a5156d-35ae-595e-8157-f48db30bb628", None, ["6ff0b3ef-bbb5-5367-b6c3-25c38be575e8"]),
    # G26
    ("db8cc373-cc56-52c8-8e51-588d6a86ba1e", None, ["2b7c5582-fdb8-5f06-8077-3612c6668b97"]),
    # G27+G67 combined – Yulia Tikhomirova absorbs Julia Tokhomirova + CC doubles
    ("0b73f6bd-2b2a-57ef-8de6-853138d11297", None, [
        "83699fe0-f724-5278-a4c3-de300953810c",  # Julia Tokhomirova
        "84351ddb-8b4f-505d-a68d-ec7954bb5de8",  # Yulia Tikhomirova Hannes Daniel [CC]
        "8176f9b3-7133-5e8e-a2a3-e54cbdca7997",  # Yulia Tikhomirova Oxana Prikhodko [CC]
    ]),
    # G27 part 2 + G49 – Oxana Prikhodko absorbs Julia Tikhomirova Oxana Prikhodko + CC
    ("4e02fcd5-e3dd-540d-aa12-776f79332aa7", None, [
        "9a68859a-b441-5d95-8ce4-98c21ac1881c",  # Julia Tikhomirova Oxana Prikhodko
        "3232bf16-8d43-599e-a88a-55a27952f024",  # Oxana Prikhodko Yves Kreil [CC]
    ]),
    # G28
    ("eb025239-b9ab-5cef-9307-7639f88a225a", None, ["00bdcbfa-31e8-5cfc-89f6-0d5b0fbff276"]),
    # G29
    ("2a6a7c9e-1d8a-4f9a-a8f5-6f3a3c1e9b0f", None, ["67169c43-0d21-5dc4-ad82-3d3fffac2a92"]),
    # G31
    ("8e17712f-7c8d-5c61-93e2-85d061117be2", None, ["27b160a0-9bda-5a92-b876-90c61bd6711f"]),
    # G32
    ("ddf2f704-a4d2-5653-a82a-45987d37645c", None, ["096b4867-627f-58be-9bce-5fa9d12ceae7"]),
    # G33
    ("a633a48b-7677-5678-8471-f7d5ae291065", None, ["64eccff7-b6fe-589c-9c5b-e17d757c1798"]),
    # G34
    ("fbf52ca9-ba84-50bc-bce9-0244718c18e7", None, ["24ed0890-189e-5781-ac29-6098b5f48cc2"]),
    # G35
    ("ebad1497-e45b-538a-a18a-0a3cda5d730c", None, ["95c5cebc-ac1a-502f-b2cd-ebbc18269709"]),
    # G36
    ("43e31230-b102-5a58-a636-9da126870284", None, ["631c3797-beda-560e-8d5b-8b42846eb1d8"]),
    # G37 – KEEP Lon Smith
    ("60860e34-935d-5bb0-b718-a430fa4a142e", None, ["750aa0d1-72e5-5f79-940b-5d6b95fcd52f"]),
    # G38
    ("7732f45f-1020-563b-8dde-27c2c5fa9b09", None, ["82753d91-f644-5f4e-a9f6-fdcfd1dcf688"]),
    # G39
    ("fd8d3afc-a726-5ac8-841c-be6459e303bf", None, ["806e058d-8393-5b7d-9948-70a12d7379ab"]),
    # G40
    ("a5c9d383-777c-5f21-9604-6fd1cba79fb4", None, ["6ccee5e2-9e97-56d9-8be7-241072fbbf1d"]),
    # G41
    ("c7f3770c-6ebc-5e28-aeb0-c7913a67576f", None, [
        "ed2028b8-34b4-5569-acaf-cce91c823aa9",
        "071fce72-5f33-5892-a233-1d6c79a67568"]),
    # G42
    ("816fca6c-66be-5d1c-8141-ab2457e99a94", None, ["337dae34-54b0-5196-9943-88a7d247b0d7"]),
    # G43
    ("bb89d2bb-12af-5038-a092-0b4cf1269860", None, ["604f47af-933c-5934-a6bd-796ad27cb546"]),
    # G44
    ("9c8376c9-68ea-572f-abe2-822871cf0e7b", None, ["c99369b7-39a1-5601-95ba-8669b3fd2528"]),
    # G45
    ("eddfd46f-0832-5ae5-88fb-295807d3d337", None, ["c219fe71-93b7-5152-ae2b-256268eaafa8"]),
    # G46
    ("f52e0fe8-4eff-5532-9af4-14124f1e92ee", None, [
        "5cc26733-7b25-5c64-8831-272b05ce8106",
        "95cbdeb1-c0ee-5f8a-8d48-623e59d7bb9d"]),
    # G47
    ("eae790d9-83f5-5339-8828-c641f33ec86d", None, [
        "8a3a2528-4d12-5ff7-af0c-b6492c6612a7",
        "80c97760-d30b-5bc2-89cc-819aac040103"]),
    # G48
    ("9aca54b3-8989-5d9e-8ca3-b5420d233d8f", None, [
        "e31ad846-5778-5d40-b422-96906ce89f5c",
        "02f54255-a792-5878-b018-a3081c9d73e2"]),
    # G50
    ("ee1aedfe-c34a-5e70-abf9-44650e1b1859", None, [
        "44f780a2-6b4e-58bd-b563-ebeead408516",
        "7d31f233-3798-5af5-a3fe-451889126916"]),
    # G51
    ("0b3ffb10-2257-510e-a69a-26ff2fc87508", None, ["d643748b-552b-5a35-83aa-da8711b1ab9e"]),
    # G52
    ("384c1fd0-bc17-53d8-9680-55b03876dddf", None, ["bcd13c29-494a-5cfe-af23-65afb65de251"]),
    # G53
    ("bf058f46-4c2d-53cc-8caa-041c92a8f9fb", None, [
        "cf46bff4-6f2e-5dca-b6b3-a2053d3f450b",
        "b4c5aff8-9d84-59f4-ae27-d7b5b55b0c8b"]),
    # G54
    ("658a5146-c0da-5c84-8b5e-cd17b5ab1e90", None, [
        "05879ebc-11f6-5e12-940b-7a0d8dfbc145",
        "774f407b-3ca1-5f92-bf07-c582d86ea8a6"]),
    # G55
    ("7466be9e-b5c1-594a-8ee8-1c5c71b75157", None, ["b6ebc9ff-6076-5f81-b668-a2b4a56f0997"]),
    # G56
    ("1dc7bf3f-8a4d-57b0-912b-b6626fb29cfb", None, ["d9507c1b-52af-51c2-921c-50013d3b8d8f"]),
    # G57 – fix typo Koscienlny→Koscielny; merge doubles entry into individual
    ("153997cf-41c0-528b-85ef-d897c3186262", "Piotrek Koscielny",
        ["0e731f8b-2a4e-52ea-959d-ac545a017611"]),
    # G58
    ("0858e74f-3f58-51c5-8d9b-4a6f2333c08c", None, ["721563da-ef12-5e08-acba-974baa25f9c5"]),
    # G59
    ("21aff1eb-8eee-5870-b16b-d22012a6970a", None, [
        "11162260-78ae-51ed-883b-24aec58c17ed",
        "88248782-62c9-58eb-ba2a-f8f1bf02d0bd"]),
    # G60
    ("5c162da2-c670-5845-a3d6-a2ed097eb59b", None, ["0e67d530-15c3-5fc6-815b-c131b1826fc0"]),
    # G61
    ("95c181e2-ed8a-52bc-934c-cdf27acb1372", None, ["3666bb59-c3ad-5a4f-b399-7f8636cd72ff"]),
    # G62
    ("ee71cb5a-4b77-54c2-9bc7-c11165318ba5", None, [
        "cdd16786-2d91-5557-82ac-dcd4857c6fea",
        "b2d18ca0-20d6-5c0e-b598-f8f1776ca57f"]),
    # G64
    ("407048f7-6d55-5cec-b458-9170268c6335", None, ["a86a2032-743b-5e72-a3c1-8500f5cf0428"]),
    # G65
    ("97d60d0e-3503-5814-8b1d-3fe6bb6bff86", None, ["b31774a3-5b85-5c7d-9ce4-49006b3a6e9d"]),
    # G66
    ("4f12fea1-3237-5a49-bc94-31fd3e10d91c", None, ["ea58c649-d9cf-5319-8642-955775eb7db1"]),
    # G68
    ("c4f49339-ef54-500b-bedc-3e3ae2e081e1", None, ["23e4d460-7038-5df6-9858-cf133b2a2f0c"]),
    # REC-A: Aleksi Öhman
    ("998c87dc-4197-5f4d-97aa-f54b82f96a13", None, ["d38c8b25-f75d-5038-9f01-6fc23d7439c0"]),
    # REC-B: Adam Keith
    ("6c5e5fe4-c2d5-5d13-85df-1c751c10286e", None, ["57ad139d-7e97-5c38-b428-f673aabb1f05"]),
    # REC-C: Andreas Wolff
    ("a03090dc-7534-5454-aab3-f3a074f67d33", None, ["14147b38-a643-5f5a-a96e-5fc525c782a3"]),
    # REC-D: Ben Barrows
    ("cb2f5aeb-165a-5e47-96a0-b80622a0d252", None, ["e2b82836-8e14-5e8d-99f3-f1ba320aea06"]),
    # REC-F: José Bolívar
    ("ffdcaf7f-8188-5fa1-be12-6dbfdb4b9804", None, ["ca64a226-9831-54d2-b8cc-c9a72ef37f9a"]),
    # REC-G: Luis Lovera
    ("0462b4fa-e09a-5bb5-88f5-0e67d41a7dd2", None, ["05ab69b0-0db9-51e8-80a4-f0975cb8019f"]),
    # REC-K: Sébastien Maillet (fix canon + 3 merges)
    ("a9518594-35d9-53cd-8e8d-a7bc4de94236", "Sébastien Maillet", [
        "ad156bf0-a1fa-5d7b-9272-dac2de633172",  # Sébastian Maillet (typo)
        "ff478aa0-9ead-54aa-ae3b-0f90aed71a05",  # Seb Maillet
        "e4240b21-28ac-5eef-bcf5-c0e47ab30d3a",  # Sébastien Maillet Radoslav Rusev
    ]),
    # REC-L: Yeison Alzate Marulanda
    ("b6f808c7-3271-538c-b5df-36c2781dc89d", None, ["1e239558-36f9-55f8-981d-62c36bb534e7"]),
    # REC-M: Daniel Urrutia
    ("37031ab2-57df-5be0-8ea4-586101ecffb4", None, ["038c8436-a060-5e58-a565-4804d3cc0956"]),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def pipe_merge(*lists: str) -> str:
    """Merge pipe-separated token lists, deduplicating while preserving order."""
    seen: list[str] = []
    seen_set: set[str] = set()
    for lst in lists:
        for tok in lst.split(" | "):
            tok = tok.strip()
            if tok and tok not in seen_set:
                seen.append(tok)
                seen_set.add(tok)
    return " | ".join(seen)


def note_merge(*notes: str) -> str:
    parts = [n.strip() for n in notes if n.strip()]
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Write output files (default: dry run)")
    args = parser.parse_args()

    # Build merge lookup: merge_pid → keep_pid
    merge_to_keep: dict[str, str] = {}
    keep_canon_override: dict[str, str] = {}  # keep_pid → new canon
    all_merge_pids: set[str] = set()

    for keep_pid, new_canon, merge_pids in MERGES:
        for mpid in merge_pids:
            if mpid in merge_to_keep:
                print(f"ERROR: {mpid[:8]} appears in multiple merge groups!", file=sys.stderr)
                return 1
            merge_to_keep[mpid] = keep_pid
            all_merge_pids.add(mpid)
        if new_canon:
            keep_canon_override[keep_pid] = new_canon

    print(f"Merge map: {len(MERGES)} groups, {len(all_merge_pids)} rows to remove")

    # Load Truth
    truth_rows: list[dict] = []
    truth_fieldnames: list[str] = []
    with TRUTH_IN.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        truth_fieldnames = reader.fieldnames or []
        for r in reader:
            truth_rows.append(dict(r))

    print(f"Loaded Truth: {len(truth_rows)} rows")

    # Index by pid
    truth_by_pid: dict[str, dict] = {r["effective_person_id"]: r for r in truth_rows}

    # Validate all PIDs exist
    missing = []
    for keep_pid, _, merge_pids in MERGES:
        if keep_pid not in truth_by_pid:
            missing.append(f"KEEP {keep_pid[:8]} not found")
        for mpid in merge_pids:
            if mpid not in truth_by_pid:
                missing.append(f"MERGE {mpid[:8]} not found")
    if missing:
        for m in missing:
            print(f"ERROR: {m}", file=sys.stderr)
        return 1
    print(f"All PIDs validated OK")

    # Apply merges to Truth rows
    merged_count = 0
    for keep_pid, new_canon, merge_pids in MERGES:
        keep_row = truth_by_pid[keep_pid]

        # Apply canon override
        if new_canon:
            old_canon = keep_row["person_canon"]
            keep_row["person_canon"] = new_canon
            keep_row["person_canon.1"] = new_canon
            print(f"  Canon fix: {old_canon!r} → {new_canon!r}")

        for mpid in merge_pids:
            merge_row = truth_by_pid[mpid]
            # Absorb player_ids_seen
            keep_row["player_ids_seen"] = pipe_merge(
                keep_row["player_ids_seen"], merge_row["player_ids_seen"])
            # Absorb player_names_seen
            keep_row["player_names_seen"] = pipe_merge(
                keep_row["player_names_seen"], merge_row["player_names_seen"])
            # Absorb aliases
            keep_row["aliases"] = pipe_merge(
                keep_row["aliases"], merge_row["person_canon"],
                merge_row["aliases"])
            keep_row["alias_statuses"] = pipe_merge(
                keep_row["alias_statuses"], merge_row["alias_statuses"])
            # Absorb notes
            keep_row["notes"] = note_merge(
                keep_row["notes"],
                f"merged from {merge_row['person_canon']} ({mpid[:8]})")
            # Absorb aliases_presentable
            keep_row["aliases_presentable"] = pipe_merge(
                keep_row["aliases_presentable"],
                merge_row.get("aliases_presentable", ""))
            merged_count += 1

    # Build output Truth rows (exclude merged rows)
    truth_out = [r for r in truth_rows if r["effective_person_id"] not in all_merge_pids]
    removed = len(truth_rows) - len(truth_out)
    print(f"\nTruth: {len(truth_rows)} → {len(truth_out)} rows (-{removed} merged)")

    # Load Placements
    placements_rows: list[dict] = []
    placements_fieldnames: list[str] = []
    with PLACEMENTS_IN.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        placements_fieldnames = reader.fieldnames or []
        for r in reader:
            placements_rows.append(dict(r))

    print(f"Loaded Placements: {len(placements_rows)} rows")

    # Build canon lookup for keep rows after override
    keep_canon: dict[str, str] = {
        r["effective_person_id"]: r["person_canon"] for r in truth_out
    }

    # Remap Placements
    placements_remapped = 0
    for row in placements_rows:
        pid = row.get("person_id", "")
        if pid in merge_to_keep:
            new_keep_pid = merge_to_keep[pid]
            row["person_id"] = new_keep_pid
            row["person_canon"] = keep_canon.get(new_keep_pid, row["person_canon"])
            if "norm" in row:
                import re, unicodedata
                c = row["person_canon"]
                c2 = unicodedata.normalize("NFKD", c)
                c2 = "".join(ch for ch in c2 if not unicodedata.combining(ch))
                c2 = c2.lower()
                c2 = re.sub(r"\s+", " ", c2).strip()
                row["norm"] = c2
            placements_remapped += 1
        elif pid in keep_canon_override:
            # Canon was fixed on a KEEP row — update person_canon in placements
            row["person_canon"] = keep_canon_override[pid]
            if "norm" in row:
                import re, unicodedata
                c = row["person_canon"]
                c2 = unicodedata.normalize("NFKD", c)
                c2 = "".join(ch for ch in c2 if not unicodedata.combining(ch))
                c2 = c2.lower()
                c2 = re.sub(r"\s+", " ", c2).strip()
                row["norm"] = c2
            placements_remapped += 1

    print(f"Placements remapped: {placements_remapped} rows")

    if not args.apply:
        print("\nDry run complete. Pass --apply to write files.")
        return 0

    # Write Truth v18
    with TRUTH_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=truth_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(truth_out)
    print(f"\nWrote: {TRUTH_OUT} ({len(truth_out)} rows)")

    # Write Placements v18
    with PLACEMENTS_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=placements_fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(placements_rows)
    print(f"Wrote: {PLACEMENTS_OUT} ({len(placements_rows)} rows)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
