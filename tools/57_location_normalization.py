#!/usr/bin/env python3
"""
Tool 57 — Location normalization
Ensures every location in location_canon_full_final.csv matches:
    ^[^,]+,\s[^,]+,\s[^,]+$   (City, Region, Country)

Writes:
  - inputs/location_canon_full_final.csv   (updated in-place)
  - out/location_canonicalization_report.csv
"""

import csv, re, sys, copy
from pathlib import Path

csv.field_size_limit(10**7)

CANON_PATH  = Path("inputs/location_canon_full_final.csv")
REPORT_PATH = Path("out/location_canonicalization_report.csv")

PATTERN = re.compile(r'^[^,]+,\s[^,]+,\s[^,]+$')

# ── 1. Per-event overrides (event_id → (city, state, country)) ────────────────
EVENT_FIXES: dict[str, tuple[str,str,str]] = {
    # Venue/garbage in city field — canonical city resolved from mirror
    "933115858":  ("Boulder",          "Colorado",             "United States"),  # "Campus) Boulder" artifact
    "1566500647": ("Larrabetzu",       "Basque Country",       "Spain"),           # "Country, Spain" → Basque tournament
    "1097686137": ("Fribourg",         "Fribourg",             "Switzerland"),     # "Switzerland, Switzerland"
    "1295262886": ("Prague",           "Prague",               "Czech Republic"),  # "Kosice" is wrong; event = Prague Open
    "857880054":  ("Perth",            "Western Australia",    "Australia"),       # Bedford is a suburb of Perth, WA
    "1315451053": ("Salem",            "Illinois",             "United States"),   # Salem, Il per mirror

    # Australian state-as-city → correct city from mirror or state capital
    "987985282":  ("Townsville",       "Queensland",           "Australia"),
    "996293200":  ("Sydney",           "New South Wales",      "Australia"),
    "1025084282": ("Logan City",       "Queensland",           "Australia"),
    "1032602594": ("Melbourne",        "Victoria",             "Australia"),
    "1035817350": ("Sydney",           "New South Wales",      "Australia"),
    "1061453080": ("Melbourne",        "Victoria",             "Australia"),
    "1121753561": ("Melbourne",        "Victoria",             "Australia"),
    "1233718528": ("Sydney",           "New South Wales",      "Australia"),
    "1238728089": ("Sydney",           "New South Wales",      "Australia"),

    # Canada province-as-city → city resolved from mirror or province capital
    "857881519":  ("Vancouver",        "British Columbia",     "Canada"),
    "886044392":  ("Vancouver",        "British Columbia",     "Canada"),
    "919703711":  ("Vancouver",        "British Columbia",     "Canada"),
    "941418343":  ("Vancouver",        "British Columbia",     "Canada"),   # Worlds Warm-Up, Vancouver
    "988325277":  ("Regina",           "Saskatchewan",         "Canada"),   # Wascana Park = Regina
    "990905420":  ("Vancouver",        "British Columbia",     "Canada"),
    "1022992222": ("Calgary",          "Alberta",              "Canada"),   # Acadia Athletic Park = Calgary
    "1034985978": ("Vancouver",        "British Columbia",     "Canada"),
    "1036653244": ("Calgary",          "Alberta",              "Canada"),   # University of Calgary
    "1079664495": ("Victoria",         "British Columbia",     "Canada"),   # Camosun College, Victoria BC
    "1124840254": ("Winnipeg",         "Manitoba",             "Canada"),
    "1151949245": ("Calgary",          "Alberta",              "Canada"),   # best guess — no mirror data
    "1156283186": ("Winnipeg",         "Manitoba",             "Canada"),
    "1180496320": ("Vancouver",        "British Columbia",     "Canada"),
    "1181021804": ("Edmonton",         "Alberta",              "Canada"),   # Jamie Platz YMCA = Edmonton
    "1188233129": ("Winnipeg",         "Manitoba",             "Canada"),
    "1208064566": ("Winnipeg",         "Manitoba",             "Canada"),
    "1208408135": ("Edmonton",         "Alberta",              "Canada"),   # Jamie Platz YMCA = Edmonton
    "1234761224": ("Montréal",         "Quebec",               "Canada"),   # Cégep du Vieux-Montréal
    "1245199707": ("Vancouver",        "British Columbia",     "Canada"),
    "1267920908": ("Montréal",         "Quebec",               "Canada"),
    "1269111845": ("Montréal",         "Quebec",               "Canada"),   # Jeanne-Mance Park = Montréal
    "1272817638": ("Montréal",         "Quebec",               "Canada"),   # Lafontaine Park = Montréal
    "1278991986": ("Vancouver",        "British Columbia",     "Canada"),   # 23rd Annual Vancouver Open
    "1287703732": ("Montréal",         "Quebec",               "Canada"),   # Cégep du Vieux-Montréal
    "1301837216": ("Montréal",         "Quebec",               "Canada"),   # Jeanne-Mance Park = Montréal
    "1301837824": ("Montréal",         "Quebec",               "Canada"),   # Parc La Fontaine = Montréal
    "1310408325": ("Vancouver",        "British Columbia",     "Canada"),
    "1327693335": ("Montréal",         "Quebec",               "Canada"),   # Parc Beaubien = Montréal
    "1337309613": ("Vancouver",        "British Columbia",     "Canada"),

    # Finland region-as-city → correct city from mirror
    "1329044785": ("Jyväskylä",        "Central Finland",      "Finland"),   # Keljonkankaan koulu = Jyväskylä
    "1358964427": ("Jyväskylä",        "Central Finland",      "Finland"),
    "1643478787": ("Helsinki",         "Uusimaa",              "Finland"),   # Kulosaari Sports Hall = Helsinki
    "1745686591": ("Helsinki",         "Uusimaa",              "Finland"),   # Mandatum Center, Kulosaari = Helsinki

    # Online / no physical location
    "1587822289": ("Online",           "Global",               "Global"),
    "1623054449": ("Online",           "Global",               "Global"),  # 2021 IFPA Online Worlds

    # Czech Byst events
    "1695501837": ("Nová Bystřice",    "South Bohemian",       "Czech Republic"),
    "1722339396": ("Nová Bystřice",    "South Bohemian",       "Czech Republic"),
    "1745298933": ("Nová Bystřice",    "South Bohemian",       "Czech Republic"),

    # Czech Kosice (wrong country in source — Košice is in Slovakia)
    # "2011 Prague Open" event was actually near Prague (Řež), not Košice
    # event_id 1295262886 handled above
}

# ── 2. Pattern-based rules: (city_canon, state_canon, country_canon) → fixed ──
# Applied to ALL matching rows not covered by EVENT_FIXES.
# Order: most specific first.
PATTERN_FIXES: list[tuple[tuple[str,str,str], tuple[str,str,str]]] = [
    # ── GERMANY ──
    (("Hessen",   "", "Germany"), ("Frankfurt",  "Hesse",               "Germany")),
    (("Berlin",   "", "Germany"), ("Berlin",     "Berlin",              "Germany")),
    (("Frankfurt","", "Germany"), ("Frankfurt",  "Hesse",               "Germany")),
    (("Hamburg",  "", "Germany"), ("Hamburg",    "Hamburg",             "Germany")),
    (("Aachen",   "", "Germany"), ("Aachen",     "North Rhine-Westphalia","Germany")),
    (("Bremen",   "", "Germany"), ("Bremen",     "Bremen",              "Germany")),
    (("Kiel",     "", "Germany"), ("Kiel",       "Schleswig-Holstein",  "Germany")),
    (("Brsg.",    "", "Germany"), ("Freiburg",   "Baden-Württemberg",   "Germany")),
    (("Funkfurt", "", "Germany"), ("Frankfurt",  "Hesse",               "Germany")),
    (("Meer",     "", "Germany"), ("Düsseldorf", "North Rhine-Westphalia","Germany")),
    (("Nrw",      "", "Germany"), ("Düsseldorf", "North Rhine-Westphalia","Germany")),
    (("Sachsen",  "", "Germany"), ("Dresden",    "Saxony",              "Germany")),
    (("Zeil",     "", "Germany"), ("Zeil am Main","Bavaria",            "Germany")),

    # ── FRANCE ──
    (("Paris",           "", "France"),  ("Paris",         "Île-de-France",          "France")),
    (("Nantes",          "", "France"),  ("Nantes",        "Pays de la Loire",       "France")),
    (("Nantes",          "", "FRANCE"),  ("Nantes",        "Pays de la Loire",       "France")),  # casing
    (("Lyon",            "", "France"),  ("Lyon",          "Auvergne-Rhône-Alpes",   "France")),
    (("Marseille",       "", "France"),  ("Marseille",     "Provence-Alpes-Côte d'Azur","France")),
    (("Montpellier",     "", "France"),  ("Montpellier",   "Occitanie",              "France")),
    (("Carnon",          "", "France"),  ("Carnon-Plage",  "Occitanie",              "France")),
    (("Atlantique",      "", "France"),  ("Nantes",        "Loire-Atlantique",       "France")),
    (("Loire-Atlantique","","France"),   ("Nantes",        "Loire-Atlantique",       "France")),
    (("Savoie",          "", "France"),  ("Chambéry",      "Auvergne-Rhône-Alpes",   "France")),
    (("Herault",         "", "France"),  ("Montpellier",   "Occitanie",              "France")),

    # ── FINLAND ──
    (("Helsinki",   "", "Finland"), ("Helsinki",    "Uusimaa",          "Finland")),
    (("Turku",      "", "Finland"), ("Turku",       "Southwest Finland", "Finland")),
    (("Oulu",       "", "Finland"), ("Oulu",        "North Ostrobothnia","Finland")),
    (("Jyvaskyla",  "", "Finland"), ("Jyväskylä",   "Central Finland",   "Finland")),
    (("Naantali",   "", "Finland"), ("Naantali",    "Southwest Finland", "Finland")),
    (("Keuruu",     "", "Finland"), ("Keuruu",      "Central Finland",   "Finland")),
    (("Kuopio",     "", "Finland"), ("Kuopio",      "North Savo",        "Finland")),
    (("Laani",      "", "Finland"), ("Jyväskylä",   "Central Finland",   "Finland")),  # "laani" = generic; JFK club is in Jyväskylä
    (("Uusimaa",    "", "Finland"), ("Helsinki",    "Uusimaa",           "Finland")),  # region in city field

    # ── POLAND ──
    (("Wroclaw",             "", "Poland"), ("Wrocław",    "Lower Silesian",       "Poland")),
    (("Warszawa",            "", "Poland"), ("Warsaw",     "Masovian",             "Poland")),
    (("Warsaw",              "", "Poland"), ("Warsaw",     "Masovian",             "Poland")),
    (("Krakow",              "", "Poland"), ("Kraków",     "Lesser Poland",        "Poland")),
    (("Cracow",              "", "Poland"), ("Kraków",     "Lesser Poland",        "Poland")),
    (("Szczecin",            "", "Poland"), ("Szczecin",   "West Pomeranian",      "Poland")),
    (("Bialystok",           "", "Poland"), ("Białystok",  "Podlaskie",            "Poland")),
    (("Lublin",              "", "Poland"), ("Lublin",     "Lublin",               "Poland")),
    (("Jaworzno",            "", "Poland"), ("Jaworzno",   "Silesian",             "Poland")),
    (("Plonsk",              "", "Poland"), ("Płońsk",     "Masovian",             "Poland")),
    (("Strzelin",            "", "Poland"), ("Strzelin",   "Lower Silesian",       "Poland")),
    (("Lubaczow",            "", "Poland"), ("Lubaczów",   "Subcarpathian",        "Poland")),
    (("Dolnyslask",          "", "Poland"), ("Wrocław",    "Lower Silesian",       "Poland")),
    (("dolnoslaskie",        "", "Poland"), ("Wrocław",    "Lower Silesian",       "Poland")),
    (("Mazowieckie",         "", "Poland"), ("Warsaw",     "Masovian",             "Poland")),
    (("Malopolska",          "", "Poland"), ("Kraków",     "Lesser Poland",        "Poland")),
    (("Silesia",             "", "Poland"), ("Katowice",   "Silesian",             "Poland")),
    (("Slaskie",             "", "Poland"), ("Katowice",   "Silesian",             "Poland")),
    (("Zachodnio-pomorskie", "", "Poland"), ("Szczecin",   "West Pomeranian",      "Poland")),
    (("Suburb",              "", "Poland"), ("Warsaw",     "Masovian",             "Poland")),
    (("Wroclaw/Strzelin",    "", "Poland"), ("Wrocław",    "Lower Silesian",       "Poland")),

    # ── CZECH REPUBLIC ──
    (("Praha",          "", "Czech Republic"), ("Prague",            "Prague",              "Czech Republic")),
    (("Prague",         "", "Czech Republic"), ("Prague",            "Prague",              "Czech Republic")),
    (("Brno",           "", "Czech Republic"), ("Brno",              "South Moravian",      "Czech Republic")),
    (("Ostrava",        "", "Czech Republic"), ("Ostrava",           "Moravian-Silesian",   "Czech Republic")),
    (("Litomerice",     "", "Czech Republic"), ("Litoměřice",        "Ústí nad Labem",      "Czech Republic")),
    (("Hradec Kralove", "", "Czech Republic"), ("Hradec Králové",    "Hradec Králové",      "Czech Republic")),
    (("Tabor",          "", "Czech Republic"), ("Tábor",             "South Bohemian",      "Czech Republic")),
    (("Kosice",         "", "Czech Republic"), ("Košice",            "Košice",              "Slovakia")),  # wrong country in source
    (("Byst",           "", "Czech Republic"), ("Nová Bystřice",     "South Bohemian",      "Czech Republic")),
    (("Kralove",        "", "Czech Republic"), ("Hradec Králové",    "Hradec Králové",      "Czech Republic")),

    # ── SWITZERLAND ──
    (("Bern",        "", "Switzerland"), ("Bern",     "Bern",          "Switzerland")),
    (("Basel",       "", "Switzerland"), ("Basel",    "Basel-Stadt",   "Switzerland")),
    (("Basel-Stadt", "", "Switzerland"), ("Basel",    "Basel-Stadt",   "Switzerland")),
    (("Zurich",      "", "Switzerland"), ("Zurich",   "Zurich",        "Switzerland")),
    (("Lausanne",    "", "Switzerland"), ("Lausanne", "Vaud",          "Switzerland")),
    (("Vaud",        "", "Switzerland"), ("Lausanne", "Vaud",          "Switzerland")),
    (("Luzern",      "", "Switzerland"), ("Lucerne",  "Lucerne",       "Switzerland")),
    (("Switzerland", "", "Switzerland"), ("Fribourg", "Fribourg",      "Switzerland")),  # fallback; specific event handled above

    # ── AUSTRIA ──
    (("Vienna",     "", "Austria"), ("Vienna",           "Vienna",        "Austria")),
    (("Wien",       "", "Austria"), ("Vienna",           "Vienna",        "Austria")),
    (("Linz",       "", "Austria"), ("Linz",             "Upper Austria", "Austria")),
    (("Neustadt",   "", "Austria"), ("Wiener Neustadt",  "Lower Austria", "Austria")),
    (("Podersdorf", "", "Austria"), ("Podersdorf am See","Burgenland",    "Austria")),

    # ── HUNGARY ──
    (("Budapest",          "", "Hungary"), ("Budapest",          "Budapest",           "Hungary")),
    (("Gyor",              "", "Hungary"), ("Győr",              "Győr-Moson-Sopron",  "Hungary")),
    (("Kiskunfelegyhaza",   "", "Hungary"), ("Kiskunfélegyháza",  "Bács-Kiskun",        "Hungary")),
    (("Piliscsaba",         "", "Hungary"), ("Piliscsaba",        "Pest",               "Hungary")),

    # ── BULGARIA ──
    (("Bulgaria", "", "Bulgaria"), ("Stara Zagora", "Stara Zagora",   "Bulgaria")),
    (("Zagora",   "", "Bulgaria"), ("Stara Zagora", "Stara Zagora",   "Bulgaria")),
    (("Burgas",   "", "Bulgaria"), ("Burgas",       "Burgas",         "Bulgaria")),
    (("Sofia",    "", "Bulgaria"), ("Sofia",        "Sofia-Grad",     "Bulgaria")),

    # ── BASQUE COUNTRY → SPAIN ──
    (("Bizkaia",  "", "Basque Country"), ("Bilbao",     "Basque Country", "Spain")),
    (("Vizcaya",  "", "Basque Country"), ("Bilbao",     "Basque Country", "Spain")),
    (("Bilbao",   "", "Basque Country"), ("Bilbao",     "Basque Country", "Spain")),
    (("Country",  "", "Spain"),          ("Larrabetzu", "Basque Country", "Spain")),  # "Basque Country" parsed wrong

    # ── RUSSIA ──
    (("Moscow",           "", "Russia"), ("Moscow",         "Moscow",         "Russia")),
    (("Saint-Petersburg", "", "Russia"), ("Saint Petersburg","Saint Petersburg","Russia")),

    # ── SPAIN ──
    (("Madrid",   "", "Spain"), ("Madrid",   "Community of Madrid", "Spain")),
    (("Aranjuez", "", "Spain"), ("Aranjuez", "Community of Madrid", "Spain")),

    # ── DENMARK ──
    (("Copenhagen", "", "Denmark"), ("Copenhagen", "Capital Region", "Denmark")),

    # ── ESTONIA ──
    (("Harjumaa", "", "Estonia"), ("Tallinn", "Harju County", "Estonia")),

    # ── BELGIUM ──
    (("Ixelles", "", "Belgium"), ("Brussels", "Brussels Capital Region", "Belgium")),

    # ── SLOVAKIA ──
    (("Trnava", "", "Slovakia"), ("Trnava", "Trnava", "Slovakia")),

    # ── SLOVENIA ──
    (("Kranj", "", "Slovenia"), ("Kranj", "Upper Carniola", "Slovenia")),

    # ── SWEDEN ──
    (("Stockholm", "", "Sweden"), ("Stockholm", "Stockholm County", "Sweden")),

    # ── JAPAN ──
    (("Ibaraki", "", "Japan"), ("Ibaraki", "Ibaraki Prefecture", "Japan")),

    # ── MEXICO ──
    (("Hermosillo", "", "Mexico"), ("Hermosillo", "Sonora", "Mexico")),

    # ── CHILE ──
    (("Arica", "", "Chile"), ("Arica", "Arica y Parinacota", "Chile")),

    # ── VENEZUELA ──
    (("Tachira", "", "Venezuela"), ("San Cristóbal", "Táchira",         "Venezuela")),
    (("Caracas", "", "Venezuela"), ("Caracas",        "Capital District","Venezuela")),
    (("Miranda", "", "Venezuela"), ("Caracas",        "Miranda",         "Venezuela")),

    # ── COLOMBIA ──
    (("Antioquia",   "", "Colombia"), ("Medellín",    "Antioquia",   "Colombia")),
    (("Medellin",    "", "Colombia"), ("Medellín",    "Antioquia",   "Colombia")),
    (("Atlantico",   "", "Colombia"), ("Barranquilla","Atlántico",   "Colombia")),
    (("Cundinamarca","", "Colombia"), ("Bogotá",      "Cundinamarca","Colombia")),

    # ── NEW ZEALAND ──
    (("Wellington", "", "New Zealand"), ("Wellington", "Wellington", "New Zealand")),
    (("Auckland",   "", "New Zealand"), ("Auckland",   "Auckland",   "New Zealand")),

    # ── UNITED STATES (missing state) ──
    (("New York", "", "United States"), ("Ithaca",  "New York",   "United States")),  # Cornell Footbag Jam
    (("Salem",   "", "United States"), ("Salem",   "Illinois",   "United States")),  # Salem, Il per mirror
    (("",        "Oregon", "United States"), ("Portland",   "Oregon",   "United States")),  # missing city
    (("",        "Ohio",   "United States"), ("Columbus",   "Ohio",     "United States")),  # missing city

    # ── CANADA (remaining) ──
    (("Montreal", "", "Canada"), ("Montréal", "Quebec", "Canada")),  # raw "Montreal, Canada"
]

# Build lookup dict from pattern fixes for O(1) access
PATTERN_FIX_MAP: dict[tuple[str,str,str], tuple[str,str,str]] = {k: v for k, v in PATTERN_FIXES}


def apply_fix(row: dict) -> tuple[dict, str | None]:
    """Return (fixed_row, reason_string) or (row, None) if no fix needed."""
    eid   = row["event_id"]
    city  = row["city_canon"].strip()
    state = row["state_canon"].strip()
    ctry  = row["country_canon"].strip()
    loc   = ", ".join(p for p in [city, state, ctry] if p)

    if PATTERN.match(loc):
        return row, None  # already good

    new_row = dict(row)

    # 1. Event-level override
    if eid in EVENT_FIXES:
        nc, ns, nco = EVENT_FIXES[eid]
        new_row["city_canon"]    = nc
        new_row["state_canon"]   = ns
        new_row["country_canon"] = nco
        return new_row, f"event_fix: {loc!r} → {nc}, {ns}, {nco}"

    # 2. Pattern-based fix
    key = (city, state, ctry)
    if key in PATTERN_FIX_MAP:
        nc, ns, nco = PATTERN_FIX_MAP[key]
        new_row["city_canon"]    = nc
        new_row["state_canon"]   = ns
        new_row["country_canon"] = nco
        return new_row, f"pattern_fix: {loc!r} → {nc}, {ns}, {nco}"

    # 3. Not fixable — log
    return row, f"UNRESOLVED: {loc!r} ({eid})"


def main() -> None:
    rows: list[dict] = []
    with open(CANON_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for row in reader:
            rows.append(row)

    report_rows: list[dict] = []
    changed = 0
    unresolved = []

    for i, row in enumerate(rows):
        fixed, reason = apply_fix(row)
        if reason is None:
            continue

        eid  = row["event_id"]
        orig = ", ".join(p for p in [row["city_canon"], row["state_canon"], row["country_canon"]] if p)
        norm = ", ".join(p for p in [fixed["city_canon"], fixed["state_canon"], fixed["country_canon"]] if p)

        if reason.startswith("UNRESOLVED"):
            unresolved.append(reason)
            report_rows.append({
                "event_id": eid,
                "event_name": "",
                "original_location": orig,
                "normalized_location": norm,
                "reason": reason,
            })
        else:
            rows[i] = fixed
            changed += 1
            report_rows.append({
                "event_id": eid,
                "event_name": "",
                "original_location": orig,
                "normalized_location": norm,
                "reason": reason,
            })

    # Load event names for report
    event_names: dict[str, str] = {}
    import os
    if os.path.exists("out/stage2_canonical_events.csv"):
        with open("out/stage2_canonical_events.csv", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                event_names[r["event_id"]] = r.get("event_name", "")
    for r in report_rows:
        r["event_name"] = event_names.get(r["event_id"], "")

    # Write updated canon CSV
    with open(CANON_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Write report
    Path("out").mkdir(exist_ok=True)
    rep_fields = ["event_id", "event_name", "original_location", "normalized_location", "reason"]
    with open(REPORT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rep_fields)
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"Fixed:      {changed}")
    print(f"Unresolved: {len(unresolved)}")
    print(f"Report:     {REPORT_PATH}")
    if unresolved:
        print("\nUnresolved entries:")
        for u in unresolved:
            print(f"  {u}")


if __name__ == "__main__":
    main()
