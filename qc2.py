import pandas as pd
import re

PF = pd.read_csv("out/Placements_Flat.csv")

issues = []

def is_doubles(div):
    return "doubles" in str(div).lower()

def is_singles(div):
    d = str(div).lower()
    return any(x in d for x in ["singles", "freestyle", "golf", "consecutive"])

def looks_like_city(name):
    if pd.isna(name):
        return False
    return bool(re.search(r"/\s*[A-Za-z\s]+[A-Z]{2}$", name))

# Group by event/div/place
groups = PF.groupby(["event_id", "division_canon", "place"])

for (event, div, place), g in groups:

    players = g[g["competitor_type"] == "player"]
    teams = g[g["competitor_type"] == "team"]

    num_players = len(players)

    # --- A. Doubles missing partner ---
    if is_doubles(div):
        if num_players == 1:
            issues.append((event, div, place, "HIGH", "doubles_only_one_player"))

    # --- B. Singles with multiple players (NOT tie) ---
    if is_singles(div):
        if num_players > 1:
            places = players["place"].unique()
            if len(places) == 1:
                # tie → OK
                pass
            else:
                issues.append((event, div, place, "HIGH", "singles_wrong_player_count"))

    # --- C. Team row in singles ---
    if is_singles(div):
        if len(teams) > 0:
            issues.append((event, div, place, "HIGH", "team_row_in_singles"))

    # --- D. NON_PERSON ---
    if (g["person_canon"] == "__NON_PERSON__").any():
        issues.append((event, div, place, "HIGH", "non_person_present"))

    # --- E. City artifact ---
    for name in g["team_display_name"].dropna():
        if looks_like_city(name):
            issues.append((event, div, place, "HIGH", "city_as_partner"))

# Save
df = pd.DataFrame(issues, columns=["event", "division", "place", "severity", "issue"])
df.to_csv("out/qc_workbook_v2.csv", index=False)

print("QC v2 issues:", len(df))
