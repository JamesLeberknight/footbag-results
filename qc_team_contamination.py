import pandas as pd
import re

PF = "out/Placements_Flat.csv"

df = pd.read_csv(PF)

issues = []

CLUB_PATTERNS = [
    r"\bFC\b",
    r"\bFootbag\b",
    r"\bClub\b",
    r"\bTeam\b",
]

def looks_like_club(s):
    return any(re.search(p, s, re.IGNORECASE) for p in CLUB_PATTERNS)

def is_bad_case(name):
    return (
        name != name.title() and
        any(c.islower() for c in name) and
        any(c.isupper() for c in name)
    )

for _, row in df.iterrows():
    team = str(row.get("team_display_name", ""))
    division = str(row.get("division_canon", ""))
    event = row.get("event_id")

    if not team or team == "nan":
        continue

    parts = re.split(r"\s*/\s*|\s*-\s*", team)

    # 1. Club contamination
    for p in parts:
        if looks_like_club(p):
            issues.append(("CLUB_LEAK", event, division, team))
            break

    # 2. Singles wrongly containing "/"
    if "Singles" in division and "/" in team:
        issues.append(("SINGLES_SLASH", event, division, team))

    # 3. Lowercase canonical leak
    for p in parts:
        if is_bad_case(p):
            issues.append(("BAD_CASE", event, division, team))
            break

# Report
print(f"Total issues: {len(issues)}\n")

for i in issues[:50]:
    print(i)
