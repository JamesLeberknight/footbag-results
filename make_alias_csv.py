import re
import uuid
import csv
from pathlib import Path

# Fixed namespace UUID for stable uuid5 generation (do not change once you start using it)
NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")

def presentable_choice(names):
    # Prefer diacritics/non-ascii, avoid ALL CAPS, prefer longer/more specific
    def score(n: str):
        non_ascii = any(ord(ch) > 127 for ch in n)
        all_caps = n.isupper()
        return (1 if non_ascii else 0, 0 if all_caps else 1, len(n), n)
    return sorted(names, key=score, reverse=True)[0]

def strip_comment(s: str):
    # Keep inline notes if you used ///maybe etc.
    note = ""
    m = re.search(r"\s*(///.*)$", s)
    if m:
        note = m.group(1).strip()
        s = s[:m.start()].strip()
    return s.strip(), note.strip()

def is_noiseish(s: str) -> bool:
    t = s.lower()
    return any(k in t for k in [
        " aka ", "aka", "trick", "whirr", "moebius", "atomic blender", "gauntlet",
        "event record", "prize", "zombie", "represent", "leg over"
    ])

def main():
    txt_path = Path("alias-candidates.txt")
    text = txt_path.read_text(encoding="utf-8", errors="replace")

    # Groups are separated by blank lines
    groups = [g.strip() for g in re.split(r"\n\s*\n+", text) if g.strip()]

    out_path = Path("alias_candidates_with_person_id.csv")
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["alias", "person_id", "person_canon", "status", "notes"])

        for g in groups:
            lines = [ln.strip() for ln in g.splitlines() if ln.strip()]
            names = []
            notes_by = {}

            for ln in lines:
                base, note = strip_comment(ln)
                if not base:
                    continue
                names.append(base)
                if note:
                    notes_by[base] = note

            if not names:
                continue

            canon = presentable_choice(names)
            person_id = str(uuid.uuid5(NAMESPACE, canon))

            for alias in names:
                note = notes_by.get(alias, "")
                if is_noiseish(alias):
                    note = (note + " noise?").strip()
                if alias != canon and not note:
                    if any(ord(ch) > 127 for ch in alias) or any(ord(ch) > 127 for ch in canon):
                        note = "spelling/diacritics"
                    else:
                        note = "spelling"

                w.writerow([alias, person_id, canon, "candidate", note])

    print(f"Wrote {out_path} ({len(groups)} groups)")

if __name__ == "__main__":
    main()
