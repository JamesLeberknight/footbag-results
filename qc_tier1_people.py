# qc_tier1_people.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import re
import pandas as pd


@dataclass
class Issue:
    check_id: str
    severity: str   # "ERROR" | "WARN" | "INFO"
    field: str
    message: str
    example_value: str = ""
    context: dict | None = None


_RE_MOJIBAKE = re.compile(r"[¶¦±¼¿]|Mo¶|Fr±|Gwó¼d¼", re.UNICODE)
_RE_MULTI_PERSON = re.compile(r"\b(and|&|/|\+|vs\.?)\b", re.IGNORECASE)
_RE_TRAILING_JUNK = re.compile(r"[\*\-–—]+$")  # "Name*", "Name -"
_RE_OPEN_PAREN = re.compile(r"\([^)]*$")       # "Name (Phoenix" missing close paren


def looks_like_person(name: str) -> bool:
    s = (name or "").strip()
    if not s:
        return False
    if len(s.split()) < 2:
        return False
    low = s.lower()
    if low in {"na", "dnf", "()", "nd", "th"}:
        return False
    if any(x in low for x in ["club", "footbag", "position", "match", "ifpa", "footstar"]):
        return False
    if re.search(r'-[A-Z]+\)\s*$', s):   # "-CANADA)", "-USA)"
        return False
    if s.upper().startswith("RESULTS"):
        return False
    if re.search(r'\b\d{3,}\b', s):       # 3+ digit number token (IFPA/handicap IDs)
        return False
    if low.rstrip().endswith(" and") or low.rstrip().endswith(") and") or low.rstrip().endswith(") or"):
        return False
    return True


def load_pf(pf_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(pf_csv)
    # normalize empties
    for c in ["player1_name", "player2_name", "player1_person_id", "player2_person_id",
              "player1_person_canon", "player2_person_canon"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)
    return df


def run_tier1_people_qc(pf: pd.DataFrame, top_n: int = 200) -> tuple[dict, list[Issue]]:
    issues: list[Issue] = []

    def scan_side(side: int) -> None:
        name = pf.get(f"player{side}_name", pd.Series([""] * len(pf)))
        pid  = pf.get(f"player{side}_person_id", pd.Series([""] * len(pf)))
        canon = pf.get(f"player{side}_person_canon", pd.Series([""] * len(pf)))

        # 1) looks-like-person but missing person_id
        mask_unmapped = name.map(looks_like_person) & (pid.str.strip() == "")
        if mask_unmapped.any():
            top = name[mask_unmapped].value_counts().head(top_n)
            for n, cnt in top.items():
                issues.append(Issue(
                    check_id="T1_UNMAPPED_PERSON_NAME",
                    severity="WARN",
                    field=f"player{side}_person_id",
                    message="Name looks like a person but has no person_id (candidate for Person_Aliases / cleanup).",
                    example_value=f"{n} (count={cnt})",
                ))

        # 2) canon missing when person_id present
        mask_canon_missing = (pid.str.strip() != "") & (canon.str.strip() == "")
        if mask_canon_missing.any():
            ex = name[mask_canon_missing].value_counts().head(20)
            for n, cnt in ex.items():
                issues.append(Issue(
                    check_id="T1_CANON_MISSING_WITH_PERSON_ID",
                    severity="ERROR",
                    field=f"player{side}_person_canon",
                    message="person_id present but person_canon is empty (should be deterministic).",
                    example_value=f"{n} (count={cnt})",
                ))

        # 3) mojibake remnants
        mask_moj = name.str.contains(_RE_MOJIBAKE, na=False)
        if mask_moj.any():
            ex = name[mask_moj].value_counts().head(50)
            for n, cnt in ex.items():
                issues.append(Issue(
                    check_id="T1_NAME_MOJIBAKE_REMAINS",
                    severity="WARN",
                    field=f"player{side}_name",
                    message="Name contains mojibake-like characters; should be repaired before aliasing.",
                    example_value=f"{n} (count={cnt})",
                ))

        # 4) multi-person strings in a single name field
        mask_multi = name.str.contains(_RE_MULTI_PERSON, na=False) & (name.str.len() > 0)
        if mask_multi.any():
            ex = name[mask_multi].value_counts().head(50)
            for n, cnt in ex.items():
                issues.append(Issue(
                    check_id="T1_MULTI_PERSON_IN_NAME_FIELD",
                    severity="WARN",
                    field=f"player{side}_name",
                    message="Single name field appears to contain multiple people; needs parsing/splitting or quarantine.",
                    example_value=f"{n} (count={cnt})",
                ))

        # 5) trailing junk markers (require 2+ words — pure noise like "*" or "G*" are excluded)
        mask_tail = name.str.contains(_RE_TRAILING_JUNK, na=False) & (name.str.split().str.len() >= 2)
        if mask_tail.any():
            ex = name[mask_tail].value_counts().head(50)
            for n, cnt in ex.items():
                issues.append(Issue(
                    check_id="T1_TRAILING_JUNK_MARKER",
                    severity="INFO",
                    field=f"player{side}_name",
                    message="Name ends with junk marker (*, -, –); consider cleanup rule.",
                    example_value=f"{n} (count={cnt})",
                ))

        # 6) open parenthesis fragment
        mask_open = name.str.contains(_RE_OPEN_PAREN, na=False)
        if mask_open.any():
            ex = name[mask_open].value_counts().head(50)
            for n, cnt in ex.items():
                issues.append(Issue(
                    check_id="T1_OPEN_PAREN_FRAGMENT",
                    severity="INFO",
                    field=f"player{side}_name",
                    message="Name contains an unmatched '(' fragment (often location spill).",
                    example_value=f"{n} (count={cnt})",
                ))

    scan_side(1)
    scan_side(2)

    summary = {
        "issues_total": len(issues),
        "counts_by_check_id": pd.Series([i.check_id for i in issues]).value_counts().to_dict(),
        "counts_by_severity": pd.Series([i.severity for i in issues]).value_counts().to_dict(),
    }
    return summary, issues


def main() -> int:
    repo = Path(__file__).resolve().parent
    pf_csv = repo / "out" / "Placements_Flat.csv"
    out_dir = repo / "out"

    pf = load_pf(pf_csv)
    summary, issues = run_tier1_people_qc(pf)

    (out_dir / "qc_tier1_people_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (out_dir / "qc_tier1_people_issues.jsonl").open("w", encoding="utf-8") as f:
        for i in issues:
            f.write(json.dumps(i.__dict__, ensure_ascii=False) + "\n")

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
