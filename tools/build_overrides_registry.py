#!/usr/bin/env python3
"""
build_overrides_registry.py

One-off helper: extract override dicts/sets from your existing pipeline code
WITHOUT modifying pipeline logic, and write a single JSONL registry.

It reads literal assignments like YEAR_OVERRIDES = {...} using ast.literal_eval,
so it is safe (no execution), but it assumes the overrides are literals.
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any, Dict, Iterable


def _extract_literal_assignments(py_path: Path, names: Iterable[str]) -> Dict[str, Any]:
    src = py_path.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(py_path))
    wanted = set(names)
    out: Dict[str, Any] = {}

    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            var = node.targets[0].id
            if var in wanted:
                out[var] = ast.literal_eval(node.value)  # dict/set/list/tuple/str/int only
    return out


def _as_event_id_str(eid: Any) -> str:
    # normalize event_id keys that might be int in Stage 1
    try:
        return str(int(eid))
    except Exception:
        return str(eid).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage1", type=Path, default=Path("01_parse_mirror.py"))
    ap.add_argument("--stage2", type=Path, default=Path("02_canonicalize_results.py"))
    ap.add_argument("--out", type=Path, default=Path("overrides/events_overrides.jsonl"))
    args = ap.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    stage1_names = ["YEAR_OVERRIDES", "DATE_OVERRIDES"]
    stage2_names = [
        "YEAR_OVERRIDES",
        "LOCATION_OVERRIDES",
        "EVENT_TYPE_OVERRIDES",
        "EVENT_NAME_OVERRIDES",
        "JUNK_EVENTS_TO_EXCLUDE",
        "KNOWN_BROKEN_SOURCE_EVENTS",
    ]

    s1 = _extract_literal_assignments(args.stage1, stage1_names)
    s2 = _extract_literal_assignments(args.stage2, stage2_names)

    # merge into a single per-event record
    by_event: Dict[str, Dict[str, Any]] = {}

    def upsert(eid: Any, **fields: Any) -> None:
        k = _as_event_id_str(eid)
        rec = by_event.setdefault(k, {"event_id": k})
        for f, v in fields.items():
            if v is None:
                continue
            rec[f] = v

    # Stage 1
    for eid, year in (s1.get("YEAR_OVERRIDES") or {}).items():
        upsert(eid, year=int(year), source="stage1.YEAR_OVERRIDES", confidence="high")
    for eid, date in (s1.get("DATE_OVERRIDES") or {}).items():
        upsert(eid, date=str(date), source="stage1.DATE_OVERRIDES", confidence="high")

    # Stage 2
    for eid, year in (s2.get("YEAR_OVERRIDES") or {}).items():
        upsert(eid, year=int(year), source="stage2.YEAR_OVERRIDES", confidence="high")
    for eid, loc in (s2.get("LOCATION_OVERRIDES") or {}).items():
        upsert(eid, location=str(loc), source="stage2.LOCATION_OVERRIDES", confidence="high")
    for eid, et in (s2.get("EVENT_TYPE_OVERRIDES") or {}).items():
        upsert(eid, event_type=str(et), source="stage2.EVENT_TYPE_OVERRIDES", confidence="high")
    for eid, nm in (s2.get("EVENT_NAME_OVERRIDES") or {}).items():
        upsert(eid, event_name=str(nm), source="stage2.EVENT_NAME_OVERRIDES", confidence="high")

    # Sets: exclude / broken_source flags
    for eid in (s2.get("JUNK_EVENTS_TO_EXCLUDE") or set()):
        upsert(eid, exclude=True, reason="JUNK_EVENTS_TO_EXCLUDE", source="stage2.JUNK_EVENTS_TO_EXCLUDE", confidence="high")
    for eid in (s2.get("KNOWN_BROKEN_SOURCE_EVENTS") or set()):
        upsert(eid, broken_source=True, reason="KNOWN_BROKEN_SOURCE_EVENTS", source="stage2.KNOWN_BROKEN_SOURCE_EVENTS", confidence="high")

    # write JSONL deterministically
    with args.out.open("w", encoding="utf-8") as f:
        for eid in sorted(by_event.keys(), key=lambda x: int(x) if x.isdigit() else x):
            f.write(json.dumps(by_event[eid], ensure_ascii=False, sort_keys=True) + "\n")

    print(f"Wrote {args.out} with {len(by_event)} event override records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
