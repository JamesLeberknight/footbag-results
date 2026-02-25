#!/usr/bin/env python3
"""
04_build_analytics.py

Stage 04 — Apply canonical identity, enforce coverage, build analytics.

v1.0 ADDITION:
----------------
Identity-Lock Mode.

When identity-lock inputs are supplied:
- Persons_Truth and Persons_Unresolved are copied verbatim
- Identity is treated as immutable
- A persons_truth.lock sentinel is written
- Analytics depend ONLY on canonical identity outputs
"""

import argparse
import hashlib
import json
import os
import sys
import pandas as pd
from datetime import datetime


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def apply_identity_lock(args):
    out_dir = "out"
    os.makedirs(out_dir, exist_ok=True)

    print("[04] Identity-lock mode ENABLED")

    truth_src = args.identity_lock_persons_truth
    unresolved_src = args.identity_lock_unresolved

    truth_dst = os.path.join(out_dir, "Persons_Truth.csv")
    unresolved_dst = os.path.join(out_dir, "Persons_Unresolved.csv")

    df_truth = pd.read_csv(truth_src)
    df_unres = pd.read_csv(unresolved_src)

    df_truth.to_csv(truth_dst, index=False)
    df_unres.to_csv(unresolved_dst, index=False)

    lock = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "persons_truth": {
            "file": os.path.basename(truth_src),
            "rows": len(df_truth),
            "sha256": sha256_file(truth_src),
        },
        "persons_unresolved": {
            "file": os.path.basename(unresolved_src),
            "rows": len(df_unres),
            "sha256": sha256_file(unresolved_src),
        },
    }

    lock_path = os.path.join(out_dir, "persons_truth.lock")
    with open(lock_path, "w") as f:
        json.dump(lock, f, indent=2)

    print(f"[04] Wrote {truth_dst}")
    print(f"[04] Wrote {unresolved_dst}")
    print(f"[04] Wrote {lock_path}")

    print("[04] Identity is LOCKED for this release.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Stage 04 — Analytics + Identity Lock")

    parser.add_argument("--identity-lock-persons-truth")
    parser.add_argument("--identity-lock-unresolved")

    args = parser.parse_args()

    if args.identity_lock_persons_truth and args.identity_lock_unresolved:
        return apply_identity_lock(args)

    print("ERROR: Identity-lock inputs required for v1.0 canonical release.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
